package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ucantone/did"

	"github.com/fil-forge/ingot/taskrunner"
)

// MultipartQueue is the task queue a multipart upload's changes run on: its
// creation, its parts, its Complete and its Abort. Its subject is the upload
// ID, so one change to an upload runs at a time. It is first in the queue
// order: a create runs queue-bucket.admit-upload as a child, and a Complete
// runs queue-bucket.commit as one.
const MultipartQueue = "queue-multipart-upload"

// The queue-multipart-upload actions.
const (
	MultipartCreateAction   = "create"
	MultipartAddPartAction  = "add-part"
	MultipartCompleteAction = "complete"
	MultipartAbortAction    = "abort"
	MultipartReapAction     = "reap"
)

// CreatedSession is the write that records a new upload, open.
func CreatedSession(s MultipartSession) (Statement, error) {
	if !s.Space.Defined() {
		return Statement{}, errors.New("registry: create session: space is required")
	}
	meta, err := marshalMetadata(s.Metadata)
	if err != nil {
		return Statement{}, err
	}
	return Statement{
		`INSERT INTO ingot.multipart_sessions
		   (upload_id, bucket, object_key, state, content_type, metadata,
		    content_encoding, content_disposition, content_language, cache_control, expires,
		    website_redirect_location, checksum_algorithm, checksum_type,
		    lock_mode, lock_retain_until, lock_legal_hold, tagging, space, plaintext)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)`,
		[]any{s.UploadID, s.Bucket, s.ObjectKey, SessionOpen, nullString(s.ContentType), meta,
			nullString(s.ContentEncoding), nullString(s.ContentDisposition),
			nullString(s.ContentLanguage), nullString(s.CacheControl), nullString(s.Expires),
			nullString(s.WebsiteRedirectLocation), nullString(s.ChecksumAlgorithm), nullString(s.ChecksumType),
			nullString(s.LockMode), s.LockRetainUntil, nullString(s.LockLegalHold), nullString(s.Tagging),
			s.Space.String(), s.Plaintext},
	}, nil
}

// DeletedSession is the write that deletes an upload; its parts and their
// trees go with it.
func DeletedSession(uploadID string) Statement {
	return Statement{`DELETE FROM ingot.multipart_sessions WHERE upload_id = $1`, []any{uploadID}}
}

// ReapedSession is the write that deletes a completed upload; an open one,
// which no Complete has committed, stays.
func ReapedSession(uploadID string) Statement {
	return Statement{`DELETE FROM ingot.multipart_sessions WHERE upload_id = $1 AND state = $2`, []any{uploadID, SessionCompleted}}
}

// CompletedSession is the write that records uploadID's Complete with the
// object it committed: the session moves from open to completed with the
// object's etag and versionID ("" for none). It is a guard: an upload no
// longer open fails the commit, which then runs again and refuses the
// Complete.
func CompletedSession(uploadID, etag, versionID string) Statement {
	return Statement{
		`UPDATE ingot.multipart_sessions
		    SET state = $2, committed_etag = $3, committed_version_id = $4, state_changed_at = now()
		  WHERE upload_id = $1 AND state = $5`,
		[]any{uploadID, SessionCompleted, etag, nullString(versionID), SessionOpen},
	}
}

// DeletedPartTrees is the write that drops uploadID's part trees, which a
// committed object's manifest makes dead.
func DeletedPartTrees(uploadID string) Statement {
	return Statement{`DELETE FROM ingot.multipart_part_trees WHERE upload_id = $1`, []any{uploadID}}
}

// PartWrites returns the writes that record p, a part of an open upload, and
// its tree (nil for none), superseding a prior row for the part number and
// its tree row. They take the part's pending references over, queue the
// upload's accept, and queue a release, due now and carrying release's
// authority, of each blob the replaced row named and p does not; those blobs
// are returned for the caller to run. An upload missing or no longer open is
// ErrNotFound, and a reference already gone ErrReferenceExpired. The writes
// run on the upload's subject, so the row read here is the one they replace.
func (r *Postgres) PartWrites(ctx context.Context, p MultipartPart, tree *PartTree, release ReleaseTask) (CommitWrites, []multihash.Multihash, error) {
	var w CommitWrites
	var spaceStr, state string
	err := r.pool.QueryRow(ctx, `SELECT space, state FROM ingot.multipart_sessions WHERE upload_id = $1`, p.UploadID).Scan(&spaceStr, &state)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && state != SessionOpen) {
		return w, nil, ErrNotFound
	}
	if err != nil {
		return w, nil, fmt.Errorf("registry: put part: read session: %w", err)
	}
	space, err := did.Parse(spaceStr)
	if err != nil {
		return w, nil, fmt.Errorf("registry: put part: session space: %w", err)
	}
	var replaced []multihash.Multihash
	if err := r.pool.QueryRow(ctx,
		`SELECT blob_digests FROM ingot.multipart_parts WHERE upload_id = $1 AND part_number = $2`,
		p.UploadID, p.PartNumber).Scan(&replaced); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return w, nil, fmt.Errorf("registry: put part: read replaced: %w", err)
	}

	pstate := p.State
	if pstate == "" {
		pstate = PartParked
	}
	// BlobDigests ([]multihash.Multihash) rides pgx's underlying-type plan into
	// bytea[]: pgx must try that plan before its Stringer plan, or a Multihash
	// (whose String() is base58) would land as text — pgx v5.10+ orders them
	// correctly; don't downgrade below that.
	//
	// The row lands only while the upload is open: a guard, though the
	// upload's subject already keeps a Complete or an Abort from running
	// meanwhile.
	w.Guards = append(w.Guards, Statement{
		`INSERT INTO ingot.multipart_parts (upload_id, part_number, etag_md5, size, checksum, blob_digests, span_lengths, state, unaccepted)
		 SELECT s.upload_id, $2, $3, $4, $5, $6, $7, $8, CASE WHEN $8 = 'parked' THEN $6::bytea[] ELSE '{}' END
		   FROM ingot.multipart_sessions s
		  WHERE s.upload_id = $1 AND s.state = $9
		 ON CONFLICT (upload_id, part_number) DO UPDATE
		   SET etag_md5 = EXCLUDED.etag_md5, size = EXCLUDED.size, checksum = EXCLUDED.checksum,
		       blob_digests = EXCLUDED.blob_digests, span_lengths = EXCLUDED.span_lengths, state = EXCLUDED.state,
		       unaccepted = EXCLUDED.unaccepted`,
		[]any{p.UploadID, p.PartNumber, p.ETagMD5, p.Size, p.Checksum, p.BlobDigests, p.SpanLengths, pstate, SessionOpen},
	})
	if err := r.refWrites(ctx, &w, p.Refs); err != nil {
		return CommitWrites{}, nil, err
	}
	// The tree row follows the part row: a superseding part without a tree
	// must not keep its predecessor's.
	if t := tree; t != nil {
		w.Statements = append(w.Statements, Statement{
			`INSERT INTO ingot.multipart_part_trees (upload_id, part_number, tree_offset, chunk_log, nodes, blocks, head, tail, root)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			 ON CONFLICT (upload_id, part_number) DO UPDATE
			   SET tree_offset = EXCLUDED.tree_offset, chunk_log = EXCLUDED.chunk_log, nodes = EXCLUDED.nodes,
			       blocks = EXCLUDED.blocks, head = EXCLUDED.head, tail = EXCLUDED.tail, root = EXCLUDED.root`,
			[]any{p.UploadID, p.PartNumber, t.Offset, int16(t.ChunkLog), t.Nodes, t.Blocks, t.Head, t.Tail, t.Root},
		})
	} else {
		w.Statements = append(w.Statements, Statement{
			`DELETE FROM ingot.multipart_part_trees WHERE upload_id = $1 AND part_number = $2`,
			[]any{p.UploadID, p.PartNumber},
		})
	}
	w.Spawns = append(w.Spawns, AcceptSpec(p.UploadID))
	// The replaced row's blobs this part does not name lose their last part
	// reference with this write; their releases are queued with it, due now
	// (a part blob was never readable through the catalog, so no reader
	// grace is owed).
	var released []multihash.Multihash
	for _, d := range digestsDropped(replaced, p.BlobDigests) {
		spec, err := release.Spec(space, d, DueNow())
		if err != nil {
			return CommitWrites{}, nil, err
		}
		w.Spawns = append(w.Spawns, spec)
		released = append(released, d)
	}
	return w, released, nil
}

// refWrites adds the writes that take refs, pending references, over for
// the row the commit writes: all or none, with their expiries cancelled. A
// reference already gone is ErrReferenceExpired; one that goes before the
// writes commit fails their guard.
func (r *Postgres) refWrites(ctx context.Context, w *CommitWrites, refs []taskrunner.TaskID) error {
	refs = sortedRefs(refs)
	if len(refs) == 0 {
		return nil
	}
	var expires []taskrunner.TaskID
	if err := r.pool.QueryRow(ctx,
		`SELECT coalesce(array_agg(expire_task), '{}') FROM ingot.blob_pending_refs WHERE add_task = ANY($1)`,
		refs).Scan(&expires); err != nil {
		return fmt.Errorf("registry: read pending references: %w", err)
	}
	if len(expires) != len(refs) {
		return ErrReferenceExpired
	}
	w.Guards = append(w.Guards, takeoverGuard(refs))
	w.Expiries = append(w.Expiries, expires...)
	return nil
}
