package registry

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"

	"github.com/fil-forge/ingot/taskrunner"
)

// BucketQueue is the task queue a bucket's changes run on: its object
// commits, the shipping of its catalog, its settings and its lifecycle. Its
// subject is the bucket's space (BucketSubject), so one change to a bucket
// runs at a time.
const BucketQueue = "queue-bucket"

// The queue-bucket actions.
const (
	BucketCreateAction        = "create"
	BucketCommitAction        = "commit"
	BucketShipAction          = "ship"
	BucketSetVersioningAction = "set-versioning"
	BucketSetLockConfigAction = "set-object-lock-config"
	BucketDeleteAction        = "delete"
)

// BucketSubject is the subject of space's bucket on BucketQueue.
func BucketSubject(space did.DID) string { return space.String() }

// CommitWrites is what a root commit writes, for a queue-bucket.commit to
// commit with its outcome: its statements, the tasks it queues and the
// queue-space-shard.expire tasks it cancels.
type CommitWrites struct {
	// Statements are the writes, in order.
	Statements []Statement
	// Guards are writes that must each affect a row, or the commit fails and
	// runs again (taskrunner.Exec.WriteConditional): the root's advance from
	// the root the commit read, and the take-over of its pending references.
	Guards []Statement
	// Spawns are the tasks the commit queues: upload registrations and
	// releases.
	Spawns []taskrunner.Spec
	// Expiries are the queue-space-shard.expire tasks of the pending
	// references the commit's claims take over.
	Expiries []taskrunner.TaskID
}

// RootCommitWrites returns the writes that advance bucket name's root from
// expect to next and record c with it: c's catalog blocks journaled under
// their segment while it is unshipped, its upload registrations, its claims
// (taking over their pending references), its drops (queueing the release of
// each digest the space no longer claims afterwards, due c.ReleaseGrace from
// now) and gc, the catalog blocks the commit made garbage. It reads what it
// needs before any of them is written, so it runs under the bucket's subject,
// where nothing else changes the bucket's claims meanwhile. A pending
// reference already gone fails it with ErrReferenceExpired; one that goes
// before the writes commit fails their conditional take-over.
func (r *Postgres) RootCommitWrites(ctx context.Context, name string, expect, next cid.Cid, c RootCommit, gc []cid.Cid) (CommitWrites, error) {
	var w CommitWrites
	if j := c.Journal; len(j.Blocks) > 0 {
		cids := make([][]byte, len(j.Blocks))
		data := make([][]byte, len(j.Blocks))
		for i, b := range j.Blocks {
			cids[i] = b.Cid().Bytes()
			data[i] = b.RawData()
		}
		// A segment already shipped has its blocks on Forge, and its ship
		// runs on this subject too, so it cannot ship meanwhile. A bucket
		// already gone journals nothing, and its root's advance then fails.
		w.Statements = append(w.Statements, Statement{
			SQL: `INSERT INTO ingot.catalog_journal (seq, bucket, cid, data)
			      SELECT $1, $2, c, d FROM unnest($3::bytea[], $4::bytea[]) AS u(c, d)
			       WHERE EXISTS (SELECT 1 FROM ingot.segments WHERE seq = $1 AND shipped_at IS NULL)
			         AND EXISTS (SELECT 1 FROM ingot.buckets WHERE name = $2)
			      ON CONFLICT (seq, cid) DO NOTHING`,
			Args: []any{int64(j.Segment), name, cids, data},
		})
	}
	var expectBytes []byte
	if expect.Defined() {
		expectBytes = expect.Bytes()
	}
	w.Guards = append(w.Guards, Statement{
		SQL:  `UPDATE ingot.buckets SET root_cid = $1 WHERE name = $2 AND root_cid IS NOT DISTINCT FROM $3`,
		Args: []any{next.Bytes(), name, expectBytes},
	})
	for _, reg := range c.Registrations {
		spec, err := reg.Spec()
		if err != nil {
			return CommitWrites{}, err
		}
		w.Spawns = append(w.Spawns, spec)
	}
	if err := r.claimWrites(ctx, &w, c.Claims); err != nil {
		return CommitWrites{}, err
	}
	if err := r.dropWrites(ctx, &w, c); err != nil {
		return CommitWrites{}, err
	}
	if len(gc) > 0 {
		raw := make([][]byte, len(gc))
		for i, c := range gc {
			raw[i] = c.Bytes()
		}
		w.Statements = append(w.Statements, Statement{
			SQL: `INSERT INTO ingot.gc_candidates (cid, bucket)
			      SELECT c, $2 FROM unnest($1::bytea[]) AS u(c)
			      ON CONFLICT (cid) DO NOTHING`,
			Args: []any{raw, name},
		})
	}
	return w, nil
}

// claimWrites adds the writes that record claims: the pending references
// they take over, all or none, and the claims themselves, which publish the
// space's intent for each digest (IntentPublished).
func (r *Postgres) claimWrites(ctx context.Context, w *CommitWrites, claims []BlobClaim) error {
	if len(claims) == 0 {
		return nil
	}
	n := len(claims)
	digests := make([][]byte, n)
	spaces := make([]string, n)
	buckets := make([]string, n)
	keys := make([]string, n)
	versions := make([]string, n)
	var refs []taskrunner.TaskID
	for i, c := range claims {
		digests[i], spaces[i], buckets[i], keys[i], versions[i] = c.Digest, c.Space.String(), c.Bucket, c.ObjectKey, c.VersionID
		if c.Ref != uuid.Nil {
			refs = append(refs, c.Ref)
		}
	}
	slices.SortFunc(refs, func(a, b taskrunner.TaskID) int { return bytes.Compare(a[:], b[:]) })
	refs = slices.Compact(refs)
	if len(refs) > 0 {
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
	}
	w.Statements = append(w.Statements,
		Statement{
			SQL: `INSERT INTO ingot.blob_refs (digest, bucket, object_key, version_id, space)
			      SELECT * FROM unnest($1::bytea[], $2::text[], $3::text[], $4::text[], $5::text[])
			      ON CONFLICT (digest, bucket, object_key, version_id) DO NOTHING`,
			Args: []any{digests, buckets, keys, versions, spaces},
		},
		Statement{
			SQL: `UPDATE ingot.upload_intents i SET state = $3, updated_at = now()
			        FROM (SELECT DISTINCT s, d FROM unnest($1::text[], $2::bytea[]) AS u(s, d)) u
			       WHERE i.space = u.s AND i.digest = u.d AND i.state <> $3`,
			Args: []any{spaces, digests, IntentPublished},
		},
	)
	return nil
}

// dropWrites adds the writes that delete c's drops, and queues a release for
// each dropped digest the space claims nothing else on once the commit lands:
// no claim outside the drops, and none among c's own claims.
func (r *Postgres) dropWrites(ctx context.Context, w *CommitWrites, c RootCommit) error {
	if len(c.Drops) == 0 {
		return nil
	}
	n := len(c.Drops)
	digests := make([][]byte, n)
	spaces := make([]string, n)
	buckets := make([]string, n)
	keys := make([]string, n)
	versions := make([]string, n)
	for i, d := range c.Drops {
		digests[i], spaces[i], buckets[i], keys[i], versions[i] = d.Digest, d.Space.String(), d.Bucket, d.ObjectKey, d.VersionID
	}
	rows, err := r.pool.Query(ctx,
		`SELECT DISTINCT r.space, r.digest FROM ingot.blob_refs r
		   JOIN (SELECT DISTINCT s, d FROM unnest($1::text[], $2::bytea[]) AS u(s, d)) u ON r.space = u.s AND r.digest = u.d
		  WHERE (r.digest, r.bucket, r.object_key, r.version_id) NOT IN
		        (SELECT * FROM unnest($2::bytea[], $3::text[], $4::text[], $5::text[]))`,
		spaces, digests, buckets, keys, versions)
	if err != nil {
		return fmt.Errorf("registry: drop claims: remaining claims: %w", err)
	}
	claimed := map[string]bool{}
	for rows.Next() {
		var s string
		var d []byte
		if err := rows.Scan(&s, &d); err != nil {
			rows.Close()
			return fmt.Errorf("registry: drop claims: remaining claims: %w", err)
		}
		claimed[s+"\x00"+string(d)] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("registry: drop claims: remaining claims: %w", err)
	}
	for _, cl := range c.Claims {
		claimed[cl.Space.String()+"\x00"+string(cl.Digest)] = true
	}
	w.Statements = append(w.Statements, Statement{
		SQL: `DELETE FROM ingot.blob_refs r
		       USING unnest($1::bytea[], $2::text[], $3::text[], $4::text[]) AS u(d, b, k, v)
		       WHERE r.digest = u.d AND r.bucket = u.b AND r.object_key = u.k AND r.version_id = u.v`,
		Args: []any{digests, buckets, keys, versions},
	})
	// The grace runs from now, after the catalog fsync: a reader that
	// resolved the discarded version meanwhile is owed the whole of it. A
	// release already pending keeps the later time.
	due := time.Now().Add(c.ReleaseGrace)
	if c.ReleaseGrace <= 0 {
		due = DueNow()
	}
	seen := map[string]bool{}
	for _, d := range c.Drops {
		k := d.Space.String() + "\x00" + string(d.Digest)
		if claimed[k] || seen[k] {
			continue
		}
		seen[k] = true
		spec, err := c.Release.Spec(d.Space, d.Digest, due)
		if err != nil {
			return err
		}
		w.Spawns = append(w.Spawns, spec)
	}
	return nil
}

// PendingRefsHeld reports whether every one of refs, the IDs of the
// queue-space-shard.add tasks that took them, is still a pending reference.
func (r *Postgres) PendingRefsHeld(ctx context.Context, refs []taskrunner.TaskID) (bool, error) {
	refs = slices.Clone(refs)
	slices.SortFunc(refs, func(a, b taskrunner.TaskID) int { return bytes.Compare(a[:], b[:]) })
	refs = slices.Compact(refs)
	if len(refs) == 0 {
		return true, nil
	}
	var n int
	if err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM ingot.blob_pending_refs WHERE add_task = ANY($1)`, refs).Scan(&n); err != nil {
		return false, fmt.Errorf("registry: read pending references: %w", err)
	}
	return n == len(refs), nil
}

// CompletedSession is the write that records uploadID's Complete with the
// commit that installs its object: it moves the session from completing to
// completed with the object's etag and versionID ("" for none). It is a
// guard: a session no longer completing, as a teardown leaves it, fails the
// commit, which then runs again and refuses the Complete.
func CompletedSession(uploadID, etag, versionID string) Statement {
	return Statement{
		`UPDATE ingot.multipart_sessions
		    SET state = $2, committed_etag = $3, committed_version_id = $4, state_changed_at = now()
		  WHERE upload_id = $1 AND state = $5`,
		[]any{uploadID, SessionCompleted, etag, nullString(versionID), SessionCompleting},
	}
}

// takeoverGuard is the guard that takes refs, pending references, over for
// the rows a commit writes: it deletes them and holds only if it deleted every
// one. An expiry that drops a reference after the commit read them, even while
// this statement runs, leaves fewer rows deleted than references: the
// statement then returns no row, the guard fails, and the commit rolls back
// with the rows it did delete.
func takeoverGuard(refs []taskrunner.TaskID) Statement {
	return Statement{
		SQL: `WITH taken AS (DELETE FROM ingot.blob_pending_refs WHERE add_task = ANY($1) RETURNING 1)
		      SELECT 1 WHERE (SELECT count(*) FROM taken) = $2`,
		Args: []any{refs, len(refs)},
	}
}

