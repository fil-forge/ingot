package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/logstore"
)

// Segment-level methods for *Postgres. These satisfy logstore.Meta;
// the compile-time assertion at the bottom of the file pins the
// interface.

func (r *Postgres) NextSegmentSeq(ctx context.Context) (uint64, error) {
	var seq uint64
	if err := r.pool.QueryRow(ctx, `SELECT nextval('ingot.segment_seq')`).Scan(&seq); err != nil {
		return 0, fmt.Errorf("registry: next segment seq: %w", err)
	}
	return seq, nil
}

func (r *Postgres) InsertSegmentOpen(ctx context.Context, seq uint64) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.segments (seq, state, size_bytes) VALUES ($1, 'open', 0)
		 ON CONFLICT (seq) DO NOTHING`,
		int64(seq))
	if err != nil {
		return fmt.Errorf("registry: insert segment %d: %w", seq, err)
	}
	return nil
}

func (r *Postgres) MarkSegmentSealed(ctx context.Context, seq uint64, sealedAt int64, sizeBytes int64, sha256 []byte, opRoots []blockstore.OpRoot) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin seal %d: %w", seq, err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE ingot.segments
		   SET state = 'sealed', sealed_at = $2, size_bytes = $3, car_sha256 = $4
		 WHERE seq = $1 AND state = 'open'`,
		int64(seq), sealedAt, sizeBytes, sha256)
	if err != nil {
		return fmt.Errorf("registry: seal %d: %w", seq, err)
	}
	if tag.RowsAffected() == 0 {
		// Either the segment is missing or it has already advanced past
		// 'open'. Treat as a no-op so seal is idempotent against
		// crashes between disk seal and DB update.
		return nil
	}

	if err := insertOpRootsTx(ctx, tx, seq, opRoots); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("registry: commit seal %d: %w", seq, err)
	}
	return nil
}

func (r *Postgres) MarkSegmentFlushed(ctx context.Context, seq uint64, flushedAt int64, opRoots []blockstore.OpRoot) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin flush %d: %w", seq, err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE ingot.segments SET state = 'flushed', flushed_at = $2 WHERE seq = $1 AND state = 'sealed'`,
		int64(seq), flushedAt)
	if err != nil {
		return fmt.Errorf("registry: flush %d: %w", seq, err)
	}
	if tag.RowsAffected() == 0 {
		// Already flushed (or somehow rolled back to open). Idempotent.
		return nil
	}

	// Apply forge_root advances in slice order. Segments flush in seq
	// order, and within a segment the slice order is the order of
	// commits, so the last write for each bucket wins.
	//
	// TODO(frrist/ingot): the UPDATE below is unconditional on
	// root_cid, which is incorrect when a writer's logstore.Commit
	// succeeds but its subsequent registry.CASRoot fails (transient
	// Postgres error, context cancellation between the two calls).
	// In that case, the op_root for newRoot is durable in the log
	// even though the bucket's published root_cid never advanced.
	// When this segment flushes, the loop below blindly sets
	// forge_root_cid = newRoot — even though root_cid is still
	// oldRoot — breaking the invariant "forge_root_cid is a Root the
	// bucket has actually published, with its full DAG in Forge."
	//
	// Fix: gate the UPDATE on root_cid, e.g.
	//
	//   UPDATE ingot.buckets
	//      SET forge_root_cid = $1
	//    WHERE name = $2 AND root_cid = $1
	//
	// With segments flushing in seq order, this naturally lets a
	// later segment's flush advance forge_root_cid for the bucket
	// once root_cid has caught up via a successful CASRoot, and
	// silently skips orphan op_roots from failed commits.
	//
	// Out of scope for the bucketop refactor; track separately.
	for _, opr := range opRoots {
		if !opr.Root.Defined() {
			continue
		}
		if _, err := tx.Exec(ctx,
			`UPDATE ingot.buckets SET forge_root_cid = $1 WHERE name = $2`,
			opr.Root.Bytes(), opr.Bucket); err != nil {
			return fmt.Errorf("registry: advance forge_root for %q: %w", opr.Bucket, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("registry: commit flush %d: %w", seq, err)
	}
	return nil
}

func (r *Postgres) DeleteSegment(ctx context.Context, seq uint64) error {
	if _, err := r.pool.Exec(ctx, `DELETE FROM ingot.segments WHERE seq = $1`, int64(seq)); err != nil {
		return fmt.Errorf("registry: delete segment %d: %w", seq, err)
	}
	return nil
}

func (r *Postgres) ListUnflushedSegments(ctx context.Context) ([]logstore.SegmentMeta, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT seq, state, COALESCE(sealed_at, 0), COALESCE(flushed_at, 0), size_bytes, car_sha256
		   FROM ingot.segments
		  WHERE state IN ('open', 'sealed')
		  ORDER BY seq ASC`)
	if err != nil {
		return nil, fmt.Errorf("registry: list unflushed segments: %w", err)
	}
	defer rows.Close()

	var out []logstore.SegmentMeta
	for rows.Next() {
		var (
			seqInt  int64
			stateS  string
			sealed  int64
			flushed int64
			size    int64
			sha     []byte
		)
		if err := rows.Scan(&seqInt, &stateS, &sealed, &flushed, &size, &sha); err != nil {
			return nil, fmt.Errorf("registry: scan segment: %w", err)
		}
		state, ok := logstore.ParseState(stateS)
		if !ok {
			return nil, fmt.Errorf("registry: bad segment state %q for seq %d", stateS, seqInt)
		}
		out = append(out, logstore.SegmentMeta{
			Seq:       uint64(seqInt),
			State:     state,
			SealedAt:  sealed,
			FlushedAt: flushed,
			SizeBytes: size,
			SHA256:    sha,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list segments rows: %w", err)
	}

	// Hydrate op_roots for sealed segments only (open segments have
	// none). Done in a second pass to keep the query simple.
	for i := range out {
		if out[i].State != logstore.StateSealed {
			continue
		}
		ops, err := r.fetchOpRoots(ctx, out[i].Seq)
		if err != nil {
			return nil, err
		}
		out[i].OpRoots = ops
	}
	return out, nil
}

func (r *Postgres) RehydrateSegment(ctx context.Context, m logstore.SegmentMeta) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin rehydrate %d: %w", m.Seq, err)
	}
	defer tx.Rollback(ctx)

	// Replace any existing rows for this seq.
	if _, err := tx.Exec(ctx, `DELETE FROM ingot.segments WHERE seq = $1`, int64(m.Seq)); err != nil {
		return fmt.Errorf("registry: rehydrate clear %d: %w", m.Seq, err)
	}

	var sealedAt, flushedAt *int64
	if m.SealedAt != 0 {
		v := m.SealedAt
		sealedAt = &v
	}
	if m.FlushedAt != 0 {
		v := m.FlushedAt
		flushedAt = &v
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO ingot.segments (seq, state, sealed_at, flushed_at, size_bytes, car_sha256)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		int64(m.Seq), m.State.String(), sealedAt, flushedAt, m.SizeBytes, m.SHA256); err != nil {
		return fmt.Errorf("registry: rehydrate insert %d: %w", m.Seq, err)
	}

	if err := insertOpRootsTx(ctx, tx, m.Seq, m.OpRoots); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("registry: rehydrate commit %d: %w", m.Seq, err)
	}
	return nil
}

func (r *Postgres) fetchOpRoots(ctx context.Context, seq uint64) ([]blockstore.OpRoot, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT bucket, root_cid FROM ingot.segment_op_roots WHERE seq = $1 ORDER BY seq_within ASC`,
		int64(seq))
	if err != nil {
		return nil, fmt.Errorf("registry: fetch op_roots %d: %w", seq, err)
	}
	defer rows.Close()

	var out []blockstore.OpRoot
	for rows.Next() {
		var bucket string
		var rootBytes []byte
		if err := rows.Scan(&bucket, &rootBytes); err != nil {
			return nil, fmt.Errorf("registry: scan op_root: %w", err)
		}
		c, err := cid.Cast(rootBytes)
		if err != nil {
			return nil, fmt.Errorf("registry: bad root_cid for %q seq %d: %w", bucket, seq, err)
		}
		out = append(out, blockstore.OpRoot{Bucket: bucket, Root: c})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: fetch op_roots rows %d: %w", seq, err)
	}
	return out, nil
}

func insertOpRootsTx(ctx context.Context, tx pgx.Tx, seq uint64, opRoots []blockstore.OpRoot) error {
	if len(opRoots) == 0 {
		return nil
	}
	for i, opr := range opRoots {
		if !opr.Root.Defined() {
			return errors.New("registry: op_root.Root must be defined")
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO ingot.segment_op_roots (seq, seq_within, bucket, root_cid)
			 VALUES ($1, $2, $3, $4)`,
			int64(seq), i, opr.Bucket, opr.Root.Bytes()); err != nil {
			return fmt.Errorf("registry: insert op_root %d/%d: %w", seq, i, err)
		}
	}
	return nil
}

// Compile-time assertion: Postgres satisfies logstore.Meta.
var _ logstore.Meta = (*Postgres)(nil)
