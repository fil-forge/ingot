package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/logstore"
)

// Segment-level methods for *Postgres. These satisfy logstore.Meta;
// the compile-time assertion at the bottom of the file pins the
// interface. Segments are single-plane: the `plane` column scopes every
// query, and seq (from one shared sequence) is globally unique.

func (r *Postgres) NextSegmentSeq(ctx context.Context) (uint64, error) {
	var seq uint64
	if err := r.pool.QueryRow(ctx, `SELECT nextval('ingot.segment_seq')`).Scan(&seq); err != nil {
		return 0, fmt.Errorf("registry: next segment seq: %w", err)
	}
	return seq, nil
}

func (r *Postgres) InsertSegmentOpen(ctx context.Context, plane blockstore.Plane, seq uint64, bucket string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.segments (seq, plane, bucket, state, size_bytes)
		 VALUES ($1, $2, $3, 'open', 0)
		 ON CONFLICT (seq) DO NOTHING`,
		int64(seq), plane.String(), bucket)
	if err != nil {
		return fmt.Errorf("registry: insert %s segment %d: %w", plane, seq, err)
	}
	return nil
}

func (r *Postgres) MarkSegmentSealed(ctx context.Context, plane blockstore.Plane, seq uint64, sealedAt int64,
	size int64, sha []byte, opRoots []blockstore.OpRoot) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin seal %d: %w", seq, err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE ingot.segments
		   SET state = 'sealed', sealed_at = $2, size_bytes = $3, sha256 = $4
		 WHERE seq = $1 AND plane = $5 AND state = 'open'`,
		int64(seq), sealedAt, size, sha, plane.String())
	if err != nil {
		return fmt.Errorf("registry: seal %d: %w", seq, err)
	}
	if tag.RowsAffected() == 0 {
		// Missing or already past 'open' — idempotent no-op.
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

func (r *Postgres) MarkSegmentShipped(ctx context.Context, plane blockstore.Plane, seq uint64, shippedAt int64, indexDigest multihash.Multihash, opRoots []blockstore.OpRoot) error {
	// Only the catalog plane ships: stamp shipped_at AND advance forge_root_cid
	// for every op-root in this segment, atomically.
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin ship catalog %d: %w", seq, err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE ingot.segments SET shipped_at = $2, index_digest = $3
		 WHERE seq = $1 AND plane = 'catalog' AND shipped_at IS NULL`,
		int64(seq), shippedAt, indexDigest)
	if err != nil {
		return fmt.Errorf("registry: mark catalog shipped %d: %w", seq, err)
	}
	if tag.RowsAffected() == 0 {
		// Already shipped — idempotent.
		return nil
	}

	// Apply forge_root advances in slice order. Segments ship in seq
	// order, and within a segment the slice order is commit order, so the
	// last write for each bucket wins.
	//
	// The advance is GUARDED on root_cid: forge_root_cid moves to opr.Root only
	// if the bucket's committed root still equals opr.Root. This closes the
	// orphan-root hole — a writer whose logstore.Commit landed an op-root but
	// whose subsequent CASRoot failed (cross-process race) leaves a durable
	// op-root the bucket never adopted; without the guard, shipping that
	// segment would advance forge_root past the bucket's real root.
	for _, opr := range opRoots {
		if !opr.Root.Defined() {
			continue
		}
		if _, err := tx.Exec(ctx,
			`UPDATE ingot.buckets SET forge_root_cid = $1 WHERE name = $2 AND root_cid = $1`,
			opr.Root.Bytes(), opr.Bucket); err != nil {
			return fmt.Errorf("registry: advance forge_root for %q: %w", opr.Bucket, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("registry: commit ship catalog %d: %w", seq, err)
	}
	return nil
}

func (r *Postgres) DeleteSegment(ctx context.Context, plane blockstore.Plane, seq uint64) error {
	if _, err := r.pool.Exec(ctx,
		`DELETE FROM ingot.segments WHERE seq = $1 AND plane = $2`,
		int64(seq), plane.String()); err != nil {
		return fmt.Errorf("registry: delete segment %d: %w", seq, err)
	}
	return nil
}

func (r *Postgres) ListSegments(ctx context.Context, plane blockstore.Plane, bucket string) ([]logstore.SegmentMeta, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT seq, state, COALESCE(sealed_at, 0), size_bytes, sha256, COALESCE(shipped_at, 0), index_digest
		   FROM ingot.segments
		  WHERE plane = $1 AND bucket = $2 AND state IN ('open', 'sealed')
		  ORDER BY seq ASC`,
		plane.String(), bucket)
	if err != nil {
		return nil, fmt.Errorf("registry: list %s segments for %q: %w", plane, bucket, err)
	}
	defer rows.Close()

	var out []logstore.SegmentMeta
	for rows.Next() {
		var (
			seqInt      int64
			stateS      string
			sealed      int64
			size        int64
			sha         []byte
			shippedAt   int64
			indexDigest multihash.Multihash
		)
		if err := rows.Scan(&seqInt, &stateS, &sealed, &size, &sha, &shippedAt, &indexDigest); err != nil {
			return nil, fmt.Errorf("registry: scan segment: %w", err)
		}
		state, ok := logstore.ParseState(stateS)
		if !ok {
			return nil, fmt.Errorf("registry: bad segment state %q for seq %d", stateS, seqInt)
		}
		out = append(out, logstore.SegmentMeta{
			Seq:         uint64(seqInt),
			Plane:       plane,
			Bucket:      bucket,
			State:       state,
			SealedAt:    sealed,
			Size:        size,
			SHA256:      sha,
			ShippedAt:   shippedAt,
			IndexDigest: indexDigest,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list segments rows: %w", err)
	}

	// Hydrate op_roots for sealed segments (open segments have none; data
	// segments never have any).
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

func (r *Postgres) ListSegmentBuckets(ctx context.Context, plane blockstore.Plane) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT DISTINCT bucket FROM ingot.segments WHERE plane = $1 ORDER BY bucket ASC`,
		plane.String())
	if err != nil {
		return nil, fmt.Errorf("registry: list %s segment buckets: %w", plane, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var bucket string
		if err := rows.Scan(&bucket); err != nil {
			return nil, fmt.Errorf("registry: scan segment bucket: %w", err)
		}
		out = append(out, bucket)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list segment buckets rows: %w", err)
	}
	return out, nil
}

func (r *Postgres) RehydrateSegment(ctx context.Context, m logstore.SegmentMeta) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin rehydrate %d: %w", m.Seq, err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM ingot.segments WHERE seq = $1`, int64(m.Seq)); err != nil {
		return fmt.Errorf("registry: rehydrate clear %d: %w", m.Seq, err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO ingot.segments
		   (seq, plane, bucket, state, sealed_at, size_bytes, sha256, shipped_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		int64(m.Seq), m.Plane.String(), m.Bucket, m.State.String(), nullableInt(m.SealedAt),
		m.Size, m.SHA256, nullableInt(m.ShippedAt)); err != nil {
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

// nullableInt maps a zero timestamp to SQL NULL.
func nullableInt(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
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
