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
// interface. Segments are single-plane: the `plane` column scopes every
// query, and seq (from one shared sequence) is globally unique.

func (r *Postgres) NextSegmentSeq(ctx context.Context) (uint64, error) {
	var seq uint64
	if err := r.pool.QueryRow(ctx, `SELECT nextval('ingot.segment_seq')`).Scan(&seq); err != nil {
		return 0, fmt.Errorf("registry: next segment seq: %w", err)
	}
	return seq, nil
}

func (r *Postgres) InsertSegmentOpen(ctx context.Context, plane blockstore.Plane, seq uint64) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.segments (seq, plane, state, size_bytes)
		 VALUES ($1, $2, 'open', 0)
		 ON CONFLICT (seq) DO NOTHING`,
		int64(seq), plane.String())
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

func (r *Postgres) MarkSegmentShipped(ctx context.Context, plane blockstore.Plane, seq uint64, shippedAt int64, opRoots []blockstore.OpRoot) error {
	if plane == blockstore.PlaneData {
		_, err := r.pool.Exec(ctx,
			`UPDATE ingot.segments SET shipped_at = $2
			 WHERE seq = $1 AND plane = 'data' AND shipped_at IS NULL`,
			int64(seq), shippedAt)
		if err != nil {
			return fmt.Errorf("registry: mark data shipped %d: %w", seq, err)
		}
		return nil
	}

	// Catalog plane: stamp shipped_at AND advance forge_root_cid for every
	// op-root in this segment, atomically.
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin ship catalog %d: %w", seq, err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE ingot.segments SET shipped_at = $2
		 WHERE seq = $1 AND plane = 'catalog' AND shipped_at IS NULL`,
		int64(seq), shippedAt)
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
	// TODO(frrist/ingot): the UPDATE below is unconditional on root_cid,
	// which is incorrect when a writer's logstore.Commit succeeds but its
	// subsequent registry.CASRoot fails. Gate on root_cid. Tracked
	// separately.
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

func (r *Postgres) ListSegments(ctx context.Context, plane blockstore.Plane) ([]logstore.SegmentMeta, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT seq, state, COALESCE(sealed_at, 0), size_bytes, sha256, COALESCE(shipped_at, 0)
		   FROM ingot.segments
		  WHERE plane = $1 AND state IN ('open', 'sealed')
		  ORDER BY seq ASC`,
		plane.String())
	if err != nil {
		return nil, fmt.Errorf("registry: list %s segments: %w", plane, err)
	}
	defer rows.Close()

	var out []logstore.SegmentMeta
	for rows.Next() {
		var (
			seqInt    int64
			stateS    string
			sealed    int64
			size      int64
			sha       []byte
			shippedAt int64
		)
		if err := rows.Scan(&seqInt, &stateS, &sealed, &size, &sha, &shippedAt); err != nil {
			return nil, fmt.Errorf("registry: scan segment: %w", err)
		}
		state, ok := logstore.ParseState(stateS)
		if !ok {
			return nil, fmt.Errorf("registry: bad segment state %q for seq %d", stateS, seqInt)
		}
		out = append(out, logstore.SegmentMeta{
			Seq:       uint64(seqInt),
			Plane:     plane,
			State:     state,
			SealedAt:  sealed,
			Size:      size,
			SHA256:    sha,
			ShippedAt: shippedAt,
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
		   (seq, plane, state, sealed_at, size_bytes, sha256, shipped_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		int64(m.Seq), m.Plane.String(), m.State.String(), nullableInt(m.SealedAt),
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
