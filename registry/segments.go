package registry

import (
	"context"
	"errors"
	"fmt"

	block "github.com/ipfs/go-block-format"
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
	if tag.RowsAffected() > 0 {
		// Apply forge_root advances in slice order: within a segment the
		// slice order is commit order, so the last write for each bucket
		// wins. Segments needn't ship in seq order (recovery queues the
		// newest first); the guard below keeps an older segment's advance
		// from moving forge_root_cid back.
		//
		// The advance is GUARDED on root_cid: forge_root_cid moves to
		// opr.Root only if the bucket's committed root still equals
		// opr.Root. This closes the orphan-root hole — a writer whose
		// logstore.Commit landed an op-root but whose subsequent CASRoot
		// failed (cross-process race) leaves a durable op-root the bucket
		// never adopted; without the guard, shipping that segment would
		// advance forge_root past the bucket's real root.
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
	}
	// The segment's journaled blocks are on Forge now, so drop them, also
	// when an earlier stamp already landed (idempotent). The UPDATE above
	// took the segment row's lock first, so a commit that was journaling
	// into this segment (journalBlocks holds a share lock on the row) has
	// committed, and this statement sees its rows; any later commit sees the
	// segment shipped and journals nothing.
	//
	// Deleting a bucket, or emptying its root (EmptyRoot), locks its row and
	// then deletes its journal rows, so the rows' bucket is locked first here
	// too: the advances above lock it only where the guard matched. KEY SHARE
	// conflicts with those FOR UPDATE locks but not with a commit's root
	// swap.
	if _, err := tx.Exec(ctx, `
		SELECT 1 FROM ingot.buckets
		 WHERE name IN (SELECT bucket FROM ingot.catalog_journal WHERE seq = $1)
		 ORDER BY name
		   FOR KEY SHARE`, int64(seq)); err != nil {
		return fmt.Errorf("registry: lock buckets of catalog segment %d's journal: %w", seq, err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM ingot.catalog_journal WHERE seq = $1`, int64(seq)); err != nil {
		return fmt.Errorf("registry: drop journal of catalog segment %d: %w", seq, err)
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

// RetireSegment stamps retired_at and drops the segment's op-root rows in
// one transaction. The op-roots were only needed to advance forge_root at
// ship time and to recover the segment from disk; neither applies once its
// files are gone.
func (r *Postgres) RetireSegment(ctx context.Context, plane blockstore.Plane, seq uint64, retiredAt int64) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin retire segment %d: %w", seq, err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE ingot.segments SET retired_at = $3
		 WHERE seq = $1 AND plane = $2 AND retired_at IS NULL`,
		int64(seq), plane.String(), retiredAt)
	if err != nil {
		return fmt.Errorf("registry: retire segment %d: %w", seq, err)
	}
	if tag.RowsAffected() == 0 {
		// Already retired, or no such row — idempotent.
		return nil
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM ingot.segment_op_roots WHERE seq = $1`, int64(seq)); err != nil {
		return fmt.Errorf("registry: drop op-roots of retired segment %d: %w", seq, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("registry: commit retire segment %d: %w", seq, err)
	}
	return nil
}

func (r *Postgres) ListSegments(ctx context.Context, plane blockstore.Plane, bucket string) ([]logstore.SegmentMeta, error) {
	out, err := r.listSegments(ctx, plane, bucket, false)
	if err != nil {
		return nil, err
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

func (r *Postgres) ListRetiredSegments(ctx context.Context, plane blockstore.Plane, bucket string) ([]logstore.SegmentMeta, error) {
	return r.listSegments(ctx, plane, bucket, true)
}

// listSegments reads bucket's segment rows for plane, either the unretired
// ones or the retired ones, ordered by seq, without op-roots.
func (r *Postgres) listSegments(ctx context.Context, plane blockstore.Plane, bucket string, retired bool) ([]logstore.SegmentMeta, error) {
	// The predicate is spelled out rather than parameterized, so the planner
	// can use the partial index on unretired rows.
	which := "retired_at IS NULL"
	if retired {
		which = "retired_at IS NOT NULL"
	}
	rows, err := r.pool.Query(ctx,
		`SELECT seq, state, COALESCE(sealed_at, 0), size_bytes, sha256, COALESCE(shipped_at, 0), index_digest,
		        COALESCE(retired_at, 0)
		   FROM ingot.segments
		  WHERE plane = $1 AND bucket = $2 AND `+which+`
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
			retiredAt   int64
		)
		if err := rows.Scan(&seqInt, &stateS, &sealed, &size, &sha, &shippedAt, &indexDigest, &retiredAt); err != nil {
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
			RetiredAt:   retiredAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list segments rows: %w", err)
	}
	return out, nil
}

func (r *Postgres) ListSegmentBuckets(ctx context.Context, plane blockstore.Plane) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT DISTINCT bucket FROM ingot.segments
		  WHERE plane = $1 AND retired_at IS NULL
		  ORDER BY bucket ASC`,
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

// EnsureSegmentSeqAbove moves ingot.segment_seq past seq unless it already
// is (logstore.Meta). Ingot calls it at startup, before its first nextval,
// and no other process draws from the sequence, so the read and the setval
// race nothing.
func (r *Postgres) EnsureSegmentSeqAbove(ctx context.Context, seq uint64) error {
	if _, err := r.pool.Exec(ctx,
		`SELECT setval('ingot.segment_seq', $1)
		   FROM ingot.segment_seq
		  WHERE last_value < $1 OR (last_value = $1 AND NOT is_called)`,
		int64(seq)); err != nil {
		return fmt.Errorf("registry: move segment seq past %d: %w", seq, err)
	}
	return nil
}

func (r *Postgres) JournalBuckets(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT DISTINCT bucket FROM ingot.catalog_journal ORDER BY bucket`)
	if err != nil {
		return nil, fmt.Errorf("registry: list journal buckets: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("registry: list journal buckets: %w", err)
	}
	return out, nil
}

func (r *Postgres) JournalSegments(ctx context.Context, bucket string) ([]uint64, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT DISTINCT seq FROM ingot.catalog_journal WHERE bucket = $1 ORDER BY seq`, bucket)
	if err != nil {
		return nil, fmt.Errorf("registry: list journal segments of %q: %w", bucket, err)
	}
	seqs, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("registry: list journal segments of %q: %w", bucket, err)
	}
	out := make([]uint64, len(seqs))
	for i, s := range seqs {
		out[i] = uint64(s)
	}
	return out, nil
}

// ReadJournal pages through one segment's journaled blocks on the primary
// key, (seq, cid) (logstore.Meta).
func (r *Postgres) ReadJournal(ctx context.Context, seq uint64, after cid.Cid, limit int) ([]block.Block, error) {
	afterKey := []byte{} // a nil slice would bind NULL, and cid > NULL matches nothing
	if after.Defined() {
		afterKey = after.Bytes()
	}
	rows, err := r.pool.Query(ctx,
		`SELECT cid, data FROM ingot.catalog_journal
		  WHERE seq = $1 AND cid > $2
		  ORDER BY cid LIMIT $3`,
		int64(seq), afterKey, limit)
	if err != nil {
		return nil, fmt.Errorf("registry: read journal of segment %d: %w", seq, err)
	}
	defer rows.Close()
	var out []block.Block
	for rows.Next() {
		var key, data []byte
		if err := rows.Scan(&key, &data); err != nil {
			return nil, fmt.Errorf("registry: scan journal of segment %d: %w", seq, err)
		}
		c, err := cid.Cast(key)
		if err != nil {
			return nil, fmt.Errorf("registry: journal of segment %d: bad cid: %w", seq, err)
		}
		blk, err := block.NewBlockWithCid(data, c)
		if err != nil {
			return nil, fmt.Errorf("registry: journal of segment %d: block %s: %w", seq, c, err)
		}
		out = append(out, blk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: read journal of segment %d: %w", seq, err)
	}
	return out, nil
}

// MoveJournal re-files a range of a segment's journaled blocks under another
// segment (logstore.Meta), keeping each row's created_at so the journal's age
// gauge still reports how long the block has waited. The locks follow the
// journal's order: the bucket row first, as a deletion or EmptyRoot takes it,
// then the target segment's row, as a commit journaling into it does. A
// target that has shipped since takes no rows, and the source's rows go.
func (r *Postgres) MoveJournal(ctx context.Context, bucket string, from, to uint64, first, last cid.Cid) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin journal move %d→%d: %w", from, to, err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx,
		`SELECT 1 FROM ingot.buckets WHERE name = $1 FOR KEY SHARE`, bucket); err != nil {
		return fmt.Errorf("registry: journal move %d→%d: lock bucket: %w", from, to, err)
	}
	var shipped bool
	if err := tx.QueryRow(ctx,
		`SELECT shipped_at IS NOT NULL FROM ingot.segments WHERE seq = $1 FOR SHARE`,
		int64(to)).Scan(&shipped); err != nil {
		return fmt.Errorf("registry: journal move %d→%d: lock target segment: %w", from, to, err)
	}
	if !shipped {
		if _, err := tx.Exec(ctx,
			`INSERT INTO ingot.catalog_journal (seq, bucket, cid, data, created_at)
			 SELECT $2, bucket, cid, data, created_at FROM ingot.catalog_journal
			  WHERE seq = $1 AND bucket = $3 AND cid >= $4 AND cid <= $5
			 ON CONFLICT (seq, cid) DO NOTHING`,
			int64(from), int64(to), bucket, first.Bytes(), last.Bytes()); err != nil {
			return fmt.Errorf("registry: journal move %d→%d: copy: %w", from, to, err)
		}
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM ingot.catalog_journal
		  WHERE seq = $1 AND bucket = $2 AND cid >= $3 AND cid <= $4`,
		int64(from), bucket, first.Bytes(), last.Bytes()); err != nil {
		return fmt.Errorf("registry: journal move %d→%d: drop source: %w", from, to, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("registry: commit journal move %d→%d: %w", from, to, err)
	}
	return nil
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
