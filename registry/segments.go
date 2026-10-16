package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/fil-forge/ucantone/did"
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

func (r *Postgres) MarkSegmentShipped(ctx context.Context, plane blockstore.Plane, seq uint64, shippedAt int64, indexDigest multihash.Multihash, deadBytes int64, opRoots []blockstore.OpRoot) error {
	// Only the catalog plane ships: stamp shipped_at AND advance forge_root_cid
	// for every op-root in this segment, atomically.
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin ship catalog %d: %w", seq, err)
	}
	defer tx.Rollback(ctx)

	var dead *int64
	if deadBytes >= 0 {
		dead = &deadBytes
	}
	tag, err := tx.Exec(ctx,
		`UPDATE ingot.segments SET shipped_at = $2, index_digest = $3, dead_bytes = $4
		 WHERE seq = $1 AND plane = 'catalog' AND shipped_at IS NULL`,
		int64(seq), shippedAt, indexDigest, dead)
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
	rows, err := r.pool.Query(ctx,
		`SELECT seq, state, COALESCE(sealed_at, 0), size_bytes, sha256, COALESCE(shipped_at, 0), index_digest,
		        COALESCE(retired_at, 0), COALESCE(dead_bytes, -1)
		   FROM ingot.segments
		  WHERE plane = $1 AND bucket = $2 AND (retired_at IS NOT NULL) = $3
		  ORDER BY seq ASC`,
		plane.String(), bucket, retired)
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
			deadBytes   int64
		)
		if err := rows.Scan(&seqInt, &stateS, &sealed, &size, &sha, &shippedAt, &indexDigest, &retiredAt, &deadBytes); err != nil {
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
			DeadBytes:   deadBytes,
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

// CatalogUsage sums each bucket's shipped catalog segments: those whose
// ship registered blobs in its space (index_digest is set exactly then). Each
// entry carries the bucket's current root and its last completed walk.
func (r *Postgres) CatalogUsage(ctx context.Context) ([]CatalogUsage, error) {
	return r.catalogUsage(ctx, "")
}

// BucketCatalogUsage is CatalogUsage for one bucket: nil when it has no
// shipped catalog segments.
func (r *Postgres) BucketCatalogUsage(ctx context.Context, bucket string) (*CatalogUsage, error) {
	usage, err := r.catalogUsage(ctx, "AND b.name = $1", bucket)
	if err != nil || len(usage) == 0 {
		return nil, err
	}
	return &usage[0], nil
}

// catalogUsage runs the catalog usage query, its WHERE clause extended by
// and (over the args).
func (r *Postgres) catalogUsage(ctx context.Context, and string, args ...any) ([]CatalogUsage, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT b.name, b.space, b.root_cid,
		        SUM(s.size_bytes),
		        COALESCE(SUM(s.dead_bytes), 0),
		        COALESCE(SUM(s.size_bytes) FILTER (WHERE s.dead_bytes IS NULL), 0),
		        w.bucket IS NOT NULL, w.root_cid, w.started_at, w.finished_at, w.live_bytes,
		        w.forge_bytes, w.dead_on_arrival_bytes, w.unmeasured_bytes
		   FROM ingot.segments s
		   JOIN ingot.buckets b ON b.name = s.bucket
		   LEFT JOIN ingot.catalog_walks w ON w.bucket = b.name
		  WHERE s.plane = 'catalog' AND s.index_digest IS NOT NULL `+and+`
		  GROUP BY b.name, w.bucket
		  ORDER BY b.name`, args...)
	if err != nil {
		return nil, fmt.Errorf("registry: catalog usage: %w", err)
	}
	defer rows.Close()

	var out []CatalogUsage
	for rows.Next() {
		var (
			u                    CatalogUsage
			spaceStr             string
			root, walkRoot       []byte
			walked               bool
			started, finished    *int64
			live, forge, doa, um *int64
		)
		if err := rows.Scan(&u.Bucket, &spaceStr, &root, &u.Bytes, &u.DeadOnArrivalBytes, &u.UnmeasuredBytes,
			&walked, &walkRoot, &started, &finished, &live, &forge, &doa, &um); err != nil {
			return nil, fmt.Errorf("registry: scan catalog usage: %w", err)
		}
		if u.Space, err = did.Parse(spaceStr); err != nil {
			return nil, fmt.Errorf("registry: parse space %q of bucket %q: %w", spaceStr, u.Bucket, err)
		}
		if u.Root, err = castOptionalCid(root); err != nil {
			return nil, fmt.Errorf("registry: root of bucket %q: %w", u.Bucket, err)
		}
		if walked {
			w := CatalogWalk{StartedAt: *started, FinishedAt: *finished, LiveBytes: *live,
				ForgeBytes: *forge, DeadOnArrivalBytes: *doa, UnmeasuredBytes: *um}
			if w.Root, err = castOptionalCid(walkRoot); err != nil {
				return nil, fmt.Errorf("registry: walked root of bucket %q: %w", u.Bucket, err)
			}
			u.Walk = &w
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: catalog usage rows: %w", err)
	}
	return out, nil
}

// castOptionalCid decodes a nullable CID column: NULL is cid.Undef.
func castOptionalCid(b []byte) (cid.Cid, error) {
	if len(b) == 0 {
		return cid.Undef, nil
	}
	return cid.Cast(b)
}

// optionalCidBytes encodes a CID for a nullable column: cid.Undef is NULL.
func optionalCidBytes(c cid.Cid) []byte {
	if !c.Defined() {
		return nil
	}
	return c.Bytes()
}

// CatalogSubtrees returns the subtrees remembered for bucket.
func (r *Postgres) CatalogSubtrees(ctx context.Context, bucket string) ([]CatalogSubtree, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT cid, bytes, blocks, children FROM ingot.catalog_walk_subtrees WHERE bucket = $1`, bucket)
	if err != nil {
		return nil, fmt.Errorf("registry: catalog subtrees of %q: %w", bucket, err)
	}
	defer rows.Close()
	var out []CatalogSubtree
	for rows.Next() {
		var (
			st          CatalogSubtree
			c, children []byte
		)
		if err := rows.Scan(&c, &st.Bytes, &st.Blocks, &children); err != nil {
			return nil, fmt.Errorf("registry: scan catalog subtree: %w", err)
		}
		if st.CID, err = cid.Cast(c); err != nil {
			return nil, fmt.Errorf("registry: catalog subtree cid of %q: %w", bucket, err)
		}
		if st.Children, err = splitCids(children); err != nil {
			return nil, fmt.Errorf("registry: catalog subtree children of %q: %w", bucket, err)
		}
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: catalog subtrees rows: %w", err)
	}
	return out, nil
}

// PutCatalogSubtrees inserts subtrees for bucket in one statement. A CID's
// figures never change, so a row already there is left as it is.
func (r *Postgres) PutCatalogSubtrees(ctx context.Context, bucket string, subtrees []CatalogSubtree) error {
	if len(subtrees) == 0 {
		return nil
	}
	cids := make([][]byte, len(subtrees))
	bytes := make([]int64, len(subtrees))
	blocks := make([]int64, len(subtrees))
	children := make([][]byte, len(subtrees))
	for i, st := range subtrees {
		cids[i], bytes[i], blocks[i], children[i] = st.CID.Bytes(), st.Bytes, st.Blocks, joinCids(st.Children)
	}
	if _, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.catalog_walk_subtrees (bucket, cid, bytes, blocks, children)
		 SELECT $1, unnest($2::bytea[]), unnest($3::bigint[]), unnest($4::bigint[]), unnest($5::bytea[])
		 ON CONFLICT (bucket, cid) DO NOTHING`,
		bucket, cids, bytes, blocks, children); err != nil {
		return fmt.Errorf("registry: put catalog subtrees of %q: %w", bucket, err)
	}
	return nil
}

// joinCids concatenates CIDs for a bytea column; splitCids reads them back,
// a CID's encoding being self-delimiting.
func joinCids(cids []cid.Cid) []byte {
	out := []byte{}
	for _, c := range cids {
		out = append(out, c.Bytes()...)
	}
	return out
}

func splitCids(b []byte) ([]cid.Cid, error) {
	var out []cid.Cid
	for len(b) > 0 {
		n, c, err := cid.CidFromBytes(b)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
		b = b[n:]
	}
	return out, nil
}

// CompleteCatalogWalk records the walk and drops the listed subtrees in one
// transaction.
func (r *Postgres) CompleteCatalogWalk(ctx context.Context, bucket string, w CatalogWalk, drop []cid.Cid) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin catalog walk of %q: %w", bucket, err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`INSERT INTO ingot.catalog_walks
		   (bucket, root_cid, started_at, finished_at, live_bytes, forge_bytes, dead_on_arrival_bytes, unmeasured_bytes)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (bucket) DO UPDATE SET
		   root_cid = EXCLUDED.root_cid, started_at = EXCLUDED.started_at, finished_at = EXCLUDED.finished_at,
		   live_bytes = EXCLUDED.live_bytes, forge_bytes = EXCLUDED.forge_bytes,
		   dead_on_arrival_bytes = EXCLUDED.dead_on_arrival_bytes, unmeasured_bytes = EXCLUDED.unmeasured_bytes`,
		bucket, optionalCidBytes(w.Root), w.StartedAt, w.FinishedAt, w.LiveBytes,
		w.ForgeBytes, w.DeadOnArrivalBytes, w.UnmeasuredBytes); err != nil {
		return fmt.Errorf("registry: record catalog walk of %q: %w", bucket, err)
	}
	if len(drop) > 0 {
		dropBytes := make([][]byte, len(drop))
		for i, c := range drop {
			dropBytes[i] = c.Bytes()
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM ingot.catalog_walk_subtrees WHERE bucket = $1 AND cid = ANY($2::bytea[])`,
			bucket, dropBytes); err != nil {
			return fmt.Errorf("registry: drop catalog subtrees of %q: %w", bucket, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("registry: commit catalog walk of %q: %w", bucket, err)
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
