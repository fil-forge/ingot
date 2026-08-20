package logstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
)

// Manager is the per-bucket catalog log: one [Store] per bucket, each rooted
// under its own directory, so a sealed segment holds exactly one bucket's
// MST/manifest blocks and ships to that bucket's Forge space.
//
// Manager implements [blockstore.Log], so the write path (bucketop's
// OpStaging) and the layered read tier hold it exactly as they held the
// single global Store:
//
//   - AppendBatch routes to the bucket named by the op-root, lazily opening
//     that bucket's Store on first write.
//   - Get consults every open bucket store (their in-memory segment indexes)
//     and returns the first hit — reads carry no bucket context today, so
//     the lookup is linear in open buckets. Threading the bucket into the
//     read path is the follow-up that removes this scan.
//   - Close shuts every bucket store down.
type Manager struct {
	dir      string
	meta     Meta
	catalog  PlaneConfig
	flushFor func(bucket string) FlushFunc
	logger   *zap.Logger

	mu     sync.Mutex
	stores map[string]*Store
	closed bool
}

// Compile-time assertion: the Manager stands where the single Store stood.
var _ blockstore.Log = (*Manager)(nil)

// ManagerConfig wires a Manager. Defaults follow Config's.
type ManagerConfig struct {
	// Dir is the on-disk root; bucket b's segments live under <Dir>/<b>/.
	// Created if missing.
	Dir string

	// Meta is the persistence backing for segment metadata, shared by every
	// bucket store (rows are stamped with their bucket). Required.
	Meta Meta

	// Catalog is the pipeline template applied to every bucket's store.
	// Its Flush field is ignored — the per-bucket flush comes from FlushFor.
	Catalog PlaneConfig

	// FlushFor returns the ship closure for one bucket's sealed segments
	// (resolving the bucket's Forge space at ship time). Required when
	// Catalog.Ship is true; ignored otherwise.
	FlushFor func(bucket string) FlushFunc

	// Logger is optional; defaults to zap.NewNop().
	Logger *zap.Logger
}

// OpenManager initializes the per-bucket log root and re-opens every bucket
// that left segments behind — the union of on-disk bucket directories and
// buckets with segment rows — so unshipped sealed segments re-enqueue at
// startup instead of waiting for the bucket's next write.
func OpenManager(ctx context.Context, cfg ManagerConfig) (*Manager, error) {
	if cfg.Dir == "" {
		return nil, errors.New("logstore: manager: Dir is required")
	}
	if cfg.Meta == nil {
		return nil, errors.New("logstore: manager: Meta is required")
	}
	if cfg.Catalog.Ship && cfg.FlushFor == nil {
		return nil, errors.New("logstore: manager: FlushFor is required when Catalog.Ship is true")
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("logstore: manager: mkdir %s: %w", cfg.Dir, err)
	}

	m := &Manager{
		dir:      cfg.Dir,
		meta:     cfg.Meta,
		catalog:  cfg.Catalog,
		flushFor: cfg.FlushFor,
		logger:   cfg.Logger,
		stores:   map[string]*Store{},
	}

	buckets := map[string]struct{}{}
	entries, err := os.ReadDir(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("logstore: manager: scan %s: %w", cfg.Dir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			buckets[e.Name()] = struct{}{}
		}
	}
	fromMeta, err := cfg.Meta.ListSegmentBuckets(ctx, blockstore.PlaneCatalog)
	if err != nil {
		return nil, fmt.Errorf("logstore: manager: list segment buckets: %w", err)
	}
	for _, b := range fromMeta {
		buckets[b] = struct{}{}
	}

	for b := range buckets {
		if _, err := m.logFor(ctx, b); err != nil {
			_ = m.Close(ctx)
			return nil, fmt.Errorf("logstore: manager: recover bucket %q: %w", b, err)
		}
	}
	return m, nil
}

// logFor returns bucket's log, opening it (with recovery) on first use.
func (m *Manager) logFor(ctx context.Context, bucket string) (blockstore.Log, error) {
	if err := validBucketDir(bucket); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, errors.New("logstore: manager: closed")
	}
	if s, ok := m.stores[bucket]; ok {
		return s, nil
	}

	pc := m.catalog
	pc.Flush = nil
	if pc.Ship {
		pc.Flush = m.flushFor(bucket)
	}
	s, err := Open(ctx, Config{
		Dir:     filepath.Join(m.dir, bucket),
		Bucket:  bucket,
		Meta:    m.meta,
		Catalog: pc,
		Logger:  m.logger.With(zap.String("bucket", bucket)),
	})
	if err != nil {
		return nil, fmt.Errorf("logstore: manager: open log for %q: %w", bucket, err)
	}
	m.stores[bucket] = s
	return s, nil
}

// AppendBatch routes the batch to the log of the bucket named by the
// op-root (every batch belongs to exactly one bucket — OpStaging tags it).
func (m *Manager) AppendBatch(ctx context.Context, catalogBlocks []block.Block, opRoot blockstore.OpRoot) error {
	if opRoot.Bucket == "" {
		return errors.New("logstore: manager: AppendBatch: opRoot.Bucket must be set")
	}
	log, err := m.logFor(ctx, opRoot.Bucket)
	if err != nil {
		return err
	}
	return log.AppendBatch(ctx, catalogBlocks, opRoot)
}

// Get returns the catalog block holding c from whichever open bucket store
// has it, or blockstore.ErrNotFound. Reads carry no bucket context, so this
// is a linear scan over open stores' in-memory indexes.
func (m *Manager) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	m.mu.Lock()
	stores := make([]*Store, 0, len(m.stores))
	for _, s := range m.stores {
		stores = append(stores, s)
	}
	m.mu.Unlock()

	for _, s := range stores {
		b, err := s.Get(ctx, c)
		if err == nil {
			return b, nil
		}
		if !errors.Is(err, blockstore.ErrNotFound) {
			return nil, err
		}
	}
	return nil, blockstore.ErrNotFound
}

// Close shuts down every bucket store (sealing open segments, draining
// flush queues). The manager is unusable afterwards.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true

	var errs []error
	for bucket, s := range m.stores {
		if err := s.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("close log for %q: %w", bucket, err))
		}
	}
	m.stores = nil
	if len(errs) > 0 {
		return fmt.Errorf("logstore: manager: %v", errs)
	}
	return nil
}

// QuiesceBucketLog stops bucket's flush pipeline and waits for any in-flight
// ship to finish, so the segment rows ShippedSegmentDigests reads afterwards
// are final: a ship that was mid-flight has either completed (registered its
// blobs AND stamped shipped_at + index_digest) or aborted. Without this,
// DeleteBucket races the flush goroutine — a segment whose CAR just
// registered but wasn't yet marked shipped would be invisible to the release
// pass, and the space delete would be refused. The closed store reopens
// lazily on the bucket's next use, so a delete that fails downstream leaves
// the bucket functional.
func (m *Manager) QuiesceBucketLog(ctx context.Context, bucket string) error {
	if err := validBucketDir(bucket); err != nil {
		return err
	}
	m.mu.Lock()
	s, ok := m.stores[bucket]
	delete(m.stores, bucket)
	m.mu.Unlock()
	if !ok {
		return nil
	}
	if err := s.Close(ctx); err != nil {
		return fmt.Errorf("logstore: manager: quiesce log for %q: %w", bucket, err)
	}
	return nil
}

// ShippedSegmentDigests returns the multihash of every blob the bucket's
// catalog segments may have registered in its space: each shipped segment's
// CAR and its sharded-dag-index blob, plus the CAR of any sealed-but-
// unshipped segment (a flush aborted between the CAR's blob/add and the
// shipped stamp leaves that registration behind; releasing an unregistered
// blob is a no-op, so over-listing is safe). DeleteBucket must release them
// all before the space itself can be deleted — the tenant service refuses to
// delete a space that still holds registrations. Call QuiesceBucketLog first
// so no ship is in flight while this reads.
func (m *Manager) ShippedSegmentDigests(ctx context.Context, bucket string) ([][]byte, error) {
	rows, err := m.meta.ListSegments(ctx, blockstore.PlaneCatalog, bucket)
	if err != nil {
		return nil, fmt.Errorf("logstore: manager: list segments for %q: %w", bucket, err)
	}
	var out [][]byte
	for _, r := range rows {
		if r.State != StateSealed || len(r.SHA256) == 0 {
			continue
		}
		carDigest, err := multihash.Encode(r.SHA256, multihash.SHA2_256)
		if err != nil {
			return nil, fmt.Errorf("logstore: manager: encode segment %d sha: %w", r.Seq, err)
		}
		out = append(out, carDigest)
		if r.ShippedAt != 0 && len(r.IndexDigest) > 0 {
			out = append(out, r.IndexDigest)
		}
	}
	return out, nil
}

// RemoveBucketLog deletes bucket's log entirely: closes its store (dropping
// queued-but-unshipped segments — a deleted bucket's history has nowhere to
// ship), unlinks its directory, and removes its segment rows. Used by
// DeleteBucket after the registry row is gone.
func (m *Manager) RemoveBucketLog(ctx context.Context, bucket string) error {
	if err := validBucketDir(bucket); err != nil {
		return err
	}

	m.mu.Lock()
	s, ok := m.stores[bucket]
	delete(m.stores, bucket)
	m.mu.Unlock()

	if ok {
		if err := s.Close(ctx); err != nil {
			return fmt.Errorf("logstore: manager: close log for %q: %w", bucket, err)
		}
	}
	if err := os.RemoveAll(filepath.Join(m.dir, bucket)); err != nil {
		return fmt.Errorf("logstore: manager: remove log dir for %q: %w", bucket, err)
	}
	rows, err := m.meta.ListSegments(ctx, blockstore.PlaneCatalog, bucket)
	if err != nil {
		return fmt.Errorf("logstore: manager: list segments for %q: %w", bucket, err)
	}
	for _, r := range rows {
		if err := m.meta.DeleteSegment(ctx, r.Plane, r.Seq); err != nil {
			return fmt.Errorf("logstore: manager: delete segment %d for %q: %w", r.Seq, bucket, err)
		}
	}
	return nil
}

// validBucketDir guards the bucket-name-as-directory mapping. S3 bucket
// names (validated at the protocol layer) never trip this; it exists so a
// crafted name can never escape the manager's root.
func validBucketDir(bucket string) error {
	if bucket == "" || bucket == "." || bucket == ".." ||
		strings.ContainsAny(bucket, `/\`) {
		return fmt.Errorf("logstore: manager: invalid bucket name %q", bucket)
	}
	return nil
}
