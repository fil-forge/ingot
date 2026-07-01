// Package inmem provides in-memory implementations of ingot's
// collaborator interfaces (registry.Registry + logstore.Meta as one
// MemStore, plus a no-op block reader and a no-op uploader). They back
// both the test harness and the daemon's standalone (no-Forge) mode, so
// the two paths construct the server through identical wiring.
//
// MemStore keeps bucket + segment metadata in memory only — it resets on
// restart. Segment CARs still persist on local disk via logstore; in
// standalone mode both planes are configured never to ship, so those
// CARs are retained and serve all reads.
package inmem

import (
	"context"
	"io"
	"sort"
	"sync"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// MemStore is an in-memory registry.Registry + logstore.Meta. The two
// interfaces overlap on bucket state because shipping the catalog plane
// advances forge_root_cid; production wires a single *registry.Postgres
// for both seams, and this fake follows suit.
type MemStore struct {
	mu       sync.Mutex
	buckets  map[string]*registry.State
	segments map[uint64]*logstore.SegmentMeta
	nextSeq  uint64

	// The architecture's relational surface (docs/architecture.md §5–§7),
	// mirroring the Postgres tables so the in-process suite exercises the
	// same code paths. See stores.go for the methods over these.
	blobRefs  map[claimKey]registry.BlobClaim
	intents   map[string]registry.UploadIntent          // keyed by string(digest)
	locations map[locKey]registry.BlobLocation          // keyed by (space, digest)
	sessions  map[string]registry.MultipartSession      // keyed by uploadID
	parts     map[string]map[int]registry.MultipartPart // uploadID -> partNumber -> part
	gcCands   map[string]struct{}                       // keyed by string(cid)
}

// claimKey / locKey are the composite map keys for the blob_refs and
// blob_locations tables (digest bytes carried as a string for comparability).
type claimKey struct {
	digest, bucket, objectKey, versionID string
}

type locKey struct {
	space, digest string
}

// NewMemStore returns an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{
		buckets:   map[string]*registry.State{},
		segments:  map[uint64]*logstore.SegmentMeta{},
		blobRefs:  map[claimKey]registry.BlobClaim{},
		intents:   map[string]registry.UploadIntent{},
		locations: map[locKey]registry.BlobLocation{},
		sessions:  map[string]registry.MultipartSession{},
		parts:     map[string]map[int]registry.MultipartPart{},
		gcCands:   map[string]struct{}{},
	}
}

// Registry methods ===========================================================

func (m *MemStore) Create(_ context.Context, name string, createdAt int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[name]; ok {
		return registry.ErrExists
	}
	m.buckets[name] = &registry.State{Name: name, CreatedAt: createdAt}
	return nil
}

func (m *MemStore) Get(_ context.Context, name string) (*registry.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.buckets[name]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := *s
	return &cp, nil
}

func (m *MemStore) List(_ context.Context) ([]*registry.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*registry.State, 0, len(m.buckets))
	for _, s := range m.buckets {
		cp := *s
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *MemStore) Delete(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[name]; !ok {
		return registry.ErrNotFound
	}
	delete(m.buckets, name)
	return nil
}

func (m *MemStore) CASRoot(_ context.Context, name string, expect, next cid.Cid) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.buckets[name]
	if !ok {
		return registry.ErrNotFound
	}
	if !s.Root.Equals(expect) {
		return registry.ErrConflict
	}
	s.Root = next
	return nil
}

func (m *MemStore) SetForgeRoot(_ context.Context, name string, root cid.Cid) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.buckets[name]
	if !ok {
		return registry.ErrNotFound
	}
	s.ForgeRoot = root
	return nil
}

// Meta methods ===============================================================
//
// Segments are single-plane and keyed by their globally-unique seq; the
// Plane field discriminates so ListSegments can scope by plane.

func (m *MemStore) NextSegmentSeq(_ context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextSeq++
	return m.nextSeq, nil
}

func (m *MemStore) InsertSegmentOpen(_ context.Context, plane blockstore.Plane, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.segments[seq]; ok {
		return nil
	}
	m.segments[seq] = &logstore.SegmentMeta{Seq: seq, Plane: plane, State: logstore.StateOpen}
	return nil
}

func (m *MemStore) MarkSegmentSealed(_ context.Context, plane blockstore.Plane, seq uint64, sealedAt int64,
	size int64, sha []byte, opRoots []blockstore.OpRoot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.segments[seq]
	if !ok || r.State != logstore.StateOpen {
		return nil
	}
	r.State = logstore.StateSealed
	r.SealedAt = sealedAt
	r.Size = size
	r.SHA256 = append([]byte(nil), sha...)
	r.OpRoots = append([]blockstore.OpRoot(nil), opRoots...)
	return nil
}

func (m *MemStore) MarkSegmentShipped(_ context.Context, plane blockstore.Plane, seq uint64, shippedAt int64, opRoots []blockstore.OpRoot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.segments[seq]; ok {
		r.ShippedAt = shippedAt
	}
	if plane == blockstore.PlaneCatalog {
		for _, opr := range opRoots {
			// Guard the advance on the bucket's committed root (see the Postgres
			// MarkSegmentShipped): a durable op-root the bucket never adopted
			// (CASRoot lost the race) must not advance forge_root.
			if b, ok := m.buckets[opr.Bucket]; ok && b.Root.Equals(opr.Root) {
				b.ForgeRoot = opr.Root
			}
		}
	}
	return nil
}

func (m *MemStore) DeleteSegment(_ context.Context, plane blockstore.Plane, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.segments, seq)
	return nil
}

func (m *MemStore) ListSegments(_ context.Context, plane blockstore.Plane) ([]logstore.SegmentMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []logstore.SegmentMeta
	for _, r := range m.segments {
		if r.Plane != plane {
			continue
		}
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

func (m *MemStore) RehydrateSegment(_ context.Context, sm logstore.SegmentMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := sm
	m.segments[sm.Seq] = &cp
	return nil
}

// NopBaseReader is the base tier of the layered read path when there is
// no network: every miss past the local log returns ErrNotFound.
type NopBaseReader struct{}

func (NopBaseReader) GetBlock(_ context.Context, _ cid.Cid) (block.Block, error) {
	return nil, blockstore.ErrNotFound
}

// OpenBlob is the streaming-read counterpart: with no network tier, an evicted
// body blob is unrecoverable. (The harness never evicts, so reads come from the
// spool.)
func (NopBaseReader) OpenBlob(_ context.Context, _ multihash.Multihash) (io.ReadCloser, error) {
	return nil, blockstore.ErrNotFound
}

// NopUploader is a no-op upload sink for the in-memory suite and standalone
// mode. SubmitShard ships nothing (a catalog plane is marked shipped without
// touching the network); UploadBlob accepts a body blob without touching the
// network, so the spool's local copy serves all reads.
type NopUploader struct{}

func (NopUploader) SubmitShard(_ context.Context, _ blockstore.Plane, _ uploader.CARShard) error {
	return nil
}

func (NopUploader) UploadBlob(_ context.Context, _ multihash.Multihash, size int64, _ string) (uploader.BlobLocation, error) {
	return uploader.BlobLocation{Size: size}, nil
}

func (NopUploader) RemoveBlob(_ context.Context, _ multihash.Multihash) error { return nil }

// Compile-time guarantees.
var (
	_ registry.Registry      = (*MemStore)(nil)
	_ logstore.Meta          = (*MemStore)(nil)
	_ blockstore.BlockReader = NopBaseReader{}
	_ blockstore.BlobReader  = NopBaseReader{}
	_ uploader.Uploader      = NopUploader{}
	_ uploader.BodyUploader  = NopUploader{}
	_ uploader.BlobRemover   = NopUploader{}
)
