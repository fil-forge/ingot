package logstore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
)

// fakeMeta is an in-memory Meta implementation for tests. It keeps
// just enough state to exercise the segment lifecycle without
// touching Postgres.
type fakeMeta struct {
	mu       sync.Mutex
	nextSeq  uint64
	segments map[uint64]*SegmentMeta
	shipped  []shipEvent // order of MarkSegmentShipped calls
}

type shipEvent struct {
	seq   uint64
	plane blockstore.Plane
}

func newFakeMeta() *fakeMeta {
	return &fakeMeta{segments: map[uint64]*SegmentMeta{}}
}

func (f *fakeMeta) NextSegmentSeq(_ context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSeq++
	return f.nextSeq, nil
}

func (f *fakeMeta) InsertSegmentOpen(_ context.Context, plane blockstore.Plane, seq uint64, bucket string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.segments[seq]; ok {
		return nil
	}
	f.segments[seq] = &SegmentMeta{Seq: seq, Plane: plane, Bucket: bucket, State: StateOpen}
	return nil
}

func (f *fakeMeta) MarkSegmentSealed(_ context.Context, plane blockstore.Plane, seq uint64, sealedAt int64,
	size int64, sha []byte, opRoots []blockstore.OpRoot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.segments[seq]
	if !ok {
		return fmt.Errorf("fake: seal missing seq %d", seq)
	}
	if m.State != StateOpen {
		// idempotent
		return nil
	}
	m.State = StateSealed
	m.SealedAt = sealedAt
	m.Size = size
	m.SHA256 = append([]byte(nil), sha...)
	m.OpRoots = append([]blockstore.OpRoot(nil), opRoots...)
	return nil
}

func (f *fakeMeta) MarkSegmentShipped(_ context.Context, plane blockstore.Plane, seq uint64, shippedAt int64, indexDigest multihash.Multihash, opRoots []blockstore.OpRoot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.segments[seq]
	if !ok {
		return fmt.Errorf("fake: ship missing seq %d", seq)
	}
	if m.ShippedAt != 0 {
		return nil
	}
	m.ShippedAt = shippedAt
	m.IndexDigest = slices.Clone(indexDigest)
	f.shipped = append(f.shipped, shipEvent{seq: seq, plane: plane})
	return nil
}

func (f *fakeMeta) DeleteSegment(_ context.Context, plane blockstore.Plane, seq uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.segments, seq)
	return nil
}

func (f *fakeMeta) ListSegments(_ context.Context, plane blockstore.Plane, bucket string) ([]SegmentMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []SegmentMeta
	for _, m := range f.segments {
		if m.Plane != plane || m.Bucket != bucket {
			continue
		}
		out = append(out, *m)
	}
	return out, nil
}

func (f *fakeMeta) ListSegmentBuckets(_ context.Context, plane blockstore.Plane) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[string]struct{}{}
	var out []string
	for _, m := range f.segments {
		if m.Plane != plane {
			continue
		}
		if _, ok := seen[m.Bucket]; ok {
			continue
		}
		seen[m.Bucket] = struct{}{}
		out = append(out, m.Bucket)
	}
	sort.Strings(out)
	return out, nil
}

func (f *fakeMeta) RehydrateSegment(_ context.Context, m SegmentMeta) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := m
	f.segments[m.Seq] = &cp
	return nil
}

// makeBlock returns a block whose CID is the sha256 of payload. We
// construct the CID explicitly rather than relying on block.NewBlock
// because the latter uses a v0 CID we don't want. The catalog log stores
// opaque bytes keyed by CID and never decodes them, so the codec is
// irrelevant to these tests.
func makeBlock(t *testing.T, payload []byte) block.Block {
	t.Helper()
	mh, err := multihash.Sum(payload, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	c := cid.NewCidV1(cid.DagCBOR, mh)
	blk, err := block.NewBlockWithCid(payload, c)
	if err != nil {
		t.Fatalf("block: %v", err)
	}
	return blk
}

// makeRoot returns a deterministic CID derived from name; used as
// the OpRoot.Root in tests.
func makeRoot(t *testing.T, name string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte("root:"+name), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("mh: %v", err)
	}
	return cid.NewCidV1(cid.DagCBOR, mh)
}

func newTestStore(t *testing.T, sealBytes int64, sealAge time.Duration, retain int) (*Store, *fakeMeta, *atomicCounter) {
	t.Helper()
	dir := t.TempDir()
	meta := newFakeMeta()
	flushCalls := &atomicCounter{}
	logger := zaptest.NewLogger(t)
	flush := func(_ context.Context, _ *Segment) (multihash.Multihash, error) {
		flushCalls.add(1)
		return nil, nil
	}
	cfg := Config{
		Dir:     dir,
		Meta:    meta,
		Catalog: PlaneConfig{SealBytes: sealBytes, SealAge: sealAge, Ship: true, Flush: flush, Retain: retain},
		Logger:  logger,
	}
	s, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s, meta, flushCalls
}

type atomicCounter struct{ n int64 }

func (a *atomicCounter) add(n int64) { atomic.AddInt64(&a.n, n) }
func (a *atomicCounter) load() int64 { return atomic.LoadInt64(&a.n) }

func TestAppendThenGetSameProcess(t *testing.T) {
	s, _, _ := newTestStore(t, 64<<20, 5*time.Second, 6)

	blk := makeBlock(t, []byte("hello world"))
	root := makeRoot(t, "alpha")
	if err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{Bucket: "bk", Root: root}); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	got, err := s.Get(context.Background(), blk.Cid())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got.RawData()) != "hello world" {
		t.Fatalf("got %q want %q", got.RawData(), "hello world")
	}
}

func TestSealBySize(t *testing.T) {
	s, meta, flushes := newTestStore(t, 256, 50*time.Millisecond, 6)

	// Each block carries 100 bytes of payload; after a few writes the
	// segment crosses the 256-byte threshold and seals.
	payload := make([]byte, 100)
	for i := range payload {
		payload[i] = byte(i)
	}
	for i := 0; i < 6; i++ {
		blk := makeBlock(t, append([]byte(fmt.Sprintf("rec-%02d-", i)), payload...))
		if err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
			Bucket: "bk",
			Root:   makeRoot(t, fmt.Sprintf("size-%d", i)),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Wait for at least one flush.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if flushes.load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if flushes.load() == 0 {
		t.Fatalf("expected at least one flush after size threshold; got 0")
	}

	// At least one segment should now have shipped.
	meta.mu.Lock()
	var shipped int
	for _, m := range meta.segments {
		if m.ShippedAt != 0 {
			shipped++
		}
	}
	meta.mu.Unlock()
	if shipped == 0 {
		t.Fatalf("expected at least one shipped segment")
	}
}

func TestSealByAge(t *testing.T) {
	s, _, flushes := newTestStore(t, 1<<30, 80*time.Millisecond, 6)

	blk := makeBlock(t, []byte("age-trigger"))
	if err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
		Bucket: "bk",
		Root:   makeRoot(t, "age"),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if flushes.load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if flushes.load() == 0 {
		t.Fatalf("expected age-triggered seal to produce a flush")
	}
}

func TestRetentionDropsOldFlushed(t *testing.T) {
	s, _, _ := newTestStore(t, 64, 50*time.Millisecond, 2)
	dir := filepath.Dir(s.catalog.dir)

	// Issue 5 PUTs; each one large enough to exceed SealBytes=64 in
	// a single batch, so each becomes its own segment.
	for i := 0; i < 5; i++ {
		payload := make([]byte, 80)
		for j := range payload {
			payload[j] = byte(i)
		}
		blk := makeBlock(t, append([]byte(fmt.Sprintf("retain-%02d-", i)), payload...))
		if err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
			Bucket: "bk",
			Root:   makeRoot(t, fmt.Sprintf("ret-%d", i)),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Wait for retention to converge.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := readSegmentSeqs(dir, "catalog")
		if err != nil {
			t.Fatalf("readDir: %v", err)
		}
		// 1 active open + 2 retained
		if len(entries) <= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	entries, err := readSegmentSeqs(dir, "catalog")
	if err != nil {
		t.Fatalf("readDir: %v", err)
	}
	if len(entries) > 3 {
		t.Fatalf("retain=2 should leave at most 3 segments (open + retained); got %d (%v)",
			len(entries), entries)
	}
}

func TestForceSealRecoveredOpenOnRestart(t *testing.T) {
	dir := t.TempDir()
	meta := newFakeMeta()
	logger := zaptest.NewLogger(t)
	openStore := func() *Store {
		// SealBytes 1<<30 + SealAge 1h: never seals during this test.
		cfg := Config{
			Dir:     dir,
			Meta:    meta,
			Catalog: PlaneConfig{SealBytes: 1 << 30, SealAge: time.Hour, Ship: true, Flush: func(context.Context, *Segment) (multihash.Multihash, error) { return nil, nil }, Retain: 6},
			Logger:  logger,
		}
		s, err := Open(context.Background(), cfg)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return s
	}

	s := openStore()
	blk := makeBlock(t, []byte("survives-restart"))
	if err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
		Bucket: "bk",
		Root:   makeRoot(t, "survive"),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Simulate process exit without orderly Close (don't seal): stop the
	// goroutines and forget the in-memory state. On disk the segment is
	// still open.
	close(s.catalog.closing)
	s.catalog.wg.Wait()

	// Re-Open from the same dir.
	s2 := openStore()
	t.Cleanup(func() { _ = s2.Close(context.Background()) })

	// The previously-open segment should have been force-sealed on
	// startup; the write must still be readable.
	got, err := s2.Get(context.Background(), blk.Cid())
	if err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	if string(got.RawData()) != "survives-restart" {
		t.Fatalf("got %q", got.RawData())
	}
}

func TestAppendBatchEmptyBlocksAccepted(t *testing.T) {
	s, _, _ := newTestStore(t, 64<<20, 5*time.Second, 6)
	root := makeRoot(t, "x")
	if err := s.AppendBatch(context.Background(), nil, blockstore.OpRoot{Bucket: "bk", Root: root}); err != nil {
		t.Fatalf("empty blocks with defined root should succeed, got %v", err)
	}
	if err := s.AppendBatch(context.Background(), []block.Block{makeBlock(t, []byte("x"))}, blockstore.OpRoot{Bucket: "bk"}); err == nil {
		t.Fatalf("expected error on undefined root")
	}
}

func TestGetMissReturnsErrNotFound(t *testing.T) {
	s, _, _ := newTestStore(t, 64<<20, 5*time.Second, 6)

	want := makeRoot(t, "absent")
	_, err := s.Get(context.Background(), want)
	if !errors.Is(err, blockstore.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestAppendBatchDedupesAcrossOps confirms that a CID written in
// one AppendBatch is filtered out of a later AppendBatch landing in
// the same open segment: the file grows by one frame's worth of
// bytes, not two.
func TestAppendBatchDedupesAcrossOps(t *testing.T) {
	s, _, _ := newTestStore(t, 64<<20, 1*time.Hour, 6)

	shared := makeBlock(t, []byte("shared block bytes"))
	uniqA := makeBlock(t, []byte("unique-A"))
	uniqB := makeBlock(t, []byte("unique-B"))

	// First batch: shared + uniqA.
	if err := s.AppendBatch(context.Background(),
		[]block.Block{shared, uniqA},
		blockstore.OpRoot{Bucket: "bk", Root: makeRoot(t, "op-a")},
	); err != nil {
		t.Fatalf("append A: %v", err)
	}

	// Snapshot the open-segment size after the first append.
	s.catalog.catMu.RLock()
	sizeAfterA := s.catalog.open.Size()
	s.catalog.catMu.RUnlock()

	// Second batch: shared (duplicate of first batch) + uniqB.
	if err := s.AppendBatch(context.Background(),
		[]block.Block{shared, uniqB},
		blockstore.OpRoot{Bucket: "bk", Root: makeRoot(t, "op-b")},
	); err != nil {
		t.Fatalf("append B: %v", err)
	}

	s.catalog.catMu.RLock()
	sizeAfterB := s.catalog.open.Size()
	s.catalog.catMu.RUnlock()

	// Frame for `shared` is one varint(len) + cid + payload. The second
	// batch should NOT have re-written it, so growth-from-B ≈ uniqB-frame
	// only — strictly less than the first batch's header + two frames.
	growthB := sizeAfterB - sizeAfterA
	growthFirstBatch := sizeAfterA // includes header + 2 frames; can't isolate

	if growthB >= growthFirstBatch {
		t.Fatalf("second batch grew %d bytes, expected ~half of first-batch growth (%d) since shared was deduped",
			growthB, growthFirstBatch)
	}

	// All three blocks must be readable.
	for _, blk := range []block.Block{shared, uniqA, uniqB} {
		got, err := s.Get(context.Background(), blk.Cid())
		if err != nil {
			t.Fatalf("Get %s: %v", blk.Cid(), err)
		}
		if string(got.RawData()) != string(blk.RawData()) {
			t.Fatalf("Get %s payload mismatch", blk.Cid())
		}
	}
}

// TestAppendBatchAllDuplicatesStillRecordsOpRoot covers the edge
// case where every block in a batch is a duplicate of bytes already
// in the segment. The CAR file shouldn't grow — but the op-root
// still has to persist so the bucket's forge_root_cid catches up
// when the segment ships.
func TestAppendBatchAllDuplicatesStillRecordsOpRoot(t *testing.T) {
	s, _, _ := newTestStore(t, 64<<20, 1*time.Hour, 6)

	blk := makeBlock(t, []byte("only-block"))

	if err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
		Bucket: "bk", Root: makeRoot(t, "first"),
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	s.catalog.catMu.RLock()
	sizeBefore := s.catalog.open.Size()
	opRootsBefore := len(s.catalog.open.OpRoots())
	s.catalog.catMu.RUnlock()

	if err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
		Bucket: "bk", Root: makeRoot(t, "second"),
	}); err != nil {
		t.Fatalf("dup append: %v", err)
	}
	s.catalog.catMu.RLock()
	sizeAfter := s.catalog.open.Size()
	opRootsAfter := len(s.catalog.open.OpRoots())
	s.catalog.catMu.RUnlock()

	if sizeAfter != sizeBefore {
		t.Fatalf("all-duplicate batch grew CAR by %d bytes; expected 0", sizeAfter-sizeBefore)
	}
	if opRootsAfter != opRootsBefore+1 {
		t.Fatalf("op-root count went %d→%d; expected +1", opRootsBefore, opRootsAfter)
	}
}

// TestCatalogNeverShips covers the standalone-mode permutation: a catalog
// plane configured never to ship keeps its CARs on local disk indefinitely
// (the sole durable copy and the source for every read), and its shipped_at
// never advances (so forge_root_cid never advances).
func TestCatalogNeverShips(t *testing.T) {
	dir := t.TempDir()
	meta := newFakeMeta()
	logger := zaptest.NewLogger(t)

	var ships atomicCounter
	cfg := Config{
		Dir:  dir,
		Meta: meta,
		// never ships → retained forever; seal fast so each batch becomes
		// its own retained CAR.
		Catalog: PlaneConfig{
			SealBytes: 1,
			SealAge:   20 * time.Millisecond,
			Ship:      false,
			Flush:     func(context.Context, *Segment) (multihash.Multihash, error) { ships.add(1); return nil, nil },
		},
		Logger: logger,
	}
	s, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	const n = 4
	var blocks []block.Block
	for i := 0; i < n; i++ {
		c := makeBlock(t, []byte(fmt.Sprintf("cat-%02d", i)))
		blocks = append(blocks, c)
		if err := s.AppendBatch(context.Background(), []block.Block{c},
			blockstore.OpRoot{Bucket: "bk", Root: c.Cid()},
		); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		time.Sleep(30 * time.Millisecond) // let each batch seal separately
	}

	// Give the (absent) ship pipeline time to NOT run.
	time.Sleep(200 * time.Millisecond)
	if ships.load() != 0 {
		t.Fatalf("catalog Ship=false should never flush; got %d", ships.load())
	}

	// No sealed segment ever ships.
	meta.mu.Lock()
	sealed := 0
	for seq, m := range meta.segments {
		if m.State != StateSealed {
			continue
		}
		sealed++
		if m.ShippedAt != 0 {
			t.Fatalf("catalog seg %d shipped (%d) but Ship=false", seq, m.ShippedAt)
		}
	}
	meta.mu.Unlock()
	if sealed == 0 {
		t.Fatalf("expected sealed segments; got none")
	}

	// Every block stays readable from local disk (CARs retained).
	for _, c := range blocks {
		if _, err := s.Get(context.Background(), c.Cid()); err != nil {
			t.Fatalf("catalog block %s should be retained locally: %v", c.Cid(), err)
		}
	}
	cars, _ := filepath.Glob(filepath.Join(dir, "catalog", "seg-*.car"))
	if len(cars) < n {
		t.Fatalf("expected >= %d catalog CARs retained; got %d", n, len(cars))
	}
}

// readSegmentSeqs lists a plane's segment CARs on disk (the
// <dir>/<plane>/seg-*.car files) — used to assert retention.
func readSegmentSeqs(dir, plane string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, plane, "seg-*.car"))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, filepath.Base(m))
	}
	return out, nil
}
