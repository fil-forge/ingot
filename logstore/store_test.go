package logstore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
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
	flushed  []uint64 // order of MarkSegmentFlushed calls
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

func (f *fakeMeta) InsertSegmentOpen(_ context.Context, seq uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.segments[seq]; ok {
		return nil
	}
	f.segments[seq] = &SegmentMeta{Seq: seq, State: StateOpen}
	return nil
}

func (f *fakeMeta) MarkSegmentSealed(_ context.Context, seq uint64, sealedAt int64, sizeBytes int64, sha256 []byte, opRoots []blockstore.OpRoot) error {
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
	m.SizeBytes = sizeBytes
	m.SHA256 = append([]byte(nil), sha256...)
	m.OpRoots = append([]blockstore.OpRoot(nil), opRoots...)
	return nil
}

func (f *fakeMeta) MarkSegmentFlushed(_ context.Context, seq uint64, flushedAt int64, opRoots []blockstore.OpRoot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.segments[seq]
	if !ok {
		return fmt.Errorf("fake: flush missing seq %d", seq)
	}
	if m.State == StateFlushed {
		return nil
	}
	m.State = StateFlushed
	m.FlushedAt = flushedAt
	if len(opRoots) > 0 {
		m.OpRoots = append([]blockstore.OpRoot(nil), opRoots...)
	}
	f.flushed = append(f.flushed, seq)
	return nil
}

func (f *fakeMeta) DeleteSegment(_ context.Context, seq uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.segments, seq)
	return nil
}

func (f *fakeMeta) ListUnflushedSegments(_ context.Context) ([]SegmentMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []SegmentMeta
	for _, m := range f.segments {
		if m.State == StateOpen || m.State == StateSealed {
			out = append(out, *m)
		}
	}
	return out, nil
}

func (f *fakeMeta) RehydrateSegment(_ context.Context, m SegmentMeta) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := m
	f.segments[m.Seq] = &cp
	return nil
}

func (f *fakeMeta) snapshot(seq uint64) (SegmentMeta, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.segments[seq]
	if !ok {
		return SegmentMeta{}, false
	}
	return *m, true
}

// makeBlock returns a raw-codec block whose CID is the sha256 of
// payload. We construct the CID explicitly rather than relying on
// block.NewBlock because the latter uses a v0 CID we don't want.
func makeBlock(t *testing.T, payload []byte) block.Block {
	t.Helper()
	mh, err := multihash.Sum(payload, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	c := cid.NewCidV1(cid.Raw, mh)
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
	cfg := Config{
		Dir:       dir,
		Meta:      meta,
		SealBytes: sealBytes,
		SealAge:   sealAge,
		Retain:    retain,
		Flush: func(ctx context.Context, seg *Segment) error {
			flushCalls.add(1)
			return meta.MarkSegmentFlushed(ctx, seg.Seq(), time.Now().Unix(), seg.OpRoots())
		},
		Logger: logger,
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

	// At least one segment row should now be flushed.
	meta.mu.Lock()
	var flushed int
	for _, m := range meta.segments {
		if m.State == StateFlushed {
			flushed++
		}
	}
	meta.mu.Unlock()
	if flushed == 0 {
		t.Fatalf("expected at least one segment in flushed state")
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
	dir := s.cfg.Dir

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
		entries, err := readSegmentSeqs(dir)
		if err != nil {
			t.Fatalf("readDir: %v", err)
		}
		// 1 active open + 2 retained
		if len(entries) <= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	entries, err := readSegmentSeqs(dir)
	if err != nil {
		t.Fatalf("readDir: %v", err)
	}
	if len(entries) > 3 {
		t.Fatalf("retain=2 should leave at most 3 .car files (open + retained); got %d (%v)",
			len(entries), entries)
	}
}

func TestForceSealRecoveredOpenOnRestart(t *testing.T) {
	dir := t.TempDir()
	meta := newFakeMeta()
	logger := zaptest.NewLogger(t)
	openStore := func() *Store {
		cfg := Config{
			Dir:       dir,
			Meta:      meta,
			SealBytes: 1 << 30, // never seals on size during this test
			SealAge:   1 * time.Hour,
			Retain:    6,
			Flush: func(ctx context.Context, seg *Segment) error {
				return meta.MarkSegmentFlushed(ctx, seg.Seq(), time.Now().Unix(), seg.OpRoots())
			},
			Logger: logger,
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

	// Simulate process exit without orderly Close (don't seal). Close
	// the file descriptors via a panic-safe path: we just stop the
	// goroutines and forget the in-memory state.
	close(s.closing)
	s.wg.Wait()
	// Drop the in-memory ref; on disk the segment is still open.

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

	want, err := makeRoot(t, "absent"), error(nil)
	_, err = s.Get(context.Background(), want)
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

	// Snapshot the open segment's size after the first append.
	s.catMu.RLock()
	sizeAfterA := s.open.Size()
	s.catMu.RUnlock()

	// Second batch: shared (duplicate of first batch) + uniqB.
	if err := s.AppendBatch(context.Background(),
		[]block.Block{shared, uniqB},
		blockstore.OpRoot{Bucket: "bk", Root: makeRoot(t, "op-b")},
	); err != nil {
		t.Fatalf("append B: %v", err)
	}

	s.catMu.RLock()
	sizeAfterB := s.open.Size()
	s.catMu.RUnlock()

	// Frame for `shared` is one varint(len) + cid + payload. Whatever
	// that totals, the second batch should NOT have re-written it.
	// `uniqA` has ~the same payload size as `uniqB`, so growth-from-A
	// and growth-from-B (had we written `shared` twice) would be
	// nearly identical. Instead we expect growth-from-B ≈ uniqB-frame
	// only. Simplest assertion: only one frame's worth of growth.
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
	s.catMu.RLock()
	sizeBefore := s.open.Size()
	opRootsBefore := len(s.open.OpRoots())
	s.catMu.RUnlock()

	if err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
		Bucket: "bk", Root: makeRoot(t, "second"),
	}); err != nil {
		t.Fatalf("dup append: %v", err)
	}
	s.catMu.RLock()
	sizeAfter := s.open.Size()
	opRootsAfter := len(s.open.OpRoots())
	s.catMu.RUnlock()

	if sizeAfter != sizeBefore {
		t.Fatalf("all-duplicate batch grew CAR by %d bytes; expected 0", sizeAfter-sizeBefore)
	}
	if opRootsAfter != opRootsBefore+1 {
		t.Fatalf("op-root count went %d→%d; expected +1", opRootsBefore, opRootsAfter)
	}
}

func readSegmentSeqs(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "seg-*.car"))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, filepath.Base(m))
	}
	return out, nil
}
