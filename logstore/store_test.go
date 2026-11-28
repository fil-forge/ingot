package logstore

import (
	"context"
	"errors"
	"fmt"
	"os"
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
	"github.com/stretchr/testify/require"
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
	// failShips is how many of the next MarkSegmentShipped calls fail, and
	// onShipFail, if set, is called with each failed call's seq.
	failShips  int
	onShipFail func(seq uint64)
	// failSeals and onSealFail do the same for MarkSegmentSealed.
	failSeals  int
	onSealFail func(seq uint64)
	// sealGate, if set, holds every MarkSegmentSealed call until it is
	// closed, after calling onSealWait with the call's seq: a stamp that
	// hangs, as on a connection lost in a failover.
	sealGate   chan struct{}
	onSealWait func(seq uint64)
}

type shipEvent struct {
	seq   uint64
	plane blockstore.Plane
	// rowSealed is whether the row was stamped sealed, with a sha256, when
	// the ship stamped it.
	rowSealed bool
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

func (f *fakeMeta) EnsureSegmentSeqAbove(_ context.Context, seq uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSeq = max(f.nextSeq, seq)
	return nil
}

// The fake keeps no journal; rebuild_test.go drives the journal through
// the Postgres registry.
func (f *fakeMeta) JournalBuckets(context.Context) ([]string, error)          { return nil, nil }
func (f *fakeMeta) JournalSegments(context.Context, string) ([]uint64, error) { return nil, nil }
func (f *fakeMeta) ReadJournal(context.Context, uint64, cid.Cid, int) ([]block.Block, error) {
	return nil, nil
}
func (f *fakeMeta) MoveJournal(context.Context, string, uint64, uint64, cid.Cid, cid.Cid) error {
	return nil
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
	gate, waiting := f.sealGate, f.onSealWait
	f.mu.Unlock()
	if gate != nil {
		if waiting != nil {
			waiting(seq)
		}
		<-gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failSeals > 0 {
		f.failSeals--
		if f.onSealFail != nil {
			f.onSealFail(seq)
		}
		return fmt.Errorf("fake: seal of seq %d failed", seq)
	}
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

func (f *fakeMeta) MarkSegmentShipped(_ context.Context, plane blockstore.Plane, seq uint64, shippedAt int64, indexDigest multihash.Multihash, deadBytes int64, opRoots []blockstore.OpRoot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failShips > 0 {
		f.failShips--
		if f.onShipFail != nil {
			f.onShipFail(seq)
		}
		return fmt.Errorf("fake: ship of seq %d failed", seq)
	}
	m, ok := f.segments[seq]
	if !ok {
		return fmt.Errorf("fake: ship missing seq %d", seq)
	}
	if m.ShippedAt != 0 {
		return nil
	}
	m.ShippedAt = shippedAt
	m.IndexDigest = slices.Clone(indexDigest)
	m.DeadBytes = deadBytes
	f.shipped = append(f.shipped, shipEvent{seq: seq, plane: plane, rowSealed: m.State == StateSealed && len(m.SHA256) > 0})
	return nil
}

func (f *fakeMeta) DeleteSegment(_ context.Context, plane blockstore.Plane, seq uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.segments, seq)
	return nil
}

func (f *fakeMeta) RetireSegment(_ context.Context, plane blockstore.Plane, seq uint64, retiredAt int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m, ok := f.segments[seq]; ok && m.RetiredAt == 0 {
		m.RetiredAt = retiredAt
		m.OpRoots = nil
	}
	return nil
}

func (f *fakeMeta) ListSegments(_ context.Context, plane blockstore.Plane, bucket string) ([]SegmentMeta, error) {
	return f.listSegments(plane, bucket, false), nil
}

func (f *fakeMeta) ListRetiredSegments(_ context.Context, plane blockstore.Plane, bucket string) ([]SegmentMeta, error) {
	return f.listSegments(plane, bucket, true), nil
}

func (f *fakeMeta) listSegments(plane blockstore.Plane, bucket string, retired bool) []SegmentMeta {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []SegmentMeta
	for _, m := range f.segments {
		if m.Plane != plane || m.Bucket != bucket || (m.RetiredAt != 0) != retired {
			continue
		}
		out = append(out, *m)
	}
	return out
}

func (f *fakeMeta) ListSegmentBuckets(_ context.Context, plane blockstore.Plane) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[string]struct{}{}
	var out []string
	for _, m := range f.segments {
		if m.Plane != plane || m.RetiredAt != 0 {
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
	require.NoError(t, err, "multihash")
	c := cid.NewCidV1(cid.DagCBOR, mh)
	blk, err := block.NewBlockWithCid(payload, c)
	require.NoError(t, err, "block")
	return blk
}

// makeRoot returns a deterministic CID derived from name; used as
// the OpRoot.Root in tests.
func makeRoot(t *testing.T, name string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte("root:"+name), multihash.SHA2_256, -1)
	require.NoError(t, err, "mh")
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
	sh := &shipper{}
	cfg := Config{
		Dir:     dir,
		Meta:    meta,
		Catalog: PlaneConfig{SealBytes: sealBytes, SealAge: sealAge, Ship: true, Flush: flush, Retain: retain, OnSealed: sh.sealed},
		Logger:  logger,
	}
	s, err := Open(context.Background(), cfg)
	require.NoError(t, err, "Open")
	sh.start(t, s)
	return s, meta, flushCalls
}

// shipper stands in for the ship queue: each seal starts a Ship, retried
// until it succeeds or the test ends.
type shipper struct {
	mu   sync.Mutex
	s    *Store
	ctx  context.Context
	wg   sync.WaitGroup
	todo bool
	// stopped is set when the test ends: a seal after it starts no ship.
	stopped bool
}

func (sh *shipper) sealed() {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.stopped {
		return
	}
	if sh.s == nil {
		sh.todo = true
		return
	}
	sh.wg.Add(1)
	go func() {
		defer sh.wg.Done()
		for sh.s.Ship(sh.ctx) != nil && sh.ctx.Err() == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}()
}

// start ships through s from now on, and ships what sealed before.
func (sh *shipper) start(t *testing.T, s *Store) {
	ctx, cancel := context.WithCancel(context.Background())
	sh.mu.Lock()
	sh.s, sh.ctx = s, ctx
	todo := sh.todo
	sh.mu.Unlock()
	t.Cleanup(func() {
		// The store seals until it closes, so a seal can land here. Under
		// the lock it either started its ship already, which Wait sees, or
		// starts none.
		sh.mu.Lock()
		sh.stopped = true
		sh.mu.Unlock()
		cancel()
		sh.wg.Wait()
		_ = s.Close(context.Background())
	})
	if todo {
		sh.sealed()
	}
}

type atomicCounter struct{ n int64 }

func (a *atomicCounter) add(n int64) { atomic.AddInt64(&a.n, n) }
func (a *atomicCounter) load() int64 { return atomic.LoadInt64(&a.n) }

func TestAppendThenGetSameProcess(t *testing.T) {
	s, _, _ := newTestStore(t, 64<<20, 5*time.Second, 6)

	blk := makeBlock(t, []byte("hello world"))
	root := makeRoot(t, "alpha")
	_, err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{Bucket: "bk", Root: root})
	require.NoError(t, err, "AppendBatch")

	got, err := s.Get(context.Background(), blk.Cid())
	require.NoError(t, err, "Get")
	require.Equal(t, "hello world", string(got.RawData()))
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
		_, err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
			Bucket: "bk",
			Root:   makeRoot(t, fmt.Sprintf("size-%d", i)),
		})
		require.NoError(t, err, "append %d", i)
	}

	// Wait for at least one flush.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if flushes.load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NotZero(t, flushes.load(), "expected at least one flush after size threshold; got 0")

	// At least one segment should now have shipped.
	meta.mu.Lock()
	var shipped int
	for _, m := range meta.segments {
		if m.ShippedAt != 0 {
			shipped++
		}
	}
	meta.mu.Unlock()
	require.NotZero(t, shipped, "expected at least one shipped segment")
}

func TestSealByAge(t *testing.T) {
	s, _, flushes := newTestStore(t, 1<<30, 80*time.Millisecond, 6)

	blk := makeBlock(t, []byte("age-trigger"))
	_, err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
		Bucket: "bk",
		Root:   makeRoot(t, "age"),
	})
	require.NoError(t, err, "append")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if flushes.load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NotZero(t, flushes.load(), "expected age-triggered seal to produce a flush")
}

// TestRetentionDropsOldFlushed: retention unlinks shipped segments past the
// Retain window but keeps their rows, stamped retired with their op-roots
// dropped, as the record of what each ship registered in the space.
func TestRetentionDropsOldFlushed(t *testing.T) {
	s, meta, _ := newTestStore(t, 64, 50*time.Millisecond, 2)
	dir := filepath.Dir(s.catalog.dir)

	// Issue 5 PUTs; each one large enough to exceed SealBytes=64 in
	// a single batch. The seal it requests is asynchronous, so pause past
	// SealAge between them to land each in its own segment.
	for i := 0; i < 5; i++ {
		if i > 0 {
			time.Sleep(100 * time.Millisecond)
		}
		payload := make([]byte, 80)
		for j := range payload {
			payload[j] = byte(i)
		}
		blk := makeBlock(t, append([]byte(fmt.Sprintf("retain-%02d-", i)), payload...))
		_, err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
			Bucket: "bk",
			Root:   makeRoot(t, fmt.Sprintf("ret-%d", i)),
		})
		require.NoError(t, err, "append %d", i)
	}

	// Wait for retention to converge.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := readSegmentSeqs(dir, "catalog")
		require.NoError(t, err, "readDir")
		// 1 active open + 2 retained
		if len(entries) <= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	entries, err := readSegmentSeqs(dir, "catalog")
	require.NoError(t, err, "readDir")
	require.LessOrEqual(t, len(entries), 3, "retain=2 should leave at most 3 segments (open + retained)")

	// The retired stamp lands after the unlink; wait for it. newTestStore
	// opens the store with no bucket, so its rows carry the empty name.
	var retired []SegmentMeta
	for deadline := time.Now().Add(3 * time.Second); len(retired) == 0 && time.Now().Before(deadline); {
		retired, err = meta.ListRetiredSegments(context.Background(), blockstore.PlaneCatalog, "")
		require.NoError(t, err, "ListRetiredSegments")
		time.Sleep(20 * time.Millisecond)
	}
	require.NotEmpty(t, retired, "expected retired segment rows to survive retention")
	entries, err = readSegmentSeqs(dir, "catalog")
	require.NoError(t, err, "readDir")
	onDisk := map[string]bool{}
	for _, e := range entries {
		onDisk[e] = true
	}
	for _, r := range retired {
		require.NotZero(t, r.RetiredAt, "retired row %d = %+v, want retired_at, shipped_at and sha256 set and no op-roots", r.Seq, r)
		require.NotZero(t, r.ShippedAt, "retired row %d = %+v, want retired_at, shipped_at and sha256 set and no op-roots", r.Seq, r)
		require.NotEmpty(t, r.SHA256, "retired row %d = %+v, want retired_at, shipped_at and sha256 set and no op-roots", r.Seq, r)
		require.Empty(t, r.OpRoots, "retired row %d = %+v, want retired_at, shipped_at and sha256 set and no op-roots", r.Seq, r)
		require.False(t, onDisk[carName(r.Seq)], "retired segment %d still has its CAR on disk", r.Seq)
	}
}

// TestRecoveryRetiresSealedRowWithoutCAR: a sealed row whose CAR is gone (a
// crash between retention's unlink and its retired stamp) is retired, not
// deleted, so DeleteBucket still releases what it registered; an open row
// whose CAR is gone never shipped and is deleted.
func TestRecoveryRetiresSealedRowWithoutCAR(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	meta := newFakeMeta()
	cfg := Config{
		Dir:     dir,
		Bucket:  "bk",
		Meta:    meta,
		Catalog: PlaneConfig{SealBytes: 1 << 30, SealAge: time.Hour, Ship: false},
		Logger:  zaptest.NewLogger(t),
	}
	s, err := Open(ctx, cfg)
	require.NoError(t, err, "Open")
	_, err = s.AppendBatch(ctx, []block.Block{makeBlock(t, []byte("sealed-then-lost"))}, blockstore.OpRoot{
		Bucket: "bk",
		Root:   makeRoot(t, "lost"),
	})
	require.NoError(t, err, "append")
	catDir := s.catalog.dir
	// seals the open segment
	require.NoError(t, s.Close(ctx), "Close")
	rows, err := meta.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "want one sealed row before the loss, got %+v (err %v)", rows, err)
	require.Len(t, rows, 1, "want one sealed row before the loss, got %+v (err %v)", rows, err)
	require.Equal(t, StateSealed, rows[0].State, "want one sealed row before the loss, got %+v (err %v)", rows, err)
	sealedSeq := rows[0].Seq
	require.NoError(t, os.Remove(filepath.Join(catDir, carName(sealedSeq))), "remove CAR")
	const openSeq = 1 << 40
	require.NoError(t, meta.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, openSeq, "bk"), "InsertSegmentOpen")

	s2, err := Open(ctx, cfg)
	require.NoError(t, err, "re-Open")
	t.Cleanup(func() { _ = s2.Close(ctx) })

	retired, err := meta.ListRetiredSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "want sealed seg %d retired, got %+v (err %v)", sealedSeq, retired, err)
	require.Len(t, retired, 1, "want sealed seg %d retired, got %+v (err %v)", sealedSeq, retired, err)
	require.Equal(t, sealedSeq, retired[0].Seq, "want sealed seg %d retired, got %+v (err %v)", sealedSeq, retired, err)
	live, err := meta.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "ListSegments")
	for _, r := range live {
		require.NotEqual(t, sealedSeq, r.Seq, "seg %d should not be live after recovery: %+v", r.Seq, r)
		require.NotEqual(t, uint64(openSeq), r.Seq, "seg %d should not be live after recovery: %+v", r.Seq, r)
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
		require.NoError(t, err, "Open")
		return s
	}

	s := openStore()
	blk := makeBlock(t, []byte("survives-restart"))
	_, err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
		Bucket: "bk",
		Root:   makeRoot(t, "survive"),
	})
	require.NoError(t, err, "append")

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
	require.NoError(t, err, "Get after restart")
	require.Equal(t, "survives-restart", string(got.RawData()))
}

func TestAppendBatchEmptyBlocksAccepted(t *testing.T) {
	s, _, _ := newTestStore(t, 64<<20, 5*time.Second, 6)
	root := makeRoot(t, "x")
	_, err := s.AppendBatch(context.Background(), nil, blockstore.OpRoot{Bucket: "bk", Root: root})
	require.NoError(t, err, "empty blocks with defined root should succeed")
	_, err = s.AppendBatch(context.Background(), []block.Block{makeBlock(t, []byte("x"))}, blockstore.OpRoot{Bucket: "bk"})
	require.Error(t, err, "expected error on undefined root")
}

func TestGetMissReturnsErrNotFound(t *testing.T) {
	s, _, _ := newTestStore(t, 64<<20, 5*time.Second, 6)

	want := makeRoot(t, "absent")
	_, err := s.Get(context.Background(), want)
	require.ErrorIs(t, err, blockstore.ErrNotFound)
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
	_, err := s.AppendBatch(context.Background(),
		[]block.Block{shared, uniqA},
		blockstore.OpRoot{Bucket: "bk", Root: makeRoot(t, "op-a")},
	)
	require.NoError(t, err, "append A")

	// Snapshot the open-segment size after the first append.
	s.catalog.catMu.RLock()
	sizeAfterA := s.catalog.open.Size()
	s.catalog.catMu.RUnlock()

	// Second batch: shared (duplicate of first batch) + uniqB.
	_, err = s.AppendBatch(context.Background(),
		[]block.Block{shared, uniqB},
		blockstore.OpRoot{Bucket: "bk", Root: makeRoot(t, "op-b")},
	)
	require.NoError(t, err, "append B")

	s.catalog.catMu.RLock()
	sizeAfterB := s.catalog.open.Size()
	s.catalog.catMu.RUnlock()

	// Frame for `shared` is one varint(len) + cid + payload. The second
	// batch should NOT have re-written it, so growth-from-B ≈ uniqB-frame
	// only — strictly less than the first batch's header + two frames.
	growthB := sizeAfterB - sizeAfterA
	growthFirstBatch := sizeAfterA // includes header + 2 frames; can't isolate

	require.Less(t, growthB, growthFirstBatch, "second batch grew %d bytes, expected ~half of first-batch growth (%d) since shared was deduped", growthB, growthFirstBatch)

	// All three blocks must be readable.
	for _, blk := range []block.Block{shared, uniqA, uniqB} {
		got, err := s.Get(context.Background(), blk.Cid())
		require.NoError(t, err, "Get %s", blk.Cid())
		require.Equal(t, blk.RawData(), got.RawData(), "Get %s payload mismatch", blk.Cid())
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

	_, err := s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
		Bucket: "bk", Root: makeRoot(t, "first"),
	})
	require.NoError(t, err, "first append")
	s.catalog.catMu.RLock()
	sizeBefore := s.catalog.open.Size()
	opRootsBefore := len(s.catalog.open.OpRoots())
	s.catalog.catMu.RUnlock()

	_, err = s.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
		Bucket: "bk", Root: makeRoot(t, "second"),
	})
	require.NoError(t, err, "dup append")
	s.catalog.catMu.RLock()
	sizeAfter := s.catalog.open.Size()
	opRootsAfter := len(s.catalog.open.OpRoots())
	s.catalog.catMu.RUnlock()

	require.Equal(t, sizeBefore, sizeAfter, "an all-duplicate batch grew the CAR")
	require.Equal(t, opRootsBefore+1, opRootsAfter, "op-root count")
}

// TestFailedShipStampRetriedWithoutReshipping checks that a segment whose
// ship succeeded but whose MarkSegmentShipped failed is stamped again
// without being shipped again, and doesn't count as shipped until then:
// retention retires shipped segments, and would drop this one's row while
// its journal rows, which only the stamp deletes, stayed behind.
func TestFailedShipStampRetriedWithoutReshipping(t *testing.T) {
	s, meta, flushes := newTestStore(t, 64, 50*time.Millisecond, 1)
	stampFailed := make(chan uint64, 1)
	meta.mu.Lock()
	meta.failShips = 1
	meta.onShipFail = func(seq uint64) { stampFailed <- seq }
	meta.mu.Unlock()

	blk := makeBlock(t, append([]byte("stamp-"), make([]byte, 80)...))
	seq, err := s.AppendBatch(context.Background(), []block.Block{blk},
		blockstore.OpRoot{Bucket: "bk", Root: makeRoot(t, "stamp")})
	require.NoError(t, err, "append")

	// Between the failed stamp and its retry, a backoff away, the segment
	// must not count as shipped.
	select {
	case failed := <-stampFailed:
		require.Equal(t, seq, failed, "seq whose stamp failed")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the stamp never failed")
	}
	s.catalog.catMu.RLock()
	for _, sealed := range s.catalog.sealed {
		if sealed.Seq() == seq && sealed.IsShipped() {
			s.catalog.catMu.RUnlock()
			require.FailNowf(t, "a segment counts as shipped while its stamp is failing", "segment %d", seq)
		}
	}
	s.catalog.catMu.RUnlock()

	deadline := time.Now().Add(5 * time.Second)
	for {
		meta.mu.Lock()
		shipped := len(meta.shipped)
		meta.mu.Unlock()
		if shipped > 0 {
			break
		}
		require.False(t, time.Now().After(deadline), "segment %d was never stamped shipped", seq)
		time.Sleep(50 * time.Millisecond)
	}
	got := flushes.load()
	require.Equal(t, int64(1), got, "segment shipped %d times", got)
	meta.mu.Lock()
	defer meta.mu.Unlock()
	m, ok := meta.segments[seq]
	require.True(t, ok, "segment %d row = %+v (present %v), want stamped shipped", seq, m, ok)
	require.NotZero(t, m.ShippedAt, "segment %d row = %+v (present %v)", seq, m, ok)
}

// TestFailedSealStampRecordedBeforeShip: a seal whose stamp in Meta fails
// (Postgres failing over, say) leaves the segment frozen and readable, and
// writes move on to a new segment. The ship records the seal before it
// ships, so the row it stamps shipped is sealed, with its sha256 for
// DeleteBucket's release.
func TestFailedSealStampRecordedBeforeShip(t *testing.T) {
	ctx := context.Background()
	s, meta, flushes := newTestStore(t, 64, 50*time.Millisecond, 6)
	sealFailed := make(chan uint64, 8)
	meta.mu.Lock()
	meta.failSeals = 2 // the seal itself, then the first ship's
	meta.onSealFail = func(seq uint64) { sealFailed <- seq }
	meta.mu.Unlock()

	blk := makeBlock(t, append([]byte("seal-"), make([]byte, 80)...))
	seq, err := s.AppendBatch(ctx, []block.Block{blk},
		blockstore.OpRoot{Bucket: "bk", Root: makeRoot(t, "seal")})
	require.NoError(t, err, "append")
	select {
	case failed := <-sealFailed:
		require.Equal(t, seq, failed, "seq whose seal stamp failed")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the seal stamp never failed")
	}

	got, err := s.Get(ctx, blk.Cid())
	require.NoError(t, err, "Get after a failed seal stamp")
	require.Equal(t, blk.RawData(), got.RawData(), "Get after a failed seal stamp")
	next := makeBlock(t, append([]byte("next-"), make([]byte, 80)...))
	seq2, err := s.AppendBatch(ctx, []block.Block{next},
		blockstore.OpRoot{Bucket: "bk", Root: makeRoot(t, "next")})
	require.NoError(t, err, "append after a failed seal stamp")
	require.NotEqual(t, seq, seq2, "append after a failed seal stamp went to the frozen segment %d", seq)

	deadline := time.Now().Add(5 * time.Second)
	for {
		meta.mu.Lock()
		var ship *shipEvent
		for i := range meta.shipped {
			if meta.shipped[i].seq == seq {
				ship = &meta.shipped[i]
			}
		}
		meta.mu.Unlock()
		if ship != nil {
			require.True(t, ship.rowSealed, "segment %d shipped while its row wasn't stamped sealed", seq)
			break
		}
		if time.Now().After(deadline) {
			require.FailNowf(t, "segment never shipped", "segment %d (%d flushes)", seq, flushes.load())
		}
		time.Sleep(20 * time.Millisecond)
	}
	got, err = s.Get(ctx, blk.Cid())
	require.NoError(t, err, "Get after the ship")
	require.Equal(t, blk.RawData(), got.RawData(), "Get after the ship")
}

// TestReadsGoOnWhileTheSealStampWaits: a seal stamp that hangs, as on a
// Postgres connection lost in a failover, doesn't hold up reads of the
// segment being sealed.
func TestReadsGoOnWhileTheSealStampWaits(t *testing.T) {
	ctx := context.Background()
	s, meta, _ := newTestStore(t, 64, 50*time.Millisecond, 6)
	gate := make(chan struct{})
	waiting := make(chan uint64, 1)
	meta.mu.Lock()
	meta.sealGate = gate
	meta.onSealWait = func(seq uint64) {
		select {
		case waiting <- seq:
		default:
		}
	}
	meta.mu.Unlock()
	t.Cleanup(func() { close(gate) }) // before the store's Close, registered earlier

	blk := makeBlock(t, append([]byte("hang-"), make([]byte, 80)...))
	seq, err := s.AppendBatch(ctx, []block.Block{blk},
		blockstore.OpRoot{Bucket: "bk", Root: makeRoot(t, "hang")})
	require.NoError(t, err, "append")
	select {
	case waited := <-waiting:
		require.Equal(t, seq, waited, "seq whose seal stamp waits")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the seal never reached its stamp")
	}

	got := make(chan error, 1)
	go func() {
		b, err := s.Get(ctx, blk.Cid())
		if err == nil && string(b.RawData()) != string(blk.RawData()) {
			err = fmt.Errorf("read other bytes")
		}
		got <- err
	}()
	select {
	case err := <-got:
		require.NoError(t, err, "Get while the seal stamp waits")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "a read waited on the seal's stamp")
	}
}

// TestCatalogNeverShips covers the standalone-mode permutation: a catalog
// plane configured never to ship keeps its CARs on local disk indefinitely
// (the source for every read), and its shipped_at never advances (so
// forge_root_cid never advances).
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
	require.NoError(t, err, "Open")
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	const n = 4
	var blocks []block.Block
	for i := 0; i < n; i++ {
		c := makeBlock(t, []byte(fmt.Sprintf("cat-%02d", i)))
		blocks = append(blocks, c)
		_, err := s.AppendBatch(context.Background(), []block.Block{c},
			blockstore.OpRoot{Bucket: "bk", Root: c.Cid()},
		)
		require.NoError(t, err, "append %d", i)
		time.Sleep(30 * time.Millisecond) // let each batch seal separately
	}

	// Give the (absent) ship pipeline time to NOT run.
	time.Sleep(200 * time.Millisecond)
	require.Zero(t, ships.load(), "catalog Ship=false should never flush")

	// No sealed segment ever ships.
	meta.mu.Lock()
	sealed := 0
	for seq, m := range meta.segments {
		if m.State != StateSealed {
			continue
		}
		sealed++
		require.Zero(t, m.ShippedAt, "catalog seg %d shipped (%d) but Ship=false", seq, m.ShippedAt)
	}
	meta.mu.Unlock()
	require.NotZero(t, sealed, "expected sealed segments; got none")

	// Every block stays readable from local disk (CARs retained).
	for _, c := range blocks {
		_, err := s.Get(context.Background(), c.Cid())
		require.NoError(t, err, "catalog block %s should be retained locally", c.Cid())
	}
	cars, _ := filepath.Glob(filepath.Join(dir, "catalog", "seg-*.car"))
	require.GreaterOrEqual(t, len(cars), n, "expected >= %d catalog CARs retained; got %d", n, len(cars))
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

// TestShipStopsAtTheFirstFailure checks Ship's order and its stop: it
// tries the oldest unshipped segment first, stops at the first that fails
// and returns its error, and once ships succeed, ships them all in seq
// order. A segment already shipped never goes to Flush again.
func TestShipStopsAtTheFirstFailure(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	var mu sync.Mutex
	var tried []uint64
	meta := newFakeMeta()
	s, err := Open(context.Background(), Config{
		Dir:  t.TempDir(),
		Meta: meta,
		Catalog: PlaneConfig{SealBytes: 64, SealAge: time.Hour, Ship: true,
			Flush: func(_ context.Context, seg *Segment) (multihash.Multihash, error) {
				mu.Lock()
				tried = append(tried, seg.Seq())
				mu.Unlock()
				if down.Load() {
					return nil, errors.New("upload service down")
				}
				return nil, nil
			}},
		Logger: zaptest.NewLogger(t),
	})
	require.NoError(t, err, "Open")
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	// Each batch is over SealBytes, so it seals its segment.
	for i := 0; i < 3; i++ {
		blk := makeBlock(t, append([]byte(fmt.Sprintf("order-%d-", i)), make([]byte, 80)...))
		_, err := s.AppendBatch(context.Background(), []block.Block{blk},
			blockstore.OpRoot{Bucket: "bk", Root: makeRoot(t, fmt.Sprintf("order-%d", i))})
		require.NoError(t, err, "append %d", i)
		waitFor(t, fmt.Sprintf("segment %d to seal", i), func() bool {
			s.catalog.catMu.RLock()
			defer s.catalog.catMu.RUnlock()
			return len(s.catalog.sealed) == i+1
		})
	}
	s.catalog.catMu.RLock()
	var seqs []uint64
	for _, seg := range s.catalog.sealed {
		seqs = append(seqs, seg.Seq())
	}
	s.catalog.catMu.RUnlock()
	slices.Sort(seqs)

	require.Error(t, s.Ship(context.Background()), "Ship succeeded with the upload service down")
	mu.Lock()
	if !slices.Equal(tried, seqs[:1]) {
		mu.Unlock()
		require.FailNowf(t, "a failing Ship must try once, on the oldest segment", "tried %v, oldest %d", tried, seqs[0])
	}
	mu.Unlock()

	down.Store(false)
	require.NoError(t, s.Ship(context.Background()), "Ship")
	meta.mu.Lock()
	shippedSeqs := make([]uint64, len(meta.shipped))
	for i, ev := range meta.shipped {
		shippedSeqs[i] = ev.seq
	}
	meta.mu.Unlock()
	require.True(t, slices.Equal(shippedSeqs, seqs), "shipped seqs %v, want %v in order", shippedSeqs, seqs)

	mu.Lock()
	before := len(tried)
	mu.Unlock()
	require.NoError(t, s.Ship(context.Background()), "Ship of nothing")
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, tried, before, "shipped segments went to Flush again")
}

// waitFor polls cond until it holds, failing the test after a few seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		require.False(t, time.Now().After(deadline), "timed out waiting for %s", what)
		time.Sleep(10 * time.Millisecond)
	}
}
