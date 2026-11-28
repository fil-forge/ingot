package logstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
)

// recordingFlush counts flushes per bucket so tests can assert each
// bucket's segments ship through that bucket's own closure. Every flush
// reports fakeIndexDigest as the shipped index blob, as a real ship would.
type recordingFlush struct {
	mu    sync.Mutex
	byBkt map[string]int
}

var fakeIndexDigest = multihash.Multihash("fake-index-digest-multihash")

func newRecordingFlush() *recordingFlush { return &recordingFlush{byBkt: map[string]int{}} }

func (r *recordingFlush) forBucket(bucket string) FlushFunc {
	return func(context.Context, *Segment) (multihash.Multihash, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.byBkt[bucket]++
		return fakeIndexDigest, nil
	}
}

func (r *recordingFlush) count(bucket string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byBkt[bucket]
}

func openTestManager(t *testing.T, dir string, meta Meta, flush *recordingFlush) *Manager {
	t.Helper()
	m, _ := openTestManagerAndShipper(t, dir, meta, flush)
	return m
}

func openTestManagerAndShipper(t *testing.T, dir string, meta Meta, flush *recordingFlush) (*Manager, *managerShipper) {
	t.Helper()
	sh := &managerShipper{}
	m, err := OpenManager(context.Background(), ManagerConfig{
		Dir:  dir,
		Meta: meta,
		// Tiny SealAge so appends seal + flush quickly.
		Catalog:  PlaneConfig{SealBytes: 1 << 20, SealAge: 50 * time.Millisecond, Ship: true, Retain: 2},
		FlushFor: flush.forBucket,
		Logger:   zaptest.NewLogger(t),
	})
	require.NoError(t, err, "OpenManager")
	sh.start(t, m)
	return m, sh
}

// managerShipper stands in for the ship queue: each seal starts a
// ShipBucket, retried until it succeeds or the test ends.
type managerShipper struct {
	m      *Manager
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	stopped bool
}

func (sh *managerShipper) sealed(bucket string) {
	sh.mu.Lock()
	if sh.stopped {
		sh.mu.Unlock()
		return
	}
	sh.wg.Add(1)
	sh.mu.Unlock()

	m, ctx := sh.m, sh.ctx
	go func() {
		defer sh.wg.Done()
		for ctx.Err() == nil && m.ShipBucket(ctx, bucket) != nil {
			time.Sleep(10 * time.Millisecond)
		}
	}()
}

func (sh *managerShipper) start(t *testing.T, m *Manager) {
	sh.m = m
	sh.ctx, sh.cancel = context.WithCancel(context.Background())
	t.Cleanup(func() {
		sh.stop()
		_ = m.Close(context.Background())
	})
	m.OnSealed(sh.sealed)
}

// stop waits for the ships in flight and starts no more, whatever seals
// afterwards. The ship queue runs no ship for a bucket while it removes the
// bucket's log, and a test that removes one stops the shipper first for the
// same reason: ShipBucket opens the log of a bucket the manager does not
// hold, so a ship beside the removal can open the log again as it goes.
func (sh *managerShipper) stop() {
	sh.mu.Lock()
	sh.stopped = true
	sh.mu.Unlock()
	sh.cancel()
	sh.wg.Wait()
}

func appendFor(t *testing.T, m *Manager, bucket, payload string) block.Block {
	t.Helper()
	blk := makeBlock(t, []byte(payload))
	_, err := m.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
		Bucket: bucket,
		Root:   makeRoot(t, bucket+"/"+payload),
	})
	require.NoError(t, err, "AppendBatch(%s)", bucket)
	return blk
}

// TestManager_SegregatesBuckets: two buckets appending through one Manager
// land in distinct per-bucket directories, their segments carry their own
// bucket in Meta, and each bucket's segments flush through that bucket's
// closure.
func TestManager_SegregatesBuckets(t *testing.T) {
	dir := t.TempDir()
	meta := newFakeMeta()
	flush := newRecordingFlush()
	m := openTestManager(t, dir, meta, flush)

	blkA := appendFor(t, m, "alpha", "block-in-alpha")
	blkB := appendFor(t, m, "beta", "block-in-beta")

	// Distinct per-bucket directories exist.
	for _, b := range []string{"alpha", "beta"} {
		_, err := os.Stat(filepath.Join(dir, b, "catalog"))
		require.NoError(t, err, "expected per-bucket dir for %q", b)
	}

	// Cross-bucket Get serves both buckets' blocks through one handle.
	for _, blk := range []block.Block{blkA, blkB} {
		got, err := m.Get(context.Background(), blk.Cid())
		require.NoError(t, err, "Get(%s)", blk.Cid())
		require.Equal(t, blk.RawData(), got.RawData(), "Get returned wrong block for %s", blk.Cid())
	}

	// Each bucket's segments flush through its own closure.
	require.Eventually(t, func() bool {
		return flush.count("alpha") > 0 && flush.count("beta") > 0
	}, 2*time.Second, 10*time.Millisecond, "expected both buckets to flush")

	// Meta rows are stamped per bucket.
	for _, b := range []string{"alpha", "beta"} {
		rows, err := meta.ListSegments(context.Background(), blockstore.PlaneCatalog, b)
		require.NoError(t, err, "ListSegments(%q) = %d rows, err %v", b, len(rows), err)
		require.NotEmpty(t, rows, "ListSegments(%q) = %d rows, err %v", b, len(rows), err)
	}
	buckets, err := meta.ListSegmentBuckets(context.Background(), blockstore.PlaneCatalog)
	require.NoError(t, err, "ListSegmentBuckets = %v, err %v", buckets, err)
	require.Len(t, buckets, 2, "ListSegmentBuckets = %v, err %v", buckets, err)
}

// TestManager_RecoverySweep: a restarted Manager re-opens every bucket that
// left segments behind and serves their blocks without any new write.
func TestManager_RecoverySweep(t *testing.T) {
	dir := t.TempDir()
	meta := newFakeMeta()
	flush := newRecordingFlush()

	m1 := openTestManager(t, dir, meta, flush)
	blkA := appendFor(t, m1, "alpha", "survives-restart-a")
	blkB := appendFor(t, m1, "beta", "survives-restart-b")
	require.NoError(t, m1.Close(context.Background()), "close first manager")

	m2 := openTestManager(t, dir, meta, flush)
	for _, blk := range []block.Block{blkA, blkB} {
		got, err := m2.Get(context.Background(), blk.Cid())
		require.NoError(t, err, "Get after restart (%s)", blk.Cid())
		require.NotEmpty(t, string(got.RawData()), "empty block after restart")
	}
}

// TestManager_RemoveBucketLog: removal drops the bucket's directory, its
// segment rows, and its blocks — without touching the other bucket.
func TestManager_RemoveBucketLog(t *testing.T) {
	dir := t.TempDir()
	meta := newFakeMeta()
	flush := newRecordingFlush()
	m, shipper := openTestManagerAndShipper(t, dir, meta, flush)

	blkA := appendFor(t, m, "alpha", "doomed")
	blkB := appendFor(t, m, "beta", "kept")

	shipper.stop()
	require.NoError(t, m.RemoveBucketLog(context.Background(), "alpha"), "RemoveBucketLog")

	_, err := os.Stat(filepath.Join(dir, "alpha"))
	require.True(t, os.IsNotExist(err), "expected alpha dir gone, stat err = %v", err)
	rows, err := meta.ListSegments(context.Background(), blockstore.PlaneCatalog, "alpha")
	require.NoError(t, err, "ListSegments")
	require.Empty(t, rows, "expected no alpha segment rows")
	_, err = m.Get(context.Background(), blkA.Cid())
	require.Error(t, err, "expected alpha block gone after removal")
	_, err = m.Get(context.Background(), blkB.Cid())
	require.NoError(t, err, "beta block must survive alpha's removal")
}

// TestManager_QuiesceAndShippedSegmentDigests: quiescing joins the flush
// pipeline so the enumeration afterwards is final — every shipped segment
// contributes its CAR multihash and index-blob digest, sealed-but-unshipped
// segments contribute their CAR only (a flush aborted after the CAR's
// blob/add may have registered it), and a bucket with no sealed segments
// contributes nothing.
func TestManager_QuiesceAndShippedSegmentDigests(t *testing.T) {
	dir := t.TempDir()
	meta := newFakeMeta()
	flush := newRecordingFlush()
	m := openTestManager(t, dir, meta, flush)

	appendFor(t, m, "alpha", "shipped-content")

	// Quiesce without waiting for the seal-age tick: it force-seals the open
	// segment and joins the flush pipeline, so the rows are final on return
	// regardless of whether the tail segment shipped or was dropped.
	require.NoError(t, m.QuiesceBucketLog(context.Background(), "alpha"), "QuiesceBucketLog")

	rows, err := meta.ListSegments(context.Background(), blockstore.PlaneCatalog, "alpha")
	require.NoError(t, err, "ListSegments")
	shipped := map[uint64]bool{}
	sealed := 0
	for _, r := range rows {
		if r.State == StateSealed {
			sealed++
			shipped[r.Seq] = r.ShippedAt != 0
		}
	}
	require.NotZero(t, sealed, "expected the quiesce to force-seal the open segment")

	digests, err := m.ShippedSegmentDigests(context.Background(), "alpha")
	require.NoError(t, err, "ShippedSegmentDigests")
	// Every sealed segment contributes a 34-byte sha2-256 CAR multihash;
	// shipped ones additionally the flush's index digest.
	wantLen := 0
	for _, s := range shipped {
		wantLen++
		if s {
			wantLen++
		}
	}
	require.Len(t, digests, wantLen, "got %d digests, want %d (sealed=%d shipped=%v)", len(digests), wantLen, sealed, shipped)
	for _, d := range digests {
		require.True(t, len(d) == 34 || string(d) == string(fakeIndexDigest), "unexpected digest %q (len %d)", d, len(d))
	}

	// A bucket with no sealed segments reports none, and the quiesced
	// bucket reopens lazily for new appends.
	got, err := m.ShippedSegmentDigests(context.Background(), "gamma")
	require.NoError(t, err, "gamma")
	require.Empty(t, got, "gamma digests")
	appendFor(t, m, "alpha", "post-quiesce-write")
}

// TestManager_ReleaseRetiredSegments: segments retention has already unlinked
// still registered their CAR and index blobs in the space, so DeleteBucket
// releases them through ReleaseRetiredSegments, which drops each row once its
// blobs are released. A release that fails partway keeps the rows it hadn't
// finished, and the retry releases only those.
func TestManager_ReleaseRetiredSegments(t *testing.T) {
	ctx := context.Background()
	meta := newFakeMeta()
	m := openTestManager(t, t.TempDir(), meta, newRecordingFlush())

	// Each append lands in its own segment (SealAge is 50ms); with Retain 2,
	// the older ones ship and retire.
	deadline := time.Now().Add(5 * time.Second)
	var retired []SegmentMeta
	for i := 0; len(retired) < 2; i++ {
		require.False(t, time.Now().After(deadline), "no segments retired in time (have %d)", len(retired))
		appendFor(t, m, "alpha", fmt.Sprintf("generation-%d", i))
		time.Sleep(100 * time.Millisecond)
		var err error
		retired, err = meta.ListRetiredSegments(ctx, blockstore.PlaneCatalog, "alpha")
		require.NoError(t, err, "ListRetiredSegments")
	}

	require.NoError(t, m.QuiesceBucketLog(ctx, "alpha"), "QuiesceBucketLog")
	retired, err := meta.ListRetiredSegments(ctx, blockstore.PlaneCatalog, "alpha")
	require.NoError(t, err, "ListRetiredSegments")
	cars := map[string]uint64{}
	for _, r := range retired {
		car, err := multihash.Encode(r.SHA256, multihash.SHA2_256)
		require.NoError(t, err, "encode")
		cars[string(car)] = r.Seq
	}

	// The unretired listing leaves retired segments to ReleaseRetiredSegments.
	digests, err := m.ShippedSegmentDigests(ctx, "alpha")
	require.NoError(t, err, "ShippedSegmentDigests")
	for _, d := range digests {
		seq, ok := cars[string(d)]
		require.False(t, ok, "ShippedSegmentDigests lists retired segment %d", seq)
	}

	released := map[string]int{}
	refused := errors.New("release refused")
	carCalls := 0
	err = m.ReleaseRetiredSegments(ctx, "alpha", func(d multihash.Multihash) error {
		if _, ok := cars[string(d)]; ok {
			if carCalls++; carCalls == 2 {
				return refused
			}
		}
		released[string(d)]++
		return nil
	})
	require.ErrorIs(t, err, refused, "ReleaseRetiredSegments with a refusing release")
	left, err := meta.ListRetiredSegments(ctx, blockstore.PlaneCatalog, "alpha")
	require.NoError(t, err, "ListRetiredSegments")
	require.Len(t, left, len(retired)-1, "retired rows left after one segment's release")

	require.NoError(t, m.ReleaseRetiredSegments(ctx, "alpha", func(d multihash.Multihash) error {
		released[string(d)]++
		return nil
	}), "retried ReleaseRetiredSegments")
	for car, seq := range cars {
		require.Equal(t, 1, released[car], "retired segment %d's CAR releases", seq)
	}
	got := released[string(fakeIndexDigest)]
	require.Equal(t, len(retired), got, "released index blobs, want one per retired segment")
	left, err = meta.ListRetiredSegments(ctx, blockstore.PlaneCatalog, "alpha")
	require.NoError(t, err, "ListRetiredSegments")
	require.Empty(t, left, "retired rows after the release")
}

// TestManager_RejectsUnsafeBucketNames: the bucket→directory mapping must
// not be escapable.
func TestManager_RejectsUnsafeBucketNames(t *testing.T) {
	m := openTestManager(t, t.TempDir(), newFakeMeta(), newRecordingFlush())
	for _, name := range []string{"", ".", "..", "a/b", `a\b`} {
		_, err := m.logFor(context.Background(), name)
		require.Error(t, err, "logFor(%q) should fail", name)
	}
}
