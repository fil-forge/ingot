package logstore

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/multiformats/go-multihash"
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
	m, err := OpenManager(context.Background(), ManagerConfig{
		Dir:  dir,
		Meta: meta,
		// Tiny SealAge so appends seal + flush quickly.
		Catalog:  PlaneConfig{SealBytes: 1 << 20, SealAge: 50 * time.Millisecond, Ship: true, Retain: 2},
		FlushFor: flush.forBucket,
		Logger:   zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	return m
}

func appendFor(t *testing.T, m *Manager, bucket, payload string) block.Block {
	t.Helper()
	blk := makeBlock(t, []byte(payload))
	err := m.AppendBatch(context.Background(), []block.Block{blk}, blockstore.OpRoot{
		Bucket: bucket,
		Root:   makeRoot(t, bucket+"/"+payload),
	})
	if err != nil {
		t.Fatalf("AppendBatch(%s): %v", bucket, err)
	}
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
		if _, err := os.Stat(filepath.Join(dir, b, "catalog")); err != nil {
			t.Fatalf("expected per-bucket dir for %q: %v", b, err)
		}
	}

	// Cross-bucket Get serves both buckets' blocks through one handle.
	for _, blk := range []block.Block{blkA, blkB} {
		got, err := m.Get(context.Background(), blk.Cid())
		if err != nil {
			t.Fatalf("Get(%s): %v", blk.Cid(), err)
		}
		if string(got.RawData()) != string(blk.RawData()) {
			t.Fatalf("Get returned wrong block for %s", blk.Cid())
		}
	}

	// Each bucket's segments flush through its own closure.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if flush.count("alpha") > 0 && flush.count("beta") > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if flush.count("alpha") == 0 || flush.count("beta") == 0 {
		t.Fatalf("expected both buckets to flush; alpha=%d beta=%d",
			flush.count("alpha"), flush.count("beta"))
	}

	// Meta rows are stamped per bucket.
	for _, b := range []string{"alpha", "beta"} {
		rows, err := meta.ListSegments(context.Background(), blockstore.PlaneCatalog, b)
		if err != nil || len(rows) == 0 {
			t.Fatalf("ListSegments(%q) = %d rows, err %v", b, len(rows), err)
		}
	}
	buckets, err := meta.ListSegmentBuckets(context.Background(), blockstore.PlaneCatalog)
	if err != nil || len(buckets) != 2 {
		t.Fatalf("ListSegmentBuckets = %v, err %v (want [alpha beta])", buckets, err)
	}
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
	if err := m1.Close(context.Background()); err != nil {
		t.Fatalf("close first manager: %v", err)
	}

	m2 := openTestManager(t, dir, meta, flush)
	for _, blk := range []block.Block{blkA, blkB} {
		got, err := m2.Get(context.Background(), blk.Cid())
		if err != nil {
			t.Fatalf("Get after restart (%s): %v", blk.Cid(), err)
		}
		if string(got.RawData()) == "" {
			t.Fatalf("empty block after restart")
		}
	}
}

// TestManager_RemoveBucketLog: removal drops the bucket's directory, its
// segment rows, and its blocks — without touching the other bucket.
func TestManager_RemoveBucketLog(t *testing.T) {
	dir := t.TempDir()
	meta := newFakeMeta()
	flush := newRecordingFlush()
	m := openTestManager(t, dir, meta, flush)

	blkA := appendFor(t, m, "alpha", "doomed")
	blkB := appendFor(t, m, "beta", "kept")

	if err := m.RemoveBucketLog(context.Background(), "alpha"); err != nil {
		t.Fatalf("RemoveBucketLog: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "alpha")); !os.IsNotExist(err) {
		t.Fatalf("expected alpha dir gone, stat err = %v", err)
	}
	rows, err := meta.ListSegments(context.Background(), blockstore.PlaneCatalog, "alpha")
	if err != nil || len(rows) != 0 {
		t.Fatalf("expected no alpha segment rows, got %d (err %v)", len(rows), err)
	}
	if _, err := m.Get(context.Background(), blkA.Cid()); err == nil {
		t.Fatalf("expected alpha block gone after removal")
	}
	if _, err := m.Get(context.Background(), blkB.Cid()); err != nil {
		t.Fatalf("beta block must survive alpha's removal: %v", err)
	}
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
	if err := m.QuiesceBucketLog(context.Background(), "alpha"); err != nil {
		t.Fatalf("QuiesceBucketLog: %v", err)
	}

	rows, err := meta.ListSegments(context.Background(), blockstore.PlaneCatalog, "alpha")
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	shipped := map[uint64]bool{}
	sealed := 0
	for _, r := range rows {
		if r.State == StateSealed {
			sealed++
			shipped[r.Seq] = r.ShippedAt != 0
		}
	}
	if sealed == 0 {
		t.Fatalf("expected the quiesce to force-seal the open segment")
	}

	digests, err := m.ShippedSegmentDigests(context.Background(), "alpha")
	if err != nil {
		t.Fatalf("ShippedSegmentDigests: %v", err)
	}
	// Every sealed segment contributes a 34-byte sha2-256 CAR multihash;
	// shipped ones additionally the flush's index digest.
	wantLen := 0
	for _, s := range shipped {
		wantLen++
		if s {
			wantLen++
		}
	}
	if len(digests) != wantLen {
		t.Fatalf("got %d digests, want %d (sealed=%d shipped=%v)", len(digests), wantLen, sealed, shipped)
	}
	for _, d := range digests {
		if len(d) != 34 && string(d) != string(fakeIndexDigest) {
			t.Fatalf("unexpected digest %q (len %d)", d, len(d))
		}
	}

	// A bucket with no sealed segments reports none, and the quiesced
	// bucket reopens lazily for new appends.
	if got, err := m.ShippedSegmentDigests(context.Background(), "gamma"); err != nil || len(got) != 0 {
		t.Fatalf("gamma: got %d digests, err %v", len(got), err)
	}
	appendFor(t, m, "alpha", "post-quiesce-write")
}

// TestManager_RejectsUnsafeBucketNames: the bucket→directory mapping must
// not be escapable.
func TestManager_RejectsUnsafeBucketNames(t *testing.T) {
	m := openTestManager(t, t.TempDir(), newFakeMeta(), newRecordingFlush())
	for _, name := range []string{"", ".", "..", "a/b", `a\b`} {
		if _, err := m.logFor(context.Background(), name); err == nil {
			t.Fatalf("logFor(%q) should fail", name)
		}
	}
}
