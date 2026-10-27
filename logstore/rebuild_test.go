package logstore_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
)

// shipRecorder is a FlushFunc source that records the seqs it shipped.
type shipRecorder struct {
	mu   sync.Mutex
	seqs map[uint64]bool
}

func (r *shipRecorder) forBucket(string) logstore.FlushFunc {
	return func(_ context.Context, seg *logstore.Segment) (multihash.Multihash, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.seqs == nil {
			r.seqs = map[uint64]bool{}
		}
		r.seqs[seg.Seq()] = true
		return multihash.Multihash("rebuild-test-index"), nil
	}
}

func (r *shipRecorder) shipped() map[uint64]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[uint64]bool{}
	for s := range r.seqs {
		out[s] = true
	}
	return out
}

func openRebuildManager(t *testing.T, dir string, meta logstore.Meta, ship *shipRecorder) (*logstore.Manager, error) {
	t.Helper()
	m, err := logstore.OpenManager(context.Background(), logstore.ManagerConfig{
		Dir:  dir,
		Meta: meta,
		// A small SealBytes makes the rebuild split across segments.
		Catalog:  logstore.PlaneConfig{SealBytes: 8 << 10, SealAge: time.Hour, Ship: true, Retain: 100},
		FlushFor: ship.forBucket,
		Logger:   zaptest.NewLogger(t),
	})
	if err == nil {
		t.Cleanup(func() { _ = m.Close(context.Background()) })
	}
	return m, err
}

// journalSegment opens segment rows for bucket in mem and journals n blocks
// under it through a root commit, as a write does, without writing any
// segment file: the state a lost data volume leaves behind.
func journalSegment(t *testing.T, mem *inmem.MemStore, bucket string, n int) (uint64, []block.Block) {
	t.Helper()
	ctx := context.Background()
	seq, err := mem.NextSegmentSeq(ctx)
	if err != nil {
		t.Fatalf("NextSegmentSeq: %v", err)
	}
	if err := mem.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, bucket); err != nil {
		t.Fatalf("InsertSegmentOpen: %v", err)
	}
	blocks := make([]block.Block, n)
	for i := range blocks {
		blocks[i] = block.NewBlock([]byte(fmt.Sprintf("%s/seg-%d/block-%04d/%s", bucket, seq, i, strings.Repeat("x", 64))))
	}
	st, err := mem.Get(ctx, bucket)
	if err != nil {
		t.Fatalf("Get %s: %v", bucket, err)
	}
	if err := mem.CommitRoot(ctx, bucket, st.Root, blocks[n-1].Cid(), registry.RootCommit{
		Journal: blockstore.Appended{Segment: seq, Blocks: blocks},
	}); err != nil {
		t.Fatalf("CommitRoot: %v", err)
	}
	return seq, blocks
}

func requireReadable(t *testing.T, m *logstore.Manager, blocks []block.Block) {
	t.Helper()
	for _, b := range blocks {
		got, err := m.Get(context.Background(), b.Cid())
		if err != nil {
			t.Fatalf("Get %s after the rebuild: %v", b.Cid(), err)
		}
		if string(got.RawData()) != string(b.RawData()) {
			t.Fatalf("Get %s returned other bytes", b.Cid())
		}
	}
}

// onDiskSeqs returns the seqs of the segment CARs in bucket's catalog
// directory.
func onDiskSeqs(t *testing.T, dir, bucket string) map[uint64]bool {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, bucket, blockstore.PlaneCatalog.String()))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	out := map[uint64]bool{}
	for _, e := range entries {
		stem, ok := strings.CutSuffix(strings.TrimPrefix(e.Name(), "seg-"), ".car")
		if !ok {
			continue
		}
		seq, err := strconv.ParseUint(stem, 10, 64)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		out[seq] = true
	}
	return out
}

// TestRebuild_WipedVolume: with the data volume gone, the blocks the journal
// holds for the bucket's open and sealed segments are rebuilt into new
// segments, readable at once, with the journal rows re-filed under them and
// shipped from them. The lost open segment's row goes; the lost sealed one's
// is retired, as recovery treats any fileless row.
func TestRebuild_WipedVolume(t *testing.T) {
	ctx := context.Background()
	mem := inmem.NewMemStore()
	if err := mem.Create(ctx, "bk", did.Undef, registry.CreateState{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	sealedSeq, sealedBlocks := journalSegment(t, mem, "bk", 10)
	if err := mem.MarkSegmentSealed(ctx, blockstore.PlaneCatalog, sealedSeq, time.Now().Unix(), 1, make([]byte, 32), nil); err != nil {
		t.Fatalf("MarkSegmentSealed: %v", err)
	}
	// More than two journal pages, so the rebuild pages and splits.
	openSeq, openBlocks := journalSegment(t, mem, "bk", 600)

	dir := t.TempDir()
	ship := &shipRecorder{}
	m, err := openRebuildManager(t, dir, mem, ship)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	requireReadable(t, m, append(sealedBlocks, openBlocks...))

	for _, lost := range []uint64{sealedSeq, openSeq} {
		if rows := mem.JournaledBlocks(lost); len(rows) != 0 {
			t.Fatalf("%d journal rows still under lost segment %d", len(rows), lost)
		}
	}
	live, err := mem.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	for _, r := range live {
		if r.Seq == sealedSeq || r.Seq == openSeq {
			t.Fatalf("lost segment %d still has a live row (%v)", r.Seq, r.State)
		}
	}
	retired, err := mem.ListRetiredSegments(ctx, blockstore.PlaneCatalog, "bk")
	if err != nil {
		t.Fatalf("ListRetiredSegments: %v", err)
	}
	if len(retired) != 1 || retired[0].Seq != sealedSeq {
		t.Fatalf("retired rows = %+v, want the lost sealed segment %d", retired, sealedSeq)
	}

	rebuilt := onDiskSeqs(t, dir, "bk")
	if len(rebuilt) < 2 {
		t.Fatalf("rebuilt into %d segments, want the small SealBytes to split it", len(rebuilt))
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		stats, err := mem.CatalogJournalStats(ctx)
		if err != nil {
			t.Fatalf("CatalogJournalStats: %v", err)
		}
		shipped := ship.shipped()
		all := true
		for seq := range rebuilt {
			all = all && shipped[seq]
		}
		if all && stats.Blocks == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rebuilt segments %v, shipped %v, %d journal rows left", rebuilt, shipped, stats.Blocks)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRebuild_JournalOnlyBucket: a bucket whose lost segment's row is already
// gone (an older recovery deleted it) is found from the journal alone.
func TestRebuild_JournalOnlyBucket(t *testing.T) {
	ctx := context.Background()
	mem := inmem.NewMemStore()
	if err := mem.Create(ctx, "bk", did.Undef, registry.CreateState{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	seq, blocks := journalSegment(t, mem, "bk", 5)
	if err := mem.DeleteSegment(ctx, blockstore.PlaneCatalog, seq); err != nil {
		t.Fatalf("DeleteSegment: %v", err)
	}
	if b, _ := mem.ListSegmentBuckets(ctx, blockstore.PlaneCatalog); len(b) != 0 {
		t.Fatalf("segment buckets = %v, want none; the test proves nothing", b)
	}

	m, err := openRebuildManager(t, t.TempDir(), mem, &shipRecorder{})
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	requireReadable(t, m, blocks)
}

// failSecondMove fails the second MoveJournal, as a crash between a page's
// fsync and its journal move would leave things.
type failSecondMove struct {
	*inmem.MemStore
	calls int
}

func (f *failSecondMove) MoveJournal(ctx context.Context, bucket string, from, to uint64, first, last cid.Cid) error {
	if f.calls++; f.calls == 2 {
		return errors.New("injected move failure")
	}
	return f.MemStore.MoveJournal(ctx, bucket, from, to, first, last)
}

// TestRebuild_ResumesAfterFailure: a rebuild that fails partway leaves the
// unmoved rows under the lost segment, and the next start rebuilds them,
// ending with every block readable and every journal row under a segment
// that is on disk.
func TestRebuild_ResumesAfterFailure(t *testing.T) {
	ctx := context.Background()
	mem := inmem.NewMemStore()
	if err := mem.Create(ctx, "bk", did.Undef, registry.CreateState{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	lost, blocks := journalSegment(t, mem, "bk", 600)
	dir := t.TempDir()

	if _, err := openRebuildManager(t, dir, &failSecondMove{MemStore: mem}, &shipRecorder{}); err == nil {
		t.Fatal("OpenManager with a failing move succeeded; the test proves nothing")
	}
	if rows := mem.JournaledBlocks(lost); len(rows) == 0 || len(rows) == len(blocks) {
		t.Fatalf("%d of %d rows under the lost segment after the failure, want some moved and some not", len(rows), len(blocks))
	}

	m, err := openRebuildManager(t, dir, mem, &shipRecorder{})
	if err != nil {
		t.Fatalf("OpenManager after the failure: %v", err)
	}
	requireReadable(t, m, blocks)
	if rows := mem.JournaledBlocks(lost); len(rows) != 0 {
		t.Fatalf("%d journal rows still under the lost segment", len(rows))
	}
	onDisk := onDiskSeqs(t, dir, "bk")
	segs, err := mem.JournalSegments(ctx, "bk")
	if err != nil {
		t.Fatalf("JournalSegments: %v", err)
	}
	for _, seq := range segs {
		if !onDisk[seq] {
			t.Fatalf("journal rows under segment %d, which is not on disk (on disk: %v)", seq, onDisk)
		}
	}
}

// TestOpenManager_SeqPastDisk: a Meta restored from behind the disk knows
// none of its segments, so its allocator would hand their seqs out again.
// Opening the manager moves it past every seq on disk, and restores each
// sealed segment's row from its .idx.
func TestOpenManager_SeqPastDisk(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	ahead := inmem.NewMemStore()
	m, err := openRebuildManager(t, dir, ahead, &shipRecorder{})
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	// Each append outgrows SealBytes; wait for its seal so every block lands
	// in a segment of its own.
	var written []block.Block
	for i := range 3 {
		b := block.NewBlock([]byte(fmt.Sprintf("written-%d/%s", i, strings.Repeat("y", 9<<10))))
		if _, err := m.AppendBatch(ctx, []block.Block{b}, blockstore.OpRoot{Bucket: "bk", Root: b.Cid()}); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
		written = append(written, b)
		deadline := time.Now().Add(5 * time.Second)
		for {
			idx, _ := filepath.Glob(filepath.Join(dir, "bk", blockstore.PlaneCatalog.String(), "seg-*.idx"))
			if len(idx) > i {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("append %d never sealed", i)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := m.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var top uint64
	for seq := range onDiskSeqs(t, dir, "bk") {
		top = max(top, seq)
	}
	if top == 0 {
		t.Fatal("no segment on disk; the test proves nothing")
	}

	behind := inmem.NewMemStore()
	m2, err := openRebuildManager(t, dir, behind, &shipRecorder{})
	if err != nil {
		t.Fatalf("OpenManager over a Meta behind the disk: %v", err)
	}
	requireReadable(t, m2, written)
	rows, err := behind.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("%d rows restored, want one per sealed segment on disk (3)", len(rows))
	}
	for _, r := range rows {
		if r.State != logstore.StateSealed || len(r.SHA256) == 0 {
			t.Fatalf("restored row %d is %v (sha %x), want sealed from its .idx", r.Seq, r.State, r.SHA256)
		}
	}
	next, err := behind.NextSegmentSeq(ctx)
	if err != nil {
		t.Fatalf("NextSegmentSeq: %v", err)
	}
	if next <= top {
		t.Fatalf("NextSegmentSeq = %d after opening over segments up to %d", next, top)
	}
}
