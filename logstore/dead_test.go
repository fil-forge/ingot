package logstore

import (
	"bytes"
	"context"
	"testing"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/cars"
)

// linkBlock returns a dag-cbor block holding name followed by links, so a
// walk finds exactly links in it.
func linkBlock(t *testing.T, name string, links ...cid.Cid) block.Block {
	t.Helper()
	var buf bytes.Buffer
	cw := cbg.NewCborWriter(&buf)
	if err := cw.WriteMajorTypeHeader(cbg.MajArray, uint64(1+len(links))); err != nil {
		t.Fatalf("array header: %v", err)
	}
	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(name))); err != nil {
		t.Fatalf("name header: %v", err)
	}
	if _, err := cw.WriteString(name); err != nil {
		t.Fatalf("name: %v", err)
	}
	for _, l := range links {
		if err := cbg.WriteCid(cw, l); err != nil {
			t.Fatalf("link: %v", err)
		}
	}
	return makeBlock(t, buf.Bytes())
}

func frameSize(blks ...block.Block) int64 {
	var n int64
	for _, b := range blks {
		n += cars.FrameSize(b.Cid(), uint64(len(b.RawData())))
	}
	return n
}

// shippedDeadBytes appends each batch to a fresh store, seals them into one
// segment, which then ships, and returns the dead bytes the ship recorded.
func shippedDeadBytes(t *testing.T, batches [][]block.Block, roots []cid.Cid) int64 {
	t.Helper()
	// Neither seal trigger fires on its own: the seal below is the only one.
	s, meta, _ := newTestStore(t, 64<<20, time.Hour, 6)
	ctx := context.Background()
	for i, blks := range batches {
		if _, err := s.AppendBatch(ctx, blks, blockstore.OpRoot{Bucket: "bk", Root: roots[i]}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := s.catalog.sealOpenIfDue(ctx, true); err != nil {
		t.Fatalf("seal: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		meta.mu.Lock()
		var shipped []SegmentMeta
		for _, m := range meta.segments {
			if m.ShippedAt != 0 {
				shipped = append(shipped, *m)
			}
		}
		meta.mu.Unlock()
		if len(shipped) > 1 {
			t.Fatalf("batches landed in %d segments, want 1", len(shipped))
		}
		if len(shipped) == 1 {
			return shipped[0].DeadBytes
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("segment never shipped")
	return 0
}

// TestShipRecordsDeadOnArrivalBytes: blocks that later writes in the same
// segment stopped reaching count as dead; blocks the segment's last root
// reaches, directly or through other blocks of the segment, don't. A link
// to a block outside the segment is not followed.
func TestShipRecordsDeadOnArrivalBytes(t *testing.T) {
	older := makeRoot(t, "in an older segment")
	a := linkBlock(t, "a")
	r1 := linkBlock(t, "r1", a.Cid())
	b := linkBlock(t, "b")
	r2 := linkBlock(t, "r2", a.Cid(), b.Cid())
	c := linkBlock(t, "c", older)
	r3 := linkBlock(t, "r3", b.Cid(), c.Cid())

	got := shippedDeadBytes(t,
		[][]block.Block{{a, r1}, {b, r2}, {c, r3}},
		[]cid.Cid{r1.Cid(), r2.Cid(), r3.Cid()})
	if want := frameSize(a, r1, r2); got != want {
		t.Fatalf("dead bytes = %d, want %d (a, r1 and r2)", got, want)
	}
}

// TestShipRecordsDeadOnArrivalWhenRootIsOlder: a segment whose last root is
// a block of an older segment (as when a delete unwraps the tree's top to an
// existing subtree) reaches none of its own blocks.
func TestShipRecordsDeadOnArrivalWhenRootIsOlder(t *testing.T) {
	older := makeRoot(t, "in an older segment")
	a := linkBlock(t, "a", older)
	r1 := linkBlock(t, "r1", a.Cid())

	got := shippedDeadBytes(t,
		[][]block.Block{{a, r1}, nil},
		[]cid.Cid{r1.Cid(), older})
	if want := frameSize(a, r1); got != want {
		t.Fatalf("dead bytes = %d, want %d (every block)", got, want)
	}
}

// TestShipRecordsUnmeasurableSegment: a segment the walk cannot measure (its
// root is not well-formed CBOR) still ships, recorded as unmeasured.
func TestShipRecordsUnmeasurableSegment(t *testing.T) {
	// An array header promising five items, and nothing after it.
	bad := makeBlock(t, []byte{0x85})

	if got := shippedDeadBytes(t, [][]block.Block{{bad}}, []cid.Cid{bad.Cid()}); got != -1 {
		t.Fatalf("dead bytes = %d, want -1 (unmeasured)", got)
	}
}

// TestDeadOnArrivalWithoutOpRoots: a segment with no op-root has no root to
// walk from. Empty, it has nothing dead; holding blocks (as a segment rebuilt
// from the catalog journal does), it goes unmeasured rather than all dead.
func TestDeadOnArrivalWithoutOpRoots(t *testing.T) {
	ctx := context.Background()
	meta := newFakeMeta()
	dir := t.TempDir()

	empty, err := createOpenSegment(ctx, dir, 1, blockstore.PlaneCatalog, "bk", meta, zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	if err := empty.seal(ctx, meta); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if got := empty.deadOnArrival(); got != 0 {
		t.Fatalf("empty segment: dead bytes = %d, want 0", got)
	}

	rootless, err := createOpenSegment(ctx, dir, 2, blockstore.PlaneCatalog, "bk", meta, zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	if err := rootless.append([]block.Block{linkBlock(t, "a"), linkBlock(t, "b")}, nil); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := rootless.seal(ctx, meta); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if got := rootless.deadOnArrival(); got != -1 {
		t.Fatalf("segment with blocks and no op-root: dead bytes = %d, want -1 (unmeasured)", got)
	}
}

// TestDeadOnArrivalMeasuredOnce: a segment is measured at its first ship
// attempt and the figure kept, so a later attempt (the retry sweep's, after
// the first ones failed) reuses it rather than reading the segment again.
// Retiring the segment between the two shows which happened: a second
// measurement would find no files and come out unmeasured.
func TestDeadOnArrivalMeasuredOnce(t *testing.T) {
	ctx := context.Background()
	meta := newFakeMeta()
	seg, err := createOpenSegment(ctx, t.TempDir(), 1, blockstore.PlaneCatalog, "bk", meta, zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	a := linkBlock(t, "a")
	r1 := linkBlock(t, "r1")
	if err := seg.append([]block.Block{a, r1}, []blockstore.OpRoot{{Bucket: "bk", Root: r1.Cid()}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := seg.seal(ctx, meta); err != nil {
		t.Fatalf("seal: %v", err)
	}

	want := frameSize(a)
	if got := seg.deadOnArrival(); got != want {
		t.Fatalf("first measurement = %d, want %d (a)", got, want)
	}
	if err := seg.retire(); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if got := seg.deadOnArrival(); got != want {
		t.Fatalf("second call = %d, want the first measurement %d", got, want)
	}
}
