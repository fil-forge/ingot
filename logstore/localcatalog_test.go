package logstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
)

// carBytes sums the size of every catalog CAR of bucket under dir.
func carBytes(t *testing.T, dir, bucket string) int64 {
	t.Helper()
	cars, err := filepath.Glob(filepath.Join(dir, bucket, blockstore.PlaneCatalog.String(), "seg-*.car"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	var n int64
	for _, p := range cars {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		n += st.Size()
	}
	return n
}

// TestMeasureLocalCatalog: the walk follows the root's links across
// segments, counts a block reached by two paths once, leaves out blocks
// later writes superseded, counts a missing catalog block but not a link of
// another codec, and tolerates a torn trailing frame and a segment whose
// header isn't whole yet.
func TestMeasureLocalCatalog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(ctx, Config{
		Dir:     filepath.Join(dir, "bk"),
		Bucket:  "bk",
		Meta:    newFakeMeta(),
		Catalog: PlaneConfig{SealBytes: 64 << 20, SealAge: time.Hour, Retain: 6},
		Logger:  zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	mh, err := multihash.Sum([]byte("an object body"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	body := cid.NewCidV1(cid.Raw, mh)
	absent := makeRoot(t, "a catalog block no segment holds")

	a := linkBlock(t, "a", body)
	r1 := linkBlock(t, "r1", a.Cid())
	if _, err := s.AppendBatch(ctx, []block.Block{a, r1}, blockstore.OpRoot{Bucket: "bk", Root: r1.Cid()}); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if err := s.catalog.sealOpenIfDue(ctx, true); err != nil {
		t.Fatalf("seal: %v", err)
	}
	b := linkBlock(t, "b", a.Cid()) // r2 reaches a directly and through b
	r2 := linkBlock(t, "r2", a.Cid(), b.Cid(), absent)
	if _, err := s.AppendBatch(ctx, []block.Block{b, r2}, blockstore.OpRoot{Bucket: "bk", Root: r2.Cid()}); err != nil {
		t.Fatalf("append 2: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	want := LocalCatalog{
		FileBytes:     carBytes(t, dir, "bk"),
		LiveBytes:     frameSize(a, b, r2),
		LiveBlocks:    3,
		MissingBlocks: 1,
	}
	got, err := MeasureLocalCatalog(dir, "bk", r2.Cid())
	if err != nil {
		t.Fatalf("MeasureLocalCatalog: %v", err)
	}
	if got != want {
		t.Fatalf("measured %+v, want %+v (a, b and r2 live; r1 superseded; absent missing; the body link not counted)", got, want)
	}

	// Half a frame appended to the newest CAR, as a write in progress would
	// leave it, changes nothing but the file size.
	cars, _ := filepath.Glob(filepath.Join(dir, "bk", blockstore.PlaneCatalog.String(), "seg-*.car"))
	f, err := os.OpenFile(cars[len(cars)-1], os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open CAR: %v", err)
	}
	if _, err := f.Write([]byte{0x40, 0x01, 0x71}); err != nil {
		t.Fatalf("append torn frame: %v", err)
	}
	_ = f.Close()
	want.FileBytes += 3
	if got, err = MeasureLocalCatalog(dir, "bk", r2.Cid()); err != nil || got != want {
		t.Fatalf("with a torn tail: %+v, %v; want %+v", got, err, want)
	}

	// A segment just created, empty or with half its header written,
	// counts its size and holds no blocks.
	planeDir := filepath.Join(dir, "bk", blockstore.PlaneCatalog.String())
	for i, data := range [][]byte{nil, {0x3a}} {
		if err := os.WriteFile(filepath.Join(planeDir, fmt.Sprintf("seg-%020d.car", 100+i)), data, 0o644); err != nil {
			t.Fatalf("write a new segment: %v", err)
		}
		want.FileBytes += int64(len(data))
	}
	if got, err = MeasureLocalCatalog(dir, "bk", r2.Cid()); err != nil || got != want {
		t.Fatalf("with segments just created: %+v, %v; want %+v", got, err, want)
	}

	if got, err := MeasureLocalCatalog(dir, "bk", cid.Undef); err != nil || got.LiveBlocks != 0 || got.FileBytes != want.FileBytes {
		t.Fatalf("an empty bucket's root: %+v, %v; want no live blocks over the same files", got, err)
	}
	if got, err := MeasureLocalCatalog(dir, "gone", r2.Cid()); err != nil || got != (LocalCatalog{MissingBlocks: 1}) {
		t.Fatalf("a bucket with no log on disk: %+v, %v; want only its root missing", got, err)
	}
}
