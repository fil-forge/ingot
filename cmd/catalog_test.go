package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/cars"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
)

// cborBlock returns a dag-cbor block holding name followed by links.
func cborBlock(t *testing.T, name string, links ...cid.Cid) block.Block {
	t.Helper()
	var buf bytes.Buffer
	cw := cbg.NewCborWriter(&buf)
	if err := cw.WriteMajorTypeHeader(cbg.MajArray, uint64(1+len(links))); err != nil {
		t.Fatal(err)
	}
	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(name))); err != nil {
		t.Fatal(err)
	}
	if _, err := cw.WriteString(name); err != nil {
		t.Fatal(err)
	}
	for _, l := range links {
		if err := cbg.WriteCid(cw, l); err != nil {
			t.Fatal(err)
		}
	}
	mh, err := multihash.Sum(buf.Bytes(), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := block.NewBlockWithCid(buf.Bytes(), cid.NewCidV1(cid.DagCBOR, mh))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestCatalogUsage: one bucket's shipped segment, with a block a later write
// in it superseded, reports its bytes on Forge, the dead-on-arrival bytes
// the ship measured, its file on disk and the blocks its root reaches. A log
// directory whose bucket is gone is left out.
func TestCatalogUsage(t *testing.T) {
	ctx := context.Background()
	mem := inmem.NewMemStore()
	space, err := did.Parse("did:web:catalog-usage.example")
	if err != nil {
		t.Fatal(err)
	}
	if err := mem.Create(ctx, "bk", space, registry.CreateState{}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	segments := t.TempDir()
	index, _ := multihash.Sum([]byte("index"), multihash.SHA2_256, -1)
	s, err := logstore.Open(ctx, logstore.Config{
		Dir:    filepath.Join(segments, "bk"),
		Bucket: "bk",
		Meta:   mem,
		Catalog: logstore.PlaneConfig{SealBytes: 64 << 20, SealAge: 50 * time.Millisecond, Ship: true, Retain: 100,
			Flush: func(context.Context, *logstore.Segment) (multihash.Multihash, error) { return index, nil }},
		Logger: zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(ctx) })
	x := cborBlock(t, "x")
	r1 := cborBlock(t, "r1", x.Cid())
	r2 := cborBlock(t, "r2", x.Cid())
	// One batch, so one segment, whose last root, r2, leaves r1 dead.
	if _, err := s.AppendBatch(ctx, []block.Block{x, r1, r2}, blockstore.OpRoot{Bucket: "bk", Root: r2.Cid()}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := mem.CASRoot(ctx, "bk", cid.Undef, r2.Cid()); err != nil {
		t.Fatalf("CASRoot: %v", err)
	}
	var shipped registry.CatalogUsage
	deadline := time.Now().Add(5 * time.Second)
	for {
		usage, err := mem.CatalogUsage(ctx)
		if err != nil {
			t.Fatalf("CatalogUsage: %v", err)
		}
		if len(usage) == 1 {
			shipped = usage[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the segment never shipped")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.MkdirAll(filepath.Join(segments, "deleted-bucket", "catalog"), 0o755); err != nil {
		t.Fatal(err)
	}

	report, err := catalogUsage(ctx, mem, segments, nil)
	if err != nil {
		t.Fatalf("catalogUsage: %v", err)
	}
	files, _ := filepath.Glob(filepath.Join(segments, "bk", "catalog", "seg-*.car"))
	var local int64
	for _, p := range files {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		local += st.Size()
	}
	want := catalogFigures{
		ForgeBytes:         shipped.Bytes,
		DeadOnArrivalBytes: frame(r1),
		LocalBytes:         local,
		LiveBytes:          frame(x) + frame(r2),
		LiveBlocks:         2,
	}
	if len(report.Buckets) != 1 || report.Buckets[0].Space != space.String() || report.Buckets[0].catalogFigures != want {
		t.Fatalf("buckets = %+v, want one for %s with %+v", report.Buckets, space, want)
	}
	if report.Total != want {
		t.Fatalf("total = %+v, want %+v", report.Total, want)
	}

	// Retire the segment from disk: the walk reads it back through the
	// bucket's segment source and finds the same live blocks.
	var shippedBlocks []block.Block
	for _, p := range files {
		res, err := cars.ScanFile(p)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range res.Frames {
			shippedBlocks = append(shippedBlocks, f.Block)
		}
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	var asked []string
	source := func(bucket string, sp did.DID) logstore.FetchSegment {
		asked = append(asked, bucket+" "+sp.String())
		returned := false
		return func(context.Context, cid.Cid) ([]block.Block, error) {
			if returned {
				return nil, nil
			}
			returned = true
			return shippedBlocks, nil
		}
	}
	report, err = catalogUsage(ctx, mem, segments, source)
	if err != nil {
		t.Fatalf("catalogUsage over a retired segment: %v", err)
	}
	want.LocalBytes = 0
	want.FetchedSegments = 1
	if len(report.Buckets) != 1 || report.Buckets[0].catalogFigures != want {
		t.Fatalf("over a retired segment: buckets = %+v, want one with %+v", report.Buckets, want)
	}
	if len(asked) != 1 || asked[0] != "bk "+space.String() {
		t.Fatalf("segment source asked for %v, want the one bucket and its space", asked)
	}
}

func frame(b block.Block) int64 { return cars.FrameSize(b.Cid(), uint64(len(b.RawData()))) }

// TestCatalogUsageJSON: the report forge-perf reads keeps its shape with no
// bucket at all: an empty list and zero totals, every field present.
func TestCatalogUsageJSON(t *testing.T) {
	report, err := catalogUsage(context.Background(), inmem.NewMemStore(), filepath.Join(t.TempDir(), "segments"), nil)
	if err != nil {
		t.Fatalf("catalogUsage: %v", err)
	}
	got, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"buckets":[],"total":{"forge_bytes":0,"dead_on_arrival_bytes":0,"unmeasured_bytes":0,` +
		`"local_bytes":0,"live_bytes":0,"live_blocks":0,"missing_blocks":0,"fetched_segments":0}}`
	if string(got) != want {
		t.Fatalf("report = %s, want %s", got, want)
	}
}
