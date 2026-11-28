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
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/cars"
	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
)

// cborBlock returns a dag-cbor block holding name followed by links.
func cborBlock(t *testing.T, name string, links ...cid.Cid) block.Block {
	t.Helper()
	var buf bytes.Buffer
	cw := cbg.NewCborWriter(&buf)
	require.NoError(t, cw.WriteMajorTypeHeader(cbg.MajArray, uint64(1+len(links))))
	require.NoError(t, cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(name))))
	_, err := cw.WriteString(name)
	require.NoError(t, err)
	for _, l := range links {
		require.NoError(t, cbg.WriteCid(cw, l))
	}
	mh, err := multihash.Sum(buf.Bytes(), multihash.SHA2_256, -1)
	require.NoError(t, err)
	b, err := block.NewBlockWithCid(buf.Bytes(), cid.NewCidV1(cid.DagCBOR, mh))
	require.NoError(t, err)
	return b
}

// TestCatalogUsage: one bucket's shipped segment, with a block a later write
// in it superseded, reports its bytes on Forge, the dead-on-arrival bytes
// the ship measured, its file on disk and the blocks its root reaches. A log
// directory whose bucket is gone is left out.
func TestCatalogUsage(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	mem := registry.NewPostgres(pool)
	space, err := did.Parse("did:web:catalog-usage.example")
	require.NoError(t, err)
	require.NoError(t, mem.Create(ctx, "bk", space, registry.CreateState{Tenant: space}), "Create")

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
	require.NoError(t, err, "Open")
	t.Cleanup(func() { _ = s.Close(ctx) })
	x := cborBlock(t, "x")
	r1 := cborBlock(t, "r1", x.Cid())
	r2 := cborBlock(t, "r2", x.Cid())
	// One batch, so one segment, whose last root, r2, leaves r1 dead.
	_, err = s.AppendBatch(ctx, []block.Block{x, r1, r2}, blockstore.OpRoot{Bucket: "bk", Root: r2.Cid()})
	require.NoError(t, err, "append")
	// The bucket's root, as a commit of r2 leaves it.
	_, err = pool.Exec(ctx, `UPDATE ingot.buckets SET root_cid = $1 WHERE name = 'bk'`, r2.Cid().Bytes())
	require.NoError(t, err, "set root")
	var shipped registry.CatalogUsage
	deadline := time.Now().Add(5 * time.Second)
	for {
		// The segment seals on age; a ship runs it, as queue-bucket.ship would.
		_ = s.Ship(ctx)
		usage, err := mem.CatalogUsage(ctx)
		require.NoError(t, err, "CatalogUsage")
		if len(usage) == 1 {
			shipped = usage[0]
			break
		}
		require.False(t, time.Now().After(deadline), "the segment never shipped")
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, os.MkdirAll(filepath.Join(segments, "deleted-bucket", "catalog"), 0o755))

	report, err := catalogUsage(ctx, mem, segments, nil)
	require.NoError(t, err, "catalogUsage")
	files, _ := filepath.Glob(filepath.Join(segments, "bk", "catalog", "seg-*.car"))
	var local int64
	for _, p := range files {
		st, err := os.Stat(p)
		require.NoError(t, err)
		local += st.Size()
	}
	want := catalogFigures{
		ForgeBytes:         shipped.Bytes,
		DeadOnArrivalBytes: frame(r1),
		LocalBytes:         local,
		LiveBytes:          frame(x) + frame(r2),
		LiveBlocks:         2,
	}
	require.Len(t, report.Buckets, 1, "buckets")
	require.Equal(t, space.String(), report.Buckets[0].Space, "bucket space")
	require.Equal(t, want, report.Buckets[0].catalogFigures, "bucket figures")
	require.Equal(t, want, report.Total, "total")

	// Retire the segment from disk: the walk reads it back through the
	// bucket's segment source and finds the same live blocks.
	var shippedBlocks []block.Block
	for _, p := range files {
		res, err := cars.ScanFile(p)
		require.NoError(t, err)
		for _, f := range res.Frames {
			shippedBlocks = append(shippedBlocks, f.Block)
		}
		require.NoError(t, os.Remove(p))
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
	require.NoError(t, err, "catalogUsage over a retired segment")
	want.LocalBytes = 0
	want.FetchedSegments = 1
	require.Len(t, report.Buckets, 1, "over a retired segment: buckets")
	require.Equal(t, want, report.Buckets[0].catalogFigures, "over a retired segment: bucket figures")
	require.Len(t, asked, 1, "segment source asked for %v, want the one bucket and its space", asked)
	require.Equal(t, "bk "+space.String(), asked[0], "segment source asked for %v, want the one bucket and its space", asked)
}

func frame(b block.Block) int64 { return cars.FrameSize(b.Cid(), uint64(len(b.RawData()))) }

// TestCatalogUsageJSON: the report forge-perf reads keeps its shape with no
// bucket at all: an empty list and zero totals, every field present.
func TestCatalogUsageJSON(t *testing.T) {
	report, err := catalogUsage(context.Background(), registry.NewPostgres(pgtest.New(t)), filepath.Join(t.TempDir(), "segments"), nil)
	require.NoError(t, err, "catalogUsage")
	got, err := json.Marshal(report)
	require.NoError(t, err)
	want := `{"buckets":[],"total":{"forge_bytes":0,"dead_on_arrival_bytes":0,"unmeasured_bytes":0,` +
		`"local_bytes":0,"live_bytes":0,"live_blocks":0,"missing_blocks":0,"fetched_segments":0}}`
	require.Equal(t, want, string(got), "report")
}
