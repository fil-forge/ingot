package logstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/cars"
)

// carBytes sums the size of every catalog CAR of bucket under dir.
func carBytes(t *testing.T, dir, bucket string) int64 {
	t.Helper()
	cars, err := filepath.Glob(filepath.Join(dir, bucket, blockstore.PlaneCatalog.String(), "seg-*.car"))
	require.NoError(t, err, "glob")
	var n int64
	for _, p := range cars {
		st, err := os.Stat(p)
		require.NoError(t, err, "stat %s", p)
		n += st.Size()
	}
	return n
}

// TestWalkCatalog: the walk follows the root's links across
// segments, counts a block reached by two paths once, leaves out blocks
// later writes superseded, counts a missing catalog block but not a link of
// another codec, and tolerates a torn trailing frame and a segment whose
// header isn't whole yet.
func TestWalkCatalog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(ctx, Config{
		Dir:     filepath.Join(dir, "bk"),
		Bucket:  "bk",
		Meta:    newFakeMeta(),
		Catalog: PlaneConfig{SealBytes: 64 << 20, SealAge: time.Hour, Retain: 6},
		Logger:  zaptest.NewLogger(t),
	})
	require.NoError(t, err, "Open")

	mh, err := multihash.Sum([]byte("an object body"), multihash.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	body := cid.NewCidV1(cid.Raw, mh)
	absent := makeRoot(t, "a catalog block no segment holds")

	a := linkBlock(t, "a", body)
	r1 := linkBlock(t, "r1", a.Cid())
	_, err = s.AppendBatch(ctx, []block.Block{a, r1}, blockstore.OpRoot{Bucket: "bk", Root: r1.Cid()})
	require.NoError(t, err, "append 1")
	require.NoError(t, s.catalog.sealOpenIfDue(ctx, true), "seal")
	b := linkBlock(t, "b", a.Cid()) // r2 reaches a directly and through b
	r2 := linkBlock(t, "r2", a.Cid(), b.Cid(), absent)
	_, err = s.AppendBatch(ctx, []block.Block{b, r2}, blockstore.OpRoot{Bucket: "bk", Root: r2.Cid()})
	require.NoError(t, err, "append 2")
	require.NoError(t, s.Close(ctx), "Close")

	want := CatalogWalk{
		FileBytes:     carBytes(t, dir, "bk"),
		LiveBytes:     frameSize(a, b, r2),
		LiveBlocks:    3,
		MissingBlocks: 1,
	}
	got, err := WalkCatalog(ctx, dir, "bk", r2.Cid(), nil)
	require.NoError(t, err, "WalkCatalog")
	require.Equal(t, want, got, "a, b and r2 live; r1 superseded; absent missing; the body link not counted")

	// Half a frame appended to the newest CAR, as a write in progress would
	// leave it, changes nothing but the file size.
	cars, _ := filepath.Glob(filepath.Join(dir, "bk", blockstore.PlaneCatalog.String(), "seg-*.car"))
	f, err := os.OpenFile(cars[len(cars)-1], os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err, "open CAR")
	_, err = f.Write([]byte{0x40, 0x01, 0x71})
	require.NoError(t, err, "append torn frame")
	_ = f.Close()
	want.FileBytes += 3
	got, err = WalkCatalog(ctx, dir, "bk", r2.Cid(), nil)
	require.NoError(t, err, "with a torn tail")
	require.Equal(t, want, got, "with a torn tail")

	// A segment just created, empty or with half its header written,
	// counts its size and holds no blocks.
	planeDir := filepath.Join(dir, "bk", blockstore.PlaneCatalog.String())
	for i, data := range [][]byte{nil, {0x3a}} {
		require.NoError(t, os.WriteFile(filepath.Join(planeDir, fmt.Sprintf("seg-%020d.car", 100+i)), data, 0o644), "write a new segment")
		want.FileBytes += int64(len(data))
	}
	got, err = WalkCatalog(ctx, dir, "bk", r2.Cid(), nil)
	require.NoError(t, err, "with segments just created")
	require.Equal(t, want, got, "with segments just created")

	got, err = WalkCatalog(ctx, dir, "bk", cid.Undef, nil)
	require.NoError(t, err, "an empty bucket's root")
	require.Zero(t, got.LiveBlocks, "an empty bucket's root")
	require.Equal(t, want.FileBytes, got.FileBytes, "an empty bucket's root")
	got, err = WalkCatalog(ctx, dir, "gone", r2.Cid(), nil)
	require.NoError(t, err, "a bucket with no log on disk")
	require.Equal(t, CatalogWalk{MissingBlocks: 1}, got, "a bucket with no log on disk: want only its root missing")
}

// TestWalkCatalogFetch: a block no segment on disk holds is read with the
// rest of its shipped segment through fetch, once per segment, and counts as
// live; a block fetch cannot find stays missing, links of another codec are
// never fetched, and a fetch failure fails the walk.
func TestWalkCatalogFetch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(ctx, Config{
		Dir:     filepath.Join(dir, "bk"),
		Bucket:  "bk",
		Meta:    newFakeMeta(),
		Catalog: PlaneConfig{SealBytes: 64 << 20, SealAge: time.Hour, Retain: 6},
		Logger:  zaptest.NewLogger(t),
	})
	require.NoError(t, err, "Open")
	mh, err := multihash.Sum([]byte("an object body"), multihash.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	body := cid.NewCidV1(cid.Raw, mh)
	absent := makeRoot(t, "a catalog block no segment holds")

	a := linkBlock(t, "a", body)
	c := linkBlock(t, "c", a.Cid())
	r1 := linkBlock(t, "r1", a.Cid(), c.Cid())
	_, err = s.AppendBatch(ctx, []block.Block{a, c, r1}, blockstore.OpRoot{Bucket: "bk", Root: r1.Cid()})
	require.NoError(t, err, "append 1")
	require.NoError(t, s.catalog.sealOpenIfDue(ctx, true), "seal")
	r2 := linkBlock(t, "r2", a.Cid(), c.Cid(), absent)
	_, err = s.AppendBatch(ctx, []block.Block{r2}, blockstore.OpRoot{Bucket: "bk", Root: r2.Cid()})
	require.NoError(t, err, "append 2")
	require.NoError(t, s.Close(ctx), "Close")

	// Retire the first segment from disk, keeping its blocks as Forge would.
	segs, _ := filepath.Glob(filepath.Join(dir, "bk", blockstore.PlaneCatalog.String(), "seg-*.car"))
	require.Len(t, segs, 2, "segments on disk")
	res, err := segmentBlocks(segs[0])
	require.NoError(t, err, "scan the first segment")
	require.NoError(t, os.Remove(segs[0]), "retire")

	var asked []cid.Cid
	returned := false
	fetch := func(_ context.Context, k cid.Cid) ([]block.Block, error) {
		asked = append(asked, k)
		if k == absent || returned {
			return nil, nil
		}
		returned = true
		return res, nil
	}
	want := CatalogWalk{
		FileBytes:       carBytes(t, dir, "bk"),
		LiveBytes:       frameSize(a, c, r2),
		LiveBlocks:      3,
		MissingBlocks:   1,
		FetchedSegments: 1,
	}
	got, err := WalkCatalog(ctx, dir, "bk", r2.Cid(), fetch)
	require.NoError(t, err, "WalkCatalog")
	require.Equal(t, want, got, "walked")
	// Two asks: the absent block, and whichever of a and c the walk met
	// first, whose segment brought the other along.
	require.Len(t, asked, 2, "fetch asked for %v, want the absent block and one of a and c", asked)
	require.NotEqual(t, asked[0], asked[1], "fetch asked for %v, want the absent block and one of a and c", asked)
	require.Contains(t, asked, absent, "fetch asked for %v, want the absent block and one of a and c", asked)
	for _, k := range asked {
		require.Equal(t, uint64(cid.DagCBOR), k.Prefix().Codec, "fetch asked for %s, a link of another codec", k)
	}

	failed := errors.New("forge is down")
	_, err = WalkCatalog(ctx, dir, "bk", r2.Cid(), func(context.Context, cid.Cid) ([]block.Block, error) {
		return nil, failed
	})
	require.ErrorIs(t, err, failed, "a failed fetch")
}

// segmentBlocks reads a segment file's blocks.
func segmentBlocks(path string) ([]block.Block, error) {
	res, err := cars.ScanFile(path)
	if err != nil {
		return nil, err
	}
	blocks := make([]block.Block, len(res.Frames))
	for i, f := range res.Frames {
		blocks[i] = f.Block
	}
	return blocks, nil
}
