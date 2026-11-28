package blockstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/stretchr/testify/require"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/logstore"
)

// In-memory Meta — minimal subset duplicated here to avoid pulling
// the logstore test fake out of its package.
type fakeMeta struct {
	seq   uint64
	rows  map[uint64]*logstore.SegmentMeta
	roots []blockstore.OpRoot
}

func newFakeMeta() *fakeMeta { return &fakeMeta{rows: map[uint64]*logstore.SegmentMeta{}} }

func (f *fakeMeta) NextSegmentSeq(_ context.Context) (uint64, error) { f.seq++; return f.seq, nil }
func (f *fakeMeta) InsertSegmentOpen(_ context.Context, plane blockstore.Plane, seq uint64, bucket string) error {
	f.rows[seq] = &logstore.SegmentMeta{Seq: seq, Plane: plane, Bucket: bucket, State: logstore.StateOpen}
	return nil
}
func (f *fakeMeta) MarkSegmentSealed(_ context.Context, plane blockstore.Plane, seq uint64, sealedAt int64,
	size int64, sha []byte, opRoots []blockstore.OpRoot) error {
	r, ok := f.rows[seq]
	if !ok || r.State != logstore.StateOpen {
		return nil
	}
	r.State = logstore.StateSealed
	r.OpRoots = append([]blockstore.OpRoot(nil), opRoots...)
	f.roots = append(f.roots, opRoots...)
	return nil
}
func (f *fakeMeta) MarkSegmentShipped(_ context.Context, plane blockstore.Plane, seq uint64, shippedAt int64, _ multihash.Multihash, _ int64, _ []blockstore.OpRoot) error {
	if r, ok := f.rows[seq]; ok {
		r.ShippedAt = shippedAt
	}
	return nil
}
func (f *fakeMeta) DeleteSegment(_ context.Context, plane blockstore.Plane, seq uint64) error {
	delete(f.rows, seq)
	return nil
}
func (f *fakeMeta) RetireSegment(_ context.Context, plane blockstore.Plane, seq uint64, retiredAt int64) error {
	if r, ok := f.rows[seq]; ok && r.RetiredAt == 0 {
		r.RetiredAt = retiredAt
	}
	return nil
}
func (f *fakeMeta) ListSegments(_ context.Context, plane blockstore.Plane, bucket string) ([]logstore.SegmentMeta, error) {
	return f.listSegments(plane, bucket, false), nil
}
func (f *fakeMeta) ListRetiredSegments(_ context.Context, plane blockstore.Plane, bucket string) ([]logstore.SegmentMeta, error) {
	return f.listSegments(plane, bucket, true), nil
}
func (f *fakeMeta) listSegments(plane blockstore.Plane, bucket string, retired bool) []logstore.SegmentMeta {
	var out []logstore.SegmentMeta
	for _, r := range f.rows {
		if r.Plane != plane || r.Bucket != bucket || (r.RetiredAt != 0) != retired {
			continue
		}
		out = append(out, *r)
	}
	return out
}

func (f *fakeMeta) ListSegmentBuckets(_ context.Context, plane blockstore.Plane) ([]string, error) {
	seen := map[string]struct{}{}
	var out []string
	for _, r := range f.rows {
		if r.Plane != plane || r.RetiredAt != 0 {
			continue
		}
		if _, ok := seen[r.Bucket]; ok {
			continue
		}
		seen[r.Bucket] = struct{}{}
		out = append(out, r.Bucket)
	}
	return out, nil
}
func (f *fakeMeta) EnsureSegmentSeqAbove(_ context.Context, seq uint64) error {
	f.seq = max(f.seq, seq)
	return nil
}
func (f *fakeMeta) JournalBuckets(context.Context) ([]string, error)          { return nil, nil }
func (f *fakeMeta) JournalSegments(context.Context, string) ([]uint64, error) { return nil, nil }
func (f *fakeMeta) ReadJournal(context.Context, uint64, cid.Cid, int) ([]block.Block, error) {
	return nil, nil
}
func (f *fakeMeta) MoveJournal(context.Context, string, uint64, uint64, cid.Cid, cid.Cid) error {
	return nil
}
func (f *fakeMeta) RehydrateSegment(_ context.Context, m logstore.SegmentMeta) error {
	cp := m
	f.rows[m.Seq] = &cp
	return nil
}

// nopFlush is the per-plane ship callback for these tests: the store
// owns the ship-state transition, so the closure is a no-op.
func nopFlush(_ context.Context, _ *logstore.Segment) (multihash.Multihash, error) { return nil, nil }

// noopBase satisfies blockstore.BlockReader but always returns
// errUnknownBase so we can detect when a GetBlock falls through
// past the log layer.
type noopBase struct{}

var errUnknownBase = errors.New("base: unknown")

func (noopBase) GetBlock(_ context.Context, _ did.DID, _ cid.Cid) (block.Block, error) {
	return nil, errUnknownBase
}

func makeBlock(t *testing.T, payload []byte) block.Block {
	t.Helper()
	mh, err := multihash.Sum(payload, multihash.SHA2_256, -1)
	require.NoError(t, err, "mh")
	c := cid.NewCidV1(cid.Raw, mh)
	blk, err := block.NewBlockWithCid(payload, c)
	require.NoError(t, err, "blk")
	return blk
}

func makeRoot(t *testing.T, name string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte("r:"+name), multihash.SHA2_256, -1)
	require.NoError(t, err, "mh")
	return cid.NewCidV1(cid.DagCBOR, mh)
}

func TestLayeredAndStagingHappyPath(t *testing.T) {
	dir := t.TempDir()
	meta := newFakeMeta()
	logger := zaptest.NewLogger(t)

	log, err := logstore.Open(context.Background(), logstore.Config{
		Dir:     dir,
		Meta:    meta,
		Catalog: logstore.PlaneConfig{SealBytes: 1 << 30, SealAge: time.Hour, Ship: true, Flush: nopFlush, Retain: 6},
		Logger:  logger,
	})
	require.NoError(t, err, "logstore Open")
	t.Cleanup(func() { _ = log.Close(context.Background()) })

	bs := blockstore.NewLayered(nil, log, noopBase{})

	// Stage two blocks for bucket "alpha", commit, then Get them back
	// via the layered store.
	stage := blockstore.NewOpStaging(bs, log, "alpha", did.Undef)
	a := makeBlock(t, []byte("alpha-1"))
	b := makeBlock(t, []byte("alpha-2"))
	for _, blk := range []block.Block{a, b} {
		require.NoError(t, stage.Put(context.Background(), blk), "stage.Put")
	}
	appended, err := stage.Commit(context.Background(), makeRoot(t, "alpha"))
	require.NoError(t, err, "Commit")
	// The commit journals what the log took, under the segment it went into.
	require.NotZero(t, appended.Segment, "Commit reported no segment")
	require.Len(t, appended.Blocks, 2, "Commit reported blocks")
	require.Equal(t, a.Cid(), appended.Blocks[0].Cid(), "Commit reported blocks out of put order")
	require.Equal(t, b.Cid(), appended.Blocks[1].Cid(), "Commit reported blocks out of put order")

	for _, blk := range []block.Block{a, b} {
		got, err := bs.GetBlock(context.Background(), did.Undef, blk.Cid())
		require.NoError(t, err, "layered.Get %s", blk.Cid())
		require.Equal(t, blk.RawData(), got.RawData(), "layered.Get %s", blk.Cid())
	}
}

func TestLayeredFallsThroughToBaseOnMiss(t *testing.T) {
	dir := t.TempDir()
	meta := newFakeMeta()
	logger := zaptest.NewLogger(t)

	log, err := logstore.Open(context.Background(), logstore.Config{
		Dir:     dir,
		Meta:    meta,
		Catalog: logstore.PlaneConfig{SealBytes: 1 << 30, SealAge: time.Hour, Ship: true, Flush: nopFlush, Retain: 6},
		Logger:  logger,
	})
	require.NoError(t, err, "logstore Open")
	t.Cleanup(func() { _ = log.Close(context.Background()) })

	bs := blockstore.NewLayered(nil, log, noopBase{})
	missing := makeBlock(t, []byte("nope")).Cid()
	_, err = bs.GetBlock(context.Background(), did.Undef, missing)
	require.ErrorIs(t, err, errUnknownBase)
}

func TestStagingDiscardLeavesLogUntouched(t *testing.T) {
	dir := t.TempDir()
	meta := newFakeMeta()
	logger := zaptest.NewLogger(t)

	log, err := logstore.Open(context.Background(), logstore.Config{
		Dir:     dir,
		Meta:    meta,
		Catalog: logstore.PlaneConfig{SealBytes: 1 << 30, SealAge: time.Hour, Ship: true, Flush: nopFlush, Retain: 6},
		Logger:  logger,
	})
	require.NoError(t, err, "logstore Open")
	t.Cleanup(func() { _ = log.Close(context.Background()) })

	bs := blockstore.NewLayered(nil, log, noopBase{})
	stage := blockstore.NewOpStaging(bs, log, "alpha", did.Undef)
	blk := makeBlock(t, []byte("never-committed"))
	require.NoError(t, stage.Put(context.Background(), blk), "Put")
	stage.Discard()

	_, err = log.Get(context.Background(), blk.Cid())
	require.ErrorIs(t, err, blockstore.ErrNotFound, "Discard should leave log empty")
}
