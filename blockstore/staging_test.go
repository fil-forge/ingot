package blockstore_test

import (
	"context"
	"errors"
	"github.com/fil-forge/ucantone/did"
	"testing"
	"time"

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
func (f *fakeMeta) MarkSegmentShipped(_ context.Context, plane blockstore.Plane, seq uint64, shippedAt int64, _ []blockstore.OpRoot) error {
	if r, ok := f.rows[seq]; ok {
		r.ShippedAt = shippedAt
	}
	return nil
}
func (f *fakeMeta) DeleteSegment(_ context.Context, plane blockstore.Plane, seq uint64) error {
	delete(f.rows, seq)
	return nil
}
func (f *fakeMeta) ListSegments(_ context.Context, plane blockstore.Plane, bucket string) ([]logstore.SegmentMeta, error) {
	var out []logstore.SegmentMeta
	for _, r := range f.rows {
		if r.Plane != plane || r.Bucket != bucket {
			continue
		}
		out = append(out, *r)
	}
	return out, nil
}

func (f *fakeMeta) ListSegmentBuckets(_ context.Context, plane blockstore.Plane) ([]string, error) {
	seen := map[string]struct{}{}
	var out []string
	for _, r := range f.rows {
		if r.Plane != plane {
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
func (f *fakeMeta) RehydrateSegment(_ context.Context, m logstore.SegmentMeta) error {
	cp := m
	f.rows[m.Seq] = &cp
	return nil
}

// nopFlush is the per-plane ship callback for these tests: the store
// owns the ship-state transition, so the closure is a no-op.
func nopFlush(_ context.Context, _ *logstore.Segment) error { return nil }

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
	if err != nil {
		t.Fatalf("mh: %v", err)
	}
	c := cid.NewCidV1(cid.Raw, mh)
	blk, err := block.NewBlockWithCid(payload, c)
	if err != nil {
		t.Fatalf("blk: %v", err)
	}
	return blk
}

func makeRoot(t *testing.T, name string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte("r:"+name), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("mh: %v", err)
	}
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
	if err != nil {
		t.Fatalf("logstore Open: %v", err)
	}
	t.Cleanup(func() { _ = log.Close(context.Background()) })

	bs := blockstore.NewLayered(nil, log, noopBase{})

	// Stage two blocks for bucket "alpha", commit, then Get them back
	// via the layered store.
	stage := blockstore.NewOpStaging(bs, log, "alpha", did.Undef)
	a := makeBlock(t, []byte("alpha-1"))
	b := makeBlock(t, []byte("alpha-2"))
	for _, blk := range []block.Block{a, b} {
		if err := stage.Put(context.Background(), blk); err != nil {
			t.Fatalf("stage.Put: %v", err)
		}
	}
	if err := stage.Commit(context.Background(), makeRoot(t, "alpha")); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	for _, blk := range []block.Block{a, b} {
		got, err := bs.GetBlock(context.Background(), did.Undef, blk.Cid())
		if err != nil {
			t.Fatalf("layered.Get %s: %v", blk.Cid(), err)
		}
		if string(got.RawData()) != string(blk.RawData()) {
			t.Fatalf("layered.Get %s mismatch: got %q want %q", blk.Cid(), got.RawData(), blk.RawData())
		}
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
	if err != nil {
		t.Fatalf("logstore Open: %v", err)
	}
	t.Cleanup(func() { _ = log.Close(context.Background()) })

	bs := blockstore.NewLayered(nil, log, noopBase{})
	missing := makeBlock(t, []byte("nope")).Cid()
	_, err = bs.GetBlock(context.Background(), did.Undef, missing)
	if !errors.Is(err, errUnknownBase) {
		t.Fatalf("expected base sentinel, got %v", err)
	}
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
	if err != nil {
		t.Fatalf("logstore Open: %v", err)
	}
	t.Cleanup(func() { _ = log.Close(context.Background()) })

	bs := blockstore.NewLayered(nil, log, noopBase{})
	stage := blockstore.NewOpStaging(bs, log, "alpha", did.Undef)
	blk := makeBlock(t, []byte("never-committed"))
	if err := stage.Put(context.Background(), blk); err != nil {
		t.Fatalf("Put: %v", err)
	}
	stage.Discard()

	if _, err := log.Get(context.Background(), blk.Cid()); !errors.Is(err, blockstore.ErrNotFound) {
		t.Fatalf("Discard should leave log empty, got %v", err)
	}
}
