package blockstore_test

import (
	"context"
	"errors"
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
func (f *fakeMeta) InsertSegmentOpen(_ context.Context, seq uint64) error {
	f.rows[seq] = &logstore.SegmentMeta{Seq: seq, State: logstore.StateOpen}
	return nil
}
func (f *fakeMeta) MarkSegmentSealed(_ context.Context, seq uint64, sealedAt int64,
	dataSize int64, dataSHA []byte, catSize int64, catSHA []byte,
	opRoots []blockstore.OpRoot) error {
	r, ok := f.rows[seq]
	if !ok || r.State != logstore.StateOpen {
		return nil
	}
	r.State = logstore.StateSealed
	r.OpRoots = append([]blockstore.OpRoot(nil), opRoots...)
	f.roots = append(f.roots, opRoots...)
	return nil
}
func (f *fakeMeta) MarkSegmentShipped(_ context.Context, seq uint64, plane blockstore.Plane, shippedAt int64, _ []blockstore.OpRoot) error {
	if r, ok := f.rows[seq]; ok {
		if plane == blockstore.PlaneData {
			r.DataShippedAt = shippedAt
		} else {
			r.CatShippedAt = shippedAt
		}
	}
	return nil
}
func (f *fakeMeta) DeleteSegment(_ context.Context, seq uint64) error {
	delete(f.rows, seq)
	return nil
}
func (f *fakeMeta) ListSegments(_ context.Context) ([]logstore.SegmentMeta, error) {
	var out []logstore.SegmentMeta
	for _, r := range f.rows {
		out = append(out, *r)
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

func (noopBase) GetBlock(_ context.Context, _ cid.Cid) (block.Block, error) {
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
		Dir:       dir,
		Meta:      meta,
		SealBytes: 1 << 30,
		SealAge:   1 * time.Hour,
		Data:      logstore.PlaneConfig{Ship: true, Flush: nopFlush, Retain: 6},
		Catalog:   logstore.PlaneConfig{Ship: true, Flush: nopFlush, Retain: 6},
		Logger:    logger,
	})
	if err != nil {
		t.Fatalf("logstore Open: %v", err)
	}
	t.Cleanup(func() { _ = log.Close(context.Background()) })

	bs := blockstore.NewLayered(log, noopBase{})

	// Stage two blocks for bucket "alpha", commit, then Get them back
	// via the layered store.
	stage := blockstore.NewOpStaging(bs, log, "alpha")
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
		got, err := bs.GetBlock(context.Background(), blk.Cid())
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
		Dir:       dir,
		Meta:      meta,
		SealBytes: 1 << 30,
		SealAge:   1 * time.Hour,
		Data:      logstore.PlaneConfig{Ship: true, Flush: nopFlush, Retain: 6},
		Catalog:   logstore.PlaneConfig{Ship: true, Flush: nopFlush, Retain: 6},
		Logger:    logger,
	})
	if err != nil {
		t.Fatalf("logstore Open: %v", err)
	}
	t.Cleanup(func() { _ = log.Close(context.Background()) })

	bs := blockstore.NewLayered(log, noopBase{})
	missing := makeBlock(t, []byte("nope")).Cid()
	_, err = bs.GetBlock(context.Background(), missing)
	if !errors.Is(err, errUnknownBase) {
		t.Fatalf("expected base sentinel, got %v", err)
	}
}

func TestStagingDiscardLeavesLogUntouched(t *testing.T) {
	dir := t.TempDir()
	meta := newFakeMeta()
	logger := zaptest.NewLogger(t)

	log, err := logstore.Open(context.Background(), logstore.Config{
		Dir:       dir,
		Meta:      meta,
		SealBytes: 1 << 30,
		SealAge:   1 * time.Hour,
		Data:      logstore.PlaneConfig{Ship: true, Flush: nopFlush, Retain: 6},
		Catalog:   logstore.PlaneConfig{Ship: true, Flush: nopFlush, Retain: 6},
		Logger:    logger,
	})
	if err != nil {
		t.Fatalf("logstore Open: %v", err)
	}
	t.Cleanup(func() { _ = log.Close(context.Background()) })

	bs := blockstore.NewLayered(log, noopBase{})
	stage := blockstore.NewOpStaging(bs, log, "alpha")
	blk := makeBlock(t, []byte("never-committed"))
	if err := stage.Put(context.Background(), blk); err != nil {
		t.Fatalf("Put: %v", err)
	}
	stage.Discard()

	if _, err := log.Get(context.Background(), blk.Cid()); !errors.Is(err, blockstore.ErrNotFound) {
		t.Fatalf("Discard should leave log empty, got %v", err)
	}
}
