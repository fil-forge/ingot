package blockstore

import (
	"context"
	"testing"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// countingTier is a read tier holding one block, which counts its lookups.
type countingTier struct {
	held    block.Block
	lookups int
}

func (c *countingTier) GetBlock(_ context.Context, _ did.DID, k cid.Cid) (block.Block, error) {
	c.lookups++
	return c.Get(context.Background(), k)
}

func (c *countingTier) Get(_ context.Context, k cid.Cid) (block.Block, error) {
	if c.held == nil || !c.held.Cid().Equals(k) {
		return nil, ErrNotFound
	}
	return c.held, nil
}

func (c *countingTier) AppendBatch(context.Context, []block.Block, OpRoot) (uint64, error) {
	return 0, nil
}

func (c *countingTier) Close(context.Context) error { return nil }

func TestLayered_GetBlock_ReadsABlockFromTheLogWithoutALocalLookup(t *testing.T) {
	data := []byte("any-catalog-block")
	catalog, err := block.NewBlockWithCid(data, cid.NewCidV1(cid.DagCBOR, mustDigest(t, data)))
	if err != nil {
		t.Fatalf("block: %v", err)
	}
	local := &countingTier{}
	l := NewLayered(local, &countingTier{held: catalog}, &countingTier{})

	got, err := l.GetBlock(context.Background(), did.Undef, catalog.Cid())
	if err != nil {
		t.Fatalf("GetBlock: %v", err)
	}
	if string(got.RawData()) != "any-catalog-block" || local.lookups != 0 {
		t.Fatalf("got %q after %d local lookups, want the log's block after 0", got.RawData(), local.lookups)
	}
}
