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

func mustBlock(t *testing.T, codec uint64, data []byte) block.Block {
	t.Helper()
	b, err := block.NewBlockWithCid(data, cid.NewCidV1(codec, mustDigest(t, data)))
	if err != nil {
		t.Fatalf("block: %v", err)
	}
	return b
}

func TestLayered_GetBlock_ReadsACatalogBlockFromTheLogWithoutALocalLookup(t *testing.T) {
	catalog := mustBlock(t, cid.DagCBOR, []byte("any-catalog-block"))
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

func TestLayered_GetBlock_ReadsABodyBlockFromTheLocalCopies(t *testing.T) {
	body := mustBlock(t, cid.Raw, []byte("any-body-block"))
	base := &countingTier{}
	l := NewLayered(&countingTier{held: body}, &countingTier{}, base)

	got, err := l.GetBlock(context.Background(), did.Undef, body.Cid())
	if err != nil {
		t.Fatalf("GetBlock: %v", err)
	}
	if string(got.RawData()) != "any-body-block" || base.lookups != 0 {
		t.Fatalf("got %q after %d base lookups, want the local copy after 0", got.RawData(), base.lookups)
	}
}
