package inmem

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/libforge/testutil"
)

func testCid(t *testing.T, s string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return cid.NewCidV1(cid.DagCBOR, mh)
}

// TestMarkSegmentShipped_GuardsForgeRootOnRoot verifies the orphan-root guard:
// shipping a catalog segment advances forge_root_cid only for an op-root that
// still equals the bucket's committed root. A stale op-root (one a lost CASRoot
// left durable but the bucket never adopted) must be skipped.
func TestMarkSegmentShipped_GuardsForgeRootOnRoot(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()

	if err := m.Create(ctx, "bk", testutil.RandomDID(t)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	committed := testCid(t, "committed-root")
	stale := testCid(t, "stale-root")
	if err := m.CASRoot(ctx, "bk", cid.Undef, committed); err != nil {
		t.Fatalf("CASRoot: %v", err)
	}

	seq, _ := m.NextSegmentSeq(ctx)
	if err := m.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, "bk"); err != nil {
		t.Fatalf("InsertSegmentOpen: %v", err)
	}

	// Ship with the stale op-root LAST: unconditionally (the old behavior) it
	// would win as the last write; the guard must skip it and keep `committed`.
	if err := m.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, 100, nil, []blockstore.OpRoot{
		{Bucket: "bk", Root: committed},
		{Bucket: "bk", Root: stale},
	}); err != nil {
		t.Fatalf("MarkSegmentShipped: %v", err)
	}

	st, err := m.Get(ctx, "bk")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !st.ForgeRoot.Equals(committed) {
		t.Fatalf("forge_root = %v, want the committed root (stale op-root must be skipped)", st.ForgeRoot)
	}

	// A segment carrying ONLY a stale op-root must not advance forge_root at all.
	m2 := NewMemStore()
	_ = m2.Create(ctx, "bk2", testutil.RandomDID(t))
	_ = m2.CASRoot(ctx, "bk2", cid.Undef, committed)
	seq2, _ := m2.NextSegmentSeq(ctx)
	_ = m2.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq2, "bk2")
	if err := m2.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq2, 100, nil, []blockstore.OpRoot{
		{Bucket: "bk2", Root: stale},
	}); err != nil {
		t.Fatalf("MarkSegmentShipped(stale only): %v", err)
	}
	st2, _ := m2.Get(ctx, "bk2")
	if st2.ForgeRoot.Defined() {
		t.Fatalf("forge_root = %v, want undefined (a stale-only ship must not advance it)", st2.ForgeRoot)
	}
}
