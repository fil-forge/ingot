package inmem

import (
	"context"
	"errors"
	"testing"

	block "github.com/ipfs/go-block-format"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
)

// TestCatalogJournal checks the journal rules the Postgres store keeps too: a
// commit journals its blocks under their segment until the segment ships,
// journals nothing into a segment that already has, journals nothing when
// its swap is refused, and loses its rows with its bucket.
func TestCatalogJournal(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	if err := m.Create(ctx, "bk", testutil.RandomDID(t), registry.CreateState{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	commit := func(t *testing.T, seg uint64, blocks ...block.Block) error {
		t.Helper()
		st, err := m.Get(ctx, "bk")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		next := testCid(t, blocks[0].Cid().String())
		return m.CommitRoot(ctx, "bk", st.Root, next, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: blocks},
		})
	}
	stats := func(t *testing.T) registry.CatalogJournalStats {
		t.Helper()
		st, err := m.CatalogJournalStats(ctx)
		if err != nil {
			t.Fatalf("CatalogJournalStats: %v", err)
		}
		return st
	}

	if err := m.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, 1, "bk"); err != nil {
		t.Fatalf("InsertSegmentOpen: %v", err)
	}
	a, b := block.NewBlock([]byte("node-a")), block.NewBlock([]byte("manifest-b"))
	if err := commit(t, 1, a, b); err != nil {
		t.Fatalf("commit: %v", err)
	}
	rows := m.JournaledBlocks(1)
	if len(rows) != 2 || string(rows[string(a.Cid().Bytes())]) != "node-a" || string(rows[string(b.Cid().Bytes())]) != "manifest-b" {
		t.Fatalf("journal after commit = %v, want both blocks under segment 1", rows)
	}
	if st := stats(t); st.Blocks != 2 || st.Bytes != int64(len("node-a")+len("manifest-b")) || st.Oldest.IsZero() {
		t.Fatalf("stats = %+v, want 2 blocks of %d bytes and an oldest time", st, len("node-a")+len("manifest-b"))
	}

	// A swap that loses the CAS journals nothing.
	if err := m.CommitRoot(ctx, "bk", testCid(t, "not-the-root"), testCid(t, "next"), registry.RootCommit{
		Journal: blockstore.Appended{Segment: 1, Blocks: []block.Block{block.NewBlock([]byte("lost"))}},
	}); !errors.Is(err, registry.ErrConflict) {
		t.Fatalf("conflicting commit = %v, want ErrConflict", err)
	}
	if got := len(m.JournaledBlocks(1)); got != 2 {
		t.Fatalf("journal after a refused commit holds %d blocks, want 2", got)
	}

	// Shipping the segment drops its rows, and a commit landing in it
	// afterwards journals nothing: its blocks are on Forge.
	if err := m.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, 1, 1, nil, -1, nil); err != nil {
		t.Fatalf("MarkSegmentShipped: %v", err)
	}
	if got := m.JournaledBlocks(1); len(got) != 0 {
		t.Fatalf("journal after ship = %v, want empty", got)
	}
	if err := commit(t, 1, block.NewBlock([]byte("late"))); err != nil {
		t.Fatalf("late commit: %v", err)
	}
	if got := m.JournaledBlocks(1); len(got) != 0 {
		t.Fatalf("journal after a commit into a shipped segment = %v, want empty", got)
	}
	if st := stats(t); st.Blocks != 0 || !st.Oldest.IsZero() {
		t.Fatalf("stats after ship = %+v, want empty", st)
	}

	// A segment's rows outlive its segment row (recovery may delete that
	// row and rebuild the segment from them) but go with the bucket.
	if err := m.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, 2, "bk"); err != nil {
		t.Fatalf("InsertSegmentOpen: %v", err)
	}
	if err := commit(t, 2, block.NewBlock([]byte("kept"))); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := m.DeleteSegment(ctx, blockstore.PlaneCatalog, 2); err != nil {
		t.Fatalf("DeleteSegment: %v", err)
	}
	if got := len(m.JournaledBlocks(2)); got != 1 {
		t.Fatalf("journal after deleting the segment row holds %d blocks, want 1", got)
	}
	if err := m.Delete(ctx, "bk"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := m.JournaledBlocks(2); len(got) != 0 {
		t.Fatalf("journal after deleting the bucket = %v, want empty", got)
	}
}
