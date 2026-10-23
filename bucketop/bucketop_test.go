package bucketop_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fil-forge/libforge/testutil"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/tokenstore"
)

// recordingShips records each capture: the space and the proof store the
// capture's ctx carried.
type recordingShips struct {
	mu       sync.Mutex
	captured []capture
}

type capture struct {
	space did.DID
	store ucanlib.ProofStore
}

func (r *recordingShips) CaptureShipAuthority(ctx context.Context, space did.DID) {
	store, _ := reqscope.ProofStore(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.captured = append(r.captured, capture{space: space, store: store})
}

// TestCommitCapturesShipAuthority checks that a commit hands its request to
// Ships under the bucket's space, so a write that uploads no body still
// leaves the authority its segment's ship needs.
func TestCommitCapturesShipAuthority(t *testing.T) {
	ctx := context.Background()
	mem := inmem.NewMemStore()
	log, err := logstore.Open(ctx, logstore.Config{
		Dir:     filepath.Join(t.TempDir(), "segments"),
		Meta:    mem,
		Catalog: logstore.PlaneConfig{Ship: false},
		Logger:  zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatalf("logstore: %v", err)
	}
	t.Cleanup(func() { _ = log.Close(ctx) })
	space := testutil.RandomDID(t)
	if err := mem.Create(ctx, "bk", space, registry.CreateState{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	ships := &recordingShips{}
	reads := blockstore.NewLayered(inmem.NopBaseReader{}, log, inmem.NopBaseReader{})
	c := bucketop.NewCoordinator(bucketop.Deps{Reg: mem, Log: log, Reads: reads, Ships: ships})

	store := ucanlib.ProofStore(tokenstore.NewMemStore())
	reqCtx := context.WithValue(ctx, reqscope.ProofStoreKey(), store)
	tx, err := c.Begin(reqCtx, "bk")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Discard()
	mh, err := multihash.Sum([]byte("root"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	if err := tx.Commit(reqCtx, cid.NewCidV1(cid.DagCBOR, mh)); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	ships.mu.Lock()
	defer ships.mu.Unlock()
	if len(ships.captured) != 1 {
		t.Fatalf("captured %d times, want 1", len(ships.captured))
	}
	got := ships.captured[0]
	if got.space.String() != space.String() {
		t.Fatalf("captured for space %s, want the bucket's %s", got.space, space)
	}
	if got.store != store {
		t.Fatal("the capture's ctx didn't carry the request's proof store")
	}
}
