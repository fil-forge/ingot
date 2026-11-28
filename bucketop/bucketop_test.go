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
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/internal/testfakes"
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

// TestCommitCapturesShipAuthority checks that a commit's append hands its
// request to Ships under the bucket's space, so a write that uploads no body
// still leaves the authority its segment's ship needs.
func TestCommitCapturesShipAuthority(t *testing.T) {
	ctx := context.Background()
	mem := registry.NewPostgres(pgtest.New(t))
	log, err := logstore.Open(ctx, logstore.Config{
		Dir:     filepath.Join(t.TempDir(), "segments"),
		Meta:    mem,
		Catalog: logstore.PlaneConfig{Ship: false},
		Logger:  zaptest.NewLogger(t),
	})
	require.NoError(t, err, "logstore")
	t.Cleanup(func() { _ = log.Close(ctx) })
	space := testutil.RandomDID(t)
	require.NoError(t, mem.Create(ctx, "bk", space, registry.CreateState{Tenant: testutil.RandomDID(t)}), "create bucket")
	ships := &recordingShips{}
	reads := blockstore.NewLayered(testfakes.NopBaseReader{}, log, testfakes.NopBaseReader{})
	c := bucketop.NewCoordinator(bucketop.Deps{Reg: mem, Log: log, Reads: reads, Ships: ships})

	store := ucanlib.ProofStore(tokenstore.NewMemStore())
	reqCtx := context.WithValue(ctx, reqscope.ProofStoreKey(), store)
	tx, err := c.Begin(reqCtx, "bk")
	require.NoError(t, err, "Begin")
	defer tx.Discard()
	mh, err := multihash.Sum([]byte("root"), multihash.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	tx.Advance(cid.NewCidV1(cid.DagCBOR, mh))
	_, ok, err := tx.Append(reqCtx)
	require.NoError(t, err, "Append")
	require.True(t, ok, "Append")

	ships.mu.Lock()
	defer ships.mu.Unlock()
	require.Len(t, ships.captured, 1, "captured %d times", len(ships.captured))
	got := ships.captured[0]
	require.Equal(t, space.String(), got.space.String(), "captured for space %s", got.space)
	require.Same(t, store, got.store, "the capture's ctx didn't carry the request's proof store")
}
