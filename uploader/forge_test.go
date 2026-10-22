package uploader

import (
	"context"
	"net/url"
	"path/filepath"
	"testing"

	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/command"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/forgeclient"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/tokenstore"
)

// newTestForge builds a Forge with a real (but never-dialed) client; the
// tests here only exercise the in-memory ship-store cache and reqscope
// resolution, so the client's URL is never contacted.
func newTestForge(t *testing.T) *Forge {
	t.Helper()
	agent, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	svc, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	u, err := url.Parse("http://127.0.0.1:1")
	require.NoError(t, err)
	c, err := forgeclient.New(agent, svc.DID(), *u)
	require.NoError(t, err)
	fg, err := NewForge(ForgeConfig{Client: c})
	require.NoError(t, err)
	return fg
}

func mustSpace(t *testing.T) did.DID {
	t.Helper()
	iss, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	return iss.DID()
}

func TestForgeShipProofsCache(t *testing.T) {
	fg := newTestForge(t)
	ctx := context.Background()
	spaceA := mustSpace(t)
	spaceB := mustSpace(t)
	storeA := ucanlib.ProofStore(tokenstore.NewMemStore())

	// Absent before any capture.
	if _, ok := fg.shipProofStore(ctx, spaceA); ok {
		t.Fatal("expected no ship store before capture")
	}

	// Capture for spaceA; it resolves, but a different space stays absent.
	fg.captureShipProofs(spaceA, storeA)
	got, ok := fg.shipProofStore(ctx, spaceA)
	require.True(t, ok)
	require.Same(t, storeA, got)
	if _, ok := fg.shipProofStore(ctx, spaceB); ok {
		t.Fatal("spaceB should not resolve from spaceA's capture")
	}

	// Capturing a nil store is a no-op.
	fg.captureShipProofs(spaceB, nil)
	if _, ok := fg.shipProofStore(ctx, spaceB); ok {
		t.Fatal("nil capture must not populate the cache")
	}
}

func TestForgeShipProofsRequestScopeWins(t *testing.T) {
	fg := newTestForge(t)
	space := mustSpace(t)
	captured := ucanlib.ProofStore(tokenstore.NewMemStore())
	fg.captureShipProofs(space, captured)

	// A request-scoped store on the context takes precedence over the
	// captured fallback (the in-request write path).
	scoped := ucanlib.ProofStore(tokenstore.NewMemStore())
	ctx := context.WithValue(context.Background(), reqscope.ProofStoreKey(), scoped)

	got, ok := fg.shipProofStore(ctx, space)
	require.True(t, ok)
	require.Same(t, scoped, got)
	require.NotSame(t, captured, got)
}

// chainStore is a ProofStore that answers every lookup with chain: one that
// can authorize, or, with chain empty, one that can't.
type chainStore struct{ chain []ucan.Delegation }

func (s *chainStore) ProofChain(context.Context, did.DID, ucan.Command, did.DID) ([]ucan.Delegation, []cid.Cid, error) {
	return s.chain, nil, nil
}

// shippingStore returns a store that holds a chain for any lookup.
func shippingStore(t *testing.T) *chainStore {
	t.Helper()
	issuer, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	d, err := delegation.Delegate(issuer, mustSpace(t), mustSpace(t), command.MustParse("/blob/add"),
		delegation.WithNoExpiration())
	require.NoError(t, err)
	return &chainStore{chain: []ucan.Delegation{d}}
}

// TestForgeCaptureShipAuthority checks the capture a commit makes: the
// request's store becomes the store a ship to the space signs with, found
// later without a request and ahead of an upload's capture, while the
// release, abort and renewal paths (shipProofStore) keep the upload's, since
// a commit's grant need not cover their commands. A commit store with no
// /blob/add chain gives way to the upload's, and a ctx carrying no store
// leaves the capture in place.
func TestForgeCaptureShipAuthority(t *testing.T) {
	fg := newTestForge(t)
	ctx := context.Background()
	space := mustSpace(t)
	uploaded := shippingStore(t)
	committed := shippingStore(t)

	// With only an upload's capture, the ship falls back to it.
	fg.captureShipProofs(space, uploaded)
	got, ok := fg.catalogShipStore(ctx, space)
	require.True(t, ok)
	require.Same(t, uploaded, got)

	fg.CaptureShipAuthority(context.WithValue(ctx, reqscope.ProofStoreKey(), ucanlib.ProofStore(committed)), space)
	got, ok = fg.catalogShipStore(ctx, space)
	require.True(t, ok)
	require.Same(t, committed, got, "the ship signs with the latest commit's store")
	got, ok = fg.shipProofStore(ctx, space)
	require.True(t, ok)
	require.Same(t, uploaded, got, "a commit's store must not replace the upload's for release, abort and renewal")

	fg.CaptureShipAuthority(ctx, space)
	got, ok = fg.catalogShipStore(ctx, space)
	require.True(t, ok)
	require.Same(t, committed, got, "a ctx without a store must not replace the capture")

	// A commit store that can't build a /blob/add chain gives way to the
	// upload's.
	fg.CaptureShipAuthority(context.WithValue(ctx, reqscope.ProofStoreKey(), ucanlib.ProofStore(&chainStore{})), space)
	got, ok = fg.catalogShipStore(ctx, space)
	require.True(t, ok)
	require.Same(t, uploaded, got, "a commit store with no chain must not be shipped with")

	// A commit's capture alone is enough to ship.
	other := mustSpace(t)
	fg.CaptureShipAuthority(context.WithValue(ctx, reqscope.ProofStoreKey(), ucanlib.ProofStore(committed)), other)
	got, ok = fg.catalogShipStore(ctx, other)
	require.True(t, ok)
	require.Same(t, committed, got)
	if _, ok := fg.shipProofStore(ctx, other); ok {
		t.Fatal("a commit's capture must not serve release, abort or renewal")
	}
}

// TestSubmitShardShipsUnderCommitCapture checks SubmitShard resolves its
// authority through catalogShipStore: a space with only a commit's capture
// gets past the authority check (to the missing CAR this shard names), and
// one whose only capture holds no chain does not.
func TestSubmitShardShipsUnderCommitCapture(t *testing.T) {
	fg := newTestForge(t)
	ctx := context.Background()
	mh, err := multihash.Sum([]byte("car"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	shard := CARShard{
		Path:      filepath.Join(t.TempDir(), "missing.car"),
		Size:      1,
		SHA256:    mh,
		Positions: map[cid.Cid]blockstore.BlockLoc{cid.NewCidV1(cid.Raw, mh): {Offset: 0, Length: 1}},
	}

	space := mustSpace(t)
	fg.CaptureShipAuthority(context.WithValue(ctx, reqscope.ProofStoreKey(), ucanlib.ProofStore(shippingStore(t))), space)
	_, _, err = fg.SubmitShard(ctx, blockstore.PlaneCatalog, space, shard)
	require.ErrorContains(t, err, "missing.car", "a commit's capture should have authorized the ship")

	chainless := mustSpace(t)
	fg.CaptureShipAuthority(context.WithValue(ctx, reqscope.ProofStoreKey(), ucanlib.ProofStore(&chainStore{})), chainless)
	_, _, err = fg.SubmitShard(ctx, blockstore.PlaneCatalog, chainless, shard)
	require.ErrorContains(t, err, "no ship authority")
}
