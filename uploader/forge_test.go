package uploader

import (
	"context"
	"net/url"
	"testing"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/forgeclient"
	"github.com/fil-forge/ingot/iam"
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

// borrowable returns a context carrying store, marked as the IAM layer marks
// a request's own key store.
func borrowable(store ucanlib.ProofStore) context.Context {
	ctx := context.WithValue(context.Background(), reqscope.ProofStoreKey(), store)
	return context.WithValue(ctx, reqscope.BorrowableKey(), true)
}

// keyStore returns an access key's store in keys holding a /blob/add grant
// to agent on each of spaces, and the grants in the same order.
func keyStore(t *testing.T, keys *iam.KeyProofs, agent did.DID, spaces ...multikey.Issuer) (ucanlib.ProofStore, []ucan.Delegation) {
	t.Helper()
	key, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	var grants []ucan.Delegation
	for _, space := range spaces {
		grant, err := blobcmds.Add.Delegate(space, agent, space.DID(), delegation.WithNoExpiration())
		require.NoError(t, err)
		grants = append(grants, grant)
	}
	keys.Deposit(key.DID(), grants...)
	return keys.For(key.DID()), grants
}

func TestForgeShipProofsCache(t *testing.T) {
	fg := newTestForge(t)
	ctx := context.Background()
	spaceA := mustSpace(t)
	spaceB := mustSpace(t)
	storeA := ucanlib.ProofStore(iam.NewDelegationCache(nil))

	// Absent before any capture.
	if _, ok := fg.shipProofStore(ctx, spaceA); ok {
		t.Fatal("expected no ship store before capture")
	}

	// Capture for spaceA; it resolves, but a different space stays absent.
	fg.captureShipProofs(borrowable(storeA), spaceA)
	got, ok := fg.shipProofStore(ctx, spaceA)
	require.True(t, ok)
	require.Same(t, storeA, got)
	if _, ok := fg.shipProofStore(ctx, spaceB); ok {
		t.Fatal("spaceB should not resolve from spaceA's capture")
	}

	// A context with no store captures nothing.
	fg.captureShipProofs(context.WithValue(ctx, reqscope.BorrowableKey(), true), spaceB)
	if _, ok := fg.shipProofStore(ctx, spaceB); ok {
		t.Fatal("a capture with no store must not populate the cache")
	}
}

// TestForgeShipProofsOnlyBorrowable: a write captures its store only when the
// IAM layer marked it borrowable; one that may not be borrowed leaves the
// space's earlier capture in place.
func TestForgeShipProofsOnlyBorrowable(t *testing.T) {
	fg := newTestForge(t)
	ctx := context.Background()
	space := mustSpace(t)

	scoped := ucanlib.ProofStore(iam.NewDelegationCache(nil))
	fg.captureShipProofs(context.WithValue(ctx, reqscope.ProofStoreKey(), scoped), space)
	if _, ok := fg.shipProofStore(ctx, space); ok {
		t.Fatal("captured a store not marked borrowable")
	}

	held := ucanlib.ProofStore(iam.NewDelegationCache(nil))
	fg.captureShipProofs(borrowable(held), space)
	unmarked := context.WithValue(borrowable(scoped), reqscope.BorrowableKey(), false)
	fg.captureShipProofs(unmarked, space)
	got, ok := fg.shipProofStore(ctx, space)
	require.True(t, ok)
	require.Same(t, held, got, "a store marked not borrowable replaced the capture")
}

// TestForgeShipProofsSkipRevoked: after a revocation over one space, the
// store captured from that key builds no chain through the revoked grant,
// while the chain it builds for another space is untouched, so that space's
// ships still go out.
func TestForgeShipProofsSkipRevoked(t *testing.T) {
	fg := newTestForge(t)
	ctx := context.Background()
	agent, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	spaceA, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	spaceB, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	keys := iam.NewKeyProofs()
	store, grants := keyStore(t, keys, agent.DID(), spaceA, spaceB)
	fg.captureShipProofs(borrowable(store), spaceA.DID())
	fg.captureShipProofs(borrowable(store), spaceB.DID())

	require.Len(t, keys.InvalidateHolders(grants[1].Link()), 1)

	shipChain := func(space did.DID) []ucan.Delegation {
		t.Helper()
		got, ok := fg.shipProofStore(ctx, space)
		require.True(t, ok)
		chain, _, err := got.ProofChain(ctx, agent.DID(), blobcmds.Add.Command, space)
		require.NoError(t, err)
		return chain
	}
	require.NotEmpty(t, shipChain(spaceA.DID()), "a revocation over one space cut another space's ship authority")
	require.Empty(t, shipChain(spaceB.DID()), "a ship presented a revoked grant")
}

func TestForgeShipProofsRequestScopeWins(t *testing.T) {
	fg := newTestForge(t)
	space := mustSpace(t)
	captured := ucanlib.ProofStore(iam.NewDelegationCache(nil))
	fg.captureShipProofs(borrowable(captured), space)

	// A request-scoped store on the context takes precedence over the
	// captured fallback (the in-request write path), borrowable or not.
	scoped := ucanlib.ProofStore(tokenstore.NewMemStore())
	ctx := context.WithValue(context.Background(), reqscope.ProofStoreKey(), scoped)

	got, ok := fg.shipProofStore(ctx, space)
	require.True(t, ok)
	require.Same(t, scoped, got)
	require.NotSame(t, captured, got)
}
