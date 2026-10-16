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

// keyStore returns an access key's store in keys, holding one /blob/add
// grant on space, and the grant.
func keyStore(t *testing.T, keys *iam.KeyProofs, space multikey.Issuer) (ucanlib.ProofStore, ucan.Delegation) {
	t.Helper()
	agent, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	key, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	grant, err := blobcmds.Add.Delegate(space, agent.DID(), space.DID(), delegation.WithNoExpiration())
	require.NoError(t, err)
	keys.Deposit(key.DID(), grant)
	return keys.For(key.DID()), grant
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
// IAM layer marked it borrowable and it can report a revocation's drop; one
// that may not be borrowed leaves the space's earlier capture in place.
func TestForgeShipProofsOnlyBorrowable(t *testing.T) {
	fg := newTestForge(t)
	ctx := context.Background()
	space := mustSpace(t)

	scoped := ucanlib.ProofStore(iam.NewDelegationCache(nil))
	fg.captureShipProofs(context.WithValue(ctx, reqscope.ProofStoreKey(), scoped), space)
	if _, ok := fg.shipProofStore(ctx, space); ok {
		t.Fatal("captured a store not marked borrowable")
	}

	fg.captureShipProofs(borrowable(tokenstore.NewMemStore()), space)
	if _, ok := fg.shipProofStore(ctx, space); ok {
		t.Fatal("captured a store that can't report a revocation's drop")
	}

	held := ucanlib.ProofStore(iam.NewDelegationCache(nil))
	fg.captureShipProofs(borrowable(held), space)
	unmarked := context.WithValue(borrowable(scoped), reqscope.BorrowableKey(), false)
	fg.captureShipProofs(unmarked, space)
	got, ok := fg.shipProofStore(ctx, space)
	require.True(t, ok)
	require.Same(t, held, got, "a store not marked borrowable replaced the capture")
}

// TestForgeShipProofsDropsRevoked: once a revocation drops the captured
// store, a ship finds no authority, though it still resolves in-request,
// and a later write's store is captured afresh.
func TestForgeShipProofsDropsRevoked(t *testing.T) {
	fg := newTestForge(t)
	ctx := context.Background()
	spaceKey, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	space := spaceKey.DID()
	keys := iam.NewKeyProofs()
	store, grant := keyStore(t, keys, spaceKey)

	fg.captureShipProofs(borrowable(store), space)
	_, ok := fg.shipProofStore(ctx, space)
	require.True(t, ok)

	require.Len(t, keys.InvalidateHolders(grant.Link()), 1)
	if _, ok := fg.shipProofStore(ctx, space); ok {
		t.Fatal("a ship borrowed a store a revocation dropped")
	}
	got, ok := fg.shipProofStore(context.WithValue(ctx, reqscope.ProofStoreKey(), store), space)
	require.True(t, ok)
	require.Same(t, store, got)

	fresh, _ := keyStore(t, keys, spaceKey)
	fg.captureShipProofs(borrowable(fresh), space)
	got, ok = fg.shipProofStore(ctx, space)
	require.True(t, ok)
	require.Same(t, fresh, got)
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
