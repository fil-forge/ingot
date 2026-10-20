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
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/blockstore"
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

// keyStore returns an access key's store in keys holding a two-hop /blob/add
// chain to agent on each of spaces (space → an intermediary → agent), and the
// chains' roots in the same order.
func keyStore(t *testing.T, keys *iam.KeyProofs, agent did.DID, spaces ...multikey.Issuer) (ucanlib.ProofStore, []ucan.Delegation) {
	t.Helper()
	key, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	mid, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	var roots []ucan.Delegation
	for _, space := range spaces {
		root, err := blobcmds.Add.Delegate(space, mid.DID(), space.DID(), delegation.WithNoExpiration())
		require.NoError(t, err)
		leaf, err := blobcmds.Add.Delegate(mid, agent, space.DID(), delegation.WithNoExpiration())
		require.NoError(t, err)
		keys.Deposit(key.DID(), root, leaf)
		roots = append(roots, root)
	}
	return keys.For(key.DID()), roots
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
	markedNot := context.WithValue(borrowable(scoped), reqscope.BorrowableKey(), false)
	fg.captureShipProofs(markedNot, space)
	got, ok := fg.shipProofStore(ctx, space)
	require.True(t, ok)
	require.Same(t, held, got, "a store marked not borrowable replaced the capture")
}

// TestForgeShipProofsSkipRevoked: after a revocation over one space, the
// store captured from that key builds no chain through the revoked grant,
// while the chain it builds for another space is untouched, so that space's
// ships still go out. The revoked space's ship fails locally, naming the
// missing authority, instead of going out proofless.
func TestForgeShipProofsSkipRevoked(t *testing.T) {
	fg := newTestForge(t)
	ctx := context.Background()
	agent := fg.client.Issuer().DID()
	spaceA, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	spaceB, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	keys := iam.NewKeyProofs()
	store, roots := keyStore(t, keys, agent, spaceA, spaceB)
	fg.captureShipProofs(borrowable(store), spaceA.DID())
	fg.captureShipProofs(borrowable(store), spaceB.DID())

	require.Len(t, keys.InvalidateHolders(roots[1].Link()), 1)

	_, chain, err := fg.authorityFor(ctx, spaceA.DID(), blobcmds.Add.Command)
	require.NoError(t, err, "a revocation over one space cut another space's ship authority")
	require.Len(t, chain, 2)

	_, _, err = fg.authorityFor(ctx, spaceB.DID(), blobcmds.Add.Command)
	require.ErrorContains(t, err, "no /blob/add authority", "a ship presented a revoked grant")
	shard := CARShard{Size: 1, Positions: map[cid.Cid]blockstore.BlockLoc{cid.Undef: {}}}
	_, _, err = fg.SubmitShard(ctx, blockstore.PlaneCatalog, spaceB.DID(), shard)
	require.ErrorContains(t, err, "no /blob/add authority")
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
