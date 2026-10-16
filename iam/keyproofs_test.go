package iam_test

import (
	"context"
	"sync"
	"testing"

	contentcmds "github.com/fil-forge/libforge/commands/content"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/iam"
)

// grant mints one no-expiry /content/retrieve delegation iss→aud over iss.
func grant(t *testing.T) (ucan.Delegation, ucan.Issuer) {
	t.Helper()
	iss, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	aud, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	dlg, err := contentcmds.Retrieve.Delegate(iss, aud.DID(), iss.DID(), delegation.WithNoExpiration())
	require.NoError(t, err)
	return dlg, iss
}

func TestKeyProofsIsolation(t *testing.T) {
	ctx := context.Background()
	kp := iam.NewKeyProofs()

	keyA, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	keyB, err := ed25519.GenerateIssuer()
	require.NoError(t, err)

	// A root→leaf delegation deposited only under keyA.
	root, rootIss := grant(t)
	kp.Deposit(keyA.DID(), root)

	// keyA's store resolves it; keyB's store does not see it at all.
	chainA, _, err := kp.For(keyA.DID()).ProofChain(ctx, root.Audience(), root.Command(), rootIss.DID())
	require.NoError(t, err)
	require.Len(t, chainA, 1, "keyA sees its own delegation")

	chainB, _, err := kp.For(keyB.DID()).ProofChain(ctx, root.Audience(), root.Command(), rootIss.DID())
	require.NoError(t, err)
	require.Empty(t, chainB, "keyB must not see keyA's delegation")
}

func TestKeyProofsForIsStable(t *testing.T) {
	kp := iam.NewKeyProofs()
	key, err := ed25519.GenerateIssuer()
	require.NoError(t, err)

	a := kp.For(key.DID())
	b := kp.For(key.DID())
	require.Same(t, a, b, "For returns the same store for a key within the idle window")

	// A deposit is visible through a subsequently-fetched store (same instance).
	dlg, iss := grant(t)
	kp.Deposit(key.DID(), dlg)
	chain, _, err := kp.For(key.DID()).ProofChain(context.Background(), dlg.Audience(), dlg.Command(), iss.DID())
	require.NoError(t, err)
	require.Len(t, chain, 1)
}

func TestKeyProofsStoreRefusesRevoked(t *testing.T) {
	kp := iam.NewKeyProofs()
	key, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	store := kp.For(key.DID())

	// A delegation no store holds: the revocation still has to be remembered.
	dlg, _ := grant(t)
	require.Empty(t, kp.InvalidateHolders(dlg.Link()))

	other, _ := grant(t)
	require.False(t, store.Add(other, dlg), "any revoked delegation in the batch refuses the add")
	require.False(t, store.Contains(other.Link()), "a refused add adds nothing")
	require.True(t, store.Add(other))
	require.True(t, store.Contains(other.Link()))
}

// TestInvalidatedStoreReportsDropped: the store a revocation drops reports
// Dropped, so a holder can tell it must not keep it; the key's next store
// does not.
func TestInvalidatedStoreReportsDropped(t *testing.T) {
	kp := iam.NewKeyProofs()
	key, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	dlg, _ := grant(t)
	kp.Deposit(key.DID(), dlg)
	held := kp.For(key.DID())
	require.False(t, held.Dropped())

	require.Len(t, kp.InvalidateHolders(dlg.Link()), 1)
	require.True(t, held.Dropped())
	require.False(t, kp.For(key.DID()).Dropped())
}

// TestRequestScopedStoreKeepsRevoked: a request authorized from a response
// Hilt served after a failed commit still needs that response's delegations
// for its onward retrieval, so a store built outside KeyProofs adds them.
func TestRequestScopedStoreKeepsRevoked(t *testing.T) {
	kp := iam.NewKeyProofs()
	dlg, _ := grant(t)
	kp.InvalidateHolders(dlg.Link())

	scoped := iam.NewDelegationCache(nil)
	require.True(t, scoped.Add(dlg))
	require.True(t, scoped.Contains(dlg.Link()))
}

// TestKeyProofsDepositRacesRevocation races a deposit against the revocation
// of the delegation it carries: whichever lands first, the key's store must
// not end up holding the revoked delegation.
func TestKeyProofsDepositRacesRevocation(t *testing.T) {
	for range 200 {
		kp := iam.NewKeyProofs()
		key, err := ed25519.GenerateIssuer()
		require.NoError(t, err)
		store := kp.For(key.DID())
		dlg, _ := grant(t)

		var wg sync.WaitGroup
		wg.Go(func() { store.Add(dlg) })
		wg.Go(func() { kp.InvalidateHolders(dlg.Link()) })
		wg.Wait()

		require.False(t, kp.For(key.DID()).Contains(dlg.Link()))
	}
}
