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

func TestKeyProofsDepositUnlessRevoked(t *testing.T) {
	kp := iam.NewKeyProofs()
	key, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	store := kp.For(key.DID())

	// A delegation no store holds: the revocation still has to be remembered.
	dlg, _ := grant(t)
	require.Empty(t, kp.InvalidateHolders(dlg.Link()))

	other, _ := grant(t)
	require.False(t, kp.DepositUnlessRevoked(store, other, dlg), "any revoked delegation in the set refuses the deposit")
	require.False(t, store.Contains(other.Link()), "a refused deposit adds nothing")
	require.True(t, kp.DepositUnlessRevoked(store, other))
	require.True(t, store.Contains(other.Link()))
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
		wg.Go(func() { kp.DepositUnlessRevoked(store, dlg) })
		wg.Go(func() { kp.InvalidateHolders(dlg.Link()) })
		wg.Wait()

		require.False(t, kp.For(key.DID()).Contains(dlg.Link()))
	}
}
