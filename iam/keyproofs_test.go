package iam_test

import (
	"context"
	"testing"

	contentcmds "github.com/fil-forge/libforge/commands/content"
	"github.com/fil-forge/ucantone/did"
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

// TestKeyProofsPrincipalIndex covers the (tenant, principal) index a
// firehose principal event resolves against.
func TestKeyProofsPrincipalIndex(t *testing.T) {
	tenant := did.MustParse("did:plc:ewvi7nxzyoun6zhxrhs64oiz")
	ada := iam.PrincipalRef{Tenant: tenant, Principal: "ada"}
	grace := iam.PrincipalRef{Tenant: tenant, Principal: "grace"}

	t.Run("both stores for a pair drop, a sibling principal survives", func(t *testing.T) {
		kp := iam.NewKeyProofs()
		keyA, dlgA := boundKey(t, kp, ada)
		keyB, dlgB := boundKey(t, kp, ada)
		keyC, dlgC := boundKey(t, kp, grace)

		affected := kp.InvalidatePrincipal(ada)
		require.ElementsMatch(t, []did.DID{keyA, keyB}, affected)
		require.False(t, kp.For(keyA).Contains(dlgA.Link()), "ada's first key must lose its store")
		require.False(t, kp.For(keyB).Contains(dlgB.Link()), "ada's second key must lose its store")
		require.True(t, kp.For(keyC).Contains(dlgC.Link()), "grace's key must be untouched")
	})

	t.Run("re-delivery is a no-op", func(t *testing.T) {
		kp := iam.NewKeyProofs()
		key, _ := boundKey(t, kp, ada)

		require.Equal(t, []did.DID{key}, kp.InvalidatePrincipal(ada))
		require.Empty(t, kp.InvalidatePrincipal(ada), "the binding goes with the store")
	})

	t.Run("an unknown pair is a no-op", func(t *testing.T) {
		kp := iam.NewKeyProofs()
		key, dlg := boundKey(t, kp, ada)

		require.Empty(t, kp.InvalidatePrincipal(grace))
		require.Empty(t, kp.InvalidatePrincipal(iam.PrincipalRef{
			Tenant:    did.MustParse("did:plc:44444444444444444444444444"),
			Principal: "ada",
		}), "the same userId under another tenant is another principal")
		require.True(t, kp.For(key).Contains(dlg.Link()))
	})

	t.Run("binding is idempotent", func(t *testing.T) {
		kp := iam.NewKeyProofs()
		key, _ := boundKey(t, kp, ada)
		kp.Bind(key, ada)

		require.Equal(t, []did.DID{key}, kp.InvalidatePrincipal(ada))
	})

	t.Run("a key re-bound to another principal follows it", func(t *testing.T) {
		kp := iam.NewKeyProofs()
		key, _ := boundKey(t, kp, ada)
		kp.Bind(key, grace)

		require.Empty(t, kp.InvalidatePrincipal(ada), "the old pair must not still claim the key")
		require.Equal(t, []did.DID{key}, kp.InvalidatePrincipal(grace))
	})

	t.Run("an incomplete reference indexes nothing", func(t *testing.T) {
		kp := iam.NewKeyProofs()
		key, dlg := boundKey(t, kp, iam.PrincipalRef{Tenant: tenant})
		kp.Bind(key, iam.PrincipalRef{Principal: "ada"})

		require.Empty(t, kp.InvalidatePrincipal(iam.PrincipalRef{Tenant: tenant}))
		require.Empty(t, kp.InvalidatePrincipal(iam.PrincipalRef{Principal: "ada"}))
		require.True(t, kp.For(key).Contains(dlg.Link()))
	})

	// The index must not deadlock against InvalidateHolders: go-cache runs
	// the byKey eviction hook synchronously while that method holds the
	// registry mutex, so the hook takes a different lock.
	t.Run("a CID revocation also drops the binding", func(t *testing.T) {
		kp := iam.NewKeyProofs()
		key, dlg := boundKey(t, kp, ada)

		require.Equal(t, []did.DID{key}, kp.InvalidateHolders(dlg.Link()))
		require.Empty(t, kp.InvalidatePrincipal(ada), "the evicted store's binding must be gone")
	})
}

// boundKey generates an access key, binds it to ref and deposits one
// delegation in its store, returning the key and that delegation. Whether
// the store survives an invalidation is read back through the delegation.
func boundKey(t *testing.T, kp *iam.KeyProofs, ref iam.PrincipalRef) (did.DID, ucan.Delegation) {
	t.Helper()
	signer, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	kp.Bind(signer.DID(), ref)
	dlg, _ := grant(t)
	kp.Deposit(signer.DID(), dlg)
	require.True(t, kp.For(signer.DID()).Contains(dlg.Link()))
	return signer.DID(), dlg
}
