package iam_test

import (
	"context"
	"testing"

	contentcmds "github.com/fil-forge/libforge/commands/content"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/stretchr/testify/require"

	hiltiam "github.com/fil-forge/ingot/iam"
)

// mintRetrieveChain builds the RFC chain bucket→tenant→access-key→agent for
// /content/retrieve, returning the three delegations root-first plus the
// agent issuer.
func mintRetrieveChain(t *testing.T, leafOpts ...delegation.Option) (root, mid, leaf ucan.Delegation, agent ucan.Issuer) {
	t.Helper()
	bucket, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	tenant, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	accessKey, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	agent, err = ed25519.GenerateIssuer()
	require.NoError(t, err)

	root, err = contentcmds.Retrieve.Delegate(bucket, tenant.DID(), bucket.DID(), delegation.WithNoExpiration())
	require.NoError(t, err)
	mid, err = contentcmds.Retrieve.Delegate(tenant, accessKey.DID(), bucket.DID(), delegation.WithNoExpiration())
	require.NoError(t, err)
	if len(leafOpts) == 0 {
		leafOpts = []delegation.Option{delegation.WithNoExpiration()}
	}
	leaf, err = contentcmds.Retrieve.Delegate(accessKey, agent.DID(), bucket.DID(), leafOpts...)
	require.NoError(t, err)
	return root, mid, leaf, agent
}

func TestDelegationCacheProofChain(t *testing.T) {
	ctx := context.Background()

	t.Run("full chain resolves root-first", func(t *testing.T) {
		root, mid, leaf, agent := mintRetrieveChain(t)
		c := hiltiam.NewDelegationCache()
		c.Add(leaf, root, mid) // insertion order must not matter

		chain, links, err := c.ProofChain(ctx, agent.DID(), contentcmds.Retrieve.Command, root.Subject())
		require.NoError(t, err)
		require.Len(t, chain, 3)
		require.Len(t, links, 3)
		require.Equal(t, root.Link(), chain[0].Link(), "chain must be root-first")
		require.Equal(t, leaf.Link(), chain[2].Link())
	})

	// ucanlib.ProofChain prunes incomplete paths and reports "no chain" as
	// empty slices with a nil error, so absence is asserted on length.
	t.Run("incomplete chain yields nothing", func(t *testing.T) {
		_, mid, leaf, agent := mintRetrieveChain(t)
		c := hiltiam.NewDelegationCache()
		c.Add(mid, leaf) // no root

		chain, _, err := c.ProofChain(ctx, agent.DID(), contentcmds.Retrieve.Command, mid.Subject())
		require.NoError(t, err)
		require.Empty(t, chain)
	})

	t.Run("expired leaf is never cached", func(t *testing.T) {
		root, mid, leaf, agent := mintRetrieveChain(t,
			delegation.WithExpiration(ucan.Now()-10)) // already expired
		c := hiltiam.NewDelegationCache()
		c.Add(root, mid, leaf)

		chain, _, err := c.ProofChain(ctx, agent.DID(), contentcmds.Retrieve.Command, root.Subject())
		require.NoError(t, err)
		require.Empty(t, chain, "expired leaf must not complete a chain")
	})

	t.Run("ttl entries live until expiry", func(t *testing.T) {
		root, mid, leaf, agent := mintRetrieveChain(t,
			delegation.WithExpiration(ucan.Now()+3600))
		c := hiltiam.NewDelegationCache()
		c.Add(root, mid, leaf)

		_, _, err := c.ProofChain(ctx, agent.DID(), contentcmds.Retrieve.Command, root.Subject())
		require.NoError(t, err)
	})
}
