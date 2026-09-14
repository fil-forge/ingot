package iam_test

import (
	"context"
	"testing"
	"time"

	contentcmds "github.com/fil-forge/libforge/commands/content"
	"github.com/fil-forge/ucantone/did"
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

// TestDelegationCachePermissions covers the per-bucket action set the local
// fast path consults before it probes for chains.
func TestDelegationCachePermissions(t *testing.T) {
	bucketA, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	bucketB, err := ed25519.GenerateIssuer()
	require.NoError(t, err)

	t.Run("no set cached: unknown", func(t *testing.T) {
		c := hiltiam.NewDelegationCache()
		allowed, known := c.Permits(bucketA.DID(), "s3:GetObject")
		require.False(t, known, "an uncached bucket must send the request to hilt")
		require.False(t, allowed)
	})

	t.Run("cached set answers both ways", func(t *testing.T) {
		c := hiltiam.NewDelegationCache()
		c.PutPermissions(bucketA.DID(), time.Hour, []string{"s3:PutObject"})

		allowed, known := c.Permits(bucketA.DID(), "s3:PutObject")
		require.True(t, known)
		require.True(t, allowed)

		allowed, known = c.Permits(bucketA.DID(), "s3:GetObject")
		require.True(t, known, "the bucket's set is cached, so the answer is authoritative")
		require.False(t, allowed, "an action outside the set is denied")
	})

	t.Run("empty set denies everything", func(t *testing.T) {
		c := hiltiam.NewDelegationCache()
		c.PutPermissions(bucketA.DID(), time.Hour, []string{})

		allowed, known := c.Permits(bucketA.DID(), "s3:GetObject")
		require.True(t, known, "an empty set is an answer, not a gap")
		require.False(t, allowed)
	})

	t.Run("nil set caches nothing", func(t *testing.T) {
		// Hilt returning no permissions at all leaves the bucket unknown, so
		// the fast path keeps deferring to Hilt instead of denying.
		c := hiltiam.NewDelegationCache()
		c.PutPermissions(bucketA.DID(), time.Hour, nil)

		_, known := c.Permits(bucketA.DID(), "s3:GetObject")
		require.False(t, known)
	})

	t.Run("sets are keyed by bucket", func(t *testing.T) {
		c := hiltiam.NewDelegationCache()
		c.PutPermissions(bucketA.DID(), time.Hour, []string{"s3:GetObject"})

		allowed, known := c.Permits(bucketA.DID(), "s3:GetObject")
		require.True(t, known)
		require.True(t, allowed)

		_, known = c.Permits(bucketB.DID(), "s3:GetObject")
		require.False(t, known, "another bucket's set must not answer for this one")
	})

	t.Run("undefined bucket is unknown", func(t *testing.T) {
		// A registry row with no space, or a Hilt result with no bucket,
		// must not collide with a real bucket's set.
		c := hiltiam.NewDelegationCache()
		c.PutPermissions(did.Undef, time.Hour, []string{"s3:GetObject"})

		_, known := c.Permits(did.Undef, "s3:GetObject")
		require.False(t, known)
	})

	t.Run("expired set is unknown again", func(t *testing.T) {
		c := hiltiam.NewDelegationCache()
		c.PutPermissions(bucketA.DID(), 20*time.Millisecond, []string{"s3:GetObject"})
		_, known := c.Permits(bucketA.DID(), "s3:GetObject")
		require.True(t, known)

		time.Sleep(50 * time.Millisecond)
		_, known = c.Permits(bucketA.DID(), "s3:GetObject")
		require.False(t, known, "an expired set must not decide anything")
	})

	t.Run("non-positive ttl caches nothing", func(t *testing.T) {
		c := hiltiam.NewDelegationCache()
		c.PutPermissions(bucketA.DID(), 0, []string{"s3:GetObject"})
		_, known := c.Permits(bucketA.DID(), "s3:GetObject")
		require.False(t, known)
	})

	t.Run("a later set replaces the earlier one", func(t *testing.T) {
		c := hiltiam.NewDelegationCache()
		c.PutPermissions(bucketA.DID(), time.Hour, []string{"s3:GetObject", "s3:PutObject"})
		c.PutPermissions(bucketA.DID(), time.Hour, []string{"s3:GetObject"})

		allowed, known := c.Permits(bucketA.DID(), "s3:PutObject")
		require.True(t, known)
		require.False(t, allowed, "a narrowed set must not keep the dropped action")
	})
}
