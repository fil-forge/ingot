package iam

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/stretchr/testify/require"
)

// A cursor-gap reset cannot enumerate captured stores: they can outlive their
// registry entries. Every old generation must become unusable, even if a late
// authorize response attempts to refill the captured pointer.
func TestClearAllInvalidatesHeldProofStores(t *testing.T) {
	for _, evicted := range []bool{false, true} {
		t.Run(fmt.Sprintf("evicted=%t", evicted), func(t *testing.T) {
			kp := NewKeyProofs()
			dlg := mintDelegation(t, delegation.WithNoExpiration())
			key := dlg.Audience()
			held := kp.For(key)
			require.True(t, held.Add(dlg))
			held.PutPermissions(dlg.Subject(), time.Hour, []string{"s3:GetObject"})
			chain, _, err := held.ProofChainVia(context.Background(), dlg.Audience(), dlg.Command(), dlg.Subject(), dlg.Issuer())
			require.NoError(t, err)
			require.Len(t, chain, 1)
			pending := held.listDelegations(context.Background(), dlg.Audience(), dlg.Command(), dlg.Subject())
			if evicted {
				kp.byKey.Delete(key.String())
			}
			kp.ClearAll()
			for range pending {
				t.Fatal("an iterator captured before reset yielded old authority")
			}
			require.False(t, held.Add(dlg), "late response resurrected an old generation")
			held.PutPermissions(dlg.Subject(), time.Hour, []string{"s3:GetObject"})
			require.False(t, held.Contains(dlg.Link()))
			chain, _, err = held.ProofChain(context.Background(), dlg.Audience(), dlg.Command(), dlg.Subject())
			require.NoError(t, err)
			require.Empty(t, chain)
			chain, _, err = held.ProofChainVia(context.Background(), dlg.Audience(), dlg.Command(), dlg.Subject(), dlg.Issuer())
			require.NoError(t, err)
			require.Empty(t, chain)
			allowed, known := held.Permits(dlg.Subject(), "s3:GetObject")
			require.False(t, allowed)
			require.False(t, known)

			fresh := kp.For(key)
			require.NotSame(t, held, fresh)
			require.True(t, fresh.Add(dlg))
			fresh.PutPermissions(dlg.Subject(), time.Hour, []string{"s3:GetObject"})
			chain, _, err = fresh.ProofChain(context.Background(), dlg.Audience(), dlg.Command(), dlg.Subject())
			require.NoError(t, err)
			require.Len(t, chain, 1)
			allowed, known = fresh.Permits(dlg.Subject(), "s3:GetObject")
			require.True(t, allowed)
			require.True(t, known)
		})
	}
}

func TestClearAllRacesHeldStoreRefill(t *testing.T) {
	for range 100 {
		kp := NewKeyProofs()
		dlg := mintDelegation(t, delegation.WithNoExpiration())
		held := kp.For(dlg.Audience())
		require.True(t, held.Add(dlg))
		var wg sync.WaitGroup
		wg.Go(func() {
			held.Add(dlg)
			held.PutPermissions(dlg.Subject(), time.Hour, []string{"s3:GetObject"})
		})
		wg.Go(func() { kp.ClearAll() })
		wg.Wait()
		chain, _, err := held.ProofChain(context.Background(), dlg.Audience(), dlg.Command(), dlg.Subject())
		require.NoError(t, err)
		require.Empty(t, chain)
		_, known := held.Permits(dlg.Subject(), "s3:GetObject")
		require.False(t, known)
	}
}

// TestRevocationTTL: a revocation names only a CID, and the grant behind it
// may never expire, so the revoked set keeps it for revokedTTL — past the
// authorize horizon — and renews it each time Hilt serves the CID again.
func TestRevocationTTL(t *testing.T) {
	kp := NewKeyProofs()
	dlg := mintDelegation(t, delegation.WithNoExpiration())
	id := dlg.Link().String()
	expiresIn := func() time.Duration {
		item, ok := kp.revoked.Items()[id]
		require.True(t, ok, "the revocation is recorded")
		return time.Until(time.Unix(0, item.Expiration))
	}

	kp.InvalidateHolders(dlg.Link())
	require.InDelta(t, revokedTTL, expiresIn(), float64(time.Minute), "recorded for revokedTTL, not until midnight")

	// A refusal while the entry is young leaves it alone: a read is not a write.
	fresh := kp.revoked.Items()[id].Expiration
	require.False(t, kp.For(dlg.Audience()).Add(dlg))
	require.Equal(t, fresh, kp.revoked.Items()[id].Expiration, "a young revocation is not rewritten")

	// Hilt serving the revoked CID again once the entry has used up half its
	// life renews it.
	kp.revoked.Set(id, struct{}{}, time.Minute)
	require.False(t, kp.For(dlg.Audience()).Add(dlg))
	require.InDelta(t, revokedTTL, expiresIn(), float64(time.Minute), "a refusal renews an ageing revocation")
}
