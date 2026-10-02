package iam

import (
	"testing"
	"time"

	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/stretchr/testify/require"
)

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

	// Hilt serving the revoked CID again renews it.
	kp.revoked.Set(id, struct{}{}, time.Minute)
	require.False(t, kp.DepositUnlessRevoked(NewDelegationCache(), dlg))
	require.InDelta(t, revokedTTL, expiresIn(), float64(time.Minute), "a refusal renews the revocation")
}
