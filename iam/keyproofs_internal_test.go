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
