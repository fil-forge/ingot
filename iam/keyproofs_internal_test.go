package iam

import (
	"testing"

	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/stretchr/testify/require"
)

// TestRevocationOutlivesMidnight: a revocation names only a CID, and the
// delegation behind it may never expire (a /s3/bucket/info chain), so the
// revoked set keeps it for the process lifetime, not the authorize horizon.
func TestRevocationOutlivesMidnight(t *testing.T) {
	kp := NewKeyProofs()
	dlg := mintDelegation(t, delegation.WithNoExpiration())
	kp.InvalidateHolders(dlg.Link())

	item, ok := kp.revoked.Items()[dlg.Link().String()]
	require.True(t, ok)
	require.Zero(t, item.Expiration, "the revocation must not expire")
}
