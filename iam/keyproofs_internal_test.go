package iam

import (
	"testing"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/stretchr/testify/require"
)

// An eviction hook that fires after a request has already re-created and
// re-bound the key's store must leave the new binding alone; otherwise the
// live store would be missed by the next principal invalidation.
func TestUnbindKeepsLiveRebinding(t *testing.T) {
	kp := NewKeyProofs()
	key, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	ref := PrincipalRef{Tenant: key.DID(), Principal: "member"}
	id := key.DID().String()

	kp.For(key.DID())
	kp.Bind(key.DID(), ref)

	// The stale hook for an evicted store runs after the re-create and
	// re-bind: the store is live, so the binding stays.
	kp.unbind(id, nil)
	require.Equal(t, []string{id}, didStrings(kp.InvalidatePrincipal(ref)))

	// With no live store the hook drops the binding, and the pair is empty.
	kp.unbind(id, nil)
	require.Empty(t, kp.InvalidatePrincipal(ref))
}

func didStrings(ids []did.DID) []string {
	out := make([]string, 0, len(ids))
	for _, d := range ids {
		out = append(out, d.String())
	}
	return out
}
