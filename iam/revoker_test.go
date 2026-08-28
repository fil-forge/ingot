package iam_test

import (
	"context"
	"strings"
	"testing"
	"time"

	contentcmds "github.com/fil-forge/libforge/commands/content"
	s3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/iam"
)

// revokerFixture is one access key's cached authorization state: a
// tenant→key delegation deposited in its proof store and a verification key
// cached under its accessKeyId (the prefix-stripped did:key identifier).
type revokerFixture struct {
	key    did.DID
	access string
	dlg    ucan.Delegation
	tenant ucan.Issuer
}

func seedKey(t *testing.T, kp *iam.KeyProofs, keys *iam.VerificationKeyCache, tenants *iam.TenantCache) revokerFixture {
	t.Helper()
	tenant, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	key, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	dlg, err := contentcmds.Retrieve.Delegate(tenant, key.DID(), tenant.DID(), delegation.WithNoExpiration())
	require.NoError(t, err)
	kp.Deposit(key.DID(), dlg)
	access := strings.TrimPrefix(key.DID().String(), did.KeyPrefix)
	keys.Put(access, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: []byte("hmac-" + access)})
	tenants.Put(access, time.Hour, tenant.DID())
	return revokerFixture{key: key.DID(), access: access, dlg: dlg, tenant: tenant}
}

func TestRevokerClearsHolderCachesOnly(t *testing.T) {
	ctx := context.Background()
	kp := iam.NewKeyProofs()
	keys := iam.NewVerificationKeyCache()
	tenants := iam.NewTenantCache()
	r := iam.NewRevoker(kp, keys, tenants, nil)

	a := seedKey(t, kp, keys, tenants)
	b := seedKey(t, kp, keys, tenants)

	affected := r.Revoke(a.dlg.Link())
	require.Equal(t, []did.DID{a.key}, affected)

	// Key A's proof store is gone: a fresh store resolves nothing.
	chain, _, err := kp.For(a.key).ProofChain(ctx, a.dlg.Audience(), a.dlg.Command(), a.tenant.DID())
	require.NoError(t, err)
	require.Empty(t, chain, "revoked key's chains must not resolve")
	_, ok := keys.Get(a.access, s3.KeyKindSigV4)
	require.False(t, ok, "revoked key's verification key must be gone")
	_, ok = tenants.Get(a.access)
	require.False(t, ok, "revoked key's tenant must be gone")

	// Key B is untouched.
	chain, _, err = kp.For(b.key).ProofChain(ctx, b.dlg.Audience(), b.dlg.Command(), b.tenant.DID())
	require.NoError(t, err)
	require.Len(t, chain, 1, "unrelated key's chains must survive")
	_, ok = keys.Get(b.access, s3.KeyKindSigV4)
	require.True(t, ok, "unrelated key's verification key must survive")
	_, ok = tenants.Get(b.access)
	require.True(t, ok, "unrelated key's tenant must survive")

	// Re-delivery of the same revocation is a no-op.
	require.Empty(t, r.Revoke(a.dlg.Link()))
}

func TestRevokerUnknownCIDIsNoOp(t *testing.T) {
	ctx := context.Background()
	kp := iam.NewKeyProofs()
	keys := iam.NewVerificationKeyCache()
	tenants := iam.NewTenantCache()
	r := iam.NewRevoker(kp, keys, tenants, nil)

	a := seedKey(t, kp, keys, tenants)
	// A delegation never deposited anywhere: nothing cached depends on it.
	stranger, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	other, err := contentcmds.Retrieve.Delegate(stranger, stranger.DID(), stranger.DID(), delegation.WithNoExpiration())
	require.NoError(t, err)

	require.Empty(t, r.Revoke(other.Link()))

	chain, _, err := kp.For(a.key).ProofChain(ctx, a.dlg.Audience(), a.dlg.Command(), a.tenant.DID())
	require.NoError(t, err)
	require.Len(t, chain, 1, "unmatched revocation must not touch cached state")
	_, ok := keys.Get(a.access, s3.KeyKindSigV4)
	require.True(t, ok)
}
