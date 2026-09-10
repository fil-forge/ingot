package iam

import (
	"strings"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"
)

// Revoker applies invalidations to the local authorization caches: it is the
// bridge between the firehose consumer (which sees delegation CIDs and
// principal references) and the per-access-key state the local fast path
// reads (see Service.authorizeLocal). After Revoke or InvalidatePrincipal
// returns for key K, a request signed with K cannot authorize locally — its
// proof store is gone (defeating the chain probe and the cached action set)
// and so are its verification keys (defeating local signature verification)
// — so the request falls through to Hilt, which re-authorizes it against the
// current access or refuses.
type Revoker struct {
	proofs  *KeyProofs
	keys    *VerificationKeyCache
	tenants *TenantCache
	logger  *zap.Logger
}

// NewRevoker returns a Revoker over the caches the IAM service populates.
func NewRevoker(proofs *KeyProofs, keys *VerificationKeyCache, tenants *TenantCache, logger *zap.Logger) *Revoker {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Revoker{proofs: proofs, keys: keys, tenants: tenants, logger: logger}
}

// Revoke clears the caches of every access key whose proof store holds the
// revoked delegation, returning the affected key DIDs. An empty result means
// nothing cached referenced the delegation — then no local authorization
// decision depended on it and there was nothing to clear (Hilt remains
// authoritative for everything uncached). Idempotent: re-delivery of a
// revocation is a no-op.
func (r *Revoker) Revoke(revoked cid.Cid) []did.DID {
	affected := r.proofs.InvalidateHolders(revoked)
	r.clearKeys(affected, "iam: access key caches cleared by revocation",
		zap.Stringer("revoked", revoked))
	return affected
}

// InvalidatePrincipal clears the caches of every access key bound to
// (tenant, principal), returning the affected key DIDs. Hilt publishes the
// invalidation before it commits a change to what the principal may do, so
// the next request signed with one of these keys is re-authorized against
// the new access. An empty result means no cached key belonged to the
// principal — nothing local depended on its access. Idempotent: re-delivery
// of an invalidation, and an invalidation for a principal ingot has never
// seen, are both no-ops.
func (r *Revoker) InvalidatePrincipal(tenant did.DID, principal string) []did.DID {
	affected := r.proofs.InvalidatePrincipal(PrincipalRef{Tenant: tenant, Principal: principal})
	r.clearKeys(affected, "iam: access key caches cleared by principal invalidation",
		zap.Stringer("tenant", tenant), zap.String("principal", principal))
	return affected
}

// clearKeys drops the verification key and tenant cached for each access
// key, which is what stops a local authorization: with no verification key
// the signature cannot be checked locally, and with no tenant the write
// path has nothing to encrypt to. Both caches are keyed by the accessKeyId
// — the key's did:key identifier with the prefix stripped (see
// GetUserAccountForRequest).
func (r *Revoker) clearKeys(affected []did.DID, reason string, fields ...zap.Field) {
	for _, key := range affected {
		access := strings.TrimPrefix(key.String(), did.KeyPrefix)
		r.keys.Delete(access)
		r.tenants.Delete(access)
		r.logger.Info(reason, append([]zap.Field{zap.Stringer("access", key)}, fields...)...)
	}
}
