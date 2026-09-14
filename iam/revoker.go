package iam

import (
	"strings"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"
)

// Revoker applies UCAN revocations to the local authorization caches: it is
// the bridge between the revocation-firehose consumer (which sees only
// delegation CIDs) and the per-access-key state the local fast path reads
// (see Service.authorizeLocal). After Revoke returns for a delegation in key
// K's chains, a request signed with K cannot authorize locally — its proof
// store is gone (defeating the chain probe) and so are its verification keys
// (defeating local signature verification) — so the request falls through to
// Hilt, which refuses for a deleted key.
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
	for _, key := range affected {
		// The verification-key cache is keyed by the accessKeyId — the key's
		// did:key identifier with the prefix stripped (see
		// GetUserAccountForRequest).
		access := strings.TrimPrefix(key.String(), did.KeyPrefix)
		r.keys.Delete(access)
		r.tenants.Delete(access)
		r.logger.Info("iam: access key caches cleared by revocation",
			zap.Stringer("access", key), zap.Stringer("revoked", revoked))
	}
	return affected
}
