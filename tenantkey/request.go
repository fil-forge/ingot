package tenantkey

import (
	"context"
	"crypto/ecdh"

	"github.com/fil-forge/ingot/internal/reqscope"
)

// RequestSource is the production Source: the tenant DID comes from the
// request context (set by the IAM service from Hilt's authorize response),
// its wrap key from the Resolver. A request without a tenant — one that never
// went through the Hilt-backed IAM service — is ErrNoTenant, and the write
// fails closed.
type RequestSource struct {
	res *Resolver
}

var _ Source = (*RequestSource)(nil)

// NewRequestSource returns a RequestSource over res.
func NewRequestSource(res *Resolver) *RequestSource {
	return &RequestSource{res: res}
}

// WrapKey implements Source.
func (s *RequestSource) WrapKey(ctx context.Context) (string, *ecdh.PublicKey, error) {
	tenant, ok := reqscope.Tenant(ctx)
	if !ok {
		return "", nil, ErrNoTenant
	}
	return s.res.WrapKey(ctx, tenant)
}
