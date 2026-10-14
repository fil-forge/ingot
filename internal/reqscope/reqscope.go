// Package reqscope carries request-scoped authorization state between the
// auth layer and the network read tier through the request context.
//
// The IAM service resolves the requesting access key's proof store and
// stashes it on the request (a fasthttp user value, via fiber's Locals);
// blockstore.Forge reads it back when it needs to authorize an onward
// /content/retrieve. The two never import each other — they share only this
// key. Context is the right conduit here: the store is produced at the very
// top (auth) and consumed at the very bottom (retrieval), with nothing in
// between depending on it.
package reqscope

import (
	"context"

	"github.com/fil-forge/libforge/commands/s3"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
)

// proofStoreContextKey is the private context / user-value key type, so it can't
// collide with any other package's keys.
type proofStoreContextKey struct{}

var proofStoreKey any = proofStoreContextKey{}

// ProofStoreKey returns the fasthttp/fiber user-value (and context) key under
// which the request-scoped [ucanlib.ProofStore] is stored.
func ProofStoreKey() any {
	return proofStoreKey
}

// ProofStore returns the request-scoped retrieval proof store, or ok=false
// when none was set (a context that never went through the Hilt-backed IAM
// service, such as a background flush).
func ProofStore(ctx context.Context) (ucanlib.ProofStore, bool) {
	ps, ok := ctx.Value(proofStoreKey).(ucanlib.ProofStore)
	return ps, ok
}

type requestContextKey struct{}

var requestKey any = requestContextKey{}

// RequestKey returns context key for which the [s3.Request] is stored.
func RequestKey() any {
	return requestKey
}

// Request returns the s3.Request stored in the context, or ok=false when none
// was set.
func Request(ctx context.Context) (s3.Request, bool) {
	req, ok := ctx.Value(requestKey).(s3.Request)
	return req, ok
}

type tenantContextKey struct{}

var tenantKey any = tenantContextKey{}

// TenantKey returns the context key under which the requesting access key's
// tenant DID is stored. The IAM service learns it from Hilt's authorize
// response (and its per-key cache on the local fast path); the write path
// resolves the tenant's wrap key from it — the FEE tenant recipient every
// stored object is encrypted to.
func TenantKey() any {
	return tenantKey
}

// Tenant returns the tenant DID stored in the context, or ok=false when none
// was set (a context that never went through the Hilt-backed IAM service).
func Tenant(ctx context.Context) (did.DID, bool) {
	tenant, ok := ctx.Value(tenantKey).(did.DID)
	return tenant, ok && tenant.Defined()
}

type etagContextKey struct{}

var etagKey any = etagContextKey{}

// ETagKey returns the fasthttp/fiber user-value key under which a bucket
// policy request carries a *string holder for its response ETag. versitygw's
// backend seam returns no headers, so the gateway middleware installs the
// holder, the backend fills it from Hilt's answer, and the middleware sets
// the header from it once the controller has run.
func ETagKey() any {
	return etagKey
}

// SetETag fills the request's ETag holder; a context without one (any
// request but a bucket policy operation) ignores it.
func SetETag(ctx context.Context, etag string) {
	if holder, ok := ctx.Value(etagKey).(*string); ok {
		*holder = etag
	}
}
