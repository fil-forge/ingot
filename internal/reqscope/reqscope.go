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
// when none was set (e.g. a root-account request that never went through the
// Hilt-backed IAM service).
func ProofStore(ctx context.Context) (ucanlib.ProofStore, bool) {
	ps, ok := ctx.Value(proofStoreKey).(ucanlib.ProofStore)
	return ps, ok
}

// WithoutProofStore returns a context whose request-scoped proof store is
// masked. Hilt delegates Forge commands per S3 permission, and some
// permissions (s3:DeleteBucket) carry no blob commands at all — a flow that
// must invoke blob capabilities from such a request (DeleteBucket's implicit
// abort of in-flight multipart uploads) hides the request store so the
// uploader falls back to the space authority captured at UploadPart.
func WithoutProofStore(ctx context.Context) context.Context {
	return context.WithValue(ctx, proofStoreKey, nil)
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
