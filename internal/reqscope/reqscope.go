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

	ucanlib "github.com/fil-forge/libforge/ucan"
)

// proofStoreKey is the private context / user-value key type, so it can't
// collide with any other package's keys.
type proofStoreKey struct{}

// Key is the fasthttp/fiber user-value (and context) key under which the
// request-scoped retrieval proof store is stored. Exported so the setter
// (fiber Ctx.Locals in the auth layer) and the reader below agree on it.
var Key any = proofStoreKey{}

// ProofStore returns the request-scoped retrieval proof store, or ok=false
// when none was set (e.g. a root-account request that never went through the
// Hilt-backed IAM service).
func ProofStore(ctx context.Context) (ucanlib.ProofStore, bool) {
	ps, ok := ctx.Value(Key).(ucanlib.ProofStore)
	return ps, ok
}
