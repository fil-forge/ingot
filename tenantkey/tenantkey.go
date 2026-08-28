// Package tenantkey resolves a tenant's wrap key: the X25519 public key that
// is the FEE tenant recipient of every stored object — the FilOne encryption
// design's insurance copy, the one wrap of a blob's CEK that does not depend
// on the region. Hilt mints one per tenant and publishes it in the tenant's
// did:plc document as a Multikey verification method at the fixed fragment
// "#wrap"; the private half stays sealed in Hilt's vault.
//
// The recipient kid is the key's fingerprint: the multibase (base58btc)
// encoding of the multicodec-tagged public key, which is also the identifier
// of the key's did:key. It is deliberately not the verification-method URL.
// A fingerprint names one key forever, whichever fragment currently points at
// it, and is what Hilt's wrap-key registry matches on recovery; the fragment
// is discovery only.
//
// Rotation: Hilt replaces "#wrap" in place. A write within the resolver's
// cache TTL of a rotation still encrypts to the previous key, which Hilt
// archives rather than destroys, so such envelopes stay recoverable.
//
// Like regionkey, this package never touches the FEE API: the write path
// turns (kid, pub) into a fee.Recipient. Writes fail without a wrap key —
// there is no plaintext or region-only fallback.
package tenantkey

import (
	"context"
	"crypto/ecdh"
	"errors"
)

var (
	// ErrNoTenant reports a request carrying no tenant DID: it did not pass
	// through the Hilt-backed IAM service (e.g. a root-account request).
	ErrNoTenant = errors.New("tenantkey: request has no tenant")
	// ErrNoWrapKey reports a tenant DID document without a usable "#wrap"
	// verification method (absent, expired or revoked).
	ErrNoWrapKey = errors.New("tenantkey: tenant DID document has no wrap key")
	// ErrNotX25519 reports a "#wrap" verification method whose key is not an
	// X25519 public key in Multikey form.
	ErrNotX25519 = errors.New("tenantkey: wrap key is not an X25519 Multikey")
)

// WrapFragment is the fixed DID-document fragment Hilt publishes a tenant's
// current wrap key under (hilt's wrapkey.Fragment).
const WrapFragment = "wrap"

// Source yields the tenant wrap key for the request in ctx: the recipient
// kid (the key's fingerprint) and the X25519 public key to wrap to.
type Source interface {
	WrapKey(ctx context.Context) (kid string, pub *ecdh.PublicKey, err error)
}
