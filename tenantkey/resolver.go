package tenantkey

import (
	"context"
	"crypto/ecdh"
	"fmt"
	"net/url"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/did/plc"
	"github.com/fil-forge/ucantone/did/resolver"
)

// Resolver looks a tenant's wrap key up in its DID document.
type Resolver struct {
	docs did.Resolver
	now  func() time.Time
}

// NewResolver returns a Resolver reading DID documents from docs. Callers
// wanting a cache wrap docs themselves (see NewPLCResolver).
func NewResolver(docs did.Resolver) *Resolver {
	return &Resolver{docs: docs, now: time.Now}
}

// NewPLCResolver returns a Resolver over the did:plc directory at endpoint,
// caching resolved documents for ttl. The cache bounds both the directory
// round-trips on the write path and how long a rotation goes unseen. Only
// successful resolutions are cached; the short request timeout keeps a
// directory outage from tying up write slots.
func NewPLCResolver(endpoint url.URL, ttl time.Duration) (*Resolver, error) {
	docs, err := plc.NewResolver(endpoint, plc.WithTimeout(3*time.Second))
	if err != nil {
		return nil, fmt.Errorf("tenantkey: plc resolver: %w", err)
	}
	return NewResolver(resolver.NewCached(docs, ttl)), nil
}

// WrapKey resolves tenant's DID document and returns its current wrap key:
// the "#wrap" verification method, which must be a valid (unexpired,
// unrevoked) X25519 Multikey. The kid is the key's canonical fingerprint,
// derived from the key bytes rather than copied from the document.
func (r *Resolver) WrapKey(ctx context.Context, tenant did.DID) (string, *ecdh.PublicKey, error) {
	doc, err := r.docs.Resolve(ctx, tenant)
	if err != nil {
		return "", nil, fmt.Errorf("tenantkey: resolve %s: %w", tenant, err)
	}
	vm, ok := wrapMethod(doc)
	if !ok {
		return "", nil, fmt.Errorf("%w: %s has no #%s verification method", ErrNoWrapKey, tenant, WrapFragment)
	}
	if !vm.ValidAt(r.now()) {
		return "", nil, fmt.Errorf("%w: %s is expired or revoked", ErrNoWrapKey, vm.ID)
	}
	if vm.Type != did.MultikeyVerificationMethodType {
		return "", nil, fmt.Errorf("%w: %s has type %q", ErrNotX25519, vm.ID, vm.Type)
	}
	mb, _ := vm.Material[did.MultikeyPublicKeyMultibaseProp].(string)
	if mb == "" {
		return "", nil, fmt.Errorf("%w: %s has no %s", ErrNotX25519, vm.ID, did.MultikeyPublicKeyMultibaseProp)
	}
	pub, err := DecodePublicKey(mb)
	if err != nil {
		return "", nil, fmt.Errorf("%s: %w", vm.ID, err)
	}
	return EncodePublicKey(pub), pub, nil
}

// wrapMethod finds the verification method at WrapFragment. Method ids are
// matched on their fragment so both absolute (did:plc:…#wrap) and relative
// (#wrap) ids resolve.
func wrapMethod(doc did.Document) (did.VerificationMethod, bool) {
	if doc.VerificationMethods == nil {
		return did.VerificationMethod{}, false
	}
	for _, vm := range doc.VerificationMethods.All() {
		if vm.ID.URL != nil && vm.ID.Fragment == WrapFragment {
			return vm, true
		}
	}
	return did.VerificationMethod{}, false
}
