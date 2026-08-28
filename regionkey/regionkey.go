// Package regionkey implements the region CEK wrap of the FilOne encryption
// design: the read-path copy of a per-object content-encryption key (CEK),
// wrapped to the region's own key and stored in Ingot's local database so a
// region can serve GETs without a synchronous call to any central FilOne
// service (see the encryption RFC, "Region wrap").
//
// Per the regional security and key management RFC (which amends the
// encryption RFC's original A256KW choice), the region wrap is AES-256-GCM,
// context-bound to the blob it protects: the (space, blob digest) pair is
// bound into the wrap, so wrap material transplanted between DB rows fails to
// unwrap. In production the wrap runs inside the region's secrets manager
// (OpenBao transit, aes256-gcm96 with derived per-context keys), so the
// region KEK is non-exportable and never enters Ingot's process; host
// hardening, not process-level machinery, covers memory. This package
// defines the seam Ingot calls, plus an [InProcessProvider] with the same
// semantics for tests and development.
//
// This is Ingot-side key-management infrastructure, not part of the FEE
// library. Per the RFC the region wrap is not a COSE recipient and never
// enters the envelope; it is stored out-of-band as Ingot's own bytes in
// Ingot's DB, so nothing here touches the COSE envelope, recipients, or the
// fee package's public API. Unlike the asymmetric tenant-recipient wrap in
// the envelope header, the region wrap is symmetric: the CEK is wrapped
// straight under a 256-bit region key-encryption key (KEK).
//
// # The Provider seam
//
// [Provider] is the interface Ingot calls: Wrap(ctx, binding, cek) /
// Unwrap(ctx, binding, wrapped), where [BindingContext] is the (space, blob
// digest) pair the wrap is bound to. How many physical region KEKs exist
// stays entirely inside the implementation: v1 holds a single region-wide
// key and every binding resolves to it; if the region-key-cardinality decision
// (FIL-572) later splits the key per bucket or per tenant, only the Provider
// implementation changes, not any call site. Because the seam is an
// interface, the [InProcessProvider] and the production [OpenBaoProvider]
// are interchangeable without touching Ingot's wrap/unwrap call sites.
//
// # Versioning
//
// Every [WrappedKey] records the [KeyVersion] of the region KEK that produced
// it. Ingot stores that version alongside the wrapped bytes, so a later
// Unwrap selects the same KEK version even across a region-key rotation, and
// so old key versions can be archived rather than destroyed. This is the
// encryption RFC's rotation-readiness groundwork ("Every envelope and DB row
// carries a versioned kid from day one"); the rotation flow itself ships
// later.
package regionkey

import (
	"context"
	"errors"

	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"
)

// ErrUnknownVersion is returned by [Provider.Unwrap] when asked to unwrap
// under a region KEK version the provider does not hold — for example a wrap
// produced by a different region, or a version that was never provisioned.
// Callers can match it with [errors.Is].
//
// A wrong-key or wrong-binding unwrap where the provider *does* hold the
// named version instead fails ciphertext authentication and surfaces as
// [ErrAuthentication]; the two cases are distinct.
var ErrUnknownVersion = errors.New("regionkey: unknown key version")

// ErrAuthentication is returned (wrapped) by [Provider.Unwrap] when the
// wrapped bytes fail to authenticate: the KEK is wrong for this wrap, the
// binding context differs from the one the CEK was wrapped under, or the
// bytes were tampered with. The causes are deliberately indistinguishable —
// AES-256-GCM authenticates key, binding context, and ciphertext as one.
var ErrAuthentication = errors.New("regionkey: ciphertext authentication failed")

// BindingContext is what a region wrap is bound to: the space and blob digest
// the wrapped CEK protects. Per the regional security RFC the wrap is
// context-bound to exactly this pair, so a wrapped CEK authenticates only
// against its own (Space, Digest) and wrap material transplanted between DB
// rows fails with [ErrAuthentication]. Wrap and Unwrap must be given the
// same BindingContext.
//
// Whether different bindings also resolve to different physical KEKs remains
// the Provider's business (the region-key-cardinality decision, FIL-572); v1
// implementations hold a single region-wide KEK.
//
// Every Provider implementation encodes the pair with [bindingBytes], so the
// bound context is byte-identical across providers.
type BindingContext struct {
	// Space is the Forge space that owns the blob.
	Space did.DID
	// Digest is the ciphertext blob's multihash, as keyed in
	// blob_encryption_params.
	Digest multihash.Multihash
}

// bindingBytes is the canonical byte encoding of a binding context, shared by
// every Provider implementation: [InProcessProvider] feeds it to HKDF as the
// subkey info, [OpenBaoProvider] sends it as the transit derivation context.
// A DID string contains no NUL, so the separator makes the encoding
// unambiguous.
func bindingBytes(b BindingContext) []byte {
	space := b.Space.String()
	out := make([]byte, 0, len(space)+1+len(b.Digest))
	out = append(out, space...)
	out = append(out, 0)
	out = append(out, b.Digest...)
	return out
}

// KeyVersion identifies a specific version of a region KEK. It is stored
// alongside the wrapped CEK (see [WrappedKey]) so that [Provider.Unwrap] can
// select the KEK version that produced a wrap, which is what lets a
// region-key rotation archive old versions instead of destroying them. The
// empty string is not a valid version.
type KeyVersion string

// WrappedKey is a region-wrapped CEK together with the version of the region
// KEK that wrapped it. Ingot persists both fields in its local database (the
// region_wrapped_cek and region_key_version columns of
// blob_encryption_params); Unwrap needs the version to recover the CEK after
// a rotation.
//
// The wrapped CEK travels only in Ingot's mutable local database and never
// enters the FEE envelope or the MST — it is the shreddable, rotatable
// read-path copy, kept separate from the immutable tenant recipient that
// lives in the envelope.
type WrappedKey struct {
	// Version identifies the region KEK that produced Ciphertext.
	Version KeyVersion
	// Ciphertext is the CEK wrapped under the region KEK: opaque to callers
	// and specific to the Provider that produced it. [InProcessProvider]
	// emits nonce ‖ AES-256-GCM ciphertext ‖ tag (28 bytes longer than the
	// CEK); [OpenBaoProvider] emits the transit ciphertext string's bytes
	// ("vault:vN:…").
	Ciphertext []byte
}

// Provider wraps and unwraps region CEKs. It is the seam Ingot calls on the
// write path (Wrap the fresh CEK for storage) and the read path (Unwrap it to
// decrypt). Implementations decide how many physical KEKs back the region and
// where they live, so they are interchangeable without changing call sites;
// see the package doc.
type Provider interface {
	// Wrap encrypts cek under the current region KEK, bound to binding, and
	// returns the wrapped CEK tagged with that KEK's version. cek is treated
	// as opaque plaintext (FilOne CEKs are 32 bytes); the slice is neither
	// retained nor modified.
	Wrap(ctx context.Context, binding BindingContext, cek []byte) (WrappedKey, error)

	// Unwrap reverses Wrap, recovering the CEK from wrapped using the region
	// KEK named by wrapped.Version, bound to binding — which must equal the
	// binding the CEK was wrapped under. It returns [ErrUnknownVersion] if no
	// KEK of that version is available, and an error wrapping
	// [ErrAuthentication] if the KEK or binding is wrong for this wrap or the
	// wrapped bytes were tampered with. The returned CEK is freshly allocated
	// and owned by the caller, who should zero it once done.
	Unwrap(ctx context.Context, binding BindingContext, wrapped WrappedKey) ([]byte, error)
}
