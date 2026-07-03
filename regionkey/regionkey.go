// Package regionkey implements the region CEK wrap of the FilOne encryption
// design: the read-path copy of a per-object content-encryption key (CEK),
// wrapped to the region's own key and stored in Ingot's local database so a
// region can serve GETs without a synchronous call to any central FilOne
// service (see the RFC, "Region wrap").
//
// This is Ingot-side key-management infrastructure, not part of the FEE
// library. Per the RFC the region wrap is not a COSE recipient and never enters
// the envelope; it is stored out-of-band as Ingot's own bytes in Ingot's DB, so
// nothing here touches the COSE envelope, recipients, or the fee package's
// public API. The package borrows one primitive — A256KW (RFC 3394 AES Key
// Wrap) — from fee/aeskw, which lives in the fee tree only because the
// tenant-recipient wrap (fee/ecdhkw, ECDH-ES+A256KW) built it there as a shared
// building block. Unlike that asymmetric tenant wrap, the region wrap is
// symmetric: the CEK is wrapped straight under a 256-bit region key-encryption
// key (KEK).
//
// # The Provider seam
//
// [Provider] is the interface Ingot calls. It is scope-parameterized —
// Wrap(ctx, scope, cek) / Unwrap(ctx, scope, wrapped) — so that the question of
// how many physical region KEKs exist stays entirely inside the implementation.
// v1 holds a single region-wide key and every [Scope] resolves to it; if the
// region-key-cardinality decision (FIL-572) later splits the key per bucket or
// per tenant, only the Provider implementation changes, not any call site. A
// future non-exportable PKCS#11/HSM Provider can ignore the scope entirely,
// since non-exportability already gives the blast-radius protection multiple
// software keys exist to approximate. Because the seam is an interface, a mock,
// the in-process [SoftwareProvider], or an HSM-backed provider are
// interchangeable without touching Ingot's wrap/unwrap call sites.
//
// # Versioning
//
// Every [WrappedKey] records the [KeyVersion] of the region KEK that produced
// it. Ingot stores that version alongside the wrapped bytes, so a later Unwrap
// selects the same KEK version even across a region-key rotation, and so old
// key versions can be archived rather than destroyed. This is the RFC's
// rotation-readiness groundwork ("Every envelope and DB row carries a versioned
// kid from day one"); the rotation flow itself ships later.
//
// # Exposure hygiene
//
// A256KW is not natively offered by Vault Transit or most software KMS backends
// — it is primarily an HSM/PKCS#11 primitive — so the v1 [SoftwareProvider]
// must import the raw KEK into process memory to perform the wrap. It minimizes
// the exposure window: it fetches the KEK from a [KEKSource] only for the
// duration of a single wrap or unwrap, holds it in a locked, non-swappable
// buffer ([KEK], backed by mlock), and zeroes that buffer immediately after the
// operation on every path, including errors. This bounds raw-KEK exposure to
// microseconds per operation and is the primary practical mitigation until an
// HSM-backed Provider removes the export requirement entirely.
package regionkey

import (
	"context"
	"errors"
)

// ErrUnknownVersion is returned by a [KEKSource] (and surfaced by
// [Provider.Unwrap]) when asked for a region KEK version the source does not
// hold — for example a wrap produced by a different region, or a version that
// was never provisioned. Callers can match it with [errors.Is].
//
// A wrong-key unwrap where the source *does* hold the named version instead
// fails the A256KW integrity check and surfaces as fee/aeskw.ErrIntegrity
// (wrapped); the two cases are distinct.
var ErrUnknownVersion = errors.New("region: unknown key version")

// Scope identifies which region key protects a CEK. It carries the dimensions a
// Provider might key on if the region-key-cardinality decision (FIL-572) later
// splits the single region key; the region itself is implicit, since a Provider
// is bound to one region. In v1 there is exactly one region key and every Scope
// resolves to it, so the fields are advisory — the [SoftwareProvider] ignores
// them — but threading the Scope through from day one means that decision can
// change without altering any wrap/unwrap call site in Ingot.
type Scope struct {
	// Tenant is the DID of the tenant that owns the object's bucket, if known.
	Tenant string
	// Bucket is the name of the bucket the object belongs to, if known.
	Bucket string
}

// KeyVersion identifies a specific version of a region KEK. It is stored
// alongside the wrapped CEK (see [WrappedKey]) so that [Provider.Unwrap] can
// select the KEK version that produced a wrap, which is what lets a region-key
// rotation archive old versions instead of destroying them. The empty string
// is not a valid version.
type KeyVersion string

// WrappedKey is a region-wrapped CEK together with the version of the region
// KEK that wrapped it. Ingot persists both fields in its local database (the
// wrapped-CEK bytes and a key-version column on the object's part row); Unwrap
// needs the version to recover the CEK after a rotation.
//
// The wrapped CEK travels only in Ingot's mutable local database and never
// enters the FEE envelope or the MST — it is the shreddable, rotatable read-path
// copy, kept separate from the immutable tenant recipient that lives in the
// envelope.
type WrappedKey struct {
	// Version identifies the region KEK that produced Ciphertext.
	Version KeyVersion
	// Ciphertext is the A256KW (RFC 3394) output: the CEK wrapped under the
	// region KEK, exactly 8 bytes longer than the CEK.
	Ciphertext []byte
}

// Provider wraps and unwraps region CEKs. It is the seam Ingot calls on the
// write path (Wrap the fresh CEK for storage) and the read path (Unwrap it to
// decrypt). Implementations decide how many physical KEKs back the region and
// where they live, so they are interchangeable without changing call sites;
// see the package doc.
type Provider interface {
	// Wrap encrypts cek under the current region KEK for scope and returns the
	// wrapped CEK tagged with that KEK's version. cek must be a valid AES key —
	// a multiple of 8 bytes, at least 16 (FilOne CEKs are 32). The cek slice is
	// neither retained nor modified.
	Wrap(ctx context.Context, scope Scope, cek []byte) (WrappedKey, error)

	// Unwrap reverses Wrap, recovering the CEK from wrapped using the region KEK
	// named by wrapped.Version for scope. It returns [ErrUnknownVersion] if no
	// KEK of that version is available, and an error wrapping
	// fee/aeskw.ErrIntegrity if the KEK is wrong for this wrap or the wrapped
	// bytes were tampered with. The returned CEK is freshly allocated and owned
	// by the caller, who should zero it once done.
	Unwrap(ctx context.Context, scope Scope, wrapped WrappedKey) ([]byte, error)
}

// KEKSource custodies the raw region key-encryption keys the [SoftwareProvider]
// imports to perform A256KW in software. It is the Vault-style seam — the same
// fetch-by-identity shape Hilt uses for secret custody — so an in-memory
// single-key source ([StaticKEKSource]) backs v1 and a Vault-backed source can
// replace it without touching the Provider. A future PKCS#11/HSM Provider
// avoids this seam entirely by never exporting its key.
//
// Both methods return the KEK in a locked [KEK] buffer whose ownership passes
// to the caller. The caller MUST call [KEK.Destroy] — which zeroes and unlocks
// the memory — as soon as the single wrap or unwrap completes; [SoftwareProvider]
// does so with a defer on every path.
type KEKSource interface {
	// CurrentKEK returns the region KEK that new wraps for scope must use,
	// together with the version that names it. The version must be non-empty.
	CurrentKEK(ctx context.Context, scope Scope) (KeyVersion, *KEK, error)

	// KEKAt returns the region KEK identified by version for scope. It returns
	// [ErrUnknownVersion] if the source holds no such version.
	KEKAt(ctx context.Context, scope Scope, version KeyVersion) (*KEK, error)
}
