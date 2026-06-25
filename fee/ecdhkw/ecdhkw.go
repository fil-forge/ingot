// Package ecdhkw implements ECDH-ES+A256KW key wrapping over X25519: it
// encrypts a content-encryption key (CEK) to a recipient's X25519 public key so
// that only the holder of the matching private key can recover it.
//
// This is the tenant-recipient wrap of the FilOne encryption design. A fresh
// ephemeral X25519 key pair is generated for every Wrap; an ECDH against the
// recipient's static public key yields a shared secret, the COSE Concat-KDF
// (RFC 9053 §5.1, see kdf.go) turns that secret into a 256-bit key-encryption
// key, and AES Key Wrap (RFC 3394, the sibling fee/aeskw package) wraps the CEK
// under it. Unwrap reverses the process with the recipient's private key. Two
// useful consequences fall out of the construction:
//
//   - Recovery is self-checking. AES-KW carries an integrity check, so an
//     unwrap with the wrong private key — or against a tampered wrapped key or
//     ephemeral point — fails with an error rather than returning garbage.
//   - Each wrap is unlinkable. Because the ephemeral key is fresh per call,
//     wrapping the same CEK to the same recipient twice produces different
//     ephemeral public keys, different KEKs, and different wrapped bytes.
//
// The package is the low-level cryptographic primitive only. It returns the
// ephemeral public key and wrapped CEK as plain values; serializing them into
// a COSE_Recipient (the ephemeral key in the ephemeral-key header, the wrapped
// CEK as the recipient ciphertext, keyed by a kid) is the job of the
// higher-level fee package. Keys are passed as crypto/ecdh values directly; any
// custody or key-provider abstraction lives above this layer.
package ecdhkw

import (
	"crypto/ecdh"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/fil-forge/ingot/fee/aeskw"
)

// AlgorithmECDHESA256KW is the COSE algorithm identifier (RFC 9053, IANA COSE
// Algorithms registry) for the ECDH-ES + A256KW key-agreement-with-key-wrap
// scheme implemented here. It is exported for the higher-level fee package to
// place in the COSE_Recipient algorithm header; this package does no envelope
// encoding itself.
const AlgorithmECDHESA256KW = -31

// algA256KW is the COSE algorithm identifier for AES-256 Key Wrap. It is the
// algorithm the derived key feeds, so it is the AlgorithmID embedded in the
// Concat-KDF context (RFC 9053 §5.2) that binds the KEK to its purpose.
const algA256KW = -5

// kekLen is the length in bytes of the A256KW key-encryption key the KDF
// derives: 256 bits.
const kekLen = 32

// Wrapped is the result of Wrap: a CEK encrypted to a recipient's X25519 key
// with ECDH-ES+A256KW. Both fields are required to Unwrap.
//
// The higher-level fee package serializes these into a COSE_Recipient — the
// ephemeral key into the COSE ephemeral-key header (label -1), the wrapped CEK
// as the recipient ciphertext — alongside the kid that names the recipient.
// This package itself stays free of any envelope encoding.
type Wrapped struct {
	// EphemeralPublicKey is the sender's one-time X25519 public key, freshly
	// generated for this wrap. The recipient combines it with their private
	// key to reconstruct the key-encryption key.
	EphemeralPublicKey *ecdh.PublicKey
	// WrappedCEK is the AES-KW (RFC 3394) output: the CEK encrypted under the
	// derived KEK, 8 bytes longer than the CEK.
	WrappedCEK []byte
}

// Wrap encrypts cek to recipientPub using ECDH-ES+A256KW over X25519, drawing a
// fresh ephemeral key pair from crypto/rand. It returns the wrapped CEK and the
// ephemeral public key the recipient needs to unwrap it.
//
// recipientPub must be an X25519 key. cek must be a valid AES key — a multiple
// of 8 bytes, at least 16 (so 16, 24, or 32 bytes; FilOne CEKs are 32). The cek
// slice is not retained or modified.
func Wrap(recipientPub *ecdh.PublicKey, cek []byte) (*Wrapped, error) {
	if recipientPub == nil {
		return nil, errors.New("ecdhkw nil recipient public key")
	}
	if recipientPub.Curve() != ecdh.X25519() {
		return nil, errors.New("ecdhkw recipient public key is not X25519")
	}
	if len(cek) < 16 || len(cek)%8 != 0 {
		return nil, fmt.Errorf("ecdhkw CEK must be a multiple of 8 bytes and at least 16, got %d", len(cek))
	}

	// A fresh ephemeral key pair per wrap is what makes the output unlinkable.
	// The output can't be pinned by seeding crypto/rand: crypto/ecdh key
	// generation deliberately consumes a nondeterministic amount of entropy
	// (see randutil.MaybeReadByte). Tests pin the scheme with a decryption
	// vector (a fixed Wrapped that must Unwrap to a known CEK) instead.
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("ecdhkw generating ephemeral key: %w", err)
	}

	kek, err := deriveKEK(ephemeral, recipientPub)
	if err != nil {
		return nil, err
	}
	defer zero(kek)

	wrappedCEK, err := aeskw.Wrap(kek, cek)
	if err != nil {
		return nil, fmt.Errorf("ecdhkw wrapping CEK: %w", err)
	}
	return &Wrapped{
		EphemeralPublicKey: ephemeral.PublicKey(),
		WrappedCEK:         wrappedCEK,
	}, nil
}

// Unwrap recovers the CEK from w using recipientPriv. It reconstructs the
// key-encryption key by ECDH between recipientPriv and the ephemeral public key
// in w, then AES-KW-unwraps the CEK.
//
// It returns an error if recipientPriv is the wrong key for this wrap, if the
// ephemeral key or wrapped CEK was tampered with, or if either key is not
// X25519. A wrong-key unwrap surfaces as aeskw.ErrIntegrity (wrapped), so
// callers may match it with errors.Is.
func Unwrap(recipientPriv *ecdh.PrivateKey, w *Wrapped) ([]byte, error) {
	if recipientPriv == nil {
		return nil, errors.New("ecdhkw nil recipient private key")
	}
	if recipientPriv.Curve() != ecdh.X25519() {
		return nil, errors.New("ecdhkw recipient private key is not X25519")
	}
	if w == nil {
		return nil, errors.New("ecdhkw nil wrapped value")
	}
	if w.EphemeralPublicKey == nil {
		return nil, errors.New("ecdhkw nil ephemeral public key")
	}
	if w.EphemeralPublicKey.Curve() != ecdh.X25519() {
		return nil, errors.New("ecdhkw ephemeral public key is not X25519")
	}

	kek, err := deriveKEK(recipientPriv, w.EphemeralPublicKey)
	if err != nil {
		return nil, err
	}
	defer zero(kek)

	cek, err := aeskw.Unwrap(kek, w.WrappedCEK)
	if err != nil {
		return nil, fmt.Errorf("ecdhkw unwrapping CEK: %w", err)
	}
	return cek, nil
}

// deriveKEK performs the ECDH-ES key derivation shared by Wrap and Unwrap: an
// X25519 ECDH between local and remote, then the COSE Concat-KDF over the
// shared secret to produce the A256KW key-encryption key. ECDH symmetry is what
// makes the two paths — (ephemeral private, recipient public) on wrap and
// (recipient private, ephemeral public) on unwrap — derive the same KEK.
//
// crypto/ecdh's X25519 ECDH returns an error for a low-order ephemeral point
// (one that would force the shared secret to all-zeros), which propagates here.
func deriveKEK(local *ecdh.PrivateKey, remote *ecdh.PublicKey) ([]byte, error) {
	z, err := local.ECDH(remote)
	if err != nil {
		return nil, fmt.Errorf("ecdhkw ECDH: %w", err)
	}
	defer zero(z)

	context := kdfContext(algA256KW, kekLen*8, nil)
	return concatKDF(z, context, kekLen), nil
}

// zero overwrites b, a best-effort wipe of derived key material (the KEK and
// the ECDH shared secret) once it is no longer needed.
func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
