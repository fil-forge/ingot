package fee

import (
	"crypto/ecdh"
	"errors"
	"fmt"

	"github.com/fil-forge/ingot/fee/aeskw"
	"github.com/fil-forge/ingot/fee/cose"
	"github.com/fil-forge/ingot/fee/ecdhkw"
)

// COSE_Key parameters for the X25519 ephemeral public key an ECDH-ES+A256KW
// recipient carries in its ephemeral-key header (label -1). The ephemeral key
// is a self-describing COSE_Key map — not raw bytes — so a decoder learns the
// curve from the key itself; this matches standard COSE (RFC 9052 §7,
// RFC 9053 §7.1; IANA "COSE Key Common Parameters" and "COSE Key Type
// Parameters") and the foc-encryption reference. The map is:
//
//	{ 1: 1 (kty: OKP), -1: 4 (crv: X25519), -2: <x, the public-key bytes> }
const (
	coseKeyLabelKty    int64 = 1  // key type (common parameter)
	coseKeyLabelCrv    int64 = -1 // curve (OKP key-type parameter)
	coseKeyLabelXCoord int64 = -2 // public key (OKP key-type parameter)

	coseKeyTypeOKP     int64 = 1 // kty value: Octet Key Pair
	coseKeyCurveX25519 int64 = 4 // crv value: X25519
)

// Recipient wraps a content-encryption key (CEK) into an in-envelope
// COSE_Recipient entry: the wrapped CEK travels in the envelope, and recovery
// unwraps it with the matching key (see [RecipientUnwrapper]). [Encrypt] calls
// it once per recipient. For the alternative where the CEK is supplied out of
// band rather than carried in the envelope, see [EncryptWithCEK] and
// [DecryptWithCEK].
//
// Recipient is a sealed interface: the only implementations are the ones
// constructed by [NewECDHESRecipient] and [NewA256KWRecipient], so the set of
// key-wrap algorithms that can appear in a FEE envelope stays controlled.
type Recipient interface {
	// validate checks the recipient's invariants (a usable key and kid) without
	// doing any wrapping, so Encrypt can reject a malformed recipient before it
	// starts streaming the plaintext.
	validate() error
	// wrap encrypts cek for this recipient and returns the COSE_Recipient
	// entry — its key-wrap algorithm and kid (and, for a key-agreement scheme,
	// the ephemeral public key) in the headers, and the wrapped CEK as the
	// recipient ciphertext.
	wrap(cek []byte) (*cose.Recipient, error)
}

// ecdhESRecipient wraps the CEK to an X25519 public key with ECDH-ES+A256KW.
type ecdhESRecipient struct {
	pub *ecdh.PublicKey
	kid []byte
}

// NewECDHESRecipient returns a [Recipient] that wraps the CEK to pub with
// ECDH-ES+A256KW: a fresh ephemeral X25519 key agreement derives a key-encryption
// key, which A256KW-wraps the CEK. pub must be an X25519 key (the only curve the
// scheme supports). kid names the recipient key — e.g. a DID verification method
// ID — and must be non-empty; the library treats it as opaque and records it in
// the recipient entry so an [NewECDHESUnwrapper] with the same kid can be matched
// to it.
func NewECDHESRecipient(kid []byte, pub *ecdh.PublicKey) Recipient {
	return &ecdhESRecipient{pub: pub, kid: kid}
}

func (r *ecdhESRecipient) validate() error {
	if len(r.kid) == 0 {
		return errors.New("ECDH-ES recipient requires a non-empty kid")
	}
	if r.pub == nil {
		return errors.New("nil ECDH-ES recipient public key")
	}
	if r.pub.Curve() != ecdh.X25519() {
		return errors.New("ECDH-ES recipient public key is not X25519")
	}
	return nil
}

func (r *ecdhESRecipient) wrap(cek []byte) (*cose.Recipient, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	wrapped, err := ecdhkw.Wrap(r.pub, cek)
	if err != nil {
		return nil, fmt.Errorf("fee: ECDH-ES+A256KW wrap: %w", err)
	}
	// The ephemeral public key rides in the ephemeral-key header as a
	// self-describing COSE_Key (kty=OKP, crv=X25519), so the unwrapper recovers
	// the curve from the key rather than out of band.
	return &cose.Recipient{
		Headers: cose.Headers{
			Protected: cose.Header{}.
				Set(cose.HeaderLabelAlg, cose.AlgECDHESA256KW),
			Unprotected: cose.Header{}.
				Set(cose.HeaderLabelKID, r.kid).
				Set(cose.HeaderLabelEphemeralKey, map[any]any{
					coseKeyLabelKty:    coseKeyTypeOKP,
					coseKeyLabelCrv:    coseKeyCurveX25519,
					coseKeyLabelXCoord: wrapped.EphemeralPublicKey.Bytes(),
				}),
		},
		Ciphertext: wrapped.WrappedCEK,
	}, nil
}

// a256kwRecipient wraps the CEK under a symmetric key-encryption key (KEK) with
// A256KW (RFC 3394). Its kid is supplied by the caller and names the KEK.
type a256kwRecipient struct {
	kid []byte
	kek []byte
}

// NewA256KWRecipient returns a [Recipient] that wraps the CEK under kek with
// A256KW. kid names the KEK and is recorded in the recipient entry so an
// [NewA256KWUnwrapper] holding the same KEK can be matched to it; kid must be
// non-empty. kek must be 32 bytes: A256KW is AES-256 Key Wrap, and the recipient
// declares that algorithm, so a shorter key would misname the wrap.
//
// Where the KEK is held by an external custody service that unwraps the CEK
// itself, use [DecryptWithCEK] on recovery instead of this recipient — the CEK
// then never needs to travel in the envelope.
func NewA256KWRecipient(kid, kek []byte) Recipient {
	return &a256kwRecipient{kid: kid, kek: kek}
}

func (r *a256kwRecipient) validate() error {
	if len(r.kid) == 0 {
		return errors.New("A256KW recipient requires a non-empty kid")
	}
	if len(r.kek) != 32 {
		return fmt.Errorf("A256KW recipient KEK must be 32 bytes, got %d", len(r.kek))
	}
	return nil
}

func (r *a256kwRecipient) wrap(cek []byte) (*cose.Recipient, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	wrapped, err := aeskw.Wrap(r.kek, cek)
	if err != nil {
		return nil, fmt.Errorf("fee: A256KW wrap: %w", err)
	}
	return &cose.Recipient{
		Headers: cose.Headers{
			Protected: cose.Header{}.
				Set(cose.HeaderLabelAlg, cose.AlgA256KW),
			Unprotected: cose.Header{}.
				Set(cose.HeaderLabelKID, r.kid),
		},
		Ciphertext: wrapped,
	}, nil
}
