package fee

import (
	"crypto/ecdh"
	"fmt"

	"github.com/fil-forge/ingot/fee/aeskw"
	"github.com/fil-forge/ingot/fee/cose"
	"github.com/fil-forge/ingot/fee/ecdhkw"
)

// RecipientUnwrapper recovers a content-encryption key (CEK) from the in-envelope
// recipient that holds it. [Decrypt] uses keyID to find the matching recipient
// entry, then calls unwrap on it. (When the CEK is supplied out of band rather
// than carried in the envelope, use [DecryptWithCEK] and no unwrapper.)
//
// RecipientUnwrapper is a sealed interface: the only implementations are the ones
// constructed by [NewECDHESUnwrapper] and [NewA256KWUnwrapper], mirroring the
// wrap side.
type RecipientUnwrapper interface {
	// keyID returns the kid this unwrapper recovers CEKs for. Decrypt matches
	// it, by exact bytes, against each recipient entry's kid.
	keyID() []byte
	// unwrap recovers the CEK from the recipient entry whose kid matched. It
	// returns an error if the entry's key-wrap algorithm is not the one this
	// unwrapper handles, or if the wrapped CEK cannot be recovered.
	unwrap(r *cose.Recipient) ([]byte, error)
}

// ecdhESUnwrapper recovers a CEK wrapped with ECDH-ES+A256KW using an X25519
// private key.
type ecdhESUnwrapper struct {
	kid  []byte
	priv *ecdh.PrivateKey
}

// NewECDHESUnwrapper returns a [RecipientUnwrapper] that recovers a CEK wrapped
// to the X25519 public key matching priv. kid is the recipient key id to match —
// the same value passed to [NewECDHESRecipient]. priv must be an X25519 key.
func NewECDHESUnwrapper(kid []byte, priv *ecdh.PrivateKey) RecipientUnwrapper {
	return &ecdhESUnwrapper{kid: kid, priv: priv}
}

func (u *ecdhESUnwrapper) keyID() []byte { return u.kid }

func (u *ecdhESUnwrapper) unwrap(r *cose.Recipient) ([]byte, error) {
	alg, ok := r.Headers.Protected.Int(cose.HeaderLabelAlg)
	if !ok {
		return nil, fmt.Errorf("%w: recipient algorithm header missing or not an integer", ErrUnsupportedRecipientAlg)
	}
	if alg != cose.AlgECDHESA256KW {
		return nil, fmt.Errorf("%w: recipient algorithm %d is not ECDH-ES+A256KW", ErrUnsupportedRecipientAlg, alg)
	}
	ephPub, err := ephemeralX25519(r.Headers.Unprotected)
	if err != nil {
		return nil, err
	}
	cek, err := ecdhkw.Unwrap(u.priv, &ecdhkw.Wrapped{
		EphemeralPublicKey: ephPub,
		WrappedCEK:         r.Ciphertext,
	})
	if err != nil {
		return nil, fmt.Errorf("fee: ECDH-ES+A256KW unwrap: %w", err)
	}
	return cek, nil
}

// ephemeralX25519 extracts the sender's ephemeral X25519 public key from a
// recipient's ephemeral-key header. The ephemeral key is a self-describing
// COSE_Key map (see the coseKey* constants in recipient.go): its kty must be OKP
// and its crv X25519, and the public key travels in the x parameter (label -2).
// A missing header, a non-COSE_Key value, a mismatched key type or curve, or an
// invalid point is reported as [ErrMalformedEnvelope].
func ephemeralX25519(unprotected cose.Header) (*ecdh.PublicKey, error) {
	raw, ok := unprotected.Get(cose.HeaderLabelEphemeralKey)
	if !ok {
		return nil, fmt.Errorf("%w: ECDH-ES recipient missing ephemeral key", ErrMalformedEnvelope)
	}
	key, ok := raw.(map[any]any)
	if !ok {
		return nil, fmt.Errorf("%w: ECDH-ES ephemeral key is not a COSE_Key map", ErrMalformedEnvelope)
	}
	if kty, ok := key[coseKeyLabelKty].(int64); !ok || kty != coseKeyTypeOKP {
		return nil, fmt.Errorf("%w: ECDH-ES ephemeral key type is not OKP", ErrMalformedEnvelope)
	}
	if crv, ok := key[coseKeyLabelCrv].(int64); !ok || crv != coseKeyCurveX25519 {
		return nil, fmt.Errorf("%w: ECDH-ES ephemeral key curve is not X25519", ErrMalformedEnvelope)
	}
	x, ok := key[coseKeyLabelXCoord].([]byte)
	if !ok {
		return nil, fmt.Errorf("%w: ECDH-ES ephemeral key missing x coordinate", ErrMalformedEnvelope)
	}
	pub, err := ecdh.X25519().NewPublicKey(x)
	if err != nil {
		return nil, fmt.Errorf("%w: ECDH-ES ephemeral key: %v", ErrMalformedEnvelope, err)
	}
	return pub, nil
}

// a256kwUnwrapper recovers a CEK wrapped with A256KW under a symmetric KEK.
type a256kwUnwrapper struct {
	kid []byte
	kek []byte
}

// NewA256KWUnwrapper returns a [RecipientUnwrapper] that recovers a CEK wrapped
// under kek with A256KW. kid is the recipient key id to match — the same value
// passed to [NewA256KWRecipient]. kek must be the 32-byte KEK the CEK was wrapped
// under; a different KEK fails the unwrap with [aeskw.ErrIntegrity].
func NewA256KWUnwrapper(kid, kek []byte) RecipientUnwrapper {
	return &a256kwUnwrapper{kid: kid, kek: kek}
}

func (u *a256kwUnwrapper) keyID() []byte { return u.kid }

func (u *a256kwUnwrapper) unwrap(r *cose.Recipient) ([]byte, error) {
	alg, ok := r.Headers.Protected.Int(cose.HeaderLabelAlg)
	if !ok {
		return nil, fmt.Errorf("%w: recipient algorithm header missing or not an integer", ErrUnsupportedRecipientAlg)
	}
	if alg != cose.AlgA256KW {
		return nil, fmt.Errorf("%w: recipient algorithm %d is not A256KW", ErrUnsupportedRecipientAlg, alg)
	}
	cek, err := aeskw.Unwrap(u.kek, r.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("fee: A256KW unwrap: %w", err)
	}
	return cek, nil
}
