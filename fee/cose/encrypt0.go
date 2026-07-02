package cose

import (
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// Encrypt0 is a COSE_Encrypt0 structure (RFC 9052 §5.2) with a detached
// payload: an authenticated-encryption envelope that carries only a header pair
// and a (detached, hence null) body ciphertext, with no recipients array. The
// content-encryption key is established out of band — a key the parties already
// share, or one delivered by a mechanism outside the envelope.
//
// It is the tag-16 sibling of [Encrypt] (tag 96). Everything else — the
// detached-payload convention, the RFC 8949 core-deterministic encoding, and
// the RawProtected AAD stability across a decode/encode round trip — is
// identical.
type Encrypt0 struct {
	// Headers is the body protected/unprotected header pair.
	Headers Headers
}

// Encode serializes the envelope as a detached COSE_Encrypt0 (CBOR tag 16):
//
//	16([ protected : bstr, unprotected : map, ciphertext : null ])
//
// The body ciphertext is always null (detached); the caller appends the real
// ciphertext after the returned bytes. Encoding is RFC 8949 core deterministic,
// so the same envelope always produces identical bytes.
func (e *Encrypt0) Encode() ([]byte, error) {
	prot, err := e.Headers.protectedBytes()
	if err != nil {
		return nil, fmt.Errorf("cose: encoding protected header: %w", err)
	}
	unprot, err := e.Headers.Unprotected.normalized()
	if err != nil {
		return nil, fmt.Errorf("cose: encoding unprotected header: %w", err)
	}

	body := []any{bstr(prot), unprot, nil}
	out, err := encMode.Marshal(cbor.Tag{Number: TagCOSEEncrypt0, Content: body})
	if err != nil {
		return nil, fmt.Errorf("cose: encoding COSE_Encrypt0: %w", err)
	}
	return out, nil
}

// EncStructure returns the CBOR-encoded Enc_structure (RFC 9052 §5.3) for this
// COSE_Encrypt0 body — the AAD passed to the body AEAD:
//
//	Enc_structure = [ "Encrypt0", protected : bstr, external_aad : bstr ]
//
// The protected element is the exact serialized protected header (RawProtected
// for a decoded envelope, otherwise the deterministic serialization of
// Headers.Protected). See [Encrypt.EncStructure] for the tag-96 counterpart and
// [EncStructureBytes] for the shared builder.
func (e *Encrypt0) EncStructure(externalAAD []byte) ([]byte, error) {
	prot, err := e.Headers.protectedBytes()
	if err != nil {
		return nil, fmt.Errorf("cose: building Enc_structure: %w", err)
	}
	return EncStructureBytes(contextEncrypt0, prot, externalAAD)
}

// ProtectedBytes returns the serialized content of the body protected header —
// the byte string that appears on the wire and inside the Enc_structure. See
// [Encrypt.ProtectedBytes].
func (e *Encrypt0) ProtectedBytes() ([]byte, error) {
	return e.Headers.protectedBytes()
}
