package cose

import (
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// Encode serializes the envelope as a detached COSE_Encrypt (CBOR tag 96):
//
//	96([ protected : bstr, unprotected : map, ciphertext : null, recipients ])
//
// The body ciphertext is always null (detached); the caller appends the real
// ciphertext after the returned bytes. Encoding is RFC 8949 core
// deterministic, so the same envelope always produces identical bytes.
//
// Encode requires at least one recipient and returns ErrNoRecipients
// otherwise.
func (e *Encrypt) Encode() ([]byte, error) {
	if len(e.Recipients) == 0 {
		return nil, ErrNoRecipients
	}

	prot, err := e.Headers.protectedBytes()
	if err != nil {
		return nil, fmt.Errorf("cose: encoding protected header: %w", err)
	}
	unprot, err := e.Headers.Unprotected.normalized()
	if err != nil {
		return nil, fmt.Errorf("cose: encoding unprotected header: %w", err)
	}

	recipients := make([]any, len(e.Recipients))
	for i, r := range e.Recipients {
		if r == nil {
			return nil, fmt.Errorf("cose: recipient %d is nil", i)
		}
		rprot, err := r.Headers.protectedBytes()
		if err != nil {
			return nil, fmt.Errorf("cose: encoding recipient %d protected header: %w", i, err)
		}
		runprot, err := r.Headers.Unprotected.normalized()
		if err != nil {
			return nil, fmt.Errorf("cose: encoding recipient %d unprotected header: %w", i, err)
		}
		// r.Ciphertext is placed directly: a nil slice encodes as CBOR null,
		// a non-nil slice (including empty) as a byte string.
		recipients[i] = []any{bstr(rprot), runprot, r.Ciphertext}
	}

	body := []any{bstr(prot), unprot, nil, recipients}
	out, err := encMode.Marshal(cbor.Tag{Number: TagCOSEEncrypt, Content: body})
	if err != nil {
		return nil, fmt.Errorf("cose: encoding COSE_Encrypt: %w", err)
	}
	return out, nil
}

// EncStructure returns the CBOR-encoded Enc_structure (RFC 9052 §5.3) for this
// envelope, which is the AAD passed to the body AEAD:
//
//	Enc_structure = [ "Encrypt", protected : bstr, external_aad : bstr ]
//
// The protected element is the exact serialized protected header (RawProtected
// for a decoded envelope, otherwise the deterministic serialization of
// Headers.Protected). external_aad is the caller's additional data; pass nil
// for none, which encodes as an empty byte string.
func (e *Encrypt) EncStructure(externalAAD []byte) ([]byte, error) {
	prot, err := e.Headers.protectedBytes()
	if err != nil {
		return nil, fmt.Errorf("cose: building Enc_structure: %w", err)
	}
	return encStructureBytes(contextEncrypt, prot, externalAAD)
}

// encStructureBytes builds the CBOR-encoded COSE Enc_structure (RFC 9052 §5.3)
// from a context string, the serialized protected-header bytes, and external
// AAD:
//
//	Enc_structure = [ context, protected : bstr, external_aad : bstr ]
//
// encStructureBytes is the builder shared by [Encrypt.EncStructure] (context
// "Encrypt") and [Encrypt0.EncStructure] (context "Encrypt0"). A nil externalAAD
// encodes as an empty byte string.
func encStructureBytes(context string, protected, externalAAD []byte) ([]byte, error) {
	out, err := encMode.Marshal([]any{context, bstr(protected), bstr(externalAAD)})
	if err != nil {
		return nil, fmt.Errorf("cose: encoding Enc_structure: %w", err)
	}
	return out, nil
}

// ProtectedBytes returns the serialized content of the body protected header —
// the byte string that appears on the wire and inside the Enc_structure. It is
// RawProtected for a decoded envelope and the deterministic serialization of
// Headers.Protected otherwise. The result is empty when the protected header
// is empty.
func (e *Encrypt) ProtectedBytes() ([]byte, error) {
	return e.Headers.protectedBytes()
}

// protectedBytes returns the protected header byte-string content for this
// Headers value: the on-wire RawProtected when present (set by Decode),
// otherwise a fresh deterministic serialization of Protected. An empty
// protected header serializes to an empty (zero-length) byte string, per
// COSE's empty_or_serialized_map rule, rather than to an encoded empty map.
func (h Headers) protectedBytes() ([]byte, error) {
	if h.RawProtected != nil {
		return h.RawProtected, nil
	}
	if len(h.Protected) == 0 {
		return []byte{}, nil
	}
	return encodeMap(h.Protected)
}
