package cose

import (
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// Envelope is a COSE_Encrypt (tag 96) or COSE_Encrypt0 (tag 16) structure with a
// detached payload (RFC 9052 §5.1 / §5.2). Which of the two forms an envelope
// takes follows entirely from its recipients: an envelope carrying one or more
// [Recipient] entries is a COSE_Encrypt; a recipient-less envelope is a
// COSE_Encrypt0, whose content-encryption key is established out of band.
//
// COSE never permits a COSE_Encrypt with an empty recipients array (§5.1
// requires at least one), so the two forms partition cleanly on recipient
// presence. The CBOR tag (96 vs 16), the array shape (4 elements vs 3), and the
// Enc_structure context ("Encrypt" vs "Encrypt0") are therefore all *computed*
// from Recipients — never stored, so they cannot drift, and an "Encrypt0 with
// recipients" is unrepresentable.
type Envelope struct {
	// Headers is the body protected/unprotected header pair. Its RawProtected
	// (set by Decode) is the source of truth for the Enc_structure of a decoded
	// envelope, so AAD survives an encode/decode round trip byte-for-byte.
	Headers Headers
	// Recipients are the per-recipient wrapped-key entries. A non-empty slice
	// makes this a COSE_Encrypt (tag 96); an empty (or nil) slice makes it a
	// recipient-less COSE_Encrypt0 (tag 16).
	Recipients []*Recipient
}

// isEncrypt0 reports whether this envelope is the recipient-less COSE_Encrypt0
// (tag 16) form. Recipient presence is the single source of truth for the form,
// so the tag, array shape and Enc_structure context all derive from it.
func (e *Envelope) isEncrypt0() bool { return len(e.Recipients) == 0 }

// Encode serializes the envelope as a detached COSE_Encrypt (tag 96) when it has
// recipients, or a detached COSE_Encrypt0 (tag 16) when it has none:
//
//	96([ protected : bstr, unprotected : map, ciphertext : null, recipients ])
//	16([ protected : bstr, unprotected : map, ciphertext : null ])
//
// The body ciphertext is always null (detached); the caller appends the real
// ciphertext after the returned bytes. Encoding is RFC 8949 core deterministic,
// so the same envelope always produces identical bytes.
func (e *Envelope) Encode() ([]byte, error) {
	prot, err := e.Headers.protectedBytes()
	if err != nil {
		return nil, fmt.Errorf("cose: encoding protected header: %w", err)
	}
	unprot, err := e.Headers.Unprotected.normalized()
	if err != nil {
		return nil, fmt.Errorf("cose: encoding unprotected header: %w", err)
	}

	if e.isEncrypt0() {
		body := []any{bstr(prot), unprot, nil}
		out, err := encMode.Marshal(cbor.Tag{Number: TagCOSEEncrypt0, Content: body})
		if err != nil {
			return nil, fmt.Errorf("cose: encoding COSE_Encrypt0: %w", err)
		}
		return out, nil
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
// envelope — the AAD passed to the body AEAD:
//
//	Enc_structure = [ context, protected : bstr, external_aad : bstr ]
//
// The context is "Encrypt" for a COSE_Encrypt (recipients present) or "Encrypt0"
// for a COSE_Encrypt0 (none), matching what [Envelope.Encode] emits. The
// protected element is the exact serialized protected header (RawProtected for a
// decoded envelope, otherwise the deterministic serialization of
// Headers.Protected). external_aad is the caller's additional data; pass nil for
// none, which encodes as an empty byte string.
func (e *Envelope) EncStructure(externalAAD []byte) ([]byte, error) {
	prot, err := e.Headers.protectedBytes()
	if err != nil {
		return nil, fmt.Errorf("cose: building Enc_structure: %w", err)
	}
	context := contextEncrypt
	if e.isEncrypt0() {
		context = contextEncrypt0
	}
	return encStructureBytes(context, prot, externalAAD)
}

// ProtectedBytes returns the serialized content of the body protected header —
// the byte string that appears on the wire and inside the Enc_structure. It is
// RawProtected for a decoded envelope and the deterministic serialization of
// Headers.Protected otherwise. The result is empty when the protected header is
// empty.
func (e *Envelope) ProtectedBytes() ([]byte, error) {
	return e.Headers.protectedBytes()
}

// encStructureBytes builds the CBOR-encoded COSE Enc_structure (RFC 9052 §5.3)
// from a context string, the serialized protected-header bytes, and external
// AAD:
//
//	Enc_structure = [ context, protected : bstr, external_aad : bstr ]
//
// It backs [Envelope.EncStructure]; the context is "Encrypt" or "Encrypt0". A
// nil externalAAD encodes as an empty byte string.
func encStructureBytes(context string, protected, externalAAD []byte) ([]byte, error) {
	out, err := encMode.Marshal([]any{context, bstr(protected), bstr(externalAAD)})
	if err != nil {
		return nil, fmt.Errorf("cose: encoding Enc_structure: %w", err)
	}
	return out, nil
}

// protectedBytes returns the protected header byte-string content for this
// Headers value: the on-wire RawProtected when present (set by Decode),
// otherwise a fresh deterministic serialization of Protected. An empty protected
// header serializes to an empty (zero-length) byte string, per COSE's
// empty_or_serialized_map rule, rather than to an encoded empty map.
func (h Headers) protectedBytes() ([]byte, error) {
	if h.RawProtected != nil {
		return h.RawProtected, nil
	}
	if len(h.Protected) == 0 {
		return []byte{}, nil
	}
	return encodeMap(h.Protected)
}
