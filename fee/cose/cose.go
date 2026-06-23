// Package cose implements just enough of COSE (CBOR Object Signing and
// Encryption, RFC 9052) to encode and decode a COSE_Encrypt structure
// (CBOR tag 96) with a detached payload, and to build the Enc_structure
// that COSE feeds to an AEAD as Additional Authenticated Data (AAD).
//
// It is the low-level envelope layer beneath the FEE (Filecoin Encryption
// Envelope) package: cose carries opaque header parameters and wrapped
// content-encryption keys, but performs no cryptography itself. Key
// agreement, key wrap (e.g. ECDH-ES+A256KW over X25519) and the content
// AEAD (e.g. AES-256-GCM, chunked AES-256-GCM-STREAM) all live in the
// higher-level fee package. cose is deliberately FEE-agnostic: the
// FEE-specific header values (the typ string, the private-use algorithm
// and parameter labels) are supplied by the caller — see [WithExpectedType]
// and the standard COSE label constants below.
//
// # Detached payload
//
// A COSE_Encrypt is written as:
//
//	96([ protected, unprotected, ciphertext, recipients ])
//
// In a detached envelope the ciphertext field is always CBOR null; the real
// ciphertext travels separately, conventionally appended directly after the
// envelope bytes (envelope || ciphertext). [Decode] decodes the single,
// self-delimited envelope item and returns whatever bytes follow it, so a
// caller can recover that trailing ciphertext in one pass.
//
// # Enc_structure (AAD)
//
// The bytes that authenticate the protected header are produced by
// [Encrypt.EncStructure]:
//
//	Enc_structure = [ "Encrypt", protected : bstr, external_aad : bstr ]
//
// The protected element is the exact serialized protected-header byte string
// — the same bytes that appear on the wire — so AAD is stable across an
// encode/decode round trip and any tampering with the protected header is
// detected by the AEAD.
//
// # Scope
//
// This package implements COSE_Encrypt (tag 96, the multi-recipient form,
// which also covers the single-recipient case) with a detached payload. It
// does not implement COSE_Encrypt0 (tag 16), nested recipients, or
// non-detached (inline) ciphertext; encountering a nested recipient or a
// non-null body ciphertext during [Decode] is reported as an error rather
// than silently dropped.
package cose

import (
	"errors"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// TagCOSEEncrypt is the CBOR tag number for a COSE_Encrypt structure
// (RFC 9052 §2).
const TagCOSEEncrypt uint64 = 96

// contextEncrypt is the context string used in the Enc_structure for a
// COSE_Encrypt body (RFC 9052 §5.3).
const contextEncrypt = "Encrypt"

// Standard COSE header parameter labels (RFC 9052 §3.1 and the IANA "COSE
// Header Parameters" registry; HeaderLabelType is from RFC 9596). These are
// generic COSE labels, not FEE-specific ones — a caller is free to use any
// integer or text-string label; these are provided for convenience.
const (
	HeaderLabelAlg          int64 = 1  // algorithm identifier
	HeaderLabelCrit         int64 = 2  // critical headers
	HeaderLabelContentType  int64 = 3  // content type of the plaintext
	HeaderLabelKID          int64 = 4  // key identifier
	HeaderLabelIV           int64 = 5  // initialization vector
	HeaderLabelPartialIV    int64 = 6  // partial IV
	HeaderLabelType         int64 = 16 // "typ" explicit type (RFC 9596)
	HeaderLabelEphemeralKey int64 = -1 // ephemeral key, in a recipient header
)

// Sentinel errors. Every error returned by this package wraps one of these,
// so callers can classify failures with errors.Is.
var (
	// ErrNotEncrypt means the input is not a COSE_Encrypt (tag 96) item.
	ErrNotEncrypt = errors.New("cose: not a COSE_Encrypt (tag 96) structure")
	// ErrMalformed means the CBOR could not be parsed into a well-formed
	// COSE_Encrypt structure. A decode that returns ErrMalformed returns no
	// partial structure.
	ErrMalformed = errors.New("cose: malformed COSE_Encrypt structure")
	// ErrDetachedPayload means the body ciphertext field was not CBOR null;
	// this package only handles detached payloads.
	ErrDetachedPayload = errors.New("cose: body ciphertext must be null (detached payload)")
	// ErrNoRecipients means the recipients array was missing or empty; a
	// COSE_Encrypt must carry at least one recipient.
	ErrNoRecipients = errors.New("cose: COSE_Encrypt requires at least one recipient")
	// ErrUnexpectedType means the protected "typ" header was absent or did
	// not equal the value required via WithExpectedType.
	ErrUnexpectedType = errors.New("cose: unexpected or missing typ header")
	// ErrInvalidLabel means a header map key was neither an integer nor a
	// text string, which COSE does not permit.
	ErrInvalidLabel = errors.New("cose: header label must be an integer or text string")
)

// CBOR major types, read from the first byte of an encoded item. Used to
// validate structure shape before decoding, so malformed input yields a
// precise error instead of a lenient coercion.
const (
	majorByteString byte = 2
	majorArray      byte = 4
	majorMap        byte = 5
)

// encMode encodes in RFC 8949 core deterministic form (definite lengths,
// shortest integers, canonical map-key ordering). Deterministic encoding is
// what keeps the protected header — and therefore the AAD — byte-stable.
//
// decMode rejects duplicate map keys (which COSE forbids) and decodes every
// CBOR integer to a Go int64 (erroring only if one overflows int64) so that
// header labels and values have a single, predictable representation.
var (
	encMode cbor.EncMode
	decMode cbor.DecMode
)

func init() {
	em, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		panic(fmt.Sprintf("cose: building CBOR encode mode: %v", err))
	}
	encMode = em

	dm, err := cbor.DecOptions{
		DupMapKey: cbor.DupMapKeyEnforcedAPF,
		IntDec:    cbor.IntDecConvertSignedOrFail,
	}.DecMode()
	if err != nil {
		panic(fmt.Sprintf("cose: building CBOR decode mode: %v", err))
	}
	decMode = dm
}

// Headers holds a COSE "Headers" pair: an integrity-protected header map and
// an unprotected header map (RFC 9052 §3).
type Headers struct {
	// Protected is the protected header map. Its entries are authenticated
	// by the AEAD via the Enc_structure. May be empty.
	Protected Header
	// Unprotected is the unprotected header map. Its entries are not
	// authenticated. May be empty.
	Unprotected Header
	// RawProtected is the exact serialized content of the protected header
	// byte string as it appeared on the wire. It is set by Decode and is the
	// source of truth for the Enc_structure of a decoded envelope, so AAD
	// verification uses the encoder's original bytes rather than a
	// re-serialization. It is nil for an in-memory envelope that has not been
	// decoded; in that case the protected bytes are derived from Protected.
	//
	// Mutating Protected after Decode does not update RawProtected; rebuild
	// the envelope instead of mutating a decoded one in place.
	RawProtected []byte
}

// Recipient is a COSE_recipient: a Headers pair plus the wrapped
// content-encryption key (RFC 9052 §5.1). Nested recipients are not
// supported.
type Recipient struct {
	// Headers carries the recipient's key-wrap algorithm (protected) and
	// identifying parameters such as the key id and ephemeral key
	// (unprotected). cose treats them as opaque.
	Headers Headers
	// Ciphertext is the wrapped CEK — the output of the recipient's key-wrap
	// algorithm. A nil Ciphertext is encoded as CBOR null (used by direct
	// key-agreement schemes that wrap no key); a non-nil value, including an
	// empty slice, is encoded as a byte string.
	Ciphertext []byte
}

// Encrypt is a COSE_Encrypt structure (RFC 9052 §5.1) with a detached
// payload. The body ciphertext is always null on the wire; the actual
// ciphertext is carried separately.
type Encrypt struct {
	// Headers is the body protected/unprotected header pair.
	Headers Headers
	// Recipients are the per-recipient wrapped-key entries. At least one is
	// required to Encode or to be produced by Decode.
	Recipients []*Recipient
}
