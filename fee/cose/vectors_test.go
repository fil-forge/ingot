package cose

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// --- shared test helpers ---------------------------------------------------

// hexDec decodes a hex string into bytes, failing the test on bad input.
func hexDec(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

// cborBstr returns the canonical CBOR encoding of a byte string (header +
// payload). cborHead emits the 8/16/32-bit length forms, so this handles byte
// strings far larger than the test vectors need. It is an independent
// re-implementation used to cross-check the encoder, so the tests don't
// validate cose against itself.
func cborBstr(b []byte) []byte {
	return append(cborHead(2, uint64(len(b))), b...)
}

// cborText returns the canonical CBOR encoding of a text string.
func cborText(s string) []byte {
	return append(cborHead(3, uint64(len(s))), s...)
}

// cborHead encodes a CBOR type header for the given major type and argument.
func cborHead(major byte, arg uint64) []byte {
	mt := major << 5
	switch {
	case arg < 24:
		return []byte{mt | byte(arg)}
	case arg < 1<<8:
		return []byte{mt | 24, byte(arg)}
	case arg < 1<<16:
		return []byte{mt | 25, byte(arg >> 8), byte(arg)}
	default:
		return []byte{mt | 26, byte(arg >> 24), byte(arg >> 16), byte(arg >> 8), byte(arg)}
	}
}

// expectedEncStructure independently assembles the Enc_structure bytes:
//
//	[ "Encrypt", protected : bstr, external_aad : bstr ]
func expectedEncStructure(protected, externalAAD []byte) []byte {
	out := []byte{0x83} // array(3)
	out = append(out, cborText(contextEncrypt)...)
	out = append(out, cborBstr(protected)...)
	out = append(out, cborBstr(externalAAD)...)
	return out
}

// --- golden vectors --------------------------------------------------------

// The minimal detached COSE_Encrypt: empty body headers, a null payload, and a
// single recipient with empty headers and a one-byte wrapped CEK (0xAA).
//
//	96([ h'', {}, null, [ [ h'', {}, h'AA' ] ] ])
//	d8 60 84 40 a0 f6 81 83 40 a0 41 aa
const minimalEnvelopeHex = "d8608440a0f6818340a041aa"

// A richer hand-encoded vector exercising protected/unprotected parsing:
//
//	protected   = { 1: 3 }                      ; alg = A256GCM
//	unprotected = { 5: h'00..bb' }              ; iv (12 bytes)
//	ciphertext  = null                          ; detached
//	recipients  = [ [ {1:-31}, {4:h'01'}, h'deadbeef' ] ]
const richEnvelopeHex = "d8608443a10103a1054c00112233445566778899aabbf6818344a101381ea104410144deadbeef"

func TestEncodeMinimalVector(t *testing.T) {
	env := &Encrypt{Recipients: []*Recipient{{Ciphertext: []byte{0xAA}}}}
	got, err := env.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	want := hexDec(t, minimalEnvelopeHex)
	if !bytes.Equal(got, want) {
		t.Fatalf("Encode minimal:\n got %x\nwant %x", got, want)
	}
}

func TestDecodeMinimalVector(t *testing.T) {
	env, rest, err := Decode(hexDec(t, minimalEnvelopeHex))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("rest = %x, want empty", rest)
	}
	if len(env.Headers.Protected) != 0 || len(env.Headers.Unprotected) != 0 {
		t.Fatalf("expected empty body headers, got %+v", env.Headers)
	}
	if len(env.Recipients) != 1 {
		t.Fatalf("recipients = %d, want 1", len(env.Recipients))
	}
	if got := env.Recipients[0].Ciphertext; !bytes.Equal(got, []byte{0xAA}) {
		t.Fatalf("recipient ciphertext = %x, want AA", got)
	}
}

func TestDecodeRichVector(t *testing.T) {
	data := hexDec(t, richEnvelopeHex)
	env, rest, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("rest = %x, want empty", rest)
	}

	if alg, ok := env.Headers.Protected.Int(HeaderLabelAlg); !ok || alg != 3 {
		t.Fatalf("protected alg = %d (ok=%v), want 3", alg, ok)
	}
	wantIV := hexDec(t, "00112233445566778899aabb")
	if iv, ok := env.Headers.Unprotected.Bytes(HeaderLabelIV); !ok || !bytes.Equal(iv, wantIV) {
		t.Fatalf("unprotected iv = %x (ok=%v), want %x", iv, ok, wantIV)
	}
	if len(env.Recipients) != 1 {
		t.Fatalf("recipients = %d, want 1", len(env.Recipients))
	}
	r := env.Recipients[0]
	if alg, ok := r.Headers.Protected.Int(HeaderLabelAlg); !ok || alg != -31 {
		t.Fatalf("recipient alg = %d (ok=%v), want -31", alg, ok)
	}
	if kid, ok := r.Headers.Unprotected.Bytes(HeaderLabelKID); !ok || !bytes.Equal(kid, []byte{0x01}) {
		t.Fatalf("recipient kid = %x (ok=%v), want 01", kid, ok)
	}
	if !bytes.Equal(r.Ciphertext, hexDec(t, "deadbeef")) {
		t.Fatalf("recipient ciphertext = %x, want deadbeef", r.Ciphertext)
	}

	// RawProtected must preserve the on-wire protected bytes, and the
	// Enc_structure must be built from them.
	if !bytes.Equal(env.Headers.RawProtected, hexDec(t, "a10103")) {
		t.Fatalf("RawProtected = %x, want a10103", env.Headers.RawProtected)
	}
	aad, err := env.EncStructure(nil)
	if err != nil {
		t.Fatalf("EncStructure: %v", err)
	}
	if want := expectedEncStructure(hexDec(t, "a10103"), nil); !bytes.Equal(aad, want) {
		t.Fatalf("EncStructure:\n got %x\nwant %x", aad, want)
	}
}

func TestEncStructureEmptyProtectedGolden(t *testing.T) {
	env := &Encrypt{Recipients: []*Recipient{{Ciphertext: []byte{0xAA}}}}
	aad, err := env.EncStructure(nil)
	if err != nil {
		t.Fatalf("EncStructure: %v", err)
	}
	// [ "Encrypt", h'', h'' ] = 83 67 "Encrypt" 40 40
	want := hexDec(t, "8367456e63727970744040")
	if !bytes.Equal(aad, want) {
		t.Fatalf("EncStructure(nil):\n got %x\nwant %x", aad, want)
	}
	if want := expectedEncStructure(nil, nil); !bytes.Equal(aad, want) {
		t.Fatalf("EncStructure(nil) vs helper:\n got %x\nwant %x", aad, want)
	}

	withAAD, err := env.EncStructure([]byte{0x01, 0x02})
	if err != nil {
		t.Fatalf("EncStructure(aad): %v", err)
	}
	if want := expectedEncStructure(nil, []byte{0x01, 0x02}); !bytes.Equal(withAAD, want) {
		t.Fatalf("EncStructure(aad):\n got %x\nwant %x", withAAD, want)
	}
}
