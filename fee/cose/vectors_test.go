package cose

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- shared test helpers ---------------------------------------------------

// hexDec decodes a hex string into bytes, failing the test on bad input.
func hexDec(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
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
// single recipient with empty headers and a one-byte wrapped key (0xAA).
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

// TestVectors checks the encoder and decoder against hand-encoded golden byte
// sequences, cross-checked by the independent CBOR re-implementation above so
// the tests don't validate cose against itself.
func TestVectors(t *testing.T) {
	t.Run("encode minimal", func(t *testing.T) {
		env := &Encrypt{Recipients: []*Recipient{{Ciphertext: []byte{0xAA}}}}
		got, err := env.Encode()
		require.NoError(t, err)
		want := hexDec(t, minimalEnvelopeHex)
		require.Equal(t, want, got)
	})

	t.Run("decode minimal", func(t *testing.T) {
		env, rest, err := Decode(hexDec(t, minimalEnvelopeHex))
		require.NoError(t, err)
		require.Len(t, rest, 0)
		require.Len(t, env.Headers.Protected, 0)
		require.Len(t, env.Headers.Unprotected, 0)
		require.Len(t, env.Recipients, 1)
		require.Equal(t, []byte{0xAA}, env.Recipients[0].Ciphertext)
	})

	t.Run("decode rich", func(t *testing.T) {
		data := hexDec(t, richEnvelopeHex)
		env, rest, err := Decode(data)
		require.NoError(t, err)
		require.Len(t, rest, 0)

		alg, ok := env.Headers.Protected.Int(HeaderLabelAlg)
		require.True(t, ok)
		require.Equal(t, AlgA256GCM, alg)
		wantIV := hexDec(t, "00112233445566778899aabb")
		iv, ok := env.Headers.Unprotected.Bytes(HeaderLabelIV)
		require.True(t, ok)
		require.Equal(t, wantIV, iv)
		require.Len(t, env.Recipients, 1)
		r := env.Recipients[0]
		ralg, ok := r.Headers.Protected.Int(HeaderLabelAlg)
		require.True(t, ok)
		require.Equal(t, AlgECDHESA256KW, ralg)
		kid, ok := r.Headers.Unprotected.Bytes(HeaderLabelKID)
		require.True(t, ok)
		require.Equal(t, []byte{0x01}, kid)
		require.Equal(t, hexDec(t, "deadbeef"), r.Ciphertext)

		// RawProtected must preserve the on-wire protected bytes, and the
		// Enc_structure must be built from them.
		require.Equal(t, hexDec(t, "a10103"), env.Headers.RawProtected)
		aad, err := env.EncStructure(nil)
		require.NoError(t, err)
		require.Equal(t, expectedEncStructure(hexDec(t, "a10103"), nil), aad)
	})

	t.Run("enc_structure empty protected", func(t *testing.T) {
		env := &Encrypt{Recipients: []*Recipient{{Ciphertext: []byte{0xAA}}}}
		aad, err := env.EncStructure(nil)
		require.NoError(t, err)
		// [ "Encrypt", h'', h'' ] = 83 67 "Encrypt" 40 40
		want := hexDec(t, "8367456e63727970744040")
		require.Equal(t, want, aad)
		require.Equal(t, expectedEncStructure(nil, nil), aad)

		withAAD, err := env.EncStructure([]byte{0x01, 0x02})
		require.NoError(t, err)
		require.Equal(t, expectedEncStructure(nil, []byte{0x01, 0x02}), withAAD)
	})
}
