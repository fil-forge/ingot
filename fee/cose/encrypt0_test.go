package cose

import (
	"bytes"
	"errors"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

// encrypt0TestType is a stand-in FEE-style typ value; cose is type-agnostic, so
// any text string works here.
const encrypt0TestType = "application/test+cose"

// sampleEncrypt0 builds a populated recipient-less envelope — a COSE_Encrypt0
// (tag 16) — with a protected {alg, typ} header and an unprotected {iv, custom}
// header. Recipient absence is what makes it the tag-16 form.
func sampleEncrypt0() *Envelope {
	return &Envelope{
		Headers: Headers{
			Protected: Header{}.
				Set(HeaderLabelAlg, int64(-65793)). // FEE chunked-STREAM private-use alg
				Set(HeaderLabelType, encrypt0TestType),
			Unprotected: Header{}.
				Set(HeaderLabelIV, bytes.Repeat([]byte{0xAB}, 7)).
				Set(int64(-65790), 262144), // FEE chunk-size private-use label
		},
	}
}

// expectedEncStructure0 independently assembles the Enc_structure bytes for a
// COSE_Encrypt0 body:
//
//	[ "Encrypt0", protected : bstr, external_aad : bstr ]
func expectedEncStructure0(protected, externalAAD []byte) []byte {
	out := []byte{0x83} // array(3)
	out = append(out, cborText(contextEncrypt0)...)
	out = append(out, cborBstr(protected)...)
	out = append(out, cborBstr(externalAAD)...)
	return out
}

func TestEncrypt0RoundTrip(t *testing.T) {
	orig := sampleEncrypt0()

	encoded, err := orig.Encode()
	require.NoError(t, err)

	// A detached ciphertext conventionally follows the envelope; Decode must
	// return it untouched as rest.
	ciphertext := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	blob := append(append([]byte(nil), encoded...), ciphertext...)

	env, rest, err := Decode(blob)
	require.NoError(t, err)
	require.Equal(t, ciphertext, rest)
	require.Len(t, env.Recipients, 0, "recipient-less envelope decodes with no recipients")

	alg, ok := env.Headers.Protected.Int(HeaderLabelAlg)
	require.True(t, ok)
	require.Equal(t, int64(-65793), alg)

	typ, ok := env.Headers.Protected.Text(HeaderLabelType)
	require.True(t, ok)
	require.Equal(t, encrypt0TestType, typ)

	iv, ok := env.Headers.Unprotected.Bytes(HeaderLabelIV)
	require.True(t, ok)
	require.Equal(t, bytes.Repeat([]byte{0xAB}, 7), iv)

	cs, ok := env.Headers.Unprotected.Int(int64(-65790))
	require.True(t, ok)
	require.Equal(t, int64(262144), cs)

	// The decoded protected bytes are preserved verbatim for AAD stability.
	wantProt, err := orig.ProtectedBytes()
	require.NoError(t, err)
	require.Equal(t, wantProt, env.Headers.RawProtected)
}

func TestEncrypt0EncodeShape(t *testing.T) {
	encoded, err := sampleEncrypt0().Encode()
	require.NoError(t, err)

	// Outer item is CBOR tag 16 (0xd0).
	require.Equal(t, byte(0xd0), encoded[0], "recipient-less envelope must encode as tag 16 (COSE_Encrypt0)")

	var tag cbor.RawTag
	require.NoError(t, decMode.Unmarshal(encoded, &tag))
	require.Equal(t, TagCOSEEncrypt0, tag.Number)

	var arr []cbor.RawMessage
	require.NoError(t, decMode.Unmarshal(tag.Content, &arr))
	require.Len(t, arr, 3, "COSE_Encrypt0 is a 3-element array (no recipients)")

	// arr[0] is a byte string (protected), arr[1] a map (unprotected), arr[2]
	// null (detached payload).
	require.Equal(t, majorByteString, cborMajor(arr[0]))
	require.Equal(t, majorMap, cborMajor(arr[1]))
	require.True(t, isNull(arr[2]), "body ciphertext must be null (detached)")
}

func TestEncrypt0EncStructure(t *testing.T) {
	env := sampleEncrypt0()
	prot, err := env.ProtectedBytes()
	require.NoError(t, err)

	aad, err := env.EncStructure(nil)
	require.NoError(t, err)
	require.Equal(t, expectedEncStructure0(prot, nil), aad)

	// The context string is "Encrypt0", distinguishing it from the tag-96
	// "Encrypt" context even for an identical protected header. The only
	// difference between the two envelopes is recipient presence.
	enc := &Envelope{Headers: env.Headers, Recipients: []*Recipient{{
		Headers:    Headers{Protected: Header{}.Set(HeaderLabelAlg, AlgA256KW)},
		Ciphertext: bytes.Repeat([]byte{0x01}, 40),
	}}}
	aad96, err := enc.EncStructure(nil)
	require.NoError(t, err)
	require.NotEqual(t, aad, aad96, "Encrypt0 and Encrypt AAD must differ by context")

	withExt, err := env.EncStructure([]byte{0x09, 0x08})
	require.NoError(t, err)
	require.Equal(t, expectedEncStructure0(prot, []byte{0x09, 0x08}), withExt)
}

func TestEncrypt0EncStructureUsesRawProtected(t *testing.T) {
	// A decoded envelope must build its AAD from the on-wire protected bytes,
	// so the AAD survives a decode/encode round trip byte-for-byte.
	orig := sampleEncrypt0()
	encoded, err := orig.Encode()
	require.NoError(t, err)
	want, err := orig.EncStructure([]byte("ext"))
	require.NoError(t, err)

	decoded, _, err := Decode(encoded)
	require.NoError(t, err)
	got, err := decoded.EncStructure([]byte("ext"))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestDecodeEncrypt0Shape covers decoding the recipient-less (tag 16) form:
// malformed CBOR, a non-null (inline) body, the wrong element count, and the typ
// check. (Unified Decode also accepts tag 96, so there is no "rejects tag 96"
// case — a tag-96 envelope decodes as an Envelope carrying recipients.)
func TestDecodeEncrypt0Shape(t *testing.T) {
	t.Run("rejects malformed CBOR", func(t *testing.T) {
		_, _, err := Decode([]byte{0xff, 0xff, 0xff})
		require.ErrorIs(t, err, ErrMalformed)
	})

	t.Run("rejects non-null body ciphertext", func(t *testing.T) {
		// 16([ h'', {}, h'01' ]) — inline (non-detached) body.
		body := []any{[]byte{}, Header{}, []byte{0x01}}
		bad, err := encMode.Marshal(cbor.Tag{Number: TagCOSEEncrypt0, Content: body})
		require.NoError(t, err)
		_, _, err = Decode(bad)
		require.ErrorIs(t, err, ErrDetachedPayload)
	})

	t.Run("rejects wrong element count", func(t *testing.T) {
		// A 4-element array under tag 16 violates the COSE_Encrypt0 shape.
		body := []any{[]byte{}, Header{}, nil, []any{}}
		bad, err := encMode.Marshal(cbor.Tag{Number: TagCOSEEncrypt0, Content: body})
		require.NoError(t, err)
		_, _, err = Decode(bad)
		require.ErrorIs(t, err, ErrMalformed)
	})

	t.Run("enforces expected typ", func(t *testing.T) {
		enc, err := sampleEncrypt0().Encode()
		require.NoError(t, err)

		_, _, err = Decode(enc, WithExpectedType(encrypt0TestType))
		require.NoError(t, err)

		_, _, err = Decode(enc, WithExpectedType("application/other"))
		require.ErrorIs(t, err, ErrUnexpectedType)
	})
}

func TestPeekTag(t *testing.T) {
	enc0, err := sampleEncrypt0().Encode()
	require.NoError(t, err)
	tag, err := PeekTag(enc0)
	require.NoError(t, err)
	require.Equal(t, TagCOSEEncrypt0, tag)

	enc96, err := sampleEnvelope().Encode()
	require.NoError(t, err)
	tag, err = PeekTag(enc96)
	require.NoError(t, err)
	require.Equal(t, TagCOSEEncrypt, tag)

	// Same sentinel as Decode for the same bytes: a non-tag item is not a COSE
	// encrypt structure at all.
	_, err = PeekTag([]byte{0x01, 0x02}) // a bare integer, not a tag
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrNotEncrypt))
}
