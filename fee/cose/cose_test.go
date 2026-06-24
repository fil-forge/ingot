package cose

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// feeTypeExample is a stand-in for the FEE envelope type. cose itself is
// FEE-agnostic; the value is supplied by the caller via WithExpectedType.
const feeTypeExample = "application/vnd.filecoin-encryption+cose"

// FEE private-use labels live in the higher-level fee package; the integers
// here are used only to prove that arbitrary integer labels round-trip.
const (
	labelChunkSize   int64 = -1
	labelAppMetadata int64 = -65792
)

// sampleEnvelope builds a fully-populated multi-recipient envelope that
// exercises every header value kind: positive and negative integer labels, a
// text-string label, byte strings, a nested map, and a custom string label.
func sampleEnvelope() *Encrypt {
	protected := Header{}.
		Set(HeaderLabelAlg, -65793).
		Set(HeaderLabelType, feeTypeExample).
		Set(HeaderLabelContentType, "text/plain").
		Set(labelChunkSize, 262144).
		Set(labelAppMetadata, map[any]any{"owner": "tenant-a", "parts": int64(3)}).
		Set("x-custom", []byte{0x01, 0x02, 0x03})

	unprotected := Header{}.
		Set(HeaderLabelIV, bytes.Repeat([]byte{0xAB}, 7)).
		Set(int64(100), "unauthenticated")

	mkRecipient := func(kid string, cek []byte) *Recipient {
		return &Recipient{
			Headers: Headers{
				Protected: Header{}.Set(HeaderLabelAlg, -31), // ECDH-ES+A256KW
				Unprotected: Header{}.
					Set(HeaderLabelKID, []byte(kid)).
					Set(HeaderLabelEphemeralKey, map[any]any{
						int64(1):  int64(1),                       // kty: OKP
						int64(-1): int64(4),                       // crv: X25519
						int64(-2): bytes.Repeat([]byte{0x09}, 32), // x
					}),
			},
			Ciphertext: cek,
		}
	}

	return &Encrypt{
		Headers: Headers{Protected: protected, Unprotected: unprotected},
		Recipients: []*Recipient{
			mkRecipient("did:web:hilt.example:wrap:tenant-a#wrap-1", bytes.Repeat([]byte{0x11}, 40)),
			mkRecipient("did:web:hilt.example:wrap:tenant-a#wrap-2", bytes.Repeat([]byte{0x22}, 40)),
			mkRecipient("did:web:hilt.example:region#region-1", bytes.Repeat([]byte{0x33}, 40)),
		},
	}
}

func TestRoundTripPreservesAllFields(t *testing.T) {
	orig := sampleEnvelope()

	encoded, err := orig.Encode()
	require.NoError(t, err)

	env, rest, err := Decode(encoded, WithExpectedType(feeTypeExample))
	require.NoError(t, err)
	require.Len(t, rest, 0)

	// Whole-map equality is the strongest "intact" check: every protected and
	// unprotected entry, including the nested map and custom label, must
	// survive. Expected values use the normalized (int64) integer form.
	wantProtected := Header{
		HeaderLabelAlg:         int64(-65793),
		HeaderLabelType:        feeTypeExample,
		HeaderLabelContentType: "text/plain",
		labelChunkSize:         int64(262144),
		labelAppMetadata:       map[any]any{"owner": "tenant-a", "parts": int64(3)},
		"x-custom":             []byte{0x01, 0x02, 0x03},
	}
	require.Equal(t, wantProtected, env.Headers.Protected)

	wantUnprotected := Header{
		HeaderLabelIV: bytes.Repeat([]byte{0xAB}, 7),
		int64(100):    "unauthenticated",
	}
	require.Equal(t, wantUnprotected, env.Headers.Unprotected)

	// Re-encoding a decoded envelope reproduces the original bytes exactly
	// (deterministic, lossless).
	reencoded, err := env.Encode()
	require.NoError(t, err)
	require.Equal(t, encoded, reencoded)
}

func TestRoundTripMultiRecipient(t *testing.T) {
	orig := sampleEnvelope()
	encoded, err := orig.Encode()
	require.NoError(t, err)
	env, _, err := Decode(encoded)
	require.NoError(t, err)

	require.Len(t, env.Recipients, len(orig.Recipients))
	// Order and per-recipient fields must be preserved.
	for i, want := range orig.Recipients {
		got := env.Recipients[i]
		wantKID, _ := want.Headers.Unprotected.Bytes(HeaderLabelKID)
		gotKID, ok := got.Headers.Unprotected.Bytes(HeaderLabelKID)
		require.True(t, ok)
		require.Equal(t, wantKID, gotKID)
		alg, ok := got.Headers.Protected.Int(HeaderLabelAlg)
		require.True(t, ok)
		require.Equal(t, int64(-31), alg)
		require.Equal(t, want.Ciphertext, got.Ciphertext)
		require.Equal(t, want.Headers.Unprotected, got.Headers.Unprotected)
	}
}

func TestEncodeIsDeterministic(t *testing.T) {
	env := sampleEnvelope()
	a, err := env.Encode()
	require.NoError(t, err)
	b, err := env.Encode()
	require.NoError(t, err)
	require.Equal(t, a, b)

	// Header map-key insertion order must not affect the bytes: a protected
	// header built in a different order encodes identically (canonical sort).
	reordered := &Encrypt{
		Headers: Headers{Protected: Header{}.
			Set(labelAppMetadata, map[any]any{"parts": int64(3), "owner": "tenant-a"}).
			Set("x-custom", []byte{0x01, 0x02, 0x03}).
			Set(labelChunkSize, 262144).
			Set(HeaderLabelContentType, "text/plain").
			Set(HeaderLabelType, feeTypeExample).
			Set(HeaderLabelAlg, -65793),
			Unprotected: Header{}.Set(int64(100), "unauthenticated").Set(HeaderLabelIV, bytes.Repeat([]byte{0xAB}, 7)),
		},
		Recipients: env.Recipients,
	}
	c, err := reordered.Encode()
	require.NoError(t, err)
	require.Equal(t, a, c)
}

func TestEncodeRequiresRecipient(t *testing.T) {
	env := &Encrypt{Headers: Headers{Protected: Header{}.Set(HeaderLabelAlg, 3)}}
	_, err := env.Encode()
	require.Error(t, err)
}

func TestDetachedPayloadRoundTrip(t *testing.T) {
	env := sampleEnvelope()
	envelope, err := env.Encode()
	require.NoError(t, err)

	ciphertext := []byte("the detached ciphertext bytes, opaque to cose")
	blob := append(append([]byte{}, envelope...), ciphertext...)

	decoded, rest, err := Decode(blob)
	require.NoError(t, err)
	require.Equal(t, ciphertext, rest)
	require.Len(t, decoded.Recipients, len(env.Recipients))
}

func TestEmptyProtectedHeader(t *testing.T) {
	env := &Encrypt{
		Headers:    Headers{Unprotected: Header{}.Set(HeaderLabelIV, []byte{0x01})},
		Recipients: []*Recipient{{Ciphertext: []byte{0xAA}}},
	}
	encoded, err := env.Encode()
	require.NoError(t, err)
	// An empty protected header must serialize as an empty byte string (0x40),
	// not as an encoded empty map. The protected element follows the tag (d860)
	// and array head (84).
	require.Equal(t, byte(0x84), encoded[2], "empty protected not encoded as h'': bytes = %x", encoded[:6])
	require.Equal(t, byte(0x40), encoded[3], "empty protected not encoded as h'': bytes = %x", encoded[:6])

	decoded, _, err := Decode(encoded)
	require.NoError(t, err)
	require.Len(t, decoded.Headers.Protected, 0)
	pb, err := decoded.ProtectedBytes()
	require.NoError(t, err)
	require.Len(t, pb, 0)
}

func TestEncStructureBindsProtectedNotUnprotected(t *testing.T) {
	base := func() *Encrypt {
		return &Encrypt{
			Headers: Headers{
				Protected:   Header{}.Set(HeaderLabelAlg, 3),
				Unprotected: Header{}.Set(HeaderLabelIV, []byte{0x01}),
			},
			Recipients: []*Recipient{{Ciphertext: []byte{0xAA}}},
		}
	}

	aad := func(e *Encrypt) []byte {
		t.Helper()
		b, err := e.EncStructure(nil)
		require.NoError(t, err)
		return b
	}

	ref := aad(base())

	// Changing the unprotected header must NOT change the AAD.
	sameAAD := base()
	sameAAD.Headers.Unprotected.Set(HeaderLabelIV, []byte{0x02})
	require.Equal(t, ref, aad(sameAAD), "AAD changed when only the unprotected header changed")

	// Changing the protected header MUST change the AAD.
	diffAAD := base()
	diffAAD.Headers.Protected.Set(HeaderLabelAlg, -65793)
	require.NotEqual(t, ref, aad(diffAAD), "AAD unchanged when the protected header changed")

	// external_aad must be incorporated.
	withExt := aad(base())
	withExtBytes, err := base().EncStructure([]byte("aad"))
	require.NoError(t, err)
	require.NotEqual(t, withExt, withExtBytes, "external_aad not incorporated into Enc_structure")
}

func TestEncStructureStableAcrossRoundTrip(t *testing.T) {
	orig := sampleEnvelope()
	want, err := orig.EncStructure([]byte("ext"))
	require.NoError(t, err)

	encoded, err := orig.Encode()
	require.NoError(t, err)
	decoded, _, err := Decode(encoded)
	require.NoError(t, err)
	got, err := decoded.EncStructure([]byte("ext"))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestRoundTripLargeUnsignedValue(t *testing.T) {
	// A header value that exceeds MaxInt64 is preserved as uint64 and must
	// survive a round trip: a value this package can Encode it must also be
	// able to Decode. Regression for the previously strict decode mode, which
	// rejected any unsigned integer above MaxInt64.
	const big = uint64(math.MaxUint64)
	env := &Encrypt{
		Headers: Headers{Protected: Header{}.
			Set("big", big).
			Set("nested", map[any]any{"n": big, "small": int64(7)})},
		Recipients: []*Recipient{{
			Headers:    Headers{Protected: Header{}.Set(HeaderLabelAlg, -31)},
			Ciphertext: []byte{0x01},
		}},
	}

	encoded, err := env.Encode()
	require.NoError(t, err)
	decoded, _, err := Decode(encoded)
	require.NoError(t, err)

	// The top-level value comes back as the same uint64.
	got, ok := decoded.Headers.Protected.Uint("big")
	require.True(t, ok)
	require.Equal(t, big, got)
	// Recursion: a large unsigned stays uint64 inside a nested map, while a
	// value that fits is normalized back to int64.
	nested, ok := decoded.Headers.Protected.Get("nested")
	require.True(t, ok)
	want := map[any]any{"n": big, "small": int64(7)}
	require.Equal(t, want, nested)
}
