package cose

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// exampleType is an arbitrary explicit type (the COSE "typ" header, RFC 9596).
// cose ascribes no meaning to it; the value is supplied by the caller and only
// checked when WithExpectedType is passed to Decode.
const exampleType = "application/example"

// Two arbitrary private-use integer labels — one small-negative, one
// large-negative — used only to show that integer labels round-trip regardless
// of sign or magnitude. cose ascribes no meaning to them.
const (
	labelNegSmall int64 = -1
	labelNegLarge int64 = -70000
)

// sampleEnvelope builds a fully-populated multi-recipient envelope that
// exercises every header value kind: positive and negative integer labels, a
// text-string label, byte strings, a nested map, and a custom string label.
// The specific labels and values are arbitrary — cose is header-agnostic.
func sampleEnvelope() *Envelope {
	protected := Header{}.
		Set(HeaderLabelAlg, AlgA256GCM).
		Set(HeaderLabelType, exampleType).
		Set(HeaderLabelContentType, "text/plain").
		Set(labelNegSmall, 262144).
		Set(labelNegLarge, map[any]any{"name": "example", "count": int64(3)}).
		Set("x-custom", []byte{0x01, 0x02, 0x03})

	unprotected := Header{}.
		Set(HeaderLabelIV, bytes.Repeat([]byte{0xAB}, 7)).
		Set(int64(100), "unauthenticated")

	mkRecipient := func(kid string, wrappedKey []byte) *Recipient {
		return &Recipient{
			Headers: Headers{
				Protected: Header{}.Set(HeaderLabelAlg, AlgECDHESA256KW),
				Unprotected: Header{}.
					Set(HeaderLabelKID, []byte(kid)).
					Set(HeaderLabelEphemeralKey, map[any]any{
						int64(1):  int64(1),                       // kty: OKP
						int64(-1): int64(4),                       // crv: X25519
						int64(-2): bytes.Repeat([]byte{0x09}, 32), // x
					}),
			},
			Ciphertext: wrappedKey,
		}
	}

	return &Envelope{
		Headers: Headers{Protected: protected, Unprotected: unprotected},
		Recipients: []*Recipient{
			mkRecipient("key-1", bytes.Repeat([]byte{0x11}, 40)),
			mkRecipient("key-2", bytes.Repeat([]byte{0x22}, 40)),
			mkRecipient("key-3", bytes.Repeat([]byte{0x33}, 40)),
		},
	}
}

// TestRoundTrip groups the encode→decode preservation checks: a populated
// envelope must survive a round trip with every header field, every recipient,
// a detached payload, and an out-of-int64-range value all intact.
func TestRoundTrip(t *testing.T) {
	t.Run("preserves all fields", func(t *testing.T) {
		orig := sampleEnvelope()

		encoded, err := orig.Encode()
		require.NoError(t, err)

		env, rest, err := Decode(encoded, WithExpectedType(exampleType))
		require.NoError(t, err)
		require.Len(t, rest, 0)

		// Whole-map equality is the strongest "intact" check: every protected and
		// unprotected entry, including the nested map and custom label, must
		// survive. Expected values use the normalized (int64) integer form.
		wantProtected := Header{
			HeaderLabelAlg:         AlgA256GCM,
			HeaderLabelType:        exampleType,
			HeaderLabelContentType: "text/plain",
			labelNegSmall:          int64(262144),
			labelNegLarge:          map[any]any{"name": "example", "count": int64(3)},
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
	})

	t.Run("multi recipient", func(t *testing.T) {
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
			require.Equal(t, AlgECDHESA256KW, alg)
			require.Equal(t, want.Ciphertext, got.Ciphertext)
			require.Equal(t, want.Headers.Unprotected, got.Headers.Unprotected)
		}
	})

	t.Run("detached payload", func(t *testing.T) {
		env := sampleEnvelope()
		envelope, err := env.Encode()
		require.NoError(t, err)

		ciphertext := []byte("the detached ciphertext bytes, opaque to cose")
		blob := append(append([]byte{}, envelope...), ciphertext...)

		decoded, rest, err := Decode(blob)
		require.NoError(t, err)
		require.Equal(t, ciphertext, rest)
		require.Len(t, decoded.Recipients, len(env.Recipients))
	})

	t.Run("large unsigned value", func(t *testing.T) {
		// A header value that exceeds MaxInt64 is preserved as uint64 and must
		// survive a round trip: a value this package can Encode it must also be
		// able to Decode. Regression for the previously strict decode mode, which
		// rejected any unsigned integer above MaxInt64.
		const big = uint64(math.MaxUint64)
		env := &Envelope{
			Headers: Headers{Protected: Header{}.
				Set("big", big).
				Set("nested", map[any]any{"n": big, "small": int64(7)})},
			Recipients: []*Recipient{{
				Headers:    Headers{Protected: Header{}.Set(HeaderLabelAlg, AlgECDHESA256KW)},
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
	})
}

// TestEncode groups encoder behavior: deterministic canonical output, recipient
// validation, and the empty-protected-header wire form.
func TestEncode(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		env := sampleEnvelope()
		a, err := env.Encode()
		require.NoError(t, err)
		b, err := env.Encode()
		require.NoError(t, err)
		require.Equal(t, a, b)

		// Header map-key insertion order must not affect the bytes: a protected
		// header built in a different order — including the nested map's keys —
		// encodes identically (canonical sort).
		reordered := &Envelope{
			Headers: Headers{Protected: Header{}.
				Set(labelNegLarge, map[any]any{"count": int64(3), "name": "example"}).
				Set("x-custom", []byte{0x01, 0x02, 0x03}).
				Set(labelNegSmall, 262144).
				Set(HeaderLabelContentType, "text/plain").
				Set(HeaderLabelType, exampleType).
				Set(HeaderLabelAlg, AlgA256GCM),
				Unprotected: Header{}.Set(int64(100), "unauthenticated").Set(HeaderLabelIV, bytes.Repeat([]byte{0xAB}, 7)),
			},
			Recipients: env.Recipients,
		}
		c, err := reordered.Encode()
		require.NoError(t, err)
		require.Equal(t, a, c)
	})

	t.Run("recipient-less encodes as COSE_Encrypt0", func(t *testing.T) {
		// With no recipients an Envelope is a COSE_Encrypt0 (tag 16), not an
		// error: recipient presence is what selects the form.
		env := &Envelope{Headers: Headers{Protected: Header{}.Set(HeaderLabelAlg, AlgA256GCM)}}
		encoded, err := env.Encode()
		require.NoError(t, err)
		require.Equal(t, byte(0xd0), encoded[0], "recipient-less envelope must encode as tag 16")

		decoded, _, err := Decode(encoded)
		require.NoError(t, err)
		require.Len(t, decoded.Recipients, 0)
	})

	t.Run("empty protected header", func(t *testing.T) {
		env := &Envelope{
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
	})
}

// TestEncStructure groups the Enc_structure / AAD checks: the AAD must bind the
// protected header (and external_aad) but not the unprotected header, and must
// stay stable across an encode→decode round trip.
func TestEncStructure(t *testing.T) {
	t.Run("binds protected not unprotected", func(t *testing.T) {
		base := func() *Envelope {
			return &Envelope{
				Headers: Headers{
					Protected:   Header{}.Set(HeaderLabelAlg, AlgA256GCM),
					Unprotected: Header{}.Set(HeaderLabelIV, []byte{0x01}),
				},
				Recipients: []*Recipient{{Ciphertext: []byte{0xAA}}},
			}
		}

		aad := func(e *Envelope) []byte {
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

		// Changing the protected header (here, to a different algorithm) MUST
		// change the AAD.
		diffAAD := base()
		diffAAD.Headers.Protected.Set(HeaderLabelAlg, AlgA256KW)
		require.NotEqual(t, ref, aad(diffAAD), "AAD unchanged when the protected header changed")

		// external_aad must be incorporated.
		withExt := aad(base())
		withExtBytes, err := base().EncStructure([]byte("aad"))
		require.NoError(t, err)
		require.NotEqual(t, withExt, withExtBytes, "external_aad not incorporated into Enc_structure")
	})

	t.Run("stable across round trip", func(t *testing.T) {
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
	})
}
