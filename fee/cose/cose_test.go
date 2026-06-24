package cose

import (
	"bytes"
	"math"
	"reflect"
	"testing"
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
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	env, rest, err := Decode(encoded, WithExpectedType(feeTypeExample))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("rest = %x, want empty", rest)
	}

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
	if !reflect.DeepEqual(env.Headers.Protected, wantProtected) {
		t.Fatalf("protected mismatch:\n got %#v\nwant %#v", env.Headers.Protected, wantProtected)
	}

	wantUnprotected := Header{
		HeaderLabelIV: bytes.Repeat([]byte{0xAB}, 7),
		int64(100):    "unauthenticated",
	}
	if !reflect.DeepEqual(env.Headers.Unprotected, wantUnprotected) {
		t.Fatalf("unprotected mismatch:\n got %#v\nwant %#v", env.Headers.Unprotected, wantUnprotected)
	}

	// Re-encoding a decoded envelope reproduces the original bytes exactly
	// (deterministic, lossless).
	reencoded, err := env.Encode()
	if err != nil {
		t.Fatalf("re-Encode: %v", err)
	}
	if !bytes.Equal(reencoded, encoded) {
		t.Fatalf("re-encode not byte-identical:\n got %x\nwant %x", reencoded, encoded)
	}
}

func TestRoundTripMultiRecipient(t *testing.T) {
	orig := sampleEnvelope()
	encoded, err := orig.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	env, _, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if len(env.Recipients) != len(orig.Recipients) {
		t.Fatalf("recipients = %d, want %d", len(env.Recipients), len(orig.Recipients))
	}
	// Order and per-recipient fields must be preserved.
	for i, want := range orig.Recipients {
		got := env.Recipients[i]
		wantKID, _ := want.Headers.Unprotected.Bytes(HeaderLabelKID)
		gotKID, ok := got.Headers.Unprotected.Bytes(HeaderLabelKID)
		if !ok || !bytes.Equal(gotKID, wantKID) {
			t.Errorf("recipient %d kid = %x (ok=%v), want %x", i, gotKID, ok, wantKID)
		}
		if alg, ok := got.Headers.Protected.Int(HeaderLabelAlg); !ok || alg != -31 {
			t.Errorf("recipient %d alg = %d (ok=%v), want -31", i, alg, ok)
		}
		if !bytes.Equal(got.Ciphertext, want.Ciphertext) {
			t.Errorf("recipient %d ciphertext = %x, want %x", i, got.Ciphertext, want.Ciphertext)
		}
		if !reflect.DeepEqual(got.Headers.Unprotected, want.Headers.Unprotected) {
			t.Errorf("recipient %d unprotected mismatch:\n got %#v\nwant %#v",
				i, got.Headers.Unprotected, want.Headers.Unprotected)
		}
	}
}

func TestEncodeIsDeterministic(t *testing.T) {
	env := sampleEnvelope()
	a, err := env.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	b, err := env.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("Encode not deterministic:\n a=%x\n b=%x", a, b)
	}

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
	if err != nil {
		t.Fatalf("Encode reordered: %v", err)
	}
	if !bytes.Equal(a, c) {
		t.Fatalf("encoding depends on insertion order:\n a=%x\n c=%x", a, c)
	}
}

func TestEncodeRequiresRecipient(t *testing.T) {
	env := &Encrypt{Headers: Headers{Protected: Header{}.Set(HeaderLabelAlg, 3)}}
	if _, err := env.Encode(); err == nil {
		t.Fatal("Encode with no recipients: want error, got nil")
	}
}

func TestDetachedPayloadRoundTrip(t *testing.T) {
	env := sampleEnvelope()
	envelope, err := env.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	ciphertext := []byte("the detached ciphertext bytes, opaque to cose")
	blob := append(append([]byte{}, envelope...), ciphertext...)

	decoded, rest, err := Decode(blob)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(rest, ciphertext) {
		t.Fatalf("rest = %q, want %q", rest, ciphertext)
	}
	if len(decoded.Recipients) != len(env.Recipients) {
		t.Fatalf("recipients = %d, want %d", len(decoded.Recipients), len(env.Recipients))
	}
}

func TestEmptyProtectedHeader(t *testing.T) {
	env := &Encrypt{
		Headers:    Headers{Unprotected: Header{}.Set(HeaderLabelIV, []byte{0x01})},
		Recipients: []*Recipient{{Ciphertext: []byte{0xAA}}},
	}
	encoded, err := env.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// An empty protected header must serialize as an empty byte string (0x40),
	// not as an encoded empty map. The protected element follows the tag (d860)
	// and array head (84).
	if encoded[2] != 0x84 || encoded[3] != 0x40 {
		t.Fatalf("empty protected not encoded as h'': bytes = %x", encoded[:6])
	}

	decoded, _, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(decoded.Headers.Protected) != 0 {
		t.Fatalf("protected = %#v, want empty", decoded.Headers.Protected)
	}
	pb, err := decoded.ProtectedBytes()
	if err != nil {
		t.Fatalf("ProtectedBytes: %v", err)
	}
	if len(pb) != 0 {
		t.Fatalf("ProtectedBytes = %x, want empty", pb)
	}
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
		if err != nil {
			t.Fatalf("EncStructure: %v", err)
		}
		return b
	}

	ref := aad(base())

	// Changing the unprotected header must NOT change the AAD.
	sameAAD := base()
	sameAAD.Headers.Unprotected.Set(HeaderLabelIV, []byte{0x02})
	if !bytes.Equal(ref, aad(sameAAD)) {
		t.Error("AAD changed when only the unprotected header changed")
	}

	// Changing the protected header MUST change the AAD.
	diffAAD := base()
	diffAAD.Headers.Protected.Set(HeaderLabelAlg, -65793)
	if bytes.Equal(ref, aad(diffAAD)) {
		t.Error("AAD unchanged when the protected header changed")
	}

	// external_aad must be incorporated.
	withExt := aad(base())
	withExtBytes, err := base().EncStructure([]byte("aad"))
	if err != nil {
		t.Fatalf("EncStructure(ext): %v", err)
	}
	if bytes.Equal(withExt, withExtBytes) {
		t.Error("external_aad not incorporated into Enc_structure")
	}
}

func TestEncStructureStableAcrossRoundTrip(t *testing.T) {
	orig := sampleEnvelope()
	want, err := orig.EncStructure([]byte("ext"))
	if err != nil {
		t.Fatalf("EncStructure: %v", err)
	}

	encoded, err := orig.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, _, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	got, err := decoded.EncStructure([]byte("ext"))
	if err != nil {
		t.Fatalf("EncStructure (decoded): %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("AAD not stable across round trip:\n got %x\nwant %x", got, want)
	}
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
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, _, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// The top-level value comes back as the same uint64.
	got, ok := decoded.Headers.Protected.Uint("big")
	if !ok || got != big {
		t.Fatalf("top-level big = %d (ok=%v), want %d", got, ok, big)
	}
	// Recursion: a large unsigned stays uint64 inside a nested map, while a
	// value that fits is normalized back to int64.
	nested, ok := decoded.Headers.Protected.Get("nested")
	if !ok {
		t.Fatal("nested map missing after round trip")
	}
	want := map[any]any{"n": big, "small": int64(7)}
	if !reflect.DeepEqual(nested, want) {
		t.Fatalf("nested map mismatch:\n got %#v\nwant %#v", nested, want)
	}
}
