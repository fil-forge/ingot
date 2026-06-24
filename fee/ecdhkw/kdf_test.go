package ecdhkw

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestKDFContextCanonical pins the exact CBOR encoding of the COSE_KDF_Context
// for the parameters this package uses (AlgorithmID = A256KW = -5, 256-bit
// derived key, empty protected header). The bytes are the canonical, shortest-
// form encoding of:
//
//	[ -5, [null,null,null], [null,null,null], [256, h''] ]
//
// A divergence here would silently change every derived KEK, so this is the
// anchor for cross-implementation compatibility.
func TestKDFContextCanonical(t *testing.T) {
	got := kdfContext(algA256KW, kekLen*8, nil)
	want := mustDecode(t, "842483f6f6f6"+"83f6f6f6"+"8219010040")
	require.Equal(t, want, got, "kdfContext mismatch")
}

// TestKDFContextProtected confirms a non-empty protected header is embedded as
// a CBOR byte string verbatim (so the structure stays correct if a future
// caller does carry one).
func TestKDFContextProtected(t *testing.T) {
	got := kdfContext(algA256KW, kekLen*8, []byte{0xa1, 0x01, 0x38, 0x1e})
	// ...trailing SuppPubInfo: [256, h'a101381e'] -> 82 19 0100 44 a101381e
	want := mustDecode(t, "842483f6f6f6"+"83f6f6f6"+"821901004"+"4a101381e")
	require.Equal(t, want, got, "kdfContext(protected) mismatch")
}

// TestConcatKDFSingleBlock checks the one-round case (the A256KW case: 32-byte
// output from SHA-256) against an independent one-shot hash of
// counter(1) || z || otherInfo.
func TestConcatKDFSingleBlock(t *testing.T) {
	z := bytes.Repeat([]byte{0xAB}, 32)
	other := []byte("other-info")

	got := concatKDF(z, other, 32)

	h := sha256.New()
	h.Write([]byte{0x00, 0x00, 0x00, 0x01})
	h.Write(z)
	h.Write(other)
	want := h.Sum(nil)

	require.Equal(t, want, got, "concatKDF single block mismatch")
}

// TestConcatKDFMultiBlock checks the multi-round case: two SHA-256 blocks
// (counter 1 then counter 2), concatenated and truncated to the requested
// length.
func TestConcatKDFMultiBlock(t *testing.T) {
	z := bytes.Repeat([]byte{0x07}, 32)
	other := []byte("ctx")

	got := concatKDF(z, other, 48)
	require.Len(t, got, 48)

	block := func(counter byte) []byte {
		h := sha256.New()
		h.Write([]byte{0x00, 0x00, 0x00, counter})
		h.Write(z)
		h.Write(other)
		return h.Sum(nil)
	}
	want := append(block(1), block(2)...)[:48]

	require.Equal(t, want, got, "concatKDF multi block mismatch")
}

// TestConcatKDFContextSensitivity confirms the derived key depends on the
// context bytes — derivations under different contexts must not collide.
func TestConcatKDFContextSensitivity(t *testing.T) {
	z := bytes.Repeat([]byte{0x42}, 32)
	a := concatKDF(z, kdfContext(algA256KW, 256, nil), 32)
	b := concatKDF(z, kdfContext(algA256KW, 128, nil), 32)
	require.NotEqual(t, a, b, "derivations under different keyDataLength contexts collided")
}

// TestCBORHeadEncoding pins the shortest-form (canonical) CBOR head encoding
// across every argument-size class, for the major types the context uses.
func TestCBORHeadEncoding(t *testing.T) {
	tests := []struct {
		name  string
		major byte
		arg   uint64
		want  string
	}{
		{"uint tiny", cborUint, 0, "00"},
		{"uint max inline", cborUint, 23, "17"},
		{"uint 1-byte", cborUint, 24, "1818"},
		{"uint 1-byte max", cborUint, 255, "18ff"},
		{"uint 2-byte", cborUint, 256, "190100"},
		{"uint 2-byte max", cborUint, 65535, "19ffff"},
		{"uint 4-byte", cborUint, 65536, "1a00010000"},
		{"uint 4-byte max", cborUint, 1<<32 - 1, "1affffffff"},
		{"uint 8-byte", cborUint, 1 << 32, "1b0000000100000000"},
		{"array(4)", cborArray, 4, "84"},
		{"empty bstr", cborBytes, 0, "40"},
		{"4-byte bstr", cborBytes, 4, "44"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := hex.EncodeToString(cborHead(nil, tc.major, tc.arg))
			require.Equalf(t, tc.want, got, "cborHead(%d, %d)", tc.major, tc.arg)
		})
	}
}

// TestCBORIntEncoding pins the integer encoding, including the negative range
// (CBOR major type 1, argument = -1-n) used for the algorithm identifier.
func TestCBORIntEncoding(t *testing.T) {
	tests := []struct {
		n    int64
		want string
	}{
		{0, "00"},
		{23, "17"},
		{24, "1818"},
		{-1, "20"},
		{-5, "24"}, // A256KW
		{-24, "37"},
		{-25, "3818"},
		{-256, "38ff"},
		{-257, "390100"},
	}
	for _, tc := range tests {
		got := hex.EncodeToString(cborInt(nil, tc.n))
		require.Equalf(t, tc.want, got, "cborInt(%d)", tc.n)
	}
}
