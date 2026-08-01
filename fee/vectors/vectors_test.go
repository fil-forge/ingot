package vectors

import (
	"bytes"
	"crypto/ecdh"
	"encoding/hex"
	"os"
	"testing"

	"github.com/fil-forge/ingot/fee/aeskw"
	"github.com/fil-forge/ingot/fee/cose"
	"github.com/fil-forge/ingot/fee/ecdhkw"
	"github.com/stretchr/testify/require"
)

// coreVectors names the fixtures that cover the three acceptance criteria. The
// suite fails if any is missing, so a dropped fixture can't silently reduce
// coverage. The producer records which side sealed the blob:
//
//	single-chunk-go   AC1: Go seals a single-chunk file; the TS reference decrypts it.
//	multi-chunk-ts    AC2: the TS reference seals a multi-chunk file; Go decrypts it.
//	multi-recipient-go AC3: Go seals a multi-recipient envelope; the TS reference
//	                   parses each recipient and decrypts the body from the CEK.
var coreVectors = map[string]string{
	"single-chunk-go":    "go",
	"multi-chunk-ts":     "ts",
	"multi-recipient-go": "go",
}

// TestVectors verifies that this Go implementation decrypts every committed
// fixture — whichever side produced it — and, for multi-recipient envelopes,
// that each recipient's CEK really unwraps to the shared CEK. It is
// deterministic: it only reads fixed files and runs deterministic decrypt/unwrap.
func TestVectors(t *testing.T) {
	fixtures, err := loadFixtures()
	require.NoError(t, err)
	require.NotEmpty(t, fixtures, "no fixtures found; run FEE_VECTORS_REGEN=1 go test -run TestGenerate and pull-foc-encryption.sh")

	seen := map[string]string{}
	for _, f := range fixtures {
		f := f
		t.Run(f.meta.Name, func(t *testing.T) {
			seen[f.meta.Name] = f.meta.Producer

			// The fixture must declare the FEE profile we reconciled to.
			require.Equal(t, feeTyp, f.meta.Typ, "envelope typ")
			require.Equal(t, algChunkedStream, f.meta.Algorithm, "body algorithm")

			tag, err := cose.PeekTag(f.blob)
			require.NoError(t, err)
			require.Equal(t, f.meta.Tag, tag, "declared vs actual COSE tag")

			cek, err := hex.DecodeString(f.meta.CEKHex)
			require.NoError(t, err)

			// AC core: Go decrypts the blob from the shared CEK, exactly as the
			// reference's decrypt(blob, cek) does.
			got, err := decryptFEE(f.blob, cek)
			require.NoError(t, err, "decrypt body")
			require.Equal(t, f.plaintext, got, "recovered plaintext")

			// Determinism: a second decrypt yields the same bytes.
			again, err := decryptFEE(f.blob, cek)
			require.NoError(t, err)
			require.Equal(t, got, again)

			if f.meta.Tag == cose.TagCOSEEncrypt {
				verifyRecipients(t, f, cek)
			} else {
				require.Empty(t, f.meta.Recipients, "tag-16 envelope must have no recipients")
			}
		})
	}

	for name, producer := range coreVectors {
		require.Equalf(t, producer, seen[name],
			"missing/mismatched core fixture %q (have %q); regenerate Go fixtures and run pull-foc-encryption.sh", name, seen[name])
	}
}

// verifyRecipients decodes the tag-96 envelope and, for each declared
// recipient, actually unwraps the CEK on the Go side and checks it equals the
// shared CEK — the assertion the reference cannot make (it has no unwrap code).
func verifyRecipients(t *testing.T, f fixture, cek []byte) {
	t.Helper()
	p, err := decodeFEE(f.blob)
	require.NoError(t, err)
	require.Len(t, p.recipients, len(f.meta.Recipients), "recipient count meta vs envelope")

	byAlg := map[int64]*cose.Recipient{}
	for _, r := range p.recipients {
		alg, ok := r.Headers.Protected.Int(cose.HeaderLabelAlg)
		require.True(t, ok, "recipient algorithm present")
		byAlg[alg] = r
	}

	for _, rm := range f.meta.Recipients {
		r := byAlg[rm.Algorithm]
		require.NotNilf(t, r, "envelope missing recipient alg %d", rm.Algorithm)

		// The kid binds the descriptor to its key.
		kid, ok := r.Headers.Unprotected.Bytes(cose.HeaderLabelKID)
		require.True(t, ok, "recipient kid present")
		require.Equal(t, rm.KidHex, hex.EncodeToString(kid), "recipient kid")

		switch rm.Kind {
		case "ecdh-es-a256kw":
			require.Equal(t, cose.AlgECDHESA256KW, rm.Algorithm)
			priv := mustX25519Priv(t, rm.TenantX25519PrivHex)
			ephPub := ephemeralPub(t, r)
			unwrapped, err := ecdhkw.Unwrap(priv, &ecdhkw.Wrapped{
				EphemeralPublicKey: ephPub,
				WrappedCEK:         r.Ciphertext,
			})
			require.NoError(t, err, "ECDH-ES+A256KW unwrap")
			require.Equal(t, cek, unwrapped, "unwrapped CEK matches shared CEK")

			// Wrong key must fail rather than return garbage.
			wrongPriv, err := ecdh.X25519().GenerateKey(zeroReader{})
			require.NoError(t, err)
			_, err = ecdhkw.Unwrap(wrongPriv, &ecdhkw.Wrapped{EphemeralPublicKey: ephPub, WrappedCEK: r.Ciphertext})
			require.Error(t, err, "unwrap with wrong key must fail")

		case "a256kw":
			require.Equal(t, cose.AlgA256KW, rm.Algorithm)
			kek, err := hex.DecodeString(rm.A256KWKEKHex)
			require.NoError(t, err)
			unwrapped, err := aeskw.Unwrap(kek, r.Ciphertext)
			require.NoError(t, err, "A256KW unwrap")
			require.Equal(t, cek, unwrapped, "unwrapped CEK matches shared CEK")

		default:
			t.Fatalf("unknown recipient kind %q", rm.Kind)
		}
	}
}

func ephemeralPub(t *testing.T, r *cose.Recipient) *ecdh.PublicKey {
	t.Helper()
	v, ok := r.Headers.Unprotected.Get(cose.HeaderLabelEphemeralKey)
	require.True(t, ok, "ephemeral key header present")
	ek, ok := v.(map[any]any)
	require.True(t, ok, "ephemeral key is a COSE_Key map")
	x, ok := ek[int64(-2)].([]byte)
	require.True(t, ok, "ephemeral key x coordinate present")
	pub, err := ecdh.X25519().NewPublicKey(x)
	require.NoError(t, err)
	return pub
}

func mustX25519Priv(t *testing.T, h string) *ecdh.PrivateKey {
	t.Helper()
	raw, err := hex.DecodeString(h)
	require.NoError(t, err)
	priv, err := ecdh.X25519().NewPrivateKey(raw)
	require.NoError(t, err)
	return priv
}

// zeroReader is a deterministic non-random source used only to mint a throwaway
// "wrong" key for the negative unwrap check.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0x2b
	}
	return len(p), nil
}

// TestGenerate (re)writes the Go-produced fixtures. It is guarded by
// FEE_VECTORS_REGEN so a normal `go test` never rewrites checked-in files. The
// TS-produced fixtures come from pull-foc-encryption.sh, not from here.
//
//	FEE_VECTORS_REGEN=1 GOWORK=off go test ./fee/vectors -run TestGenerate -v
func TestGenerate(t *testing.T) {
	if os.Getenv("FEE_VECTORS_REGEN") == "" {
		t.Skip("set FEE_VECTORS_REGEN=1 to regenerate the Go-produced fixtures")
	}

	// AC1 — single-chunk file sealed in Go (tag 16), for the reference to decrypt.
	genGoBody(t, "single-chunk-go",
		"AC1: single-chunk file encrypted in Go; decrypts in foc-encryption (TS).",
		[]byte("FEE cross-impl vector FIL-473: single chunk, sealed in Go, opened in TS.\n"))

	// A Go-sealed multi-chunk file too (tag 16), so the reference is exercised
	// across a chunk boundary in the Go->TS direction as well.
	genGoBody(t, "multi-chunk-go",
		"Multi-chunk file encrypted in Go (spans several STREAM chunks); decrypts in foc-encryption (TS).",
		bytes.Repeat([]byte("multi-chunk-go/FIL-473 "), 700)) // ~15 KiB > chunk size

	// AC3 — multi-recipient envelope sealed in Go (tag 96) with a real
	// ECDH-ES+A256KW (X25519) recipient and a real A256KW recipient.
	genGoMultiRecipient(t, "multi-recipient-go",
		"AC3: multi-recipient envelope (ECDH-ES+A256KW/X25519 and A256KW) encrypted in Go; foc-encryption parses recipients and decrypts the body from the CEK.",
		[]byte("FEE cross-impl vector FIL-473: multi-recipient envelope, two wrapped CEKs.\n"))
}

func genGoBody(t *testing.T, name, desc string, plaintext []byte) {
	t.Helper()
	cek, baseNonce := testCEK(name), testBaseNonce()
	blob, chunkCount, err := composeFEE(plaintext, cek, baseNonce, vectorChunkSize, nil)
	require.NoError(t, err)
	m := vectorMeta{
		Name: name, Producer: "go", Description: desc,
		Tag: cose.TagCOSEEncrypt0, Algorithm: algChunkedStream, Typ: feeTyp,
		ChunkSize: vectorChunkSize, ChunkCount: chunkCount,
		CEKHex: hexEncode(cek), BaseNonceHex: hexEncode(baseNonce),
	}
	require.NoError(t, writeFixture(name, m, blob, plaintext))
	// Self-check: it decrypts under our own reader.
	got, err := decryptFEE(blob, cek)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
	t.Logf("wrote %s (%d bytes, %d chunk(s))", name, len(blob), chunkCount)
}

func genGoMultiRecipient(t *testing.T, name, desc string, plaintext []byte) {
	t.Helper()
	cek, baseNonce := testCEK(name), testBaseNonce()
	ecdhR, err := ecdhESRecipient(cek)
	require.NoError(t, err)
	a256R, err := a256kwRecipient(cek)
	require.NoError(t, err)

	blob, chunkCount, err := composeFEE(plaintext, cek, baseNonce, vectorChunkSize, []*cose.Recipient{ecdhR, a256R})
	require.NoError(t, err)

	tenantPriv, err := testTenantKey()
	require.NoError(t, err)

	m := vectorMeta{
		Name: name, Producer: "go", Description: desc,
		Tag: cose.TagCOSEEncrypt, Algorithm: algChunkedStream, Typ: feeTyp,
		ChunkSize: vectorChunkSize, ChunkCount: chunkCount,
		CEKHex: hexEncode(cek), BaseNonceHex: hexEncode(baseNonce),
		Recipients: []recipientMeta{
			{
				Algorithm: cose.AlgECDHESA256KW, Kind: "ecdh-es-a256kw",
				KidHex:              hexEncode(tenantPriv.PublicKey().Bytes()),
				TenantX25519PrivHex: hexEncode(tenantPriv.Bytes()),
			},
			{
				Algorithm: cose.AlgA256KW, Kind: "a256kw",
				KidHex:       hexEncode([]byte(a256kwKid)),
				A256KWKEKHex: hexEncode(testA256KWKEK()),
			},
		},
	}
	require.NoError(t, writeFixture(name, m, blob, plaintext))

	got, err := decryptFEE(blob, cek)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
	t.Logf("wrote %s (%d bytes, %d recipients)", name, len(blob), len(m.Recipients))
}
