package vectors

import (
	"bytes"
	"crypto/ecdh"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/fil-forge/ingot/fee/aeskw"
	"github.com/fil-forge/ingot/fee/aesstream"
	"github.com/fil-forge/ingot/fee/cose"
	"github.com/fil-forge/ingot/fee/ecdhkw"
)

// FEE wire constants. These MUST match the foc-encryption reference
// (packages/foc-encryption/src/cose/headers.ts at the pinned commit — see
// pull-foc-encryption.sh). They are the values this issue reconciled the Go
// side to; the private-use labels are FEE-specific and live here (the caller),
// not in the FEE-agnostic cose package.
const (
	// feeTyp is the COSE protected "typ" (label 16) that pins the envelope
	// profile. Reference: FOC_ENVELOPE_TYPE.
	feeTyp = "application/vnd.foc-envelope+cose"

	// algChunkedStream is the private-use COSE algorithm id for the chunked
	// AES-256-GCM-STREAM body cipher. Reference: CHUNKED_AES_256_GCM_STREAM.
	algChunkedStream = int64(-65793)

	// labelChunkSize and labelChunkCount are the private-use unprotected header
	// labels carrying the STREAM geometry. Reference: CoseHeaderParam.CHUNK_SIZE
	// / CHUNK_COUNT.
	labelChunkSize  = int64(-65790)
	labelChunkCount = int64(-65791)

	// vectorChunkSize is the plaintext chunk size used by the Go-produced
	// fixtures. aesstream requires >= 4 KiB (MinChunkSize); the reference
	// accepts any size and reads the effective size from the envelope, so this
	// is the smallest value that interoperates and keeps multi-chunk fixtures
	// small.
	vectorChunkSize = aesstream.MinChunkSize // 4 KiB
)

// --- deterministic, NON-SECRET test key material -------------------------
//
// All key material is derived from fixed labels so the fixtures are
// reproducible and self-describing. None of it is secret; it exists only to
// pin cross-implementation vectors, exactly as the issue requires.

func deriveBytes(label string, n int) []byte {
	sum := sha256.Sum256([]byte(label))
	if n > len(sum) {
		panic("vectors: derive length exceeds 32")
	}
	return sum[:n]
}

func testCEK() []byte       { return deriveBytes("fil-473-fee-cek-v1", 32) }
func testBaseNonce() []byte { return deriveBytes("fil-473-fee-base-nonce-v1", aesstream.BaseNonceSize) }
func testA256KWKEK() []byte { return deriveBytes("fil-473-fee-region-kek-v1", 32) }

// testTenantKey returns the fixed X25519 recipient key pair used by the
// ECDH-ES+A256KW recipient. The scalar is derived from a label; X25519 clamps
// it internally.
func testTenantKey() (*ecdh.PrivateKey, error) {
	return ecdh.X25519().NewPrivateKey(deriveBytes("fil-473-fee-tenant-x25519-v1", 32))
}

const a256kwKid = "region-key-1"

// --- envelope composition (inlined; mirrors what a fee.Encrypt would do) ---

// chunkCountFor reports how many STREAM chunks a plaintext of nPlain bytes
// occupies at the given chunk size, matching the reference: max(1, ceil(n/c)).
// Empty input is one (empty) final chunk.
func chunkCountFor(nPlain, chunkSize int) int {
	if nPlain == 0 {
		return 1
	}
	return (nPlain + chunkSize - 1) / chunkSize
}

// composeFEE seals plaintext into a FEE blob (envelope || ciphertext) using the
// chunked AES-256-GCM-STREAM body cipher. recipients==nil yields a COSE_Encrypt0
// (tag 16); a non-empty recipients slice yields a COSE_Encrypt (tag 96). The
// body AAD is the envelope's own Enc_structure, so its context tracks the form
// (recipient presence) with no separate selection.
func composeFEE(plaintext, cek, baseNonce []byte, chunkSize int, recipients []*cose.Recipient) ([]byte, int, error) {
	chunkCount := chunkCountFor(len(plaintext), chunkSize)
	headers := cose.Headers{
		Protected: cose.Header{}.
			Set(cose.HeaderLabelAlg, algChunkedStream).
			Set(cose.HeaderLabelType, feeTyp),
		Unprotected: cose.Header{}.
			Set(cose.HeaderLabelIV, baseNonce).
			Set(labelChunkSize, int64(chunkSize)).
			Set(labelChunkCount, int64(chunkCount)),
	}

	// Recipient presence selects the form: nil/empty -> COSE_Encrypt0 (tag 16),
	// non-empty -> COSE_Encrypt (tag 96).
	env := &cose.Envelope{Headers: headers, Recipients: recipients}

	aad, err := env.EncStructure(nil)
	if err != nil {
		return nil, 0, fmt.Errorf("enc_structure: %w", err)
	}

	var body bytes.Buffer
	w, err := aesstream.NewWriter(&body, aesstream.Config{Key: cek, BaseNonce: baseNonce, AAD: aad, ChunkSize: chunkSize})
	if err != nil {
		return nil, 0, fmt.Errorf("new writer: %w", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		return nil, 0, fmt.Errorf("write body: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, 0, fmt.Errorf("close body: %w", err)
	}

	envBytes, err := env.Encode()
	if err != nil {
		return nil, 0, fmt.Errorf("encode envelope: %w", err)
	}

	blob := make([]byte, 0, len(envBytes)+body.Len())
	blob = append(blob, envBytes...)
	blob = append(blob, body.Bytes()...)
	return blob, chunkCount, nil
}

// ecdhESRecipient wraps cek to the fixed tenant X25519 key via ECDH-ES+A256KW
// and returns the COSE_Recipient entry (alg -31, kid = tenant public key, the
// fresh ephemeral key in the ephemeral-key header, wrapped CEK as ciphertext).
func ecdhESRecipient(cek []byte) (*cose.Recipient, error) {
	priv, err := testTenantKey()
	if err != nil {
		return nil, err
	}
	w, err := ecdhkw.Wrap(priv.PublicKey(), cek)
	if err != nil {
		return nil, err
	}
	return &cose.Recipient{
		Headers: cose.Headers{
			Protected: cose.Header{}.Set(cose.HeaderLabelAlg, cose.AlgECDHESA256KW),
			Unprotected: cose.Header{}.
				Set(cose.HeaderLabelKID, priv.PublicKey().Bytes()).
				Set(cose.HeaderLabelEphemeralKey, map[any]any{
					int64(1):  int64(1), // kty: OKP
					int64(-1): int64(4), // crv: X25519
					int64(-2): w.EphemeralPublicKey.Bytes(),
				}),
		},
		Ciphertext: w.WrappedCEK,
	}, nil
}

// a256kwRecipient wraps cek under the fixed region KEK via A256KW and returns
// the COSE_Recipient entry (alg -5, a symmetric kid, wrapped CEK as ciphertext).
func a256kwRecipient(cek []byte) (*cose.Recipient, error) {
	wrapped, err := aeskw.Wrap(testA256KWKEK(), cek)
	if err != nil {
		return nil, err
	}
	return &cose.Recipient{
		Headers: cose.Headers{
			Protected:   cose.Header{}.Set(cose.HeaderLabelAlg, cose.AlgA256KW),
			Unprotected: cose.Header{}.Set(cose.HeaderLabelKID, []byte(a256kwKid)),
		},
		Ciphertext: wrapped,
	}, nil
}

// --- envelope parsing + body decryption (Go consuming any FEE blob) --------

// parsedFEE is what decodeFEE recovers from a blob: the header pair, the
// detached ciphertext, the resolved geometry, and the body AAD (the decoded
// envelope's Enc_structure, whose context already matches the form). recipients
// is nil for a recipient-less (tag 16) envelope.
type parsedFEE struct {
	headers    cose.Headers
	recipients []*cose.Recipient
	ciphertext []byte
	baseNonce  []byte
	chunkSize  int
	aad        []byte
}

// decodeFEE parses a FEE blob (tag 16 or tag 96), enforcing the FEE typ, and
// returns the header pair, recipients (nil for tag 16), detached ciphertext and
// STREAM geometry. A single cose.Decode dispatches on the tag.
func decodeFEE(blob []byte) (*parsedFEE, error) {
	env, rest, err := cose.Decode(blob, cose.WithExpectedType(feeTyp))
	if err != nil {
		return nil, err
	}
	out := &parsedFEE{headers: env.Headers, recipients: env.Recipients, ciphertext: rest}
	if out.aad, err = env.EncStructure(nil); err != nil {
		return nil, fmt.Errorf("enc_structure: %w", err)
	}

	alg, ok := out.headers.Protected.Int(cose.HeaderLabelAlg)
	if !ok || alg != algChunkedStream {
		return nil, fmt.Errorf("unexpected body algorithm %d (want %d)", alg, algChunkedStream)
	}
	iv, ok := out.headers.Unprotected.Bytes(cose.HeaderLabelIV)
	if !ok {
		return nil, fmt.Errorf("missing iv (base nonce)")
	}
	out.baseNonce = iv
	if cs, ok := out.headers.Unprotected.Int(labelChunkSize); ok {
		out.chunkSize = int(cs)
	} else {
		out.chunkSize = aesstream.DefaultChunkSize
	}
	return out, nil
}

// decryptFEE decodes a FEE blob and decrypts its body with the given CEK,
// exactly as the reference does (the CEK is supplied directly; recipients are
// not consulted). It returns the recovered plaintext.
func decryptFEE(blob, cek []byte) ([]byte, error) {
	p, err := decodeFEE(blob)
	if err != nil {
		return nil, err
	}
	r, err := aesstream.NewReader(bytes.NewReader(p.ciphertext), aesstream.Config{
		Key: cek, BaseNonce: p.baseNonce, AAD: p.aad, ChunkSize: p.chunkSize,
	})
	if err != nil {
		return nil, fmt.Errorf("new reader: %w", err)
	}
	return io.ReadAll(r)
}

// --- fixture on-disk format ------------------------------------------------

type recipientMeta struct {
	Algorithm           int64  `json:"algorithm"`
	Kind                string `json:"kind"` // "ecdh-es-a256kw" | "a256kw"
	KidHex              string `json:"kid_hex"`
	TenantX25519PrivHex string `json:"tenant_x25519_priv_hex,omitempty"`
	A256KWKEKHex        string `json:"a256kw_kek_hex,omitempty"`
}

// vectorMeta is the JSON sidecar committed next to each fixture. The TS driver
// (fee/vectors/ts) writes and reads the same shape, so field names must stay in
// sync with driver.ts.
type vectorMeta struct {
	Name         string          `json:"name"`
	Producer     string          `json:"producer"` // "go" | "ts"
	Description  string          `json:"description"`
	Tag          uint64          `json:"tag"`
	Algorithm    int64           `json:"algorithm"`
	Typ          string          `json:"typ"`
	ChunkSize    int             `json:"chunk_size"`
	ChunkCount   int             `json:"chunk_count"`
	CEKHex       string          `json:"cek_hex"`
	BaseNonceHex string          `json:"base_nonce_hex,omitempty"`
	Recipients   []recipientMeta `json:"recipients,omitempty"`
}

const testdataDir = "testdata"

// fixture bundles a loaded vector.
type fixture struct {
	dir       string
	meta      vectorMeta
	blob      []byte
	plaintext []byte
}

// loadFixtures reads every testdata/<name>/ directory holding a meta.json.
func loadFixtures() ([]fixture, error) {
	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		return nil, err
	}
	var out []fixture
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(testdataDir, e.Name())
		metaBytes, err := os.ReadFile(filepath.Join(dir, "meta.json"))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		var m vectorMeta
		if err := json.Unmarshal(metaBytes, &m); err != nil {
			return nil, fmt.Errorf("%s/meta.json: %w", dir, err)
		}
		blob, err := os.ReadFile(filepath.Join(dir, "blob.bin"))
		if err != nil {
			return nil, err
		}
		plain, err := os.ReadFile(filepath.Join(dir, "plaintext.bin"))
		if err != nil {
			return nil, err
		}
		out = append(out, fixture{dir: dir, meta: m, blob: blob, plaintext: plain})
	}
	return out, nil
}

// writeFixture writes a fixture directory (blob.bin, plaintext.bin, meta.json).
func writeFixture(name string, m vectorMeta, blob, plaintext []byte) error {
	dir := filepath.Join(testdataDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "blob.bin"), blob, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "plaintext.bin"), plaintext, 0o644); err != nil {
		return err
	}
	metaBytes, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	metaBytes = append(metaBytes, '\n')
	return os.WriteFile(filepath.Join(dir, "meta.json"), metaBytes, 0o644)
}

func hexEncode(b []byte) string { return hex.EncodeToString(b) }
