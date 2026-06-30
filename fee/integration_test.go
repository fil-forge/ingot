// Package fee_test's integration test is the FEE (FilOne File Encryption
// Envelope) insurance-recovery artifact. It is the v1 confidence check for the
// Hilt recovery path (see FIL-474): it proves the composed fee API recovers
// plaintext from an envelope using only an archived tenant X25519 private key —
// no region KEK and no Ingot database.
//
// Unlike the sub-package unit tests, this exercises the whole round trip through
// the top-level fee package (fee.Encrypt / fee.Decrypt, FIL-569), which composes
// the COSE envelope, the chunked AES-256-GCM-STREAM body cipher and the
// ECDH-ES+A256KW tenant key wrap. The one place it drops to a sub-package is the
// explicit kid assertion: the issue calls for reading the recipient entry's kid
// off the wire and confirming it names the tenant key before recovery, so the
// test decodes the envelope header with fee/cose to check that directly rather
// than relying on the kid match fee.Decrypt performs internally.
//
// The envelope is not a fixture: it is built at run time by fee.Encrypt. Only
// the tenant keypair is checked in, for determinism.
package fee_test

import (
	"bytes"
	"crypto/ecdh"
	"encoding/hex"
	"io"
	"testing"

	"github.com/fil-forge/ingot/fee"
	"github.com/fil-forge/ingot/fee/aeskw"
	"github.com/fil-forge/ingot/fee/aesstream"
	"github.com/fil-forge/ingot/fee/cose"
	"github.com/stretchr/testify/require"
)

const (
	// streamChunkSize is the STREAM plaintext chunk size passed to
	// fee.WithChunkSize. The issue asks for a small chunk (vs the 256 KiB
	// default) so the multi-chunk path runs against a few-KB sample without a
	// large fixture; aesstream rejects anything below MinChunkSize (4 KiB), so
	// that minimum is the smallest legal "small" value. fee records it in the
	// envelope, so recovery recovers it without the test tracking it.
	streamChunkSize = aesstream.MinChunkSize

	// samplePlaintextSize is a few KB: large enough to span several
	// streamChunkSize chunks with a partial final chunk, small enough to stay a
	// trivial in-test fixture.
	samplePlaintextSize = 10_000
)

// tenantPrivateKeyHex is a fixed, non-secret X25519 private scalar, checked in
// for determinism (the issue's "fixed test tenant keypair"). It is test-only
// key material — never a real tenant key — so committing it is safe. Any 32
// bytes form a valid X25519 private key; these are the ASCII bytes of
// "fil-474-fee-tenant-recovery-test". The derived public key is
// 69228bafb04870ffe3b19fcdd604a9aa3e12c53b7c0e86301dc38b2a317cf37b.
const tenantPrivateKeyHex = "66696c2d3437342d6665652d74656e616e742d7265636f766572792d74657374"

// tenantKey loads the fixed tenant recipient keypair. The public key is derived
// from the private scalar, so the archived private key is the single source of
// truth — exactly what a real recovery would start from.
func tenantKey(t *testing.T) *ecdh.PrivateKey {
	t.Helper()
	seed, err := hex.DecodeString(tenantPrivateKeyHex)
	require.NoError(t, err)
	priv, err := ecdh.X25519().NewPrivateKey(seed)
	require.NoError(t, err)
	return priv
}

// samplePlaintext returns n deterministic bytes. The period (251, prime) does
// not align to streamChunkSize, so chunk boundaries fall on varying byte values
// rather than a repeating phase.
func samplePlaintext(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// encryptToEnvelope seals plaintext to a single ECDH-ES tenant recipient via the
// composed fee.Encrypt API and returns the complete wire blob
// (envelope || ciphertext).
func encryptToEnvelope(t *testing.T, recipientPub *ecdh.PublicKey, kid, plaintext []byte) []byte {
	t.Helper()
	r, err := fee.Encrypt(
		bytes.NewReader(plaintext),
		[]fee.Recipient{fee.NewECDHESRecipient(kid, recipientPub)},
		fee.WithChunkSize(streamChunkSize),
	)
	require.NoError(t, err)
	// Close immediately (mirroring fee_test.go's encrypt helper): the reader is
	// goroutine-backed, so it must be closed even if io.ReadAll or a later
	// assertion fails, or the background Encrypt goroutine/pipe leaks.
	defer r.Close()
	blob, err := io.ReadAll(r)
	require.NoError(t, err)
	return blob
}

// TestInsuranceRecoveryRoundTrip is the primary acceptance criterion: encrypt a
// plaintext sample to the test tenant key, confirm the envelope's recipient kid
// names that key, then recover with only the archived tenant private key and
// check the plaintext matches the original exactly — all through the composed
// fee API.
func TestInsuranceRecoveryRoundTrip(t *testing.T) {
	tenant := tenantKey(t)
	kid := tenant.PublicKey().Bytes()

	plaintext := samplePlaintext(samplePlaintextSize)
	require.Greater(t, len(plaintext), streamChunkSize,
		"sample must span multiple chunks so the multi-chunk STREAM path runs")

	blob := encryptToEnvelope(t, tenant.PublicKey(), kid, plaintext)

	// Confirm the envelope tags the right key for the right recipient before
	// recovery. With a single recipient the unwrap would still succeed if the
	// kid were wrong or dropped, so this explicit on-wire check is the only
	// thing that binds the recipient entry to the tenant key. fee.Decrypt also
	// matches on kid internally, but the issue calls for asserting it directly.
	env, _, err := cose.Decode(blob, cose.WithExpectedType(fee.EnvelopeType))
	require.NoError(t, err)
	require.Len(t, env.Recipients, 1)
	gotKid, ok := env.Recipients[0].Headers.Unprotected.Bytes(cose.HeaderLabelKID)
	require.True(t, ok, "recipient kid present")
	require.Equal(t, kid, gotKid, "recipient kid matches the tenant key")

	// Recover using only the archived tenant private key.
	pr, err := fee.Decrypt(bytes.NewReader(blob), fee.NewECDHESUnwrapper(kid, tenant))
	require.NoError(t, err)
	recovered, err := io.ReadAll(pr)
	require.NoError(t, err)
	require.Equal(t, plaintext, recovered, "recovered plaintext matches the original exactly")
}

// TestInsuranceRecoveryWrongPrivateKeyFailsBeforeDecrypt is the second
// acceptance criterion: unwrapping with the wrong private key returns an error,
// and decryption is never attempted on the still-encrypted data. The unwrapper
// carries the real tenant kid (so the recipient still matches), isolating the
// wrong-key behaviour to the unwrap step.
func TestInsuranceRecoveryWrongPrivateKeyFailsBeforeDecrypt(t *testing.T) {
	tenant := tenantKey(t)
	kid := tenant.PublicKey().Bytes()
	blob := encryptToEnvelope(t, tenant.PublicKey(), kid, samplePlaintext(samplePlaintextSize))

	// A different archived X25519 key. Deriving it from a distinct fixed scalar
	// keeps the test deterministic.
	wrongSeed := []byte("fil-474-fee-WRONG-tenant-key-xxx")
	require.Len(t, wrongSeed, 32)
	wrong, err := ecdh.X25519().NewPrivateKey(wrongSeed)
	require.NoError(t, err)

	pr, err := fee.Decrypt(bytes.NewReader(blob), fee.NewECDHESUnwrapper(kid, wrong))
	require.Nil(t, pr, "no plaintext reader is produced")
	require.Error(t, err)
	require.ErrorIs(t, err, aeskw.ErrIntegrity, "recovery fails at the ECDH-ES+A256KW unwrap")
	// A STREAM (aesstream) error would mean decryption had been attempted; its
	// absence shows fee.Decrypt stopped at the unwrap, before opening the body.
	require.NotErrorIs(t, err, aesstream.ErrCorrupted, "STREAM decryption is never attempted")
}

// TestInsuranceRecoveryCorruptedProtectedHeaderFailsDecode is the third
// acceptance criterion: flipping a byte in the protected-header bytes makes
// decode return an error, rather than yielding a structure that would decrypt to
// garbage plaintext. fee.Decrypt surfaces the decode failure as a wrapped
// cose.ErrMalformed and produces no plaintext reader.
func TestInsuranceRecoveryCorruptedProtectedHeaderFailsDecode(t *testing.T) {
	tenant := tenantKey(t)
	kid := tenant.PublicKey().Bytes()
	blob := encryptToEnvelope(t, tenant.PublicKey(), kid, samplePlaintext(samplePlaintextSize))

	// Decode once cleanly to locate the protected-header bytes within the blob.
	env, _, err := cose.Decode(blob, cose.WithExpectedType(fee.EnvelopeType))
	require.NoError(t, err)
	require.NotEmpty(t, env.Headers.RawProtected)

	off := bytes.Index(blob, env.Headers.RawProtected)
	require.GreaterOrEqual(t, off, 0, "protected-header bytes located in the blob")

	// Flip the first protected-header byte (the CBOR map head), so the protected
	// bytes are no longer a CBOR map and a strict decode rejects the envelope
	// outright instead of returning a structure to decrypt.
	corrupted := bytes.Clone(blob)
	corrupted[off] ^= 0xFF

	pr, err := fee.Decrypt(bytes.NewReader(corrupted), fee.NewECDHESUnwrapper(kid, tenant))
	require.Nil(t, pr, "no plaintext reader is produced")
	require.Error(t, err)
	require.ErrorIs(t, err, cose.ErrMalformed)
}
