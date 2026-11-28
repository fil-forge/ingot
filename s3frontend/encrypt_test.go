package s3frontend

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/filecoin-project/go-fee"
	"github.com/filecoin-project/go-fee/cose"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/tenantkey"
)

// These tests drive the full write path (PutObject → SplitBody →
// encryptingBlobWriter → stream + registry rows) through the public S3
// surface and check the encryption RFC's two observable criteria:
// correctness (every read returns the plaintext bytes) and opacity (what
// lands in storage shares nothing with the plaintext).

// testBody is deliberately recognizable: any 32-byte window of it appearing
// in a stored blob would prove plaintext leaked to storage.
func testBody(n int) []byte {
	const pangram = "The quick brown fox jumps over the lazy dog. "
	data := make([]byte, n)
	for i := range data {
		data[i] = pangram[i%len(pangram)]
	}
	return data
}

func getRange(t *testing.T, b *Backend, key, rng string) []byte {
	t.Helper()
	bucket := "bk"
	in := &s3.GetObjectInput{Bucket: &bucket, Key: &key}
	if rng != "" {
		in.Range = &rng
	}
	out, err := b.GetObject(context.Background(), in)
	require.NoError(t, err, "GetObject %s %q", key, rng)
	defer out.Body.Close()
	var buf bytes.Buffer
	_, err = buf.ReadFrom(out.Body)
	require.NoError(t, err, "read body")
	return buf.Bytes()
}

// TestEncryptedWrite_RoundTrip: whole and ranged GETs of a multi-blob object
// return the plaintext byte-for-byte — the correctness criterion, across
// blob and STREAM-chunk boundaries.
func TestEncryptedWrite_RoundTrip(t *testing.T) {
	const blobCeiling = 300 << 10
	b, _, _ := newRefTestBackend(t, blobCeiling)
	data := testBody(700 << 10) // → 3 blobs of ≤300 KiB plaintext

	putObj(t, b, "k1", data)
	got := len(blobDigestsOf(t, b, "k1", ""))
	require.Equal(t, 3, got, "blobs")

	body := getRange(t, b, "k1", "")
	require.True(t, bytes.Equal(data, body), "whole GET differs from plaintext (%d vs %d bytes)", len(body), len(data))
	for _, span := range [][2]int64{
		{0, 15},                                       // head of the first blob
		{blobCeiling - 8, blobCeiling + 7},            // across a blob boundary
		{(256 << 10) - 5, (256 << 10) + 4},            // across a STREAM chunk boundary
		{int64(len(data)) - 16, int64(len(data)) - 1}, // tail
	} {
		rng := fmt.Sprintf("bytes=%d-%d", span[0], span[1])
		got := getRange(t, b, "k1", rng)
		require.Equal(t, data[span[0]:span[1]+1], got, "ranged GET %s differs from plaintext", rng)
	}
}

// TestEncryptedWrite_Opacity: every stored blob is a FEE envelope — larger
// than its plaintext, named by a digest that is not hash(plaintext), sharing
// no window with the plaintext, and carrying a blob_encryption_params row.
// The object's ETag stays the plaintext md5.
func TestEncryptedWrite_Opacity(t *testing.T) {
	ctx := context.Background()
	b, mem, _ := newRefTestBackend(t)
	data := testBody(4 << 10)

	out := putObjV(t, b, "k1", data)
	want := `"` + hex.EncodeToString(md5sum(data)) + `"`
	require.Equal(t, want, out.ETag, "ETag")

	d := blobDigestOf(t, b, "k1", "")
	require.False(t, bytes.Equal(d, digestOf(t, data)), "stored digest equals hash(plaintext); blob was not encrypted")
	stored, err := os.ReadFile(localPath(b, d))
	require.NoError(t, err, "read stored blob")
	require.Greater(t, len(stored), len(data), "stored %d bytes for %d plaintext; an envelope must be larger", len(stored), len(data))
	require.False(t, bytes.Contains(stored, data[:32]), "stored blob contains plaintext bytes")

	params, err := mem.GetEncryptionParams(ctx, mem.bk(), d)
	require.NoError(t, err, "GetEncryptionParams")
	require.NotEmpty(t, params.RegionWrappedCEK, "params row incomplete: %+v", params)
	require.Greater(t, params.HeaderLen, int64(0), "params row incomplete: %+v", params)
	require.NoError(t, (fee.BodyDescriptor{HeaderLen: params.HeaderLen, AAD: params.AAD}).Validate(), "params row does not describe a FEE body")
	// The registry records the STORED size: what decryption geometry is
	// derived from, and what gets uploaded.
	loc, err := mem.GetLocation(ctx, mem.bk(), d)
	require.NoError(t, err, "GetLocation")
	require.Equal(t, int64(len(stored)), loc.Size, "recorded location size")
}

// TestEncryptedWrite_ZeroByteObject: an empty PUT stores nothing — no blobs,
// no envelope for the empty stream — and reads back empty. (An encrypted
// empty stream would not be empty on the wire; the writer's EOF probe keeps
// the zero-byte contract.)
func TestEncryptedWrite_ZeroByteObject(t *testing.T) {
	b, _, _ := newRefTestBackend(t)

	out := putObjV(t, b, "k1", nil)
	want := `"` + hex.EncodeToString(md5sum(nil)) + `"`
	require.Equal(t, want, out.ETag, "ETag")
	require.Empty(t, blobDigestsOf(t, b, "k1", ""), "blobs")
	require.Empty(t, getRange(t, b, "k1", ""), "GET body")
}

// TestEncryptedWrite_DeleteShredsParams: releasing a blob's last claim
// deletes its blob_encryption_params row — the crypto-shred that renders the
// ciphertext permanently unreadable even where copies survive.
func TestEncryptedWrite_DeleteShredsParams(t *testing.T) {
	ctx := context.Background()
	b, mem, rm := newRefTestBackend(t)
	data := testBody(1 << 10)

	putObj(t, b, "k1", data)
	d := blobDigestOf(t, b, "k1", "")
	_, err := mem.GetEncryptionParams(ctx, mem.bk(), d)
	require.NoError(t, err, "params row missing before delete")

	deleteObj(t, b, "k1")
	drainReleases(t, b) // the release is deferred; the sweep executes it
	_, err = mem.GetEncryptionParams(ctx, mem.bk(), d)
	require.Error(t, err, "params row survived the last-claim release; want it crypto-shredded")
	removed := rm.removedDigests()
	require.Equal(t, 1, removed[string(d)], "RemoveBlob calls for the blob (all: %v)", removed)
}

func md5sum(data []byte) []byte {
	s := md5.Sum(data)
	return s[:]
}

// testWrapKey is the tenant wrap keypair every write in the s3frontend tests
// encrypts to; its private half is what the recovery assertions unwrap with.
var testWrapKey = func() *ecdh.PrivateKey {
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return priv
}()

// testTenantKeys is the tenantkey.Source the test backends use: a static
// source over testWrapKey's public half.
func testTenantKeys() tenantkey.Source {
	return tenantkey.NewStatic(testWrapKey.PublicKey())
}

// recipientOf decodes a stored envelope and returns its single COSE
// recipient, failing if the envelope is not a one-recipient COSE_Encrypt.
func recipientOf(t *testing.T, stored []byte) *cose.Recipient {
	t.Helper()
	tag, err := cose.PeekTag(stored)
	require.NoError(t, err, "PeekTag")
	require.Equal(t, cose.TagCOSEEncrypt, tag, "envelope tag")
	env, _, err := cose.Decode(stored)
	require.NoError(t, err, "cose.Decode")
	require.Len(t, env.Recipients, 1, "recipients")
	return env.Recipients[0]
}

// assertTenantRecipient checks the envelope stored for digest carries the
// tenant recipient — kid = the wrap key's fingerprint — and that the tenant's
// private key alone recovers the plaintext (the encryption RFC's
// recoverability criterion: no region, no database).
func assertTenantRecipient(t *testing.T, b *Backend, digest []byte, plaintext []byte) {
	t.Helper()
	stored := storedBytes(t, b, digest)
	kid := tenantkey.EncodePublicKey(testWrapKey.PublicKey())
	rec := recipientOf(t, stored)
	got, ok := rec.Headers.Unprotected.Bytes(cose.HeaderLabelKID)
	require.True(t, ok, "recipient kid = %q, want the wrap key fingerprint %q", got, kid)
	require.Equal(t, kid, string(got), "recipient kid = %q, want the wrap key fingerprint %q", got, kid)

	pr, err := fee.Decrypt(bytes.NewReader(stored), fee.NewECDHESUnwrapper([]byte(kid), testWrapKey))
	require.NoError(t, err, "fee.Decrypt with the tenant key")
	recovered, err := io.ReadAll(pr)
	require.NoError(t, err, "read recovered plaintext")
	require.Equal(t, plaintext, recovered, "tenant-key recovery returned %d bytes that differ from the %d plaintext bytes", len(recovered), len(plaintext))

	// The kid is an exact match: another key's fingerprint unwraps nothing.
	other, _ := ecdh.X25519().GenerateKey(rand.Reader)
	_, err = fee.Decrypt(bytes.NewReader(stored), fee.NewECDHESUnwrapper([]byte(tenantkey.EncodePublicKey(other.PublicKey())), other))
	require.Error(t, err, "an unrelated key unwrapped the envelope")
}

// TestEncryptedWrite_TenantRecipient: every blob a PUT stores is a
// COSE_Encrypt whose one recipient is the tenant wrap key, recoverable with
// the tenant's private key alone.
func TestEncryptedWrite_TenantRecipient(t *testing.T) {
	const blobCeiling = 64 << 10
	b, _, _ := newRefTestBackend(t, blobCeiling)
	data := testBody(150 << 10) // 3 blobs

	putObj(t, b, "k1", data)
	digests := blobDigestsOf(t, b, "k1", "")
	require.Len(t, digests, 3, "blobs")
	for i, d := range digests {
		lo := i * blobCeiling
		hi := min(lo+blobCeiling, len(data))
		assertTenantRecipient(t, b, d, data[lo:hi])
	}
	// The read path is indifferent to the recipient: it decrypts from the
	// region wrap and the stored geometry.
	got := getRange(t, b, "k1", "")
	require.Equal(t, data, got, "GET differs from plaintext")
}

// TestEncryptedWrite_MultipartTenantRecipient: part blobs carry the tenant
// recipient too (UploadPart shares splitBody with PutObject).
func TestEncryptedWrite_MultipartTenantRecipient(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	key := "mp-recipient"
	part := testBody(int(backend.MinPartSize))

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, part, nil)
	require.NoError(t, err, "UploadPart")
	one := int32(1)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil)
	require.NoError(t, err, "Complete")
	assertTenantRecipient(t, b, blobDigestOf(t, b, key, ""), part)
}

// failingTenantKeys is a tenantkey.Source that never yields a key.
type failingTenantKeys struct{ err error }

func (f failingTenantKeys) WrapKey(context.Context) (string, *ecdh.PublicKey, error) {
	return "", nil, f.err
}

// TestEncryptedWrite_FailsClosedWithoutRecipient: a write whose tenant wrap
// key cannot be obtained is refused before anything is stored or recorded —
// there is no region-only fallback.
func TestEncryptedWrite_FailsClosedWithoutRecipient(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	b.tenantKeys = failingTenantKeys{err: tenantkey.ErrNoTenant}
	data := testBody(4 << 10)

	bucket, key := "bk", "k1"
	_, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data),
	})
	require.ErrorIs(t, err, tenantkey.ErrNoTenant, "PutObject")
	_, _, err = getObjV(t, b, key, "")
	require.Error(t, err, "object exists after a refused write")
	entries, err := os.ReadDir(b.cache.Path(nil)) // Path of the empty digest is the cache dir itself
	require.NoError(t, err, "read cache dir")
	require.Empty(t, entries, "cache holds %d files after a refused write", len(entries))

	uploadID := mpCreate(t, b, "mp", "", "")
	_, err = mpUploadPart(t, b, "mp", uploadID, 1, data, nil)
	require.ErrorIs(t, err, tenantkey.ErrNoTenant, "UploadPart")
}
