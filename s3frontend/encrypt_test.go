package s3frontend

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/filecoin-project/go-fee"
	"github.com/filecoin-project/go-fee/cose"

	"github.com/fil-forge/ingot/tenantkey"
)

// These tests drive the full write path (PutObject → SplitBody →
// encryptingBlobWriter → spool + registry rows) through the public S3
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
	if err != nil {
		t.Fatalf("GetObject %s %q: %v", key, rng, err)
	}
	defer out.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(out.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
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
	if got := len(blobDigestsOf(t, b, "k1", "")); got != 3 {
		t.Fatalf("blobs = %d, want 3", got)
	}

	if got := getRange(t, b, "k1", ""); !bytes.Equal(got, data) {
		t.Fatalf("whole GET differs from plaintext (%d vs %d bytes)", len(got), len(data))
	}
	for _, span := range [][2]int64{
		{0, 15},                                       // head of the first blob
		{blobCeiling - 8, blobCeiling + 7},            // across a blob boundary
		{(256 << 10) - 5, (256 << 10) + 4},            // across a STREAM chunk boundary
		{int64(len(data)) - 16, int64(len(data)) - 1}, // tail
	} {
		rng := fmt.Sprintf("bytes=%d-%d", span[0], span[1])
		if got := getRange(t, b, "k1", rng); !bytes.Equal(got, data[span[0]:span[1]+1]) {
			t.Fatalf("ranged GET %s differs from plaintext", rng)
		}
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
	if want := `"` + hex.EncodeToString(md5sum(data)) + `"`; out.ETag != want {
		t.Fatalf("ETag = %q, want the plaintext md5 %q", out.ETag, want)
	}

	d := blobDigestOf(t, b, "k1", "")
	if bytes.Equal(d, digestOf(t, data)) {
		t.Fatalf("stored digest equals hash(plaintext); blob was not encrypted")
	}
	stored, err := os.ReadFile(b.spool.Path(d))
	if err != nil {
		t.Fatalf("read spooled blob: %v", err)
	}
	if len(stored) <= len(data) {
		t.Fatalf("stored %d bytes for %d plaintext; an envelope must be larger", len(stored), len(data))
	}
	if bytes.Contains(stored, data[:32]) {
		t.Fatalf("stored blob contains plaintext bytes")
	}

	params, err := mem.GetEncryptionParams(ctx, did.Undef, d)
	if err != nil {
		t.Fatalf("GetEncryptionParams: %v", err)
	}
	if len(params.RegionWrappedCEK) == 0 || params.HeaderLen <= 0 || params.ChunkSize <= 0 {
		t.Fatalf("params row incomplete: %+v", params)
	}
	// The registry records the STORED size: what decryption geometry is
	// derived from, and what gets uploaded.
	loc, err := mem.GetLocation(ctx, did.Undef, d)
	if err != nil {
		t.Fatalf("GetLocation: %v", err)
	}
	if loc.Size != int64(len(stored)) {
		t.Fatalf("recorded location size = %d, want the stored %d", loc.Size, len(stored))
	}
}

// TestEncryptedWrite_ZeroByteObject: an empty PUT stores nothing — no blobs,
// no envelope for the empty stream — and reads back empty. (An encrypted
// empty stream would not be empty on the wire; the writer's EOF probe keeps
// the zero-byte contract.)
func TestEncryptedWrite_ZeroByteObject(t *testing.T) {
	b, _, _ := newRefTestBackend(t)

	out := putObjV(t, b, "k1", nil)
	if want := `"` + hex.EncodeToString(md5sum(nil)) + `"`; out.ETag != want {
		t.Fatalf("ETag = %q, want the empty md5 %q", out.ETag, want)
	}
	if got := len(blobDigestsOf(t, b, "k1", "")); got != 0 {
		t.Fatalf("blobs = %d, want 0", got)
	}
	if got := getRange(t, b, "k1", ""); len(got) != 0 {
		t.Fatalf("GET returned %d bytes, want 0", len(got))
	}
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
	if _, err := mem.GetEncryptionParams(ctx, did.Undef, d); err != nil {
		t.Fatalf("params row missing before delete: %v", err)
	}

	deleteObj(t, b, "k1")
	drainReleases(t, b) // the release is deferred; the sweep executes it
	if _, err := mem.GetEncryptionParams(ctx, did.Undef, d); err == nil {
		t.Fatalf("params row survived the last-claim release; want it crypto-shredded")
	}
	if rm.removedDigests()[string(d)] != 1 {
		t.Fatalf("expected one RemoveBlob; got %v", rm.removedDigests())
	}
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

// recipientOf decodes a spooled envelope and returns its single COSE
// recipient, failing if the envelope is not a one-recipient COSE_Encrypt.
func recipientOf(t *testing.T, stored []byte) *cose.Recipient {
	t.Helper()
	tag, err := cose.PeekTag(stored)
	if err != nil {
		t.Fatalf("PeekTag: %v", err)
	}
	if tag != cose.TagCOSEEncrypt {
		t.Fatalf("envelope tag = %d, want %d (COSE_Encrypt with recipients)", tag, cose.TagCOSEEncrypt)
	}
	env, _, err := cose.Decode(stored)
	if err != nil {
		t.Fatalf("cose.Decode: %v", err)
	}
	if len(env.Recipients) != 1 {
		t.Fatalf("recipients = %d, want exactly the tenant", len(env.Recipients))
	}
	return env.Recipients[0]
}

// assertTenantRecipient checks the envelope stored for digest carries the
// tenant recipient — kid = the wrap key's fingerprint — and that the tenant's
// private key alone recovers the plaintext (the encryption RFC's
// recoverability criterion: no region, no database).
func assertTenantRecipient(t *testing.T, b *Backend, digest []byte, plaintext []byte) {
	t.Helper()
	stored, err := os.ReadFile(b.spool.Path(digest))
	if err != nil {
		t.Fatalf("read spooled blob: %v", err)
	}
	kid := tenantkey.EncodePublicKey(testWrapKey.PublicKey())
	rec := recipientOf(t, stored)
	if got, ok := rec.Headers.Unprotected.Bytes(cose.HeaderLabelKID); !ok || string(got) != kid {
		t.Fatalf("recipient kid = %q, want the wrap key fingerprint %q", got, kid)
	}

	pr, err := fee.Decrypt(bytes.NewReader(stored), fee.NewECDHESUnwrapper([]byte(kid), testWrapKey))
	if err != nil {
		t.Fatalf("fee.Decrypt with the tenant key: %v", err)
	}
	recovered, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("read recovered plaintext: %v", err)
	}
	if !bytes.Equal(recovered, plaintext) {
		t.Fatalf("tenant-key recovery returned %d bytes that differ from the %d plaintext bytes", len(recovered), len(plaintext))
	}

	// The kid is an exact match: another key's fingerprint unwraps nothing.
	other, _ := ecdh.X25519().GenerateKey(rand.Reader)
	if _, err := fee.Decrypt(bytes.NewReader(stored), fee.NewECDHESUnwrapper([]byte(tenantkey.EncodePublicKey(other.PublicKey())), other)); err == nil {
		t.Fatalf("an unrelated key unwrapped the envelope")
	}
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
	if len(digests) != 3 {
		t.Fatalf("blobs = %d, want 3", len(digests))
	}
	for i, d := range digests {
		lo := i * blobCeiling
		hi := min(lo+blobCeiling, len(data))
		assertTenantRecipient(t, b, d, data[lo:hi])
	}
	// The read path is indifferent to the recipient: it decrypts from the
	// region wrap and the stored geometry.
	if got := getRange(t, b, "k1", ""); !bytes.Equal(got, data) {
		t.Fatalf("GET differs from plaintext")
	}
}

// TestEncryptedWrite_MultipartTenantRecipient: part blobs carry the tenant
// recipient too (UploadPart shares splitSpool with PutObject).
func TestEncryptedWrite_MultipartTenantRecipient(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	key := "mp-recipient"
	part := testBody(int(backend.MinPartSize))

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, part, nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	assertTenantRecipient(t, b, blobDigestOf(t, b, key, ""), part)
}

// failingTenantKeys is a tenantkey.Source that never yields a key.
type failingTenantKeys struct{ err error }

func (f failingTenantKeys) WrapKey(context.Context) (string, *ecdh.PublicKey, error) {
	return "", nil, f.err
}

// TestEncryptedWrite_FailsClosedWithoutRecipient: a write whose tenant wrap
// key cannot be obtained is refused before anything is spooled or recorded —
// there is no region-only fallback.
func TestEncryptedWrite_FailsClosedWithoutRecipient(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	b.tenantKeys = failingTenantKeys{err: tenantkey.ErrNoTenant}
	data := testBody(4 << 10)

	bucket, key := "bk", "k1"
	if _, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(data),
	}); !errors.Is(err, tenantkey.ErrNoTenant) {
		t.Fatalf("PutObject err = %v, want ErrNoTenant", err)
	}
	if _, _, err := getObjV(t, b, key, ""); err == nil {
		t.Fatalf("object exists after a refused write")
	}
	entries, err := os.ReadDir(b.spool.Path(nil)) // Path of the empty digest is the spool dir itself
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("spool holds %d files after a refused write, want 0", len(entries))
	}

	uploadID := mpCreate(t, b, "mp", "", "")
	if _, err := mpUploadPart(t, b, "mp", uploadID, 1, data, nil); !errors.Is(err, tenantkey.ErrNoTenant) {
		t.Fatalf("UploadPart err = %v, want ErrNoTenant", err)
	}
}
