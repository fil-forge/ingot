package s3frontend

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/ucantone/did"
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
