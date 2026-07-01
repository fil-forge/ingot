package testing_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/tests/integration"
	"go.uber.org/zap/zaptest"

	mstesting "github.com/fil-forge/ingot/testing"
)

// blobClient builds an S3 client pointed at the harness — the same
// construction the upstream integration suite uses (path-style, static
// creds).
func blobClient(h *mstesting.Harness) *s3.Client {
	return integration.NewS3Conf(
		integration.WithEndpoint(h.Endpoint),
		integration.WithAccess(h.AccessKey),
		integration.WithSecret(h.SecretKey),
		integration.WithRegion(h.Region),
	).GetClient()
}

func quotedMD5(b []byte) string {
	sum := md5.Sum(b)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// patternBytes returns n deterministic bytes — a cheap way to make a payload
// whose every byte position is verifiable after a round trip. The (i>>16) term
// mixes in the 64 KiB-blob index so each coarse-split blob has distinct content
// (a purely position-linear pattern would make equal-length blobs identical and
// dedup them in the spool).
func patternBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7 + (i>>16)*101)
	}
	return b
}

// TestBlobSplit_MultiBlobRoundTrip drives a PUT of an object several
// times larger than max_blob_size so its body is coarsely split into
// multiple BlobRefs, then verifies the whole-object GET, ranged GETs
// (one spanning a blob boundary, one wholly inside a later blob), and
// the md5 ETag all reconstruct the exact bytes.
func TestBlobSplit_MultiBlobRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	const maxBlob = 64 << 10 // 64 KiB blob ceiling
	h, err := mstesting.StartHarness(ctx,
		mstesting.WithLogger(zaptest.NewLogger(t)),
		mstesting.WithMaxBlobSize(maxBlob),
	)
	if err != nil {
		t.Fatalf("StartHarness: %v", err)
	}
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	cl := blobClient(h)
	const bucket = "blob-split"
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// 3.5 blobs: blob0..blob2 are full (64 KiB), blob3 is a half blob.
	const size = 3*maxBlob + maxBlob/2
	data := patternBytes(size)

	put, err := cl.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("big"),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if want := quotedMD5(data); aws.ToString(put.ETag) != want {
		t.Fatalf("PUT ETag = %s, want %s", aws.ToString(put.ETag), want)
	}

	// The bodies must be spooled to local disk by digest (the data-plane
	// inversion), not journaled into the log. A 3.5-blob object spools 4 files.
	if got := spoolBlobCount(t, h); got != 4 {
		t.Fatalf("spool holds %d blobs, want 4 — bodies must be spooled, not logged", got)
	}

	// Whole-object GET.
	if got := getBody(t, ctx, cl, bucket, "big", ""); !bytes.Equal(got, data) {
		t.Fatalf("whole GET mismatch: got %d bytes, want %d", len(got), len(data))
	}

	// Ranged GET spanning the blob0→blob1 boundary.
	rangeCases := []struct{ start, end int }{
		{maxBlob - 10, maxBlob + 2000}, // crosses a blob boundary
		{maxBlob + 5, maxBlob + 105},   // wholly inside blob1, non-zero in-blob offset
		{size - 1, size - 1},           // final byte only
		{0, 0},                         // first byte only
	}
	for _, rc := range rangeCases {
		hdr := fmt.Sprintf("bytes=%d-%d", rc.start, rc.end)
		got := getBody(t, ctx, cl, bucket, "big", hdr)
		want := data[rc.start : rc.end+1]
		if !bytes.Equal(got, want) {
			t.Fatalf("range %s mismatch: got %d bytes, want %d", hdr, len(got), len(want))
		}
	}

	// HEAD reflects the full size and the same ETag.
	head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("big")})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if aws.ToInt64(head.ContentLength) != size {
		t.Fatalf("HEAD ContentLength = %d, want %d", aws.ToInt64(head.ContentLength), size)
	}
	if want := quotedMD5(data); aws.ToString(head.ETag) != want {
		t.Fatalf("HEAD ETag = %s, want %s", aws.ToString(head.ETag), want)
	}
}

// TestBlobSplit_ZeroByteObject verifies a 0-byte object stores no blob,
// round-trips empty, and carries the well-known empty-content md5 ETag.
func TestBlobSplit_ZeroByteObject(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	h, err := mstesting.StartHarness(ctx, mstesting.WithLogger(zaptest.NewLogger(t)))
	if err != nil {
		t.Fatalf("StartHarness: %v", err)
	}
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	cl := blobClient(h)
	const bucket = "zero-byte"
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	const emptyMD5 = `"d41d8cd98f00b204e9800998ecf8427e"`
	put, err := cl.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("empty"),
		Body:   bytes.NewReader(nil),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if aws.ToString(put.ETag) != emptyMD5 {
		t.Fatalf("PUT ETag = %s, want %s", aws.ToString(put.ETag), emptyMD5)
	}

	if got := getBody(t, ctx, cl, bucket, "empty", ""); len(got) != 0 {
		t.Fatalf("zero-byte GET returned %d bytes, want 0", len(got))
	}

	head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("empty")})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if aws.ToInt64(head.ContentLength) != 0 {
		t.Fatalf("HEAD ContentLength = %d, want 0", aws.ToInt64(head.ContentLength))
	}
	if aws.ToString(head.ETag) != emptyMD5 {
		t.Fatalf("HEAD ETag = %s, want %s", aws.ToString(head.ETag), emptyMD5)
	}
}

// spoolBlobCount counts the body blobs on disk in the harness spool, ignoring
// in-progress temp files. Used to prove object bodies are spooled by digest.
func spoolBlobCount(t *testing.T, h *mstesting.Harness) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(h.DataDir(), "spool"))
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && !strings.HasPrefix(e.Name(), ".tmp") {
			n++
		}
	}
	return n
}

func getBody(t *testing.T, ctx context.Context, cl *s3.Client, bucket, key, rangeHdr string) []byte {
	t.Helper()
	in := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if rangeHdr != "" {
		in.Range = aws.String(rangeHdr)
	}
	out, err := cl.GetObject(ctx, in)
	if err != nil {
		t.Fatalf("GetObject %s (range %q): %v", key, rangeHdr, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read body %s: %v", key, err)
	}
	return b
}
