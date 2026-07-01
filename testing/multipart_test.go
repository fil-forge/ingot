package testing_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap/zaptest"

	mstesting "github.com/fil-forge/ingot/testing"
)

// TestMultipart_RoundTrip drives Create → UploadPart×3 → Complete and verifies
// the assembled object reads back byte-exact, with a multipart "-N" ETag. Parts
// are coarse-split (max_blob_size small) so a part spans multiple body blobs.
func TestMultipart_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	const maxBlob = 32 << 10
	h, err := mstesting.StartHarness(ctx,
		mstesting.WithLogger(zaptest.NewLogger(t)),
		mstesting.WithMaxBlobSize(maxBlob),
	)
	if err != nil {
		t.Fatalf("StartHarness: %v", err)
	}
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	cl := blobClient(h)
	const bucket, key = "mpbucket", "big/obj"
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), ContentType: aws.String("text/plain"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := create.UploadId

	// Three parts of varying sizes; part 1 spans several blobs.
	partData := [][]byte{patternBytes(70 << 10), patternBytes(40 << 10), patternBytes(9 << 10)}
	var completed []types.CompletedPart
	var whole []byte
	for i, data := range partData {
		pn := int32(i + 1)
		up, err := cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID,
			PartNumber: aws.Int32(pn), Body: bytes.NewReader(data),
		})
		if err != nil {
			t.Fatalf("UploadPart %d: %v", pn, err)
		}
		completed = append(completed, types.CompletedPart{PartNumber: aws.Int32(pn), ETag: up.ETag})
		whole = append(whole, data...)
	}

	comp, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if et := strings.Trim(aws.ToString(comp.ETag), `"`); !strings.HasSuffix(et, "-3") {
		t.Fatalf("complete ETag = %q, want a multipart -3 suffix", et)
	}

	// The assembled object reads back byte-exact, and HEAD reports the full size.
	if got := getBody(t, ctx, cl, bucket, key, ""); !bytes.Equal(got, whole) {
		t.Fatalf("multipart GET mismatch: got %d bytes, want %d", len(got), len(whole))
	}
	head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if aws.ToInt64(head.ContentLength) != int64(len(whole)) {
		t.Fatalf("HEAD size = %d, want %d", aws.ToInt64(head.ContentLength), len(whole))
	}
	// A ranged GET spanning the part-1→part-2 boundary still reconstructs.
	r := getBody(t, ctx, cl, bucket, key, "bytes=65536-90000")
	if !bytes.Equal(r, whole[65536:90001]) {
		t.Fatalf("ranged multipart GET mismatch: got %d bytes", len(r))
	}
}

// TestMultipart_FailedCompleteStaysAbortable is the regression for the
// zombie-session bug: a Complete that fails validation (wrong part ETag) must
// leave the session abortable (and retriable), not stuck in 'completing'.
func TestMultipart_FailedCompleteStaysAbortable(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	h, err := mstesting.StartHarness(ctx, mstesting.WithLogger(zaptest.NewLogger(t)))
	if err != nil {
		t.Fatalf("StartHarness: %v", err)
	}
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	cl := blobClient(h)
	const bucket, key = "mpretry", "obj"
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	data := patternBytes(8 << 10)
	up, err := cl.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		PartNumber: aws.Int32(1), Body: bytes.NewReader(data),
	})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}

	// Complete with a WRONG part ETag → InvalidPart. The session must not zombie.
	if _, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{PartNumber: aws.Int32(1), ETag: aws.String("\"deadbeefdeadbeefdeadbeefdeadbeef\"")},
		}},
	}); err == nil {
		t.Fatal("Complete with a wrong part ETag: want an error, got nil")
	}

	// A retry with the correct ETag now succeeds (session was reverted to open).
	if _, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{PartNumber: aws.Int32(1), ETag: up.ETag},
		}},
	}); err != nil {
		t.Fatalf("Complete retry after a failed Complete: %v", err)
	}
	if got := getBody(t, ctx, cl, bucket, key, ""); !bytes.Equal(got, data) {
		t.Fatalf("retried multipart object mismatch: %d bytes", len(got))
	}
}

// TestMultipart_Abort verifies an aborted upload leaves no object and the
// upload id becomes unusable.
func TestMultipart_Abort(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	h, err := mstesting.StartHarness(ctx, mstesting.WithLogger(zaptest.NewLogger(t)))
	if err != nil {
		t.Fatalf("StartHarness: %v", err)
	}
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	cl := blobClient(h)
	const bucket, key = "mpabort", "obj"
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if _, err := cl.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		PartNumber: aws.Int32(1), Body: bytes.NewReader(patternBytes(4 << 10)),
	}); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	if _, err := cl.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
	}); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	// The object was never created.
	if _, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err == nil {
		t.Fatalf("HeadObject after abort: want an error (no object), got nil")
	}
	// The upload id is gone — a second abort fails with NoSuchUpload.
	if _, err := cl.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
	}); err == nil {
		t.Fatalf("second AbortMultipartUpload: want NoSuchUpload, got nil")
	}
}
