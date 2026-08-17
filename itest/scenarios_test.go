//go:build itest

package itest

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestForgeScenarios covers ingot-unique behaviors the upstream versitygw
// suite cannot assert — internal blob-plane properties and session-state
// recovery — against a forge-mode stack whose ingot config lowers
// max_blob_size to 64 KiB (testdata/config-smallblob.yaml) so small objects
// coarse-split into multiple body blobs. One stack is shared by all
// subtests; spool assertions are delta-based so subtest order doesn't
// matter.
//
// These were ported from the old in-process suite; assertions that merely
// duplicated upstream versitygw coverage (e.g. abort semantics) were dropped
// in the move.
func TestForgeScenarios(t *testing.T) {
	const maxBlob = 64 << 10 // must match testdata/config-smallblob.yaml

	s, endpoint := forgeStack(t, withSmallBlobConfig())
	ctx := t.Context()
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "scenarios")
	cl := sdkClient(forgeS3Conf(endpoint, accessKey, secretKey))

	// BlobSplitMultiBlobRoundTrip: a PUT several times larger than
	// max_blob_size is coarsely split into multiple BlobRefs; the
	// whole-object GET, boundary-spanning ranged GETs, and md5 ETag all
	// reconstruct the exact bytes, and the bodies land in the spool by
	// digest (the data-plane inversion) — not journaled into the log.
	t.Run("BlobSplitMultiBlobRoundTrip", func(t *testing.T) {
		const bucket = "blob-split"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}

		// 3.5 blobs: blob0..blob2 are full (64 KiB), blob3 is a half blob.
		const size = 3*maxBlob + maxBlob/2
		data := patternBytes(size)

		spoolBefore := spoolBlobCount(t, ctx, s)
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
		if got := spoolBlobCount(t, ctx, s) - spoolBefore; got != 4 {
			t.Fatalf("PUT added %d spool blobs, want 4 — bodies must be spooled by digest, not logged", got)
		}

		if got := getBody(t, ctx, cl, bucket, "big", ""); !bytes.Equal(got, data) {
			t.Fatalf("whole GET mismatch: got %d bytes, want %d", len(got), len(data))
		}

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
	})

	// ZeroByteObject: a 0-byte object stores no blob, round-trips empty,
	// and carries the well-known empty-content md5 ETag.
	t.Run("ZeroByteObject", func(t *testing.T) {
		const bucket = "zero-byte"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}

		const emptyMD5 = `"d41d8cd98f00b204e9800998ecf8427e"`
		spoolBefore := spoolBlobCount(t, ctx, s)
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
		if got := spoolBlobCount(t, ctx, s) - spoolBefore; got != 0 {
			t.Fatalf("zero-byte PUT added %d spool blobs, want 0", got)
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
	})

	// MultipartPartSpansBlobs: Create → UploadPart×3 → Complete where part 1
	// is larger than max_blob_size, so a single part coarse-splits into
	// multiple internal body blobs. Upstream can assert multipart round-trip
	// but has no notion of ingot's internal split; the boundary-spanning
	// ranged GET proves reassembly across both part and blob boundaries.
	t.Run("MultipartPartSpansBlobs", func(t *testing.T) {
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

		// Non-final parts must meet S3's 5 MiB minimum (enforced at Complete);
		// at 64 KiB max_blob_size each spans dozens of internal blobs. The
		// final part is small (exempt from the minimum).
		partData := [][]byte{patternBytes(6 << 20), patternBytes(5 << 20), patternBytes(9 << 10)}
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
		p1 := len(partData[0])
		hdr := fmt.Sprintf("bytes=%d-%d", p1-10, p1+2000)
		if got := getBody(t, ctx, cl, bucket, key, hdr); !bytes.Equal(got, whole[p1-10:p1+2001]) {
			t.Fatalf("ranged multipart GET across the part boundary mismatch: got %d bytes", len(got))
		}
	})

	// MultipartAbortCleansSpool: aborting an upload discards its parts — the
	// registry rows go (upstream AbortMultipartUpload_success verifies via
	// ListMultipartUploads) and, ingot-specifically, the parts' spooled blobs
	// are deleted, since under the spool model an abort's cleanup is entirely
	// local (nothing shipped to the network before Complete).
	t.Run("MultipartAbortCleansSpool", func(t *testing.T) {
		const bucket, key = "mpabort-spool", "obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		spoolBefore := spoolBlobCount(t, ctx, s)
		// A 150 KiB part spans three 64 KiB blobs. The content must be unique
		// to this test — patternBytes at the same offsets would dedup against
		// the round-trip subtests' already-spooled blobs and skew the counts.
		part := patternBytes(150 << 10)
		for i := range part {
			part[i] ^= 0xA5
		}
		if _, err := cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(1), Body: bytes.NewReader(part),
		}); err != nil {
			t.Fatalf("UploadPart: %v", err)
		}
		if got := spoolBlobCount(t, ctx, s) - spoolBefore; got != 3 {
			t.Fatalf("UploadPart added %d spool blobs, want 3", got)
		}
		if _, err := cl.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		}); err != nil {
			t.Fatalf("AbortMultipartUpload: %v", err)
		}
		if got := spoolBlobCount(t, ctx, s) - spoolBefore; got != 0 {
			t.Fatalf("abort left %d spooled part blobs behind, want 0", got)
		}
	})

	// MultipartFailedCompleteStaysAbortable: the zombie-session regression. A
	// Complete that fails validation (wrong part ETag) must leave the session
	// open — a retry with the correct ETag succeeds and reads back
	// byte-exact. Upstream tests re-complete-after-success
	// (already_completed) but not recovery from a FAILED complete; the
	// session-state revert out of 'completing' is ingot catalog behavior.
	t.Run("MultipartFailedCompleteStaysAbortable", func(t *testing.T) {
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
	})

	// CORS: the gateway answers browser CORS from cors_allowed_origins
	// (testdata/config-smallblob.yaml) — a document s3frontend reports as
	// every bucket's CORS configuration, which is what drives versitygw's
	// preflight route and per-route CORS middleware. Asserted on the wire
	// because none of that behavior lives in ingot code.
	t.Run("CORS", func(t *testing.T) {
		const (
			bucket   = "cors"
			key      = "obj"
			origin   = "https://feature-1.dev.example" // matches https://*.dev.example
			maxAge   = "600"
			disallow = "https://evil.example"
		)
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		data := []byte("cors body")
		if _, err := cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
		}); err != nil {
			t.Fatalf("PutObject: %v", err)
		}

		// Preflight. Browsers send these unsigned, so this also pins that
		// the OPTIONS route sits ahead of SigV4.
		preflight := func(t *testing.T, origin string) *http.Response {
			t.Helper()
			req, err := http.NewRequestWithContext(ctx, http.MethodOptions, fmt.Sprintf("%s/%s/%s", endpoint, bucket, key), nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Origin", origin)
			req.Header.Set("Access-Control-Request-Method", "PUT")
			req.Header.Set("Access-Control-Request-Headers", "authorization, x-amz-content-sha256, x-amz-date")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("preflight: %v", err)
			}
			t.Cleanup(func() { _ = resp.Body.Close() })
			return resp
		}

		resp := preflight(t, origin)
		// CORS preflight spec allows any "ok status" (200-299).
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			t.Errorf("preflight status = %d, want 2xx", resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != origin {
			t.Errorf("preflight Allow-Origin = %q, want the request origin echoed", got)
		}
		if got := resp.Header.Get("Access-Control-Allow-Methods"); !strings.Contains(got, "PUT") {
			t.Errorf("preflight Allow-Methods = %q, want it to include PUT", got)
		}
		if got := resp.Header.Get("Access-Control-Max-Age"); got != maxAge {
			t.Errorf("preflight Max-Age = %q, want %s", got, maxAge)
		}

		if got := preflight(t, disallow).Header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("preflight Allow-Origin = %q for a disallowed origin, want unset", got)
		}

		// A real cross-origin read: a presigned GET is how a browser fetches
		// an object, and the response must expose ETag to JavaScript.
		presigned, err := s3.NewPresignClient(cl).PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("PresignGetObject: %v", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, presigned.URL, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Origin", origin)
		get, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("presigned GET: %v", err)
		}
		defer func() { _ = get.Body.Close() }()
		if get.StatusCode != http.StatusOK {
			t.Fatalf("presigned GET status = %d, want 200", get.StatusCode)
		}
		if got := get.Header.Get("Access-Control-Allow-Origin"); got != origin {
			t.Errorf("GET Allow-Origin = %q, want the request origin echoed", got)
		}
		if got := get.Header.Get("Access-Control-Expose-Headers"); !strings.Contains(got, "ETag") {
			t.Errorf("GET Expose-Headers = %q, want it to include ETag", got)
		}
	})
}
