//go:build itest

package itest

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	msbucket "github.com/fil-forge/ingot/bucket"
)

// TestForgeEncryption is the end-to-end encryption suite: the
// encryption-specific behaviors the round-trip and conformance tests don't
// pin, against a forge-mode stack under the DEFAULT config (~254 MiB
// max_blob_size, aesstream's 256 KiB chunks) — the tamper subtests depend on
// a blob holding several chunks, which the smallblob config collapses.
//
// The skipped subtests at the bottom are suite assertions blocked on
// unbuilt features; each skip names the missing capability. Related
// coverage lives elsewhere: round trips and cross-part range GETs in
// TestForgeScenarios and the versity tables, abort blob-cleanup in
// TestForgeScenarios and TestForgeDeferredMultipart, the 5 GiB max part in
// TestForgeMaxSizePart.
func TestForgeEncryption(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t)
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "encryption")
	cl := sdkClient(forgeS3Conf(endpoint, accessKey, secretKey))

	// Tamper setup, shared by the two tamper subtests: PUT a 1 MiB object
	// (one blob, four 256 KiB chunks) and corrupt its spooled envelope's
	// tail — inside the FINAL chunk, so earlier chunks stay intact. The
	// spool is the first read tier and does no digest re-verification, so
	// every subsequent GET reads the tampered ciphertext and only the GCM
	// tag stands between it and the client. (Never wipe the spool here the
	// way the eviction tests do: piri's pristine copy would serve the read.)
	const (
		tamperBucket = "tamper"
		tamperKey    = "obj"
		chunkSize    = 256 << 10
		tamperSize   = 4 * chunkSize
	)
	tamperData := patternBytes(tamperSize)
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(tamperBucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	spoolBefore := spoolBlobPaths(t, ctx, s)
	if _, err := cl.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(tamperBucket), Key: aws.String(tamperKey), Body: bytes.NewReader(tamperData),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	added := newSpoolPaths(spoolBefore, spoolBlobPaths(t, ctx, s))
	if len(added) != 1 {
		t.Fatalf("PUT spooled %d envelopes, want 1 (a 1 MiB object is a single blob under the default config)", len(added))
	}
	corruptSpoolFileTail(t, ctx, s, added[0], 100)

	// A tampered chunk must never reach the client as plaintext. Decryption
	// streams after the 200 and Content-Length are already written, so the
	// contract is a mid-body failure (read error or short body), not an S3
	// error code — asserting "full-length body with no error" is the
	// vulnerability this test exists to catch.
	t.Run("TamperedCiphertextWholeGET", func(t *testing.T) {
		out, err := cl.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(tamperBucket), Key: aws.String(tamperKey),
		})
		if err != nil {
			// The opener reads catalog rows, not the envelope, so the
			// request itself normally succeeds — but a pre-body rejection
			// is still a rejection.
			t.Logf("GetObject rejected before streaming: %v", err)
			return
		}
		defer out.Body.Close()
		got, readErr := io.ReadAll(out.Body)
		if readErr == nil && len(got) == tamperSize {
			t.Fatalf("whole GET of a tampered object returned all %d bytes with no error — corrupt plaintext served silently", len(got))
		}
		t.Logf("tampered whole GET failed mid-stream as required: read %d/%d bytes, err=%v", len(got), tamperSize, readErr)
		if logs, lerr := s.Logs(ctx, "ingot"); lerr == nil && strings.Contains(logs, "failed authentication") {
			t.Logf("ingot logged the aesstream authentication failure")
		}
	})

	// Range GETs fetch and verify only the chunks overlapping the range: a
	// range inside an untampered chunk stays byte-exact after the tamper,
	// and a range touching the tampered chunk fails.
	t.Run("TamperedCiphertextRangeGET", func(t *testing.T) {
		clean := getBody(t, ctx, cl, tamperBucket, tamperKey, "bytes=1000-2000")
		if !bytes.Equal(clean, tamperData[1000:2001]) {
			t.Fatalf("range in an untampered chunk mismatched after tampering elsewhere (%d bytes)", len(clean))
		}

		hdr := fmt.Sprintf("bytes=%d-%d", tamperSize-50, tamperSize-1)
		out, err := cl.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(tamperBucket), Key: aws.String(tamperKey), Range: aws.String(hdr),
		})
		if err != nil {
			t.Logf("ranged GET rejected before streaming: %v", err)
			return
		}
		defer out.Body.Close()
		got, readErr := io.ReadAll(out.Body)
		if readErr == nil && len(got) == 50 {
			t.Fatalf("range over the tampered chunk returned all %d bytes with no error — corrupt plaintext served silently", len(got))
		}
		t.Logf("tampered ranged GET failed as required: read %d/50 bytes, err=%v", len(got), readErr)
	})

	// DELETE of a multipart-created object: the accepted-blob release path
	// (distinct from abort's /blob/abort), with one claim per part blob.
	// TestForgeDeleteReleasesNetworkBlob pins the same chain for a single
	// PUT; this pins it for an object assembled from parts.
	t.Run("MultipartDeleteReleasesBlobs", func(t *testing.T) {
		const bucket, key = "mp-delete", "obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}

		partData := [][]byte{patternBytes(6 << 20), patternBytes(5 << 20), patternBytes(9 << 10)}
		before := spoolBlobPaths(t, ctx, s)
		var completed []types.CompletedPart
		var whole []byte
		for i, data := range partData {
			pn := int32(i + 1)
			up, err := cl.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
				PartNumber: aws.Int32(pn), Body: bytes.NewReader(data),
			})
			if err != nil {
				t.Fatalf("UploadPart %d: %v", pn, err)
			}
			completed = append(completed, types.CompletedPart{PartNumber: aws.Int32(pn), ETag: up.ETag})
			whole = append(whole, data...)
		}
		if _, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
		}); err != nil {
			t.Fatalf("CompleteMultipartUpload: %v", err)
		}
		added := newSpoolPaths(before, spoolBlobPaths(t, ctx, s))
		if len(added) != 3 {
			t.Fatalf("multipart upload spooled %d envelopes, want 3 (one per part under the default config)", len(added))
		}

		// Evict only THIS object's spool copies (the shared stack's other
		// objects keep theirs), so the pre-delete read must come from piri —
		// proving all three part blobs are durable on the network before we
		// assert the release traverses it.
		if out, errOut, err := s.Exec(ctx, "ingot", "rm", "-f", added[0], added[1], added[2]); err != nil {
			t.Fatalf("evict this object's spool copies: %v (stdout=%s stderr=%s)", err, out, errOut)
		}
		if got := getBody(t, ctx, cl, bucket, key, ""); !bytes.Equal(got, whole) {
			t.Fatalf("read-through from piri before delete mismatched: got %d bytes, want %d", len(got), len(whole))
		}

		if _, err := cl.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
			t.Fatalf("DeleteObject: %v", err)
		}
		if _, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err == nil {
			t.Fatalf("GET after delete succeeded, want NoSuchKey")
		} else if !strings.Contains(err.Error(), "NoSuchKey") {
			t.Fatalf("GET after delete failed with %v, want NoSuchKey", err)
		}

		// The release reached piri and its sweep finalized the byte release
		// (asynchronous end to end — poll the provider's logs through to the
		// finalization line, the forge_delete_test pattern).
		waitForPiriLog(t, ctx, s, "/blob/release", 2*time.Minute)
		waitForPiriLog(t, ctx, s, "queueing piece removal", 2*time.Minute)
		waitForPiriLog(t, ctx, s, "finalized piece removal", 3*time.Minute)
		t.Logf("multipart delete OK: %d part blobs released through piri and the sweep finalized", len(added))
	})

	// The 5 GiB part cap (versitygw's auth middleware, strictly greater-
	// than) rejects on the DECLARED length before reading the body, so the
	// over-limit probe is cheap and always runs; the expensive at-limit
	// upload is TestForgeMaxSizePart's job.
	t.Run("PartOverMaxSizeRejected", func(t *testing.T) {
		const bucket, key = "big-reject", "obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		const tooBig = int64(5)<<30 + 1
		_, err = cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(1), Body: newPatternReader(tooBig), ContentLength: aws.Int64(tooBig),
		})
		if err == nil {
			t.Fatalf("UploadPart of 5 GiB + 1 succeeded, want EntityTooLarge")
		}
		if !strings.Contains(err.Error(), "EntityTooLarge") {
			t.Fatalf("UploadPart of 5 GiB + 1 failed with %v, want EntityTooLarge", err)
		}
		if _, err := cl.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		}); err != nil {
			t.Fatalf("AbortMultipartUpload: %v", err)
		}
	})

	// --- Suite assertions blocked on unbuilt features. Each skip names the
	// missing capability and the assertions to write when it lands. ---

	t.Run("ShredThenRead", func(t *testing.T) {
		// When DELETE cryptoshreds the object: DELETE destroys every
		// segment's region-wrap row and queues the blobs for true deletion;
		// GET/HEAD return NoSuchKey (already asserted above and in
		// TestForgeDeleteReleasesNetworkBlob). ingot's own
		// blob_encryption_params deletion exists today (s3frontend
		// releaseBlobs); the region-wrap destruction does not.
		t.Skip("blocked on DELETE cryptoshred (region-wrap row destruction) — not implemented yet")
	})

	t.Run("OverwriteRace", func(t *testing.T) {
		// When overwrite atomicity (generation swap) lands: a GET concurrent
		// with an overwrite serves a consistent old or new object, never a
		// mix, and the superseded generation's keys are shredded. The flip
		// signal already exists upstream:
		// CompleteMultipartUpload_racey_data_integrity sits in the versity
		// XFail table (versity_multipart_test.go) and the ratchet flags it
		// the moment atomicity lands.
		t.Skip("blocked on overwrite atomicity via generation swap — not implemented yet")
	})

	t.Run("AbortShredsKeyRows", func(t *testing.T) {
		// When multipart key-row hygiene lands: abort and abandoned-upload
		// expiry shred the orphaned parts' key rows and queue their blobs
		// for true deletion; re-uploading a part number does the same for
		// the superseded part. Blob-side abort cleanup is already covered
		// (MultipartAbortCleansSpool, TestForgeDeferredMultipart/AbortRejects).
		t.Skip("blocked on multipart key-row hygiene (abort/expiry shred) — not implemented yet")
	})
}

// TestForgeMaxSizePart pushes S3's maximum part size — exactly 5 GiB, the
// AWS-matching cap versitygw enforces — through the encrypting write path as
// one multipart part (21 internal blobs at the default max_blob_size), then
// reads it back: HEAD size, ranged spot checks across every internal
// boundary type, and a full stream-compared GET that GCM-verifies every
// chunk of every blob. It is the end-to-end regression gate for
// bucket.DefaultMaxBlobSize's envelope allowance: every max-size split's
// FEE envelope must clear piri's piece cap on the real ship path.
//
// Gated behind INGOT_ITEST_BIG=1: it moves ~10-15 GiB through the Docker
// stack (spool copy + piri copy; nothing reclaims the spool) and takes
// minutes. CI sets the gate (.github/workflows/go-test.yml); a plain local
// `make itest` skips it.
func TestForgeMaxSizePart(t *testing.T) {
	if os.Getenv("INGOT_ITEST_BIG") == "" {
		t.Skip("set INGOT_ITEST_BIG=1 to run the 5 GiB max-part test (~10-15 GiB of disk churn)")
	}
	ctx := t.Context()
	s, endpoint := forgeStack(t)
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "bigpart")
	// Not the shared sdkClient: the upstream S3Conf pins a short per-request
	// HTTP timeout, and a 5 GiB body streams for minutes before UploadPart's
	// response headers arrive.
	cl := bigObjectClient(t, endpoint, accessKey, secretKey)

	const (
		bucket    = "big-part"
		key       = "obj"
		size      = int64(5) << 30 // exactly the cap; size+1 is PartOverMaxSizeRejected's case
		chunkSize = int64(256) << 10
		blobSize  = msbucket.DefaultMaxBlobSize // the default config's max_blob_size
	)
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	start := time.Now()
	up, err := cl.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		PartNumber: aws.Int32(1), Body: newPatternReader(size), ContentLength: aws.Int64(size),
	})
	if err != nil {
		t.Fatalf("UploadPart (5 GiB): %v", err)
	}
	if _, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{PartNumber: aws.Int32(1), ETag: up.ETag},
		}},
	}); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	t.Logf("5 GiB part uploaded and completed in %s", time.Since(start))

	head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if got := aws.ToInt64(head.ContentLength); got != size {
		t.Fatalf("HEAD ContentLength = %d, want %d", got, size)
	}

	for _, rc := range []struct {
		name       string
		start, end int64
	}{
		{"head", 0, 1023},
		{"chunk boundary", chunkSize - 100, chunkSize + 100},
		{"blob boundary", blobSize - 100, blobSize + 100},
		{"tail", size - 1024, size - 1},
	} {
		hdr := fmt.Sprintf("bytes=%d-%d", rc.start, rc.end)
		if got := getBody(t, ctx, cl, bucket, key, hdr); !bytes.Equal(got, patternRange(rc.start, rc.end)) {
			t.Fatalf("%s range %s mismatch (%d bytes)", rc.name, hdr, len(got))
		}
	}

	start = time.Now()
	out, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObject (5 GiB): %v", err)
	}
	defer out.Body.Close()
	buf := make([]byte, 4<<20)
	want := make([]byte, 4<<20)
	var pos int64
	for pos < size {
		n, rerr := io.ReadFull(out.Body, buf)
		if n > 0 {
			expected := want[:n]
			for i := range expected {
				expected[i] = patternByteAt(pos + int64(i))
			}
			if !bytes.Equal(buf[:n], expected) {
				t.Fatalf("read-back mismatch in the block at offset %d", pos)
			}
			pos += int64(n)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			t.Fatalf("read at offset %d: %v", pos, rerr)
		}
	}
	if pos != size {
		t.Fatalf("full GET returned %d bytes, want %d", pos, size)
	}
	t.Logf("5 GiB read back byte-exact in %s", time.Since(start))
}
