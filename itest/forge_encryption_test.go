//go:build itest

package itest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/smelt/pkg/stack"
	"github.com/filecoin-project/go-fee/cose"

	msbucket "github.com/fil-forge/ingot/bucket"
)

// TestForgeEncryption is the end-to-end encryption suite: the
// encryption-specific behaviors the round-trip and conformance tests don't
// pin, against a forge-mode stack under the DEFAULT config (~254 MiB
// max_blob_size, aesstream's 256 KiB chunks) — the tamper subtests depend on
// a blob holding several chunks, which the smallblob config collapses.
//
// Related coverage lives elsewhere: round trips and cross-part range GETs
// in TestForgeScenarios and the versity tables, abort blob-cleanup in
// TestForgeScenarios and TestForgeDeferredMultipart, expiry-sweep shred in
// TestForgeMultipartExpiryShred, the 5 GiB max part in
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

	// ShredThenRead: DELETE cryptoshreds the object. The
	// blob_encryption_params row is the region wrap of the per-blob CEK —
	// deleting it is the per-blob crypto-shred (migration 00014) — and
	// DeleteObject removes it, the location row, and the network claim for
	// every body blob. The spooled envelope survives with its single tenant
	// recipient: the insurance copy hilt's custody can still open until true
	// deletion. Versioned buckets' delete-marker path deliberately does not
	// shred (S3 semantics); this covers the unversioned path.
	t.Run("ShredThenRead", func(t *testing.T) {
		const bucket, key = "shred", "obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		// Two parts → two body blobs, so "every segment's row" is plural.
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		partData := [][]byte{tagged(patternBytes(5<<20), 0x3C), tagged(patternBytes(9<<10), 0x5A)}
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
		if got := getBody(t, ctx, cl, bucket, key, ""); !bytes.Equal(got, whole) {
			t.Fatalf("round-trip before delete mismatched: %d bytes", len(got))
		}

		digests := objectBlobDigestsHex(t, ctx, s, bucket, key)
		if len(digests) != len(partData) {
			t.Fatalf("blob_refs records %d digests, want %d (one per part)", len(digests), len(partData))
		}
		// Pre-delete sanity: without this, the zero-rows assertion below
		// would pass vacuously against an unencrypted write.
		if n := countRowsForDigests(t, ctx, s, "ingot.blob_encryption_params", "digest", digests); n != len(digests) {
			t.Fatalf("pre-delete: %d enc-params rows for %d blobs", n, len(digests))
		}

		if _, err := cl.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
			t.Fatalf("DeleteObject: %v", err)
		}
		if _, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err == nil || !strings.Contains(err.Error(), "NoSuchKey") {
			t.Fatalf("GET after delete: want NoSuchKey, got %v", err)
		}
		if _, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err == nil {
			t.Fatalf("HEAD after delete succeeded, want NotFound")
		}

		// The claims drop synchronously with the delete; the shred (enc-params
		// + location rows) is deferred behind the release grace (60s default)
		// and executed by the release sweeper — poll it through.
		refsQ := fmt.Sprintf(`SELECT count(*) FROM ingot.blob_refs WHERE bucket = '%s' AND object_key = '%s'`, bucket, key)
		if out := ingotSQL(t, ctx, s, refsQ); out != "0" {
			t.Fatalf("%s blob_refs rows survived the delete", out)
		}
		shredDeadline := time.Now().Add(3 * time.Minute)
		for {
			enc := countRowsForDigests(t, ctx, s, "ingot.blob_encryption_params", "digest", digests)
			loc := countRowsForDigests(t, ctx, s, "ingot.blob_locations", "digest", digests)
			if enc == 0 && loc == 0 {
				break
			}
			if time.Now().After(shredDeadline) {
				t.Fatalf("deferred shred never executed: %d enc-params + %d location rows survive", enc, loc)
			}
			time.Sleep(5 * time.Second)
		}

		// The insurance copy: nothing removes spool files on delete, and the
		// surviving envelope's sole recipient is still the tenant wrap key —
		// recoverable from hilt's custody alone until true deletion.
		env := spooledEnvelopeAt(t, ctx, s, "/data/spool/"+digests[0])
		wantKID := hiltActiveWrapKID(t, ctx, s, "encryption")
		if len(env.Recipients) != 1 {
			t.Fatalf("surviving envelope has %d recipients, want 1 (the tenant)", len(env.Recipients))
		}
		if kid, ok := env.Recipients[0].Headers.Unprotected.Bytes(cose.HeaderLabelKID); !ok || string(kid) != wantKID {
			t.Fatalf("surviving envelope recipient kid = %q, want the tenant wrap key %q", kid, wantKID)
		}
		t.Logf("shred OK: %d region-wrap rows destroyed, envelope + tenant recipient survive", len(digests))
	})

	// AbortShredsKeyRows: aborting an upload shreds the orphaned parts' key
	// rows — cleanupPartBlobs deletes each part blob's enc-params row,
	// upload intent, and park row (the spool and piri unwind are pinned by
	// MultipartAbortCleansSpool and TestForgeDeferredMultipart/AbortRejects).
	// Expiry-sweep shred is TestForgeMultipartExpiryShred (needs a low-TTL
	// stack).
	t.Run("AbortShredsKeyRows", func(t *testing.T) {
		const bucket, key = "mp-shred-abort", "obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		if _, err := cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(1), Body: bytes.NewReader(tagged(patternBytes(5<<20), 0x3D)),
		}); err != nil {
			t.Fatalf("UploadPart: %v", err)
		}
		// Capture before the abort: the session cascade destroys the part
		// rows, and with them the only index to these digests.
		digests := partBlobDigestsHex(t, ctx, s, aws.ToString(create.UploadId), 1)
		if n := countRowsForDigests(t, ctx, s, "ingot.blob_encryption_params", "digest", digests); n != len(digests) {
			t.Fatalf("pre-abort: %d enc-params rows for %d part blobs", n, len(digests))
		}

		if _, err := cl.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		}); err != nil {
			t.Fatalf("AbortMultipartUpload: %v", err)
		}

		for _, tbl := range []string{"ingot.blob_encryption_params", "ingot.upload_intents", "ingot.blob_parks"} {
			if n := countRowsForDigests(t, ctx, s, tbl, "digest", digests); n != 0 {
				t.Fatalf("%d %s rows survived the abort", n, tbl)
			}
		}
		t.Logf("abort shredded %d part-blob key rows (enc-params, intents, parks)", len(digests))
	})

	// SupersededPartShredsKeyRow: re-uploading a part number shreds the
	// superseded part's key row synchronously (cleanupPartBlobs runs inline
	// at UploadPart), and Complete serves the winner.
	t.Run("SupersededPartShredsKeyRow", func(t *testing.T) {
		const bucket, key = "mp-shred-supersede", "obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		uploadPart := func(data []byte) *s3.UploadPartOutput {
			up, err := cl.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
				PartNumber: aws.Int32(1), Body: bytes.NewReader(data),
			})
			if err != nil {
				t.Fatalf("UploadPart: %v", err)
			}
			return up
		}

		uploadPart(tagged(patternBytes(5<<20), 0x4E))
		oldDigests := partBlobDigestsHex(t, ctx, s, aws.ToString(create.UploadId), 1)
		if n := countRowsForDigests(t, ctx, s, "ingot.blob_encryption_params", "digest", oldDigests); n != len(oldDigests) {
			t.Fatalf("pre-supersede: %d enc-params rows for %d part blobs", n, len(oldDigests))
		}

		winner := tagged(patternBytes(5<<20), 0x77)
		up2 := uploadPart(winner)
		newDigests := partBlobDigestsHex(t, ctx, s, aws.ToString(create.UploadId), 1)

		if n := countRowsForDigests(t, ctx, s, "ingot.blob_encryption_params", "digest", oldDigests); n != 0 {
			t.Fatalf("%d enc-params rows of the superseded part survived", n)
		}
		if n := countRowsForDigests(t, ctx, s, "ingot.blob_encryption_params", "digest", newDigests); n != len(newDigests) {
			t.Fatalf("the winning part has %d enc-params rows, want %d", n, len(newDigests))
		}

		if _, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: up2.ETag},
			}},
		}); err != nil {
			t.Fatalf("CompleteMultipartUpload: %v", err)
		}
		if got := getBody(t, ctx, cl, bucket, key, ""); !bytes.Equal(got, winner) {
			t.Fatalf("object is not the winning part's bytes (%d bytes)", len(got))
		}
		t.Logf("supersede shredded %d key rows; winner round-trips", len(oldDigests))
	})

	// CompleteShredsOrphanParts: a part uploaded but omitted from the
	// Complete list is reaped at Complete — key row, upload intent, and park
	// row gone — while the winner commits and round-trips.
	t.Run("CompleteShredsOrphanParts", func(t *testing.T) {
		const bucket, key = "mp-shred-orphan", "obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		winner := tagged(patternBytes(5<<20), 0x2B)
		up1, err := cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(1), Body: bytes.NewReader(winner),
		})
		if err != nil {
			t.Fatalf("UploadPart 1: %v", err)
		}
		if _, err := cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(2), Body: bytes.NewReader(tagged(patternBytes(5<<20), 0x9C)),
		}); err != nil {
			t.Fatalf("UploadPart 2: %v", err)
		}
		orphan := partBlobDigestsHex(t, ctx, s, aws.ToString(create.UploadId), 2)
		if n := countRowsForDigests(t, ctx, s, "ingot.blob_encryption_params", "digest", orphan); n != len(orphan) {
			t.Fatalf("pre-complete: %d enc-params rows for %d orphan blobs", n, len(orphan))
		}

		if _, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: up1.ETag},
			}},
		}); err != nil {
			t.Fatalf("CompleteMultipartUpload: %v", err)
		}

		for _, tbl := range []string{"ingot.blob_encryption_params", "ingot.upload_intents", "ingot.blob_parks"} {
			if n := countRowsForDigests(t, ctx, s, tbl, "digest", orphan); n != 0 {
				t.Fatalf("%d %s rows survived for the orphaned part", n, tbl)
			}
		}
		if got := getBody(t, ctx, cl, bucket, key, ""); !bytes.Equal(got, winner) {
			t.Fatalf("winner mismatch after orphan reap (%d bytes)", len(got))
		}
		t.Logf("Complete reaped %d orphan part blobs; winner round-trips", len(orphan))
	})

	// OverwriteRace: a GET concurrent with an overwrite serves exactly the
	// old or the new object — length, sha256, and ETag from one generation,
	// never a mix, a short read, or an error. The guarantees under test: the
	// catalog root swap is atomic; each generation's claims are keyed
	// per-generation; and the superseded generation's enc-params/location
	// rows are shredded only after the release grace, so readers holding the
	// prior root finish first.
	t.Run("OverwriteRace", func(t *testing.T) {
		const bucket, key = "overwrite-race", "obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		oldData := tagged(patternBytes(1<<20), 0x51)
		newData := tagged(patternBytes(1<<20+64<<10), 0x62) // different length amplifies mix detection
		oldETag, newETag := quotedMD5(oldData), quotedMD5(newData)
		oldSum, newSum := sha256.Sum256(oldData), sha256.Sum256(newData)

		if _, err := cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(oldData),
		}); err != nil {
			t.Fatalf("PutObject (old): %v", err)
		}
		oldDigests := objectBlobDigestsHex(t, ctx, s, bucket, key)

		start := make(chan struct{})
		stop := make(chan struct{})
		violations := make(chan string, 16)
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for {
					select {
					case <-stop:
						return
					default:
					}
					out, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
					if err != nil {
						violations <- fmt.Sprintf("GET error during overwrite: %v", err)
						return
					}
					body, rerr := io.ReadAll(out.Body)
					_ = out.Body.Close()
					if rerr != nil {
						violations <- fmt.Sprintf("body read error during overwrite: %v", rerr)
						return
					}
					etag := aws.ToString(out.ETag)
					sum := sha256.Sum256(body)
					oldOK := len(body) == len(oldData) && sum == oldSum && etag == oldETag
					newOK := len(body) == len(newData) && sum == newSum && etag == newETag
					if !oldOK && !newOK {
						violations <- fmt.Sprintf("mixed read: %d bytes, etag %s", len(body), etag)
						return
					}
				}
			}()
		}
		close(start)
		if _, err := cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(newData),
		}); err != nil {
			t.Fatalf("PutObject (overwrite): %v", err)
		}
		time.Sleep(500 * time.Millisecond) // a few post-swap reads
		close(stop)
		wg.Wait()
		close(violations)
		for v := range violations {
			t.Errorf("%s", v)
		}
		if t.Failed() {
			t.FailNow()
		}

		if got := getBody(t, ctx, cl, bucket, key, ""); !bytes.Equal(got, newData) {
			t.Fatalf("final GET is not the new object (%d bytes)", len(got))
		}
		// The superseded generation's rows shred after the release grace.
		shredDeadline := time.Now().Add(3 * time.Minute)
		for {
			enc := countRowsForDigests(t, ctx, s, "ingot.blob_encryption_params", "digest", oldDigests)
			loc := countRowsForDigests(t, ctx, s, "ingot.blob_locations", "digest", oldDigests)
			if enc == 0 && loc == 0 {
				break
			}
			if time.Now().After(shredDeadline) {
				t.Fatalf("superseded generation never shredded: %d enc-params + %d location rows survive", enc, loc)
			}
			time.Sleep(5 * time.Second)
		}
		t.Logf("overwrite race clean: every concurrent read was exactly old or new; superseded generation shredded")
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

// TestForgeMultipartExpiryShred proves the abandoned-session sweeper is a
// full abort: an unfinished multipart upload past multipart_session_ttl
// loses its session row, and its parked parts' key rows (enc-params),
// upload intents, and park rows are shredded — the same cleanup a client
// abort runs. Dedicated stack: config-mpttl.yaml's 30s TTL also reaps
// completed sessions, so it cannot share the encryption suite's stack.
func TestForgeMultipartExpiryShred(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t, withMultipartTTLConfig())
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "mpexpiry")
	cl := sdkClient(forgeS3Conf(endpoint, accessKey, secretKey))

	const bucket, key = "mp-expiry", "obj"
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if _, err := cl.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		PartNumber: aws.Int32(1), Body: bytes.NewReader(tagged(patternBytes(5<<20), 0x66)),
	}); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	digests := partBlobDigestsHex(t, ctx, s, aws.ToString(create.UploadId), 1)
	if n := countRowsForDigests(t, ctx, s, "ingot.blob_encryption_params", "digest", digests); n != len(digests) {
		t.Fatalf("pre-expiry: %d enc-params rows for %d part blobs", n, len(digests))
	}

	// Abandon the upload. The sweeper ticks at ttl/2 (15s here); give it a
	// few cycles of headroom.
	sessionQ := fmt.Sprintf(`SELECT count(*) FROM ingot.multipart_sessions WHERE upload_id = '%s'`, aws.ToString(create.UploadId))
	deadline := time.Now().Add(3 * time.Minute)
	for {
		if ingotSQL(t, ctx, s, sessionQ) == "0" &&
			countRowsForDigests(t, ctx, s, "ingot.blob_encryption_params", "digest", digests) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("session or enc-params rows survived %s past the 30s TTL — the sweeper did not shred", time.Since(deadline.Add(-3*time.Minute)))
		}
		time.Sleep(5 * time.Second)
	}
	for _, tbl := range []string{"ingot.upload_intents", "ingot.blob_parks"} {
		if n := countRowsForDigests(t, ctx, s, tbl, "digest", digests); n != 0 {
			t.Fatalf("%d %s rows survived the expiry sweep", n, tbl)
		}
	}
	t.Logf("expiry sweep shredded %d part-blob key rows and the session", len(digests))
}

// spooledEnvelopeAt reads the spooled blob at path inside the ingot
// container and decodes its COSE envelope header. The spool filename is the
// hex ciphertext multihash, so a blob_refs digest maps to
// /data/spool/<hex>.
func spooledEnvelopeAt(t *testing.T, ctx context.Context, s *stack.Stack, path string) *cose.Envelope {
	t.Helper()
	out, errOut, err := s.Exec(ctx, "ingot", "sh", "-c", fmt.Sprintf(`base64 < %q`, path))
	if err != nil {
		t.Fatalf("read spooled blob %s: %v (stderr=%s)", path, err, errOut)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(out), ""))
	if err != nil {
		t.Fatalf("decode spooled blob %s: %v", path, err)
	}
	env, _, err := cose.Decode(raw)
	if err != nil {
		t.Fatalf("decode COSE envelope %s: %v", path, err)
	}
	return env
}
