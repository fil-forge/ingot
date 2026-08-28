//go:build itest

package itest

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/libforge/digestutil"
	"github.com/fil-forge/smelt/pkg/stack"
	"github.com/multiformats/go-multihash"
)

// TestForgeDeferredMultipart is the deferred-accept regression gate (§7.2):
// UploadPart must make each part blob durable on piri WITHOUT accepting it
// (parked — outside the PDP pipeline), Complete must conclude (accept) every
// part, and Abort must unwind parked blobs with /blob/abort → /blob/reject.
//
// Runs on the stock smelt-SDK images like every other itest. It needs
// piri:main ≥ fil-forge/piri#30 (/blob/reject) and sprue:main ≥
// fil-forge/sprue#33 (/blob/abort forwarding); until piri#30 publishes,
// point INGOT_ITEST_PIRI_IMAGE at a branch image (the forgeStack escape
// hatch).
func TestForgeDeferredMultipart(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t)
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "mpdeferred")
	cl := sdkClient(forgeS3Conf(endpoint, accessKey, secretKey))

	// RoundTrip: parts park at UploadPart (durable on piri, NOT accepted),
	// Complete concludes them, and the object round-trips.
	t.Run("RoundTrip", func(t *testing.T) {
		const bucket, key = "mp-deferred", "obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}

		// Unique per-part content (XOR-tagged so nothing dedups against other
		// tests); each part is a single blob under the default max_blob_size.
		partData := [][]byte{tagged(patternBytes(6<<20), 0x11), tagged(patternBytes(90<<10), 0x22)}
		var completed []types.CompletedPart
		var whole []byte
		var partDigests []string
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
			// The stored digest names the encrypted envelope, minted at
			// ingest — read it back from ingot's part registry rather than
			// predicting hash(plaintext).
			partDigests = append(partDigests, partBlobDigests(t, ctx, s, aws.ToString(create.UploadId), pn)...)
		}
		if len(partDigests) != len(partData) {
			t.Fatalf("parts stored %d blobs, want one per part (%d)", len(partDigests), len(partData))
		}

		// The parts are durable on piri (allocated + PUT) but parked: no
		// /blob/accept for their digests until Complete.
		for _, d := range partDigests {
			waitForPiriLogLine(t, ctx, s, 30*time.Second, "/blob/allocate", d)
			if piriLogHasLine(t, ctx, s, "/blob/accept", d) {
				t.Fatalf("part blob %s was accepted before Complete — parking is broken", d)
			}
		}

		comp, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
		})
		if err != nil {
			t.Fatalf("CompleteMultipartUpload: %v", err)
		}
		if et := strings.Trim(aws.ToString(comp.ETag), `"`); !strings.HasSuffix(et, "-2") {
			t.Fatalf("complete ETag = %q, want a multipart -2 suffix", et)
		}

		// Complete concluded every part: accepts landed on piri, and the
		// object round-trips.
		for _, d := range partDigests {
			waitForPiriLogLine(t, ctx, s, 60*time.Second, "/blob/accept", d)
		}
		if got := getBody(t, ctx, cl, bucket, key, ""); !bytes.Equal(got, whole) {
			t.Fatalf("GET mismatch: got %d bytes, want %d", len(got), len(whole))
		}
	})

	// AbortRejects: an aborted upload's parked blobs are unwound on piri
	// via /blob/reject — never accepted, bytes released.
	t.Run("AbortRejects", func(t *testing.T) {
		const bucket, key = "mp-abort-unalloc", "obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		data := tagged(patternBytes(512<<10), 0x33)
		if _, err := cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(1), Body: bytes.NewReader(data),
		}); err != nil {
			t.Fatalf("UploadPart: %v", err)
		}
		digest := partBlobDigests(t, ctx, s, aws.ToString(create.UploadId), 1)[0]
		waitForPiriLogLine(t, ctx, s, 30*time.Second, "/blob/allocate", digest)

		if _, err := cl.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		}); err != nil {
			t.Fatalf("AbortMultipartUpload: %v", err)
		}

		// The abort traversed ingot → sprue → piri (as /blob/reject), and the blob was
		// never accepted.
		waitForPiriLogLine(t, ctx, s, 60*time.Second, "/blob/reject", digest)
		if piriLogHasLine(t, ctx, s, "/blob/accept", digest) {
			t.Fatalf("aborted part blob %s was accepted — abort/accept exclusivity is broken", digest)
		}
		t.Logf("abort unwound parked blob %s via /blob/reject", digest)
	})
}

// tagged XORs b with tag so each caller gets globally unique content that
// cannot dedup against other tests' blobs.
func tagged(b []byte, tag byte) []byte {
	for i := range b {
		b[i] ^= tag
	}
	return b
}

// partBlobDigests reads one uploaded part's stored blob digests from ingot's
// Postgres, base58-formatted the way piri's handlers log them
// (digestutil.Format). With encryption the stored digest names the FEE
// envelope, minted at ingest — it cannot be predicted from the plaintext.
func partBlobDigests(t *testing.T, ctx context.Context, s *stack.Stack, uploadID string, partNumber int32) []string {
	t.Helper()
	q := fmt.Sprintf(
		`SELECT encode(d, 'hex') FROM ingot.multipart_parts, unnest(blob_digests) AS d WHERE upload_id = '%s' AND part_number = %d`,
		uploadID, partNumber)
	out, errOut, err := s.Exec(ctx, "ingot-postgres", "psql", "-U", "ingot", "-d", "ingot", "-tAc", q)
	if err != nil {
		t.Fatalf("query part %d digests: %v (stderr=%s)", partNumber, err, errOut)
	}
	var digests []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		raw, err := hex.DecodeString(line)
		if err != nil {
			t.Fatalf("decode digest hex %q: %v", line, err)
		}
		digests = append(digests, digestutil.Format(multihash.Multihash(raw)))
	}
	if len(digests) == 0 {
		t.Fatalf("no blob digests recorded for upload %s part %d", uploadID, partNumber)
	}
	return digests
}

// piriLogHasLine reports whether any single piri-0 log line contains all
// substrings.
func piriLogHasLine(t *testing.T, ctx context.Context, s *stack.Stack, substrs ...string) bool {
	t.Helper()
	logs, err := s.Logs(ctx, "piri-0")
	if err != nil {
		t.Fatalf("piri-0 logs: %v", err)
	}
	for _, line := range strings.Split(logs, "\n") {
		ok := true
		for _, sub := range substrs {
			if !strings.Contains(line, sub) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// waitForPiriLogLine polls until one piri-0 log line contains all substrings.
func waitForPiriLogLine(t *testing.T, ctx context.Context, s *stack.Stack, timeout time.Duration, substrs ...string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if piriLogHasLine(t, ctx, s, substrs...) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context done waiting for piri log %v: %v", substrs, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("piri-0 logs never contained a line with all of %v within %s", substrs, timeout)
}
