//go:build itest

package itest

import (
	"bytes"
	"context"
	"os"
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
// part, and Abort must unwind parked blobs with /blob/unallocate.
//
// Requires piri + sprue builds with /blob/remove + /blob/unallocate; the
// published images predate them, so inject working-tree binaries and skip
// otherwise (same gate as TestForgeDeleteReleasesNetworkBlob):
//
//	(cd ../../piri  && CGO_ENABLED=0 GOOS=linux go build -o /tmp/piri  ./cmd)
//	(cd ../../sprue && CGO_ENABLED=0 GOOS=linux go build -o /tmp/sprue ./cmd/main.go)
//	ITEST_PIRI_BIN=/tmp/piri ITEST_SPRUE_BIN=/tmp/sprue \
//	  go test -tags itest ./itest -run TestForgeDeferredMultipart -v -timeout 1200s
func TestForgeDeferredMultipart(t *testing.T) {
	piriBin := os.Getenv("ITEST_PIRI_BIN")
	sprueBin := os.Getenv("ITEST_SPRUE_BIN")
	if piriBin == "" || sprueBin == "" {
		t.Skip("requires piri+sprue builds with the /blob/unallocate chain: set ITEST_PIRI_BIN and ITEST_SPRUE_BIN (see test doc comment)")
	}

	ctx := t.Context()
	s, endpoint := forgeStack(t,
		stack.WithServiceBinary("piri", piriBin),
		stack.WithServiceBinary("upload", sprueBin),
	)
	ingotSelfProvision(t, ctx, s, "test@example.com")
	cl := sdkClient(forgeS3Conf(endpoint))

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
			partDigests = append(partDigests, b58Digest(t, data))
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

	// AbortUnallocates: an aborted upload's parked blobs are unwound on piri
	// via /blob/unallocate — never accepted, bytes released.
	t.Run("AbortUnallocates", func(t *testing.T) {
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
		digest := b58Digest(t, data)
		if _, err := cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(1), Body: bytes.NewReader(data),
		}); err != nil {
			t.Fatalf("UploadPart: %v", err)
		}
		waitForPiriLogLine(t, ctx, s, 30*time.Second, "/blob/allocate", digest)

		if _, err := cl.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		}); err != nil {
			t.Fatalf("AbortMultipartUpload: %v", err)
		}

		// The unallocate traversed ingot → sprue → piri, and the blob was
		// never accepted.
		waitForPiriLogLine(t, ctx, s, 60*time.Second, "/blob/unallocate", digest)
		if piriLogHasLine(t, ctx, s, "/blob/accept", digest) {
			t.Fatalf("aborted part blob %s was accepted — abort/accept exclusivity is broken", digest)
		}
		t.Logf("abort unwound parked blob %s via /blob/unallocate", digest)
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

// b58Digest returns the base58 sha2-256 multihash of data — the form piri's
// handlers log blob digests in (digestutil.Format).
func b58Digest(t *testing.T, data []byte) string {
	t.Helper()
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return digestutil.Format(mh)
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
