//go:build itest

package itest

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestForgeMultipartManyParts measures how long CompleteMultipartUpload takes
// as a function of part count. It is the benchmark for the batched
// /ucan/conclude work: every part parks one blob at UploadPart, and Complete
// must accept all of them before it can answer, so its wall time is the
// protocol cost of N accepts.
//
// The number that matters is "complete" in the summary line; the upload
// phase is reported only so a run can be sanity-checked against the disk and
// network it moved. Run it once against the published images for a baseline,
// then again with INGOT_ITEST_UPLOAD_BINARY pointed at a local sprue build.
//
// Gated behind INGOT_ITEST_MP_BENCH=1: at the default 1000 parts it moves
// ~5 GiB through the stack and lands several times that on the Docker disk
// once the spool, piri's copy and sprue's agent-message store are counted.
// Check free space first — when the Docker VM's disk fills, MinIO answers
// sprue with 507 and every UploadPart fails as an opaque 500. Reclaim the
// previous run's volumes (`docker volume ls -qf dangling=true | grep
// smeltery-`) between runs, and lower INGOT_ITEST_MP_PARTS to fit.
//
//	INGOT_ITEST_MP_BENCH=1 INGOT_ITEST_MP_PARTS=1000 \
//	  GOWORK=off go test -tags itest ./itest -run TestForgeMultipartManyParts -v -timeout 60m
func TestForgeMultipartManyParts(t *testing.T) {
	if os.Getenv("INGOT_ITEST_MP_BENCH") == "" {
		t.Skip("set INGOT_ITEST_MP_BENCH=1 to run the many-parts multipart benchmark")
	}
	parts := 1000
	if v := os.Getenv("INGOT_ITEST_MP_PARTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("INGOT_ITEST_MP_PARTS=%q: want a positive integer", v)
		}
		parts = n
	}
	uploadWorkers := 16
	if v := os.Getenv("INGOT_ITEST_MP_WORKERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("INGOT_ITEST_MP_WORKERS=%q: want a positive integer", v)
		}
		uploadWorkers = n
	}

	// 5 MiB is the floor: Complete rejects any part but the last below
	// backend.MinPartSize, so this is the smallest part count-to-bytes ratio
	// the real S3 path allows. One part is one blob under the default
	// max_blob_size.
	const partSize = int64(5) << 20
	total := partSize * int64(parts)

	ctx := t.Context()
	s, endpoint := forgeStack(t)
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "mpbench")
	// Not sdkClient: the upstream S3Conf pins a 30s per-request timeout, and
	// Complete over thousands of parts is the thing being measured.
	cl := bigObjectClient(t, endpoint, accessKey, secretKey)

	const bucket, key = "mp-bench", "obj"
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := aws.ToString(create.UploadId)
	t.Logf("uploading %d parts of %d bytes (%.1f GiB total) with %d workers",
		parts, partSize, float64(total)/float64(1<<30), uploadWorkers)

	completed := make([]types.CompletedPart, parts)
	var (
		mu       sync.Mutex
		firstErr error
	)
	uploadStart := time.Now()
	sem := make(chan struct{}, uploadWorkers)
	var wg sync.WaitGroup
	for i := 0; i < parts; i++ {
		pn := int32(i + 1)
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			mu.Lock()
			stop := firstErr != nil
			mu.Unlock()
			if stop {
				return
			}
			up, err := cl.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
				PartNumber: aws.Int32(pn), Body: newMPPartReader(pn, partSize),
				ContentLength: aws.Int64(partSize),
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("UploadPart %d: %w", pn, err)
				}
				return
			}
			completed[pn-1] = types.CompletedPart{PartNumber: aws.Int32(pn), ETag: up.ETag}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatalf("%v", firstErr)
	}
	uploadTook := time.Since(uploadStart)
	t.Logf("uploaded %d parts in %s (%.1f MiB/s)", parts, uploadTook.Round(time.Millisecond),
		float64(total)/(1<<20)/uploadTook.Seconds())

	completeStart := time.Now()
	if _, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	}); err != nil {
		t.Fatalf("CompleteMultipartUpload after %s: %v", time.Since(completeStart).Round(time.Millisecond), err)
	}
	completeTook := time.Since(completeStart)

	// The object must actually be readable: a completion that skipped accepts
	// would be a meaningless number.
	head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if got := aws.ToInt64(head.ContentLength); got != total {
		t.Fatalf("HeadObject size = %d, want %d", got, total)
	}
	// Spot-check the first, a middle and the last part through ranged reads:
	// each range lands in a different part blob, so a missed accept shows up.
	for _, pn := range []int32{1, int32(parts/2 + 1), int32(parts)} {
		off := int64(pn-1) * partSize
		const n = 4096
		want := mpPartBytes(pn, 0, n)
		got := getBody(t, ctx, cl, bucket, key, fmt.Sprintf("bytes=%d-%d", off, off+n-1))
		if len(got) != n {
			t.Fatalf("ranged GET of part %d: got %d bytes, want %d", pn, len(got), n)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("ranged GET of part %d: byte %d = %#x, want %#x", pn, i, got[i], want[i])
			}
		}
	}

	t.Logf("BENCH parts=%d partSize=%d totalBytes=%d upload=%s complete=%s completePerPart=%s",
		parts, partSize, total, uploadTook.Round(time.Millisecond),
		completeTook.Round(time.Millisecond),
		(completeTook / time.Duration(parts)).Round(time.Microsecond))
}

// mpPartByteAt is the many-parts benchmark's content formula: every part has
// distinct bytes so nothing dedups against another part, the spool, or
// another test's blobs.
func mpPartByteAt(part int32, i int64) byte {
	// Mixed rather than summed: a linear term in the part number repeats
	// every 256 parts once truncated to a byte, so part 1 and part 257 would
	// generate identical content and dedup against each other in the spool,
	// quietly measuring far fewer blobs than the test asked for.
	h := uint64(part)*0x9E3779B97F4A7C15 ^ uint64(i)*0xBF58476D1CE4E5B9
	h ^= h >> 29
	h *= 0x94D049BB133111EB
	h ^= h >> 32
	return byte(h)
}

// mpPartBytes materializes n bytes of a part's content starting at off.
func mpPartBytes(part int32, off int64, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = mpPartByteAt(part, off+int64(i))
	}
	return b
}

// mpPartReader streams one part's content without allocating it. It is
// seekable so the AWS SDK can rewind it for signing and retries.
type mpPartReader struct {
	part      int32
	off, size int64
}

func newMPPartReader(part int32, size int64) *mpPartReader {
	return &mpPartReader{part: part, size: size}
}

func (r *mpPartReader) Read(p []byte) (int, error) {
	if r.off >= r.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if rem := r.size - r.off; rem < n {
		n = rem
	}
	for i := int64(0); i < n; i++ {
		p[i] = mpPartByteAt(r.part, r.off+i)
	}
	r.off += n
	return int(n), nil
}

func (r *mpPartReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.off + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, fmt.Errorf("mpPartReader.Seek: invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("mpPartReader.Seek: negative position %d", abs)
	}
	r.off = abs
	return abs, nil
}
