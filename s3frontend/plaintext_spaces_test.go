package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
)

// A blob stored as received has the digest of its bytes, so the same bytes
// written to two buckets of different spaces share a digest: one spool
// file, and an upload, a park, a claim and a release per space. These tests
// write identical bytes to "bk" (space did.Undef) and a second bucket of its
// own space, and check that one space's lifecycle never takes the other's
// rows or file.

// secondPlaintextBucket creates "bk2" in a space of its own and sets both
// buckets to store as received. Returns bk2's space.
func secondPlaintextBucket(t *testing.T, b *Backend, mem *inmem.MemStore) did.DID {
	t.Helper()
	ctx := context.Background()
	space := testutil.RandomDID(t)
	if err := mem.Create(ctx, "bk2", space, registry.CreateState{}); err != nil {
		t.Fatalf("create bk2: %v", err)
	}
	b.allowNoneEncryption = true
	for _, bucket := range []string{"bk", "bk2"} {
		if err := b.PutBucketEncryption(ctx, bucket, sseConfig(sseNone)); err != nil {
			t.Fatalf("PutBucketEncryption %s none: %v", bucket, err)
		}
	}
	return space
}

func putIn(t *testing.T, b *Backend, bucket, key string, data []byte) {
	t.Helper()
	if _, err := b.PutObject(context.Background(), s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data)}); err != nil {
		t.Fatalf("PutObject %s/%s: %v", bucket, key, err)
	}
}

func getFrom(t *testing.T, b *Backend, bucket, key string) []byte {
	t.Helper()
	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("GetObject %s/%s: %v", bucket, key, err)
	}
	defer out.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(out.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	return buf.Bytes()
}

func deleteFrom(t *testing.T, b *Backend, bucket, key string) {
	t.Helper()
	if _, err := b.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		t.Fatalf("DeleteObject %s/%s: %v", bucket, key, err)
	}
}

// uploadOnePart starts a multipart upload of key in bucket, uploads part as
// its only part, and returns the upload id and the part for Complete.
func uploadOnePart(t *testing.T, b *Backend, bucket, key string, part []byte) (string, []types.CompletedPart) {
	t.Helper()
	ctx := context.Background()
	res, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("CreateMultipartUpload %s/%s: %v", bucket, key, err)
	}
	n := int32(1)
	out, err := b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &res.UploadId, PartNumber: &n, Body: bytes.NewReader(part)})
	if err != nil {
		t.Fatalf("UploadPart %s/%s: %v", bucket, key, err)
	}
	return res.UploadId, []types.CompletedPart{{ETag: out.ETag, PartNumber: &n}}
}

func completeIn(t *testing.T, b *Backend, bucket, key, uploadID string, parts []types.CompletedPart) {
	t.Helper()
	if _, _, err := b.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket: &bucket, Key: &key, UploadId: &uploadID, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		t.Fatalf("CompleteMultipartUpload %s/%s: %v", bucket, key, err)
	}
}

// localCopyExists reports whether the blob is on local disk, in the spool or
// the cache.
func localCopyExists(b *Backend, d multihash.Multihash) bool {
	_, spoolErr := os.Stat(b.spool.Path(d))
	_, cacheErr := os.Stat(b.cache.Path(d))
	return spoolErr == nil || cacheErr == nil
}

// TestPlaintextSpaces_EachSpaceParksItsOwnCopy: the same part bytes uploaded
// to two spaces are parked once per space, so each Complete concludes its
// own allocation.
func TestPlaintextSpaces_EachSpaceParksItsOwnCopy(t *testing.T) {
	b, mem, _ := newParkingBackend(t)
	space2 := secondPlaintextBucket(t, b, mem)
	ctx := context.Background()
	part := testBody(5 << 20)

	id1, parts1 := uploadOnePart(t, b, "bk", "k", part)
	id2, parts2 := uploadOnePart(t, b, "bk2", "k", part)
	d := hygienePartDigests(t, mem, id1, 1)[0]
	if d2 := hygienePartDigests(t, mem, id2, 1)[0]; !bytes.Equal(d, d2) {
		t.Fatalf("identical parts parked under different digests")
	}
	for _, space := range []did.DID{did.Undef, space2} {
		if _, err := mem.GetPark(ctx, space, d); err != nil {
			t.Fatalf("park for %x in %s: %v", d, space, err)
		}
	}

	completeIn(t, b, "bk", "k", id1, parts1)
	completeIn(t, b, "bk2", "k", id2, parts2)
	for _, bucket := range []string{"bk", "bk2"} {
		if got := getFrom(t, b, bucket, "k"); !bytes.Equal(got, part) {
			t.Fatalf("%s/k reads back %d bytes, want the %d-byte part", bucket, len(got), len(part))
		}
	}
}

// TestPlaintextSpaces_ReleaseInOneSpaceKeepsTheOther: deleting one space's
// object releases that space's claim, location and intent, and nothing of
// the other space's: its intent, its local copy and its reads survive. The
// local file goes with the last intent.
func TestPlaintextSpaces_ReleaseInOneSpaceKeepsTheOther(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	space2 := secondPlaintextBucket(t, b, mem)
	ctx := context.Background()
	data := testBody(8 << 10)

	putIn(t, b, "bk", "k", data)
	putIn(t, b, "bk2", "k", data)
	d := blobDigestOf(t, b, "k", "")
	if !localCopyExists(b, d) {
		t.Fatalf("no local copy after the writes")
	}

	deleteFrom(t, b, "bk", "k")
	drainReleases(t, b)

	if _, err := mem.GetIntent(ctx, did.Undef, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("the deleted space's intent survived its release (err=%v)", err)
	}
	in, err := mem.GetIntent(ctx, space2, d)
	if err != nil || in.State != registry.IntentPublished {
		t.Fatalf("the other space's intent after the release = %+v, %v; want published", in, err)
	}
	if !localCopyExists(b, d) {
		t.Fatalf("the release removed the local copy the other space's intent still names")
	}
	if got := getFrom(t, b, "bk2", "k"); !bytes.Equal(got, data) {
		t.Fatalf("bk2/k no longer reads back after bk/k was deleted")
	}
	if n := rm.removedDigests()[string(d)]; n != 1 {
		t.Fatalf("network removes of %x = %d, want 1 (the deleted space's)", d, n)
	}

	deleteFrom(t, b, "bk2", "k")
	drainReleases(t, b)
	if _, err := mem.GetIntent(ctx, space2, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("the last intent survived its release (err=%v)", err)
	}
	if localCopyExists(b, d) {
		t.Fatalf("the local copy survived the last intent")
	}
}

// TestPlaintextSpaces_ReleaseInOneSpaceKeepsTheOthersPark: a release in one
// space leaves the other space's park row alone, so the other space's
// Complete still concludes its own allocation.
func TestPlaintextSpaces_ReleaseInOneSpaceKeepsTheOthersPark(t *testing.T) {
	pu := &parkingUploader{}
	// Single-shot PUTs upload and accept; only part blobs park.
	b, mem := newDeferredBackend(t, pu, func(d *Deps) { d.Uploader = inmem.NopUploader{} })
	space2 := secondPlaintextBucket(t, b, mem)
	ctx := context.Background()
	data := testBody(5 << 20)

	putIn(t, b, "bk", "k", data)
	d := blobDigestOf(t, b, "k", "")
	id, parts := uploadOnePart(t, b, "bk2", "k", data)
	if pd := hygienePartDigests(t, mem, id, 1)[0]; !bytes.Equal(pd, d) {
		t.Fatalf("the part and the object stored different digests for the same bytes")
	}
	if _, err := mem.GetPark(ctx, space2, d); err != nil {
		t.Fatalf("bk2's part is not parked: %v", err)
	}

	deleteFrom(t, b, "bk", "k")
	drainReleases(t, b)

	if _, err := mem.GetPark(ctx, space2, d); err != nil {
		t.Fatalf("bk's release took bk2's park: %v", err)
	}
	if pu.abortedDigests()[string(d)] {
		t.Fatalf("bk's release aborted bk2's allocation on the provider")
	}
	completeIn(t, b, "bk2", "k", id, parts)
	if got := getFrom(t, b, "bk2", "k"); !bytes.Equal(got, data) {
		t.Fatalf("bk2/k does not read back after bk/k was deleted")
	}
}

// TestPlaintextSpaces_SameSpaceRewriteMakesTheReleaseStale: within one
// space, identical bytes written again while the deleted twin's release is
// pending re-claim the digest; the release finds the claim and drops its
// record, and the new object keeps its blob.
func TestPlaintextSpaces_SameSpaceRewriteMakesTheReleaseStale(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	putIn(t, b, "bk", "old", data)
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	putIn(t, b, "bk", "new", data)
	drainReleases(t, b)

	if pending, _ := mem.ListReleasesBySpace(ctx, did.Undef); len(pending) != 0 {
		t.Fatalf("release records after the drain = %v, want none (stale, dropped)", pending)
	}
	if n := rm.removedDigests()[string(d)]; n != 0 {
		t.Fatalf("the re-claimed blob was removed from the network %d times", n)
	}
	in, err := mem.GetIntent(ctx, did.Undef, d)
	if err != nil || in.State != registry.IntentPublished {
		t.Fatalf("intent after the rewrite = %+v, %v; want published", in, err)
	}
	if got := getFrom(t, b, "bk", "new"); !bytes.Equal(got, data) {
		t.Fatalf("the rewritten object does not read back")
	}
}

// Within one space, a write of bytes whose previous object was deleted meets
// that object's release. The write settles who owns the digest before it
// reuses the located blob (reuseLocated).

// TestPlaintextRelease_WriteCancelsAPendingRelease: the record is not yet
// started, so the write cancels it and reuses the blob without uploading.
func TestPlaintextRelease_WriteCancelsAPendingRelease(t *testing.T) {
	up := &countingUploader{calls: map[string]int{}}
	b, mem := newDeferredBackend(t, up)
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	putIn(t, b, "bk", "old", data)
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	if pending, _ := mem.ListReleasesBySpace(ctx, did.Undef); len(pending) != 1 {
		t.Fatalf("release records after the delete = %d, want 1", len(pending))
	}
	putIn(t, b, "bk", "new", data)

	if pending, _ := mem.ListReleasesBySpace(ctx, did.Undef); len(pending) != 0 {
		t.Fatalf("the write left the release record in place: %v", pending)
	}
	up.mu.Lock()
	uploads := up.calls[string(d)]
	up.mu.Unlock()
	if uploads != 1 {
		t.Fatalf("uploads of %x = %d, want 1 (the located blob was reused)", d, uploads)
	}
	drainReleases(t, b)
	if got := getFrom(t, b, "bk", "new"); !bytes.Equal(got, data) {
		t.Fatalf("the rewritten object does not read back")
	}
}

// TestPlaintextRelease_WriteWaitsOutAStartedRelease: the release has taken
// the blob, so the write waits until it has run, keeps its own spool file
// and intent through it, and then uploads afresh.
func TestPlaintextRelease_WriteWaitsOutAStartedRelease(t *testing.T) {
	up := &countingUploader{calls: map[string]int{}}
	b, mem := newDeferredBackend(t, up)
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	putIn(t, b, "bk", "old", data)
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	if s, err := mem.StartRelease(ctx, did.Undef, d); err != nil || s != registry.ReleaseStarted {
		t.Fatalf("StartRelease = %v, %v", s, err)
	}

	done := make(chan error, 1)
	go func() {
		bucket, key := "bk", "new"
		_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data)})
		done <- err
	}()
	// The write is waiting: nothing has finished, and its spool file is in
	// place for the upload that follows.
	select {
	case err := <-done:
		t.Fatalf("PutObject returned %v while the release was started", err)
	case <-time.After(100 * time.Millisecond):
	}
	if !localCopyExists(b, d) {
		t.Fatalf("no spool file while the write waits")
	}

	// The release runs: it removes the blob from the provider and drops its
	// record, and leaves the waiting write's intent and file alone.
	drainReleases(t, b)
	if err := <-done; err != nil {
		t.Fatalf("PutObject after the release: %v", err)
	}
	up.mu.Lock()
	uploads := up.calls[string(d)]
	up.mu.Unlock()
	if uploads != 2 {
		t.Fatalf("uploads of %x = %d, want 2 (the blob was uploaded again after its release)", d, uploads)
	}
	if _, err := mem.GetLocation(ctx, did.Undef, d); err != nil {
		t.Fatalf("no location after the fresh upload: %v", err)
	}
	in, err := mem.GetIntent(ctx, did.Undef, d)
	if err != nil || in.State != registry.IntentPublished {
		t.Fatalf("intent after the rewrite = %+v, %v; want published", in, err)
	}
	if got := getFrom(t, b, "bk", "new"); !bytes.Equal(got, data) {
		t.Fatalf("the rewritten object does not read back")
	}
}

// TestPlaintextRelease_WaitIsBounded: a release that never finishes (it
// failed after starting and the sweeper has not retried it) holds the write
// only so long; then the write is refused with SlowDown for the client to
// retry.
func TestPlaintextRelease_WaitIsBounded(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)
	putIn(t, b, "bk", "old", data)
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	if s, err := mem.StartRelease(ctx, did.Undef, d); err != nil || s != registry.ReleaseStarted {
		t.Fatalf("StartRelease = %v, %v", s, err)
	}
	limit := reuseWaitLimit
	reuseWaitLimit = 50 * time.Millisecond
	t.Cleanup(func() { reuseWaitLimit = limit })

	bucket, key := "bk", "new"
	_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data)})
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "SlowDown" {
		t.Fatalf("PutObject during a release that never finishes: err = %v, want SlowDown", err)
	}
}

// TestPlaintextSpaces_ReleaseIgnoresAnotherSpacesHold: a hold says whose
// write needs the file. A release in one space meeting the other space's
// hold deletes its own intent and record and leaves the file; it does not
// mistake the hold for a write of its own waiting on it.
func TestPlaintextSpaces_ReleaseIgnoresAnotherSpacesHold(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	space2 := secondPlaintextBucket(t, b, mem)
	ctx := context.Background()
	data := testBody(8 << 10)

	putIn(t, b, "bk", "k", data)
	d := blobDigestOf(t, b, "k", "")
	// A write in bk2 is between spooling and uploading the same bytes.
	unhold := b.spool.Hold(space2, d)
	defer unhold()

	deleteFrom(t, b, "bk", "k")
	drainReleases(t, b)

	if _, err := mem.GetIntent(ctx, did.Undef, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("the releasing space's intent survived behind another space's hold (err=%v)", err)
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, did.Undef); len(pending) != 0 {
		t.Fatalf("release record survived: %v", pending)
	}
	if !localCopyExists(b, d) {
		t.Fatalf("the release removed the file the other space's write holds")
	}
}
