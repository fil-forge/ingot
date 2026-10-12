package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
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

// TestPlaintextSpaces_CacheMoveWaitsForAnotherWritesHold: a write that
// finishes while another write still holds the same bytes leaves the file
// in the spool, where the other write uploads from; the next write's cache
// move takes it once the hold has ended.
func TestPlaintextSpaces_CacheMoveWaitsForAnotherWritesHold(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	space2 := secondPlaintextBucket(t, b, mem)
	data := testBody(8 << 10)
	d := digestOf(t, data)

	// A write in bk2 is between spooling and uploading the same bytes.
	unhold := b.spool.Hold(space2, d)
	putIn(t, b, "bk", "one", data)
	if _, err := os.Stat(b.spool.Path(d)); err != nil {
		t.Fatalf("the finished write moved the file the other write holds: %v", err)
	}
	unhold()
	putIn(t, b, "bk", "two", data)
	_, spoolErr := os.Stat(b.spool.Path(d))
	_, cacheErr := os.Stat(b.cache.Path(d))
	if spoolErr == nil || cacheErr != nil {
		t.Fatalf("after the hold ended: spool err=%v cache err=%v, want the file moved to the cache", spoolErr, cacheErr)
	}
}

// TestPlaintextRelease_WriteWaitsOutAStartedReleaseWithNoLocation: a
// release that has already dropped the blob's location, or one of a blob
// that never had one, is settled before the write uploads afresh, so the
// release cannot remove what the write adds.
func TestPlaintextRelease_WriteWaitsOutAStartedReleaseWithNoLocation(t *testing.T) {
	up := &countingUploader{calls: map[string]int{}}
	b, mem := newDeferredBackend(t, up)
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	putIn(t, b, "bk", "old", data)
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	// The release is started and past its location delete.
	if s, err := mem.StartRelease(ctx, did.Undef, d); err != nil || s != registry.ReleaseStarted {
		t.Fatalf("StartRelease = %v, %v", s, err)
	}
	if err := mem.DeleteLocation(ctx, did.Undef, d); err != nil {
		t.Fatalf("DeleteLocation: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		bucket, key := "bk", "new"
		_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data)})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("PutObject returned %v while the release was started", err)
	case <-time.After(100 * time.Millisecond):
	}
	up.mu.Lock()
	early := up.calls[string(d)]
	up.mu.Unlock()
	if early != 1 {
		t.Fatalf("uploads of %x while the release was started = %d, want 1 (the write waited)", d, early)
	}

	drainReleases(t, b)
	if err := <-done; err != nil {
		t.Fatalf("PutObject after the release: %v", err)
	}
	up.mu.Lock()
	uploads := up.calls[string(d)]
	up.mu.Unlock()
	if uploads != 2 {
		t.Fatalf("uploads of %x = %d, want 2", d, uploads)
	}
	if got := getFrom(t, b, "bk", "new"); !bytes.Equal(got, data) {
		t.Fatalf("the rewritten object does not read back")
	}
}

// startAfterSettle is a release store that starts a release of the digest
// right after a write has settled it, as a sweep that runs between the
// write's settle and its commit does.
type startAfterSettle struct {
	registry.PendingReleaseStore
	digest multihash.Multihash
	space  did.DID
	armed  bool
}

func (s *startAfterSettle) CancelRelease(ctx context.Context, space did.DID, digest multihash.Multihash) (registry.ReleaseCancel, error) {
	c, err := s.PendingReleaseStore.CancelRelease(ctx, space, digest)
	if err == nil && s.armed && bytes.Equal(digest, s.digest) {
		s.armed = false
		if err := s.PendingReleaseStore.EnqueueRelease(ctx, s.space, s.digest, time.Now()); err != nil {
			return 0, err
		}
		if _, err := s.PendingReleaseStore.StartRelease(ctx, s.space, s.digest); err != nil {
			return 0, err
		}
	}
	return c, err
}

// TestPlaintextRelease_ClaimRefusedDuringAStartedRelease: a release that
// starts after the write settled it and before its commit refuses the
// claim, so the write fails with SlowDown rather than commit a manifest
// over a blob the release is removing. The body has two blobs and the
// release is of the second: the first blob's claim does not land either,
// since no generation would ever drop it.
func TestPlaintextRelease_ClaimRefusedDuringAStartedRelease(t *testing.T) {
	rel := &startAfterSettle{}
	b, mem := newDeferredBackend(t, inmem.NopUploader{}, func(d *Deps) {
		rel.PendingReleaseStore = d.PendingReleases
		d.PendingReleases = rel
		d.MaxBlobSize = 4 << 10
	})
	setNone(t, b)
	data := testBody(8 << 10)
	first, second := digestOf(t, data[:4<<10]), digestOf(t, data[4<<10:])
	rel.digest, rel.space, rel.armed = second, did.Undef, true

	bucket, key := "bk", "k"
	_, err := b.PutObject(context.Background(), s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data)})
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "SlowDown" {
		t.Fatalf("PutObject with a release started before its commit: err = %v, want SlowDown", err)
	}
	for _, d := range []multihash.Multihash{first, second} {
		if n, _ := mem.CountClaims(context.Background(), did.Undef, d); n != 0 {
			t.Fatalf("a claim on %x landed although the commit was refused: %d", d, n)
		}
	}
}

// TestPlaintextRelease_StreamedWriteWaitsOutAStartedRelease: a blob sent
// to its provider as it was spooled settles the release at its conclude.
// After a waited-out release the stream is given up, since the release may
// have taken what it parked, and the blob is uploaded again from the spool
// file the write still holds.
func TestPlaintextRelease_StreamedWriteWaitsOutAStartedRelease(t *testing.T) {
	su := newStreamingUploader()
	b, mem := newStreamingBackend(t, su)
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	if err := putSized(t, b, "old", data, int64(len(data))); err != nil {
		t.Fatalf("PutObject old: %v", err)
	}
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	if s, err := mem.StartRelease(ctx, did.Undef, d); err != nil || s != registry.ReleaseStarted {
		t.Fatalf("StartRelease = %v, %v", s, err)
	}

	done := make(chan error, 1)
	go func() { done <- putSized(t, b, "new", data, int64(len(data))) }()
	select {
	case err := <-done:
		t.Fatalf("PutObject returned %v while the release was started", err)
	case <-time.After(100 * time.Millisecond):
	}
	drainReleases(t, b)
	if err := <-done; err != nil {
		t.Fatalf("PutObject after the release: %v", err)
	}

	su.mu.Lock()
	var uploaded, aborted int
	for _, u := range su.uploaded {
		if bytes.Equal(u, d) {
			uploaded++
		}
	}
	aborted = len(su.aborted)
	su.mu.Unlock()
	if uploaded != 1 || aborted != 1 {
		t.Fatalf("after the waited-out release: fresh uploads of %x = %d, streams aborted = %d; want 1 and 1", d, uploaded, aborted)
	}
	if _, err := mem.GetLocation(ctx, did.Undef, d); err != nil {
		t.Fatalf("no location after the fresh upload: %v", err)
	}
	if got := getFrom(t, b, "bk", "new"); !bytes.Equal(got, data) {
		t.Fatalf("the rewritten object does not read back")
	}
}

// TestPlaintextSpaces_ConcurrentWritesOfTheSameBytesUploadOnce: two writes
// of the same bytes to one space at the same time take turns at the upload
// (blobKeyLock): the second finds the first's location and reuses it, so
// the provider is asked for the bytes once.
func TestPlaintextSpaces_ConcurrentWritesOfTheSameBytesUploadOnce(t *testing.T) {
	up := &countingUploader{calls: map[string]int{}}
	b, _ := newDeferredBackend(t, up)
	setNone(t, b)
	data := testBody(8 << 10)
	d := digestOf(t, data)

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := range 4 {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			bucket := "bk"
			_, err := b.PutObject(context.Background(), s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data)})
			errs <- err
		}(fmt.Sprintf("k%d", i))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("PutObject: %v", err)
		}
	}
	up.mu.Lock()
	uploads := up.calls[string(d)]
	up.mu.Unlock()
	if uploads != 1 {
		t.Fatalf("uploads of %x by four concurrent writes = %d, want 1", d, uploads)
	}
	for i := range 4 {
		if got := getFrom(t, b, "bk", fmt.Sprintf("k%d", i)); !bytes.Equal(got, data) {
			t.Fatalf("k%d does not read back", i)
		}
	}
}

// TestPlaintextSpaces_ConcurrentPartsOfTheSameBytesParkOnce: two part
// uploads of the same bytes to one space at the same time park once; the
// second finds the first's park. The parking fake accepts outright, so the
// count is of uploads.
func TestPlaintextSpaces_ConcurrentPartsOfTheSameBytesParkOnce(t *testing.T) {
	up := &countingUploader{calls: map[string]int{}}
	b, mem := newDeferredBackend(t, up)
	setNone(t, b)
	part := testBody(5 << 20)
	d := digestOf(t, part)

	var wg sync.WaitGroup
	ids := make(chan string, 3)
	for i := range 3 {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			id, _ := uploadOnePart(t, b, "bk", key, part)
			ids <- id
		}(fmt.Sprintf("mp%d", i))
	}
	wg.Wait()
	close(ids)
	for id := range ids {
		if got := hygienePartDigests(t, mem, id, 1)[0]; !bytes.Equal(got, d) {
			t.Fatalf("part of %s stored under %x, want %x", id, got, d)
		}
	}
	up.mu.Lock()
	uploads := up.calls[string(d)]
	up.mu.Unlock()
	if uploads != 1 {
		t.Fatalf("uploads of %x by three concurrent part uploads = %d, want 1", d, uploads)
	}
}

// staleThenReplaced is a release store that, when a start finds a record
// stale, enqueues a replacement for the digest right after, as a drop of the
// new claim that lands between the stale decision and a delete by key would.
type staleThenReplaced struct {
	registry.PendingReleaseStore
	replaced int
}

func (s *staleThenReplaced) StartRelease(ctx context.Context, space did.DID, digest multihash.Multihash) (registry.ReleaseStart, error) {
	r, err := s.PendingReleaseStore.StartRelease(ctx, space, digest)
	if err == nil && r == registry.ReleaseClaimed {
		s.replaced++
		if err := s.PendingReleaseStore.EnqueueRelease(ctx, space, digest, time.Now().Add(time.Hour)); err != nil {
			return 0, err
		}
	}
	return r, err
}

// TestPlaintextRelease_StaleRecordDoesNotTakeAReplacement: a record found
// stale is deleted by the start itself, under the lock; a replacement
// enqueued right after survives the sweep, since nothing deletes by key.
func TestPlaintextRelease_StaleRecordDoesNotTakeAReplacement(t *testing.T) {
	rel := &staleThenReplaced{}
	b, mem := newDeferredBackend(t, inmem.NopUploader{}, func(d *Deps) {
		rel.PendingReleaseStore = d.PendingReleases
		d.PendingReleases = rel
	})
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)
	putIn(t, b, "bk", "k", data)
	d := blobDigestOf(t, b, "k", "")
	// A record for a claimed digest: stale from the start.
	if err := mem.EnqueueRelease(ctx, did.Undef, d, time.Now()); err != nil {
		t.Fatalf("EnqueueRelease: %v", err)
	}

	drainReleases(t, b)

	if rel.replaced != 1 {
		t.Fatalf("stale starts = %d, want 1", rel.replaced)
	}
	pending, _ := mem.ListReleasesBySpace(ctx, did.Undef)
	if len(pending) != 1 {
		t.Fatalf("records after the sweep = %v, want the replacement alone", pending)
	}
}

// otherSpaceDeletesFirst is a release store under which, as space A's
// release deletes its intent, space B's release has just deleted its own:
// both had seen the other's intent and left the shared file.
type otherSpaceDeletesFirst struct {
	registry.PendingReleaseStore
	first, second did.DID
	done          bool
}

func (o *otherSpaceDeletesFirst) DeleteIntentAndRelease(ctx context.Context, space did.DID, digest multihash.Multihash) (int, error) {
	if space == o.first && !o.done {
		o.done = true
		if _, err := o.PendingReleaseStore.DeleteIntentAndRelease(ctx, o.second, digest); err != nil {
			return 0, err
		}
	}
	return o.PendingReleaseStore.DeleteIntentAndRelease(ctx, space, digest)
}

// TestPlaintextSpaces_LastReleaseOutRemovesTheSharedFile: two spaces
// releasing the same digest at once each see the other's intent and leave
// the file; the one whose delete finds no intent left removes it.
func TestPlaintextSpaces_LastReleaseOutRemovesTheSharedFile(t *testing.T) {
	rel := &otherSpaceDeletesFirst{}
	b, mem, _ := newRefTestBackend(t)
	rel.PendingReleaseStore = b.pendingReleases
	b.pendingReleases = rel
	space2 := secondPlaintextBucket(t, b, mem)
	rel.first, rel.second = did.Undef, space2
	ctx := context.Background()
	data := testBody(8 << 10)

	putIn(t, b, "bk", "k", data)
	putIn(t, b, "bk2", "k", data)
	d := blobDigestOf(t, b, "k", "")
	deleteFrom(t, b, "bk", "k")
	deleteFrom(t, b, "bk2", "k")
	drainReleases(t, b)

	for _, sp := range []did.DID{did.Undef, space2} {
		if _, err := mem.GetIntent(ctx, sp, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("intent in %s survived (err=%v)", sp, err)
		}
	}
	if localCopyExists(b, d) {
		t.Fatalf("the shared file survived both releases")
	}
}
