package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
)

// A blob stored as received has the digest of its bytes, so the same bytes
// written to two buckets of different spaces share a digest: one cached
// copy, and an upload, a park, a claim and a release per space. These tests
// write identical bytes to "bk" (space mem.bk()) and a second bucket of its
// own space, and check that one space's lifecycle never takes the other's
// rows or copy.

// secondPlaintextBucket creates "bk2" in a space of its own and sets both
// buckets to store as received. Returns bk2's space.
func secondPlaintextBucket(t *testing.T, b *Backend, mem *testStore) did.DID {
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
	if _, err := b.PutObject(context.Background(), s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)}); err != nil {
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
	out, err := b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &res.UploadId, PartNumber: &n, Body: bytes.NewReader(part), ContentLength: byteLen(part)})
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

// localCopyExists reports whether the blob is cached.
func localCopyExists(b *Backend, d multihash.Multihash) bool {
	_, err := os.Stat(b.cache.Path(d))
	return err == nil
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
	for _, space := range []did.DID{mem.bk(), space2} {
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

	if _, err := mem.GetIntent(ctx, mem.bk(), d); !errors.Is(err, registry.ErrNotFound) {
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
	b, mem := newDeferredBackend(t, pu)
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
// space, identical bytes written again while the deleted twin's release
// waits out its reader grace re-claim the digest; the release finds the
// claim and does nothing, and the new object keeps its blob.
func TestPlaintextSpaces_SameSpaceRewriteMakesTheReleaseStale(t *testing.T) {
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) {
		d.Remover = rm
		d.ReleaseGrace = time.Hour
	})
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	putIn(t, b, "bk", "old", data)
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	putIn(t, b, "bk", "new", data)
	if err := b.blobs.DrainSpace(ctx, mem.bk()); err != nil {
		t.Fatalf("run the space's releases: %v", err)
	}

	if pending, _ := mem.ListReleasesBySpace(ctx, mem.bk()); len(pending) != 0 {
		t.Fatalf("release records after the drain = %v, want none (stale, dropped)", pending)
	}
	if n := rm.removedDigests()[string(d)]; n != 0 {
		t.Fatalf("the re-claimed blob was removed from the network %d times", n)
	}
	in, err := mem.GetIntent(ctx, mem.bk(), d)
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

// TestPlaintextRelease_WriteCancelsAPendingRelease: the release is not yet
// started, so the write cancels it, and uploads the blob afresh rather than
// reusing it, since a cancelled release may already have removed it.
func TestPlaintextRelease_WriteCancelsAPendingRelease(t *testing.T) {
	up := &countingUploader{calls: map[string]int{}}
	b, mem := newDeferredBackend(t, up)
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	putIn(t, b, "bk", "old", data)
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	if pending, _ := mem.ListReleasesBySpace(ctx, mem.bk()); len(pending) != 1 {
		t.Fatalf("release records after the delete = %d, want 1", len(pending))
	}
	putIn(t, b, "bk", "new", data)

	if pending, _ := mem.ListReleasesBySpace(ctx, mem.bk()); len(pending) != 0 {
		t.Fatalf("the write left the release record in place: %v", pending)
	}
	if sent, accepted, _ := streamCounts(t, b, d); sent != 2 || accepted != 2 {
		t.Fatalf("streams of %x: %d sent, %d accepted; want 2 and 2 (uploaded afresh after the cancel)", d, sent, accepted)
	}
	drainReleases(t, b)
	if got := getFrom(t, b, "bk", "new"); !bytes.Equal(got, data) {
		t.Fatalf("the rewritten object does not read back")
	}
}

// TestPlaintextRelease_WriteWaitsOutAStartedRelease: the release has taken
// the blob, so the write waits until it has run, keeps its own local copy
// and intent through it, and then concludes its own stream.
func TestPlaintextRelease_WriteWaitsOutAStartedRelease(t *testing.T) {
	up := &countingUploader{calls: map[string]int{}}
	gate := newGatedRemover()
	b, mem := newDeferredBackend(t, up, withGate(gate))
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	putIn(t, b, "bk", "old", data)
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	finish := holdRelease(t, b, gate)

	done := make(chan error, 1)
	go func() {
		bucket, key := "bk", "new"
		_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
		done <- err
	}()
	// The write is waiting: nothing has finished, and its local copy is still
	// in progress, for the cache once the write is accepted.
	select {
	case err := <-done:
		t.Fatalf("PutObject returned %v while the release was started", err)
	case <-time.After(100 * time.Millisecond):
	}
	if b.cache.InFlight() == 0 {
		t.Fatalf("no local copy in progress while the write waits")
	}

	// The release finishes: it removes the blob from the provider, and leaves
	// the waiting write's intent alone.
	finish()
	if err := <-done; err != nil {
		t.Fatalf("PutObject after the release: %v", err)
	}
	if sent, accepted, _ := streamCounts(t, b, d); sent != 2 || accepted != 2 {
		t.Fatalf("streams of %x: %d sent, %d accepted; want 2 and 2 (the blob was uploaded again after its release)", d, sent, accepted)
	}
	if _, err := mem.GetLocation(ctx, mem.bk(), d); err != nil {
		t.Fatalf("no location after the fresh upload: %v", err)
	}
	in, err := mem.GetIntent(ctx, mem.bk(), d)
	if err != nil || in.State != registry.IntentPublished {
		t.Fatalf("intent after the rewrite = %+v, %v; want published", in, err)
	}
	if got := getFrom(t, b, "bk", "new"); !bytes.Equal(got, data) {
		t.Fatalf("the rewritten object does not read back")
	}
}

// TestPlaintextRelease_WaitIsBounded: a release that does not finish holds the
// write only so long; then the write is refused with SlowDown for the client
// to retry.
func TestPlaintextRelease_WaitIsBounded(t *testing.T) {
	gate := newGatedRemover()
	b, _ := newDeferredBackend(t, testfakes.NopUploader{}, withGate(gate),
		withBlobs(func(d *spaceshardqueue.Deps) { d.AddWait = 50 * time.Millisecond }))
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)
	putIn(t, b, "bk", "old", data)
	deleteFrom(t, b, "bk", "old")
	holdRelease(t, b, gate)

	bucket, key := "bk", "new"
	_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "SlowDown" {
		t.Fatalf("PutObject during a release that never finishes: err = %v, want SlowDown", err)
	}
}

// TestPlaintextRelease_WriteWaitsOutAStartedReleaseWithNoLocation: a
// release that has already dropped the blob's location, or one of a blob
// that never had one, is settled before the write uploads afresh, so the
// release cannot remove what the write adds.
func TestPlaintextRelease_WriteWaitsOutAStartedReleaseWithNoLocation(t *testing.T) {
	up := &countingUploader{calls: map[string]int{}}
	gate := newGatedRemover()
	b, mem := newDeferredBackend(t, up, withGate(gate))
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	putIn(t, b, "bk", "old", data)
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	// The release is under way, and the blob's location already gone.
	finish := holdRelease(t, b, gate)
	if err := mem.DeleteLocation(ctx, mem.bk(), d); err != nil {
		t.Fatalf("DeleteLocation: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		bucket, key := "bk", "new"
		_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("PutObject returned %v while the release was started", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, accepted, _ := streamCounts(t, b, d); accepted != 1 {
		t.Fatalf("streams of %x accepted while the release was started = %d, want 1 (the write waited)", d, accepted)
	}

	finish()
	if err := <-done; err != nil {
		t.Fatalf("PutObject after the release: %v", err)
	}
	if sent, accepted, _ := streamCounts(t, b, d); sent != 2 || accepted != 2 {
		t.Fatalf("streams of %x: %d sent, %d accepted; want 2 and 2", d, sent, accepted)
	}
	if got := getFrom(t, b, "bk", "new"); !bytes.Equal(got, data) {
		t.Fatalf("the rewritten object does not read back")
	}
}

// releaseBeforeCommit is a registry that, before the first commit of the
// write, starts a release of digest and holds it in its network remove, as a
// release that runs between the write's settle and its commit does.
type releaseBeforeCommit struct {
	registry.Registry
	start func()
}

func (r *releaseBeforeCommit) RootCommitWrites(ctx context.Context, name string, expect, next cid.Cid, c registry.RootCommit, gc []cid.Cid) (registry.CommitWrites, error) {
	if r.start != nil {
		start := r.start
		r.start = nil
		start()
	}
	return r.Registry.RootCommitWrites(ctx, name, expect, next, c, gc)
}

// TestPlaintextRelease_StreamedWriteWaitsOutAStartedRelease: a streamed blob
// settles the release at its conclude. The space takes the digest again once
// the release's removal has its receipt, so after a waited-out release the
// write concludes its own stream.
func TestPlaintextRelease_StreamedWriteWaitsOutAStartedRelease(t *testing.T) {
	su := newStreamingUploader()
	gate := newGatedRemover()
	b, mem := newStreamingBackend(t, su, withGate(gate))
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	if err := putSized(t, b, "old", data, int64(len(data))); err != nil {
		t.Fatalf("PutObject old: %v", err)
	}
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	finish := holdRelease(t, b, gate)

	done := make(chan error, 1)
	go func() { done <- putSized(t, b, "new", data, int64(len(data))) }()
	select {
	case err := <-done:
		t.Fatalf("PutObject returned %v while the release was started", err)
	case <-time.After(100 * time.Millisecond):
	}
	finish()
	if err := <-done; err != nil {
		t.Fatalf("PutObject after the release: %v", err)
	}

	su.mu.Lock()
	var concluded int
	for _, c := range su.concluded {
		if bytes.Equal(c.Digest, d) {
			concluded++
		}
	}
	aborted := len(su.aborted)
	su.mu.Unlock()
	if aborted != 0 || concluded != 2 {
		t.Fatalf("after the waited-out release: streams of %x aborted = %d, concluded = %d; want 0 and 2", d, aborted, concluded)
	}
	if _, err := mem.GetLocation(ctx, mem.bk(), d); err != nil {
		t.Fatalf("no location after the fresh upload: %v", err)
	}
	if got := getFrom(t, b, "bk", "new"); !bytes.Equal(got, data) {
		t.Fatalf("the rewritten object does not read back")
	}
}

// TestPlaintextSpaces_ConcurrentWritesOfTheSameBytesAcceptOnce: writes of the
// same bytes to one space at the same time each stream them, then take turns
// at the conclude (blobKeyLock): the first is accepted, and each later one
// finds its location, reuses it and gives up its own allocation.
func TestPlaintextSpaces_ConcurrentWritesOfTheSameBytesAcceptOnce(t *testing.T) {
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
			_, err := b.PutObject(context.Background(), s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
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
	if sent, accepted, aborted := streamCounts(t, b, d); sent != 4 || accepted != 1 || aborted != 3 {
		t.Fatalf("streams of %x by four concurrent writes: %d sent, %d accepted, %d aborted; want 4, 1, 3", d, sent, accepted, aborted)
	}
	for i := range 4 {
		if got := getFrom(t, b, "bk", fmt.Sprintf("k%d", i)); !bytes.Equal(got, data) {
			t.Fatalf("k%d does not read back", i)
		}
	}
}

// TestPlaintextSpaces_ConcurrentPartsOfTheSameBytesParkOnce: part uploads of
// the same bytes to one space at the same time each stream them, and park
// once: each later one finds the first's park and gives up its own
// allocation.
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
	if sent, _, aborted := streamCounts(t, b, d); sent != 3 || aborted != 2 {
		t.Fatalf("streams of %x by three concurrent part uploads: %d sent, %d aborted; want 3 and 2", d, sent, aborted)
	}
	if _, err := mem.ParkByDigest(context.Background(), d); err != nil {
		t.Fatalf("the shared park: %v", err)
	}
}

// TestPlaintextRelease_StaleReleaseRemovesNothing: a release of a digest
// claimed again since it was queued finds the claim and does nothing.
func TestPlaintextRelease_StaleReleaseRemovesNothing(t *testing.T) {
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) { d.Remover = rm })
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)
	putIn(t, b, "bk", "k", data)
	d := blobDigestOf(t, b, "k", "")
	if err := mem.EnqueueRelease(ctx, mem.bk(), d, registry.DueNow()); err != nil {
		t.Fatalf("EnqueueRelease: %v", err)
	}

	if n, err := b.SweepPendingReleases(ctx); err != nil || n != 0 {
		t.Fatalf("SweepPendingReleases = %d, %v; want nothing released", n, err)
	}
	if rm.removedDigests()[string(d)] > 0 {
		t.Fatalf("the stale release removed %x from the network", d)
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, mem.bk()); len(pending) != 0 {
		t.Fatalf("releases after the sweep = %v, want none", pending)
	}
	if got := getFrom(t, b, "bk", "k"); !bytes.Equal(got, data) {
		t.Fatalf("the object does not read back")
	}
}

// TestPlaintextSpaces_LastReleaseOutRemovesTheSharedFile: two spaces release
// the same digest; the first sees the other's intent and leaves the file, and
// the last out removes it.
func TestPlaintextSpaces_LastReleaseOutRemovesTheSharedFile(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	space2 := secondPlaintextBucket(t, b, mem)
	ctx := context.Background()
	data := testBody(8 << 10)

	putIn(t, b, "bk", "k", data)
	putIn(t, b, "bk2", "k", data)
	d := blobDigestOf(t, b, "k", "")
	deleteFrom(t, b, "bk", "k")
	deleteFrom(t, b, "bk2", "k")
	drainReleases(t, b)

	for _, sp := range []did.DID{mem.bk(), space2} {
		if _, err := mem.GetIntent(ctx, sp, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("intent in %s survived (err=%v)", sp, err)
		}
	}
	if localCopyExists(b, d) {
		t.Fatalf("the shared file survived both releases")
	}
}

// TestPlaintextRelease_ReleaseBeforeTheCommitIsStale: a release of the
// write's blob that runs after its add and before its commit finds the
// write's reference and does nothing, so the write commits over a blob its
// provider still holds.
func TestPlaintextRelease_ReleaseBeforeTheCommitIsStale(t *testing.T) {
	rm := &recordingRemover{}
	reg := &releaseBeforeCommit{}
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) {
		reg.Registry = d.Registry
		d.Registry = reg
		d.Remover = rm
	})
	setNone(t, b)
	data := testBody(8 << 10)
	d := digestOf(t, data)
	reg.start = func() {
		if err := mem.EnqueueRelease(context.Background(), mem.bk(), d, registry.DueNow()); err != nil {
			t.Fatalf("EnqueueRelease: %v", err)
		}
		drainReleases(t, b)
	}

	putIn(t, b, "bk", "k", data)
	if reg.start != nil {
		t.Fatal("the release never ran")
	}
	if n := rm.removedDigests()[string(d)]; n != 0 {
		t.Fatalf("the referenced blob was removed from the network %d times", n)
	}
	if got := getFrom(t, b, "bk", "k"); !bytes.Equal(got, data) {
		t.Fatalf("the object does not read back")
	}
}

// TestPlaintextRelease_ExpiredReferenceRefusesTheCommit: a write whose
// reference to one of its blobs expired before its commit, its blob released,
// fails with SlowDown for the client to retry, and none of its claims land.
func TestPlaintextRelease_ExpiredReferenceRefusesTheCommit(t *testing.T) {
	rm := &recordingRemover{}
	reg := &releaseBeforeCommit{}
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) {
		reg.Registry = d.Registry
		d.Registry = reg
		d.Remover = rm
		d.MaxBlobSize = 4 << 10
	})
	setNone(t, b)
	data := testBody(8 << 10)
	first, second := digestOf(t, data[:4<<10]), digestOf(t, data[4<<10:])
	reg.start = func() {
		expireRefsTo(t, b, mem.bk(), second)
	}

	bucket, key := "bk", "k"
	_, err := b.PutObject(context.Background(), s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "SlowDown" {
		t.Fatalf("PutObject whose reference expired before its commit: err = %v, want SlowDown", err)
	}
	if n := rm.removedDigests()[string(second)]; n != 1 {
		t.Fatalf("the expired reference's blob was removed %d times, want 1", n)
	}
	n, _ := mem.CountReferences(context.Background(), mem.bk(), first, uuid.Nil)
	if n != 1 {
		t.Fatalf("references to the first blob = %d, want only the write's pending one", n)
	}
	if claims, _ := mem.CountClaimRows(context.Background(), mem.bk(), first); claims != 0 {
		t.Fatalf("a claim on the first blob landed although the commit was refused: %d", claims)
	}
}

// TestPlaintextRelease_ConcurrentStreamedWritesConcludeOnce: two streamed
// writes of the same bytes to one space take turns at the conclude; the
// second finds the first's location, gives up its stream and reuses the
// blob, so the provider holds one allocation.
func TestPlaintextRelease_ConcurrentStreamedWritesConcludeOnce(t *testing.T) {
	su := newStreamingUploader()
	b, _ := newStreamingBackend(t, su)
	setNone(t, b)
	data := testBody(8 << 10)

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for i := range 3 {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			errs <- putSized(t, b, key, data, int64(len(data)))
		}(fmt.Sprintf("s%d", i))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("PutObject: %v", err)
		}
	}
	su.mu.Lock()
	started, concluded, aborted := len(su.started), len(su.concluded), len(su.aborted)
	su.mu.Unlock()
	if started != 3 || concluded != 1 || aborted != 2 {
		t.Fatalf("streams started/concluded/aborted = %d/%d/%d, want 3/1/2", started, concluded, aborted)
	}
	for i := range 3 {
		if got := getFrom(t, b, "bk", fmt.Sprintf("s%d", i)); !bytes.Equal(got, data) {
			t.Fatalf("s%d does not read back", i)
		}
	}
}

// TestPlaintextRelease_StreamedPartLocatedMeanwhileReuses: a streamed part
// whose digest another write accepted for the space while the stream was in
// flight records no park beside that location: the stream is given up and
// the part reuses the located blob.
func TestPlaintextRelease_StreamedPartLocatedMeanwhileReuses(t *testing.T) {
	su := newStreamingUploader()
	b, mem := newStreamingBackend(t, su)
	ctx := context.Background()
	setNone(t, b)
	// One blob: below the streaming backend's blob ceiling, and a single
	// last part may be small.
	part := testBody(8 << 10)
	d := digestOf(t, part)

	// The object write accepts the bytes for the space first.
	if err := putSized(t, b, "obj", part, int64(len(part))); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	// A streamed part declares its length, as a streamed object does.
	bucket, key := "bk", "k"
	created, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	id, n, size := created.UploadId, int32(1), int64(len(part))
	out, err := b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &id, PartNumber: &n, Body: bytes.NewReader(part), ContentLength: &size})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	parts := []types.CompletedPart{{ETag: out.ETag, PartNumber: &n}}

	if _, err := mem.GetPark(ctx, mem.bk(), d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("a park was recorded beside the location (err=%v)", err)
	}
	in, err := mem.GetIntent(ctx, mem.bk(), d)
	if err != nil || in.State != registry.IntentAccepted {
		t.Fatalf("the part's intent = %+v, %v; want accepted (reused)", in, err)
	}
	su.mu.Lock()
	aborted := len(su.aborted)
	su.mu.Unlock()
	if aborted != 1 {
		t.Fatalf("streams aborted = %d, want 1 (the part's surplus allocation)", aborted)
	}
	completeIn(t, b, "bk", "k", id, parts)
	if got := getFrom(t, b, "bk", "k"); !bytes.Equal(got, part) {
		t.Fatalf("the completed object does not read back")
	}
}

// gatedRemover holds each network remove until the test opens it, so a
// release can be caught under way.
type gatedRemover struct {
	recordingRemover
	waiting chan struct{}
	open    chan struct{}
}

func newGatedRemover() *gatedRemover {
	return &gatedRemover{waiting: make(chan struct{}, 1), open: make(chan struct{})}
}

func (g *gatedRemover) RemoveBlob(ctx context.Context, space did.DID, d multihash.Multihash) error {
	select {
	case g.waiting <- struct{}{}:
	default:
	}
	select {
	case <-g.open:
	case <-ctx.Done():
		return ctx.Err()
	}
	return g.recordingRemover.RemoveBlob(ctx, space, d)
}

// withGate makes the backend's network removes wait on gate.
func withGate(gate *gatedRemover) func(*Deps) {
	return func(d *Deps) { d.Remover = gate }
}

// withBlobs adjusts the backend's queue-space-shard dependencies.
func withBlobs(tune func(*spaceshardqueue.Deps)) func(*Deps) {
	return func(d *Deps) {
		prev := d.tuneBlobs
		d.tuneBlobs = func(bd *spaceshardqueue.Deps) {
			if prev != nil {
				prev(bd)
			}
			tune(bd)
		}
	}
}

// holdRelease runs b's due releases in the background and returns once one
// is held in its network remove: a release under way. The returned func lets
// it finish and waits for it; it is also run when the test ends.
func holdRelease(t *testing.T, b *Backend, gate *gatedRemover) (finish func()) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := b.SweepPendingReleases(context.Background())
		done <- err
	}()
	select {
	case <-gate.waiting:
	case err := <-done:
		t.Fatalf("the release finished (err %v) before it reached the network", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no release reached the network")
	}
	var once sync.Once
	finish = func() {
		once.Do(func() {
			close(gate.open)
			if err := <-done; err != nil {
				t.Errorf("held release: %v", err)
			}
		})
	}
	t.Cleanup(finish)
	return finish
}

// TestPlaintextRelease_StalledRemoveTimesOut: a release whose network remove
// never answers gives up the attempt at its time limit and stays
// queued to retry, rather than holding its worker for good.
func TestPlaintextRelease_StalledRemoveTimesOut(t *testing.T) {
	gate := newGatedRemover() // never opened
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, withGate(gate),
		withBlobs(func(d *spaceshardqueue.Deps) { d.AttemptLimit = 50 * time.Millisecond }))
	ctx := context.Background()
	setNone(t, b)

	putIn(t, b, "bk", "old", testBody(8<<10))
	deleteFrom(t, b, "bk", "old")

	done := make(chan int, 1)
	go func() {
		n, err := b.SweepPendingReleases(ctx)
		if err != nil {
			t.Errorf("SweepPendingReleases: %v", err)
		}
		done <- n
	}()
	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("%d releases finished, want none: the remove never answered", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the release waited on the stalled remove")
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, mem.bk()); len(pending) != 1 {
		t.Fatalf("releases queued after the timed-out attempt = %d, want 1 to retry", len(pending))
	}
}

// TestAbortReleasesAPublishedPartBlobItsObjectLeft: two uploads of the same
// plaintext part share its blob. One completes and its object is deleted; that
// object's release runs while the other upload's part still names the blob,
// finds it referenced and does nothing. Aborting the other upload then drops
// the last reference, and its release frees the blob.
func TestAbortReleasesAPublishedPartBlobItsObjectLeft(t *testing.T) {
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) {
		d.Remover = rm
	})
	ctx := context.Background()
	setNone(t, b)
	part := testBody(8 << 10)

	doneID, parts := uploadOnePart(t, b, "bk", "done", part)
	openID, _ := uploadOnePart(t, b, "bk", "open", part)
	completeIn(t, b, "bk", "done", doneID, parts)
	d := blobDigestOf(t, b, "done", "")
	deleteFrom(t, b, "bk", "done")
	if err := b.blobs.DrainSpace(ctx, mem.bk()); err != nil {
		t.Fatalf("run the deleted object's release: %v", err)
	}
	if n := rm.removedDigests()[string(d)]; n != 0 {
		t.Fatalf("the blob an open upload names was removed %d times", n)
	}

	bucket, key := "bk", "open"
	if err := b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &openID}); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}
	if err := b.blobs.DrainSpace(ctx, mem.bk()); err != nil {
		t.Fatalf("run the abort's releases: %v", err)
	}
	if n := rm.removedDigests()[string(d)]; n != 1 {
		t.Fatalf("the blob nothing references was removed %d times, want once", n)
	}
}
