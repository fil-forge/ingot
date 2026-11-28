package s3frontend

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, mem.Create(ctx, "bk2", space, registry.CreateState{}), "create bk2")
	b.allowNoneEncryption = true
	for _, bucket := range []string{"bk", "bk2"} {
		require.NoError(t, b.PutBucketEncryption(ctx, bucket, sseConfig(sseNone)), "PutBucketEncryption %s none", bucket)
	}
	return space
}

func putIn(t *testing.T, b *Backend, bucket, key string, data []byte) {
	t.Helper()
	_, err := b.PutObject(context.Background(), s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
	require.NoError(t, err, "PutObject %s/%s", bucket, key)
}

func getFrom(t *testing.T, b *Backend, bucket, key string) []byte {
	t.Helper()
	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "GetObject %s/%s", bucket, key)
	defer out.Body.Close()
	var buf bytes.Buffer
	_, err = buf.ReadFrom(out.Body)
	require.NoError(t, err, "read body")
	return buf.Bytes()
}

func deleteFrom(t *testing.T, b *Backend, bucket, key string) {
	t.Helper()
	_, err := b.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "DeleteObject %s/%s", bucket, key)
}

// uploadOnePart starts a multipart upload of key in bucket, uploads part as
// its only part, and returns the upload id and the part for Complete.
func uploadOnePart(t *testing.T, b *Backend, bucket, key string, part []byte) (string, []types.CompletedPart) {
	t.Helper()
	ctx := context.Background()
	res, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "CreateMultipartUpload %s/%s", bucket, key)
	n := int32(1)
	out, err := b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &res.UploadId, PartNumber: &n, Body: bytes.NewReader(part), ContentLength: byteLen(part)})
	require.NoError(t, err, "UploadPart %s/%s", bucket, key)
	return res.UploadId, []types.CompletedPart{{ETag: out.ETag, PartNumber: &n}}
}

func completeIn(t *testing.T, b *Backend, bucket, key, uploadID string, parts []types.CompletedPart) {
	t.Helper()
	_, _, err := b.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket: &bucket, Key: &key, UploadId: &uploadID, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	require.NoError(t, err, "CompleteMultipartUpload %s/%s", bucket, key)
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
	d2 := hygienePartDigests(t, mem, id2, 1)[0]
	require.True(t, bytes.Equal(d, d2), "identical parts parked under different digests")
	for _, space := range []did.DID{mem.bk(), space2} {
		_, err := mem.GetPark(ctx, space, d)
		require.NoError(t, err, "park for %x in %s", d, space)
	}

	completeIn(t, b, "bk", "k", id1, parts1)
	completeIn(t, b, "bk2", "k", id2, parts2)
	for _, bucket := range []string{"bk", "bk2"} {
		got := getFrom(t, b, bucket, "k")
		require.Equal(t, part, got, "%s/k reads back %d bytes", bucket, len(got))
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
	require.True(t, localCopyExists(b, d), "no local copy after the writes")

	deleteFrom(t, b, "bk", "k")
	drainReleases(t, b)

	_, err := mem.GetIntent(ctx, mem.bk(), d)
	require.ErrorIs(t, err, registry.ErrNotFound, "the deleted space's intent survived its release")
	in, err := mem.GetIntent(ctx, space2, d)
	require.NoError(t, err, "the other space's intent after the release = %+v, %v", in, err)
	require.Equal(t, registry.IntentPublished, in.State, "the other space's intent after the release = %+v, %v", in, err)
	require.True(t, localCopyExists(b, d), "the release removed the local copy the other space's intent still names")
	got := getFrom(t, b, "bk2", "k")
	require.Equal(t, data, got, "bk2/k no longer reads back after bk/k was deleted")
	n := rm.removedDigests()[string(d)]
	require.Equal(t, 1, n, "network removes of %x = %d, want 1 (the deleted space's)", d, n)

	deleteFrom(t, b, "bk2", "k")
	drainReleases(t, b)
	_, err = mem.GetIntent(ctx, space2, d)
	require.ErrorIs(t, err, registry.ErrNotFound, "the last intent survived its release")
	settleCache(t, b, d) // the release queues the eviction as its own task
	require.False(t, localCopyExists(b, d), "the local copy survived the last intent")
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
	pd := hygienePartDigests(t, mem, id, 1)[0]
	require.True(t, bytes.Equal(pd, d), "the part and the object stored different digests for the same bytes")
	_, err := mem.GetPark(ctx, space2, d)
	require.NoError(t, err, "bk2's part is not parked")

	deleteFrom(t, b, "bk", "k")
	drainReleases(t, b)

	_, err = mem.GetPark(ctx, space2, d)
	require.NoError(t, err, "bk's release took bk2's park")
	require.False(t, pu.abortedDigests()[string(d)], "bk's release aborted bk2's allocation on the provider")
	completeIn(t, b, "bk2", "k", id, parts)
	got := getFrom(t, b, "bk2", "k")
	require.Equal(t, data, got, "bk2/k does not read back after bk/k was deleted")
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
	require.NoError(t, b.blobs.DrainSpace(ctx, mem.bk()), "run the space's releases")

	pending, _ := mem.ListReleasesBySpace(ctx, mem.bk())
	require.Empty(t, pending, "release records after the drain")
	n := rm.removedDigests()[string(d)]
	require.Zero(t, n, "the re-claimed blob was removed from the network %d times", n)
	in, err := mem.GetIntent(ctx, mem.bk(), d)
	require.NoError(t, err, "intent after the rewrite = %+v, %v", in, err)
	require.Equal(t, registry.IntentPublished, in.State, "intent after the rewrite = %+v, %v", in, err)
	got := getFrom(t, b, "bk", "new")
	require.Equal(t, data, got, "the rewritten object does not read back")
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
	pending, _ := mem.ListReleasesBySpace(ctx, mem.bk())
	require.Len(t, pending, 1, "release records after the delete")
	putIn(t, b, "bk", "new", data)

	pending, _ = mem.ListReleasesBySpace(ctx, mem.bk())
	require.Empty(t, pending, "the write left the release record in place")
	sent, accepted, _ := streamCounts(t, b, d)
	require.Equal(t, 2, sent, "streams of %x: %d sent, %d accepted; want 2 and 2 (uploaded afresh after the cancel)", d, sent, accepted)
	require.Equal(t, 2, accepted, "streams of %x: %d sent, %d accepted; want 2 and 2 (uploaded afresh after the cancel)", d, sent, accepted)
	drainReleases(t, b)
	got := getFrom(t, b, "bk", "new")
	require.Equal(t, data, got, "the rewritten object does not read back")
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
		require.FailNow(t, "PutObject returned while the release was started", "err: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NotZero(t, b.cache.InFlight(), "no local copy in progress while the write waits")

	// The release finishes: it removes the blob from the provider, and leaves
	// the waiting write's intent alone.
	finish()
	require.NoError(t, <-done, "PutObject after the release")
	sent, accepted, _ := streamCounts(t, b, d)
	require.Equal(t, 2, sent, "streams of %x sent (the blob was uploaded again after its release)", d)
	require.Equal(t, 2, accepted, "streams of %x accepted (the blob was uploaded again after its release)", d)
	_, err := mem.GetLocation(ctx, mem.bk(), d)
	require.NoError(t, err, "no location after the fresh upload")
	in, err := mem.GetIntent(ctx, mem.bk(), d)
	require.NoError(t, err, "intent after the rewrite = %+v, %v", in, err)
	require.Equal(t, registry.IntentPublished, in.State, "intent after the rewrite = %+v, %v", in, err)
	got := getFrom(t, b, "bk", "new")
	require.Equal(t, data, got, "the rewritten object does not read back")
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
	// The short wait bounds every write, so on a slow machine the write that
	// sets up can be refused too: it retries for a while, as a client would.
	var apiErr s3err.APIError
	for bucket, key, deadline := "bk", "old", time.Now().Add(10*time.Second); ; {
		_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
		if err == nil {
			break
		}
		require.ErrorAs(t, err, &apiErr, "PutObject bk/old")
		require.Equal(t, "SlowDown", apiErr.Code, "PutObject bk/old")
		require.False(t, time.Now().After(deadline), "PutObject bk/old")
	}
	deleteFrom(t, b, "bk", "old")
	holdRelease(t, b, gate)

	bucket, key := "bk", "new"
	_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
	require.ErrorAs(t, err, &apiErr, "PutObject during a release that never finishes: err = %v, want SlowDown", err)
	require.Equal(t, "SlowDown", apiErr.Code, "PutObject during a release that never finishes: err")
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
	require.NoError(t, mem.DeleteLocation(ctx, mem.bk(), d), "DeleteLocation")

	done := make(chan error, 1)
	go func() {
		bucket, key := "bk", "new"
		_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
		done <- err
	}()
	select {
	case err := <-done:
		require.FailNow(t, "PutObject returned while the release was started", "err: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	_, accepted, _ := streamCounts(t, b, d)
	require.Equal(t, 1, accepted, "streams of %x accepted while the release was started = %d, want 1 (the write waited)", d, accepted)

	finish()
	require.NoError(t, <-done, "PutObject after the release")
	sent, accepted, _ := streamCounts(t, b, d)
	require.Equal(t, 2, sent, "streams of %x sent", d)
	require.Equal(t, 2, accepted, "streams of %x accepted", d)
	got := getFrom(t, b, "bk", "new")
	require.Equal(t, data, got, "the rewritten object does not read back")
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

	require.NoError(t, putSized(t, b, "old", data, int64(len(data))), "PutObject old")
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	finish := holdRelease(t, b, gate)

	done := make(chan error, 1)
	go func() { done <- putSized(t, b, "new", data, int64(len(data))) }()
	select {
	case err := <-done:
		require.FailNow(t, "PutObject returned while the release was started", "err: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	finish()
	require.NoError(t, <-done, "PutObject after the release")

	su.mu.Lock()
	var concluded int
	for _, c := range su.concluded {
		if bytes.Equal(c.Digest, d) {
			concluded++
		}
	}
	aborted := len(su.aborted)
	su.mu.Unlock()
	require.Zero(t, aborted, "after the waited-out release: streams of %x aborted = %d, concluded", d, aborted)
	require.Equal(t, 2, concluded, "after the waited-out release: streams of %x aborted = %d, concluded", d, aborted)
	_, err := mem.GetLocation(ctx, mem.bk(), d)
	require.NoError(t, err, "no location after the fresh upload")
	got := getFrom(t, b, "bk", "new")
	require.Equal(t, data, got, "the rewritten object does not read back")
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
		require.NoError(t, err, "PutObject")
	}
	sent, accepted, aborted := streamCounts(t, b, d)
	require.Equal(t, 4, sent, "streams of %x by four concurrent writes: %d sent, %d accepted, %d aborted", d, sent, accepted, aborted)
	require.Equal(t, 1, accepted, "streams of %x by four concurrent writes: %d sent, %d accepted, %d aborted", d, sent, accepted, aborted)
	require.Equal(t, 3, aborted, "streams of %x by four concurrent writes: %d sent, %d accepted, %d aborted", d, sent, accepted, aborted)
	for i := range 4 {
		got := getFrom(t, b, "bk", fmt.Sprintf("k%d", i))
		require.Equal(t, data, got, "k%d does not read back", i)
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
		got := hygienePartDigests(t, mem, id, 1)[0]
		require.True(t, bytes.Equal(got, d), "part of %s stored under %x, want %x", id, got, d)
	}
	sent, _, aborted := streamCounts(t, b, d)
	require.Equal(t, 3, sent, "streams of %x by three concurrent part uploads: %d sent, %d aborted", d, sent, aborted)
	require.Equal(t, 2, aborted, "streams of %x by three concurrent part uploads: %d sent, %d aborted", d, sent, aborted)
	_, err := mem.ParkByDigest(context.Background(), d)
	require.NoError(t, err, "the shared park")
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
	require.NoError(t, mem.EnqueueRelease(ctx, mem.bk(), d, registry.DueNow()), "EnqueueRelease")

	n, err := b.SweepPendingReleases(ctx)
	require.NoError(t, err, "SweepPendingReleases = %d, %v", n, err)
	require.Zero(t, n, "SweepPendingReleases = %d, %v", n, err)
	require.LessOrEqual(t, rm.removedDigests()[string(d)], 0, "the stale release removed %x from the network", d)
	pending, _ := mem.ListReleasesBySpace(ctx, mem.bk())
	require.Empty(t, pending, "releases after the sweep")
	got := getFrom(t, b, "bk", "k")
	require.Equal(t, data, got, "the object does not read back")
}

// TestPlaintextSpaces_LastReleaseOutRemovesTheSharedFile: two spaces release
// the same digest; the eviction after the first sees the other's intent and
// leaves the file, and the one after the last removes it.
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
		_, err := mem.GetIntent(ctx, sp, d)
		require.ErrorIs(t, err, registry.ErrNotFound, "intent in %s survived (err=%v)", sp, err)
	}
	settleCache(t, b, d) // the release queues the eviction as its own task
	require.False(t, localCopyExists(b, d), "the shared file survived both releases")
}

// TestPlaintextSpaces_OverlappingReleasesRemoveTheSharedFile: two spaces'
// releases of the same digest run at once, each past its checks before the
// other commits. The file still goes: each release's eviction runs after it
// commits, and the one that finds no intent left removes it.
func TestPlaintextSpaces_OverlappingReleasesRemoveTheSharedFile(t *testing.T) {
	gate := newGatedRemover()
	mem := newTestStore(t)
	b, mem, _ := newRefTestBackendOver(t, mem, newRetainedLog(t, mem), 0, withGate(gate))
	space2 := secondPlaintextBucket(t, b, mem)
	ctx := context.Background()
	data := testBody(8 << 10)

	putIn(t, b, "bk", "k", data)
	putIn(t, b, "bk2", "k", data)
	d := blobDigestOf(t, b, "k", "")

	// Each release is held at its network remove, past its checks, until
	// both have reached it.
	deleted := make(chan error, 2)
	for _, bucket := range []string{"bk", "bk2"} {
		go func() {
			_, err := b.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("k")})
			deleted <- err
		}()
		select {
		case <-gate.waiting:
		case <-time.After(20 * time.Second):
			require.FailNow(t, "release never reached its network remove", "bucket %s", bucket)
		}
	}
	close(gate.open)
	for range 2 {
		require.NoError(t, <-deleted, "DeleteObject")
	}
	drainReleases(t, b)

	for _, sp := range []did.DID{mem.bk(), space2} {
		_, err := mem.GetIntent(ctx, sp, d)
		require.ErrorIs(t, err, registry.ErrNotFound, "intent in %s", sp)
	}
	settleCache(t, b, d)
	require.False(t, localCopyExists(b, d), "the shared file survived both releases")
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
		require.NoError(t, mem.EnqueueRelease(context.Background(), mem.bk(), d, registry.DueNow()), "EnqueueRelease")
		drainReleases(t, b)
	}

	putIn(t, b, "bk", "k", data)
	require.Nil(t, reg.start, "the release never ran")
	n := rm.removedDigests()[string(d)]
	require.Zero(t, n, "the referenced blob was removed from the network %d times", n)
	got := getFrom(t, b, "bk", "k")
	require.Equal(t, data, got, "the object does not read back")
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
	require.ErrorAs(t, err, &apiErr, "PutObject whose reference expired before its commit")
	require.Equal(t, "SlowDown", apiErr.Code, "PutObject whose reference expired before its commit")
	require.Equal(t, 1, rm.removedDigests()[string(second)], "removals of the expired reference's blob")
	n, _ := mem.CountReferences(context.Background(), mem.bk(), first, uuid.Nil)
	require.Equal(t, 1, n, "references to the first blob: want only the write's pending one")
	rows, _ := mem.CountClaimRows(context.Background(), mem.bk(), first)
	require.Zero(t, rows, "a claim on the first blob landed although the commit was refused")
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
		require.NoError(t, err, "PutObject")
	}
	su.mu.Lock()
	started, concluded, aborted := len(su.started), len(su.concluded), len(su.aborted)
	su.mu.Unlock()
	require.Equal(t, 3, started, "streams started/concluded/aborted = %d/%d/%d", started, concluded, aborted)
	require.Equal(t, 1, concluded, "streams started/concluded/aborted = %d/%d/%d", started, concluded, aborted)
	require.Equal(t, 2, aborted, "streams started/concluded/aborted = %d/%d/%d", started, concluded, aborted)
	for i := range 3 {
		got := getFrom(t, b, "bk", fmt.Sprintf("s%d", i))
		require.Equal(t, data, got, "s%d does not read back", i)
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
	require.NoError(t, putSized(t, b, "obj", part, int64(len(part))), "PutObject")
	// A streamed part declares its length, as a streamed object does.
	bucket, key := "bk", "k"
	created, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "CreateMultipartUpload")
	id, n, size := created.UploadId, int32(1), int64(len(part))
	out, err := b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &id, PartNumber: &n, Body: bytes.NewReader(part), ContentLength: &size})
	require.NoError(t, err, "UploadPart")
	parts := []types.CompletedPart{{ETag: out.ETag, PartNumber: &n}}

	_, err = mem.GetPark(ctx, mem.bk(), d)
	require.ErrorIs(t, err, registry.ErrNotFound, "a park was recorded beside the location")
	in, err := mem.GetIntent(ctx, mem.bk(), d)
	require.NoError(t, err, "the part's intent = %+v, %v", in, err)
	require.Equal(t, registry.IntentAccepted, in.State, "the part's intent = %+v, %v", in, err)
	su.mu.Lock()
	aborted := len(su.aborted)
	su.mu.Unlock()
	require.Equal(t, 1, aborted, "streams aborted = %d, want 1 (the part's surplus allocation)", aborted)
	completeIn(t, b, "bk", "k", id, parts)
	got := getFrom(t, b, "bk", "k")
	require.Equal(t, part, got, "the completed object does not read back")
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
		require.FailNow(t, "the release finished before it reached the network", "err: %v", err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no release reached the network")
	}
	var once sync.Once
	finish = func() {
		once.Do(func() {
			close(gate.open)
			assert.NoError(t, <-done, "held release")
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
		assert.NoError(t, err, "SweepPendingReleases")
		done <- n
	}()
	select {
	case n := <-done:
		require.Zero(t, n, "%d releases finished, want none: the remove never answered", n)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the release waited on the stalled remove")
	}
	pending, _ := mem.ListReleasesBySpace(ctx, mem.bk())
	require.Len(t, pending, 1, "releases queued after the timed-out attempt")
}

// stallingParks stalls a release once, after the last of its checks and
// before it touches the network.
type stallingParks struct {
	registry.ParkStore
	once  sync.Once
	stall func(ctx context.Context, space did.DID, digest multihash.Multihash)
}

func (s *stallingParks) GetPark(ctx context.Context, space did.DID, digest multihash.Multihash) (*registry.BlobPark, error) {
	s.once.Do(func() { s.stall(ctx, space, digest) })
	// The release carries on as one that did not watch its context would.
	return s.ParkStore.GetPark(context.WithoutCancel(ctx), space, digest)
}

// TestPlaintextRelease_LostLeaseSendsNoRemove: a release that passes its
// checks and then loses its subject's lease to another consumer does not go
// on to remove the blob: that consumer may have released it, and a write
// added it again. The release runs once the lease is free.
func TestPlaintextRelease_LostLeaseSendsNoRemove(t *testing.T) {
	rm := &recordingRemover{}
	var mem *testStore
	stalled := make(chan struct{})
	leases := "ingot." + strings.ReplaceAll(spaceshardqueue.Name, "-", "_") + "_leases"
	const takenFor = 3 * time.Second
	stall := func(ctx context.Context, space did.DID, digest multihash.Multihash) {
		defer close(stalled)
		tag, err := mem.pool.Exec(context.Background(), `UPDATE `+leases+`
			SET owner = $2, expires = now() + $3 * interval '1 millisecond' WHERE subject = $1`,
			spaceshardqueue.Subject(space, digest), uuid.New(), takenFor.Milliseconds())
		assert.NoError(t, err, "take the release's lease")
		assert.Equal(t, int64(1), tag.RowsAffected(), "take the release's lease: rows")
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Second):
			assert.Fail(t, "the release's context did not end after its lease was taken over")
		}
	}
	b, mem := newDeferredBackend(t, testfakes.NopUploader{},
		func(d *Deps) { d.Remover = rm },
		withBlobs(func(d *spaceshardqueue.Deps) { d.Parks = &stallingParks{ParkStore: d.Parks, stall: stall} }))
	setNone(t, b)

	putIn(t, b, "bk", "old", testBody(8<<10))
	deleteFrom(t, b, "bk", "old")

	done := make(chan error, 1)
	go func() {
		_, err := b.SweepPendingReleases(context.Background())
		done <- err
	}()
	select {
	case <-stalled:
	case <-time.After(20 * time.Second):
		require.FailNow(t, "the release never reached its last check")
	}
	// The other consumer holds the lease for a while yet, so only the
	// stalled run could send a remove now.
	time.Sleep(takenFor / 3)
	n := len(rm.removedDigests())
	require.Zero(t, n, "the release removed %d blobs after losing its lease", n)
	require.NoError(t, <-done, "SweepPendingReleases")
	deadline := time.Now().Add(20 * time.Second)
	for len(rm.removedDigests()) == 0 {
		require.False(t, time.Now().After(deadline), "the release did not run once its lease was free")
		time.Sleep(50 * time.Millisecond)
		drainReleases(t, b)
	}
	require.Len(t, rm.removed, 1, "removes sent")
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
	require.NoError(t, b.blobs.DrainSpace(ctx, mem.bk()), "run the deleted object's release")
	n := rm.removedDigests()[string(d)]
	require.Zero(t, n, "the blob an open upload names was removed %d times", n)

	bucket, key := "bk", "open"
	require.NoError(t, b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &openID}), "AbortMultipartUpload")
	require.NoError(t, b.blobs.DrainSpace(ctx, mem.bk()), "run the abort's releases")
	require.Equal(t, 1, rm.removedDigests()[string(d)], "removals of the blob nothing references")
}
