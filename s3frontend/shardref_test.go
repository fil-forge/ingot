package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/google/uuid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/libforge/taskrunner"
	"github.com/fil-forge/libforge/taskrunner/taskrunnertest"
)

// lostAnswerRemover removes a blob and then, for its next `lose` calls,
// answers with an error, as a remove whose answer is lost does: the blob is
// gone from the provider and the release cannot tell.
type lostAnswerRemover struct {
	*recordingRemover
	lose int
}

func (l *lostAnswerRemover) RemoveBlob(ctx context.Context, space did.DID, d multihash.Multihash) error {
	if err := l.recordingRemover.RemoveBlob(ctx, space, d); err != nil {
		return err
	}
	if l.lose > 0 {
		l.lose--
		return errors.New("upload service connection reset")
	}
	return nil
}

// TestPlaintextRelease_WriteWaitsBehindARetryingRelease: a release whose
// remove reached the provider but whose answer was lost retries holding the
// blob's subject. A write of the same bytes waits for the retry, then finds
// the blob unlocated and concludes its own stream, rather than reusing a
// blob the provider no longer holds.
func TestPlaintextRelease_WriteWaitsBehindARetryingRelease(t *testing.T) {
	rm := &lostAnswerRemover{recordingRemover: &recordingRemover{}, lose: 1}
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) { d.Remover = rm })
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	putIn(t, b, "bk", "old", data)
	d := blobDigestOf(t, b, "old", "")
	deleteFrom(t, b, "bk", "old")
	drainReleases(t, b) // the remove goes through, and its answer is lost
	n := rm.removedDigests()[string(d)]
	require.Equal(t, 1, n, "removes of %x before the write", d)

	done := make(chan error, 1)
	go func() {
		bucket, key := "bk", "new"
		_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
		done <- err
	}()
	select {
	case err := <-done:
		require.FailNow(t, "PutObject returned while the release waited to retry", "err: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	drainReleases(t, b) // the retry finishes the release
	require.NoError(t, <-done, "PutObject after the retry")
	sent, accepted, _ := streamCounts(t, b, d)
	require.Equal(t, 2, sent, "streams of %x: %d sent, %d accepted; want 2 and 2 (the write concluded its own stream)", d, sent, accepted)
	require.Equal(t, 2, accepted, "streams of %x: %d sent, %d accepted; want 2 and 2 (the write concluded its own stream)", d, sent, accepted)
	_, err := mem.GetLocation(ctx, mem.bk(), d)
	require.NoError(t, err, "no location after the write")
	got := getFrom(t, b, "bk", "new")
	require.Equal(t, data, got, "the rewritten object does not read back")
}

// TestPendingReferenceHoldsTheBlobUntilItExpires: a write's reference keeps a
// blob whose last claim is dropped, and its expiry releases the blob once
// nothing else references it.
func TestPendingReferenceHoldsTheBlobUntilItExpires(t *testing.T) {
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) { d.Remover = rm })
	ctx := context.Background()
	putIn(t, b, "bk", "k", testBody(4<<10))
	st, err := mem.Get(ctx, "bk")
	require.NoError(t, err)
	rv, err := b.resolveVersion(ctx, "bk", "k", "")
	require.NoError(t, err)
	blobs := storedBlobs(t, b, rv)
	d := blobs[0].Digest
	// A same-space copy that takes its reference and dies before its commit.
	refs, err := b.pinBlobs(ctx, st, blobs)
	require.NoError(t, err, "pinBlobs")

	deleteFrom(t, b, "bk", "k")
	drainReleases(t, b)
	n := rm.removedDigests()[string(d)]
	require.Zero(t, n, "the blob the copy references was removed %d times", n)

	b.blobs.DropRefsNow(ctx, mem.bk(), []taskrunner.TaskID{refs[string(d)]}, []multihash.Multihash{d})
	require.Equal(t, 1, rm.removedDigests()[string(d)], "removes of %x once its reference expired", d)
	refsLeft, _ := mem.CountReferences(ctx, mem.bk(), d, uuid.Nil)
	require.Zero(t, refsLeft, "references to %x after the expiry", d)
}

// TestAFailedWritesCleanupLeavesAnotherWritesReference: two writes in flight
// hold references to the same blob. One fails and expires its own at once;
// the other's reference keeps its deadline, so the blob stays.
func TestAFailedWritesCleanupLeavesAnotherWritesReference(t *testing.T) {
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) { d.Remover = rm })
	ctx := context.Background()
	putIn(t, b, "bk", "k", testBody(4<<10))
	st, err := mem.Get(ctx, "bk")
	require.NoError(t, err)
	rv, err := b.resolveVersion(ctx, "bk", "k", "")
	require.NoError(t, err)
	blobs := storedBlobs(t, b, rv)
	d := blobs[0].Digest
	failed, err := b.pinBlobs(ctx, st, blobs)
	require.NoError(t, err, "pinBlobs")
	_, err = b.pinBlobs(ctx, st, blobs)
	require.NoError(t, err, "pinBlobs")
	deleteFrom(t, b, "bk", "k")
	drainReleases(t, b)

	b.blobs.DropRefsNow(ctx, mem.bk(), []taskrunner.TaskID{failed[string(d)]}, []multihash.Multihash{d})
	n, _ := mem.CountReferences(ctx, mem.bk(), d, uuid.Nil)
	require.Equal(t, 1, n, "references to %x after one write's cleanup", d)
	require.Zero(t, rm.removedDigests()[string(d)], "removes of the blob another write references")
}

// TestCommitCancelsItsReferencesExpiries: the commit that claims a write's
// references cancels their expiries with them.
func TestCommitCancelsItsReferencesExpiries(t *testing.T) {
	b, mem := newDeferredBackend(t, testfakes.NopUploader{})
	ctx := context.Background()
	putIn(t, b, "bk", "k", testBody(4<<10))
	tasks, err := taskrunnertest.Tasks(ctx, mem.pool, "ingot", spaceshardqueue.Name)
	require.NoError(t, err)
	expiries := 0
	for _, q := range tasks {
		if q.Action != spaceshardqueue.ExpireAction {
			continue
		}
		expiries++
		require.Equal(t, "cancelled", q.State, "expiry %s is %s after the commit", q.ID, q.State)
	}
	require.NotZero(t, expiries, "the write queued no expiry")
}

// expireRefsTo expires every pending reference to digest in space at once, as
// their deadlines passing would, and waits for the release they leave due.
func expireRefsTo(t *testing.T, b *Backend, space did.DID, digest multihash.Multihash) {
	t.Helper()
	ctx := context.Background()
	_, err := storeFor(b).tasks.Expedite(ctx, spaceshardqueue.Name, spaceshardqueue.Subject(space, digest), spaceshardqueue.ExpireAction)
	require.NoError(t, err, "expedite the expiries")
	b.blobs.ReleaseNow(ctx, space, []multihash.Multihash{digest})
	b.blobs.ReleaseNow(ctx, space, []multihash.Multihash{digest})
}
