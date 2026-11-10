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

	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
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
	if n := rm.removedDigests()[string(d)]; n != 1 {
		t.Fatalf("removes of %x before the write = %d, want 1", d, n)
	}

	done := make(chan error, 1)
	go func() {
		bucket, key := "bk", "new"
		_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("PutObject returned %v while the release waited to retry", err)
	case <-time.After(200 * time.Millisecond):
	}
	drainReleases(t, b) // the retry finishes the release
	if err := <-done; err != nil {
		t.Fatalf("PutObject after the retry: %v", err)
	}
	if sent, accepted, _ := streamCounts(t, b, d); sent != 2 || accepted != 2 {
		t.Fatalf("streams of %x: %d sent, %d accepted; want 2 and 2 (the write concluded its own stream)", d, sent, accepted)
	}
	if _, err := mem.GetLocation(ctx, mem.bk(), d); err != nil {
		t.Fatalf("no location after the write: %v", err)
	}
	if got := getFrom(t, b, "bk", "new"); !bytes.Equal(got, data) {
		t.Fatalf("the rewritten object does not read back")
	}
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
	if err != nil {
		t.Fatal(err)
	}
	rv, err := b.resolveVersion(ctx, "bk", "k", "")
	if err != nil {
		t.Fatal(err)
	}
	blobs := storedBlobs(t, b, rv)
	d := blobs[0].Digest
	// A same-space copy that takes its reference and dies before its commit.
	refs, err := b.pinBlobs(ctx, st, blobs)
	if err != nil {
		t.Fatalf("pinBlobs: %v", err)
	}

	deleteFrom(t, b, "bk", "k")
	drainReleases(t, b)
	if n := rm.removedDigests()[string(d)]; n != 0 {
		t.Fatalf("the blob the copy references was removed %d times", n)
	}

	b.blobs.DropRefsNow(ctx, mem.bk(), []taskrunner.TaskID{refs[string(d)]}, []multihash.Multihash{d})
	if n := rm.removedDigests()[string(d)]; n != 1 {
		t.Fatalf("removes of %x once its reference expired = %d, want 1", d, n)
	}
	if n, _ := mem.CountReferences(ctx, mem.bk(), d, uuid.Nil); n != 0 {
		t.Fatalf("references to %x after the expiry = %d, want 0", d, n)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	rv, err := b.resolveVersion(ctx, "bk", "k", "")
	if err != nil {
		t.Fatal(err)
	}
	blobs := storedBlobs(t, b, rv)
	d := blobs[0].Digest
	failed, err := b.pinBlobs(ctx, st, blobs)
	if err != nil {
		t.Fatalf("pinBlobs: %v", err)
	}
	if _, err := b.pinBlobs(ctx, st, blobs); err != nil {
		t.Fatalf("pinBlobs: %v", err)
	}
	deleteFrom(t, b, "bk", "k")
	drainReleases(t, b)

	b.blobs.DropRefsNow(ctx, mem.bk(), []taskrunner.TaskID{failed[string(d)]}, []multihash.Multihash{d})
	if n, _ := mem.CountReferences(ctx, mem.bk(), d, uuid.Nil); n != 1 {
		t.Fatalf("references to %x after one write's cleanup = %d, want the other write's", d, n)
	}
	if n := rm.removedDigests()[string(d)]; n != 0 {
		t.Fatalf("the blob another write references was removed %d times", n)
	}
}

// TestCommitCancelsItsReferencesExpiries: the commit that claims a write's
// references cancels their expiries with them.
func TestCommitCancelsItsReferencesExpiries(t *testing.T) {
	b, mem := newDeferredBackend(t, testfakes.NopUploader{})
	ctx := context.Background()
	putIn(t, b, "bk", "k", testBody(4<<10))
	tasks, err := taskrunnertest.Tasks(ctx, mem.pool, "ingot", registry.ReleaseQueue)
	if err != nil {
		t.Fatal(err)
	}
	expiries := 0
	for _, q := range tasks {
		if q.Action != registry.ExpireAction {
			continue
		}
		expiries++
		if q.State != "cancelled" {
			t.Fatalf("expiry %s is %s after the commit, want cancelled", q.ID, q.State)
		}
	}
	if expiries == 0 {
		t.Fatal("the write queued no expiry")
	}
}

// expireRefsTo expires every pending reference to digest in space at once, as
// their deadlines passing would, and waits for the release they leave due.
func expireRefsTo(t *testing.T, b *Backend, space did.DID, digest multihash.Multihash) {
	t.Helper()
	ctx := context.Background()
	if _, err := storeFor(b).tasks.Expedite(ctx, registry.ReleaseQueue, registry.ReleaseSubject(space, digest), registry.ExpireAction); err != nil {
		t.Fatalf("expedite the expiries: %v", err)
	}
	b.blobs.ReleaseNow(ctx, space, []multihash.Multihash{digest})
	b.blobs.ReleaseNow(ctx, space, []multihash.Multihash{digest})
}
