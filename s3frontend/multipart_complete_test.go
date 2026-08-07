package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// CompleteMultipartUpload race behavior (issue #69): a Complete that loses the
// open→completing latch to another Complete joins the in-flight completion
// instead of answering the terminal NoSuchUpload, and the conclude fan-out
// runs bounded-parallel so Complete stops holding the connection for O(parts)
// sequential round trips.

// compressJoinSchedule shrinks the completing-join poll/budget for a test and
// restores the defaults on cleanup.
func compressJoinSchedule(t *testing.T, poll, wait time.Duration) {
	t.Helper()
	oldPoll, oldWait := completeJoinPoll, completeJoinWait
	completeJoinPoll, completeJoinWait = poll, wait
	t.Cleanup(func() { completeJoinPoll, completeJoinWait = oldPoll, oldWait })
}

// mpTwoParts uploads a two-part session (no declared checksum) and returns its
// upload id plus the Complete part list.
func mpTwoParts(t *testing.T, b *Backend, key string) (string, []types.CompletedPart) {
	t.Helper()
	uploadID := mpCreate(t, b, key, "", "")
	var parts []types.CompletedPart
	for i, data := range [][]byte{bytes.Repeat([]byte("a"), int(backend.MinPartSize)), []byte("tail")} {
		out, err := mpUploadPart(t, b, key, uploadID, int32(i+1), data, nil)
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		n := int32(i + 1)
		parts = append(parts, types.CompletedPart{PartNumber: &n, ETag: out.ETag})
	}
	return uploadID, parts
}

// TestCompleteJoinsInflightPeer: a Complete arriving while another Complete
// holds the session in 'completing' (the client-timeout retry window) must
// await the peer and report its outcome — a NoSuchUpload here tells the
// client its upload is gone while that upload is actively committing.
func TestCompleteJoinsInflightPeer(t *testing.T) {
	compressJoinSchedule(t, 5*time.Millisecond, 5*time.Second)
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	key := "join-inflight"
	uploadID, parts := mpTwoParts(t, b, key)

	// A peer holds the completing latch...
	if won, err := mem.LatchSession(ctx, uploadID, registry.SessionOpen, registry.SessionCompleting); err != nil || !won {
		t.Fatalf("latch to completing: won=%v err=%v", won, err)
	}
	// ...and resolves it to 'completed' a beat after the retry arrives.
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = mem.LatchSession(ctx, uploadID, registry.SessionCompleting, registry.SessionCompleted)
	}()

	res, err := mpComplete(t, b, key, uploadID, parts, nil)
	if err != nil {
		t.Fatalf("Complete during completing window: %v (want joined success)", err)
	}
	if res.ETag == nil || !strings.HasSuffix(strings.Trim(*res.ETag, `"`), "-2") {
		t.Fatalf("joined ETag = %v, want multipart ETag with -2 suffix", res.ETag)
	}
	// The peer owned the commit; the joiner must replay its outcome, not
	// re-commit — the simulated peer wrote no object, so none may exist.
	bucket := "bk"
	if _, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key}); err == nil {
		t.Fatal("joiner committed the object itself; want replay of the peer's outcome")
	}
}

// TestCompleteTakesOverAfterPeerReverts: when the in-flight peer fails and
// reverts the session to 'open', a joining Complete contends for the latch
// again and finishes the upload itself.
func TestCompleteTakesOverAfterPeerReverts(t *testing.T) {
	compressJoinSchedule(t, 5*time.Millisecond, 5*time.Second)
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	key := "join-takeover"
	uploadID, parts := mpTwoParts(t, b, key)

	if won, err := mem.LatchSession(ctx, uploadID, registry.SessionOpen, registry.SessionCompleting); err != nil || !won {
		t.Fatalf("latch to completing: won=%v err=%v", won, err)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = mem.LatchSession(ctx, uploadID, registry.SessionCompleting, registry.SessionOpen)
	}()

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("Complete after peer revert: %v (want takeover success)", err)
	}
	bucket := "bk"
	if _, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		t.Fatalf("HeadObject after takeover: %v (want committed object)", err)
	}
}

// TestCompleteJoinBudgetSlowDown: a session stuck in 'completing' (wedged
// peer, or a crash that stranded the row until the sweeper reaps it) answers
// a Complete retry with the retryable SlowDown once the join budget is spent
// — never the terminal NoSuchUpload.
func TestCompleteJoinBudgetSlowDown(t *testing.T) {
	compressJoinSchedule(t, 5*time.Millisecond, 30*time.Millisecond)
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	key := "join-budget"
	uploadID, parts := mpTwoParts(t, b, key)

	if won, err := mem.LatchSession(ctx, uploadID, registry.SessionOpen, registry.SessionCompleting); err != nil || !won {
		t.Fatalf("latch to completing: won=%v err=%v", won, err)
	}
	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	if !errors.Is(err, s3err.GetAPIError(s3err.ErrSlowDown)) {
		t.Fatalf("Complete against stuck completing session: %v, want SlowDown", err)
	}
}

// TestCompleteDuringAbortIsNoSuchUpload: losing the latch to an Abort still
// reports NoSuchUpload — only a Complete peer is joined.
func TestCompleteDuringAbortIsNoSuchUpload(t *testing.T) {
	compressJoinSchedule(t, 5*time.Millisecond, 5*time.Second)
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	key := "abort-race"
	uploadID, parts := mpTwoParts(t, b, key)

	if won, err := mem.LatchSession(ctx, uploadID, registry.SessionOpen, registry.SessionAborting); err != nil || !won {
		t.Fatalf("latch to aborting: won=%v err=%v", won, err)
	}
	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	if !errors.Is(err, s3err.GetAPIError(s3err.ErrNoSuchUpload)) {
		t.Fatalf("Complete against aborting session: %v, want NoSuchUpload", err)
	}
}

// parkingDeferred parks every blob at UploadPart and records each
// ConcludeBlobBatch call, so a test can assert Complete concludes its parked
// blobs together in batches rather than blob-by-blob. failDigests marks
// digests whose first conclude attempt reports a per-blob failure (cleared
// once used, so a retry succeeds).
type parkingDeferred struct {
	inmem.NopUploader
	mu          sync.Mutex
	calls       [][]uploader.UploadedBlob
	failDigests map[string]struct{}
}

func (d *parkingDeferred) UploadBlob(_ context.Context, _ did.DID, digest multihash.Multihash, size int64, _ string, _ ...uploader.UploadOption) (uploader.UploadedBlob, error) {
	task := cid.NewCidV1(cid.Raw, digest)
	return uploader.UploadedBlob{
		Digest:        digest,
		Size:          size,
		AddTask:       task,
		AcceptTask:    task,
		PutInvocation: []byte("parked-put-invocation"),
	}, nil
}

func (d *parkingDeferred) ConcludeBlobBatch(_ context.Context, _ did.DID, parked []uploader.UploadedBlob) ([]uploader.ConcludeBlobResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, append([]uploader.UploadedBlob(nil), parked...))
	results := make([]uploader.ConcludeBlobResult, len(parked))
	for i, p := range parked {
		if _, ok := d.failDigests[string(p.Digest)]; ok {
			delete(d.failDigests, string(p.Digest))
			results[i] = uploader.ConcludeBlobResult{Err: errors.New("injected conclude failure")}
			continue
		}
		results[i] = uploader.ConcludeBlobResult{Location: uploader.BlobLocation{Provider: "did:test:piri", URL: "http://piri/blob", Size: p.Size}}
	}
	return results, nil
}

func (d *parkingDeferred) batchCalls() [][]uploader.UploadedBlob {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([][]uploader.UploadedBlob(nil), d.calls...)
}

// newParkingTestBackend is newRefTestBackend with a parking deferred uploader
// wired in, so Complete exercises the real park → batch-conclude path.
func newParkingTestBackend(t *testing.T, def *parkingDeferred) (*Backend, *inmem.MemStore) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	mem := inmem.NewMemStore()
	spool, err := blockstore.NewSpool(filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	log, err := logstore.Open(ctx, logstore.Config{
		Dir:     filepath.Join(dir, "segments"),
		Meta:    mem,
		Catalog: logstore.PlaneConfig{Ship: false},
		Logger:  zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatalf("logstore: %v", err)
	}
	t.Cleanup(func() { _ = log.Close(ctx) })

	b := New(Deps{
		Authority: mem,
		Registry:  mem,
		Intents:   mem,
		Locations: mem,
		BlobRefs:  mem,
		GC:        mem,
		Multipart: mem,
		Parks:     mem,
		Reads:     blockstore.NewLayered(spool, log, inmem.NopBaseReader{}),
		Log:       log,
		Spool:     spool,
		Uploader:  inmem.NopUploader{},
		Deferred:  def,
		Remover:   &recordingRemover{},
	})
	if err := mem.Create(ctx, "bk", did.Undef, registry.CreateState{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return b, mem
}

// TestCompleteConcludesParkedBlobsInOneBatch: Complete's accept phase sends
// distinct parked digests to the deferred uploader as ONE batch — blob-by-blob
// concludes cost one round trip + receipt fetch per part, holding the
// connection past client read timeouts at a few hundred parts (issue #69).
func TestCompleteConcludesParkedBlobsInOneBatch(t *testing.T) {
	def := &parkingDeferred{}
	b, _ := newParkingTestBackend(t, def)

	// Two parts with distinct content → two parked digests → one batch call
	// carrying both.
	key := "batch-conclude"
	uploadID := mpCreate(t, b, key, "", "")
	var parts []types.CompletedPart
	for i, data := range [][]byte{bytes.Repeat([]byte("a"), int(backend.MinPartSize)), bytes.Repeat([]byte("b"), 16)} {
		out, uerr := mpUploadPart(t, b, key, uploadID, int32(i+1), data, nil)
		if uerr != nil {
			t.Fatalf("UploadPart %d: %v", i+1, uerr)
		}
		n := int32(i + 1)
		parts = append(parts, types.CompletedPart{PartNumber: &n, ETag: out.ETag})
	}
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	calls := def.batchCalls()
	if len(calls) != 1 || len(calls[0]) != 2 {
		sizes := make([]int, len(calls))
		for i, c := range calls {
			sizes[i] = len(c)
		}
		t.Fatalf("ConcludeBlobBatch calls = %v (want one call with both parked blobs)", sizes)
	}
}

// TestCompleteResumesAfterPartialConcludeFailure: a per-blob conclude failure
// fails the Complete but keeps every concluded blob's recorded location, so
// the retry re-concludes ONLY the failed blob (healing, issue #69).
func TestCompleteResumesAfterPartialConcludeFailure(t *testing.T) {
	partA := bytes.Repeat([]byte("a"), int(backend.MinPartSize))
	partB := bytes.Repeat([]byte("b"), 16)
	def := &parkingDeferred{failDigests: map[string]struct{}{string(digestOf(t, partB)): {}}}
	b, mem := newParkingTestBackend(t, def)
	ctx := context.Background()

	key := "partial-conclude"
	uploadID := mpCreate(t, b, key, "", "")
	var parts []types.CompletedPart
	for i, data := range [][]byte{partA, partB} {
		out, uerr := mpUploadPart(t, b, key, uploadID, int32(i+1), data, nil)
		if uerr != nil {
			t.Fatalf("UploadPart %d: %v", i+1, uerr)
		}
		n := int32(i + 1)
		parts = append(parts, types.CompletedPart{PartNumber: &n, ETag: out.ETag})
	}

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err == nil {
		t.Fatal("Complete with an injected conclude failure: want error, got nil")
	}
	// The successful blob's location was recorded despite the batch error...
	if loc, err := mem.GetLocation(ctx, did.Undef, digestOf(t, partA)); err != nil || loc == nil {
		t.Fatalf("location for concluded blob after failed Complete: %v, %v (want recorded)", loc, err)
	}
	// ...so the retry succeeds and re-concludes only the failed blob.
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("Complete retry: %v", err)
	}
	calls := def.batchCalls()
	if len(calls) != 2 {
		t.Fatalf("ConcludeBlobBatch calls = %d, want 2 (initial + retry)", len(calls))
	}
	if len(calls[1]) != 1 || string(calls[1][0].Digest) != string(digestOf(t, partB)) {
		t.Fatalf("retry batch = %d blobs, want only the failed blob", len(calls[1]))
	}
}
