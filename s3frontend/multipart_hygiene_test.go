package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	cmds3 "github.com/fil-forge/libforge/commands/s3"
)

// Multipart hygiene: every path that orphans a part blob — Complete omitting
// it, supersede, abort, and the sweeper — must release it fully: enc-params
// row (the crypto-shred), upload intent, park row, and the network copy.
// Under the NopUploader every part blob is IntentAccepted (there is no
// network to park on), so these tests exercise the accepted-at-zero-claims
// release arm; the parked arm is covered by the parkingUploader tests below
// and the Docker itests.

// hygienePartDigests reads the blob digests recorded for uploadID's part n.
func hygienePartDigests(t *testing.T, mem *testStore, uploadID string, n int) []multihash.Multihash {
	t.Helper()
	parts, err := mem.ListParts(context.Background(), uploadID)
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	for _, p := range parts {
		if p.PartNumber == n {
			if len(p.BlobDigests) == 0 {
				t.Fatalf("part %d has no blob digests", n)
			}
			return p.BlobDigests
		}
	}
	t.Fatalf("part %d not recorded for upload %s", n, uploadID)
	return nil
}

// drainReleases executes every due release (unit backends run with
// a zero release grace, so everything enqueued is immediately due).
func drainReleases(t *testing.T, b *Backend) {
	t.Helper()
	if _, err := b.SweepPendingReleases(context.Background()); err != nil {
		t.Fatalf("SweepPendingReleases: %v", err)
	}
}

// assertReleased drains the deferred-release queue, then asserts a digest's
// enc-params row and upload intent are gone and (wantRemoved) its network
// release was invoked: a remove of an accepted blob or an abort of a parked
// one.
func assertReleased(t *testing.T, b *Backend, mem *testStore, rm *recordingRemover, d multihash.Multihash, wantRemoved bool) {
	t.Helper()
	drainReleases(t, b)
	ctx := context.Background()
	if _, err := mem.GetEncryptionParams(ctx, mem.bk(), d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("enc-params row for %x survived (err=%v) — crypto-shred missing", d, err)
	}
	if _, err := mem.IntentByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("upload intent for %x survived (err=%v)", d, err)
	}
	if got := rm.removedDigests()[string(d)] > 0 || abortedOn(b, d); got != wantRemoved {
		t.Fatalf("network release of %x invoked = %v, want %v", d, got, wantRemoved)
	}
}

// assertRetained asserts a digest's enc-params row survives and no network
// release was invoked for it.
func assertRetained(t *testing.T, mem *testStore, rm *recordingRemover, d multihash.Multihash) {
	t.Helper()
	if _, err := mem.GetEncryptionParams(context.Background(), mem.bk(), d); err != nil {
		t.Fatalf("enc-params row for retained blob %x: %v", d, err)
	}
	if rm.removedDigests()[string(d)] > 0 {
		t.Fatalf("retained blob %x was released on the network", d)
	}
}

// TestCompleteReapsOrphanParts: a part uploaded but omitted from the winning
// list is fully released at Complete, while the winners (guarded by the keep
// set — their part rows are retained for idempotency) stay untouched.
func TestCompleteReapsOrphanParts(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	key := "orphan"
	winner := testBody(int(backend.MinPartSize))

	uploadID := mpCreate(t, b, key, "", "")
	out1, err := mpUploadPart(t, b, key, uploadID, 1, winner, nil)
	if err != nil {
		t.Fatalf("UploadPart 1: %v", err)
	}
	if _, err := mpUploadPart(t, b, key, uploadID, 2, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart 2: %v", err)
	}
	winners := hygienePartDigests(t, mem, uploadID, 1)
	orphans := hygienePartDigests(t, mem, uploadID, 2)

	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out1.ETag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	for _, d := range orphans {
		assertReleased(t, b, mem, rm, d, true)
	}
	for _, d := range winners {
		assertRetained(t, mem, rm, d)
	}
	if got := getRange(t, b, key, ""); !bytes.Equal(got, winner) {
		t.Fatalf("winner mismatch after orphan reap (%d bytes)", len(got))
	}
}

// TestSupersededPartReleased: re-uploading a part number releases the
// replaced part's blobs inline (last write wins at Complete).
func TestSupersededPartReleased(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	key := "supersede"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart (loser): %v", err)
	}
	old := hygienePartDigests(t, mem, uploadID, 1)

	winner := testBody(int(backend.MinPartSize) + 100)
	out2, err := mpUploadPart(t, b, key, uploadID, 1, winner, nil)
	if err != nil {
		t.Fatalf("UploadPart (winner): %v", err)
	}
	for _, d := range old {
		assertReleased(t, b, mem, rm, d, true)
	}
	for _, d := range hygienePartDigests(t, mem, uploadID, 1) {
		assertRetained(t, mem, rm, d)
	}

	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out2.ETag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got := getRange(t, b, key, ""); !bytes.Equal(got, winner) {
		t.Fatalf("winner mismatch after supersede (%d bytes)", len(got))
	}
}

// TestAbortReleasesUnclaimedBlobs: a client abort releases every part blob —
// including accepted-with-zero-claims ones, the state every part blob has
// under the NopUploader.
func TestAbortReleasesUnclaimedBlobs(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	bucket, key := "bk", "abort"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	digests := hygienePartDigests(t, mem, uploadID, 1)

	if err := b.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket: &bucket, Key: &key, UploadId: &uploadID,
	}); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if _, err := mem.GetSession(context.Background(), uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("session survived the abort (err=%v)", err)
	}
	for _, d := range digests {
		assertReleased(t, b, mem, rm, d, true)
	}
}

// TestSweepDropsCompletedSessionKeepingWinners: the sweeper drops a
// completed session's row without touching its winners — they hold reference
// claims and belong to the object.
func TestSweepDropsCompletedSessionKeepingWinners(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	ctx := context.Background()
	key := "done"
	data := testBody(int(backend.MinPartSize))

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, data, nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	winners := blobDigestsOf(t, b, key, "")

	if n, err := sweepUploads(ctx, b); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("completed session survived the sweep (err=%v)", err)
	}
	for _, d := range winners {
		assertRetained(t, mem, rm, d)
	}
	if got := getRange(t, b, key, ""); !bytes.Equal(got, data) {
		t.Fatalf("object unreadable after the sweep (%d bytes)", len(got))
	}
}

// TestCompleteFailsClosedOnMissingParamsRow: a part recorded before parts
// kept their blobs' plaintext lengths has them derived at Complete. A part
// blob whose enc-params row is gone then fails Complete (blobPlaintextLen
// cannot derive the plaintext span) instead of committing a manifest with an
// envelope-sized span; the session reverts to open.
func TestCompleteFailsClosedOnMissingParamsRow(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	key := "failclosed"

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	if err := mem.DeleteEncryptionParams(ctx, mem.bk(), d); err != nil {
		t.Fatalf("DeleteEncryptionParams: %v", err)
	}
	// The part as a row from before the lengths were kept.
	part := partRecord(t, mem, uploadID, 1)
	part.SpanLengths = nil
	trees, err := mem.ListPartTrees(ctx, uploadID)
	if err != nil {
		t.Fatalf("ListPartTrees: %v", err)
	}
	if _, err := mem.PutPart(ctx, part, trees[1], registry.ReleaseTask{}); err != nil {
		t.Fatalf("PutPart: %v", err)
	}

	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil); err == nil {
		t.Fatal("Complete over a missing enc-params row succeeded — a corrupt span would have committed")
	} else if !strings.Contains(err.Error(), "no encryption-params row") {
		t.Fatalf("Complete error = %v, want the missing-row rejection", err)
	}
	sess, err := mem.GetSession(ctx, uploadID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.State != registry.SessionOpen {
		t.Fatalf("session state = %q after failed Complete, want open (abortable)", sess.State)
	}
}

// TestDrainSpaceReleasesIgnoresGrace: DeleteBucket must be able to execute a
// space's pending releases immediately — deleted objects' blobs would
// otherwise stay registered behind the reader grace and hilt would refuse
// the space deletion.
func TestDrainSpaceReleasesIgnoresGrace(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	ctx := context.Background()
	data := testBody(1 << 10)

	putObj(t, b, "k1", data)
	d := blobDigestOf(t, b, "k1", "")
	// A live grace: the delete queues the blob's release an hour out, so
	// neither a worker nor the ordinary sweep touches it.
	b.releaseGrace = time.Hour
	deleteObj(t, b, "k1")

	if n, err := b.SweepPendingReleases(ctx); err != nil || n != 0 {
		t.Fatalf("sweep executed %d releases (err=%v), want 0 — intent should not be due", n, err)
	}

	if err := b.drainSpaceReleases(ctx, mem.bk()); err != nil {
		t.Fatalf("drainSpaceReleases: %v", err)
	}
	if _, err := mem.GetEncryptionParams(ctx, mem.bk(), d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("enc-params row survived the space drain (err=%v)", err)
	}
	if rm.removedDigests()[string(d)] != 1 {
		t.Fatalf("expected one RemoveBlob from the space drain; got %v", rm.removedDigests())
	}
	assertLocalCopyReleased(t, b, mem, d)
}

// TestConcurrentCompletesReplayWinner: racing Completes of one upload must
// all return the winner's ETag — latch losers wait for the winner's terminal
// state and replay its result instead of failing with NoSuchUpload.
func TestConcurrentCompletesReplayWinner(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	key := "race"
	data := testBody(int(backend.MinPartSize))

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, data, nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	one := int32(1)
	parts := []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}

	const racers = 5
	etags := make([]string, racers)
	errs := make([]error, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := mpComplete(t, b, key, uploadID, parts, nil)
			if err != nil {
				errs[i] = err
				return
			}
			etags[i] = *res.ETag
		}(i)
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("racer %d: %v", i, errs[i])
		}
	}
	for i := 1; i < racers; i++ {
		if etags[i] != etags[0] {
			t.Fatalf("racer %d ETag %q differs from racer 0's %q", i, etags[i], etags[0])
		}
	}
	if got := getRange(t, b, key, ""); !bytes.Equal(got, data) {
		t.Fatalf("object mismatch after racing Completes (%d bytes)", len(got))
	}
}

// TestReCompleteAfterOverwriteReplaysCommittedResult: a duplicate Complete
// after a plain PUT overwrote the key replays the ETag the winning Complete
// recorded on the session — never InvalidPart from comparing against the live
// key — and leaves the overwrite in place.
func TestReCompleteAfterOverwriteReplaysCommittedResult(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	key := "replay-after-overwrite"
	data := testBody(int(backend.MinPartSize))

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, data, nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	one := int32(1)
	parts := []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}
	first, err := mpComplete(t, b, key, uploadID, parts, nil)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	overwrite := []byte("overwritten by a single-shot PUT")
	putObj(t, b, key, overwrite)

	again, err := mpComplete(t, b, key, uploadID, parts, nil)
	if err != nil {
		t.Fatalf("re-Complete after overwrite: %v", err)
	}
	if *again.ETag != *first.ETag {
		t.Fatalf("re-Complete ETag %q, want the committed %q", *again.ETag, *first.ETag)
	}
	if got := getRange(t, b, key, ""); !bytes.Equal(got, overwrite) {
		t.Fatalf("re-Complete disturbed the overwrite (%d bytes)", len(got))
	}
}

// TestCompleteWaitBudgetIsOperationAborted: a Complete whose wait for
// another Complete of its upload runs out reports OperationAborted (a 409),
// not NoSuchUpload — the upload exists and is merely slow. The other then
// commits, and a repeat replays it.
func TestCompleteWaitBudgetIsOperationAborted(t *testing.T) {
	g := newGatedConcluder()
	b, _ := newDeferredBackend(t, g)
	key := "slow-winner"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 1)
	cancelQueuedAccepts(t, b, uploadID)

	// The first Complete holds the upload while its accept concludes.
	first := make(chan error, 1)
	go func() {
		_, err := mpComplete(t, b, key, uploadID, parts, nil)
		first <- err
	}()
	<-g.entered
	saveBudget := replayWaitBudget
	replayWaitBudget = 50 * time.Millisecond
	t.Cleanup(func() { replayWaitBudget = saveBudget })

	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "OperationAborted" {
		t.Fatalf("Complete against a slow winner: err = %v, want OperationAborted", err)
	}
	close(g.open)
	if err := <-first; err != nil {
		t.Fatalf("first Complete: %v", err)
	}
	replayWaitBudget = saveBudget
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("repeated Complete: %v", err)
	}
}

// parkingUploader parks instead of accepting: UploadBlob returns no
// Location, so part blobs stay IntentParked — the provider shape the
// NopUploader cannot produce. AbortBlob calls are recorded; the abort of a
// digest in acceptedOnProvider is refused as already accepted, as a provider
// answers for a blob whose conclude ran without ingot learning of it.
type parkingUploader struct {
	testfakes.NopUploader
	mu                 sync.Mutex
	aborted            []string
	acceptedOnProvider map[string]bool
	// abortFails makes the next abortFails aborts fail outright, as an
	// unreachable upload service does; they are not recorded as aborted.
	abortFails int
}

func (p *parkingUploader) UploadBlob(_ context.Context, _ did.DID, digest multihash.Multihash, size int64, _ string) (uploader.UploadedBlob, error) {
	c := cid.NewCidV1(cid.Raw, digest)
	return uploader.UploadedBlob{Digest: digest, Size: size, AddTask: c, AcceptTask: c, PutInvocation: []byte("put")}, nil
}

// AbortBlob records the aborted blob's digest, which its add task carries.
func (p *parkingUploader) AbortBlob(_ context.Context, _ did.DID, add cid.Cid) error {
	d := add.Hash()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.abortFails > 0 {
		p.abortFails--
		return errors.New("upload service unreachable")
	}
	p.aborted = append(p.aborted, string(d))
	if p.acceptedOnProvider[string(d)] {
		return uploader.ErrBlobAccepted
	}
	return nil
}

func (p *parkingUploader) abortedDigests() map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := map[string]bool{}
	for _, d := range p.aborted {
		m[d] = true
	}
	return m
}

// newParkingBackend is newRefTestBackend with a parking deferred uploader,
// so part blobs reach IntentParked and abort exercises the /blob/abort arm.
func newParkingBackend(t *testing.T) (*Backend, *testStore, *parkingUploader) {
	t.Helper()
	pu := &parkingUploader{}
	b, mem := newDeferredBackend(t, pu)
	return b, mem, pu
}

// newDeferredBackend builds an in-process backend around a caller-supplied
// deferred uploader, so a test can observe how the completion path drives it.
// Each mod edits the wiring before the backend is built, so a test can swap
// one store for a faulty one.
func newDeferredBackend(t *testing.T, up deferredTestUploader, mods ...func(*Deps)) (*Backend, *testStore) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	mem := newTestStore(t)
	cache, err := blockstore.NewBlobCache(filepath.Join(dir, "cache"))
	if err != nil {
		t.Fatalf("cache: %v", err)
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

	deps := Deps{
		Authority:  mem,
		Registry:   mem,
		Intents:    mem,
		Locations:  mem,
		GC:         mem,
		Multipart:  mem,
		Parks:      mem,
		Reads:      blockstore.NewLayered(cache, log, testfakes.NopBaseReader{}),
		Log:        log,
		Cache:      cache,
		Deferred:   up,
		Remover:    &recordingRemover{},
		Registrar:  testfakes.NopUploader{},
		EncParams:  mem,
		RegionKeys: testRegionKeys(t),
		TenantKeys: testTenantKeys(),
		Claims:     mem,
	}
	for _, mod := range mods {
		mod(&deps)
	}
	withStreaming(t, &deps, dir)
	b := mem.newBackend(t, deps)
	if err := mem.Create(ctx, "bk", did.Undef, registry.CreateState{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return b, mem
}

// deferredTestUploader is what newDeferredBackend needs of a fake: the
// uploader seams the write paths use, and the park the streaming adapter
// hands each streamed blob to (see streamingAdapter).
type deferredTestUploader interface {
	uploader.Uploader
	uploader.DeferredBodyUploader
	UploadBlob(ctx context.Context, space did.DID, digest multihash.Multihash, size int64, localPath string) (uploader.UploadedBlob, error)
}

// TestAbortUnparksParkedBlob: abort of a genuinely parked part blob releases
// it on the provider (/blob/abort), drops the park row, and shreds the key
// row.
func TestAbortUnparksParkedBlob(t *testing.T) {
	b, mem, pu := newParkingBackend(t)
	ctx := context.Background()
	bucket, key := "bk", "parked"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	digests := hygienePartDigests(t, mem, uploadID, 1)
	for _, d := range digests {
		in, err := mem.IntentByDigest(ctx, d)
		if err != nil || in.State != registry.IntentParked {
			t.Fatalf("part blob %x intent = %v/%v, want parked", d, in, err)
		}
	}

	if err := b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: &bucket, Key: &key, UploadId: &uploadID,
	}); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	aborted := pu.abortedDigests()
	for _, d := range digests {
		if !aborted[string(d)] {
			t.Fatalf("parked blob %x was not aborted on the provider", d)
		}
		if _, err := mem.ParkByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("park row for %x survived (err=%v)", d, err)
		}
		if _, err := mem.GetEncryptionParams(ctx, mem.bk(), d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("enc-params row for %x survived (err=%v)", d, err)
		}
	}
}

// haltingConcluder concludes like the parking uploader until halted; halted,
// it answers with every blob located but the last and an error, which is
// what a conclude that failed partway hands back. It records what each call
// was asked to conclude.
type haltingConcluder struct {
	parkingUploader
	halt  bool
	calls [][]string
}

func (h *haltingConcluder) ConcludeBlobs(ctx context.Context, space did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	digests := make([]string, len(parked))
	for i, p := range parked {
		digests[i] = string(p.Digest)
	}
	h.calls = append(h.calls, digests)
	locations, err := h.parkingUploader.ConcludeBlobs(ctx, space, parked)
	if err != nil || !h.halt {
		return locations, err
	}
	locations[len(locations)-1] = nil
	return locations, errors.New("upload service went away")
}

// failOncePutLocation is a location store whose next PutLocation fails,
// remembering which blob it refused; every later write goes through.
type failOncePutLocation struct {
	registry.LocationStore
	armed   bool
	refused multihash.Multihash
}

func (f *failOncePutLocation) PutLocation(ctx context.Context, loc registry.BlobLocation) error {
	if f.armed {
		f.armed = false
		f.refused = loc.Digest
		return errors.New("locations table unavailable")
	}
	return f.LocationStore.PutLocation(ctx, loc)
}

// failDeletePark is a park store that refuses its next `fails` deletes,
// leaving a row behind for a blob whose acceptance is fully recorded.
type failDeletePark struct {
	registry.ParkStore
	fails int
}

func (f *failDeletePark) DeletePark(ctx context.Context, space did.DID, digest multihash.Multihash) error {
	if f.fails > 0 {
		f.fails--
		return errors.New("parks table unavailable")
	}
	return f.ParkStore.DeletePark(ctx, space, digest)
}

// completeTwoParts uploads two distinct parts and returns what Complete needs.
func completeTwoParts(t *testing.T, b *Backend, key string) (string, []types.CompletedPart) {
	t.Helper()
	uploadID := mpCreate(t, b, key, "", "")
	var parts []types.CompletedPart
	for n := int32(1); n <= 2; n++ {
		body := testBody(int(backend.MinPartSize))
		body[0] = byte(n)
		out, err := mpUploadPart(t, b, key, uploadID, n, body, nil)
		if err != nil {
			t.Fatalf("UploadPart %d: %v", n, err)
		}
		num := n
		parts = append(parts, types.CompletedPart{PartNumber: &num, ETag: out.ETag})
	}
	return uploadID, parts
}

// assertAcceptedAndUnparked checks the tables say a blob is accepted, located,
// and holds no park row.
func assertAcceptedAndUnparked(t *testing.T, mem *testStore, d multihash.Multihash) {
	t.Helper()
	ctx := context.Background()
	// Accepted, or published once a commit has claimed it.
	if in, err := mem.IntentByDigest(ctx, d); err != nil || (in.State != registry.IntentAccepted && in.State != registry.IntentPublished) {
		t.Fatalf("blob %x intent = %v/%v, want accepted or published", d, in, err)
	}
	if loc, err := mem.GetLocation(ctx, mem.bk(), d); err != nil || loc == nil {
		t.Fatalf("blob %x has no location (err=%v)", d, err)
	}
	if _, err := mem.ParkByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for blob %x survived (err=%v)", d, err)
	}
}

// plantStaleAcceptedPark puts uploadID's two part blobs in the state another
// write's acceptance of the same bytes leaves them: both located and
// accepted, one park row still standing beside its location. It returns the
// digests and the one still parked.
func plantStaleAcceptedPark(t *testing.T, mem *testStore, uploadID string) (digests []multihash.Multihash, stale multihash.Multihash) {
	t.Helper()
	ctx := context.Background()
	sess, err := mem.GetSession(ctx, uploadID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	digests = hygienePartDigests(t, mem, uploadID, 1)
	digests = append(digests, hygienePartDigests(t, mem, uploadID, 2)...)
	for i, d := range digests {
		loc := registry.BlobLocation{Space: sess.Space, Digest: d, Provider: "did:key:zNode", URL: "https://node.example/blob", Size: 1}
		if err := mem.PutLocation(ctx, loc); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		if i > 0 {
			if err := mem.DeletePark(ctx, sess.Space, d); err != nil {
				t.Fatalf("DeletePark: %v", err)
			}
		}
		if err := mem.SetIntentState(ctx, sess.Space, d, registry.IntentAccepted); err != nil {
			t.Fatalf("SetIntentState: %v", err)
		}
	}
	return digests, digests[0]
}

// TestCompleteDropsStaleParkOfLocatedBlob: a blob recorded as accepted whose
// park row was never dropped is located, so Complete neither concludes it
// again nor leaves the park row standing.
func TestCompleteDropsStaleParkOfLocatedBlob(t *testing.T) {
	hc := &haltingConcluder{}
	b, mem := newDeferredBackend(t, hc)
	key := "stale-park"
	uploadID, parts := completeTwoParts(t, b, key)
	digests, _ := plantStaleAcceptedPark(t, mem, uploadID)

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(hc.calls) != 0 {
		t.Fatalf("conclude calls = %v, want none: every blob is located", hc.calls)
	}
	for _, d := range digests {
		assertAcceptedAndUnparked(t, mem, d)
	}
}

// TestSweepDropsStaleParkOfAcceptedBlob: when no Complete ever retries, the
// sweeper reaps the session and drops the stale park row along with it. The
// blob is accepted, so it is released through the accepted path and never
// aborted on the provider as if it were parked.
func TestSweepDropsStaleParkOfAcceptedBlob(t *testing.T) {
	hc := &haltingConcluder{}
	b, mem := newDeferredBackend(t, hc)
	ctx := context.Background()
	key := "sweep-stale-park"
	uploadID, _ := completeTwoParts(t, b, key)
	digests, _ := plantStaleAcceptedPark(t, mem, uploadID)

	if n, err := sweepUploads(ctx, b); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	aborted := hc.abortedDigests()
	for _, d := range digests {
		if _, err := mem.ParkByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("park row for accepted blob %x survived the sweep (err=%v)", d, err)
		}
		if aborted[string(d)] {
			t.Fatalf("accepted blob %x was aborted on the provider as though parked", d)
		}
	}
}

// TestReleaseSweepDropsParkTheSessionSweepCouldNot: an accepted blob's stale
// park, which the session sweep cannot drop, is dropped by the release the
// sweep queues, with the location and the intent, once the session row that
// indexed it is gone.
func TestReleaseSweepDropsParkTheSessionSweepCouldNot(t *testing.T) {
	hc := &haltingConcluder{}
	parks := &failDeletePark{}
	b, mem := newDeferredBackend(t, hc, func(d *Deps) {
		parks.ParkStore = d.Parks
		d.Parks = parks
	})
	ctx := context.Background()
	key := "release-drops-park"
	uploadID, _ := completeTwoParts(t, b, key)
	_, stale := plantStaleAcceptedPark(t, mem, uploadID)

	// The session sweep's delete fails.
	parks.fails = 1
	if n, err := sweepUploads(ctx, b); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	drainReleases(t, b)
	if _, err := mem.ParkByDigest(ctx, stale); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for %x survived the release sweep (err=%v)", stale, err)
	}
	if _, err := mem.GetLocation(ctx, mem.bk(), stale); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("location for %x survived the release sweep (err=%v)", stale, err)
	}
	if _, err := mem.IntentByDigest(ctx, stale); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("intent for %x survived the release sweep (err=%v)", stale, err)
	}
}

// locatedEverywhere is a location store that reports every digest as already
// located, which is what an UploadPart sees when its content was accepted
// before (part bodies are encrypted per upload, so the same plaintext cannot
// reproduce a digest from outside).
type locatedEverywhere struct {
	registry.LocationStore
}

func (l locatedEverywhere) GetLocation(ctx context.Context, space did.DID, digest multihash.Multihash) (*registry.BlobLocation, error) {
	loc, err := l.LocationStore.GetLocation(ctx, space, digest)
	if errors.Is(err, registry.ErrNotFound) {
		return &registry.BlobLocation{Space: space, Digest: digest, Provider: "did:key:zNode", URL: "https://node.example/blob", Size: 1}, nil
	}
	return loc, err
}

// TestUploadPartToleratesFailedStaleParkDelete: a part whose content is
// already located is durable the moment its dedup is recorded, so failing to
// drop a stale park row for it must not fail the write. Complete's dedup path
// drops the row instead.
func TestUploadPartToleratesFailedStaleParkDelete(t *testing.T) {
	hc := &haltingConcluder{}
	parks := &failDeletePark{fails: 1}
	b, mem := newDeferredBackend(t, hc, func(d *Deps) {
		parks.ParkStore = d.Parks
		d.Parks = parks
		d.Locations = locatedEverywhere{LocationStore: d.Locations}
	})
	ctx := context.Background()
	key := "dedup-part"
	one := int32(1)

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	if err != nil {
		t.Fatalf("UploadPart of a located blob failed on a park delete: %v", err)
	}
	if parks.fails > 0 {
		t.Fatal("the park store never refused a delete")
	}
	digests := hygienePartDigests(t, mem, uploadID, 1)
	if len(digests) != 1 {
		t.Fatalf("part digests = %d, want 1", len(digests))
	}
	d := digests[0]
	if in, err := mem.IntentByDigest(ctx, d); err != nil || in.State != registry.IntentAccepted {
		t.Fatalf("dedup part intent = %v/%v, want accepted", in, err)
	}
	// The fake answers located; the claim at Complete reads the row itself.
	if err := mem.PutLocation(ctx, registry.BlobLocation{Space: mem.bk(), Digest: d, Provider: "did:key:zNode", URL: "https://node.example/blob", Size: 1}); err != nil {
		t.Fatalf("PutLocation: %v", err)
	}

	// The stale row such a failure leaves behind, as a Complete that failed
	// after recording the acceptance would.
	if _, err := mem.PutPark(ctx, registry.BlobPark{Space: mem.bk(), Digest: d, AddTask: []byte{1}, AcceptTask: []byte{1}, PutInvocation: []byte{1}, Size: 1}); err != nil {
		t.Fatalf("PutPark: %v", err)
	}
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(hc.calls) != 0 {
		t.Fatalf("conclude calls = %v, want none: the blob was located throughout", hc.calls)
	}
	if _, err := mem.ParkByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("stale park row survived Complete (err=%v)", err)
	}
}

// TestSweepReleasesBlobTheProviderHoldsAccepted: a conclude ran but ingot
// never learned of it (the response was lost, or Complete died between the
// accept and recording it), so the blob is parked locally and accepted on the
// provider. When the session expires the abort is refused as already
// accepted; the sweeper must then release the blob as accepted rather than
// leave the provider holding an allocation nothing will ever revisit.
func TestSweepReleasesBlobTheProviderHoldsAccepted(t *testing.T) {
	pu := &parkingUploader{acceptedOnProvider: map[string]bool{}}
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, pu, func(d *Deps) { d.Remover = rm })
	ctx := context.Background()

	uploadID := mpCreate(t, b, "lost-accept", "", "")
	if _, err := mpUploadPart(t, b, "lost-accept", uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	digests := hygienePartDigests(t, mem, uploadID, 1)
	if len(digests) != 1 {
		t.Fatalf("part digests = %d, want 1", len(digests))
	}
	d := digests[0]
	if in, err := mem.IntentByDigest(ctx, d); err != nil || in.State != registry.IntentParked {
		t.Fatalf("part blob intent = %v/%v, want parked", in, err)
	}
	pu.acceptedOnProvider[string(d)] = true

	if n, err := sweepUploads(ctx, b); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("abort was never attempted for %x", d)
	}
	if _, err := mem.ParkByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for %x survived the sweep (err=%v)", d, err)
	}
	// Released as an accepted blob: the network claim is removed.
	assertReleased(t, b, mem, rm, d, true)
}

// failRemover refuses its next `fails` network removes, recording the rest.
type failRemover struct {
	*recordingRemover
	fails int
}

func (f *failRemover) RemoveBlob(ctx context.Context, space did.DID, d multihash.Multihash) error {
	if f.fails > 0 {
		f.fails--
		return errors.New("upload service unavailable")
	}
	return f.recordingRemover.RemoveBlob(ctx, space, d)
}

// TestReleaseOfABlobAnInFlightPartReferencesIsStale: a release of a digest a
// part of an open session references does nothing, since the part is a
// reference, and the blob stays parked through the session's Complete.
func TestReleaseOfABlobAnInFlightPartReferencesIsStale(t *testing.T) {
	b, mem, pu := newParkingBackend(t)
	ctx := context.Background()
	key := "in-flight"

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	if err := mem.EnqueueRelease(ctx, mem.bk(), d, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("EnqueueRelease: %v", err)
	}

	if n, err := b.SweepPendingReleases(ctx); err != nil || n != 0 {
		t.Fatalf("sweep executed %d releases (err=%v), want 0: the part is in flight", n, err)
	}
	if _, err := mem.ParkByDigest(ctx, d); err != nil {
		t.Fatalf("park row for the in-flight part was taken by the release sweep: %v", err)
	}
	if pu.abortedDigests()[string(d)] {
		t.Fatalf("in-flight part blob %x was aborted on the provider", d)
	}
	if pending, err := mem.ListReleasesBySpace(ctx, mem.bk()); err != nil || len(pending) != 0 {
		t.Fatalf("pending releases = %v/%v, want none: the release was stale", pending, err)
	}

	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if _, err := mem.GetEncryptionParams(ctx, mem.bk(), d); err != nil {
		t.Fatalf("claimed blob %x lost its enc-params row: %v", d, err)
	}
}

// TestSupersedeRecordsReleasesWithTheWrite: a re-upload of a part records
// the superseded blobs' releases in the write that replaces the part, so a
// write that fails leaves the old part intact and nothing released, and one
// that lands has the releases recorded with it.
func TestSupersedeRecordsReleasesWithTheWrite(t *testing.T) {
	pu := &parkingUploader{}
	mp := &failPutPart{}
	b, mem := newDeferredBackend(t, pu, func(d *Deps) {
		mp.MultipartStore = d.Multipart
		d.Multipart = mp
	})
	ctx := context.Background()
	key := "supersede-record"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart (first): %v", err)
	}
	old := hygienePartDigests(t, mem, uploadID, 1)

	mp.fails = 1
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)+100), nil); err == nil {
		t.Fatal("re-upload succeeded although the part could not be recorded")
	}
	if got := hygienePartDigests(t, mem, uploadID, 1); len(got) != len(old) || string(got[0]) != string(old[0]) {
		t.Fatalf("part 1 blobs after the failed re-upload = %x, want the old part intact %x", got, old)
	}
	for _, d := range old {
		if _, err := mem.ParkByDigest(ctx, d); err != nil {
			t.Fatalf("old blob %x lost its park on a failed re-upload: %v", d, err)
		}
		if pu.abortedDigests()[string(d)] {
			t.Fatalf("old blob %x was aborted on a failed re-upload", d)
		}
	}
	drainReleases(t, b)
	for _, d := range old {
		if pu.abortedDigests()[string(d)] {
			t.Fatalf("a failed re-upload recorded the old blob %x for release", d)
		}
	}

	// With the store back, the re-upload lands and the old blobs go.
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)+100), nil); err != nil {
		t.Fatalf("UploadPart (retry): %v", err)
	}
	for _, d := range old {
		if !pu.abortedDigests()[string(d)] {
			t.Fatalf("superseded blob %x was not released on the provider", d)
		}
		if _, err := mem.ParkByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("park row for superseded %x survived (err=%v)", d, err)
		}
	}
}

// TestReleaseKeepsRowsUntilTheNetworkStepSucceeds: a release whose network
// remove fails leaves the location row in place, so the retry reads the same
// state and takes the same step; the rows go only once the network holds
// nothing.
func TestReleaseKeepsRowsUntilTheNetworkStepSucceeds(t *testing.T) {
	rm := &failRemover{recordingRemover: &recordingRemover{}, fails: 1}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) { d.Remover = rm })
	ctx := context.Background()
	key := "network-retry"

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	deleteObj(t, b, key)

	if n, err := b.SweepPendingReleases(ctx); err != nil || n != 0 {
		t.Fatalf("first sweep executed %d releases (err=%v), want 0: the network remove failed", n, err)
	}
	if loc, err := mem.GetLocation(ctx, mem.bk(), d); err != nil || loc == nil {
		t.Fatalf("location row went although the network still holds the blob (err=%v)", err)
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, mem.bk()); len(pending) != 1 {
		t.Fatalf("release record after the failed attempt = %v, want it kept", pending)
	}
	// A part keeps no local copy; the intent is what the retry needs.
	// TestReleaseFreesCachedCopyOfDeletedPutObject covers a cached copy kept
	// for the retry.
	if in, err := mem.IntentByDigest(ctx, d); err != nil || in.State != registry.IntentPublished {
		t.Fatalf("intent after the failed attempt = %v/%v, want published and kept for the retry", in, err)
	}

	if n, err := b.SweepPendingReleases(ctx); err != nil || n != 1 {
		t.Fatalf("second sweep executed %d releases (err=%v), want 1", n, err)
	}
	if rm.removedDigests()[string(d)] != 1 {
		t.Fatalf("RemoveBlob calls for %x = %d, want 1", d, rm.removedDigests()[string(d)])
	}
	if _, err := mem.GetLocation(ctx, mem.bk(), d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("location row survived the release (err=%v)", err)
	}
	assertLocalCopyReleased(t, b, mem, d)
}

// TestReleaseFreesCachedCopyOfDeletedPutObject: deleting a single-PUT object
// frees its local disk once its release finishes. A release whose network
// remove fails keeps the intent and cached copy for the retry.
func TestReleaseFreesCachedCopyOfDeletedPutObject(t *testing.T) {
	rm := &failRemover{recordingRemover: &recordingRemover{}, fails: 1}
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) { d.Remover = rm })
	ctx := context.Background()
	key := "put-delete"

	putObj(t, b, key, testBody(1<<10))
	d := blobDigestOf(t, b, key, "")
	deleteObj(t, b, key)

	if n, err := b.SweepPendingReleases(ctx); err != nil || n != 0 {
		t.Fatalf("first sweep executed %d releases (err=%v), want 0: the network remove failed", n, err)
	}
	if in, err := mem.IntentByDigest(ctx, d); err != nil || in.State != registry.IntentPublished {
		t.Fatalf("intent after the failed attempt = %v/%v, want published and kept for the retry", in, err)
	}
	if _, err := os.Stat(localPath(b, d)); err != nil {
		t.Fatalf("cached copy after the failed attempt: %v, want it kept for the retry", err)
	}

	if n, err := b.SweepPendingReleases(ctx); err != nil || n != 1 {
		t.Fatalf("second sweep executed %d releases (err=%v), want 1", n, err)
	}
	settleCache(t, b, d) // the release evicts the cached copy through the queue
	assertLocalCopyReleased(t, b, mem, d)
}

// assertLocalCopyReleased fails the test unless the blob's upload intent and
// cached copy are both gone, as a finished release leaves them. The copy goes
// through the queue-shard.evict the release queues, which runs after it, so
// it is waited for.
func assertLocalCopyReleased(t *testing.T, b *Backend, mem *testStore, d multihash.Multihash) {
	t.Helper()
	if in, err := mem.IntentByDigest(context.Background(), d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("blob %x intent = %v/%v after its release, want it deleted", d, in, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := os.Stat(localPath(b, d))
		if os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("blob %x cached copy after its release: stat err=%v, want not-exist", d, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCompletedSessionReapKeepsWinnersOfDeletedObject: a completed session
// is retained after its object is deleted. Its winners' releases were
// recorded by the delete as ordinary releases; the reap must not re-record
// them as part blobs. Their intents were published by their claims, so the
// reap leaves them to the object's own releases, which remove each winner
// once and then drop its intent and cached copy.
func TestCompletedSessionReapKeepsWinnersOfDeletedObject(t *testing.T) {
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) { d.Remover = rm })
	ctx := context.Background()
	key := "deleted-winners"
	uploadID, parts := completeTwoParts(t, b, key)
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	winners := blobDigestsOf(t, b, key, "")
	deleteObj(t, b, key)

	if n, err := sweepUploads(ctx, b); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	drainReleases(t, b)
	for _, d := range winners {
		if rm.removedDigests()[string(d)] != 1 {
			t.Fatalf("winner %x removed %d times, want once by the object's own release", d, rm.removedDigests()[string(d)])
		}
		assertLocalCopyReleased(t, b, mem, d)
	}
}

// TestReapAfterDeletedObjectReleased: the reverse order of
// TestCompletedSessionReapKeepsWinnersOfDeletedObject. The object's release
// runs first and deletes its winners' intents; the completed session's reap
// then finds no intent, treats each winner as already released, and records
// no second release.
func TestReapAfterDeletedObjectReleased(t *testing.T) {
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) { d.Remover = rm })
	ctx := context.Background()
	key := "released-then-reaped"
	uploadID, parts := completeTwoParts(t, b, key)
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	winners := blobDigestsOf(t, b, key, "")
	deleteObj(t, b, key)
	drainReleases(t, b)
	for _, d := range winners {
		assertLocalCopyReleased(t, b, mem, d)
	}

	if n, err := sweepUploads(ctx, b); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, mem.bk()); len(pending) != 0 {
		t.Fatalf("reap after the release recorded %d releases, want none", len(pending))
	}
	drainReleases(t, b)
	for _, d := range winners {
		if rm.removedDigests()[string(d)] != 1 {
			t.Fatalf("winner %x removed %d times, want once by the object's own release", d, rm.removedDigests()[string(d)])
		}
	}
}

// TestAbortRecordsBlobAnotherAbortingSessionReferences: two sessions share a
// part blob and both are torn down. Skipping the blob because the other
// session's part still references it would let both skip it, and both would
// cascade their rows away. Each abort queues it regardless, and the release
// itself sees whether an open session still needs it.
func TestAbortRecordsBlobAnotherAbortingSessionReferences(t *testing.T) {
	b, mem, pu := newParkingBackend(t)
	ctx := context.Background()
	bucket, key := "bk", "shared"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	// A second session referencing the same blob, aborted first: its
	// release is stale while this session's part still names the blob.
	const other = "other-session"
	if err := mem.CreateSession(ctx, registry.MultipartSession{UploadID: other, Bucket: bucket, Space: mem.bk(), ObjectKey: "other"}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := mem.PutPart(ctx, registry.MultipartPart{UploadID: other, PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{d}, State: registry.PartParked}, nil, registry.ReleaseTask{}); err != nil {
		t.Fatalf("PutPart: %v", err)
	}
	if err := b.abortUpload(ctx, registry.MultipartSession{UploadID: other, Space: mem.bk()}); err != nil {
		t.Fatalf("abort other: %v", err)
	}
	if pu.abortedDigests()[string(d)] {
		t.Fatalf("shared blob %x released while this session's part still names it", d)
	}

	if err := b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("shared blob %x was not released: the other session's part reference stopped the record", d)
	}
	if _, err := mem.ParkByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for %x survived (err=%v)", d, err)
	}
}

// TestReleaseWaitsForAnotherOpenSessionSharingTheBlob: the same shared blob,
// but the other session is still open. The abort's release is stale, since
// that session's part references the blob, and that session's own abort
// releases it.
func TestReleaseWaitsForAnotherOpenSessionSharingTheBlob(t *testing.T) {
	b, mem, pu := newParkingBackend(t)
	ctx := context.Background()
	bucket, key := "bk", "shared-open"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	const other = "other-open"
	if err := mem.CreateSession(ctx, registry.MultipartSession{UploadID: other, Bucket: bucket, Space: mem.bk(), ObjectKey: "other"}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := mem.PutPart(ctx, registry.MultipartPart{UploadID: other, PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{d}, State: registry.PartParked}, nil, registry.ReleaseTask{}); err != nil {
		t.Fatalf("PutPart: %v", err)
	}

	if err := b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if pu.abortedDigests()[string(d)] {
		t.Fatalf("shared blob %x was released while another open session references it", d)
	}
	if _, err := mem.ParkByDigest(ctx, d); err != nil {
		t.Fatalf("park row for %x went while another open session references it: %v", d, err)
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, mem.bk()); len(pending) != 0 {
		t.Fatalf("pending releases = %v, want none: the release was stale", pending)
	}
	otherKey := "other"
	otherID := other
	if err := b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &otherKey, UploadId: &otherID}); err != nil {
		t.Fatalf("Abort the other session: %v", err)
	}
	drainReleases(t, b)
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("shared blob %x was not released once no session referenced it", d)
	}
}

// TestSweepReleasesSessionThatOutlivedItsBucket: a session created while its
// bucket was being deleted outlives the bucket row. Its space was recorded at
// create, so the sweep still records and runs its parts' releases against
// that space rather than dropping the only index to the blobs.
func TestSweepReleasesSessionThatOutlivedItsBucket(t *testing.T) {
	pu := &parkingUploader{}
	b, mem := newDeferredBackend(t, pu)
	ctx := context.Background()
	bucket, key := "doomed", "part"
	space, err := did.Parse("did:web:doomed.example")
	if err != nil {
		t.Fatalf("did.Parse: %v", err)
	}
	if err := mem.Create(ctx, bucket, space, registry.CreateState{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	res, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := res.UploadId
	one := int32(1)
	if _, err := b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &one, Body: bytes.NewReader(testBody(int(backend.MinPartSize))), ContentLength: byteLen(testBody(int(backend.MinPartSize)))}); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	if sess, err := mem.GetSession(ctx, uploadID); err != nil || sess.Space != space {
		t.Fatalf("session space = %v/%v, want %v recorded at create", sess, err, space)
	}

	// The bucket row goes while the session stands.
	if err := mem.Delete(ctx, bucket, space); err != nil {
		t.Fatalf("delete bucket row: %v", err)
	}
	if n, err := sweepUploads(ctx, b); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("session survived the sweep (err=%v)", err)
	}
	if spaces := releaseSpaces(t, mem); !slices.Contains(spaces, space) {
		t.Fatalf("releases queued against %v, want the session's space %v", spaces, space)
	}
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("parked blob %x was not released on the provider", d)
	}
	if _, err := mem.ParkByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for %x survived (err=%v)", d, err)
	}
}

// TestRecreatedBucketDisownsItsPredecessorsUploads: the session's bucket is
// deleted and recreated under the same name with a new space. The parts were
// parked in the old space, so the new bucket does not know the upload: parts,
// Complete and Abort all report NoSuchUpload, and the sweeper tears the
// session down against the space recorded on it, never the new bucket's.
func TestRecreatedBucketDisownsItsPredecessorsUploads(t *testing.T) {
	pu := &parkingUploader{}
	b, mem := newDeferredBackend(t, pu)
	ctx := context.Background()
	bucket, key := "reborn", "part"
	oldSpace, _ := did.Parse("did:web:reborn-old.example")
	newSpace, _ := did.Parse("did:web:reborn-new.example")
	if err := mem.Create(ctx, bucket, oldSpace, registry.CreateState{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	res, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := res.UploadId
	one := int32(1)
	out, err := b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &one, Body: bytes.NewReader(testBody(int(backend.MinPartSize))), ContentLength: byteLen(testBody(int(backend.MinPartSize)))})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	// Same name, new space.
	if err := mem.Delete(ctx, bucket, oldSpace); err != nil {
		t.Fatalf("delete bucket row: %v", err)
	}
	if err := mem.Create(ctx, bucket, newSpace, registry.CreateState{}); err != nil {
		t.Fatalf("recreate bucket: %v", err)
	}

	wantNoSuchUpload := func(op string, err error) {
		t.Helper()
		var apiErr s3err.APIError
		if !errors.As(err, &apiErr) || apiErr.Code != "NoSuchUpload" {
			t.Fatalf("%s against the recreated bucket: err = %v, want NoSuchUpload", op, err)
		}
	}
	two := int32(2)
	_, err = b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &two, Body: bytes.NewReader(testBody(int(backend.MinPartSize))), ContentLength: byteLen(testBody(int(backend.MinPartSize)))})
	wantNoSuchUpload("UploadPart", err)
	_, _, err = b.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}}})
	wantNoSuchUpload("Complete", err)
	wantNoSuchUpload("Abort", b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}))
	// Nor does the new owner see the predecessor's keys and upload ids.
	listed, err := b.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(listed.Uploads) != 0 {
		t.Fatalf("the recreated bucket lists %d of its predecessor's uploads, want 0: %+v", len(listed.Uploads), listed.Uploads)
	}
	if _, err := mem.GetSession(ctx, uploadID); err != nil {
		t.Fatalf("the disowned session should stand for the sweeper: %v", err)
	}
	// Deleting the new bucket neither waits on the predecessor's upload nor
	// tears it down.
	del := context.WithValue(ctx, reqscope.RequestKey(), cmds3.Request{})
	if err := b.DeleteBucket(del, bucket); err != nil {
		t.Fatalf("DeleteBucket of the recreated bucket: %v", err)
	}
	if _, err := mem.GetSession(ctx, uploadID); err != nil {
		t.Fatalf("the predecessor's session after the new bucket's delete: %v", err)
	}

	if n, err := sweepUploads(ctx, b); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if spaces := releaseSpaces(t, mem); !slices.Contains(spaces, oldSpace) || slices.Contains(spaces, newSpace) {
		t.Fatalf("releases queued against %v, want the session's own space %v and not %v", spaces, oldSpace, newSpace)
	}
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("parked blob %x was not released on the provider", d)
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("session survived the sweep (err=%v)", err)
	}
}

// teardownBeforePutPart is an upload registrar that runs hook once, at the
// second capture of /blob/remove authority after it is armed: UploadPart
// captures it to park the part's blobs and again for the part's add-part,
// right before it runs, so the hook lands in the window after UploadPart
// streamed its blobs and before its part row, where a teardown can take the
// session.
type teardownBeforePutPart struct {
	uploader.UploadRegistrar
	hook     func()
	captures int
}

func (s *teardownBeforePutPart) CaptureAuthority(ctx context.Context, space did.DID, cmd ucan.Command) ([]byte, error) {
	if cmd == blobcmds.Remove.Command && s.hook != nil {
		if s.captures++; s.captures == 2 {
			hook := s.hook
			s.hook = nil
			hook()
		}
	}
	return s.UploadRegistrar.CaptureAuthority(ctx, space, cmd)
}

// TestUploadPartRefusedAfterTeardownReleasesItsBlobs: a sweep takes the
// session after an UploadPart was admitted and streamed its blobs. The part
// row is refused, since the session is no longer open, the upload fails
// with NoSuchUpload, and the blobs it streamed are released rather than
// left behind with no row pointing at them.
func TestUploadPartRefusedAfterTeardownReleasesItsBlobs(t *testing.T) {
	mp := &teardownBeforePutPart{}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		mp.UploadRegistrar = d.Registrar
		d.Registrar = mp
	})
	ctx := context.Background()
	key := "refused-part"
	uploadID := mpCreate(t, b, key, "", "")
	mp.hook = func() {
		if n, err := sweepUploads(ctx, b); err != nil || n != 1 {
			t.Errorf("sweep before the part row: cleaned=%d err=%v, want the open session", n, err)
		}
	}

	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "NoSuchUpload" {
		t.Fatalf("UploadPart whose session was taken: err = %v, want NoSuchUpload", err)
	}
	if mp.hook != nil {
		t.Fatal("the teardown never ran")
	}
	drainReleases(t, b)
	entries, err := os.ReadDir(b.cache.Path(nil))
	if err != nil {
		t.Fatalf("read cache dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("cache holds %d blobs after the refused part was released, want 0", len(entries))
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, mem.bk()); len(pending) != 0 {
		t.Fatalf("pending releases after the refused part = %v, want none", pending)
	}
}

// failPutPart is a multipart store whose next `fails` part writes fail with
// a store error, as an unreachable registry does.
type failPutPart struct {
	registry.MultipartStore
	fails int
}

func (f *failPutPart) PartWrites(ctx context.Context, p registry.MultipartPart, tree *registry.PartTree, release registry.ReleaseTask) (registry.CommitWrites, []multihash.Multihash, error) {
	if f.fails > 0 {
		f.fails--
		return registry.CommitWrites{}, nil, errors.New("parts table unavailable")
	}
	return f.MultipartStore.PartWrites(ctx, p, tree, release)
}

// TestUploadPartFailedRowWriteReleasesItsBlobs: the part row write fails for
// a reason other than the session being gone. No row takes the write's
// references to the parked blobs over, so the upload expires them before
// failing, and the releases that leaves abort the blobs on the provider they
// streamed to, with the request's authority; the session stays open for the
// retry.
func TestUploadPartFailedRowWriteReleasesItsBlobs(t *testing.T) {
	rm := &recordingRemover{}
	mp := &failPutPart{fails: 1}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		mp.MultipartStore = d.Multipart
		d.Multipart = mp
		d.Remover = rm
	})
	ctx := context.Background()
	key := "failed-row"
	uploadID := mpCreate(t, b, key, "", "")

	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err == nil {
		t.Fatal("UploadPart succeeded although the part row write failed")
	}
	if mp.fails > 0 {
		t.Fatal("the part store never refused a write")
	}
	drainReleases(t, b)
	entries, err := os.ReadDir(b.cache.Path(nil))
	if err != nil {
		t.Fatalf("read cache dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("cache holds %d blobs after the failed row write, want 0", len(entries))
	}
	if n := abortCount(b); n != 1 {
		t.Fatalf("aborted %d parks, want the one that streamed", n)
	}
	if sess, err := mem.GetSession(ctx, uploadID); err != nil || sess.State != registry.SessionOpen {
		t.Fatalf("session after the failed part = %v/%v, want still open", sess, err)
	}
	// The retry lands.
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("retry UploadPart: %v", err)
	}
}

// failShred is an encryption-params store whose next `fails` deletes fail,
// as a registry outage does during a release's crypto-shred.
type failShred struct {
	registry.EncryptionParamsStore
	fails int
}

func (f *failShred) DeleteEncryptionParams(ctx context.Context, space did.DID, digest multihash.Multihash) error {
	if f.fails > 0 {
		f.fails--
		return errors.New("encryption params table unavailable")
	}
	return f.EncryptionParamsStore.DeleteEncryptionParams(ctx, space, digest)
}

// TestReleaseKeepsIntentUntilLocalCleanupSucceeds: a release of a parked
// part blob fails at its crypto-shred. The intent and the park must survive
// that attempt; the retry then aborts the blob on the provider and finishes
// the local cleanup.
func TestReleaseKeepsIntentUntilLocalCleanupSucceeds(t *testing.T) {
	rm := &recordingRemover{}
	mp := &failPutPart{fails: 1}
	shred := &failShred{}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		mp.MultipartStore = d.Multipart
		d.Multipart = mp
		shred.EncryptionParamsStore = d.EncParams
		d.EncParams = shred
		d.Remover = rm
	})
	ctx := context.Background()
	key := "shred-fails"
	uploadID := mpCreate(t, b, key, "", "")
	// The in-request release attempt fails its shred; the record stays.
	shred.fails = 1
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err == nil {
		t.Fatal("UploadPart succeeded although the part row write failed")
	}
	if shred.fails > 0 {
		t.Fatal("the shred was never attempted")
	}
	pending, _ := mem.ListReleasesBySpace(ctx, mem.bk())
	if len(pending) != 1 {
		t.Fatalf("pending releases after the failed attempt = %d, want 1", len(pending))
	}
	d := pending[0].Digest
	if in, err := mem.IntentByDigest(ctx, d); err != nil || in.State != registry.IntentParked {
		t.Fatalf("intent after the failed attempt = %v/%v, want kept as parked", in, err)
	}

	drainReleases(t, b)
	if _, err := mem.IntentByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("intent survived the retry (err=%v)", err)
	}
	if !abortedOn(b, d) {
		t.Fatalf("the parked blob %x was not aborted", d)
	}
}

// localPath returns where a blob's cached copy is, so a stat of it reports
// whether the blob is cached.
func localPath(b *Backend, digest multihash.Multihash) string {
	return b.cache.Path(digest)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// TestSupersedeRecordsReleaseOfSharedBlob: in a session storing parts as
// received, two parts of identical bytes share one blob. Re-uploading one of
// them queues a release for the shared blob all the same; the release is
// stale while the other part still names it, and the session's end queues it
// again.
// Skipping the record because a sibling names the digest would race a
// concurrent re-upload of that sibling, leaving the blob with no record.
func TestSupersedeRecordsReleaseOfSharedBlob(t *testing.T) {
	b, mem, pu := newParkingBackend(t)
	setNone(t, b)
	ctx := context.Background()
	key := "supersede-shared"

	uploadID := mpCreate(t, b, key, "", "")
	same := testBody(int(backend.MinPartSize))
	for n := int32(1); n <= 2; n++ {
		if _, err := mpUploadPart(t, b, key, uploadID, n, same, nil); err != nil {
			t.Fatalf("UploadPart %d: %v", n, err)
		}
	}
	shared := hygienePartDigests(t, mem, uploadID, 1)[0]
	if other := hygienePartDigests(t, mem, uploadID, 2)[0]; string(other) != string(shared) {
		t.Fatalf("parts of identical bytes stored as received have distinct blobs: %x vs %x", shared, other)
	}

	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)+100), nil); err != nil {
		t.Fatalf("UploadPart (supersede): %v", err)
	}
	// Part 2 still names the blob: the release, run as the supersede
	// returns, is stale, and the blob stays.
	drainReleases(t, b)
	if _, err := mem.ParkByDigest(ctx, shared); err != nil {
		t.Fatalf("shared blob's park taken while part 2 names it: %v", err)
	}
	if pu.abortedDigests()[string(shared)] {
		t.Fatalf("shared blob %x aborted on the provider while part 2 names it", shared)
	}
	if pending, err := mem.ListReleasesBySpace(ctx, mem.bk()); err != nil || len(pending) != 0 {
		t.Fatalf("releases after the supersede = %v (err %v), want none: stale", pending, err)
	}

	// The session ends: nothing names the blob and the release runs.
	bucket := "bk"
	if err := b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if err := b.drainSpaceReleases(ctx, mem.bk()); err != nil {
		t.Fatalf("drainSpaceReleases: %v", err)
	}
	if !pu.abortedDigests()[string(shared)] {
		t.Fatalf("shared blob %x not released once the session ended", shared)
	}
	if _, err := mem.ParkByDigest(ctx, shared); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for %x survived the session (err=%v)", shared, err)
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, mem.bk()); len(pending) != 0 {
		t.Fatalf("release records survived the session: %v", pending)
	}
}

// gatedPartListing is a multipart store whose first `parties` ListParts calls
// wait for one another after reading, so that many uploads of one part
// number all list the parts before any of them writes its row. listed
// records how many parts each gated call saw.
type gatedPartListing struct {
	registry.MultipartStore
	mu      sync.Mutex
	parties int
	met     sync.WaitGroup
	listed  []int
}

func newGatedPartListing(parties int) *gatedPartListing {
	g := &gatedPartListing{parties: parties}
	g.met.Add(parties)
	return g
}

func (g *gatedPartListing) ListParts(ctx context.Context, uploadID string) ([]registry.MultipartPart, error) {
	g.mu.Lock()
	gated := g.parties > 0
	if gated {
		g.parties--
	}
	g.mu.Unlock()
	parts, err := g.MultipartStore.ListParts(ctx, uploadID)
	if gated {
		g.mu.Lock()
		g.listed = append(g.listed, len(parts))
		g.mu.Unlock()
		g.met.Done()
		g.met.Wait()
	}
	return parts, err
}

// uploadRecorder is a parkingUploader that remembers every digest uploaded.
type uploadRecorder struct {
	*parkingUploader
	mu       sync.Mutex
	uploaded []multihash.Multihash
}

func (u *uploadRecorder) UploadBlob(ctx context.Context, space did.DID, digest multihash.Multihash, size int64, name string) (uploader.UploadedBlob, error) {
	u.mu.Lock()
	u.uploaded = append(u.uploaded, digest)
	u.mu.Unlock()
	return u.parkingUploader.UploadBlob(ctx, space, digest, size, name)
}

// TestConcurrentUploadsOfOnePartReleaseTheOverwrittenBlobs: two uploads of
// the same part number both list the parts before either writes, so neither
// sees the other's row. The second write replaces the first's row, and the
// first's blobs are named by no part. Recording the replacement records
// their releases with it; once both requests are done every blob no part
// names is released, and the part's own stay.
func TestConcurrentUploadsOfOnePartReleaseTheOverwrittenBlobs(t *testing.T) {
	pu := &uploadRecorder{parkingUploader: &parkingUploader{}}
	gate := newGatedPartListing(2)
	b, mem := newDeferredBackend(t, pu, func(d *Deps) {
		gate.MultipartStore = d.Multipart
		d.Multipart = gate
	})
	ctx := context.Background()
	key := "concurrent-part"
	uploadID := mpCreate(t, b, key, "", "")

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		body := testBody(int(backend.MinPartSize) + i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := mpUploadPart(t, b, key, uploadID, 1, body, nil)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("UploadPart: %v", err)
		}
	}

	gate.mu.Lock()
	listed := append([]int(nil), gate.listed...)
	gate.mu.Unlock()
	if len(listed) != 2 || listed[0] != 0 || listed[1] != 0 {
		t.Fatalf("gated listings saw %v parts, want both empty: neither write may land before both have listed", listed)
	}
	named := map[string]bool{}
	for _, d := range hygienePartDigests(t, mem, uploadID, 1) {
		named[string(d)] = true
	}
	pu.mu.Lock()
	uploaded := append([]multihash.Multihash(nil), pu.uploaded...)
	pu.mu.Unlock()
	var orphans []multihash.Multihash
	for _, d := range uploaded {
		if !named[string(d)] {
			orphans = append(orphans, d)
		}
	}
	if len(orphans) == 0 {
		t.Fatal("every uploaded blob is named by the part: the writes did not interleave")
	}
	// The replacing write recorded the overwritten blobs' releases with its
	// row, and ran them once its own part was parked; one its sibling still
	// held then stands recorded for the sweeper.
	recorded := map[string]bool{}
	pending, err := mem.ListReleasesBySpace(ctx, mem.bk())
	if err != nil {
		t.Fatalf("ListReleasesBySpace: %v", err)
	}
	for _, pr := range pending {
		recorded[string(pr.Digest)] = true
	}
	for _, d := range orphans {
		if _, err := mem.IntentByDigest(ctx, d); errors.Is(err, registry.ErrNotFound) && pu.abortedDigests()[string(d)] {
			continue // released already
		}
		if !recorded[string(d)] {
			t.Fatalf("blob %x of the overwritten part has no release record", d)
		}
	}

	if err := b.drainSpaceReleases(ctx, mem.bk()); err != nil {
		t.Fatalf("drainSpaceReleases: %v", err)
	}
	for _, d := range orphans {
		if !pu.abortedDigests()[string(d)] {
			t.Fatalf("overwritten blob %x not released on the provider", d)
		}
		if _, err := mem.IntentByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("intent for overwritten blob %x survived (err=%v)", d, err)
		}
	}
	for d := range named {
		if pu.abortedDigests()[d] {
			t.Fatalf("the part's own blob %x was released", d)
		}
		if _, err := mem.ParkByDigest(ctx, multihash.Multihash(d)); err != nil {
			t.Fatalf("the part's own blob %x lost its park: %v", d, err)
		}
	}
}
