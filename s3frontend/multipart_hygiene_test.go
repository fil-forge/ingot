package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"os"
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
	"github.com/fil-forge/versitygw/s3response"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// Multipart hygiene: every path that orphans a part blob — Complete omitting
// it, supersede, abort, and the sweeper — must release it fully: enc-params
// row (the crypto-shred), upload intent, park row, and the network copy.
// Under the NopUploader every part blob is IntentAccepted (there is no
// network to park on), so these tests exercise the accepted-at-zero-claims
// release arm; the parked arm is covered by the parkingUploader tests below
// and the Docker itests.

// hygienePartDigests reads the blob digests recorded for uploadID's part n.
func hygienePartDigests(t *testing.T, mem *inmem.MemStore, uploadID string, n int) []multihash.Multihash {
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

// drainReleases executes every due deferred release (unit backends run with
// a zero release grace, so everything enqueued is immediately due).
func drainReleases(t *testing.T, b *Backend) {
	t.Helper()
	if _, err := b.SweepPendingReleases(context.Background()); err != nil {
		t.Fatalf("SweepPendingReleases: %v", err)
	}
}

// assertReleased drains the deferred-release queue, then asserts a digest's
// enc-params row and upload intent are gone and (wantRemoved) its network
// release was invoked.
func assertReleased(t *testing.T, b *Backend, mem *inmem.MemStore, rm *recordingRemover, d multihash.Multihash, wantRemoved bool) {
	t.Helper()
	drainReleases(t, b)
	ctx := context.Background()
	if _, err := mem.GetEncryptionParams(ctx, did.Undef, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("enc-params row for %x survived (err=%v) — crypto-shred missing", d, err)
	}
	if _, err := mem.GetIntent(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("upload intent for %x survived (err=%v)", d, err)
	}
	if got := rm.removedDigests()[string(d)] > 0; got != wantRemoved {
		t.Fatalf("RemoveBlob(%x) invoked = %v, want %v", d, got, wantRemoved)
	}
}

// assertRetained asserts a digest's enc-params row survives and no network
// release was invoked for it.
func assertRetained(t *testing.T, mem *inmem.MemStore, rm *recordingRemover, d multihash.Multihash) {
	t.Helper()
	if _, err := mem.GetEncryptionParams(context.Background(), did.Undef, d); err != nil {
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

// TestSweepReapsCompletingSession: a session a crash stranded in
// 'completing' before the commit (no claims exist) is reaped like an abort —
// blobs released, session gone.
func TestSweepReapsCompletingSession(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	ctx := context.Background()

	uploadID := mpCreate(t, b, "stranded", "", "")
	if _, err := mpUploadPart(t, b, "stranded", uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	digests := hygienePartDigests(t, mem, uploadID, 1)
	if won, err := mem.LatchSession(ctx, uploadID, registry.SessionOpen, registry.SessionCompleting); err != nil || !won {
		t.Fatalf("latch to completing: won=%v err=%v", won, err)
	}

	// Negative TTL = a future cutoff: everything is stale, no backdating.
	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("completing session survived the sweep (err=%v)", err)
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

	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
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

// TestCompleteFailsClosedOnMissingParamsRow: a part blob whose enc-params
// row is gone fails Complete (blobPlaintextLen cannot derive the plaintext
// span) instead of committing a manifest with an envelope-sized span; the
// session reverts to open.
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
	if err := mem.DeleteEncryptionParams(ctx, did.Undef, d); err != nil {
		t.Fatalf("DeleteEncryptionParams: %v", err)
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
	deleteObj(t, b, "k1")

	// Simulate a live grace: push the intent's not_before into the future so
	// the ordinary sweep would not touch it.
	if err := mem.EnqueueRelease(ctx, did.Undef, d, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("EnqueueRelease (extend): %v", err)
	}
	if n, err := b.SweepPendingReleases(ctx); err != nil || n != 0 {
		t.Fatalf("sweep executed %d releases (err=%v), want 0 — intent should not be due", n, err)
	}

	if err := b.drainSpaceReleases(ctx, did.Undef); err != nil {
		t.Fatalf("drainSpaceReleases: %v", err)
	}
	if _, err := mem.GetEncryptionParams(ctx, did.Undef, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("enc-params row survived the space drain (err=%v)", err)
	}
	if rm.removedDigests()[string(d)] != 1 {
		t.Fatalf("expected one RemoveBlob from the space drain; got %v", rm.removedDigests())
	}
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

// TestCompleteWaitBudgetIsOperationAborted: a latch loser whose wait for the
// winner runs out reports OperationAborted (a retryable 409), not
// NoSuchUpload — the session exists and is merely slow.
func TestCompleteWaitBudgetIsOperationAborted(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	key := "slow-winner"
	data := testBody(int(backend.MinPartSize))

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, data, nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	one := int32(1)
	parts := []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}

	// A winner that is still committing: hold the session in 'completing'.
	if won, err := mem.LatchSession(context.Background(), uploadID, registry.SessionOpen, registry.SessionCompleting); err != nil || !won {
		t.Fatalf("latch to completing: won=%v err=%v", won, err)
	}
	saveBudget, saveTries := replayWaitBudget, replayMaxTries
	replayWaitBudget, replayMaxTries = 50*time.Millisecond, 4
	t.Cleanup(func() { replayWaitBudget, replayMaxTries = saveBudget, saveTries })

	_, err = mpComplete(t, b, key, uploadID, parts, nil)
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "OperationAborted" {
		t.Fatalf("Complete against a slow winner: err = %v, want OperationAborted", err)
	}
}

// parkingUploader parks instead of accepting: UploadBlob returns no
// Location, so part blobs stay IntentParked — the provider shape the
// NopUploader cannot produce. AbortBlob calls are recorded; the abort of a
// digest in acceptedOnProvider is refused as already accepted, as a provider
// answers for a blob whose conclude ran without ingot learning of it.
type parkingUploader struct {
	inmem.NopUploader
	mu                 sync.Mutex
	aborted            []string
	acceptedOnProvider map[string]bool
	// abortFails makes the next abortFails aborts fail outright, as an
	// unreachable upload service does; they are not recorded as aborted.
	abortFails int
}

func (p *parkingUploader) UploadBlob(_ context.Context, _ did.DID, digest multihash.Multihash, size int64, _ string, _ ...uploader.UploadOption) (uploader.UploadedBlob, error) {
	c := cid.NewCidV1(cid.Raw, digest)
	return uploader.UploadedBlob{Digest: digest, Size: size, AddTask: c, AcceptTask: c}, nil
}

func (p *parkingUploader) AbortBlob(_ context.Context, _ did.DID, d multihash.Multihash, _ cid.Cid) error {
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
func newParkingBackend(t *testing.T) (*Backend, *inmem.MemStore, *parkingUploader) {
	t.Helper()
	pu := &parkingUploader{}
	b, mem := newDeferredBackend(t, pu)
	return b, mem, pu
}

// newDeferredBackend builds an in-process backend around a caller-supplied
// deferred uploader, so a test can observe how the completion path drives it.
// Each mod edits the wiring before the backend is built, so a test can swap
// one store for a faulty one.
func newDeferredBackend(t *testing.T, up deferredTestUploader, mods ...func(*Deps)) (*Backend, *inmem.MemStore) {
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

	deps := Deps{
		Authority:       mem,
		Registry:        mem,
		Intents:         mem,
		Locations:       mem,
		BlobRefs:        mem,
		GC:              mem,
		Multipart:       mem,
		Parks:           mem,
		Reads:           blockstore.NewLayered(spool, log, inmem.NopBaseReader{}),
		Log:             log,
		Spool:           spool,
		Uploader:        up,
		Deferred:        up,
		Remover:         &recordingRemover{},
		EncParams:       mem,
		RegionKeys:      testRegionKeys(t),
		TenantKeys:      testTenantKeys(),
		PendingReleases: mem,
	}
	for _, mod := range mods {
		mod(&deps)
	}
	b := New(deps)
	if err := mem.Create(ctx, "bk", did.Undef, registry.CreateState{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return b, mem
}

// deferredTestUploader is what newDeferredBackend needs of a fake: the
// uploader seams the multipart path uses.
type deferredTestUploader interface {
	uploader.Uploader
	uploader.DeferredBodyUploader
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
		in, err := mem.GetIntent(ctx, d)
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
		if _, err := mem.GetPark(ctx, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("park row for %x survived (err=%v)", d, err)
		}
		if _, err := mem.GetEncryptionParams(ctx, did.Undef, d); !errors.Is(err, registry.ErrNotFound) {
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

// TestCompleteRecordsAcceptancesWhenConcludeFails: a conclude that fails after
// the upload service accepted some of the blobs leaves those recorded as
// accepted with their parks dropped and the rest parked, so the next Complete
// concludes only what remains and nothing accepted is ever aborted as parked.
func TestCompleteRecordsAcceptancesWhenConcludeFails(t *testing.T) {
	hc := &haltingConcluder{halt: true}
	b, mem := newDeferredBackend(t, hc)
	ctx := context.Background()
	key := "partial"

	uploadID := mpCreate(t, b, key, "", "")
	var parts []types.CompletedPart
	for n := int32(1); n <= 2; n++ {
		// Distinct content per part, so each part is its own blob.
		body := testBody(int(backend.MinPartSize))
		body[0] = byte(n)
		out, err := mpUploadPart(t, b, key, uploadID, n, body, nil)
		if err != nil {
			t.Fatalf("UploadPart %d: %v", n, err)
		}
		num := n
		parts = append(parts, types.CompletedPart{PartNumber: &num, ETag: out.ETag})
	}

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err == nil {
		t.Fatal("Complete succeeded against a conclude that failed")
	}
	if len(hc.calls) != 1 || len(hc.calls[0]) != 2 {
		t.Fatalf("conclude calls = %v, want one call for two blobs", hc.calls)
	}
	accepted := multihash.Multihash(hc.calls[0][0])
	stillParked := multihash.Multihash(hc.calls[0][1])

	if in, err := mem.GetIntent(ctx, accepted); err != nil || in.State != registry.IntentAccepted {
		t.Fatalf("accepted blob intent = %v/%v, want accepted", in, err)
	}
	if loc, err := mem.GetLocation(ctx, did.Undef, accepted); err != nil || loc == nil {
		t.Fatalf("accepted blob has no location (err=%v)", err)
	}
	if _, err := mem.GetPark(ctx, accepted); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for the accepted blob survived (err=%v)", err)
	}
	if in, err := mem.GetIntent(ctx, stillParked); err != nil || in.State != registry.IntentParked {
		t.Fatalf("unconcluded blob intent = %v/%v, want parked", in, err)
	}
	if _, err := mem.GetPark(ctx, stillParked); err != nil {
		t.Fatalf("park row for the unconcluded blob is gone: %v", err)
	}

	// The retry has only the still-parked blob to conclude.
	hc.halt = false
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("retry Complete: %v", err)
	}
	if len(hc.calls) != 2 || len(hc.calls[1]) != 1 || hc.calls[1][0] != string(stillParked) {
		t.Fatalf("retry conclude calls = %v, want exactly the still-parked blob", hc.calls[1:])
	}
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

// TestCompleteRecordsEveryAcceptanceWhenOneFailsToPersist: the upload
// service accepts the whole batch, but recording one blob's location fails.
// Every other accepted blob is still recorded with its park dropped — a
// persistence failure for one blob must not leave the rest parked, where
// session expiry would abort them — and the next Complete concludes only the
// blob whose record did not land.
func TestCompleteRecordsEveryAcceptanceWhenOneFailsToPersist(t *testing.T) {
	hc := &haltingConcluder{}
	locs := &failOncePutLocation{armed: true}
	b, mem := newDeferredBackend(t, hc, func(d *Deps) {
		locs.LocationStore = d.Locations
		d.Locations = locs
	})
	ctx := context.Background()
	key := "persist-fail"

	uploadID := mpCreate(t, b, key, "", "")
	var parts []types.CompletedPart
	for n := int32(1); n <= 2; n++ {
		// Distinct content per part, so each part is its own blob.
		body := testBody(int(backend.MinPartSize))
		body[0] = byte(n)
		out, err := mpUploadPart(t, b, key, uploadID, n, body, nil)
		if err != nil {
			t.Fatalf("UploadPart %d: %v", n, err)
		}
		num := n
		parts = append(parts, types.CompletedPart{PartNumber: &num, ETag: out.ETag})
	}

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err == nil {
		t.Fatal("Complete succeeded although one location failed to persist")
	}
	if len(hc.calls) != 1 || len(hc.calls[0]) != 2 {
		t.Fatalf("conclude calls = %v, want one call for two blobs", hc.calls)
	}
	if locs.armed || locs.refused == nil {
		t.Fatal("the location store never refused a write")
	}
	var recorded multihash.Multihash
	for _, d := range hc.calls[0] {
		if d != string(locs.refused) {
			recorded = multihash.Multihash(d)
		}
	}

	if in, err := mem.GetIntent(ctx, recorded); err != nil || in.State != registry.IntentAccepted {
		t.Fatalf("recorded blob intent = %v/%v, want accepted", in, err)
	}
	if loc, err := mem.GetLocation(ctx, did.Undef, recorded); err != nil || loc == nil {
		t.Fatalf("recorded blob has no location (err=%v)", err)
	}
	if _, err := mem.GetPark(ctx, recorded); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for the recorded blob survived (err=%v)", err)
	}
	if in, err := mem.GetIntent(ctx, locs.refused); err != nil || in.State != registry.IntentParked {
		t.Fatalf("refused blob intent = %v/%v, want parked", in, err)
	}
	if _, err := mem.GetPark(ctx, locs.refused); err != nil {
		t.Fatalf("park row for the refused blob is gone: %v", err)
	}

	// The retry has only the refused blob to conclude.
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("retry Complete: %v", err)
	}
	if len(hc.calls) != 2 || len(hc.calls[1]) != 1 || hc.calls[1][0] != string(locs.refused) {
		t.Fatalf("retry conclude calls = %v, want exactly the refused blob", hc.calls[1:])
	}
	// The retry committed the object, so the claim published the intent.
	if in, err := mem.GetIntent(ctx, locs.refused); err != nil || in.State != registry.IntentPublished {
		t.Fatalf("refused blob intent after retry = %v/%v, want published", in, err)
	}
}

// failOnceMarkAccepted is an intent store whose next transition to accepted
// fails, after the location for that blob has already been recorded.
type failOnceMarkAccepted struct {
	registry.IntentStore
	armed bool
}

func (f *failOnceMarkAccepted) SetIntentState(ctx context.Context, digest multihash.Multihash, state string) error {
	if f.armed && state == registry.IntentAccepted {
		f.armed = false
		return errors.New("intents table unavailable")
	}
	return f.IntentStore.SetIntentState(ctx, digest, state)
}

// failDeletePark is a park store that refuses its next `fails` deletes,
// leaving a row behind for a blob whose acceptance is fully recorded.
type failDeletePark struct {
	registry.ParkStore
	fails int
}

func (f *failDeletePark) DeletePark(ctx context.Context, digest multihash.Multihash) error {
	if f.fails > 0 {
		f.fails--
		return errors.New("parks table unavailable")
	}
	return f.ParkStore.DeletePark(ctx, digest)
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
func assertAcceptedAndUnparked(t *testing.T, mem *inmem.MemStore, d multihash.Multihash) {
	t.Helper()
	ctx := context.Background()
	// Accepted, or published once a commit has claimed it.
	if in, err := mem.GetIntent(ctx, d); err != nil || (in.State != registry.IntentAccepted && in.State != registry.IntentPublished) {
		t.Fatalf("blob %x intent = %v/%v, want accepted or published", d, in, err)
	}
	if loc, err := mem.GetLocation(ctx, did.Undef, d); err != nil || loc == nil {
		t.Fatalf("blob %x has no location (err=%v)", d, err)
	}
	if _, err := mem.GetPark(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for blob %x survived (err=%v)", d, err)
	}
}

// TestCompleteRetryDropsParkAfterFailedIntentUpdate: a Complete that records
// a blob's location and then fails to mark its intent leaves the blob parked
// with a location. The retry finds the location, so it neither concludes the
// blob again nor leaves the park row standing.
func TestCompleteRetryDropsParkAfterFailedIntentUpdate(t *testing.T) {
	hc := &haltingConcluder{}
	intents := &failOnceMarkAccepted{armed: true}
	b, mem := newDeferredBackend(t, hc, func(d *Deps) {
		intents.IntentStore = d.Intents
		d.Intents = intents
	})
	key := "intent-fail"
	uploadID, parts := completeTwoParts(t, b, key)

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err == nil {
		t.Fatal("Complete succeeded although one intent failed to update")
	}
	if intents.armed {
		t.Fatal("the intent store never refused a write")
	}
	digests := hygienePartDigests(t, mem, uploadID, 1)
	digests = append(digests, hygienePartDigests(t, mem, uploadID, 2)...)
	// Both acceptances ran and both locations landed; only one intent did.
	for _, d := range digests {
		if loc, err := mem.GetLocation(context.Background(), did.Undef, d); err != nil || loc == nil {
			t.Fatalf("blob %x has no location (err=%v)", d, err)
		}
	}

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("retry Complete: %v", err)
	}
	if len(hc.calls) != 1 {
		t.Fatalf("conclude calls = %v, want the retry to conclude nothing: every blob is located", hc.calls)
	}
	for _, d := range digests {
		assertAcceptedAndUnparked(t, mem, d)
	}
}

// TestCompleteRetryDropsParkAfterFailedDelete: a Complete that records a
// blob's acceptance and then fails to drop its park leaves a stale row. The
// retry takes the dedup path for that blob and drops the row from there.
func TestCompleteRetryDropsParkAfterFailedDelete(t *testing.T) {
	hc := &haltingConcluder{}
	parks := &failDeletePark{fails: 1}
	b, mem := newDeferredBackend(t, hc, func(d *Deps) {
		parks.ParkStore = d.Parks
		d.Parks = parks
	})
	key := "park-delete-fail"
	uploadID, parts := completeTwoParts(t, b, key)

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err == nil {
		t.Fatal("Complete succeeded although one park failed to delete")
	}
	if parks.fails > 0 {
		t.Fatal("the park store never refused a delete")
	}
	stale := 0
	digests := hygienePartDigests(t, mem, uploadID, 1)
	digests = append(digests, hygienePartDigests(t, mem, uploadID, 2)...)
	for _, d := range digests {
		if _, err := mem.GetPark(context.Background(), d); err == nil {
			stale++
		}
	}
	if stale != 1 {
		t.Fatalf("stale park rows after the failed Complete = %d, want 1", stale)
	}

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("retry Complete: %v", err)
	}
	if len(hc.calls) != 1 {
		t.Fatalf("conclude calls = %v, want the retry to conclude nothing: every blob is located", hc.calls)
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
	parks := &failDeletePark{fails: 1}
	b, mem := newDeferredBackend(t, hc, func(d *Deps) {
		parks.ParkStore = d.Parks
		d.Parks = parks
	})
	ctx := context.Background()
	key := "sweep-stale-park"
	uploadID, parts := completeTwoParts(t, b, key)

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err == nil {
		t.Fatal("Complete succeeded although one park failed to delete")
	}
	digests := hygienePartDigests(t, mem, uploadID, 1)
	digests = append(digests, hygienePartDigests(t, mem, uploadID, 2)...)

	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	aborted := hc.abortedDigests()
	for _, d := range digests {
		if _, err := mem.GetPark(ctx, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("park row for accepted blob %x survived the sweep (err=%v)", d, err)
		}
		if aborted[string(d)] {
			t.Fatalf("accepted blob %x was aborted on the provider as though parked", d)
		}
	}
}

// TestReleaseSweepDropsParkTheSessionSweepCouldNot: the session sweep's own
// attempt to drop an accepted blob's stale park fails, and by then the intent
// and session rows are gone, so nothing else would find the row. The release
// the sweep enqueued is the durable retry: it stands until the park is gone.
func TestReleaseSweepDropsParkTheSessionSweepCouldNot(t *testing.T) {
	hc := &haltingConcluder{}
	parks := &failDeletePark{fails: 1}
	b, mem := newDeferredBackend(t, hc, func(d *Deps) {
		parks.ParkStore = d.Parks
		d.Parks = parks
	})
	ctx := context.Background()
	key := "release-drops-park"
	uploadID, parts := completeTwoParts(t, b, key)

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err == nil {
		t.Fatal("Complete succeeded although one park failed to delete")
	}
	digests := hygienePartDigests(t, mem, uploadID, 1)
	digests = append(digests, hygienePartDigests(t, mem, uploadID, 2)...)
	var stale multihash.Multihash
	for _, d := range digests {
		if _, err := mem.GetPark(ctx, d); err == nil {
			stale = d
		}
	}
	if stale == nil {
		t.Fatal("no stale park row after the failed Complete")
	}

	// The session sweep's delete fails too.
	parks.fails = 1
	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if _, err := mem.GetPark(ctx, stale); err != nil {
		t.Fatalf("park row should have survived the failed sweep delete: %v", err)
	}
	// The failed attempt keeps the intent: it is the retry's state.
	if _, err := mem.GetIntent(ctx, stale); err != nil {
		t.Fatalf("intent for %x did not survive the failed attempt: %v", stale, err)
	}

	// The release sweep is the retry, with a working store this time.
	drainReleases(t, b)
	if _, err := mem.GetPark(ctx, stale); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for %x survived the release sweep (err=%v)", stale, err)
	}
	if _, err := mem.GetLocation(ctx, did.Undef, stale); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("location for %x survived the release sweep (err=%v)", stale, err)
	}
	if _, err := mem.GetIntent(ctx, stale); !errors.Is(err, registry.ErrNotFound) {
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
	if in, err := mem.GetIntent(ctx, d); err != nil || in.State != registry.IntentAccepted {
		t.Fatalf("dedup part intent = %v/%v, want accepted", in, err)
	}

	// The stale row such a failure leaves behind, as a Complete that failed
	// after recording the acceptance would.
	if err := mem.PutPark(ctx, registry.BlobPark{Digest: d, AddTask: []byte{1}, AcceptTask: []byte{1}, Size: 1}); err != nil {
		t.Fatalf("PutPark: %v", err)
	}
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(hc.calls) != 0 {
		t.Fatalf("conclude calls = %v, want none: the blob was located throughout", hc.calls)
	}
	if _, err := mem.GetPark(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("stale park row survived Complete (err=%v)", err)
	}
}

// TestSweepReleasesLocatedBlobWhoseIntentLagged: a Complete records a blob's
// location, fails to mark its intent, and is never retried. On expiry the
// sweeper must read the location as the acceptance it is: the blob is
// released through the accepted path, never aborted on the provider as though
// it were still parked.
func TestSweepReleasesLocatedBlobWhoseIntentLagged(t *testing.T) {
	hc := &haltingConcluder{}
	intents := &failOnceMarkAccepted{armed: true}
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, hc, func(d *Deps) {
		intents.IntentStore = d.Intents
		d.Intents = intents
		d.Remover = rm
	})
	ctx := context.Background()
	key := "intent-lagged"
	uploadID, parts := completeTwoParts(t, b, key)

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err == nil {
		t.Fatal("Complete succeeded although one intent failed to update")
	}
	digests := hygienePartDigests(t, mem, uploadID, 1)
	digests = append(digests, hygienePartDigests(t, mem, uploadID, 2)...)
	var lagged multihash.Multihash
	for _, d := range digests {
		if in, err := mem.GetIntent(ctx, d); err == nil && in.State == registry.IntentParked {
			lagged = d
		}
	}
	if lagged == nil {
		t.Fatal("no blob was left parked with a location")
	}
	if loc, err := mem.GetLocation(ctx, did.Undef, lagged); err != nil || loc == nil {
		t.Fatalf("lagged blob has no location (err=%v)", err)
	}

	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if hc.abortedDigests()[string(lagged)] {
		t.Fatalf("located blob %x was aborted on the provider as though parked", lagged)
	}
	if _, err := mem.GetPark(ctx, lagged); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for located blob %x survived the sweep (err=%v)", lagged, err)
	}
	assertReleased(t, b, mem, rm, lagged, true)
	if _, err := mem.GetLocation(ctx, did.Undef, lagged); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("location for %x survived the release (err=%v)", lagged, err)
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
	if in, err := mem.GetIntent(ctx, d); err != nil || in.State != registry.IntentParked {
		t.Fatalf("part blob intent = %v/%v, want parked", in, err)
	}
	pu.acceptedOnProvider[string(d)] = true

	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("abort was never attempted for %x", d)
	}
	if _, err := mem.GetPark(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for %x survived the sweep (err=%v)", d, err)
	}
	// Released as an accepted blob: the network claim is removed.
	assertReleased(t, b, mem, rm, d, true)
}

// failEnqueueReleases is a release store whose next `fails` bulk enqueues
// fail, as a registry outage during a session teardown does.
type failEnqueueReleases struct {
	registry.PendingReleaseStore
	fails int
}

func (f *failEnqueueReleases) EnqueueReleases(ctx context.Context, space did.DID, digests []multihash.Multihash, notBefore time.Time) ([]registry.PendingRelease, error) {
	if f.fails > 0 {
		f.fails--
		return nil, errors.New("release table unavailable")
	}
	return f.PendingReleaseStore.EnqueueReleases(ctx, space, digests, notBefore)
}

// failCompleteSession is a multipart store whose next `fails` completion
// latches fail after the object has committed, stranding the session in
// 'completing'.
type failCompleteSession struct {
	registry.MultipartStore
	fails int
}

func (f *failCompleteSession) CompleteSession(ctx context.Context, uploadID, etag, versionID string) (bool, error) {
	if f.fails > 0 {
		f.fails--
		return false, errors.New("sessions table unavailable")
	}
	return f.MultipartStore.CompleteSession(ctx, uploadID, etag, versionID)
}

// failDeleteSession is a multipart store whose next `fails` session deletes
// fail, after the releases for its parts have been recorded.
type failDeleteSession struct {
	registry.MultipartStore
	fails int
}

func (f *failDeleteSession) DeleteSession(ctx context.Context, uploadID string) error {
	if f.fails > 0 {
		f.fails--
		return errors.New("sessions table unavailable")
	}
	return f.MultipartStore.DeleteSession(ctx, uploadID)
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

// TestAbortKeepsSessionWhenRecordingReleasesFails: an Abort that cannot
// record its parts' releases fails and leaves the session latched, so the
// part rows — the only index to the blobs — survive for the sweeper, which
// then releases the blobs on the provider.
func TestAbortKeepsSessionWhenRecordingReleasesFails(t *testing.T) {
	pu := &parkingUploader{}
	rel := &failEnqueueReleases{fails: 1}
	b, mem := newDeferredBackend(t, pu, func(d *Deps) {
		rel.PendingReleaseStore = d.PendingReleases
		d.PendingReleases = rel
	})
	ctx := context.Background()
	bucket, key := "bk", "abort-record-fails"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	err := b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID})
	if err == nil {
		t.Fatal("Abort succeeded although the releases could not be recorded")
	}
	sess, err := mem.GetSession(ctx, uploadID)
	if err != nil || sess.State != registry.SessionAborting {
		t.Fatalf("session after failed Abort = %v/%v, want latched aborting", sess, err)
	}
	if _, err := mem.GetPark(ctx, d); err != nil {
		t.Fatalf("park row went before its release was recorded: %v", err)
	}
	if pu.abortedDigests()[string(d)] {
		t.Fatal("blob was aborted on the provider before its release was recorded")
	}

	// The sweeper finishes the abort from the surviving part rows.
	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("aborting session survived the sweep (err=%v)", err)
	}
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("parked blob %x was not released on the provider", d)
	}
	if _, err := mem.GetPark(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for %x survived (err=%v)", d, err)
	}
}

// TestAbortReleasesFromRecordsWhenSessionDeleteFails: the releases are
// recorded before the session row goes, so a failure to delete the row loses
// nothing — the release sweep executes the records while the row is still
// latched aborting, and a later sweep drops the row.
func TestAbortReleasesFromRecordsWhenSessionDeleteFails(t *testing.T) {
	pu := &parkingUploader{}
	mp := &failDeleteSession{fails: 1}
	b, mem := newDeferredBackend(t, pu, func(d *Deps) {
		mp.MultipartStore = d.Multipart
		d.Multipart = mp
	})
	ctx := context.Background()
	bucket, key := "bk", "abort-delete-fails"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	if err := b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}); err == nil {
		t.Fatal("Abort succeeded although the session could not be deleted")
	}
	pending, err := mem.ListReleasesBySpace(ctx, did.Undef)
	if err != nil || len(pending) != 1 || string(pending[0].Digest) != string(d) {
		t.Fatalf("pending releases = %v/%v, want the part blob's record", pending, err)
	}

	drainReleases(t, b)
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("parked blob %x was not released from its record", d)
	}
	if _, err := mem.GetPark(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for %x survived (err=%v)", d, err)
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, did.Undef); len(pending) != 0 {
		t.Fatalf("release record survived its execution: %v", pending)
	}
	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("aborting session survived the sweep (err=%v)", err)
	}
}

// TestReleaseDefersWhileAnInFlightPartReferencesTheBlob: a release record
// for a digest a part of an open session references waits, pushed behind the
// records due now, and becomes stale once that session claims the blob.
func TestReleaseDefersWhileAnInFlightPartReferencesTheBlob(t *testing.T) {
	b, mem, pu := newParkingBackend(t)
	ctx := context.Background()
	key := "in-flight"

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	if err := mem.EnqueueRelease(ctx, did.Undef, d, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("EnqueueRelease: %v", err)
	}

	before := time.Now()
	if n, err := b.SweepPendingReleases(ctx); err != nil || n != 0 {
		t.Fatalf("sweep executed %d releases (err=%v), want 0: the part is in flight", n, err)
	}
	if _, err := mem.GetPark(ctx, d); err != nil {
		t.Fatalf("park row for the in-flight part was taken by the release sweep: %v", err)
	}
	if pu.abortedDigests()[string(d)] {
		t.Fatalf("in-flight part blob %x was aborted on the provider", d)
	}
	pending, err := mem.ListReleasesBySpace(ctx, did.Undef)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending releases = %d/%v, want the deferred record to stand", len(pending), err)
	}
	if !pending[0].NotBefore.After(before) {
		t.Fatalf("deferred record not_before = %v, want pushed behind the records due now", pending[0].NotBefore)
	}

	// The session claims the blob; the record is stale and goes.
	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if err := b.drainSpaceReleases(ctx, did.Undef); err != nil {
		t.Fatalf("drainSpaceReleases: %v", err)
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, did.Undef); len(pending) != 0 {
		t.Fatalf("stale record survived the claim: %v", pending)
	}
	if _, err := mem.GetEncryptionParams(ctx, did.Undef, d); err != nil {
		t.Fatalf("claimed blob %x lost its enc-params row: %v", d, err)
	}
}

// TestCompletedSessionReapReleasesOrphansWhoseRecordingFailed: Complete's
// own release of an omitted part is best-effort — here both recording the
// release and the inline attempt fail. The completed-session reap derives
// the orphan again from the retained part rows, records it, and releases it,
// so the orphan is released late rather than never.
func TestCompletedSessionReapReleasesOrphansWhoseRecordingFailed(t *testing.T) {
	pu := &parkingUploader{}
	rel := &failEnqueueReleases{}
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, pu, func(d *Deps) {
		rel.PendingReleaseStore = d.PendingReleases
		d.PendingReleases = rel
		d.Remover = rm
	})
	ctx := context.Background()
	key := "orphan-late"
	uploadID, parts := completeTwoParts(t, b, key)
	part1 := hygienePartDigests(t, mem, uploadID, 1)
	orphans := hygienePartDigests(t, mem, uploadID, 2)

	// Complete with part 1 only; the orphan pass can neither record the
	// release nor abort the blob on the provider.
	rel.fails = 1
	pu.abortFails = 1
	if _, err := mpComplete(t, b, key, uploadID, parts[:1], nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if rel.fails > 0 || pu.abortFails > 0 {
		t.Fatalf("the failures were not exercised: record fails left=%d, abort fails left=%d", rel.fails, pu.abortFails)
	}
	drainReleases(t, b)
	for _, d := range orphans {
		if _, err := mem.GetPark(ctx, d); err != nil {
			t.Fatalf("orphan %x lost its park with no record and no release: %v", d, err)
		}
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, did.Undef); len(pending) != 0 {
		t.Fatalf("a release record exists although recording failed: %v", pending)
	}

	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	drainReleases(t, b)
	// The orphan was never concluded, so its release aborts the parked
	// allocation on the provider and tears the local rows down.
	for _, d := range orphans {
		if !pu.abortedDigests()[string(d)] {
			t.Fatalf("orphan %x was not released on the provider", d)
		}
		if _, err := mem.GetPark(ctx, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("park row for orphan %x survived (err=%v)", d, err)
		}
		if _, err := mem.GetEncryptionParams(ctx, did.Undef, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("enc-params row for orphan %x survived (err=%v)", d, err)
		}
		if _, err := mem.GetIntent(ctx, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("intent for orphan %x survived (err=%v)", d, err)
		}
	}
	for _, d := range part1 {
		assertRetained(t, mem, rm, d)
	}
}

// TestSupersedeRecordsReleasesBeforeWriting: a re-upload of a part records
// the superseded blobs' releases before it writes anything, so a failure to
// record fails the request with the old part intact, and nothing is released.
func TestSupersedeRecordsReleasesBeforeWriting(t *testing.T) {
	pu := &parkingUploader{}
	rel := &failEnqueueReleases{}
	b, mem := newDeferredBackend(t, pu, func(d *Deps) {
		rel.PendingReleaseStore = d.PendingReleases
		d.PendingReleases = rel
	})
	ctx := context.Background()
	key := "supersede-record"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart (first): %v", err)
	}
	old := hygienePartDigests(t, mem, uploadID, 1)

	rel.fails = 1
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)+100), nil); err == nil {
		t.Fatal("re-upload succeeded although the superseded releases could not be recorded")
	}
	if got := hygienePartDigests(t, mem, uploadID, 1); len(got) != len(old) || string(got[0]) != string(old[0]) {
		t.Fatalf("part 1 blobs after the failed re-upload = %x, want the old part intact %x", got, old)
	}
	for _, d := range old {
		if _, err := mem.GetPark(ctx, d); err != nil {
			t.Fatalf("old blob %x lost its park on a failed re-upload: %v", d, err)
		}
		if pu.abortedDigests()[string(d)] {
			t.Fatalf("old blob %x was aborted on a failed re-upload", d)
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
		if _, err := mem.GetPark(ctx, d); !errors.Is(err, registry.ErrNotFound) {
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
	if loc, err := mem.GetLocation(ctx, did.Undef, d); err != nil || loc == nil {
		t.Fatalf("location row went although the network still holds the blob (err=%v)", err)
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, did.Undef); len(pending) != 1 {
		t.Fatalf("release record after the failed attempt = %v, want it kept", pending)
	}

	if n, err := b.SweepPendingReleases(ctx); err != nil || n != 1 {
		t.Fatalf("second sweep executed %d releases (err=%v), want 1", n, err)
	}
	if rm.removedDigests()[string(d)] != 1 {
		t.Fatalf("RemoveBlob calls for %x = %d, want 1", d, rm.removedDigests()[string(d)])
	}
	if _, err := mem.GetLocation(ctx, did.Undef, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("location row survived the release (err=%v)", err)
	}
	// A committed blob's ingest artifacts are not the release's to remove:
	// the spool copy is the insurance copy until eviction.
	if in, err := mem.GetIntent(ctx, d); err != nil || in.State != registry.IntentPublished {
		t.Fatalf("a deleted object's blob intent = %v/%v, want published and kept", in, err)
	}
}

// TestCompletedSessionReapKeepsWinnersOfDeletedObject: a completed session
// is retained after its object is deleted. Its winners' releases were
// recorded by the delete as ordinary releases; the reap must not re-record
// them as part blobs, which would take the spool copy that survives a DELETE
// as the insurance copy. Their intents were published by their claims, so the
// reap leaves them alone.
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

	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	drainReleases(t, b)
	for _, d := range winners {
		if rm.removedDigests()[string(d)] != 1 {
			t.Fatalf("winner %x removed %d times, want once by the object's own release", d, rm.removedDigests()[string(d)])
		}
		if in, err := mem.GetIntent(ctx, d); err != nil || in.State != registry.IntentPublished {
			t.Fatalf("winner %x intent = %v/%v, want published and kept: the spool copy is the insurance copy", d, in, err)
		}
	}
}

// TestCompletingSessionWhoseLatchFailedKeepsCommittedBlobs: the object
// commits, but the completing→completed latch fails, so the session is
// reaped through the abort path after its object was deleted. Its blobs'
// intents were published by their claims, so the reap leaves them to the
// object's own releases and their spool copies survive.
func TestCompletingSessionWhoseLatchFailedKeepsCommittedBlobs(t *testing.T) {
	rm := &recordingRemover{}
	mp := &failCompleteSession{}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		mp.MultipartStore = d.Multipart
		d.Multipart = mp
		d.Remover = rm
	})
	ctx := context.Background()
	key := "latch-failed"
	uploadID, parts := completeTwoParts(t, b, key)
	mp.fails = 1
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if mp.fails > 0 {
		t.Fatal("the latch never failed")
	}
	if sess, err := mem.GetSession(ctx, uploadID); err != nil || sess.State != registry.SessionCompleting {
		t.Fatalf("session after the failed latch = %v/%v, want stranded completing", sess, err)
	}
	winners := blobDigestsOf(t, b, key, "")
	deleteObj(t, b, key)

	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("stranded session survived the sweep (err=%v)", err)
	}
	drainReleases(t, b)
	for _, d := range winners {
		if rm.removedDigests()[string(d)] != 1 {
			t.Fatalf("committed blob %x removed %d times, want once", d, rm.removedDigests()[string(d)])
		}
		if in, err := mem.GetIntent(ctx, d); err != nil || in.State != registry.IntentPublished {
			t.Fatalf("committed blob %x intent = %v/%v, want published and kept", d, in, err)
		}
	}
}

// TestAbortRecordsBlobAnotherAbortingSessionReferences: two sessions share a
// part blob and both are being torn down. Skipping the blob because the other
// session's part still references it would let both skip it, and both would
// cascade their rows away. The abort records it regardless and the release
// itself sees that no in-flight session needs it.
func TestAbortRecordsBlobAnotherAbortingSessionReferences(t *testing.T) {
	b, mem, pu := newParkingBackend(t)
	ctx := context.Background()
	bucket, key := "bk", "shared"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	// A second session referencing the same blob, already latched aborting.
	const other = "other-session"
	if err := mem.CreateSession(ctx, registry.MultipartSession{UploadID: other, Bucket: bucket, ObjectKey: "other"}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := mem.PutPart(ctx, registry.MultipartPart{UploadID: other, PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{d}, State: registry.PartParked}); err != nil {
		t.Fatalf("PutPart: %v", err)
	}
	if won, err := mem.LatchSession(ctx, other, registry.SessionOpen, registry.SessionAborting); err != nil || !won {
		t.Fatalf("latch other: won=%v err=%v", won, err)
	}

	if err := b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("shared blob %x was not released: the other session's part reference stopped the record", d)
	}
	if _, err := mem.GetPark(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for %x survived (err=%v)", d, err)
	}
}

// TestReleaseWaitsForAnotherOpenSessionSharingTheBlob: the same shared blob,
// but the other session is still open. The abort records the release and the
// release waits, since that session means to claim the blob.
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
	if err := mem.CreateSession(ctx, registry.MultipartSession{UploadID: other, Bucket: bucket, ObjectKey: "other"}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := mem.PutPart(ctx, registry.MultipartPart{UploadID: other, PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{d}, State: registry.PartParked}); err != nil {
		t.Fatalf("PutPart: %v", err)
	}

	if err := b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if pu.abortedDigests()[string(d)] {
		t.Fatalf("shared blob %x was released while another open session references it", d)
	}
	if _, err := mem.GetPark(ctx, d); err != nil {
		t.Fatalf("park row for %x went while another open session references it: %v", d, err)
	}
	pending, _ := mem.ListReleasesBySpace(ctx, did.Undef)
	if len(pending) != 1 || string(pending[0].Digest) != string(d) {
		t.Fatalf("pending releases = %v, want the shared blob's record standing", pending)
	}
}

// recordingReleases records the space each bulk enqueue was made against.
type recordingReleases struct {
	registry.PendingReleaseStore
	spaces []did.DID
}

func (r *recordingReleases) EnqueueReleases(ctx context.Context, space did.DID, digests []multihash.Multihash, notBefore time.Time) ([]registry.PendingRelease, error) {
	r.spaces = append(r.spaces, space)
	return r.PendingReleaseStore.EnqueueReleases(ctx, space, digests, notBefore)
}

// TestSweepReleasesSessionThatOutlivedItsBucket: a session created while its
// bucket was being deleted outlives the bucket row. Its space was recorded at
// create, so the sweep still records and runs its parts' releases against
// that space rather than dropping the only index to the blobs.
func TestSweepReleasesSessionThatOutlivedItsBucket(t *testing.T) {
	pu := &parkingUploader{}
	rel := &recordingReleases{}
	b, mem := newDeferredBackend(t, pu, func(d *Deps) {
		rel.PendingReleaseStore = d.PendingReleases
		d.PendingReleases = rel
	})
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
	if _, err := b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &one, Body: bytes.NewReader(testBody(int(backend.MinPartSize)))}); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	if sess, err := mem.GetSession(ctx, uploadID); err != nil || sess.Space != space {
		t.Fatalf("session space = %v/%v, want %v recorded at create", sess, err, space)
	}

	// The bucket row goes while the session stands.
	if err := mem.Delete(ctx, bucket); err != nil {
		t.Fatalf("delete bucket row: %v", err)
	}
	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("session survived the sweep (err=%v)", err)
	}
	if len(rel.spaces) == 0 || rel.spaces[len(rel.spaces)-1] != space {
		t.Fatalf("releases recorded against %v, want the session's space %v", rel.spaces, space)
	}
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("parked blob %x was not released on the provider", d)
	}
	if _, err := mem.GetPark(ctx, d); !errors.Is(err, registry.ErrNotFound) {
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
	rel := &recordingReleases{}
	b, mem := newDeferredBackend(t, pu, func(d *Deps) {
		rel.PendingReleaseStore = d.PendingReleases
		d.PendingReleases = rel
	})
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
	out, err := b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &one, Body: bytes.NewReader(testBody(int(backend.MinPartSize)))})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	// Same name, new space.
	if err := mem.Delete(ctx, bucket); err != nil {
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
	_, err = b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &two, Body: bytes.NewReader(testBody(int(backend.MinPartSize)))})
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

	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n == 0 {
		t.Fatalf("sweep: cleaned=%d err=%v", n, err)
	}
	if len(rel.spaces) == 0 || rel.spaces[len(rel.spaces)-1] != oldSpace {
		t.Fatalf("releases recorded against %v, want the session's own space %v", rel.spaces, oldSpace)
	}
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("parked blob %x was not released on the provider", d)
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("session survived the sweep (err=%v)", err)
	}
}

// TestSweepLeavesALiveCompleteOnAnOldSession: a Complete on a session older
// than the TTL latches it to 'completing', which restarts the sweeper's
// clock. The sweep leaves that row alone while still aborting an equally old
// session nobody has touched.
func TestSweepLeavesALiveCompleteOnAnOldSession(t *testing.T) {
	b, mem, pu := newParkingBackend(t)
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour)
	mk := func(id string) multihash.Multihash {
		if err := mem.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "bk", ObjectKey: id, CreatedAt: old}); err != nil {
			t.Fatalf("CreateSession %s: %v", id, err)
		}
		d, err := multihash.Sum([]byte("blob-"+id), multihash.SHA2_256, -1)
		if err != nil {
			t.Fatalf("multihash.Sum: %v", err)
		}
		if err := mem.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{d}, State: registry.PartParked}); err != nil {
			t.Fatalf("PutPart %s: %v", id, err)
		}
		// The write path spools every part blob before recording the part,
		// so a part row never exists without its blob's intent.
		if err := mem.PutIntent(ctx, registry.UploadIntent{Digest: d, LocalPath: "/spool/" + id, Size: 1, State: registry.IntentParked, Bucket: "bk"}); err != nil {
			t.Fatalf("PutIntent %s: %v", id, err)
		}
		// Parked on a provider, so a release of it is an abort the fake records.
		task := cid.NewCidV1(cid.Raw, d).Bytes()
		if err := mem.PutPark(ctx, registry.BlobPark{Digest: d, AddTask: task, AcceptTask: task, Size: 1}); err != nil {
			t.Fatalf("PutPark %s: %v", id, err)
		}
		return d
	}
	completing := mk("completing-now")
	abandoned := mk("abandoned")
	if won, err := mem.LatchSession(ctx, "completing-now", registry.SessionOpen, registry.SessionCompleting); err != nil || !won {
		t.Fatalf("latch: won=%v err=%v", won, err)
	}

	if n, err := b.SweepStaleMultipartSessions(ctx, time.Hour); err != nil || n != 1 {
		t.Fatalf("sweep: cleaned=%d err=%v, want only the abandoned session", n, err)
	}
	if sess, err := mem.GetSession(ctx, "completing-now"); err != nil || sess.State != registry.SessionCompleting {
		t.Fatalf("live Complete's session = %v/%v, want left completing", sess, err)
	}
	if parts, _ := mem.ListParts(ctx, "completing-now"); len(parts) != 1 {
		t.Fatalf("live Complete's parts = %d, want intact", len(parts))
	}
	if pu.abortedDigests()[string(completing)] {
		t.Fatal("the sweep aborted a blob a live Complete is concluding")
	}
	if _, err := mem.GetSession(ctx, "abandoned"); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("abandoned session survived (err=%v)", err)
	}
	if !pu.abortedDigests()[string(abandoned)] {
		t.Fatal("the abandoned session's blob was not released")
	}
}

// hookAfterParkDeletes is a park store that runs hook once, right after its
// n-th DeletePark: Complete drops each concluded blob's park as its last
// recording step, so with n = the session's blob count the hook lands in the
// window between the off-lock conclude and the locked commit, where a sweep
// or DeleteBucket can take the session.
type hookAfterParkDeletes struct {
	registry.ParkStore
	n    int
	hook func()
}

func (h *hookAfterParkDeletes) DeletePark(ctx context.Context, digest multihash.Multihash) error {
	err := h.ParkStore.DeletePark(ctx, digest)
	h.n--
	if h.n == 0 && h.hook != nil {
		hook := h.hook
		h.hook = nil
		hook()
	}
	return err
}

// TestCompleteFailsWhenItsSessionIsTakenBeforeCommit: a sweep (DeleteBucket
// latches the same way) takes a 'completing' session after its Complete has
// concluded the blobs off the bucket lock, and releases them. Complete
// re-checks its latch under the lock and fails with NoSuchUpload instead of
// committing a manifest over released blobs.
func TestCompleteFailsWhenItsSessionIsTakenBeforeCommit(t *testing.T) {
	rm := &recordingRemover{}
	parks := &hookAfterParkDeletes{n: 2}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		parks.ParkStore = d.Parks
		d.Parks = parks
		d.Remover = rm
	})
	ctx := context.Background()
	bucket, key := "bk", "taken-before-commit"
	uploadID, parts := completeTwoParts(t, b, key)
	digests := hygienePartDigests(t, mem, uploadID, 1)
	digests = append(digests, hygienePartDigests(t, mem, uploadID, 2)...)
	parks.hook = func() {
		if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n != 1 {
			t.Errorf("sweep before commit: cleaned=%d err=%v, want the completing session", n, err)
		}
	}

	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "NoSuchUpload" {
		t.Fatalf("Complete whose session was taken: err = %v, want NoSuchUpload", err)
	}
	if parks.hook != nil {
		t.Fatal("the teardown never ran")
	}
	if _, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key}); err == nil {
		t.Fatal("a manifest committed over released blobs")
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("session survived (err=%v)", err)
	}
	for _, d := range digests {
		if rm.removedDigests()[string(d)] != 1 {
			t.Fatalf("accepted part blob %x removed %d times, want once", d, rm.removedDigests()[string(d)])
		}
	}
}

// teardownBeforePutPart is a multipart store that runs hook once, right
// before its next PutPart: the window after UploadPart was admitted and
// spooled its blobs, where a teardown can take the session.
type teardownBeforePutPart struct {
	registry.MultipartStore
	hook func()
}

func (s *teardownBeforePutPart) PutPart(ctx context.Context, p registry.MultipartPart) error {
	if s.hook != nil {
		hook := s.hook
		s.hook = nil
		hook()
	}
	return s.MultipartStore.PutPart(ctx, p)
}

// TestUploadPartRefusedAfterTeardownReleasesItsBlobs: a sweep takes the
// session after an UploadPart was admitted and spooled its blobs. The part
// row is refused, since the session is no longer open, the upload fails
// with NoSuchUpload, and the blobs it spooled are released rather than
// left behind with no row pointing at them.
func TestUploadPartRefusedAfterTeardownReleasesItsBlobs(t *testing.T) {
	mp := &teardownBeforePutPart{}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		mp.MultipartStore = d.Multipart
		d.Multipart = mp
	})
	ctx := context.Background()
	key := "refused-part"
	uploadID := mpCreate(t, b, key, "", "")
	mp.hook = func() {
		if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n != 1 {
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
	entries, err := os.ReadDir(b.spool.Path(nil))
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("spool holds %d blobs after the refused part was released, want 0", len(entries))
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, did.Undef); len(pending) != 0 {
		t.Fatalf("pending releases after the refused part = %v, want none", pending)
	}
}

// failPutPart is a multipart store whose next `fails` part writes fail with
// a store error, as an unreachable registry does.
type failPutPart struct {
	registry.MultipartStore
	fails int
}

func (f *failPutPart) PutPart(ctx context.Context, p registry.MultipartPart) error {
	if f.fails > 0 {
		f.fails--
		return errors.New("parts table unavailable")
	}
	return f.MultipartStore.PutPart(ctx, p)
}

// TestUploadPartFailedRowWriteReleasesItsBlobs: the part row write fails for
// a reason other than the session being gone. No row points at the spooled
// blobs, so the upload records their releases before failing; the session
// stays open for the retry. The blobs never left the node (the row is
// written before the park), so their release is local and asks the network
// for nothing: a background retry would have no authority for it.
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
	entries, err := os.ReadDir(b.spool.Path(nil))
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("spool holds %d blobs after the failed row write, want 0", len(entries))
	}
	if n := len(rm.removedDigests()); n != 0 {
		t.Fatalf("RemoveBlob called for %d blobs that never left the node, want 0", n)
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

// TestReleaseKeepsIntentUntilLocalCleanupSucceeds: a release of a blob that
// never left the node fails at its crypto-shred. The intent and spool copy
// must survive that attempt, or the retry would find neither rows nor
// intent and owe a network remove it has no authority for; the retry then
// finishes the local cleanup without touching the network.
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
	pending, _ := mem.ListReleasesBySpace(ctx, did.Undef)
	if len(pending) != 1 {
		t.Fatalf("pending releases after the failed attempt = %d, want 1", len(pending))
	}
	d := pending[0].Digest
	if in, err := mem.GetIntent(ctx, d); err != nil || in.State != registry.IntentSpooled {
		t.Fatalf("intent after the failed attempt = %v/%v, want kept as spooled", in, err)
	}
	if _, err := os.Stat(b.spool.Path(d)); err != nil {
		t.Fatalf("spool copy gone after the failed attempt: %v", err)
	}

	drainReleases(t, b)
	if _, err := mem.GetIntent(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("intent survived the retry (err=%v)", err)
	}
	if _, err := os.Stat(b.spool.Path(d)); !os.IsNotExist(err) {
		t.Fatalf("spool copy survived the retry (err=%v)", err)
	}
	if n := len(rm.removedDigests()); n != 0 {
		t.Fatalf("RemoveBlob called %d times for a blob that never left the node, want 0", n)
	}
}

// failDeleteRelease is a pending-release store whose record deletes always
// fail, as a crash or cancellation between the release and the record's
// removal does. Everything else passes through.
type failDeleteRelease struct {
	registry.PendingReleaseStore
}

func (f *failDeleteRelease) DeleteRelease(context.Context, did.DID, multihash.Multihash) error {
	return errors.New("release intents table unavailable")
}

// TestLocalOnlyReleaseDropsIntentWithItsRecord: a blob that never left the
// node is released, and the caller's removal of the release record fails.
// The record is gone regardless, because the release removed it in the same
// transaction as the intent. Were it to outlive the intent, the retry would
// read a blob with no rows and no intent and owe a network remove it has no
// authority for.
func TestLocalOnlyReleaseDropsIntentWithItsRecord(t *testing.T) {
	rm := &recordingRemover{}
	mp := &failPutPart{fails: 1}
	rel := &failDeleteRelease{}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		mp.MultipartStore = d.Multipart
		d.Multipart = mp
		rel.PendingReleaseStore = d.PendingReleases
		d.PendingReleases = rel
		d.Remover = rm
	})
	ctx := context.Background()
	key := "record-with-intent"
	uploadID := mpCreate(t, b, key, "", "")

	// The part row write fails, so the spooled blobs are released in-request;
	// the record's own deletion then fails.
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err == nil {
		t.Fatal("UploadPart succeeded although the part row write failed")
	}
	if mp.fails > 0 {
		t.Fatal("the part store never refused a write")
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, did.Undef); len(pending) != 0 {
		t.Fatalf("release records after the release = %v, want none: the record outlived its intent", pending)
	}
	entries, err := os.ReadDir(b.spool.Path(nil))
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("spool holds %d blobs after the release, want 0", len(entries))
	}

	// Nothing is left for the sweeper, and no retry ever asks the network to
	// remove a blob that never reached it.
	if n, err := b.SweepPendingReleases(ctx); err != nil || n != 0 {
		t.Fatalf("sweep after the release: released=%d err=%v, want nothing left", n, err)
	}
	if n := len(rm.removedDigests()); n != 0 {
		t.Fatalf("RemoveBlob called for %d blobs that never left the node, want 0", n)
	}
}

// TestReapDoesNotRecordAReleaseAlreadyRunToCompletion: a teardown records
// its parts' releases and then fails to delete the session, so the releases
// run from the queue first and take their blobs' intents with them. The next
// reap still has the part rows: it must recognise those blobs as gone rather
// than queue them again, since the second record's release would find
// neither rows nor intent and ask the network to remove a blob that may
// never have reached it.
func TestReapDoesNotRecordAReleaseAlreadyRunToCompletion(t *testing.T) {
	rm := &recordingRemover{}
	mp := &failDeleteSession{fails: 2}
	pu := &parkingUploader{}
	b, mem := newDeferredBackend(t, pu, func(d *Deps) {
		mp.MultipartStore = d.Multipart
		d.Multipart = mp
		d.Remover = rm
	})
	ctx := context.Background()
	key := "reaped-twice"
	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	// The first reap records the releases, then fails to drop the session.
	// One sweep attempts it twice: the open pass latches the session to
	// aborting and reaps, then the stranded-aborting pass retries.
	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n != 0 {
		t.Fatalf("first sweep: cleaned=%d err=%v, want the session kept", n, err)
	}
	if mp.fails > 0 {
		t.Fatal("the session delete never failed")
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, did.Undef); len(pending) != 1 {
		t.Fatalf("pending releases after the first reap = %d, want 1", len(pending))
	}

	// The release sweeper runs them to completion: the parked blob is
	// aborted on its provider and its intent goes with the record.
	drainReleases(t, b)
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("parked blob %x was not released on the provider", d)
	}
	if _, err := mem.GetIntent(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("intent for %x survived its release (err=%v)", d, err)
	}

	// The second reap sees the same part rows and must record nothing.
	if n, err := b.SweepStaleMultipartSessions(ctx, -time.Second); err != nil || n != 1 {
		t.Fatalf("second sweep: cleaned=%d err=%v, want the session reaped", n, err)
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("session survived the second sweep (err=%v)", err)
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, did.Undef); len(pending) != 0 {
		t.Fatalf("releases re-recorded for a blob already released = %v, want none", pending)
	}
	if n := len(rm.removedDigests()); n != 0 {
		t.Fatalf("RemoveBlob called for %d blobs, want 0: the blob was released as a parked one", n)
	}
}

// TestReleaseRemovesBlobAcceptedWithoutRows: at Complete a never-parked blob
// is uploaded and accepted, then the location fails to record, leaving no
// park and no location row. If the client aborts instead of retrying, the
// release must still remove the blob from the network: the rows' absence is
// not proof it never left the node.
func TestReleaseRemovesBlobAcceptedWithoutRows(t *testing.T) {
	rm := &recordingRemover{}
	locs := &failOncePutLocation{}
	b, mem := newDeferredBackend(t, inmem.NopUploader{}, func(d *Deps) {
		locs.LocationStore = d.Locations
		d.Locations = locs
		d.Remover = rm
	})
	ctx := context.Background()
	bucket, key := "bk", "no-rows"

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	// Make the blob look never parked: no location, intent back to spooled.
	if err := mem.DeleteLocation(ctx, did.Undef, d); err != nil {
		t.Fatalf("DeleteLocation: %v", err)
	}
	if err := mem.SetIntentState(ctx, d, registry.IntentSpooled); err != nil {
		t.Fatalf("SetIntentState: %v", err)
	}

	// Complete uploads it synchronously, the provider accepts, the location
	// write fails.
	locs.armed = true
	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil); err == nil {
		t.Fatal("Complete succeeded although the location failed to record")
	}
	if locs.armed {
		t.Fatal("the location store never refused a write")
	}
	if loc, err := mem.GetLocation(ctx, did.Undef, d); err == nil && loc != nil {
		t.Fatal("location row exists; the scenario needs the blob accepted with no rows")
	}

	if err := b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if rm.removedDigests()[string(d)] != 1 {
		t.Fatalf("RemoveBlob calls for %x = %d, want 1: the accepted blob must be removed from the network", d, rm.removedDigests()[string(d)])
	}
	if _, err := mem.GetIntent(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("intent for %x survived the release (err=%v)", d, err)
	}
}
