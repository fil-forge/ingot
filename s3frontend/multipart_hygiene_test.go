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
// NopUploader cannot produce. AbortBlob calls are recorded.
type parkingUploader struct {
	inmem.NopUploader
	mu      sync.Mutex
	aborted []string
}

func (p *parkingUploader) UploadBlob(_ context.Context, _ did.DID, digest multihash.Multihash, size int64, _ string, _ ...uploader.UploadOption) (uploader.UploadedBlob, error) {
	c := cid.NewCidV1(cid.Raw, digest)
	return uploader.UploadedBlob{Digest: digest, Size: size, AddTask: c, AcceptTask: c}, nil
}

func (p *parkingUploader) AbortBlob(_ context.Context, _ did.DID, d multihash.Multihash, _ cid.Cid) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.aborted = append(p.aborted, string(d))
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
	if in, err := mem.GetIntent(ctx, locs.refused); err != nil || in.State != registry.IntentAccepted {
		t.Fatalf("refused blob intent after retry = %v/%v, want accepted", in, err)
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

// failOnceDeletePark is a park store whose next delete fails, leaving a row
// behind for a blob whose acceptance is fully recorded.
type failOnceDeletePark struct {
	registry.ParkStore
	armed bool
}

func (f *failOnceDeletePark) DeletePark(ctx context.Context, digest multihash.Multihash) error {
	if f.armed {
		f.armed = false
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
	if in, err := mem.GetIntent(ctx, d); err != nil || in.State != registry.IntentAccepted {
		t.Fatalf("blob %x intent = %v/%v, want accepted", d, in, err)
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
	parks := &failOnceDeletePark{armed: true}
	b, mem := newDeferredBackend(t, hc, func(d *Deps) {
		parks.ParkStore = d.Parks
		d.Parks = parks
	})
	key := "park-delete-fail"
	uploadID, parts := completeTwoParts(t, b, key)

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err == nil {
		t.Fatal("Complete succeeded although one park failed to delete")
	}
	if parks.armed {
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
	parks := &failOnceDeletePark{armed: true}
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
