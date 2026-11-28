package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err, "ListParts")
	for _, p := range parts {
		if p.PartNumber == n {
			require.NotEmpty(t, p.BlobDigests, "part %d has no blob digests", n)
			return p.BlobDigests
		}
	}
	require.FailNow(t, "part not recorded", "part %d not recorded for upload %s", n, uploadID)
	return nil
}

// drainReleases executes every due release (unit backends run with
// a zero release grace, so everything enqueued is immediately due).
func drainReleases(t *testing.T, b *Backend) {
	t.Helper()
	_, err := b.SweepPendingReleases(context.Background())
	require.NoError(t, err, "SweepPendingReleases")
}

// assertReleased drains the deferred-release queue, then asserts a digest's
// enc-params row and upload intent are gone and (wantRemoved) its network
// release was invoked: a remove of an accepted blob or an abort of a parked
// one.
func assertReleased(t *testing.T, b *Backend, mem *testStore, rm *recordingRemover, d multihash.Multihash, wantRemoved bool) {
	t.Helper()
	drainReleases(t, b)
	ctx := context.Background()
	_, err := mem.GetEncryptionParams(ctx, mem.bk(), d)
	require.ErrorIs(t, err, registry.ErrNotFound, "enc-params row for %x survived — crypto-shred missing", d)
	_, err = mem.IntentByDigest(ctx, d)
	require.ErrorIs(t, err, registry.ErrNotFound, "upload intent for %x survived", d)
	got := rm.removedDigests()[string(d)] > 0 || abortedOn(b, d)
	require.Equal(t, wantRemoved, got, "network release of %x invoked", d)
}

// assertRetained asserts a digest's enc-params row survives and no network
// release was invoked for it.
func assertRetained(t *testing.T, mem *testStore, rm *recordingRemover, d multihash.Multihash) {
	t.Helper()
	_, err := mem.GetEncryptionParams(context.Background(), mem.bk(), d)
	require.NoError(t, err, "enc-params row for retained blob %x", d)
	require.LessOrEqual(t, rm.removedDigests()[string(d)], 0, "retained blob %x was released on the network", d)
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
	require.NoError(t, err, "UploadPart 1")
	_, err = mpUploadPart(t, b, key, uploadID, 2, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err, "UploadPart 2")
	winners := hygienePartDigests(t, mem, uploadID, 1)
	orphans := hygienePartDigests(t, mem, uploadID, 2)

	one := int32(1)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out1.ETag}}, nil)
	require.NoError(t, err, "Complete")

	for _, d := range orphans {
		assertReleased(t, b, mem, rm, d, true)
	}
	for _, d := range winners {
		assertRetained(t, mem, rm, d)
	}
	got := getRange(t, b, key, "")
	require.Equal(t, winner, got, "winner mismatch after orphan reap (%d bytes)", len(got))
}

// TestSupersededPartReleased: re-uploading a part number releases the
// replaced part's blobs inline (last write wins at Complete).
func TestSupersededPartReleased(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	key := "supersede"

	uploadID := mpCreate(t, b, key, "", "")
	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err, "UploadPart (loser)")
	old := hygienePartDigests(t, mem, uploadID, 1)

	winner := testBody(int(backend.MinPartSize) + 100)
	out2, err := mpUploadPart(t, b, key, uploadID, 1, winner, nil)
	require.NoError(t, err, "UploadPart (winner)")
	for _, d := range old {
		assertReleased(t, b, mem, rm, d, true)
	}
	for _, d := range hygienePartDigests(t, mem, uploadID, 1) {
		assertRetained(t, mem, rm, d)
	}

	one := int32(1)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out2.ETag}}, nil)
	require.NoError(t, err, "Complete")
	got := getRange(t, b, key, "")
	require.Equal(t, winner, got, "winner mismatch after supersede (%d bytes)", len(got))
}

// TestAbortReleasesUnclaimedBlobs: a client abort releases every part blob —
// including accepted-with-zero-claims ones, the state every part blob has
// under the NopUploader.
func TestAbortReleasesUnclaimedBlobs(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	bucket, key := "bk", "abort"

	uploadID := mpCreate(t, b, key, "", "")
	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err, "UploadPart")
	digests := hygienePartDigests(t, mem, uploadID, 1)

	require.NoError(t, b.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket: &bucket, Key: &key, UploadId: &uploadID,
	}), "Abort")
	_, err = mem.GetSession(context.Background(), uploadID)
	require.ErrorIs(t, err, registry.ErrNotFound, "session survived the abort")
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
	require.NoError(t, err, "UploadPart")
	one := int32(1)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil)
	require.NoError(t, err, "Complete")
	winners := blobDigestsOf(t, b, key, "")

	n, err := sweepUploads(ctx, b)
	require.NoError(t, err, "sweep")
	require.NotZero(t, n, "sweep: cleaned")
	_, err = mem.GetSession(ctx, uploadID)
	require.ErrorIs(t, err, registry.ErrNotFound, "completed session survived the sweep")
	for _, d := range winners {
		assertRetained(t, mem, rm, d)
	}
	got := getRange(t, b, key, "")
	require.Equal(t, data, got, "object unreadable after the sweep (%d bytes)", len(got))
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
	require.NoError(t, err, "UploadPart")
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	require.NoError(t, mem.DeleteEncryptionParams(ctx, mem.bk(), d), "DeleteEncryptionParams")
	// The part as a row from before the lengths were kept.
	part := partRecord(t, mem, uploadID, 1)
	part.SpanLengths = nil
	trees, err := mem.ListPartTrees(ctx, uploadID)
	require.NoError(t, err, "ListPartTrees")
	_, err = mem.PutPart(ctx, part, trees[1])
	require.NoError(t, err, "PutPart")

	one := int32(1)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil)
	require.Error(t, err, "Complete over a missing enc-params row succeeded — a corrupt span would have committed")
	require.ErrorContains(t, err, "no encryption-params row", "Complete error: want the missing-row rejection")
	sess, err := mem.GetSession(ctx, uploadID)
	require.NoError(t, err, "GetSession")
	require.Equal(t, registry.SessionOpen, sess.State, "session state = %q after failed Complete", sess.State)
}

// TestDrainSpaceReleasesIgnoresGrace: DeleteBucket must be able to execute a
// space's pending releases immediately — deleted objects' blobs would
// otherwise stay registered behind the reader grace and hilt would refuse
// the space deletion.
func TestDrainSpaceReleasesIgnoresGrace(t *testing.T) {
	// A live grace: the delete queues the blob's release an hour out, so
	// neither a worker nor the ordinary sweep touches it.
	mem := newTestStore(t)
	b, _, rm := newRefTestBackendOver(t, mem, newRetainedLog(t, mem), 0, func(d *Deps) { d.ReleaseGrace = time.Hour })
	ctx := context.Background()
	data := testBody(1 << 10)

	putObj(t, b, "k1", data)
	d := blobDigestOf(t, b, "k1", "")
	deleteObj(t, b, "k1")

	n, err := b.SweepPendingReleases(ctx)
	require.NoError(t, err, "sweep")
	require.Zero(t, n, "sweep executed releases — intent should not be due")

	require.NoError(t, b.blobs.DrainSpace(ctx, mem.bk()), "drain the space's releases")
	_, err = mem.GetEncryptionParams(ctx, mem.bk(), d)
	require.ErrorIs(t, err, registry.ErrNotFound, "enc-params row survived the space drain")
	removed := rm.removedDigests()
	require.Equal(t, 1, removed[string(d)], "RemoveBlob calls from the space drain (all removals: %v)", removed)
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
	require.NoError(t, err, "UploadPart")
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
		require.NoError(t, errs[i], "racer %d", i)
	}
	for i := 1; i < racers; i++ {
		require.Equal(t, etags[0], etags[i], "racer %d ETag differs from racer 0's", i)
	}
	got := getRange(t, b, key, "")
	require.Equal(t, data, got, "object mismatch after racing Completes (%d bytes)", len(got))
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
	require.NoError(t, err, "UploadPart")
	one := int32(1)
	parts := []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}
	first, err := mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "Complete")

	overwrite := []byte("overwritten by a single-shot PUT")
	putObj(t, b, key, overwrite)

	again, err := mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "re-Complete after overwrite")
	require.Equal(t, *first.ETag, *again.ETag, "re-Complete ETag")
	got := getRange(t, b, key, "")
	require.Equal(t, overwrite, got, "re-Complete disturbed the overwrite (%d bytes)", len(got))
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
	limits := storeFor(b).limits
	saveBudget := limits.CompleteWait
	limits.CompleteWait = 50 * time.Millisecond

	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	var apiErr s3err.APIError
	require.ErrorAs(t, err, &apiErr, "Complete against a slow winner")
	require.Equal(t, "OperationAborted", apiErr.Code, "Complete against a slow winner")
	close(g.open)
	require.NoError(t, <-first, "first Complete")
	limits.CompleteWait = saveBudget
	_, err = mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "repeated Complete")
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
	require.NoError(t, err, "cache")
	log, err := logstore.Open(ctx, logstore.Config{
		Dir:     filepath.Join(dir, "segments"),
		Meta:    mem,
		Catalog: logstore.PlaneConfig{Ship: false},
		Logger:  zaptest.NewLogger(t),
	})
	require.NoError(t, err, "logstore")
	t.Cleanup(func() { _ = log.Close(ctx) })

	deps := Deps{
		Authority:  mem,
		Registry:   mem,
		Intents:    mem,
		Locations:  mem,
		Multipart:  mem,
		Parks:      mem,
		Reads:      blockstore.NewLayered(cache, log, testfakes.NopBaseReader{}),
		Log:        log,
		Cache:      cache,
		Deferred:   up,
		Remover:    &recordingRemover{},
		Capturer:   testfakes.NopUploader{},
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
	require.NoError(t, mem.Create(ctx, "bk", did.Undef, registry.CreateState{}), "create bucket")
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
	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err, "UploadPart")
	digests := hygienePartDigests(t, mem, uploadID, 1)
	for _, d := range digests {
		in, err := mem.IntentByDigest(ctx, d)
		require.NoError(t, err, "part blob %x intent", d)
		require.Equal(t, registry.IntentParked, in.State, "part blob %x intent", d)
	}

	require.NoError(t, b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: &bucket, Key: &key, UploadId: &uploadID,
	}), "Abort")
	aborted := pu.abortedDigests()
	for _, d := range digests {
		require.True(t, aborted[string(d)], "parked blob %x was not aborted on the provider", d)
		_, err := mem.ParkByDigest(ctx, d)
		require.ErrorIs(t, err, registry.ErrNotFound, "park row for %x survived", d)
		_, err = mem.GetEncryptionParams(ctx, mem.bk(), d)
		require.ErrorIs(t, err, registry.ErrNotFound, "enc-params row for %x survived", d)
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
		require.NoError(t, err, "UploadPart %d", n)
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
	in, err := mem.IntentByDigest(ctx, d)
	require.NoError(t, err, "blob %x intent", d)
	require.True(t, in.State == registry.IntentAccepted || in.State == registry.IntentPublished, "blob %x intent = %v, want accepted or published", d, in.State)
	loc, err := mem.GetLocation(ctx, mem.bk(), d)
	require.NoError(t, err, "blob %x has no location", d)
	require.NotNil(t, loc, "blob %x has no location", d)
	_, err = mem.ParkByDigest(ctx, d)
	require.ErrorIs(t, err, registry.ErrNotFound, "park row for blob %x survived", d)
}

// plantStaleAcceptedPark puts uploadID's two part blobs in the state another
// write's acceptance of the same bytes leaves them: both located and
// accepted, one park row still standing beside its location. It returns the
// digests and the one still parked.
func plantStaleAcceptedPark(t *testing.T, mem *testStore, uploadID string) (digests []multihash.Multihash, stale multihash.Multihash) {
	t.Helper()
	ctx := context.Background()
	sess, err := mem.GetSession(ctx, uploadID)
	require.NoError(t, err, "GetSession")
	digests = hygienePartDigests(t, mem, uploadID, 1)
	digests = append(digests, hygienePartDigests(t, mem, uploadID, 2)...)
	for i, d := range digests {
		loc := registry.BlobLocation{Space: sess.Space, Digest: d, Provider: "did:key:zNode", URL: "https://node.example/blob", Size: 1}
		require.NoError(t, mem.PutLocation(ctx, loc), "PutLocation")
		if i > 0 {
			require.NoError(t, mem.DeletePark(ctx, sess.Space, d), "DeletePark")
		}
		require.NoError(t, mem.SetIntentState(ctx, sess.Space, d, registry.IntentAccepted), "SetIntentState")
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

	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "Complete")
	require.Empty(t, hc.calls, "conclude calls = %v, want none: every blob is located", hc.calls)
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

	n, err := sweepUploads(ctx, b)
	require.NoError(t, err, "sweep")
	require.NotZero(t, n, "sweep: cleaned")
	aborted := hc.abortedDigests()
	for _, d := range digests {
		_, err := mem.ParkByDigest(ctx, d)
		require.ErrorIs(t, err, registry.ErrNotFound, "park row for accepted blob %x survived the sweep", d)
		require.False(t, aborted[string(d)], "accepted blob %x was aborted on the provider as though parked", d)
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
	n, err := sweepUploads(ctx, b)
	require.NoError(t, err, "sweep")
	require.NotZero(t, n, "sweep: cleaned")
	drainReleases(t, b)
	_, err = mem.ParkByDigest(ctx, stale)
	require.ErrorIs(t, err, registry.ErrNotFound, "park row for %x survived the release sweep", stale)
	_, err = mem.GetLocation(ctx, mem.bk(), stale)
	require.ErrorIs(t, err, registry.ErrNotFound, "location for %x survived the release sweep", stale)
	_, err = mem.IntentByDigest(ctx, stale)
	require.ErrorIs(t, err, registry.ErrNotFound, "intent for %x survived the release sweep", stale)
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
	require.NoError(t, err, "UploadPart of a located blob failed on a park delete")
	require.LessOrEqual(t, parks.fails, 0, "the park store never refused a delete")
	digests := hygienePartDigests(t, mem, uploadID, 1)
	require.Len(t, digests, 1, "part digests")
	d := digests[0]
	in, err := mem.IntentByDigest(ctx, d)
	require.NoError(t, err, "dedup part intent")
	require.Equal(t, registry.IntentAccepted, in.State, "dedup part intent")
	// The fake answers located; the claim at Complete reads the row itself.
	require.NoError(t, mem.PutLocation(ctx, registry.BlobLocation{Space: mem.bk(), Digest: d, Provider: "did:key:zNode", URL: "https://node.example/blob", Size: 1}), "PutLocation")

	// The stale row such a failure leaves behind, as a Complete that failed
	// after recording the acceptance would.
	_, err = mem.PutPark(ctx, registry.BlobPark{Space: mem.bk(), Digest: d, AddTask: []byte{1}, AcceptTask: []byte{1}, PutInvocation: []byte{1}, Size: 1})
	require.NoError(t, err, "PutPark")
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil)
	require.NoError(t, err, "Complete")
	require.Empty(t, hc.calls, "conclude calls = %v, want none: the blob was located throughout", hc.calls)
	_, err = mem.ParkByDigest(ctx, d)
	require.ErrorIs(t, err, registry.ErrNotFound, "stale park row survived Complete")
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
	_, err := mpUploadPart(t, b, "lost-accept", uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err, "UploadPart")
	digests := hygienePartDigests(t, mem, uploadID, 1)
	require.Len(t, digests, 1, "part digests")
	d := digests[0]
	in, err := mem.IntentByDigest(ctx, d)
	require.NoError(t, err, "part blob intent")
	require.Equal(t, registry.IntentParked, in.State, "part blob intent")
	pu.acceptedOnProvider[string(d)] = true

	n, err := sweepUploads(ctx, b)
	require.NoError(t, err, "sweep")
	require.NotZero(t, n, "sweep: cleaned")
	require.True(t, pu.abortedDigests()[string(d)], "abort was never attempted for %x", d)
	_, err = mem.ParkByDigest(ctx, d)
	require.ErrorIs(t, err, registry.ErrNotFound, "park row for %x survived the sweep", d)
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
	require.NoError(t, err, "UploadPart")
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	require.NoError(t, mem.EnqueueRelease(ctx, mem.bk(), d, time.Now().Add(-time.Second)), "EnqueueRelease")

	n, err := b.SweepPendingReleases(ctx)
	require.NoError(t, err, "sweep")
	require.Zero(t, n, "sweep executed releases: the part is in flight")
	_, err = mem.ParkByDigest(ctx, d)
	require.NoError(t, err, "park row for the in-flight part was taken by the release sweep")
	require.False(t, pu.abortedDigests()[string(d)], "in-flight part blob %x was aborted on the provider", d)
	pending, err := mem.ListReleasesBySpace(ctx, mem.bk())
	require.NoError(t, err, "pending releases")
	require.Empty(t, pending, "pending releases: the release was stale")

	one := int32(1)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil)
	require.NoError(t, err, "Complete")
	_, err = mem.GetEncryptionParams(ctx, mem.bk(), d)
	require.NoError(t, err, "claimed blob %x lost its enc-params row", d)
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
	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err, "UploadPart (first)")
	old := hygienePartDigests(t, mem, uploadID, 1)

	mp.fails = 1
	_, err = mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)+100), nil)
	require.Error(t, err, "re-upload succeeded although the part could not be recorded")
	got := hygienePartDigests(t, mem, uploadID, 1)
	require.Len(t, got, len(old), "part 1 blobs after the failed re-upload: want the old part intact")
	require.Equal(t, string(old[0]), string(got[0]), "part 1 blobs after the failed re-upload: want the old part intact")
	for _, d := range old {
		_, err := mem.ParkByDigest(ctx, d)
		require.NoError(t, err, "old blob %x lost its park on a failed re-upload", d)
		require.False(t, pu.abortedDigests()[string(d)], "old blob %x was aborted on a failed re-upload", d)
	}
	drainReleases(t, b)
	for _, d := range old {
		require.False(t, pu.abortedDigests()[string(d)], "a failed re-upload recorded the old blob %x for release", d)
	}

	// With the store back, the re-upload lands and the old blobs go.
	_, err = mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)+100), nil)
	require.NoError(t, err, "UploadPart (retry)")
	for _, d := range old {
		require.True(t, pu.abortedDigests()[string(d)], "superseded blob %x was not released on the provider", d)
		_, err := mem.ParkByDigest(ctx, d)
		require.ErrorIs(t, err, registry.ErrNotFound, "park row for superseded %x survived", d)
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
	require.NoError(t, err, "UploadPart")
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	one := int32(1)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil)
	require.NoError(t, err, "Complete")
	deleteObj(t, b, key)

	n, err := b.SweepPendingReleases(ctx)
	require.NoError(t, err, "first sweep")
	require.Zero(t, n, "first sweep executed releases: the network remove failed")
	loc, err := mem.GetLocation(ctx, mem.bk(), d)
	require.NoError(t, err, "location row went although the network still holds the blob")
	require.NotNil(t, loc, "location row went although the network still holds the blob")
	pending, _ := mem.ListReleasesBySpace(ctx, mem.bk())
	require.Len(t, pending, 1, "release record after the failed attempt")
	// A part keeps no local copy; the intent is what the retry needs.
	// TestReleaseFreesCachedCopyOfDeletedPutObject covers a cached copy kept
	// for the retry.
	in, err := mem.IntentByDigest(ctx, d)
	require.NoError(t, err, "intent after the failed attempt")
	require.Equal(t, registry.IntentPublished, in.State, "intent after the failed attempt: want published and kept for the retry")

	n, err = b.SweepPendingReleases(ctx)
	require.NoError(t, err, "second sweep")
	require.Equal(t, 1, n, "second sweep executed releases")
	require.Equal(t, 1, rm.removedDigests()[string(d)], "RemoveBlob calls for %x", d)
	_, err = mem.GetLocation(ctx, mem.bk(), d)
	require.ErrorIs(t, err, registry.ErrNotFound, "location row survived the release")
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

	n, err := b.SweepPendingReleases(ctx)
	require.NoError(t, err, "first sweep")
	require.Zero(t, n, "first sweep executed releases: the network remove failed")
	in, err := mem.IntentByDigest(ctx, d)
	require.NoError(t, err, "intent after the failed attempt")
	require.Equal(t, registry.IntentPublished, in.State, "intent after the failed attempt: want published and kept for the retry")
	_, err = os.Stat(localPath(b, d))
	require.NoError(t, err, "cached copy after the failed attempt: want it kept for the retry")

	n, err = b.SweepPendingReleases(ctx)
	require.NoError(t, err, "second sweep")
	require.Equal(t, 1, n, "second sweep executed releases")
	settleCache(t, b, d) // the release evicts the cached copy through the queue
	assertLocalCopyReleased(t, b, mem, d)
}

// assertLocalCopyReleased fails the test unless the blob's upload intent and
// cached copy are both gone, as a finished release leaves them. The copy goes
// through the queue-shard.evict the release queues, which runs after it, so
// it is waited for.
func assertLocalCopyReleased(t *testing.T, b *Backend, mem *testStore, d multihash.Multihash) {
	t.Helper()
	in, err := mem.IntentByDigest(context.Background(), d)
	require.ErrorIs(t, err, registry.ErrNotFound, "blob %x intent = %v/%v after its release", d, in, err)
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := os.Stat(localPath(b, d))
		if os.IsNotExist(err) {
			return
		}
		require.False(t, time.Now().After(deadline), "blob %x cached copy after its release: stat err=%v, want not-exist", d, err)
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
	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "Complete")
	winners := blobDigestsOf(t, b, key, "")
	deleteObj(t, b, key)

	n, err := sweepUploads(ctx, b)
	require.NoError(t, err, "sweep")
	require.NotZero(t, n, "sweep: cleaned")
	drainReleases(t, b)
	for _, d := range winners {
		require.Equal(t, 1, rm.removedDigests()[string(d)], "winner %x removals: want once by the object's own release", d)
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
	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "Complete")
	winners := blobDigestsOf(t, b, key, "")
	deleteObj(t, b, key)
	drainReleases(t, b)
	for _, d := range winners {
		assertLocalCopyReleased(t, b, mem, d)
	}

	n, err := sweepUploads(ctx, b)
	require.NoError(t, err, "sweep")
	require.NotZero(t, n, "sweep: cleaned")
	pending, _ := mem.ListReleasesBySpace(ctx, mem.bk())
	require.Empty(t, pending, "reap after the release recorded %d releases", len(pending))
	drainReleases(t, b)
	for _, d := range winners {
		require.Equal(t, 1, rm.removedDigests()[string(d)], "winner %x removals: want once by the object's own release", d)
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
	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err, "UploadPart")
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	// A second session referencing the same blob, aborted first: its
	// release is stale while this session's part still names the blob.
	const other = "other-session"
	require.NoError(t, mem.CreateSession(ctx, registry.MultipartSession{UploadID: other, Bucket: bucket, Space: mem.bk(), ObjectKey: "other"}), "CreateSession")
	_, err = mem.PutPart(ctx, registry.MultipartPart{UploadID: other, PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{d}, State: registry.PartParked}, nil)
	require.NoError(t, err, "PutPart")
	require.NoError(t, b.abortUpload(ctx, registry.MultipartSession{UploadID: other, Space: mem.bk()}), "abort other")
	require.False(t, pu.abortedDigests()[string(d)], "shared blob %x released while this session's part still names it", d)

	require.NoError(t, b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}), "Abort")
	require.True(t, pu.abortedDigests()[string(d)], "shared blob %x was not released: the other session's part reference stopped the record", d)
	_, err = mem.ParkByDigest(ctx, d)
	require.ErrorIs(t, err, registry.ErrNotFound, "park row for %x survived", d)
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
	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err, "UploadPart")
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	const other = "other-open"
	require.NoError(t, mem.CreateSession(ctx, registry.MultipartSession{UploadID: other, Bucket: bucket, Space: mem.bk(), ObjectKey: "other"}), "CreateSession")
	_, err = mem.PutPart(ctx, registry.MultipartPart{UploadID: other, PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{d}, State: registry.PartParked}, nil)
	require.NoError(t, err, "PutPart")

	require.NoError(t, b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}), "Abort")
	require.False(t, pu.abortedDigests()[string(d)], "shared blob %x was released while another open session references it", d)
	_, err = mem.ParkByDigest(ctx, d)
	require.NoError(t, err, "park row for %x went while another open session references it", d)
	pending, _ := mem.ListReleasesBySpace(ctx, mem.bk())
	require.Empty(t, pending, "pending releases: the release was stale")
	otherKey := "other"
	otherID := other
	require.NoError(t, b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &otherKey, UploadId: &otherID}), "Abort the other session")
	drainReleases(t, b)
	require.True(t, pu.abortedDigests()[string(d)], "shared blob %x was not released once no session referenced it", d)
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
	require.NoError(t, err, "did.Parse")
	require.NoError(t, mem.Create(ctx, bucket, space, registry.CreateState{}), "create bucket")
	res, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "CreateMultipartUpload")
	uploadID := res.UploadId
	one := int32(1)
	_, err = b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &one, Body: bytes.NewReader(testBody(int(backend.MinPartSize))), ContentLength: byteLen(testBody(int(backend.MinPartSize)))})
	require.NoError(t, err, "UploadPart")
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	sess, err := mem.GetSession(ctx, uploadID)
	require.NoError(t, err, "session")
	require.Equal(t, space, sess.Space, "session space: want the one recorded at create")

	// The bucket row goes while the session stands.
	require.NoError(t, mem.Delete(ctx, bucket, space), "delete bucket row")
	n, err := sweepUploads(ctx, b)
	require.NoError(t, err, "sweep")
	require.NotZero(t, n, "sweep: cleaned")
	_, err = mem.GetSession(ctx, uploadID)
	require.ErrorIs(t, err, registry.ErrNotFound, "session survived the sweep")
	spaces := releaseSpaces(t, mem)
	require.True(t, slices.Contains(spaces, space), "releases queued against %v, want the session's space %v", spaces, space)
	require.True(t, pu.abortedDigests()[string(d)], "parked blob %x was not released on the provider", d)
	_, err = mem.ParkByDigest(ctx, d)
	require.ErrorIs(t, err, registry.ErrNotFound, "park row for %x survived", d)
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
	require.NoError(t, mem.Create(ctx, bucket, oldSpace, registry.CreateState{}), "create bucket")
	res, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "CreateMultipartUpload")
	uploadID := res.UploadId
	one := int32(1)
	out, err := b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &one, Body: bytes.NewReader(testBody(int(backend.MinPartSize))), ContentLength: byteLen(testBody(int(backend.MinPartSize)))})
	require.NoError(t, err, "UploadPart")
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	// Same name, new space.
	require.NoError(t, mem.Delete(ctx, bucket, oldSpace), "delete bucket row")
	require.NoError(t, mem.Create(ctx, bucket, newSpace, registry.CreateState{}), "recreate bucket")

	wantNoSuchUpload := func(op string, err error) {
		t.Helper()
		var apiErr s3err.APIError
		require.ErrorAs(t, err, &apiErr, "%s against the recreated bucket: err = %v, want NoSuchUpload", op, err)
		require.Equal(t, "NoSuchUpload", apiErr.Code, "%s against the recreated bucket: err", op)
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
	require.NoError(t, err, "ListMultipartUploads")
	require.Empty(t, listed.Uploads, "the recreated bucket lists %d of its predecessor's uploads", len(listed.Uploads))
	_, err = mem.GetSession(ctx, uploadID)
	require.NoError(t, err, "the disowned session should stand for the sweeper")
	// Deleting the new bucket neither waits on the predecessor's upload nor
	// tears it down.
	del := context.WithValue(ctx, reqscope.RequestKey(), cmds3.Request{})
	require.NoError(t, b.DeleteBucket(del, bucket), "DeleteBucket of the recreated bucket")
	_, err = mem.GetSession(ctx, uploadID)
	require.NoError(t, err, "the predecessor's session after the new bucket's delete")

	n, err := sweepUploads(ctx, b)
	require.NoError(t, err, "sweep")
	require.NotZero(t, n, "sweep: cleaned")
	spaces := releaseSpaces(t, mem)
	require.True(t, slices.Contains(spaces, oldSpace), "releases queued against %v, want the session's own space %v and not %v", spaces, oldSpace, newSpace)
	require.False(t, slices.Contains(spaces, newSpace), "releases queued against %v, want the session's own space %v and not %v", spaces, oldSpace, newSpace)
	require.True(t, pu.abortedDigests()[string(d)], "parked blob %x was not released on the provider", d)
	_, err = mem.GetSession(ctx, uploadID)
	require.ErrorIs(t, err, registry.ErrNotFound, "session survived the sweep")
}

// teardownBeforePutPart is an authority capturer that runs hook once, at the
// second capture of /blob/remove authority after it is armed: UploadPart
// captures it to park the part's blobs and again for the part's add-part,
// right before it runs, so the hook lands in the window after UploadPart
// streamed its blobs and before its part row, where a teardown can take the
// session.
type teardownBeforePutPart struct {
	uploader.AuthorityCapturer
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
	return s.AuthorityCapturer.CaptureAuthority(ctx, space, cmd)
}

// TestUploadPartRefusedAfterTeardownReleasesItsBlobs: a sweep takes the
// session after an UploadPart was admitted and streamed its blobs. The part
// row is refused, since the session is no longer open, the upload fails
// with NoSuchUpload, and the blobs it streamed are released rather than
// left behind with no row pointing at them.
func TestUploadPartRefusedAfterTeardownReleasesItsBlobs(t *testing.T) {
	mp := &teardownBeforePutPart{}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		mp.AuthorityCapturer = d.Capturer
		d.Capturer = mp
	})
	ctx := context.Background()
	key := "refused-part"
	uploadID := mpCreate(t, b, key, "", "")
	mp.hook = func() {
		n, err := sweepUploads(ctx, b)
		assert.NoError(t, err, "sweep before the part row")
		assert.Equal(t, 1, n, "sweep before the part row: cleaned")
	}

	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	var apiErr s3err.APIError
	require.ErrorAs(t, err, &apiErr, "UploadPart whose session was taken: err = %v, want NoSuchUpload", err)
	require.Equal(t, "NoSuchUpload", apiErr.Code, "UploadPart whose session was taken: err")
	require.Nil(t, mp.hook, "the teardown never ran")
	drainReleases(t, b)
	entries, err := os.ReadDir(b.cache.Path(nil))
	require.NoError(t, err, "read cache dir")
	require.Empty(t, entries, "cache holds %d blobs after the refused part was released", len(entries))
	pending, _ := mem.ListReleasesBySpace(ctx, mem.bk())
	require.Empty(t, pending, "pending releases after the refused part")
}

// failPutPart is a multipart store whose next `fails` part writes fail with
// a store error, as an unreachable registry does.
type failPutPart struct {
	registry.MultipartStore
	fails int
}

func (f *failPutPart) PartWrites(ctx context.Context, p registry.MultipartPart, tree *registry.PartTree) (registry.CommitWrites, error) {
	if f.fails > 0 {
		f.fails--
		return registry.CommitWrites{}, errors.New("parts table unavailable")
	}
	return f.MultipartStore.PartWrites(ctx, p, tree)
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

	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.Error(t, err, "UploadPart succeeded although the part row write failed")
	require.LessOrEqual(t, mp.fails, 0, "the part store never refused a write")
	drainReleases(t, b)
	entries, err := os.ReadDir(b.cache.Path(nil))
	require.NoError(t, err, "read cache dir")
	require.Empty(t, entries, "cache blobs after the failed row write")
	n := abortCount(b)
	require.Equal(t, 1, n, "aborted parks: want the one that streamed")
	sess, err := mem.GetSession(ctx, uploadID)
	require.NoError(t, err, "session after the failed part")
	require.Equal(t, registry.SessionOpen, sess.State, "session after the failed part")
	// The retry lands.
	_, err = mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err, "retry UploadPart")
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
	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.Error(t, err, "UploadPart succeeded although the part row write failed")
	require.LessOrEqual(t, shred.fails, 0, "the shred was never attempted")
	pending, _ := mem.ListReleasesBySpace(ctx, mem.bk())
	require.Len(t, pending, 1, "pending releases after the failed attempt")
	d := pending[0].Digest
	in, err := mem.IntentByDigest(ctx, d)
	require.NoError(t, err, "intent after the failed attempt")
	require.Equal(t, registry.IntentParked, in.State, "intent after the failed attempt")

	drainReleases(t, b)
	_, err = mem.IntentByDigest(ctx, d)
	require.ErrorIs(t, err, registry.ErrNotFound, "intent survived the retry")
	require.True(t, abortedOn(b, d), "the parked blob %x was not aborted", d)
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
		_, err := mpUploadPart(t, b, key, uploadID, n, same, nil)
		require.NoError(t, err, "UploadPart %d", n)
	}
	shared := hygienePartDigests(t, mem, uploadID, 1)[0]
	other := hygienePartDigests(t, mem, uploadID, 2)[0]
	require.Equal(t, string(shared), string(other), "parts of identical bytes stored as received have distinct blobs: %x vs %x", shared, other)

	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)+100), nil)
	require.NoError(t, err, "UploadPart (supersede)")
	// Part 2 still names the blob: the release, run as the supersede
	// returns, is stale, and the blob stays.
	drainReleases(t, b)
	_, err = mem.ParkByDigest(ctx, shared)
	require.NoError(t, err, "shared blob's park taken while part 2 names it")
	require.False(t, pu.abortedDigests()[string(shared)], "shared blob %x aborted on the provider while part 2 names it", shared)
	pending, err := mem.ListReleasesBySpace(ctx, mem.bk())
	require.NoError(t, err, "releases after the supersede")
	require.Empty(t, pending, "releases after the supersede")

	// The session ends: nothing names the blob and the release runs.
	bucket := "bk"
	require.NoError(t, b.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}), "Abort")
	require.NoError(t, b.blobs.DrainSpace(ctx, mem.bk()), "drain the space's releases")
	require.True(t, pu.abortedDigests()[string(shared)], "shared blob %x not released once the session ended", shared)
	_, err = mem.ParkByDigest(ctx, shared)
	require.ErrorIs(t, err, registry.ErrNotFound, "park row for %x survived the session", shared)
	pending, _ = mem.ListReleasesBySpace(ctx, mem.bk())
	require.Empty(t, pending, "release records survived the session")
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
		require.NoError(t, err, "UploadPart")
	}

	gate.mu.Lock()
	listed := append([]int(nil), gate.listed...)
	gate.mu.Unlock()
	require.Len(t, listed, 2, "gated listings saw %v parts, want both empty: neither write may land before both have listed", listed)
	require.Zero(t, listed[0], "gated listings saw %v parts, want both empty: neither write may land before both have listed", listed)
	require.Zero(t, listed[1], "gated listings saw %v parts, want both empty: neither write may land before both have listed", listed)
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
	require.NotEmpty(t, orphans, "every uploaded blob is named by the part: the writes did not interleave")
	// The replacing write recorded the overwritten blobs' releases with its
	// row, and ran them once its own part was parked; one its sibling still
	// held then stands recorded for the sweeper.
	recorded := map[string]bool{}
	pending, err := mem.ListReleasesBySpace(ctx, mem.bk())
	require.NoError(t, err, "ListReleasesBySpace")
	for _, pr := range pending {
		recorded[string(pr.Digest)] = true
	}
	for _, d := range orphans {
		if _, err := mem.IntentByDigest(ctx, d); errors.Is(err, registry.ErrNotFound) && pu.abortedDigests()[string(d)] {
			continue // released already
		}
		require.True(t, recorded[string(d)], "blob %x of the overwritten part has no release record", d)
	}

	require.NoError(t, b.blobs.DrainSpace(ctx, mem.bk()), "drain the space's releases")
	for _, d := range orphans {
		require.True(t, pu.abortedDigests()[string(d)], "overwritten blob %x not released on the provider", d)
		_, err := mem.IntentByDigest(ctx, d)
		require.ErrorIs(t, err, registry.ErrNotFound, "intent for overwritten blob %x survived", d)
	}
	for d := range named {
		require.False(t, pu.abortedDigests()[d], "the part's own blob %x was released", d)
		_, err := mem.ParkByDigest(ctx, multihash.Multihash(d))
		require.NoError(t, err, "the part's own blob %x lost its park", d)
	}
}
