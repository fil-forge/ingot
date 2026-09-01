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

// assertReleased asserts a digest's enc-params row and upload intent are gone
// and (wantRemoved) its network release was invoked.
func assertReleased(t *testing.T, mem *inmem.MemStore, rm *recordingRemover, d multihash.Multihash, wantRemoved bool) {
	t.Helper()
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
		assertReleased(t, mem, rm, d, true)
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
		assertReleased(t, mem, rm, d, true)
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
		assertReleased(t, mem, rm, d, true)
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
		assertReleased(t, mem, rm, d, true)
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

	pu := &parkingUploader{}
	b := New(Deps{
		Authority:  mem,
		Registry:   mem,
		Intents:    mem,
		Locations:  mem,
		BlobRefs:   mem,
		GC:         mem,
		Multipart:  mem,
		Parks:      mem,
		Reads:      blockstore.NewLayered(spool, log, inmem.NopBaseReader{}),
		Log:        log,
		Spool:      spool,
		Uploader:   pu,
		Deferred:   pu,
		Remover:    &recordingRemover{},
		EncParams:  mem,
		RegionKeys: testRegionKeys(t),
		TenantKeys: testTenantKeys(),
	})
	if err := mem.Create(ctx, "bk", did.Undef, registry.CreateState{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return b, mem, pu
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
