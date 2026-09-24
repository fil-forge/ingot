package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// The object count a space reports is one content entry per committed object
// version: /upload/add when a version commits, /upload/remove when one is
// retired for good. These tests pin that mapping, which follows what AWS
// reports for NumberOfObjects — current and noncurrent versions plus delete
// markers all count.

// recordingRegistrar captures the roots registered and retracted, so a test can
// assert exactly which versions a request counted.
type recordingRegistrar struct {
	mu        sync.Mutex
	added     []cid.Cid
	retracted []cid.Cid
	// batches records the size of each round trip, so a test can assert the
	// sweep spends one call on many changes rather than one per change.
	batches []int
	// spent makes PrepareAuthority fail, standing in for an expired chain the
	// space cannot renew.
	spent bool
}

func (r *recordingRegistrar) roundTrips() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.batches...)
}

func (r *recordingRegistrar) CaptureAuthority(_ context.Context, _ did.DID, _ ucan.Command) ([]byte, error) {
	return container.Encode(container.RawGzip, container.New())
}

// PrepareAuthority keeps whatever the row holds unless spent is set, which
// stands in for a chain that has outlived hilt's delegation with no live
// authority to renew from.
func (r *recordingRegistrar) PrepareAuthority(_ context.Context, space did.DID, _ ucan.Command, stored []byte) ([]byte, bool, error) {
	r.mu.Lock()
	spent := r.spent
	r.mu.Unlock()
	if spent {
		return nil, false, errors.New("no proof store for space " + space.String())
	}
	return stored, false, nil
}

// spendAuthority makes every later PrepareAuthority fail, as it does for a
// space whose stored chain has expired and which has seen no recent write.
func (r *recordingRegistrar) spendAuthority() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spent = true
}

func (r *recordingRegistrar) RegisterUploads(_ context.Context, changes []uploader.QueuedUpload) ([]error, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, len(changes))
	for _, ch := range changes {
		r.added = append(r.added, ch.Root)
	}
	return make([]error, len(changes)), nil
}

func (r *recordingRegistrar) RetractUploads(_ context.Context, changes []uploader.QueuedUpload) ([]error, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, len(changes))
	for _, ch := range changes {
		r.retracted = append(r.retracted, ch.Root)
	}
	return make([]error, len(changes)), nil
}

func (r *recordingRegistrar) snapshot() (added, retracted []cid.Cid) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]cid.Cid(nil), r.added...), append([]cid.Cid(nil), r.retracted...)
}

// count is the space's object count as sprue would derive it: adds minus
// removes.
func (r *recordingRegistrar) count() int {
	added, retracted := r.snapshot()
	return len(added) - len(retracted)
}

// newCountingBackend mirrors newRefTestBackend but swaps the no-op registrar
// for a recording one.
// drainRegistrations runs the sweeper until the queue stops shrinking, the way
// the daemon's ticker eventually would. The registrations are queued by the
// write and applied off the request path, so every assertion below drains
// first.
func drainRegistrations(t *testing.T, b *Backend) {
	t.Helper()
	for range 8 {
		n, err := b.SweepUploadRegistrations(context.Background())
		require.NoError(t, err)
		if n == 0 {
			return
		}
	}
	t.Fatal("registration queue did not drain")
}

func newCountingBackend(t *testing.T) (*Backend, *recordingRegistrar) {
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

	reg := &recordingRegistrar{}
	b := New(Deps{
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
		Uploader:        inmem.NopUploader{},
		Deferred:        inmem.NopUploader{},
		Remover:         &recordingRemover{},
		Registrar:       reg,
		UploadRegs:      mem,
		EncParams:       mem,
		RegionKeys:      testRegionKeys(t),
		TenantKeys:      testTenantKeys(),
		PendingReleases: mem,
	})
	if err := mem.Create(ctx, "bk", did.Undef, registry.CreateState{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return b, reg
}

// TestPutRegistersTheCommittedVersion: a PUT registers exactly the manifest it
// committed, and nothing else.
func TestPutRegistersTheCommittedVersion(t *testing.T) {
	b, reg := newCountingBackend(t)

	putObjV(t, b, "a", []byte("hello"))

	drainRegistrations(t, b)
	added, retracted := reg.snapshot()
	require.Len(t, added, 1, "one version committed, one entry registered")
	require.Empty(t, retracted, "nothing was superseded")
	require.Equal(t, currentManifestCID(t, b, "a"), added[0])
}

// TestOverwriteUnversionedSwapsTheEntry: in a bucket with no versioning the
// prior generation is discarded, so the count stays at one object.
func TestOverwriteUnversionedSwapsTheEntry(t *testing.T) {
	b, reg := newCountingBackend(t)

	putObjV(t, b, "a", []byte("first"))
	drainRegistrations(t, b)
	first, _ := reg.snapshot()
	putObjV(t, b, "a", []byte("second"))

	drainRegistrations(t, b)
	added, retracted := reg.snapshot()
	require.Len(t, added, 2, "both versions registered as they committed")
	require.Len(t, retracted, 1, "the discarded generation retracted")
	require.Equal(t, first[0], retracted[0], "the retracted root is the superseded one")
	require.Equal(t, 1, reg.count(), "one live object")
}

// TestOverwriteVersionedRetainsBothEntries: an enabled bucket retains the
// noncurrent version, and AWS counts it, so nothing is retracted.
func TestOverwriteVersionedRetainsBothEntries(t *testing.T) {
	b, reg := newCountingBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	putObjV(t, b, "a", []byte("first"))
	putObjV(t, b, "a", []byte("second"))

	drainRegistrations(t, b)
	added, retracted := reg.snapshot()
	require.Len(t, added, 2)
	require.Empty(t, retracted, "a retained noncurrent version still counts")
	require.Equal(t, 2, reg.count())
}

// TestDeleteMarkerCounts: a delete marker is a version and AWS counts it, so
// the versioned DELETE registers rather than retracts.
func TestDeleteMarkerCounts(t *testing.T) {
	b, reg := newCountingBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	putObjV(t, b, "a", []byte("hello"))
	if _, err := deleteObjV(t, b, "a", ""); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	drainRegistrations(t, b)
	added, retracted := reg.snapshot()
	require.Len(t, added, 2, "the object version and the delete marker")
	require.Empty(t, retracted)
	require.Equal(t, 2, reg.count())
}

// TestUnversionedDeleteRetractsTheEntry: the key is gone for good, so the
// count returns to zero.
func TestUnversionedDeleteRetractsTheEntry(t *testing.T) {
	b, reg := newCountingBackend(t)

	putObjV(t, b, "a", []byte("hello"))
	drainRegistrations(t, b)
	added, _ := reg.snapshot()
	if _, err := deleteObjV(t, b, "a", ""); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	drainRegistrations(t, b)
	_, retracted := reg.snapshot()
	require.Len(t, retracted, 1)
	require.Equal(t, added[0], retracted[0])
	require.Zero(t, reg.count())
}

// TestVersionScopedDeleteRetractsThatVersion: deleting one version by id
// retracts exactly that version's entry and leaves the other counted.
func TestVersionScopedDeleteRetractsThatVersion(t *testing.T) {
	b, reg := newCountingBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	first := putObjV(t, b, "a", []byte("first"))
	putObjV(t, b, "a", []byte("second"))
	drainRegistrations(t, b)
	added, _ := reg.snapshot()

	if _, err := deleteObjV(t, b, "a", first.VersionID); err != nil {
		t.Fatalf("DeleteObject(versionId): %v", err)
	}

	drainRegistrations(t, b)
	_, retracted := reg.snapshot()
	require.Len(t, retracted, 1)
	require.Equal(t, added[0], retracted[0], "the named version's root, not the current one")
	require.Equal(t, 1, reg.count())
}

// TestDeletingAMissingKeyCountsNothing: an idempotent DELETE of an absent key
// must not move the count.
func TestDeletingAMissingKeyCountsNothing(t *testing.T) {
	b, reg := newCountingBackend(t)

	if _, err := deleteObjV(t, b, "gone", ""); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	drainRegistrations(t, b)
	added, retracted := reg.snapshot()
	require.Empty(t, added)
	require.Empty(t, retracted)
}

// TestCompleteMultipartRegistersOnce: a multipart object counts as one, not one
// per part, and the entry is the manifest Complete committed.
func TestCompleteMultipartRegistersOnce(t *testing.T) {
	b, reg := newCountingBackend(t)

	uploadID := mpCreate(t, b, "big", "", "")
	p1, err := mpUploadPart(t, b, "big", uploadID, 1, bytes.Repeat([]byte("a"), 5*1024*1024), nil)
	require.NoError(t, err)
	p2, err := mpUploadPart(t, b, "big", uploadID, 2, []byte("tail"), nil)
	require.NoError(t, err)

	// Parts are blobs, not versions: nothing counts until Complete commits.
	drainRegistrations(t, b)
	added, _ := reg.snapshot()
	require.Empty(t, added, "uploading parts registers nothing")

	one, two := int32(1), int32(2)
	_, err = mpComplete(t, b, "big", uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: p1.ETag},
		{PartNumber: &two, ETag: p2.ETag},
	}, nil)
	require.NoError(t, err)

	drainRegistrations(t, b)
	added, retracted := reg.snapshot()
	require.Len(t, added, 1, "one object, however many parts")
	require.Empty(t, retracted)
	require.Equal(t, currentManifestCID(t, b, "big"), added[0])
}

// currentManifestCID returns the CID of the key's current version manifest —
// the root the space counts it under.
func currentManifestCID(t *testing.T, b *Backend, key string) cid.Cid {
	t.Helper()
	rv, err := b.resolveVersion(context.Background(), "bk", key, "")
	require.NoError(t, err)
	return rv.node.Manifest
}

// failingRegistrar refuses the ops named in fail, so a test can watch a queued
// row survive a refusal and land on a later sweep.
type failingRegistrar struct {
	recordingRegistrar
	mu   sync.Mutex
	fail map[string]bool
}

func (r *failingRegistrar) refuse(op string, yes bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail == nil {
		r.fail = map[string]bool{}
	}
	r.fail[op] = yes
}

func (r *failingRegistrar) refuses(op string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fail[op]
}

func (r *failingRegistrar) RegisterUploads(ctx context.Context, changes []uploader.QueuedUpload) ([]error, error) {
	if r.refuses("add") {
		return refusals(len(changes)), nil
	}
	return r.recordingRegistrar.RegisterUploads(ctx, changes)
}

func (r *failingRegistrar) RetractUploads(ctx context.Context, changes []uploader.QueuedUpload) ([]error, error) {
	if r.refuses("remove") {
		return refusals(len(changes)), nil
	}
	return r.recordingRegistrar.RetractUploads(ctx, changes)
}

// refusals reports every change in a batch as refused, the way the upload
// service answers a batch it accepted but could not act on.
func refusals(n int) []error {
	out := make([]error, n)
	for i := range out {
		out[i] = errors.New("upload service unavailable")
	}
	return out
}

// TestWriteDoesNotCallTheUploadService: the count is reporting data, so a PUT
// commits without waiting on Sprue. The change is queued and applied later.
func TestWriteDoesNotCallTheUploadService(t *testing.T) {
	b, reg := newCountingBackend(t)

	putObjV(t, b, "a", []byte("hello"))

	added, retracted := reg.snapshot()
	require.Empty(t, added, "the write path must not call the upload service")
	require.Empty(t, retracted)

	drainRegistrations(t, b)
	added, _ = reg.snapshot()
	require.Len(t, added, 1, "the sweep applies what the write queued")
}

// TestRefusedRegistrationIsRetried: a Sprue outage delays the count instead of
// losing it — the row stays queued and lands once the service recovers.
func TestRefusedRegistrationIsRetried(t *testing.T) {
	b, _ := newCountingBackend(t)
	reg := &failingRegistrar{}
	b.registrar = reg
	reg.refuse("add", true)

	putObjV(t, b, "a", []byte("hello"))

	n, err := b.SweepUploadRegistrations(context.Background())
	require.NoError(t, err)
	require.Zero(t, n, "a refused registration lands nothing")
	added, _ := reg.snapshot()
	require.Empty(t, added)

	// The row is held back with a backoff, so bring it forward the way the
	// passage of time would.
	queued, err := b.uploadRegs.ListUploadRegistrationsBySpace(context.Background(), bucketSpaceOf(t, b))
	require.NoError(t, err)
	require.Len(t, queued, 1, "the refused row is still queued")
	require.NoError(t, b.uploadRegs.RescheduleUploadRegistrations(context.Background(), []int64{queued[0].Seq}, time.Now().Add(-time.Second)))

	reg.refuse("add", false)
	drainRegistrations(t, b)
	added, _ = reg.snapshot()
	require.Len(t, added, 1, "the registration lands once the service recovers")
}

// TestRefusedAddHoldsBackItsKeysRemove: an add that has not landed must not be
// overtaken by the remove that retires it, or the root stays counted forever.
func TestRefusedAddHoldsBackItsKeysRemove(t *testing.T) {
	b, _ := newCountingBackend(t)
	reg := &failingRegistrar{}
	b.registrar = reg
	reg.refuse("add", true)

	// Two versions of one key in an unversioned bucket: the second retires the
	// first, so the queue holds add(v1), add(v2), remove(v1).
	putObjV(t, b, "a", []byte("first"))
	putObjV(t, b, "a", []byte("second"))

	n, err := b.SweepUploadRegistrations(context.Background())
	require.NoError(t, err)
	require.Zero(t, n)

	_, retracted := reg.snapshot()
	require.Empty(t, retracted,
		"the retraction must wait behind the add it retires, not overtake it")
}

// bucketSpaceOf returns the space backing the test bucket.
func bucketSpaceOf(t *testing.T, b *Backend) did.DID {
	t.Helper()
	st, err := b.reg.Get(context.Background(), "bk")
	require.NoError(t, err)
	return st.Space
}

// TestSweepBatchesChangesByOp: many queued changes cost one round trip per op,
// not one per change — additions in one call, retractions in another.
func TestSweepBatchesChangesByOp(t *testing.T) {
	b, reg := newCountingBackend(t)

	// Six writes over three keys: each key's second write supersedes its
	// first, so the queue holds six adds and three removes.
	for _, key := range []string{"a", "b", "c"} {
		putObjV(t, b, key, []byte("first"))
		putObjV(t, b, key, []byte("second"))
	}

	n, err := b.SweepUploadRegistrations(context.Background())
	require.NoError(t, err)
	require.Equal(t, 9, n, "six additions and three retractions")

	require.Equal(t, []int{6, 3}, reg.roundTrips(),
		"one call for the additions, then one for the retractions")
}

// TestSweepSendsEveryAdditionBeforeAnyRetraction: a container sorts its tokens
// bytewise, so the service may run a batch in any order. Additions and
// retractions therefore travel in separate calls, additions first, or a
// retraction could overtake the addition it retires.
func TestSweepSendsEveryAdditionBeforeAnyRetraction(t *testing.T) {
	b, reg := newCountingBackend(t)

	putObjV(t, b, "a", []byte("first"))
	v1 := currentManifestCID(t, b, "a")
	putObjV(t, b, "a", []byte("second"))
	v2 := currentManifestCID(t, b, "a")

	_, err := b.SweepUploadRegistrations(context.Background())
	require.NoError(t, err)

	added, retracted := reg.snapshot()
	require.Equal(t, []cid.Cid{v1, v2}, added)
	require.Equal(t, []cid.Cid{v1}, retracted)
	require.Equal(t, []int{2, 1}, reg.roundTrips(),
		"the retraction of v1 must not share a call with the addition of v1")
}

// TestBackedOffAdditionHoldsLaterRowsForItsKey: a change enqueued after a
// refused one must not overtake it just because it is due and the refused one
// is waiting out a backoff. Replaying the key out of order would send the
// retraction of a version whose addition has not landed — a no-op that leaves
// the root counted once the addition finally does.
func TestBackedOffAdditionHoldsLaterRowsForItsKey(t *testing.T) {
	b, _ := newCountingBackend(t)
	reg := &failingRegistrar{}
	b.registrar = reg
	reg.refuse("add", true)

	// The first version's addition is refused and backed off.
	putObjV(t, b, "a", []byte("first"))
	_, err := b.SweepUploadRegistrations(context.Background())
	require.NoError(t, err)
	added, _ := reg.snapshot()
	require.Empty(t, added)

	// A second write now queues an addition and a retraction that are due
	// immediately, behind an addition that is not.
	reg.refuse("add", false)
	putObjV(t, b, "a", []byte("second"))

	n, err := b.SweepUploadRegistrations(context.Background())
	require.NoError(t, err)
	require.Zero(t, n, "the key is held behind its backed-off addition")

	added, retracted := reg.snapshot()
	require.Empty(t, retracted,
		"a retraction must never be claimed while an earlier change for its key is held")
	require.Empty(t, added)
}

// countingRegStore counts the store calls a sweep makes, so a test can assert
// the batched round trip is not undone by a database call per change.
type countingRegStore struct {
	registry.UploadRegistrationStore
	deletes     int
	reschedules int
	refreshes   int
}

func (c *countingRegStore) RefreshUploadRegistrationProofs(ctx context.Context, seq int64, proofs []byte) error {
	c.refreshes++
	return c.UploadRegistrationStore.RefreshUploadRegistrationProofs(ctx, seq, proofs)
}

func (c *countingRegStore) DeleteUploadRegistrations(ctx context.Context, seqs []int64) error {
	if len(seqs) > 0 {
		c.deletes++
	}
	return c.UploadRegistrationStore.DeleteUploadRegistrations(ctx, seqs)
}

func (c *countingRegStore) RescheduleUploadRegistrations(ctx context.Context, seqs []int64, nextAt time.Time) error {
	if len(seqs) > 0 {
		c.reschedules++
	}
	return c.UploadRegistrationStore.RescheduleUploadRegistrations(ctx, seqs, nextAt)
}

// TestSweepSettlesRowsInBatches: nine landed changes cost one delete per op,
// not one per change.
func TestSweepSettlesRowsInBatches(t *testing.T) {
	b, _ := newCountingBackend(t)
	counting := &countingRegStore{UploadRegistrationStore: b.uploadRegs}
	b.uploadRegs = counting

	for _, key := range []string{"a", "b", "c"} {
		putObjV(t, b, key, []byte("first"))
		putObjV(t, b, key, []byte("second"))
	}

	n, err := b.SweepUploadRegistrations(context.Background())
	require.NoError(t, err)
	require.Equal(t, 9, n)

	require.Equal(t, 2, counting.deletes,
		"one delete for the additions that landed, one for the retractions")
	require.Zero(t, counting.reschedules)
}

// TestSweepRescheduleGroupsByEarnedDelay: refused rows are held back in one
// statement per distinct backoff, not one per row.
func TestSweepRescheduleGroupsByEarnedDelay(t *testing.T) {
	b, _ := newCountingBackend(t)
	reg := &failingRegistrar{}
	b.registrar = reg
	reg.refuse("add", true)
	counting := &countingRegStore{UploadRegistrationStore: b.uploadRegs}
	b.uploadRegs = counting

	// Three keys, all refused on their first attempt, so all have earned the
	// same delay and settle together.
	for _, key := range []string{"a", "b", "c"} {
		putObjV(t, b, key, []byte("first"))
	}

	n, err := b.SweepUploadRegistrations(context.Background())
	require.NoError(t, err)
	require.Zero(t, n)

	require.Equal(t, 1, counting.reschedules,
		"three rows owed the same wait settle in one statement")
	require.Zero(t, counting.deletes)
}

// sweepUntilSettled runs sweeps until nothing more lands, bringing any backoff
// forward each time so the test does not wait out real delays.
func sweepUntilSettled(t *testing.T, b *Backend, reg *countingRegStore) {
	t.Helper()
	for range uploadRegistrationDeadLetterAfter + 2 {
		if _, err := b.SweepUploadRegistrations(context.Background()); err != nil {
			t.Fatalf("sweep: %v", err)
		}
		queued, err := b.uploadRegs.ListUploadRegistrationsBySpace(context.Background(), bucketSpaceOf(t, b))
		require.NoError(t, err)
		var live []int64
		for _, q := range queued {
			if q.DeadLetteredAt == nil {
				live = append(live, q.Seq)
			}
		}
		if len(live) == 0 {
			return
		}
		require.NoError(t, b.uploadRegs.RescheduleUploadRegistrations(
			context.Background(), live, time.Now().Add(-time.Second)))
	}
}

// TestSpentAuthorityIsDeadLetteredNotRetriedForever: hilt's delegation to the
// gateway expires at the next UTC midnight, so a change queued late in the day
// can outlive its own authority. Once it has and the space has none to lend,
// the row is dead-lettered rather than retried for good.
func TestSpentAuthorityIsDeadLetteredNotRetriedForever(t *testing.T) {
	b, _ := newCountingBackend(t)
	reg := &recordingRegistrar{}
	b.registrar = reg
	counting := &countingRegStore{UploadRegistrationStore: b.uploadRegs}
	b.uploadRegs = counting

	putObjV(t, b, "a", []byte("hello"))
	reg.spendAuthority()

	sweepUntilSettled(t, b, counting)

	dead, err := b.uploadRegs.ListDeadLetteredUploadRegistrations(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, dead, 1, "the change is dead-lettered once its authority cannot be renewed")
	require.Contains(t, dead[0].DeadLetterReason, "authority")

	added, _ := reg.snapshot()
	require.Empty(t, added, "nothing was ever sent")
}

// TestDeadLetteredRowDoesNotHoldBackItsKey: the dead letter exists so a change
// that can never be made stops blocking the ones behind it. The count is short
// by that change, not frozen at it.
func TestDeadLetteredRowDoesNotHoldBackItsKey(t *testing.T) {
	b, _ := newCountingBackend(t)
	reg := &recordingRegistrar{}
	b.registrar = reg
	counting := &countingRegStore{UploadRegistrationStore: b.uploadRegs}
	b.uploadRegs = counting

	// The first version's change loses its authority and is dead-lettered.
	putObjV(t, b, "a", []byte("first"))
	reg.spendAuthority()
	sweepUntilSettled(t, b, counting)
	dead, err := b.uploadRegs.ListDeadLetteredUploadRegistrations(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, dead, 1)

	// A later write to the same key must still be counted: the dead-lettered
	// row is out of the way rather than in front of it.
	reg.mu.Lock()
	reg.spent = false
	reg.mu.Unlock()
	putObjV(t, b, "a", []byte("second"))
	drainRegistrations(t, b)

	added, _ := reg.snapshot()
	require.Len(t, added, 1, "the later version is registered despite the dead-lettered one")
	require.Equal(t, currentManifestCID(t, b, "a"), added[0])
}

// renewingRegistrar starts with spent authority and recovers once renew is
// called, standing in for a space that is written again while its queued
// changes are waiting.
type renewingRegistrar struct {
	recordingRegistrar
	renewals int
}

func (r *renewingRegistrar) PrepareAuthority(ctx context.Context, space did.DID, cmd ucan.Command, stored []byte) ([]byte, bool, error) {
	r.mu.Lock()
	spent := r.spent
	if !spent {
		r.renewals++
	}
	r.mu.Unlock()
	if spent {
		return nil, false, errors.New("no proof store for space " + space.String())
	}
	// Renewed: a different chain from the one the row holds.
	fresh, err := container.Encode(container.RawGzip, container.New())
	if err != nil {
		return nil, false, err
	}
	return fresh, true, nil
}

// TestRenewedAuthorityRescuesAQueuedChange: a row whose stored chain has
// expired is sent on renewed authority rather than dead-lettered, and the renewal is
// written back so a restart does not have to repeat it.
func TestRenewedAuthorityRescuesAQueuedChange(t *testing.T) {
	b, _ := newCountingBackend(t)
	reg := &renewingRegistrar{}
	reg.spent = true
	b.registrar = reg
	counting := &countingRegStore{UploadRegistrationStore: b.uploadRegs}
	b.uploadRegs = counting

	putObjV(t, b, "a", []byte("hello"))

	// While the space has no authority to lend, the change waits.
	_, err := b.SweepUploadRegistrations(context.Background())
	require.NoError(t, err)
	added, _ := reg.snapshot()
	require.Empty(t, added)
	dead, err := b.uploadRegs.ListDeadLetteredUploadRegistrations(context.Background(), 10)
	require.NoError(t, err)
	require.Empty(t, dead, "one failure is not enough to dead-letter")

	// A write to the space leaves fresh authority behind; the queued change
	// now goes out on it.
	reg.mu.Lock()
	reg.spent = false
	reg.mu.Unlock()
	queued, err := b.uploadRegs.ListUploadRegistrationsBySpace(context.Background(), bucketSpaceOf(t, b))
	require.NoError(t, err)
	require.NoError(t, b.uploadRegs.RescheduleUploadRegistrations(
		context.Background(), []int64{queued[0].Seq}, time.Now().Add(-time.Second)))

	drainRegistrations(t, b)
	added, _ = reg.snapshot()
	require.Len(t, added, 1, "the change lands on renewed authority")
	require.Positive(t, reg.renewals)
	require.Positive(t, counting.refreshes, "the renewed chain is written back to the row")
}
