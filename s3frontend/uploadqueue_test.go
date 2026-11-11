package s3frontend

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/internal/authority"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
	"github.com/fil-forge/ingot/uploader"
)

// queueFixture runs the queue-space-upload handlers over a fresh schema, with
// a backend holding only what they need.
type queueFixture struct {
	b     *Backend
	r     *taskrunner.Runner
	space did.DID
}

// newQueueFixture starts the upload queue with the given registrar and number
// of workers. Zero workers leaves the queued changes to a bucket delete's
// drain.
func newQueueFixture(t *testing.T, registrar uploader.UploadRegistrar, workers int, tweak ...func(*Backend)) *queueFixture {
	t.Helper()
	f := newStoppedQueueFixture(t, registrar, workers, tweak...)
	f.start(t)
	return f
}

// newStoppedQueueFixture is newQueueFixture with its runner not yet started,
// so a test can queue changes before any worker takes one (start).
func newStoppedQueueFixture(t *testing.T, registrar uploader.UploadRegistrar, workers int, tweak ...func(*Backend)) *queueFixture {
	t.Helper()
	env := taskrunnertest.NewEnv(t)
	r, err := taskrunner.New(env.DB, taskrunner.Options{
		Schema:       env.Schema,
		Queues:       []taskrunner.QueueOptions{{Name: registry.UploadQueue, Workers: workers}},
		LeaseTTL:     time.Second,
		PollInterval: 20 * time.Millisecond,
		Logger:       zaptest.NewLogger(t),
	})
	require.NoError(t, err)
	b := &Backend{
		registrar:    registrar,
		proofs:       authority.New(registrar),
		logger:       zaptest.NewLogger(t),
		uploadRetry:  taskrunner.RetryPolicy{First: 5 * time.Millisecond, Max: 20 * time.Millisecond, Multiplier: 2, HoldsSubject: true},
		drainTimeout: 500 * time.Millisecond,
	}
	for _, f := range tweak {
		f(b)
	}
	require.NoError(t, b.registerUploadTasks(r))
	require.NoError(t, r.Migrate(context.Background()))
	return &queueFixture{b: b, r: r, space: testutil.RandomDID(t)}
}

// start starts the fixture's runner.
func (f *queueFixture) start(t *testing.T) {
	t.Helper()
	require.NoError(t, f.r.Start(context.Background()))
	t.Cleanup(func() { _ = f.r.Stop(context.Background()) })
}

// queue submits a change for root on the fixture's space, as a commit would.
func (f *queueFixture) queue(t *testing.T, op registry.UploadRegistrationOp, root cid.Cid) taskrunner.TaskID {
	t.Helper()
	spec, err := registry.UploadRegistration{Bucket: "bk", ObjectKey: root.String(), Space: f.space, Root: root, Op: op, Proofs: []byte("proofs")}.Spec()
	require.NoError(t, err)
	id, err := f.r.Submit(context.Background(), spec)
	require.NoError(t, err)
	return id
}

func liveRoot(t *testing.T, s string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mh)
}

func TestQueuedRegistrationLands(t *testing.T) {
	reg := &recordingRegistrar{}
	f := newQueueFixture(t, reg, 1)
	root := liveRoot(t, "a")
	id := f.queue(t, registry.UploadRegistrationAdd, root)
	_, err := f.r.Wait(context.Background(), registry.UploadQueue, id)
	require.NoError(t, err)
	added, _ := reg.snapshot()
	require.Equal(t, []cid.Cid{root}, added)
}

// A refused change is retried until the upload service takes it, and holds
// back the changes behind it, so a retraction never overtakes its addition.
func TestRefusedAddIsRetriedAndHoldsBackTheRemove(t *testing.T) {
	reg := &failingRegistrar{}
	reg.refuse("add", true)
	f := newQueueFixture(t, reg, 1)
	root := liveRoot(t, "a")
	f.queue(t, registry.UploadRegistrationAdd, root)
	remove := f.queue(t, registry.UploadRegistrationRemove, root)

	time.Sleep(200 * time.Millisecond)
	_, retracted := reg.snapshot()
	require.Empty(t, retracted, "the retraction must wait behind the refused addition")

	reg.refuse("add", false)
	_, err := f.r.Wait(context.Background(), registry.UploadQueue, remove)
	require.NoError(t, err)
	added, retracted := reg.snapshot()
	require.Equal(t, []cid.Cid{root}, added, "the addition lands once the service recovers")
	require.Equal(t, []cid.Cid{root}, retracted, "and the retraction after it")
}

// Consecutive changes of one op go out in one round trip, and the space's
// order holds between them.
func TestChangesBatchByOpInCommitOrder(t *testing.T) {
	reg := &recordingRegistrar{}
	f := newStoppedQueueFixture(t, reg, 1)
	a, b, c := liveRoot(t, "a"), liveRoot(t, "b"), liveRoot(t, "c")
	f.queue(t, registry.UploadRegistrationAdd, a)
	f.queue(t, registry.UploadRegistrationAdd, b)
	f.queue(t, registry.UploadRegistrationRemove, a)
	f.queue(t, registry.UploadRegistrationRemove, b)
	f.queue(t, registry.UploadRegistrationAdd, c)
	f.start(t)

	require.NoError(t, f.b.DrainSpaceRegistrations(context.Background(), f.space))
	require.Equal(t, []int{2, 2, 1}, reg.roundTrips())
	added, retracted := reg.snapshot()
	require.Equal(t, []cid.Cid{a, b, c}, added)
	require.Equal(t, []cid.Cid{a, b}, retracted)
}

// A change whose authority is spent, with nothing in the space to renew it
// from, fails for good after its attempts, and stops holding back the changes
// behind it.
func TestSpentAuthorityFailsAfterItsAttempts(t *testing.T) {
	reg := &recordingRegistrar{}
	reg.spendAuthority()
	f := newQueueFixture(t, reg, 1)
	stuck := f.queue(t, registry.UploadRegistrationAdd, liveRoot(t, "a"))

	_, err := f.r.Wait(context.Background(), registry.UploadQueue, stuck)
	var failed *taskrunner.HandlerError
	require.ErrorAs(t, err, &failed)
	require.Contains(t, failed.Message, "authority expired")
	list, err := f.r.ListFailed(context.Background(), registry.UploadQueue, f.space.String(), taskrunner.TaskID{}, 10)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, uploadRegistrationDeadLetterAfter-1, list[0].Task.Attempt, "failed on its last attempt")

	reg.mu.Lock()
	reg.spent = false
	reg.mu.Unlock()
	next := liveRoot(t, "b")
	id := f.queue(t, registry.UploadRegistrationAdd, next)
	_, err = f.r.Wait(context.Background(), registry.UploadQueue, id)
	require.NoError(t, err)
	added, _ := reg.snapshot()
	require.Equal(t, []cid.Cid{next}, added, "the failed change no longer holds the space")
}

// A change whose stored chain has expired goes out on authority renewed from
// the space, once the space has some to lend.
func TestRenewedAuthorityRescuesAQueuedChange(t *testing.T) {
	reg := &renewingRegistrar{}
	reg.spent = true
	// Slow enough that the change is still retrying when the space recovers.
	f := newQueueFixture(t, reg, 1, func(b *Backend) {
		b.uploadRetry = taskrunner.RetryPolicy{First: 50 * time.Millisecond, Max: 50 * time.Millisecond, HoldsSubject: true}
	})
	root := liveRoot(t, "a")
	id := f.queue(t, registry.UploadRegistrationAdd, root)

	time.Sleep(120 * time.Millisecond)
	added, _ := reg.snapshot()
	require.Empty(t, added, "nothing goes out while the space has no authority to lend")
	reg.mu.Lock()
	reg.spent = false
	reg.mu.Unlock()

	_, err := f.r.Wait(context.Background(), registry.UploadQueue, id)
	require.NoError(t, err)
	added, _ = reg.snapshot()
	require.Equal(t, []cid.Cid{root}, added)
	require.Positive(t, reg.renewals)
}

// A bucket delete sends its space's queued changes before the space goes.
func TestDrainSendsTheSpacesQueuedChanges(t *testing.T) {
	reg := &recordingRegistrar{}
	f := newQueueFixture(t, reg, 1)
	root := liveRoot(t, "a")
	f.queue(t, registry.UploadRegistrationRemove, root)

	require.NoError(t, f.b.DrainSpaceRegistrations(context.Background(), f.space))
	_, retracted := reg.snapshot()
	require.Equal(t, []cid.Cid{root}, retracted)
}

// A delete whose space's changes the upload service will not take fails, for
// the client to retry, rather than leaving the objects counted.
func TestDrainFailsWhenTheServiceRefuses(t *testing.T) {
	reg := &failingRegistrar{}
	reg.refuse("remove", true)
	f := newQueueFixture(t, reg, 1)
	f.queue(t, registry.UploadRegistrationRemove, liveRoot(t, "a"))

	err := f.b.DrainSpaceRegistrations(context.Background(), f.space)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "retry"), err.Error())
}

// A retraction that failed for good is resent with the delete request's own
// authority, since nothing will ever send it after the bucket is gone.
func TestDrainResendsAFailedRetraction(t *testing.T) {
	reg := &recordingRegistrar{}
	reg.spendAuthority()
	f := newQueueFixture(t, reg, 1)
	root := liveRoot(t, "a")
	stuck := f.queue(t, registry.UploadRegistrationRemove, root)
	_, err := f.r.Wait(context.Background(), registry.UploadQueue, stuck)
	require.Error(t, err)

	// The delete request carries /upload/remove in its own right.
	reg.mu.Lock()
	reg.spent = false
	reg.mu.Unlock()
	require.NoError(t, f.b.DrainSpaceRegistrations(context.Background(), f.space))
	_, retracted := reg.snapshot()
	require.Equal(t, []cid.Cid{root}, retracted)
	left, err := f.r.ListFailed(context.Background(), registry.UploadQueue, f.space.String(), taskrunner.TaskID{}, 10)
	require.NoError(t, err)
	require.Empty(t, left)
}

// A delete's drain finds every failed retraction, however many failed
// additions are listed ahead of it.
func TestDrainResendsARetractionBehindFailedAdditions(t *testing.T) {
	reg := &recordingRegistrar{}
	reg.spendAuthority()
	f := newQueueFixture(t, reg, 1, func(b *Backend) { b.failedPage = 2 })
	var stuck []taskrunner.TaskID
	for _, s := range []string{"a", "b", "c"} {
		stuck = append(stuck, f.queue(t, registry.UploadRegistrationAdd, liveRoot(t, s)))
	}
	root := liveRoot(t, "removed")
	stuck = append(stuck, f.queue(t, registry.UploadRegistrationRemove, root))
	for _, id := range stuck {
		_, err := f.r.Wait(context.Background(), registry.UploadQueue, id)
		require.Error(t, err)
	}

	reg.mu.Lock()
	reg.spent = false
	reg.mu.Unlock()
	require.NoError(t, f.b.DrainSpaceRegistrations(context.Background(), f.space))
	_, retracted := reg.snapshot()
	require.Equal(t, []cid.Cid{root}, retracted, "the retraction on the second page was resent")
}

// proofCheckingRegistrar renews the chain a change was queued with, and
// fails its first send, refused or unreachable.
type proofCheckingRegistrar struct {
	recordingRegistrar
	unreachable bool
	renewals    int
	sent        []string
}

func (r *proofCheckingRegistrar) PrepareAuthority(_ context.Context, _ did.DID, _ ucan.Command, stored []byte) ([]byte, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if string(stored) == "renewed" {
		return stored, false, nil
	}
	r.renewals++
	return []byte("renewed"), true, nil
}

func (r *proofCheckingRegistrar) RegisterUploads(_ context.Context, changes []uploader.QueuedUpload) ([]error, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range changes {
		r.sent = append(r.sent, string(c.Proofs))
	}
	if len(r.sent) > 1 {
		return make([]error, len(changes)), nil
	}
	if r.unreachable {
		return nil, errors.New("connection refused")
	}
	return refusals(len(changes)), nil
}

// A change whose authority was renewed for a send that failed keeps the
// renewal for its retry, rather than the spent chain it was queued with.
func TestRenewedAuthorityIsKeptForTheRetry(t *testing.T) {
	for _, unreachable := range []bool{false, true} {
		t.Run(map[bool]string{false: "refused", true: "unreachable"}[unreachable], func(t *testing.T) {
			reg := &proofCheckingRegistrar{unreachable: unreachable}
			f := newQueueFixture(t, reg, 1)
			id := f.queue(t, registry.UploadRegistrationAdd, liveRoot(t, "a"))
			_, err := f.r.Wait(context.Background(), registry.UploadQueue, id)
			require.NoError(t, err)
			reg.mu.Lock()
			defer reg.mu.Unlock()
			require.Equal(t, []string{"renewed", "renewed"}, reg.sent)
			require.Equal(t, 1, reg.renewals, "the retry found the renewal in its arguments")
		})
	}
}

// A resent retraction the upload service refuses fails the delete, for the
// client to retry, rather than letting the bucket go with the root counted.
func TestDrainFailsWhenAResentRetractionIsRefused(t *testing.T) {
	reg := &failingRegistrar{}
	reg.spendAuthority()
	f := newQueueFixture(t, reg, 1)
	root := liveRoot(t, "a")
	stuck := f.queue(t, registry.UploadRegistrationRemove, root)
	_, err := f.r.Wait(context.Background(), registry.UploadQueue, stuck)
	require.Error(t, err)

	reg.recordingRegistrar.mu.Lock()
	reg.spent = false
	reg.recordingRegistrar.mu.Unlock()
	reg.refuse("remove", true)
	err = f.b.DrainSpaceRegistrations(context.Background(), f.space)
	require.Error(t, err)
	require.Contains(t, err.Error(), "retry")

	reg.refuse("remove", false)
	require.NoError(t, f.b.DrainSpaceRegistrations(context.Background(), f.space))
	_, retracted := reg.snapshot()
	require.Equal(t, []cid.Cid{root}, retracted)
}
