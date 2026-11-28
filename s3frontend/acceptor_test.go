package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/queue/multipartqueue"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
	"github.com/fil-forge/libforge/taskrunner"
	"github.com/fil-forge/libforge/taskrunner/taskrunnertest"
)

// cancelQueuedAccepts cancels the accept UploadPart queued for uploadID, so
// that a test runs the upload's accepts itself, one at a time.
func cancelQueuedAccepts(t *testing.T, b *Backend, uploadID string) {
	t.Helper()
	ctx := context.Background()
	tasks, err := storeFor(b).queued(ctx, multipartqueue.Name)
	require.NoError(t, err, "queued accepts")
	var ids []taskrunner.TaskID
	for _, task := range tasks {
		if task.Subject == uploadID && task.Action == multipartqueue.AcceptAction && task.State == "pending" {
			ids = append(ids, task.ID)
		}
	}
	require.NoError(t, storeFor(b).tasks.Cancel(ctx, multipartqueue.Name, ids...), "cancel queued accepts")
}

// runAccepts runs one accept of uploadID's parked parts, as the queue's
// worker would, in place of the one UploadPart queued, and waits for the
// blobs' accepts it queues, as Complete does. A blob's accept that fails
// leaves its part parked.
func runAccepts(t *testing.T, b *Backend, uploadID string) error {
	t.Helper()
	cancelQueuedAccepts(t, b, uploadID)
	ctx := context.Background()
	if _, err := storeFor(b).tasks.Run(ctx, taskrunner.Spec{
		Queue:   multipartqueue.Name,
		Subject: uploadID,
		Action:  multipartqueue.AcceptAction,
	}); err != nil {
		return err
	}
	sess, err := b.multipart.GetSession(ctx, uploadID)
	if errors.Is(err, registry.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	parts, err := b.multipart.ListParts(ctx, uploadID)
	if err != nil {
		return err
	}
	due, err := b.blobs.ExpediteAccepts(ctx, sess.Space)
	if err != nil {
		return err
	}
	expires := time.Now().Add(10 * time.Second)
	for _, p := range parts {
		for _, d := range p.BlobDigests {
			subject := spaceshardqueue.Subject(sess.Space, d)
			if len(due[subject]) == 0 {
				continue
			}
			if err := b.blobs.Flush(ctx, subject, expires); err != nil {
				return err
			}
		}
	}
	return nil
}

// countingConcluder is a parking uploader that records what each conclude
// was asked for, and fails the next `fails` concludes outright.
type countingConcluder struct {
	parkingUploader
	mu    sync.Mutex
	calls [][]string
	fails int
}

func (c *countingConcluder) ConcludeBlobs(ctx context.Context, space did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	c.mu.Lock()
	digests := make([]string, len(parked))
	for i, p := range parked {
		digests[i] = string(p.Digest)
	}
	c.calls = append(c.calls, digests)
	fail := c.fails > 0
	if fail {
		c.fails--
	}
	c.mu.Unlock()
	if fail {
		return make([]*uploader.BlobLocation, len(parked)), errors.New("upload service unreachable")
	}
	return c.parkingUploader.ConcludeBlobs(ctx, space, parked)
}

// concluded returns the size of each conclude call so far.
func (c *countingConcluder) concluded() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int, len(c.calls))
	for i, call := range c.calls {
		out[i] = len(call)
	}
	return out
}

// uploadDistinctParts uploads n parts of distinct content, one blob each.
func uploadDistinctParts(t *testing.T, b *Backend, key, uploadID string, n int) []types.CompletedPart {
	t.Helper()
	var parts []types.CompletedPart
	for i := int32(1); i <= int32(n); i++ {
		body := testBody(int(backend.MinPartSize))
		body[0] = byte(i)
		out, err := mpUploadPart(t, b, key, uploadID, i, body, nil)
		require.NoError(t, err, "UploadPart %d", i)
		num := i
		parts = append(parts, types.CompletedPart{PartNumber: &num, ETag: out.ETag})
	}
	return parts
}

// partStates returns each recorded part's state by part number.
func partStates(t *testing.T, mem *testStore, uploadID string) map[int]string {
	t.Helper()
	parts, err := mem.ListParts(context.Background(), uploadID)
	require.NoError(t, err, "ListParts")
	out := map[int]string{}
	for _, p := range parts {
		out[p.PartNumber] = p.State
	}
	return out
}

// allIn reports whether every part is in state.
func allIn(states map[int]string, state string) bool {
	for _, s := range states {
		if s != state {
			return false
		}
	}
	return len(states) > 0
}

// TestAcceptAcceptsPartsWhileUploading: each UploadPart queues the upload's
// accept, which accepts the parts while the upload is still open. Complete
// then finds every blob located and concludes nothing.
func TestAcceptAcceptsPartsWhileUploading(t *testing.T) {
	cc := &countingConcluder{}
	b, mem := newDeferredBackend(t, cc)
	key := "accept-during-upload"

	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 3)
	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	states := partStates(t, mem, uploadID)
	require.True(t, allIn(states, registry.PartAccepted), "part states = %v, want all accepted", states)
	for n := 1; n <= 3; n++ {
		for _, d := range hygienePartDigests(t, mem, uploadID, n) {
			assertAcceptedAndUnparked(t, mem, d)
		}
	}
	concludedBefore := cc.concluded()

	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "Complete")
	got := cc.concluded()
	require.Len(t, got, len(concludedBefore), "conclude calls after Complete = %v, want none beyond the accept's %v", got, concludedBefore)
}

// TestUploadedPartsAreAcceptedInTheBackground: with no delay, the accept an
// UploadPart queues runs on the upload queue's workers, and its blobs'
// accepts on the space-shard queue's, so the parts are accepted with nothing
// else run.
func TestUploadedPartsAreAcceptedInTheBackground(t *testing.T) {
	b, mem := newDeferredBackend(t, &countingConcluder{})
	storeFor(b).limits.AcceptDelay = 0
	key := "accept-in-background"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 2)

	deadline := time.Now().Add(10 * time.Second)
	for {
		states := partStates(t, mem, uploadID)
		if allIn(states, registry.PartAccepted) {
			break
		}
		require.False(t, time.Now().After(deadline), "part states = %v, want all accepted by the background accept", states)
		time.Sleep(20 * time.Millisecond)
	}
}

// TestUploadPartQueuesOneAccept: a part is recorded parked and the upload's
// accept is queued with it, durably; the parts of one upload share one
// pending accept.
func TestUploadPartQueuesOneAccept(t *testing.T) {
	b, mem := newDeferredBackend(t, &countingConcluder{})
	key := "accept-queued"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 2)

	states := partStates(t, mem, uploadID)
	require.True(t, allIn(states, registry.PartParked), "part states = %v, want all parked", states)
	tasks, err := mem.queued(context.Background(), multipartqueue.Name)
	require.NoError(t, err, "queued")
	pending := 0
	for _, task := range tasks {
		if task.Subject == uploadID && task.Action == multipartqueue.AcceptAction && task.State == "pending" {
			pending++
		}
	}
	require.Equal(t, 1, pending, "pending accepts for the upload")
}

// TestAcceptConcludesEachBlobOnItsOwn: the upload's accept queues an accept
// for each blob, under the blob's subject, which concludes that blob alone.
func TestAcceptConcludesEachBlobOnItsOwn(t *testing.T) {
	cc := &countingConcluder{}
	b, mem := newDeferredBackend(t, cc)
	key := "accept-per-blob"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 3)

	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	got := cc.concluded()
	require.True(t, slices.Equal(got, []int{1, 1, 1}), "conclude calls = %v, want one per blob", got)
	states := partStates(t, mem, uploadID)
	require.True(t, allIn(states, registry.PartAccepted), "part states = %v, want all accepted", states)
}

// TestAcceptLeavesASessionNoLongerOpen: a completed session's parts are
// its object's, and its accept concludes nothing.
func TestAcceptLeavesASessionNoLongerOpen(t *testing.T) {
	cc := &countingConcluder{}
	b, mem := newDeferredBackend(t, cc)
	uploadID := mpCreate(t, b, "completed", "", "")
	uploadDistinctParts(t, b, "completed", uploadID, 1)
	mem.setSessionState(t, uploadID, registry.SessionCompleted)

	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	states := partStates(t, mem, uploadID)
	require.True(t, allIn(states, registry.PartParked), "part states = %v, want parked", states)
	got := cc.concluded()
	require.Empty(t, got, "conclude calls")
}

// selectiveConcluder concludes like the parking uploader, except that a
// conclude carrying a blocked digest waits for its context, and a failing
// digest gets no location and an error while the others in the conclude are
// accepted.
type selectiveConcluder struct {
	parkingUploader
	mu      sync.Mutex
	blocked map[string]bool
	failing map[string]bool
}

func (s *selectiveConcluder) ConcludeBlobs(ctx context.Context, space did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	s.mu.Lock()
	var block bool
	fail := map[int]bool{}
	for i, p := range parked {
		block = block || s.blocked[string(p.Digest)]
		fail[i] = s.failing[string(p.Digest)]
	}
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return make([]*uploader.BlobLocation, len(parked)), ctx.Err()
	}
	locations, err := s.parkingUploader.ConcludeBlobs(ctx, space, parked)
	if err != nil {
		return locations, err
	}
	var errs []error
	for i := range parked {
		if fail[i] {
			locations[i] = nil
			errs = append(errs, errors.New("accept refused"))
		}
	}
	return locations, errors.Join(errs...)
}

// TestAFailedBlobAcceptLeavesItsPart: the upload service refuses part 2's
// blob, so that blob's accept fails and records nothing, while part 1's blob
// is accepted. The retry accepts part 2.
func TestAFailedBlobAcceptLeavesItsPart(t *testing.T) {
	sc := &selectiveConcluder{}
	b, mem := newDeferredBackend(t, sc)
	ctx := context.Background()
	key := "accept-fails"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 2)
	second := hygienePartDigests(t, mem, uploadID, 2)[0]
	sc.mu.Lock()
	sc.failing = map[string]bool{string(second): true}
	sc.mu.Unlock()

	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	got := partStates(t, mem, uploadID)
	require.Equal(t, registry.PartAccepted, got[1], "part states = %v, want part 1 accepted and part 2 parked", got)
	require.Equal(t, registry.PartParked, got[2], "part states = %v, want part 1 accepted and part 2 parked", got)
	_, err := mem.GetLocation(ctx, mem.bk(), second)
	require.ErrorIs(t, err, registry.ErrNotFound, "the refused blob is located (err=%v)", err)

	sc.mu.Lock()
	sc.failing = nil
	sc.mu.Unlock()
	require.NoError(t, runAccepts(t, b, uploadID), "retried accept")
	states := partStates(t, mem, uploadID)
	require.True(t, allIn(states, registry.PartAccepted), "part states after the retry = %v, want accepted", states)
}

// TestAcceptLeavesAPartWhoseBlobIsGone: a parked part whose blob has neither
// a park nor a location stays parked while its neighbour is accepted, and
// Complete refuses it with InvalidPart, so the client uploads it again.
func TestAcceptLeavesAPartWhoseBlobIsGone(t *testing.T) {
	b, mem := newDeferredBackend(t, &countingConcluder{})
	ctx := context.Background()
	key := "accept-no-park"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 2)
	require.NoError(t, mem.DeletePark(ctx, mem.bk(), hygienePartDigests(t, mem, uploadID, 2)[0]), "DeletePark")

	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	got := partStates(t, mem, uploadID)
	require.Equal(t, registry.PartAccepted, got[1], "part states = %v, want part 1 accepted and part 2 parked", got)
	require.Equal(t, registry.PartParked, got[2], "part states = %v, want part 1 accepted and part 2 parked", got)
	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	var apiErr s3err.APIError
	require.ErrorAs(t, err, &apiErr, "Complete: err = %v, want InvalidPart", err)
	require.Equal(t, "InvalidPart", apiErr.Code, "Complete: err")
}

// gatedConcluder holds every conclude until open is closed, and signals
// entered as each one starts.
type gatedConcluder struct {
	parkingUploader
	open    chan struct{}
	entered chan struct{}
	mu      sync.Mutex
	calls   int
}

func newGatedConcluder() *gatedConcluder {
	return &gatedConcluder{open: make(chan struct{}), entered: make(chan struct{}, 16)}
}

func (g *gatedConcluder) ConcludeBlobs(ctx context.Context, space did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	g.mu.Lock()
	g.calls++
	g.mu.Unlock()
	g.entered <- struct{}{}
	<-g.open
	return g.parkingUploader.ConcludeBlobs(ctx, space, parked)
}

func (g *gatedConcluder) concludes() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

// acceptInBackground runs uploadID's accept as a worker would, returning its
// outcome on the channel.
func acceptInBackground(t *testing.T, b *Backend, uploadID string) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- runAccepts(t, b, uploadID) }()
	return done
}

// TestCompleteWaitsForARunningAccept: Complete arriving while the upload's
// accept is concluding waits for it, and then concludes nothing itself.
func TestCompleteWaitsForARunningAccept(t *testing.T) {
	g := newGatedConcluder()
	b, mem := newDeferredBackend(t, g)
	key := "complete-waits"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 1)
	background := acceptInBackground(t, b, uploadID)
	<-g.entered

	done := make(chan error, 1)
	go func() {
		_, err := mpComplete(t, b, key, uploadID, parts, nil)
		done <- err
	}()
	select {
	case err := <-done:
		require.FailNow(t, "Complete returned before the accept finished", "err: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(g.open)
	require.NoError(t, <-background, "background accept")
	require.NoError(t, <-done, "Complete")
	n := g.concludes()
	require.Equal(t, 1, n, "concludes = %d, want the background accept's one", n)
	states := partStates(t, mem, uploadID)
	require.True(t, allIn(states, registry.PartAccepted), "part states = %v, want all accepted", states)
}

// TestCompleteOverTheWaitBudgetLeavesTheUploadOpen: an accept still running
// when the wait budget runs out makes Complete answer OperationAborted and
// leaves the upload open, so a client that retries on its own commits once
// the accept is done (botocore clients do not retry it).
func TestCompleteOverTheWaitBudgetLeavesTheUploadOpen(t *testing.T) {
	g := newGatedConcluder()
	b, mem := newDeferredBackend(t, g)
	limits := storeFor(b).limits
	saved := limits.AcceptWait
	limits.AcceptWait = 20 * time.Millisecond
	ctx := context.Background()
	key := "complete-over-budget"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 1)
	background := acceptInBackground(t, b, uploadID)
	<-g.entered

	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	var apiErr s3err.APIError
	require.ErrorAs(t, err, &apiErr, "Complete over the wait budget")
	require.Equal(t, "OperationAborted", apiErr.Code, "Complete over the wait budget")
	sess, err := mem.GetSession(ctx, uploadID)
	require.NoError(t, err, "session after the refused Complete")
	require.Equal(t, registry.SessionOpen, sess.State, "session after the refused Complete")

	close(g.open)
	require.NoError(t, <-background, "background accept")
	// The retry's accept starts once the background one's lease is free,
	// which a loaded machine can take longer than the shrunk budget to see.
	limits.AcceptWait = saved
	_, err = mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "retry Complete")
}

// TestCompleteWaitsThroughAFailedConclude: a part whose blob's accept fails
// and waits to retry holds Complete for the rest of its wait budget, which
// then answers OperationAborted and leaves the upload open. The client's
// retry brings the accept's retry forward, and commits.
func TestCompleteWaitsThroughAFailedConclude(t *testing.T) {
	cc := &countingConcluder{fails: 1}
	b, mem := newDeferredBackend(t, cc)
	limits := storeFor(b).limits
	saved := limits.AcceptWait
	limits.AcceptWait = 500 * time.Millisecond
	ctx := context.Background()
	key := "complete-conclude-failing"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 1)
	cancelQueuedAccepts(t, b, uploadID)

	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	var apiErr s3err.APIError
	require.ErrorAs(t, err, &apiErr, "Complete against a failing conclude: err = %v, want OperationAborted", err)
	require.Equal(t, "OperationAborted", apiErr.Code, "Complete against a failing conclude: err")
	cc.mu.Lock()
	concludes := len(cc.calls)
	cc.mu.Unlock()
	require.Equal(t, 1, concludes, "concludes while Complete waited = %d, want the one that failed", concludes)
	sess, err := mem.GetSession(ctx, uploadID)
	require.NoError(t, err, "session after the refused Complete")
	require.Equal(t, registry.SessionOpen, sess.State, "session after the refused Complete")

	limits.AcceptWait = saved
	_, err = mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "retry Complete")
}

// TestCompleteBringsARetryingAcceptForward: a blob's accept has failed and
// waits to retry. Complete queues no second accept beside it: it brings the
// waiting one forward, which succeeds, and commits without waiting out the
// backoff.
func TestCompleteBringsARetryingAcceptForward(t *testing.T) {
	cc := &countingConcluder{fails: 1}
	b, mem := newDeferredBackend(t, cc)
	key := "complete-retrying-accept"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 1)
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	// The blob's accept fails once and waits to retry.
	require.NoError(t, runAccepts(t, b, uploadID), "accept")

	start := time.Now()
	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "Complete")
	took := time.Since(start)
	assert.LessOrEqual(t, took, 4*time.Second, "Complete took %v: it waited out the accept's backoff", took)
	blobAccept(t, mem, d) // fails the test unless the blob has exactly one accept
	cc.mu.Lock()
	concludes := len(cc.calls)
	cc.mu.Unlock()
	require.Equal(t, 2, concludes, "concludes = %d, want 2: the one that failed and its retry", concludes)
}

// stallingConcluder never answers a conclude while stalled: it waits for its
// context, as a conclude whose response never comes does.
type stallingConcluder struct {
	parkingUploader
	stalled atomic.Bool
}

func (s *stallingConcluder) ConcludeBlobs(ctx context.Context, space did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	if s.stalled.Load() {
		<-ctx.Done()
		return make([]*uploader.BlobLocation, len(parked)), ctx.Err()
	}
	return s.parkingUploader.ConcludeBlobs(ctx, space, parked)
}

// TestAcceptConcludeTimesOut: a conclude that never answers ends at
// its time limit, failing the blob's accept so its retry can run,
// rather than holding the blob's subject for good.
func TestAcceptConcludeTimesOut(t *testing.T) {
	sc := &stallingConcluder{}
	sc.stalled.Store(true)
	b, mem := newDeferredBackend(t, sc,
		withBlobs(func(d *spaceshardqueue.Deps) { d.ConcludeTimeout = 50 * time.Millisecond }))
	key := "accept-conclude-timeout"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)

	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	states := partStates(t, mem, uploadID)
	require.True(t, allIn(states, registry.PartParked), "part states after a timed-out conclude = %v, want parked", states)
	sc.stalled.Store(false)
	require.NoError(t, runAccepts(t, b, uploadID), "retried accept")
	states = partStates(t, mem, uploadID)
	require.True(t, allIn(states, registry.PartAccepted), "part states after the retry = %v, want accepted", states)
}

// TestCompleteAcceptsOnlyItsParts: Complete names part 1 and omits part 2,
// whose blob the upload service refuses. Complete waits for part 1's blob
// alone, so the dropped part cannot fail it.
func TestCompleteAcceptsOnlyItsParts(t *testing.T) {
	sc := &selectiveConcluder{}
	b, mem := newDeferredBackend(t, sc)
	key := "complete-omits-a-failing-part"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 2)
	sc.mu.Lock()
	sc.failing = map[string]bool{string(hygienePartDigests(t, mem, uploadID, 2)[0]): true}
	sc.mu.Unlock()
	// The accept UploadPart queued fails on part 2, before Complete runs
	// its own.
	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	require.Equal(t, registry.PartParked, partStates(t, mem, uploadID)[2], "part 2")

	_, err := mpComplete(t, b, key, uploadID, parts[:1], nil)
	require.NoError(t, err, "Complete of part 1")
}

// TestAcceptTakesABlobWithAReleasePending: a part's blob has a release
// pending (the same bytes, stored as received, of an object whose delete is
// behind its reader grace). The accept accepts it all the same, and the
// release, once due, finds the part's reference and leaves the blob.
func TestAcceptTakesABlobWithAReleasePending(t *testing.T) {
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, &countingConcluder{}, func(d *Deps) { d.Remover = rm })
	ctx := context.Background()
	key := "accept-releasing"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 1)
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	require.NoError(t, mem.EnqueueRelease(ctx, mem.bk(), d, registry.DueNow()), "EnqueueRelease")

	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	require.Equal(t, registry.PartAccepted, partStates(t, mem, uploadID)[1], "part state")
	drainReleases(t, b)
	assertAcceptedAndUnparked(t, mem, d)
	n := rm.removedDigests()[string(d)]
	require.Zero(t, n, "referenced blob removed %d times", n)
	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "Complete")
}

// providerConcluder concludes as the upload service and provider do: a blob
// whose allocation was aborted is refused, any other is accepted and
// registered in the space, and an abort of an accepted blob is then refused
// as already accepted (parkingUploader.AbortBlob). It is the backend's
// BlobRemover too, removing a registered blob. The before hook runs once
// ahead of the next conclude, the after hook once between its accepts and its
// reply.
type providerConcluder struct {
	parkingUploader
	before, after func()
	// registerLate registers an accepted blob after the after hook, just
	// before the reply, as the upload service does; otherwise it is
	// registered with the accept.
	registerLate bool
	registered   map[string]bool
	removed      int
	// concluded is the /blob/add task of every blob a conclude accepted.
	concluded []cid.Cid
}

func (c *providerConcluder) ConcludeBlobs(_ context.Context, _ did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	c.runOnce(&c.before)
	locations := make([]*uploader.BlobLocation, len(parked))
	var refused []error
	var accepted []string
	c.mu.Lock()
	aborted := map[string]bool{}
	for _, d := range c.aborted {
		aborted[d] = true
	}
	for i, p := range parked {
		if aborted[string(p.Digest)] && !c.acceptedOnProvider[string(p.Digest)] {
			refused = append(refused, fmt.Errorf("blob %s: allocation released", p.Digest))
			continue
		}
		if c.acceptedOnProvider == nil {
			c.acceptedOnProvider = map[string]bool{}
		}
		if c.registered == nil {
			c.registered = map[string]bool{}
		}
		c.acceptedOnProvider[string(p.Digest)] = true
		if !c.registerLate {
			c.registered[string(p.Digest)] = true
		}
		accepted = append(accepted, string(p.Digest))
		c.concluded = append(c.concluded, p.AddTask)
		locations[i] = &uploader.BlobLocation{Provider: "did:key:zNode", URL: "https://node.example/blob", Size: p.Size}
	}
	c.mu.Unlock()
	c.runOnce(&c.after)
	if c.registerLate {
		c.mu.Lock()
		for _, d := range accepted {
			c.registered[d] = true
		}
		c.mu.Unlock()
	}
	return locations, errors.Join(refused...)
}

// RemoveBlob removes the space's registration of digest, if there is one.
func (c *providerConcluder) RemoveBlob(_ context.Context, _ did.DID, digest multihash.Multihash) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.registered[string(digest)] {
		delete(c.registered, string(digest))
		c.removed++
	}
	return nil
}

// assertNoneLeft fails if a blob is still registered: accepted and never
// removed.
func (c *providerConcluder) assertNoneLeft(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for d := range c.registered {
		require.FailNow(t, "blob left registered with nothing to remove it", "blob %x", []byte(d))
	}
}

// runOnce runs and clears *hook.
func (c *providerConcluder) runOnce(hook *func()) {
	c.mu.Lock()
	h := *hook
	*hook = nil
	c.mu.Unlock()
	if h != nil {
		h()
	}
}

// assertGone checks that nothing records d any more: no location a later
// write could reuse, no park, no intent.
func assertGone(t *testing.T, mem *testStore, d multihash.Multihash) {
	t.Helper()
	ctx := context.Background()
	loc, err := mem.GetLocation(ctx, mem.bk(), d)
	require.ErrorIs(t, err, registry.ErrNotFound, "blob %s has a location after its release: %+v (err=%v)", d, loc, err)
	_, err = mem.ParkByDigest(ctx, d)
	require.ErrorIs(t, err, registry.ErrNotFound, "park row for released blob %s survived", d)
	_, err = mem.IntentByDigest(ctx, d)
	require.ErrorIs(t, err, registry.ErrNotFound, "intent for released blob %s survived", d)
}

// TestAcceptRacesATeardown: an abort or a session expiry arrives while a
// part's blob is being concluded, before the provider accepts it or after,
// and the provider registers the blob only just before it answers. The
// teardown's release runs under the blob's subject, after the accept: it
// removes the registered blob, and nothing is left registered or recorded.
func TestAcceptRacesATeardown(t *testing.T) {
	teardowns := map[string]func(t *testing.T, b *Backend, key, uploadID string){
		"abort": func(t *testing.T, b *Backend, key, uploadID string) {
			bucket := "bk"
			assert.NoError(t, b.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}), "Abort")
		},
		"expiry": func(t *testing.T, b *Backend, _, _ string) {
			n, err := sweepUploads(context.Background(), b)
			assert.NoError(t, err, "sweep")
			assert.Equal(t, 1, n, "sweep: cleaned")
		},
	}
	for name, teardown := range teardowns {
		for _, when := range []string{"before accept", "after accept"} {
			t.Run(name+"/"+when, func(t *testing.T) {
				pc := &providerConcluder{registerLate: true}
				b, mem := newDeferredBackend(t, pc, func(d *Deps) { d.Remover = pc })
				key := "race-" + name
				uploadID := mpCreate(t, b, key, "", "")
				uploadDistinctParts(t, b, key, uploadID, 1)
				d := hygienePartDigests(t, mem, uploadID, 1)[0]

				// The teardown starts mid-conclude and is given time to
				// overtake it.
				done := make(chan struct{})
				hook := func() {
					go func() {
						teardown(t, b, key, uploadID)
						close(done)
					}()
					time.Sleep(100 * time.Millisecond)
					pc.mu.Lock()
					defer pc.mu.Unlock()
					assert.False(t, pc.removed > 0 || slices.Contains(pc.aborted, string(d)), "the teardown's release ran while the blob was being concluded")
				}
				if when == "before accept" {
					pc.before = hook
				} else {
					pc.after = hook
				}
				_ = runAccepts(t, b, uploadID)
				<-done
				drainReleases(t, b)

				assertGone(t, mem, d)
				pc.assertNoneLeft(t)
				require.Equal(t, 1, pc.removed, "blob %s removed %d times", d, pc.removed)
			})
		}
	}
}

// TestAcceptRacesASupersede: a part is uploaded again while the first
// upload's blob is being concluded, after the provider has accepted it and
// before it registers it. The first blob's release runs under its subject,
// after the accept, and removes the registered blob; the accept's records
// find the row replaced and mark nothing. The next accept accepts the
// replacement, and Complete commits it.
func TestAcceptRacesASupersede(t *testing.T) {
	pc := &providerConcluder{registerLate: true}
	b, mem := newDeferredBackend(t, pc, func(d *Deps) { d.Remover = pc })
	key := "race-supersede"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	first := hygienePartDigests(t, mem, uploadID, 1)[0]

	replacement := testBody(int(backend.MinPartSize))
	replacement[0] = 0xff
	var etag *string
	superseded := make(chan struct{})
	pc.after = func() {
		// The UploadPart waits for the release it queues, which waits for
		// this accept, so it runs beside it.
		go func() {
			defer close(superseded)
			out, err := mpUploadPart(t, b, key, uploadID, 1, replacement, nil)
			if !assert.NoError(t, err, "superseding UploadPart") {
				return
			}
			etag = out.ETag
		}()
		time.Sleep(100 * time.Millisecond)
		pc.mu.Lock()
		defer pc.mu.Unlock()
		assert.False(t, pc.removed > 0 || slices.Contains(pc.aborted, string(first)), "the superseded blob's release ran while it was being concluded")
	}
	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	<-superseded
	drainReleases(t, b)

	assertGone(t, mem, first)
	pc.mu.Lock()
	stillRegistered := pc.registered[string(first)]
	pc.mu.Unlock()
	require.False(t, stillRegistered, "superseded blob %s left registered with nothing to remove it", first)
	require.NoError(t, runAccepts(t, b, uploadID), "accept of the replacement")
	second := hygienePartDigests(t, mem, uploadID, 1)[0]
	require.Equal(t, registry.PartAccepted, partStates(t, mem, uploadID)[1], "replacement part state")
	assertAcceptedAndUnparked(t, mem, second)

	one := int32(1)
	_, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: etag}}, nil)
	require.NoError(t, err, "Complete")
}

// TestAcceptRacesASupersedesRelease: a part is uploaded again, and the
// accept runs after the new upload has parked its blob and before its part
// row lands. The old row still stands, so the accept accepts the old blob;
// the write that replaces the row queues the blob's release with it, and the
// release removes the accepted blob from the provider. Nothing is left
// registered or recorded.
func TestAcceptRacesASupersedesRelease(t *testing.T) {
	pc := &providerConcluder{}
	store := &teardownBeforePutPart{}
	b, mem := newDeferredBackend(t, pc, func(d *Deps) {
		store.AuthorityCapturer = d.Capturer
		d.Capturer = store
		d.Remover = pc
	})
	ctx := context.Background()
	key := "race-supersede-release"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	old := hygienePartDigests(t, mem, uploadID, 1)[0]

	store.hook = func() {
		assert.NoError(t, runAccepts(t, b, uploadID), "accept")
	}
	replacement := testBody(int(backend.MinPartSize))
	replacement[0] = 0xee
	_, err := mpUploadPart(t, b, key, uploadID, 1, replacement, nil)
	require.NoError(t, err, "superseding UploadPart")
	require.Nil(t, store.hook, "the hook never ran")
	drainReleases(t, b)

	assertGone(t, mem, old)
	pc.mu.Lock()
	stillRegistered := pc.registered[string(old)]
	pc.mu.Unlock()
	require.False(t, stillRegistered, "superseded blob %s left registered with nothing to remove it", old)
	got, _ := mem.ReleasingDigests(ctx, mem.bk(), []multihash.Multihash{old})
	require.False(t, got[string(old)], "release of %s left pending", old)
}

// TestAcceptsShareABlob: two open uploads of the space hold the same bytes,
// so their parts share one park, and the blob has one accept, which
// concludes it once and marks both parts accepted.
func TestAcceptsShareABlob(t *testing.T) {
	cc := &countingConcluder{}
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, cc, func(d *Deps) { d.Remover = rm })
	ctx := context.Background()
	key := "shared-park"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	const other = "other-sharing"
	require.NoError(t, mem.CreateSession(ctx, registry.MultipartSession{UploadID: other, Bucket: "bk", Space: mem.bk(), ObjectKey: "other"}), "CreateSession")
	_, err := mem.PutPart(ctx, registry.MultipartPart{UploadID: other, PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{d}, SpanLengths: []int64{1}}, nil)
	require.NoError(t, err, "PutPart")

	for _, id := range []string{uploadID, other} {
		require.NoError(t, runAccepts(t, b, id), "accept of %s", id)
	}
	for _, id := range []string{uploadID, other} {
		require.Equal(t, registry.PartAccepted, partStates(t, mem, id)[1], "upload %s part state", id)
	}
	got := cc.concluded()
	require.True(t, slices.Equal(got, []int{1}), "conclude calls = %v, want the shared blob concluded once", got)
	assertAcceptedAndUnparked(t, mem, d)
	n := rm.removedDigests()[string(d)]
	require.Zero(t, n, "shared blob %s removed %d times", d, n)
}

// TestAcceptLeavesAReplacementPark: the provider accepts a part's blob, the
// park the accept concluded is taken from under it, and another write of the
// same bytes parks them again under its own allocation before the accept
// commits. The accept's record names the allocation it concluded, so it
// leaves the replacement park, and the part, which has no location yet,
// stays parked. The next accept concludes the replacement on its own
// allocation.
func TestAcceptLeavesAReplacementPark(t *testing.T) {
	pc := &providerConcluder{}
	b, mem := newDeferredBackend(t, pc, func(d *Deps) { d.Remover = pc })
	ctx := context.Background()
	key := "race-replacement-park"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	h, err := multihash.Sum([]byte("replacement allocation"), multihash.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	replacement := cid.NewCidV1(cid.Raw, h)
	pc.after = func() {
		assert.NoError(t, mem.DeletePark(ctx, mem.bk(), d), "DeletePark")
		ok, err := mem.PutPark(ctx, registry.BlobPark{Space: mem.bk(), Digest: d, AddTask: replacement.Bytes(), AcceptTask: replacement.Bytes(), PutInvocation: []byte{1}, Size: 1})
		assert.NoError(t, err, "PutPark (replacement)")
		assert.True(t, ok, "PutPark (replacement)")
	}
	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	park, err := mem.GetPark(ctx, mem.bk(), d)
	require.NoError(t, err, "replacement park after the first accept")
	require.Equal(t, replacement.Bytes(), park.AddTask, "replacement park after the first accept")
	require.Equal(t, registry.PartParked, partStates(t, mem, uploadID)[1], "part state")

	require.NoError(t, runAccepts(t, b, uploadID), "second accept")
	pc.mu.Lock()
	concluded := slices.Clone(pc.concluded)
	pc.mu.Unlock()
	require.True(t, slices.Contains(concluded, replacement), "concluded add tasks = %v; the replacement park was never concluded on its own allocation", concluded)
	assertAcceptedAndUnparked(t, mem, d)
}

// locationThenRelease is a location store whose next lookup of digest finds
// the location and then runs hook before answering: the window between the
// accept reading a blob as located and committing.
type locationThenRelease struct {
	registry.LocationStore
	mu     sync.Mutex
	digest multihash.Multihash
	hook   func()
}

func (l *locationThenRelease) GetLocation(ctx context.Context, space did.DID, digest multihash.Multihash) (*registry.BlobLocation, error) {
	loc, err := l.LocationStore.GetLocation(ctx, space, digest)
	l.mu.Lock()
	var hook func()
	if l.digest != nil && bytes.Equal(l.digest, digest) && err == nil {
		hook, l.hook, l.digest = l.hook, nil, nil
	}
	l.mu.Unlock()
	if hook != nil {
		hook()
	}
	return loc, err
}

// TestAcceptKeepsAParkThatReplacesALocation: the accept finds a part's blob
// located, and before it commits the location goes (with the park left
// beside it) and another write of the same bytes parks them again. The
// accept's writes hold only while the location stands, so the replacement
// park stays and the part stays parked; dropping the park would leave the
// replacement's allocation never concluded or aborted.
func TestAcceptKeepsAParkThatReplacesALocation(t *testing.T) {
	locs := &locationThenRelease{}
	b, mem := newDeferredBackend(t, &countingConcluder{}, func(d *Deps) {
		locs.LocationStore = d.Locations
		d.Locations = locs
	})
	ctx := context.Background()
	key := "located-then-released"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	// Another conclude recorded the blob, its park left beside the location.
	require.NoError(t, mem.PutLocation(ctx, registry.BlobLocation{Space: mem.bk(), Digest: d, Provider: "did:key:zNode", URL: "https://node.example/blob", Size: 1}), "PutLocation")
	h, err := multihash.Sum([]byte("replacing allocation"), multihash.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	replacement := cid.NewCidV1(cid.Raw, h)
	locs.mu.Lock()
	locs.digest = d
	locs.hook = func() {
		_ = mem.DeleteLocation(ctx, mem.bk(), d)
		_ = mem.DeletePark(ctx, mem.bk(), d)
		ok, err := mem.PutPark(ctx, registry.BlobPark{Space: mem.bk(), Digest: d, AddTask: replacement.Bytes(), AcceptTask: replacement.Bytes(), PutInvocation: []byte{1}, Size: 1})
		assert.NoError(t, err, "PutPark (replacement)")
		assert.True(t, ok, "PutPark (replacement)")
	}
	locs.mu.Unlock()

	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	park, err := mem.GetPark(ctx, mem.bk(), d)
	require.NoError(t, err, "replacement park dropped by a stale located path")
	require.Equal(t, replacement.Bytes(), park.AddTask, "park add task")
	require.Equal(t, registry.PartParked, partStates(t, mem, uploadID)[1], "part state")
}

// blobAccept returns the one queue-space-shard.accept queued for d in the
// test bucket's space.
func blobAccept(t *testing.T, mem *testStore, d multihash.Multihash) taskrunnertest.QueuedTask {
	t.Helper()
	tasks, err := mem.queued(context.Background(), spaceshardqueue.Name)
	require.NoError(t, err, "queued blob accepts")
	subject := spaceshardqueue.Subject(mem.bk(), d)
	var found []taskrunnertest.QueuedTask
	for _, task := range tasks {
		if task.Subject == subject && task.Action == spaceshardqueue.AcceptAction {
			found = append(found, task)
		}
	}
	require.Len(t, found, 1, "accepts queued for the blob")
	return found[0]
}

// TestAcceptRetriesUntilTheConcludeSucceeds: a blob's accept whose conclude
// keeps failing stays queued to retry however often it has failed, with no
// later part or Complete to queue it again, and accepts the part once the
// upload service answers.
func TestAcceptRetriesUntilTheConcludeSucceeds(t *testing.T) {
	const failures = 8
	cc := &countingConcluder{fails: failures}
	b, mem := newDeferredBackend(t, cc)
	ctx := context.Background()
	key := "accept-keeps-retrying"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	// The blob's accept fails once and waits to retry.
	require.NoError(t, runAccepts(t, b, uploadID), "accept")

	// Each retry is brought forward in place of waiting out its backoff. A
	// task's attempts count its failures.
	bringForward := func() {
		t.Helper()
		_, err := b.blobs.ExpediteAccepts(ctx, mem.bk())
		require.NoError(t, err, "bring the accept forward")
	}
	for failed := 2; failed <= failures; failed++ {
		bringForward()
		deadline := time.Now().Add(10 * time.Second)
		for {
			task := blobAccept(t, mem, d)
			require.Equal(t, "pending", task.State, "the accept is %s after %d failures, want it waiting to retry", task.State, task.Attempts)
			if task.Attempts >= failed {
				break
			}
			require.False(t, time.Now().After(deadline), "failure %d did not happen: %+v", failed, task)
			time.Sleep(10 * time.Millisecond)
		}
	}
	bringForward()
	deadline := time.Now().Add(10 * time.Second)
	for !allIn(partStates(t, mem, uploadID), registry.PartAccepted) {
		if time.Now().After(deadline) {
			require.FailNow(t, "parts not accepted after the conclude succeeded", "part states = %v, want accepted; accept = %+v",
				partStates(t, mem, uploadID), blobAccept(t, mem, d))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// goneConcluder concludes like the parking uploader, except that the
// provider no longer holds the allocation of a digest marked gone.
type goneConcluder struct {
	parkingUploader
	gmu  sync.Mutex
	gone map[string]bool
	// refusing runs, when set, as the provider refuses a conclude.
	refusing func()
}

func (g *goneConcluder) setGone(digests ...multihash.Multihash) {
	g.gmu.Lock()
	defer g.gmu.Unlock()
	g.gone = map[string]bool{}
	for _, d := range digests {
		g.gone[string(d)] = true
	}
}

func (g *goneConcluder) ConcludeBlobs(ctx context.Context, space did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	g.gmu.Lock()
	for _, p := range parked {
		if g.gone[string(p.Digest)] {
			refusing := g.refusing
			g.gmu.Unlock()
			if refusing != nil {
				refusing()
			}
			return make([]*uploader.BlobLocation, len(parked)), fmt.Errorf("conclude: %w", uploader.ErrAllocationGone)
		}
	}
	g.gmu.Unlock()
	return g.parkingUploader.ConcludeBlobs(ctx, space, parked)
}

// TestAcceptFailsForGoodWhenTheAllocationIsGone: the provider no longer holds
// a parked part's allocation, so its blob's accept fails without a retry and
// drops the park. Complete refuses the part with InvalidPart, and the same
// bytes uploaded again park a new allocation, which Complete accepts.
func TestAcceptFailsForGoodWhenTheAllocationIsGone(t *testing.T) {
	gc := &goneConcluder{}
	b, mem := newDeferredBackend(t, gc)
	setNone(t, b)
	ctx := context.Background()
	key := "accept-allocation-gone"
	part := testBody(int(backend.MinPartSize))
	d := digestOf(t, part)
	uploadID, parts := uploadOnePart(t, b, "bk", key, part)
	gc.setGone(d)

	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	task := blobAccept(t, mem, d)
	require.Equal(t, "failed", task.State, "the blob's accept is %s, want failed with no retry", task.State)
	_, err := mem.GetPark(ctx, mem.bk(), d)
	require.ErrorIs(t, err, registry.ErrNotFound, "park of the lost blob")
	require.Equal(t, registry.PartParked, partStates(t, mem, uploadID)[1], "part state")
	_, err = mpComplete(t, b, key, uploadID, parts, nil)
	var apiErr s3err.APIError
	require.ErrorAs(t, err, &apiErr, "Complete of the lost part")
	require.Equal(t, "InvalidPart", apiErr.Code, "Complete of the lost part")

	gc.setGone()
	out, err := mpUploadPart(t, b, key, uploadID, 1, part, nil)
	require.NoError(t, err, "UploadPart again")
	got := hygienePartDigests(t, mem, uploadID, 1)[0]
	require.True(t, bytes.Equal(got, d), "the part uploaded again is stored under %x, want %x", got, d)
	_, err = mem.GetPark(ctx, mem.bk(), d)
	require.NoError(t, err, "park of the part uploaded again")
	n := int32(1)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{ETag: out.ETag, PartNumber: &n}}, nil)
	require.NoError(t, err, "Complete after the part was uploaded again")
}

// TestALostAllocationsAcceptLeavesAReplacementPark: while the provider
// refuses a conclude because the allocation is gone, another allocation of
// the same bytes replaces the park, as a part uploaded again does once the
// attempt has outlived its lease. The accept that fails drops only the park
// it concluded: the replacement stays for its own accept.
func TestALostAllocationsAcceptLeavesAReplacementPark(t *testing.T) {
	gc := &goneConcluder{}
	b, mem := newDeferredBackend(t, gc)
	ctx := context.Background()
	key := "lost-allocation-replacement-park"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	h, err := multihash.Sum([]byte("replacement allocation"), multihash.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	replacement := cid.NewCidV1(cid.Raw, h)
	gc.setGone(d)
	gc.refusing = func() {
		assert.NoError(t, mem.DeletePark(ctx, mem.bk(), d), "DeletePark")
		ok, err := mem.PutPark(ctx, registry.BlobPark{Space: mem.bk(), Digest: d, AddTask: replacement.Bytes(), AcceptTask: replacement.Bytes(), PutInvocation: []byte{1}, Size: 1})
		assert.NoError(t, err, "PutPark (replacement)")
		assert.True(t, ok, "PutPark (replacement)")
	}
	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	task := blobAccept(t, mem, d)
	require.Equal(t, "failed", task.State, "the blob's accept")
	park, err := mem.GetPark(ctx, mem.bk(), d)
	require.NoError(t, err, "replacement park after the failed accept")
	require.Equal(t, replacement.Bytes(), park.AddTask, "replacement park after the failed accept")
}

// TestAFailingAcceptIsNotQueuedAgain: while a blob's accept keeps failing
// and waits to retry, the parts uploaded after it and the Completes the
// client sends queue no further accept of that blob.
func TestAFailingAcceptIsNotQueuedAgain(t *testing.T) {
	cc := &countingConcluder{fails: 1 << 30}
	b, mem := newDeferredBackend(t, cc)
	limits := storeFor(b).limits
	limits.AcceptWait = 300 * time.Millisecond
	key := "failing-accept-queued-once"
	uploadID := mpCreate(t, b, key, "", "")
	var parts []types.CompletedPart
	for i := int32(1); i <= 3; i++ {
		body := testBody(int(backend.MinPartSize))
		body[0] = byte(i)
		out, err := mpUploadPart(t, b, key, uploadID, i, body, nil)
		require.NoError(t, err, "UploadPart %d", i)
		num := i
		parts = append(parts, types.CompletedPart{PartNumber: &num, ETag: out.ETag})
		require.NoError(t, runAccepts(t, b, uploadID), "accept after part %d", i)
	}
	for range 2 {
		_, err := mpComplete(t, b, key, uploadID, parts, nil)
		var apiErr s3err.APIError
		require.ErrorAs(t, err, &apiErr, "Complete against a failing upload service: err = %v, want OperationAborted", err)
		require.Equal(t, "OperationAborted", apiErr.Code, "Complete against a failing upload service: err")
	}
	for n := 1; n <= 3; n++ {
		blobAccept(t, mem, hygienePartDigests(t, mem, uploadID, n)[0]) // exactly one accept each
	}
}
