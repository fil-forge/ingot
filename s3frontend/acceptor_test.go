package s3frontend

import (
	"context"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// countingConcluder is a parking uploader that records what each conclude
// was asked for, and fails the next `fails` concludes outright. Safe for the
// acceptor's goroutine and the test to use at once.
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

func (c *countingConcluder) setFails(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fails = n
}

// startAcceptors runs b's acceptors for the rest of the test.
func startAcceptors(t *testing.T, b *Backend) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b.StartAcceptors(ctx)
	t.Cleanup(func() {
		cancel()
		b.WaitAcceptors()
	})
}

// uploadDistinctParts uploads n parts of distinct content, one blob each.
func uploadDistinctParts(t *testing.T, b *Backend, key, uploadID string, n int) []types.CompletedPart {
	t.Helper()
	var parts []types.CompletedPart
	for i := int32(1); i <= int32(n); i++ {
		body := testBody(int(backend.MinPartSize))
		body[0] = byte(i)
		out, err := mpUploadPart(t, b, key, uploadID, i, body, nil)
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i, err)
		}
		num := i
		parts = append(parts, types.CompletedPart{PartNumber: &num, ETag: out.ETag})
	}
	return parts
}

// partStates returns each recorded part's state by part number.
func partStates(t *testing.T, mem *inmem.MemStore, uploadID string) map[int]string {
	t.Helper()
	parts, err := mem.ListParts(context.Background(), uploadID)
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
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

// TestAcceptorAcceptsPartsWhileUploading: each UploadPart queues its part and
// nudges the upload's acceptor, which accepts the parts while the upload is
// still open. Complete then finds every blob located and concludes nothing.
func TestAcceptorAcceptsPartsWhileUploading(t *testing.T) {
	cc := &countingConcluder{}
	b, mem := newDeferredBackend(t, cc)
	startAcceptors(t, b)
	ctx := context.Background()
	key := "accept-during-upload"

	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 3)
	if err := b.drainAcceptor(ctx, uploadID); err != nil {
		t.Fatalf("drainAcceptor: %v", err)
	}
	if states := partStates(t, mem, uploadID); !allIn(states, registry.PartAccepted) {
		t.Fatalf("part states = %v, want all accepted", states)
	}
	var digests []multihash.Multihash
	for n := 1; n <= 3; n++ {
		digests = append(digests, hygienePartDigests(t, mem, uploadID, n)...)
	}
	for _, d := range digests {
		assertAcceptedAndUnparked(t, mem, d)
	}
	concludedBefore := cc.concluded()

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got := cc.concluded(); len(got) != len(concludedBefore) {
		t.Fatalf("conclude calls after Complete = %v, want none beyond the acceptor's %v", got, concludedBefore)
	}
}

// TestAcceptorBatchesByBlobCount: a pass concludes at most acceptBatchBlobs
// blobs, and the acceptor makes passes until the queue is empty.
func TestAcceptorBatchesByBlobCount(t *testing.T) {
	saved := acceptBatchBlobs
	acceptBatchBlobs = 2
	t.Cleanup(func() { acceptBatchBlobs = saved })

	cc := &countingConcluder{}
	b, mem := newDeferredBackend(t, cc)
	ctx := context.Background()
	key := "accept-batches"

	// Uploaded before the acceptors run: the parts queue, nobody accepts.
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 3)
	if states := partStates(t, mem, uploadID); !allIn(states, registry.PartParked) {
		t.Fatalf("part states = %v, want all parked", states)
	}

	startAcceptors(t, b)
	if err := b.drainAcceptor(ctx, uploadID); err != nil {
		t.Fatalf("drainAcceptor: %v", err)
	}
	if got := cc.concluded(); len(got) != 2 || got[0] != 2 || got[1] != 1 {
		t.Fatalf("conclude calls = %v, want [2 1]", got)
	}
	if states := partStates(t, mem, uploadID); !allIn(states, registry.PartAccepted) {
		t.Fatalf("part states = %v, want all accepted", states)
	}
}

// TestAcceptorRestartSweepPicksUpParkedParts: parts queued while no acceptor
// ran (a process stopped, or a nudge went missing) are found by the restart
// sweep, which skips sessions that are no longer open or completing.
func TestAcceptorRestartSweepPicksUpParkedParts(t *testing.T) {
	cc := &countingConcluder{}
	b, mem := newDeferredBackend(t, cc)
	ctx := context.Background()

	live := mpCreate(t, b, "live", "", "")
	uploadDistinctParts(t, b, "live", live, 2)
	aborting := mpCreate(t, b, "aborting", "", "")
	uploadDistinctParts(t, b, "aborting", aborting, 1)
	if won, err := mem.LatchSession(ctx, aborting, registry.SessionOpen, registry.SessionAborting); err != nil || !won {
		t.Fatalf("latch aborting: %v %v", won, err)
	}

	startAcceptors(t, b)
	if n, err := b.NudgeParkedUploads(ctx); err != nil || n != 1 {
		t.Fatalf("NudgeParkedUploads = %d, %v; want the one live upload", n, err)
	}
	if err := b.drainAcceptor(ctx, live); err != nil {
		t.Fatalf("drainAcceptor: %v", err)
	}
	if states := partStates(t, mem, live); !allIn(states, registry.PartAccepted) {
		t.Fatalf("live part states = %v, want all accepted", states)
	}
	// Even nudged directly, an aborting session's parts are left to its
	// teardown.
	if err := b.drainAcceptor(ctx, aborting); err != nil {
		t.Fatalf("drainAcceptor (aborting): %v", err)
	}
	if states := partStates(t, mem, aborting); !allIn(states, registry.PartParked) {
		t.Fatalf("aborting part states = %v, want parked", states)
	}
	if got := cc.concluded(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("conclude calls = %v, want one for the live upload's two blobs", got)
	}
}

// TestAcceptorRetriesAfterAFailedPass: a failed conclude leaves the parts
// parked and is reported to a waiter at once; the acceptor backs off and
// retries until the conclude goes through.
func TestAcceptorRetriesAfterAFailedPass(t *testing.T) {
	savedFirst, savedMax := acceptBackoffFirst, acceptBackoffMax
	acceptBackoffFirst, acceptBackoffMax = 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { acceptBackoffFirst, acceptBackoffMax = savedFirst, savedMax })

	cc := &countingConcluder{fails: 1000}
	b, mem := newDeferredBackend(t, cc)
	ctx := context.Background()
	key := "accept-retry"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 2)

	startAcceptors(t, b)
	if err := b.drainAcceptor(ctx, uploadID); err == nil {
		t.Fatal("drainAcceptor reported no failure against a failing conclude")
	}
	if states := partStates(t, mem, uploadID); !allIn(states, registry.PartParked) {
		t.Fatalf("part states after a failed pass = %v, want parked", states)
	}

	cc.setFails(0)
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := b.drainAcceptor(ctx, uploadID)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("acceptor never recovered: %v", err)
		}
		time.Sleep(acceptBackoffMax)
	}
	if states := partStates(t, mem, uploadID); !allIn(states, registry.PartAccepted) {
		t.Fatalf("part states after the retry = %v, want accepted", states)
	}
}

// TestAcceptorLeavesAPartWithoutAParkForComplete: a parked part whose blob
// has neither a park nor a location is not the acceptor's to finish. The part
// goes back to 'spooled', where Complete uploads it from its spool copy.
func TestAcceptorLeavesAPartWithoutAParkForComplete(t *testing.T) {
	cc := &countingConcluder{}
	b, mem := newDeferredBackend(t, cc)
	ctx := context.Background()
	key := "accept-no-park"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 2)
	sess, err := mem.GetSession(ctx, uploadID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	for _, d := range hygienePartDigests(t, mem, uploadID, 2) {
		if err := mem.DeletePark(ctx, sess.Space, d); err != nil {
			t.Fatalf("DeletePark: %v", err)
		}
	}

	startAcceptors(t, b)
	if err := b.drainAcceptor(ctx, uploadID); err != nil {
		t.Fatalf("drainAcceptor: %v", err)
	}
	want := map[int]string{1: registry.PartAccepted, 2: registry.PartSpooled}
	if got := partStates(t, mem, uploadID); got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("part states = %v, want %v", got, want)
	}
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

// TestCompleteWaitsForTheAcceptor: Complete arriving while the acceptor is
// concluding the last parts waits for it, and concludes nothing itself.
func TestCompleteWaitsForTheAcceptor(t *testing.T) {
	g := newGatedConcluder()
	b, mem := newDeferredBackend(t, g)
	startAcceptors(t, b)
	key := "complete-waits"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 1)
	<-g.entered // the acceptor is concluding part 1
	body := testBody(int(backend.MinPartSize))
	body[0] = 2
	out, err := mpUploadPart(t, b, key, uploadID, 2, body, nil)
	if err != nil {
		t.Fatalf("UploadPart 2: %v", err)
	}
	two := int32(2)
	parts = append(parts, types.CompletedPart{PartNumber: &two, ETag: out.ETag})

	done := make(chan error, 1)
	go func() {
		_, err := mpComplete(t, b, key, uploadID, parts, nil)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("Complete returned before the acceptor finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(g.open)
	if err := <-done; err != nil {
		t.Fatalf("Complete: %v", err)
	}
	// The acceptor's two passes: the second part queued while the first
	// was concluding. Complete found both accepted and concluded nothing.
	if n := g.concludes(); n != 2 {
		t.Fatalf("concludes = %d, want the acceptor's two passes", n)
	}
	if states := partStates(t, mem, uploadID); !allIn(states, registry.PartAccepted) {
		t.Fatalf("part states = %v, want all accepted", states)
	}
}

// TestCompleteOverTheWaitBudgetReopensTheUpload: an acceptor still
// concluding when the wait budget runs out makes Complete answer
// OperationAborted and reopen the upload, so a client that retries on its
// own commits once the acceptor is done (botocore clients do not retry it).
func TestCompleteOverTheWaitBudgetReopensTheUpload(t *testing.T) {
	saved := acceptWaitBudget
	acceptWaitBudget = 20 * time.Millisecond
	t.Cleanup(func() { acceptWaitBudget = saved })

	g := newGatedConcluder()
	b, mem := newDeferredBackend(t, g)
	startAcceptors(t, b)
	ctx := context.Background()
	key := "complete-over-budget"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 1)
	<-g.entered

	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "OperationAborted" {
		t.Fatalf("Complete over the wait budget: err = %v, want OperationAborted", err)
	}
	if sess, err := mem.GetSession(ctx, uploadID); err != nil || sess.State != registry.SessionOpen {
		t.Fatalf("session after the refused Complete = %+v, %v; want open", sess, err)
	}

	close(g.open)
	if err := b.drainAcceptor(ctx, uploadID); err != nil {
		t.Fatalf("drainAcceptor: %v", err)
	}
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("retry Complete: %v", err)
	}
}

// TestCompleteFailsFastWhileTheAcceptorFails: an acceptor whose last pass
// failed makes Complete answer OperationAborted at once rather than wait out
// the budget.
func TestCompleteFailsFastWhileTheAcceptorFails(t *testing.T) {
	shrinkAcceptBackoff(t)
	acceptBackoffFirst, acceptBackoffMax = time.Hour, time.Hour
	cc := &countingConcluder{fails: 1}
	b, _ := newDeferredBackend(t, cc)
	startAcceptors(t, b)
	ctx := context.Background()
	key := "complete-acceptor-failing"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 1)
	if err := b.drainAcceptor(ctx, uploadID); err == nil {
		t.Fatal("the acceptor's pass did not fail")
	}

	start := time.Now()
	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "OperationAborted" {
		t.Fatalf("Complete against a failing acceptor: err = %v, want OperationAborted", err)
	}
	if took := time.Since(start); took > acceptWaitBudget/2 {
		t.Fatalf("Complete took %v; it should not wait for an acceptor in backoff", took)
	}
}

// failOnceMoveToParked is a multipart store whose next spooled-to-parked move
// fails: an UploadPart that dies after parking its blobs, leaving its part
// spooled.
type failOnceMoveToParked struct {
	registry.MultipartStore
	mu    sync.Mutex
	armed bool
}

func (f *failOnceMoveToParked) MoveParts(ctx context.Context, parts []registry.MultipartPart, from, to string) (int, error) {
	f.mu.Lock()
	fail := f.armed && to == registry.PartParked
	if fail {
		f.armed = false
	}
	f.mu.Unlock()
	if fail {
		return 0, errors.New("parts table unavailable")
	}
	return f.MultipartStore.MoveParts(ctx, parts, from, to)
}

// TestCompleteFinishesASpooledPart: a part left spooled by a request that
// died after parking its blobs is not in the acceptor's queue. Complete
// concludes it itself.
func TestCompleteFinishesASpooledPart(t *testing.T) {
	cc := &countingConcluder{}
	store := &failOnceMoveToParked{}
	b, mem := newDeferredBackend(t, cc, func(d *Deps) {
		store.MultipartStore = d.Multipart
		d.Multipart = store
	})
	startAcceptors(t, b)
	key := "complete-spooled"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 1)

	body := testBody(int(backend.MinPartSize))
	body[0] = 2
	store.mu.Lock()
	store.armed = true
	store.mu.Unlock()
	etag := ""
	if out, err := mpUploadPart(t, b, key, uploadID, 2, body, nil); err == nil {
		t.Fatalf("UploadPart succeeded although its part could not be queued (etag %s)", *out.ETag)
	}
	// The client retries nothing; the part as recorded is complete.
	p2 := partRecord(t, mem, uploadID, 2)
	if p2.State != registry.PartSpooled {
		t.Fatalf("part 2 state = %q, want spooled", p2.State)
	}
	etag = `"` + hex.EncodeToString(p2.ETagMD5) + `"`
	two := int32(2)
	parts = append(parts, types.CompletedPart{PartNumber: &two, ETag: &etag})

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	for _, d := range hygienePartDigests(t, mem, uploadID, 2) {
		assertAcceptedAndUnparked(t, mem, d)
	}
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

// TestAcceptorPassTimesOut: a conclude that never answers ends its pass at
// acceptPassTimeout, as a failure the acceptor retries, so the upload is not
// held by one stalled request.
func TestAcceptorPassTimesOut(t *testing.T) {
	shrinkAcceptBackoff(t)
	saved := acceptPassTimeout
	acceptPassTimeout = 50 * time.Millisecond
	t.Cleanup(func() { acceptPassTimeout = saved })

	sc := &stallingConcluder{}
	sc.stalled.Store(true)
	b, mem := newDeferredBackend(t, sc)
	ctx := context.Background()
	key := "accept-pass-timeout"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)

	startAcceptors(t, b)
	if err := b.drainAcceptor(ctx, uploadID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drainAcceptor against a stalled conclude = %v, want the pass deadline", err)
	}
	if states := partStates(t, mem, uploadID); !allIn(states, registry.PartParked) {
		t.Fatalf("part states after a timed-out pass = %v, want parked", states)
	}

	sc.stalled.Store(false)
	drainUntilIdle(t, b, uploadID)
	if states := partStates(t, mem, uploadID); !allIn(states, registry.PartAccepted) {
		t.Fatalf("part states after the retry = %v, want accepted", states)
	}
}
