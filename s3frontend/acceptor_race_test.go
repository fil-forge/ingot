package s3frontend

import (
	"bytes"
	"context"
	"encoding/hex"
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
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// providerConcluder concludes as the upload service and provider do: a blob
// whose allocation was aborted is refused, any other is accepted, and an
// abort of an accepted blob is then refused as already accepted
// (parkingUploader.AbortBlob). An accepted blob is registered in the space
// only once the conclude answers, as the upload service does, and
// RemoveBlob removes only a registered blob: a remove that arrives between
// the accept and the registration finds nothing, and the registration that
// follows is left behind. It is the backend's BlobRemover too. The before
// hook runs once ahead of the next conclude, the after hook once between its
// accepts and its reply.
type providerConcluder struct {
	parkingUploader
	before, after func()
	registered    map[string]bool
	removed       int
	// concluded is the /blob/add task of every blob a conclude accepted.
	concluded []cid.Cid
}

func (c *providerConcluder) ConcludeBlobs(_ context.Context, _ did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	c.runOnce(&c.before)
	locations := make([]*uploader.BlobLocation, len(parked))
	var refused []error
	c.mu.Lock()
	aborted := map[string]bool{}
	for _, d := range c.aborted {
		aborted[d] = true
	}
	for i, p := range parked {
		if aborted[string(p.Digest)] {
			refused = append(refused, fmt.Errorf("blob %s: allocation released", p.Digest))
			continue
		}
		if c.acceptedOnProvider == nil {
			c.acceptedOnProvider = map[string]bool{}
		}
		c.acceptedOnProvider[string(p.Digest)] = true
		c.concluded = append(c.concluded, p.AddTask)
		locations[i] = &uploader.BlobLocation{Provider: "did:key:zNode", URL: "https://node.example/blob", Size: p.Size}
	}
	c.mu.Unlock()
	c.runOnce(&c.after)
	c.mu.Lock()
	if c.registered == nil {
		c.registered = map[string]bool{}
	}
	for i, p := range parked {
		if locations[i] != nil {
			c.registered[string(p.Digest)] = true
		}
	}
	c.mu.Unlock()
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
// removed after its registration.
func (c *providerConcluder) assertNoneLeft(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for d := range c.registered {
		t.Fatalf("blob %x left registered with nothing to remove it", []byte(d))
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

// shrinkAcceptBackoff makes the acceptors retry at once for the rest of the
// test.
func shrinkAcceptBackoff(t *testing.T) {
	t.Helper()
	savedFirst, savedMax := acceptBackoffFirst, acceptBackoffMax
	acceptBackoffFirst, acceptBackoffMax = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { acceptBackoffFirst, acceptBackoffMax = savedFirst, savedMax })
}

// drainUntilIdle waits for uploadID's acceptor to stop with nothing to do,
// through any failed passes on the way.
func drainUntilIdle(t *testing.T, b *Backend, uploadID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := b.drainAcceptor(context.Background(), uploadID)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("acceptor for %s never went idle: %v", uploadID, err)
		}
		time.Sleep(acceptBackoffMax)
	}
}

// assertGone checks that nothing records d any more: no location a later
// write could reuse, no park, no intent.
func assertGone(t *testing.T, mem *inmem.MemStore, d multihash.Multihash) {
	t.Helper()
	ctx := context.Background()
	if loc, err := mem.GetLocation(ctx, did.Undef, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("blob %s has a location after its release: %+v (err=%v)", d, loc, err)
	}
	if _, err := mem.ParkByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("park row for released blob %s survived (err=%v)", d, err)
	}
	if _, err := mem.IntentByDigest(ctx, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("intent for released blob %s survived (err=%v)", d, err)
	}
}

// TestAcceptorRacesATeardown: an abort or a session expiry arrives while the
// acceptor concludes a part's blob, before the provider accepts it or after.
// The teardown waits for the acceptor's pass to end before it records the
// release, so the release finds the accept recorded and the registration in
// place, and removes the blob: a release run during the pass could send its
// remove before the upload service registers the accept, and the
// registration would outlive it.
func TestAcceptorRacesATeardown(t *testing.T) {
	teardowns := map[string]func(t *testing.T, b *Backend, key, uploadID string){
		"abort": func(t *testing.T, b *Backend, key, uploadID string) {
			bucket := "bk"
			if err := b.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: &uploadID}); err != nil {
				t.Errorf("Abort: %v", err)
			}
		},
		"expiry": func(t *testing.T, b *Backend, _, _ string) {
			if n, err := b.SweepStaleMultipartSessions(context.Background(), -time.Second); err != nil || n != 1 {
				t.Errorf("sweep: cleaned=%d err=%v, want the open session", n, err)
			}
		},
	}
	for name, teardown := range teardowns {
		for _, when := range []string{"before accept", "after accept"} {
			t.Run(name+"/"+when, func(t *testing.T) {
				shrinkAcceptBackoff(t)
				pc := &providerConcluder{}
				b, mem := newDeferredBackend(t, pc, func(d *Deps) { d.Remover = pc })
				key := "race-" + name
				uploadID := mpCreate(t, b, key, "", "")
				uploadDistinctParts(t, b, key, uploadID, 1)
				d := hygienePartDigests(t, mem, uploadID, 1)[0]

				// The teardown starts inside the conclude and gets a head start;
				// it may not finish until the pass does.
				done := make(chan struct{})
				hook := func() {
					go func() {
						defer close(done)
						teardown(t, b, key, uploadID)
					}()
					select {
					case <-done:
					case <-time.After(100 * time.Millisecond):
					}
				}
				if when == "before accept" {
					pc.before = hook
				} else {
					pc.after = hook
				}

				startAcceptors(t, b)
				drainUntilIdle(t, b, uploadID)
				<-done

				assertGone(t, mem, d)
				pc.assertNoneLeft(t)
				if pc.removed != 1 {
					t.Fatalf("accepted blob %s removed %d times, want once", d, pc.removed)
				}
			})
		}
	}
}

// TestAcceptorRacesASupersede: a part is uploaded again while the acceptor
// concludes the first upload of it, after the provider has accepted it. The
// first upload's release waits while the conclude is in flight, and runs
// from the release sweep once the accept is registered, so it removes the
// blob rather than sending a remove the registration would outlive. The
// acceptor leaves the replacement row to its own pass, which accepts it, and
// Complete commits the replacement.
func TestAcceptorRacesASupersede(t *testing.T) {
	shrinkAcceptBackoff(t)
	pc := &providerConcluder{}
	b, mem := newDeferredBackend(t, pc, func(d *Deps) { d.Remover = pc })
	key := "race-supersede"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	first := hygienePartDigests(t, mem, uploadID, 1)[0]

	replacement := testBody(int(backend.MinPartSize))
	replacement[0] = 0xff
	var etag *string
	pc.after = func() {
		out, err := mpUploadPart(t, b, key, uploadID, 1, replacement, nil)
		if err != nil {
			t.Errorf("superseding UploadPart: %v", err)
			return
		}
		etag = out.ETag
	}

	startAcceptors(t, b)
	drainUntilIdle(t, b, uploadID)
	// The superseded blob's release, deferred during the conclude.
	drainReleases(t, b)

	assertGone(t, mem, first)
	pc.mu.Lock()
	stillRegistered := pc.registered[string(first)]
	pc.mu.Unlock()
	if stillRegistered {
		t.Fatalf("superseded blob %s left registered with nothing to remove it", first)
	}
	second := hygienePartDigests(t, mem, uploadID, 1)[0]
	if states := partStates(t, mem, uploadID); states[1] != registry.PartAccepted {
		t.Fatalf("replacement part state = %q, want accepted", states[1])
	}
	assertAcceptedAndUnparked(t, mem, second)

	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: etag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
}

// meetingConcluder holds each conclude until `parties` of them are in
// flight, so two acceptors conclude the same park at once.
type meetingConcluder struct {
	parkingUploader
	met sync.WaitGroup
}

func (c *meetingConcluder) ConcludeBlobs(ctx context.Context, space did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	c.met.Done()
	c.met.Wait()
	return c.parkingUploader.ConcludeBlobs(ctx, space, parked)
}

// TestAcceptorsShareAPark: two open uploads of the space hold the same bytes,
// so their parts share one park, and both acceptors conclude it at once. A
// repeated conclude is harmless upstream; here the first record swaps the
// park for the location and the second finds the location, so both parts are
// accepted and the blob is recorded once.
func TestAcceptorsShareAPark(t *testing.T) {
	mc := &meetingConcluder{}
	mc.met.Add(2)
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, mc, func(d *Deps) { d.Remover = rm })
	ctx := context.Background()
	key := "shared-park"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	const other = "other-sharing"
	if err := mem.CreateSession(ctx, registry.MultipartSession{UploadID: other, Bucket: "bk", ObjectKey: "other"}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := mem.PutPart(ctx, registry.MultipartPart{UploadID: other, PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{d}, SpanLengths: []int64{1}, State: registry.PartParked}, nil); err != nil {
		t.Fatalf("PutPart: %v", err)
	}

	startAcceptors(t, b)
	if n, err := b.NudgeParkedUploads(ctx); err != nil || n != 2 {
		t.Fatalf("NudgeParkedUploads = %d, %v; want both uploads", n, err)
	}
	drainUntilIdle(t, b, uploadID)
	drainUntilIdle(t, b, other)

	for _, id := range []string{uploadID, other} {
		if states := partStates(t, mem, id); states[1] != registry.PartAccepted {
			t.Fatalf("upload %s part state = %q, want accepted", id, states[1])
		}
	}
	assertAcceptedAndUnparked(t, mem, d)
	if n := rm.removedDigests()[string(d)]; n != 0 {
		t.Fatalf("shared blob %s removed %d times, want never", d, n)
	}
}

// TestAcceptorLeavesAReplacementPark: the provider accepts a part's blob,
// the park the acceptor concluded is taken from under it (a release run
// after a pass timed out with its conclude still in flight), and another
// write of the same bytes parks them again under its own allocation before
// the acceptor records the accept. The acceptor's record names the
// allocation it concluded, so it does not take the replacement park; the
// next pass concludes that park on its own allocation.
func TestAcceptorLeavesAReplacementPark(t *testing.T) {
	shrinkAcceptBackoff(t)
	pc := &providerConcluder{}
	b, mem := newDeferredBackend(t, pc, func(d *Deps) { d.Remover = pc })
	ctx := context.Background()
	key := "race-replacement-park"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	h, err := multihash.Sum([]byte("replacement allocation"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	replacement := cid.NewCidV1(cid.Raw, h)
	pc.after = func() {
		if err := mem.DeletePark(ctx, did.Undef, d); err != nil {
			t.Errorf("DeletePark: %v", err)
		}
		if ok, err := mem.PutPark(ctx, registry.BlobPark{Space: did.Undef, Digest: d, AddTask: replacement.Bytes(), AcceptTask: replacement.Bytes(), PutInvocation: []byte{1}, Size: 1}); err != nil || !ok {
			t.Errorf("PutPark (replacement) = %v, %v", ok, err)
		}
	}

	startAcceptors(t, b)
	drainUntilIdle(t, b, uploadID)

	pc.mu.Lock()
	concluded := slices.Clone(pc.concluded)
	pc.mu.Unlock()
	if !slices.Contains(concluded, replacement) {
		t.Fatalf("concluded add tasks = %v; the replacement park was never concluded on its own allocation", concluded)
	}
	assertAcceptedAndUnparked(t, mem, d)
}

// TestAcceptorLeavesAReleasingBlobForComplete: a parked part whose blob has a
// release record pending (the same bytes stored as received by an object
// whose delete is behind its reader grace) is not the acceptor's to take
// over. The record stays, the part goes back to spooled, and Complete settles
// it the way every committing write does.
func TestAcceptorLeavesAReleasingBlobForComplete(t *testing.T) {
	cc := &countingConcluder{}
	b, mem := newDeferredBackend(t, cc)
	ctx := context.Background()
	key := "accept-releasing"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 1)
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	if err := mem.EnqueueRelease(ctx, did.Undef, d, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("EnqueueRelease: %v", err)
	}

	startAcceptors(t, b)
	if err := b.drainAcceptor(ctx, uploadID); err != nil {
		t.Fatalf("drainAcceptor: %v", err)
	}
	if got, err := mem.ReleasingDigests(ctx, did.Undef, []multihash.Multihash{d}); err != nil || !got[string(d)] {
		t.Fatalf("release record after the acceptor's pass = %v, %v; want it kept", got, err)
	}
	if states := partStates(t, mem, uploadID); states[1] != registry.PartSpooled {
		t.Fatalf("part state = %q, want spooled", states[1])
	}
	if got := cc.concluded(); len(got) != 0 {
		t.Fatalf("conclude calls = %v, want none", got)
	}

	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	assertAcceptedAndUnparked(t, mem, d)
}

// TestAcceptorRacesASupersedesRelease: a part is uploaded again, and the
// acceptor makes a pass after the new upload has recorded the old blob's
// release and before its part row lands. The acceptor leaves the old blob to
// that release, which then aborts it: taking the record over would accept a
// blob no part or object names any more, and nothing would release it.
func TestAcceptorRacesASupersedesRelease(t *testing.T) {
	pc := &providerConcluder{}
	store := &teardownBeforePutPart{}
	b, mem := newDeferredBackend(t, pc, func(d *Deps) {
		store.MultipartStore = d.Multipart
		d.Multipart = store
	})
	ctx := context.Background()
	key := "race-supersede-release"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	old := hygienePartDigests(t, mem, uploadID, 1)[0]

	startAcceptors(t, b)
	store.hook = func() {
		if err := b.drainAcceptor(ctx, uploadID); err != nil {
			t.Errorf("drainAcceptor: %v", err)
		}
	}
	replacement := testBody(int(backend.MinPartSize))
	replacement[0] = 0xee
	if _, err := mpUploadPart(t, b, key, uploadID, 1, replacement, nil); err != nil {
		t.Fatalf("superseding UploadPart: %v", err)
	}
	if store.hook != nil {
		t.Fatal("the hook never ran")
	}

	assertGone(t, mem, old)
	if !pc.abortedDigests()[string(old)] {
		t.Fatalf("superseded blob %s was not aborted", old)
	}
	if got, _ := mem.ReleasingDigests(ctx, did.Undef, []multihash.Multihash{old}); got[string(old)] {
		t.Fatalf("release record for %s left behind", old)
	}
}

// uploadCountingParker is a parking uploader that counts UploadBlob calls:
// each is one allocation on the provider.
type uploadCountingParker struct {
	parkingUploader
	uploads atomic.Int32
}

func (u *uploadCountingParker) UploadBlob(ctx context.Context, space did.DID, digest multihash.Multihash, size int64, path string, opts ...uploader.UploadOption) (uploader.UploadedBlob, error) {
	u.uploads.Add(1)
	return u.parkingUploader.UploadBlob(ctx, space, digest, size, path, opts...)
}

// parkNotYetVisible is a park store whose next lookup of digest misses, as
// when Complete classifies a blob its UploadPart is still parking.
type parkNotYetVisible struct {
	registry.ParkStore
	mu     sync.Mutex
	digest multihash.Multihash
}

func (p *parkNotYetVisible) GetPark(ctx context.Context, space did.DID, digest multihash.Multihash) (*registry.BlobPark, error) {
	p.mu.Lock()
	miss := p.digest != nil && bytes.Equal(p.digest, digest)
	if miss {
		p.digest = nil
	}
	p.mu.Unlock()
	if miss {
		return nil, registry.ErrNotFound
	}
	return p.ParkStore.GetPark(ctx, space, digest)
}

// TestCompleteConcludesAParkThatLandsMidComplete: Complete finds a spooled
// part's blob with no park, as when its UploadPart is still parking, and
// falls back to uploading it. Under the blob's lock the park is there after
// all, and Complete concludes it rather than allocating a second copy beside
// it, whose park would then never be concluded or aborted.
func TestCompleteConcludesAParkThatLandsMidComplete(t *testing.T) {
	up := &uploadCountingParker{}
	parts := &failOnceMoveToParked{}
	parks := &parkNotYetVisible{}
	b, mem := newDeferredBackend(t, up, func(d *Deps) {
		parts.MultipartStore = d.Multipart
		d.Multipart = parts
		parks.ParkStore = d.Parks
		d.Parks = parks
	})
	key := "park-lands-mid-complete"
	uploadID := mpCreate(t, b, key, "", "")

	parts.mu.Lock()
	parts.armed = true
	parts.mu.Unlock()
	body := testBody(int(backend.MinPartSize))
	if _, err := mpUploadPart(t, b, key, uploadID, 1, body, nil); err == nil {
		t.Fatal("UploadPart succeeded although its part could not be queued")
	}
	part := partRecord(t, mem, uploadID, 1)
	if part.State != registry.PartSpooled {
		t.Fatalf("part state = %q, want spooled", part.State)
	}
	d := part.BlobDigests[0]
	if _, err := mem.ParkByDigest(context.Background(), d); err != nil {
		t.Fatalf("the part's park: %v", err)
	}
	allocations := up.uploads.Load()

	parks.mu.Lock()
	parks.digest = d
	parks.mu.Unlock()
	etag := `"` + hex.EncodeToString(part.ETagMD5) + `"`
	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: &etag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if n := up.uploads.Load(); n != allocations {
		t.Fatalf("Complete allocated %d more copies of a parked blob, want none", n-allocations)
	}
	assertAcceptedAndUnparked(t, mem, d)
}

// locationThenRelease is a location store whose next lookup of digest finds
// the location and then runs hook before answering: the window between the
// acceptor reading a blob as located and acting on it.
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

// TestAcceptorKeepsAParkThatReplacesALocation: the acceptor finds a part's
// blob located, and before it acts a release removes the location (and the
// park left beside it) and another write of the same bytes parks them again.
// The acceptor rechecks under the blob's lock, finds no location, and leaves
// the replacement park alone and the part to Complete; dropping the park
// would leave the replacement's allocation never concluded or aborted.
func TestAcceptorKeepsAParkThatReplacesALocation(t *testing.T) {
	cc := &countingConcluder{}
	locs := &locationThenRelease{}
	b, mem := newDeferredBackend(t, cc, func(d *Deps) {
		locs.LocationStore = d.Locations
		d.Locations = locs
	})
	ctx := context.Background()
	key := "located-then-released"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	// Another conclude recorded the blob, its park left beside the location.
	if err := mem.PutLocation(ctx, registry.BlobLocation{Space: did.Undef, Digest: d, Provider: "did:key:zNode", URL: "https://node.example/blob", Size: 1}); err != nil {
		t.Fatalf("PutLocation: %v", err)
	}
	h, err := multihash.Sum([]byte("replacing allocation"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	replacement := cid.NewCidV1(cid.Raw, h)
	locs.mu.Lock()
	locs.digest = d
	locs.hook = func() {
		_ = mem.DeleteLocation(ctx, did.Undef, d)
		_ = mem.DeletePark(ctx, did.Undef, d)
		if ok, err := mem.PutPark(ctx, registry.BlobPark{Space: did.Undef, Digest: d, AddTask: replacement.Bytes(), AcceptTask: replacement.Bytes(), PutInvocation: []byte{1}, Size: 1}); err != nil || !ok {
			t.Errorf("PutPark (replacement) = %v, %v", ok, err)
		}
	}
	locs.mu.Unlock()

	startAcceptors(t, b)
	if err := b.drainAcceptor(ctx, uploadID); err != nil {
		t.Fatalf("drainAcceptor: %v", err)
	}
	park, err := mem.GetPark(ctx, did.Undef, d)
	if err != nil {
		t.Fatalf("replacement park dropped by a stale located path: %v", err)
	}
	if !bytes.Equal(park.AddTask, replacement.Bytes()) {
		t.Fatalf("park add task = %x, want the replacement's", park.AddTask)
	}
	if states := partStates(t, mem, uploadID); states[1] != registry.PartSpooled {
		t.Fatalf("part state = %q, want spooled (left for Complete)", states[1])
	}
}
