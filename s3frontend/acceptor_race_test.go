package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// providerConcluder concludes as a provider does: a blob whose allocation
// was aborted is refused, any other is accepted, and an abort of an accepted
// blob is then refused as already accepted (parkingUploader.AbortBlob). The
// before hook runs once ahead of the next conclude, the after hook once
// between its accepts and its reply: the two orders a release can take
// against an acceptor's conclude.
type providerConcluder struct {
	parkingUploader
	before, after func()
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
		locations[i] = &uploader.BlobLocation{Provider: "did:key:zNode", URL: "https://node.example/blob", Size: p.Size}
	}
	c.mu.Unlock()
	c.runOnce(&c.after)
	return locations, errors.Join(refused...)
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

// TestAcceptorRacesATeardown: an abort or a session expiry releases a part's
// blob while the acceptor concludes it. Whichever order the release and the
// accept take, the blob ends released on the provider exactly as its state
// requires, and the acceptor records no location for it: a location written
// after the release would let a later write of the same bytes reuse a blob
// that is gone.
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
		t.Run(name+"/release first", func(t *testing.T) {
			// The release aborts the allocation; the provider then refuses
			// the accept, so there is nothing to record.
			shrinkAcceptBackoff(t)
			pc := &providerConcluder{}
			rm := &recordingRemover{}
			b, mem := newDeferredBackend(t, pc, func(d *Deps) { d.Remover = rm })
			key := "race-" + name + "-release-first"
			uploadID := mpCreate(t, b, key, "", "")
			uploadDistinctParts(t, b, key, uploadID, 1)
			d := hygienePartDigests(t, mem, uploadID, 1)[0]
			pc.before = func() { teardown(t, b, key, uploadID) }

			startAcceptors(t, b)
			drainUntilIdle(t, b, uploadID)

			assertGone(t, mem, d)
			if !pc.abortedDigests()[string(d)] {
				t.Fatalf("blob %s was not aborted on the provider", d)
			}
			if n := rm.removedDigests()[string(d)]; n != 0 {
				t.Fatalf("blob %s removed %d times; its allocation was aborted before any accept", d, n)
			}
		})
		t.Run(name+"/accept first", func(t *testing.T) {
			// The provider accepts, then the release finds the park and its
			// abort is refused as accepted, so it removes the blob instead.
			// The acceptor's record comes after, finds the park gone, and
			// writes nothing.
			shrinkAcceptBackoff(t)
			pc := &providerConcluder{}
			rm := &recordingRemover{}
			b, mem := newDeferredBackend(t, pc, func(d *Deps) { d.Remover = rm })
			key := "race-" + name + "-accept-first"
			uploadID := mpCreate(t, b, key, "", "")
			uploadDistinctParts(t, b, key, uploadID, 1)
			d := hygienePartDigests(t, mem, uploadID, 1)[0]
			pc.after = func() { teardown(t, b, key, uploadID) }

			startAcceptors(t, b)
			drainUntilIdle(t, b, uploadID)

			assertGone(t, mem, d)
			if n := rm.removedDigests()[string(d)]; n != 1 {
				t.Fatalf("accepted blob %s removed %d times, want once", d, n)
			}
		})
	}
}

// TestAcceptorRacesASupersede: a part is uploaded again while the acceptor
// concludes the first upload of it. The first upload's blob is released and
// gets no location; the acceptor leaves the replacement row to its own pass,
// which accepts it, and Complete commits the replacement.
func TestAcceptorRacesASupersede(t *testing.T) {
	shrinkAcceptBackoff(t)
	pc := &providerConcluder{}
	rm := &recordingRemover{}
	b, mem := newDeferredBackend(t, pc, func(d *Deps) { d.Remover = rm })
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

	assertGone(t, mem, first)
	if n := rm.removedDigests()[string(first)]; n != 1 {
		t.Fatalf("superseded blob %s removed %d times, want once", first, n)
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
	if err := mem.PutPart(ctx, registry.MultipartPart{UploadID: other, PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{d}, BlobSizes: []int64{1}, State: registry.PartParked}, nil); err != nil {
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
