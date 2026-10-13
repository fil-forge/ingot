package s3frontend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/registry"
)

// TestUploadPart_FreshPartsDoNotListTheSession: writing a part reads only
// the part it would supersede, so an upload's cost per part does not grow
// with the parts already uploaded.
func TestUploadPart_FreshPartsDoNotListTheSession(t *testing.T) {
	counting := &countingListParts{}
	b, _ := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		counting.MultipartStore = d.Multipart
		d.Multipart = counting
	})
	key := "fresh-parts"

	uploadID := mpCreate(t, b, key, "", "")
	for n := int32(1); n <= 3; n++ {
		if _, err := mpUploadPart(t, b, key, uploadID, n, testBody(int(backend.MinPartSize)+int(n)), nil); err != nil {
			t.Fatalf("UploadPart %d: %v", n, err)
		}
	}
	if got := counting.count(); got != 0 {
		t.Fatalf("ListParts calls = %d, want 0", got)
	}
}

// TestUploadPart_SupersedeKeepsADigestASiblingNames: in a session storing
// parts as received, two parts with the same bytes share their digests.
// Re-uploading one of them records no release for the shared digests,
// which the other part still names.
func TestUploadPart_SupersedeKeepsADigestASiblingNames(t *testing.T) {
	b, mem, _ := newParkingBackend(t)
	ctx := context.Background()
	b.allowNoneEncryption = true
	if err := b.PutBucketEncryption(ctx, "bk", sseConfig(sseNone)); err != nil {
		t.Fatalf("PutBucketEncryption none: %v", err)
	}
	key := "shared-part"
	shared := testBody(int(backend.MinPartSize))

	uploadID := mpCreate(t, b, key, "", "")
	for n := int32(1); n <= 2; n++ {
		if _, err := mpUploadPart(t, b, key, uploadID, n, shared, nil); err != nil {
			t.Fatalf("UploadPart %d: %v", n, err)
		}
	}
	sharedDigests := hygienePartDigests(t, mem, uploadID, 1)
	if _, err := mpUploadPart(t, b, key, uploadID, 2, testBody(int(backend.MinPartSize)+100), nil); err != nil {
		t.Fatalf("UploadPart 2 (re-upload): %v", err)
	}

	releases, err := mem.ListReleasesBySpace(ctx, did.Undef)
	if err != nil {
		t.Fatalf("ListReleasesBySpace: %v", err)
	}
	keep := map[string]bool{}
	for _, d := range sharedDigests {
		keep[string(d)] = true
	}
	var recorded []multihash.Multihash
	for _, r := range releases {
		if keep[string(r.Digest)] {
			recorded = append(recorded, r.Digest)
		}
	}
	if len(recorded) != 0 {
		t.Fatalf("releases recorded for digests part 1 still names: %x", recorded)
	}
}

// TestUploadPart_FailedRecordReleasesDespiteFailedSiblingLookup: when the
// part row cannot be written and the sibling lookup fails too, the spooled
// blobs' releases are still recorded, and a session found gone still
// reports NoSuchUpload.
func TestUploadPart_FailedRecordReleasesDespiteFailedSiblingLookup(t *testing.T) {
	failing := &failRecordAndSiblings{}
	recording := &recordingEnqueue{}
	b, _ := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		failing.MultipartStore = d.Multipart
		d.Multipart = failing
		recording.PendingReleaseStore = d.PendingReleases
		d.PendingReleases = recording
	})
	key := "failed-record"

	uploadID := mpCreate(t, b, key, "", "")
	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)

	t.Run("reports NoSuchUpload", func(t *testing.T) {
		var apiErr s3err.APIError
		if !errors.As(err, &apiErr) || apiErr.Code != "NoSuchUpload" {
			t.Fatalf("UploadPart err = %v, want NoSuchUpload", err)
		}
	})
	t.Run("records the spooled blobs' releases", func(t *testing.T) {
		if got := recording.digests(); len(got) == 0 {
			t.Fatal("no release recorded for the spooled blobs")
		}
	})
}

// countingListParts counts ListParts calls.
type countingListParts struct {
	registry.MultipartStore
	mu    sync.Mutex
	lists int
}

func (c *countingListParts) ListParts(ctx context.Context, uploadID string) ([]registry.MultipartPart, error) {
	c.mu.Lock()
	c.lists++
	c.mu.Unlock()
	return c.MultipartStore.ListParts(ctx, uploadID)
}

func (c *countingListParts) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lists
}

// failRecordAndSiblings reports every part write as finding its session
// gone, and fails every sibling lookup.
type failRecordAndSiblings struct {
	registry.MultipartStore
}

func (f *failRecordAndSiblings) PutPart(context.Context, registry.MultipartPart) error {
	return registry.ErrNotFound
}

func (f *failRecordAndSiblings) SiblingPartDigests(context.Context, string, int, []multihash.Multihash) ([]multihash.Multihash, error) {
	return nil, errors.New("sibling lookup unavailable")
}

// recordingEnqueue records the digests of every EnqueueReleases call.
type recordingEnqueue struct {
	registry.PendingReleaseStore
	mu       sync.Mutex
	enqueued []multihash.Multihash
}

func (r *recordingEnqueue) EnqueueReleases(ctx context.Context, space did.DID, digests []multihash.Multihash, notBefore time.Time) ([]registry.PendingRelease, error) {
	r.mu.Lock()
	r.enqueued = append(r.enqueued, digests...)
	r.mu.Unlock()
	return r.PendingReleaseStore.EnqueueReleases(ctx, space, digests, notBefore)
}

func (r *recordingEnqueue) digests() []multihash.Multihash {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]multihash.Multihash(nil), r.enqueued...)
}
