package s3frontend

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/registry"
)

// TestUploadPart_FreshPartsDoNotScanTheSession: writing a part reads only
// the part it would supersede, so an upload's cost per part does not grow
// with the parts already uploaded. Both ListParts and SiblingPartDigests
// read across the session's parts, so a fresh part calls neither.
func TestUploadPart_FreshPartsDoNotScanTheSession(t *testing.T) {
	counting := &countingPartScans{}
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

	t.Run("does not list the session", func(t *testing.T) {
		if got := counting.listCalls(); got != 0 {
			t.Fatalf("ListParts calls = %d, want 0", got)
		}
	})
	t.Run("does not look up sibling parts", func(t *testing.T) {
		if got := counting.siblingCalls(); got != 0 {
			t.Fatalf("SiblingPartDigests calls = %d, want 0", got)
		}
	})
}

// TestUploadPart_FailedSupersedeLookupFailsTheUpload: when the registry
// cannot say which part a write supersedes, or which of the superseded
// blobs a sibling part still names, the upload fails. Going on would
// overwrite the part row without recording the superseded blobs' releases.
func TestUploadPart_FailedSupersedeLookupFailsTheUpload(t *testing.T) {
	cases := []struct {
		desc string
		arm  func(*failingPartLookups)
	}{
		{"GetPart fails", func(f *failingPartLookups) { f.failGetPart.Store(true) }},
		{"the sibling lookup fails", func(f *failingPartLookups) { f.failSiblings.Store(true) }},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			failing := &failingPartLookups{}
			b, _ := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
				failing.MultipartStore = d.Multipart
				d.Multipart = failing
			})
			key := "failed-lookup"

			uploadID := mpCreate(t, b, key, "", "")
			if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
				t.Fatalf("UploadPart 1: %v", err)
			}
			tc.arm(failing)
			_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)+1), nil)
			if !errors.Is(err, errPartLookupUnavailable) {
				t.Fatalf("UploadPart 1 (re-upload) err = %v, want %v", err, errPartLookupUnavailable)
			}
		})
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
		t.Fatalf("releases recorded for digests part 1 still names: %v", recorded)
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

// countingPartScans counts the calls that read across a session's parts:
// ListParts and SiblingPartDigests.
type countingPartScans struct {
	registry.MultipartStore
	mu       sync.Mutex
	lists    int
	siblings int
}

func (c *countingPartScans) ListParts(ctx context.Context, uploadID string) ([]registry.MultipartPart, error) {
	c.mu.Lock()
	c.lists++
	c.mu.Unlock()
	return c.MultipartStore.ListParts(ctx, uploadID)
}

func (c *countingPartScans) SiblingPartDigests(ctx context.Context, uploadID string, partNumber int, digests []multihash.Multihash) ([]multihash.Multihash, error) {
	c.mu.Lock()
	c.siblings++
	c.mu.Unlock()
	return c.MultipartStore.SiblingPartDigests(ctx, uploadID, partNumber, digests)
}

func (c *countingPartScans) listCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lists
}

func (c *countingPartScans) siblingCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.siblings
}

var errPartLookupUnavailable = errors.New("part lookup unavailable")

// failingPartLookups fails GetPart or SiblingPartDigests once armed.
type failingPartLookups struct {
	registry.MultipartStore
	failGetPart  atomic.Bool
	failSiblings atomic.Bool
}

func (f *failingPartLookups) GetPart(ctx context.Context, uploadID string, partNumber int) (*registry.MultipartPart, error) {
	if f.failGetPart.Load() {
		return nil, errPartLookupUnavailable
	}
	return f.MultipartStore.GetPart(ctx, uploadID, partNumber)
}

func (f *failingPartLookups) SiblingPartDigests(ctx context.Context, uploadID string, partNumber int, digests []multihash.Multihash) ([]multihash.Multihash, error) {
	if f.failSiblings.Load() {
		return nil, errPartLookupUnavailable
	}
	return f.MultipartStore.SiblingPartDigests(ctx, uploadID, partNumber, digests)
}

// failRecordAndSiblings reports every part write as finding its session
// gone, and fails every sibling lookup.
type failRecordAndSiblings struct {
	registry.MultipartStore
}

func (f *failRecordAndSiblings) PutPart(context.Context, registry.MultipartPart, *registry.PartTree) error {
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
