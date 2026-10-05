package s3frontend

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// TestParkedPartHoldsNoLocalCopy: once UploadPart parks a part's blobs, their
// spool copies are gone, and Complete concludes them from the park rows
// alone.
func TestParkedPartHoldsNoLocalCopy(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	key := "parked"

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	digests := hygienePartDigests(t, mem, uploadID, 1)
	for _, d := range digests {
		if _, err := os.Stat(b.spool.Path(d)); !os.IsNotExist(err) {
			t.Fatalf("parked blob %s spool copy: stat err=%v, want not-exist", d, err)
		}
		if in, err := mem.GetIntent(ctx, d); err != nil || in.State != registry.IntentParked {
			t.Fatalf("parked blob %s intent = %v/%v, want parked", d, in, err)
		}
		if _, err := mem.GetPark(ctx, d); err != nil {
			t.Fatalf("parked blob %s park row: %v", d, err)
		}
	}

	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	for _, d := range digests {
		if loc, err := mem.GetLocation(ctx, did.Undef, d); err != nil || loc == nil {
			t.Fatalf("blob %s location after Complete = %v/%v, want recorded", d, loc, err)
		}
	}
}

// failingParker fails every upload, as a provider that is down does.
type failingParker struct{ parkingUploader }

func (*failingParker) UploadBlob(context.Context, did.DID, multihash.Multihash, int64, string, ...uploader.UploadOption) (uploader.UploadedBlob, error) {
	return uploader.UploadedBlob{}, errors.New("provider unreachable")
}

// TestFailedParkKeepsLocalCopy: a part whose park failed keeps its spool copy,
// the only one, for Complete's never-parked fallback.
func TestFailedParkKeepsLocalCopy(t *testing.T) {
	b, mem := newDeferredBackend(t, &failingParker{})
	ctx := context.Background()
	key := "park-fails"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err == nil {
		t.Fatalf("UploadPart succeeded, want the park failure")
	}
	for _, d := range hygienePartDigests(t, mem, uploadID, 1) {
		if _, err := os.Stat(b.spool.Path(d)); err != nil {
			t.Fatalf("blob %s spool copy after a failed park: %v, want it kept", d, err)
		}
		if in, err := mem.GetIntent(ctx, d); err != nil || in.State != registry.IntentUploading {
			t.Fatalf("blob %s intent = %v/%v, want uploading", d, in, err)
		}
	}
}

// pathOpeningParker parks like parkingUploader, but its full upload (Complete's
// never-parked fallback) opens the spool file, as the real uploader does.
type pathOpeningParker struct{ parkingUploader }

func (p *pathOpeningParker) UploadBlob(ctx context.Context, space did.DID, digest multihash.Multihash, size int64, path string, opts ...uploader.UploadOption) (uploader.UploadedBlob, error) {
	if _, err := os.Stat(path); err != nil {
		return uploader.UploadedBlob{}, err
	}
	return p.parkingUploader.UploadBlob(ctx, space, digest, size, path, opts...)
}

// TestCompleteRacingParkConverges: a Complete that runs while a part is still
// parking finds no park row and falls back to uploading the spool copy, which
// the park then drops. That Complete fails, the session goes back to open, and
// a retried Complete concludes the part from its park row.
func TestCompleteRacingParkConverges(t *testing.T) {
	b, mem := newDeferredBackend(t, &pathOpeningParker{})
	ctx := t.Context()
	key := "racing-park"

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	d := hygienePartDigests(t, mem, uploadID, 1)[0]
	// Rewind to the window the race needs: the part's copy already dropped,
	// its park row not yet visible to Complete.
	park, err := mem.GetPark(ctx, d)
	if err != nil {
		t.Fatalf("GetPark: %v", err)
	}
	if err := mem.DeletePark(ctx, d); err != nil {
		t.Fatalf("DeletePark: %v", err)
	}

	one := int32(1)
	parts := []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err == nil {
		t.Fatalf("Complete succeeded with neither a park row nor a spool copy")
	}
	if sess, err := mem.GetSession(ctx, uploadID); err != nil || sess.State != registry.SessionOpen {
		t.Fatalf("session after the failed Complete = %v/%v, want open for the retry", sess, err)
	}

	if err := mem.PutPark(ctx, *park); err != nil {
		t.Fatalf("PutPark: %v", err)
	}
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("retried Complete: %v", err)
	}
	if loc, err := mem.GetLocation(ctx, did.Undef, d); err != nil || loc == nil {
		t.Fatalf("part location after the retry = %v/%v, want recorded", loc, err)
	}
}
