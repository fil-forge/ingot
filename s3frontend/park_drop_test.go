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
			t.Fatalf("parked blob %x spool copy: stat err=%v, want not-exist", d, err)
		}
		if in, err := mem.GetIntent(ctx, d); err != nil || in.State != registry.IntentParked {
			t.Fatalf("parked blob %x intent = %v/%v, want parked", d, in, err)
		}
		if _, err := mem.GetPark(ctx, d); err != nil {
			t.Fatalf("parked blob %x park row: %v", d, err)
		}
	}

	one := int32(1)
	if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	for _, d := range digests {
		if loc, err := mem.GetLocation(ctx, did.Undef, d); err != nil || loc == nil {
			t.Fatalf("blob %x location after Complete = %v/%v, want recorded", d, loc, err)
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
			t.Fatalf("blob %x spool copy after a failed park: %v, want it kept", d, err)
		}
		if in, err := mem.GetIntent(ctx, d); err != nil || in.State != registry.IntentUploading {
			t.Fatalf("blob %x intent = %v/%v, want uploading", d, in, err)
		}
	}
}
