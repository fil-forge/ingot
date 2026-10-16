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

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// partRecord returns uploadID's part n as recorded.
func partRecord(t *testing.T, mem *inmem.MemStore, uploadID string, n int) registry.MultipartPart {
	t.Helper()
	parts, err := mem.ListParts(context.Background(), uploadID)
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	for _, p := range parts {
		if p.PartNumber == n {
			return p
		}
	}
	t.Fatalf("part %d not recorded for upload %s", n, uploadID)
	return registry.MultipartPart{}
}

// TestParkedPartHoldsNoLocalCopy: once UploadPart parks a part's blobs, their
// spool copies are gone, the part is queued for acceptance with each blob's
// plaintext length, and Complete concludes them from the park rows alone.
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
	part := partRecord(t, mem, uploadID, 1)
	if part.State != registry.PartParked {
		t.Fatalf("part state = %q, want parked", part.State)
	}
	var total int64
	for _, n := range part.BlobSizes {
		total += n
	}
	if len(part.BlobSizes) != len(digests) || total != backend.MinPartSize {
		t.Fatalf("part blob sizes = %v, want one per blob summing to %d", part.BlobSizes, backend.MinPartSize)
	}
	for _, d := range digests {
		if _, err := os.Stat(localPath(b, d)); !os.IsNotExist(err) {
			t.Fatalf("parked blob %s spool copy: stat err=%v, want not-exist", d, err)
		}
		if in, err := mem.IntentByDigest(ctx, d); err != nil || in.State != registry.IntentParked {
			t.Fatalf("parked blob %s intent = %v/%v, want parked", d, in, err)
		}
		if _, err := mem.ParkByDigest(ctx, d); err != nil {
			t.Fatalf("parked blob %s park row: %v", d, err)
		}
		if !mem.IsEvicted(d) {
			t.Fatalf("parked blob %s intent not marked evicted", d)
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

// TestFailedParkKeepsLocalCopy: a part whose park failed stays spooled, out of
// the acceptance queue, and keeps its spool copy, the only one, for Complete's
// never-parked fallback.
func TestFailedParkKeepsLocalCopy(t *testing.T) {
	b, mem := newDeferredBackend(t, &failingParker{})
	ctx := context.Background()
	key := "park-fails"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err == nil {
		t.Fatalf("UploadPart succeeded, want the park failure")
	}
	if part := partRecord(t, mem, uploadID, 1); part.State != registry.PartSpooled {
		t.Fatalf("part state after a failed park = %q, want spooled", part.State)
	}
	for _, d := range hygienePartDigests(t, mem, uploadID, 1) {
		if _, err := os.Stat(localPath(b, d)); err != nil {
			t.Fatalf("blob %s spool copy after a failed park: %v, want it kept", d, err)
		}
		if in, err := mem.IntentByDigest(ctx, d); err != nil || in.State != registry.IntentUploading {
			t.Fatalf("blob %s intent = %v/%v, want uploading", d, in, err)
		}
	}
}
