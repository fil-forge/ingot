package s3frontend

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// batchRecordingUploader is a parking uploader that records how many blobs
// each conclude carried, so a test can tell one call carrying every blob from
// a call per blob.
type batchRecordingUploader struct {
	inmem.NopUploader
	mu sync.Mutex
	// calls holds the blob count of each ConcludeBlobs call, in order.
	calls []int
}

func (u *batchRecordingUploader) UploadBlob(_ context.Context, _ did.DID, digest multihash.Multihash, size int64, _ string, _ ...uploader.UploadOption) (uploader.UploadedBlob, error) {
	// No Location: the blob parks, which is what UploadPart does.
	c := cidOfDigest(digest)
	return uploader.UploadedBlob{Digest: digest, Size: size, AddTask: c, AcceptTask: c}, nil
}

func (u *batchRecordingUploader) ConcludeBlobs(ctx context.Context, space did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	u.mu.Lock()
	u.calls = append(u.calls, len(parked))
	u.mu.Unlock()
	return u.NopUploader.ConcludeBlobs(ctx, space, parked)
}

// TestCompleteConcludesPartsInOneCall is the batching gate on the completion
// path: a multipart upload of many parts must conclude them all in a single
// call, because each call is a round trip to the upload service and a
// completion that pays one per part is what times out on large objects.
func TestCompleteConcludesPartsInOneCall(t *testing.T) {
	const parts = 5

	b, mem, up := newBatchRecordingBackend(t)
	ctx := context.Background()
	key := "manyparts"

	uploadID := mpCreate(t, b, key, "", "")
	var completed []types.CompletedPart
	var digests []multihash.Multihash
	for i := 1; i <= parts; i++ {
		n := int32(i)
		// Distinct bodies: identical parts would share one spooled blob and
		// collapse the batch.
		body := append(testBody(int(backend.MinPartSize)), byte(i))
		out, err := mpUploadPart(t, b, key, uploadID, n, body, nil)
		if err != nil {
			t.Fatalf("UploadPart %d: %v", n, err)
		}
		completed = append(completed, types.CompletedPart{PartNumber: &n, ETag: out.ETag})
		digests = append(digests, hygienePartDigests(t, mem, uploadID, i)...)
	}

	if _, err := mpComplete(t, b, key, uploadID, completed, nil); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	up.mu.Lock()
	calls := append([]int{}, up.calls...)
	up.mu.Unlock()

	if len(calls) != 1 {
		t.Fatalf("Complete made %d ConcludeBlobs calls (%v), want 1", len(calls), calls)
	}
	if calls[0] != len(digests) {
		t.Errorf("ConcludeBlobs carried %d blobs, want all %d", calls[0], len(digests))
	}

	// Every part blob is accepted, and its park is gone.
	for _, d := range digests {
		in, err := mem.GetIntent(ctx, d)
		if err != nil {
			t.Fatalf("intent for %x: %v", d, err)
		}
		if in.State != registry.IntentAccepted {
			t.Errorf("blob %x intent = %v, want accepted", d, in.State)
		}
		if _, err := mem.GetPark(ctx, d); err == nil {
			t.Errorf("park row for %x survived the conclude", d)
		}
	}
}

func newBatchRecordingBackend(t *testing.T) (*Backend, *inmem.MemStore, *batchRecordingUploader) {
	t.Helper()
	up := &batchRecordingUploader{}
	b, mem := newDeferredBackend(t, up)
	return b, mem, up
}

func cidOfDigest(d multihash.Multihash) cid.Cid { return cid.NewCidV1(cid.Raw, d) }
