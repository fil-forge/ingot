package s3frontend

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// batchRecordingUploader is a parking uploader that records how many blobs
// each conclude carried.
type batchRecordingUploader struct {
	testfakes.NopUploader
	mu sync.Mutex
	// calls holds the blob count of each ConcludeBlobs call, in order.
	calls []int
}

func (u *batchRecordingUploader) UploadBlob(_ context.Context, _ did.DID, digest multihash.Multihash, size int64, _ string) (uploader.UploadedBlob, error) {
	// No Location: the blob parks, which is what UploadPart does.
	c := cidOfDigest(digest)
	return uploader.UploadedBlob{Digest: digest, Size: size, AddTask: c, AcceptTask: c, PutInvocation: []byte("put")}, nil
}

func (u *batchRecordingUploader) ConcludeBlobs(ctx context.Context, space did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	u.mu.Lock()
	u.calls = append(u.calls, len(parked))
	u.mu.Unlock()
	return u.NopUploader.ConcludeBlobs(ctx, space, parked)
}

// TestCompleteConcludesEachPartBlobOnce: Complete of an upload whose parts
// were not yet accepted concludes each part's blob once, on its own, under
// the blob's subject.
func TestCompleteConcludesEachPartBlobOnce(t *testing.T) {
	const parts = 5

	b, mem, up := newBatchRecordingBackend(t)
	ctx := context.Background()
	key := "manyparts"

	uploadID := mpCreate(t, b, key, "", "")
	var completed []types.CompletedPart
	var digests []multihash.Multihash
	for i := 1; i <= parts; i++ {
		n := int32(i)
		// Distinct bodies: identical parts would share one stored blob and
		// collapse the batch.
		body := append(testBody(int(backend.MinPartSize)), byte(i))
		out, err := mpUploadPart(t, b, key, uploadID, n, body, nil)
		require.NoError(t, err, "UploadPart %d", n)
		completed = append(completed, types.CompletedPart{PartNumber: &n, ETag: out.ETag})
		digests = append(digests, hygienePartDigests(t, mem, uploadID, i)...)
	}

	_, err := mpComplete(t, b, key, uploadID, completed, nil)
	require.NoError(t, err, "CompleteMultipartUpload")

	up.mu.Lock()
	calls := append([]int{}, up.calls...)
	up.mu.Unlock()

	require.Len(t, calls, len(digests), "Complete made %d ConcludeBlobs calls (%v)", len(calls), calls)
	for _, n := range calls {
		require.Equal(t, 1, n, "ConcludeBlobs calls")
	}

	// Every part blob is accepted, and its park is gone.
	for _, d := range digests {
		in, err := mem.IntentByDigest(ctx, d)
		require.NoError(t, err, "intent for %x", d)
		// Committed by the Complete: the claim published the intent.
		assert.Equal(t, registry.IntentPublished, in.State, "blob %x intent", d)
		_, err = mem.ParkByDigest(ctx, d)
		assert.Error(t, err, "park row for %x survived the conclude", d)
	}
}

func newBatchRecordingBackend(t *testing.T) (*Backend, *testStore, *batchRecordingUploader) {
	t.Helper()
	up := &batchRecordingUploader{}
	b, mem := newDeferredBackend(t, up)
	return b, mem, up
}

func cidOfDigest(d multihash.Multihash) cid.Cid { return cid.NewCidV1(cid.Raw, d) }
