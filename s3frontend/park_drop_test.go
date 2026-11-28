package s3frontend

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// partRecord returns uploadID's part n as recorded.
func partRecord(t *testing.T, mem *testStore, uploadID string, n int) registry.MultipartPart {
	t.Helper()
	parts, err := mem.ListParts(context.Background(), uploadID)
	require.NoError(t, err, "ListParts")
	for _, p := range parts {
		if p.PartNumber == n {
			return p
		}
	}
	require.FailNow(t, "part not recorded", "part %d not recorded for upload %s", n, uploadID)
	return registry.MultipartPart{}
}

// TestParkedPartHoldsNoLocalCopy: once UploadPart parks a part's blobs, none
// of them has a local copy, the part is queued for acceptance with each blob's
// plaintext length, and Complete concludes them from the park rows alone.
func TestParkedPartHoldsNoLocalCopy(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	key := "parked"

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err, "UploadPart")
	digests := hygienePartDigests(t, mem, uploadID, 1)
	part := partRecord(t, mem, uploadID, 1)
	require.Equal(t, registry.PartParked, part.State, "part state")
	var total int64
	for _, n := range part.SpanLengths {
		total += n
	}
	require.Len(t, part.SpanLengths, len(digests), "part span lengths = %v, want one per blob summing to %d", part.SpanLengths, backend.MinPartSize)
	require.Equal(t, int64(backend.MinPartSize), total, "part span lengths = %v, want one per blob summing to %d", part.SpanLengths, backend.MinPartSize)
	for _, d := range digests {
		_, err := os.Stat(localPath(b, d))
		require.True(t, os.IsNotExist(err), "parked blob %s local copy: stat err=%v, want not-exist", d, err)
		in, err := mem.IntentByDigest(ctx, d)
		require.NoError(t, err, "parked blob %s intent", d)
		require.Equal(t, registry.IntentParked, in.State, "parked blob %s intent", d)
		_, err = mem.ParkByDigest(ctx, d)
		require.NoError(t, err, "parked blob %s park row", d)
	}

	one := int32(1)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil)
	require.NoError(t, err, "Complete")
	for _, d := range digests {
		loc, err := mem.GetLocation(ctx, mem.bk(), d)
		require.NoError(t, err, "blob %s location after Complete", d)
		require.NotNil(t, loc, "blob %s location after Complete", d)
	}
}

// failingParker fails every upload, as a provider that is down does.
type failingParker struct{ parkingUploader }

func (*failingParker) UploadBlob(context.Context, did.DID, multihash.Multihash, int64, string) (uploader.UploadedBlob, error) {
	return uploader.UploadedBlob{}, errors.New("provider unreachable")
}

// TestFailedParkIsInvalidAtComplete: a part whose stream failed fails its
// UploadPart before its row is recorded. Nothing holds its bytes to upload
// them again, so a Complete naming it answers InvalidPart and the client
// uploads the part again.
func TestFailedParkIsInvalidAtComplete(t *testing.T) {
	b, mem := newDeferredBackend(t, &failingParker{})
	key := "park-fails"
	body := testBody(int(backend.MinPartSize))

	uploadID := mpCreate(t, b, key, "", "")
	_, err := mpUploadPart(t, b, key, uploadID, 1, body, nil)
	require.Error(t, err, "UploadPart succeeded, want the park failure")
	parts, err := mem.ListParts(context.Background(), uploadID)
	require.NoError(t, err, "parts after a failed park")
	require.Empty(t, parts, "parts after a failed park")
	sum := md5.Sum(body)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`
	one := int32(1)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: &etag}}, nil)
	var apiErr s3err.APIError
	require.ErrorAs(t, err, &apiErr, "Complete")
	require.Equal(t, "InvalidPart", apiErr.Code, "Complete")
}
