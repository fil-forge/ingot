package inmem_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/uploader"
	"github.com/fil-forge/ucantone/did"
)

func sha256Digest(t *testing.T, data []byte) multihash.Multihash {
	t.Helper()
	sum := sha256.Sum256(data)
	d, err := multihash.Encode(sum[:], multihash.SHA2_256)
	require.NoError(t, err)
	return d
}

func readAll(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	defer rc.Close()
	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	return b
}

func TestProvider_StoresAndServesAStreamedBlob(t *testing.T) {
	ctx := context.Background()
	p := inmem.NewProvider()
	data := []byte("a blob the provider keeps")
	digest := sha256Digest(t, data)

	sb, err := p.StartBlob(ctx, did.Undef, int64(len(data)))
	require.NoError(t, err)
	require.False(t, p.Has(digest), "an allocation is not an accepted blob")
	require.NoError(t, p.PutBlob(ctx, sb, bytes.NewReader(data)))
	require.Equal(t, digest, p.DigestOf(sb.AddTask))
	require.False(t, p.Has(digest), "a sent blob is parked until it is concluded")

	locations, err := p.ConcludeBlobs(ctx, did.Undef, []uploader.UploadedBlob{sb.Parked(digest)})
	require.NoError(t, err)
	require.Len(t, locations, 1)
	require.NotNil(t, locations[0])
	require.True(t, p.Has(digest))
	require.Zero(t, p.Pending())

	// A repeated conclude answers with the location again; an accepted blob
	// cannot be aborted.
	again, err := p.ConcludeBlobs(ctx, did.Undef, []uploader.UploadedBlob{sb.Parked(digest)})
	require.NoError(t, err)
	require.NotNil(t, again[0])
	require.ErrorIs(t, p.AbortBlob(ctx, did.Undef, sb.AddTask), uploader.ErrBlobAccepted)

	rc, err := p.OpenBlob(ctx, did.Undef, digest)
	require.NoError(t, err)
	require.Equal(t, data, readAll(t, rc))

	blk, err := p.GetBlock(ctx, did.Undef, cid.NewCidV1(cid.Raw, digest))
	require.NoError(t, err)
	require.Equal(t, data, blk.RawData())
}

func TestProvider_ConcludeRefusesWhatWasNotSentOrDoesNotMatch(t *testing.T) {
	ctx := context.Background()
	p := inmem.NewProvider()
	data := []byte("some bytes")

	unsent, err := p.StartBlob(ctx, did.Undef, int64(len(data)))
	require.NoError(t, err)
	locations, err := p.ConcludeBlobs(ctx, did.Undef, []uploader.UploadedBlob{unsent.Parked(sha256Digest(t, data))})
	require.Error(t, err)
	require.Nil(t, locations[0])

	sent, err := p.StartBlob(ctx, did.Undef, int64(len(data)))
	require.NoError(t, err)
	require.NoError(t, p.PutBlob(ctx, sent, bytes.NewReader(data)))
	locations, err = p.ConcludeBlobs(ctx, did.Undef, []uploader.UploadedBlob{sent.Parked(sha256Digest(t, []byte("other")))})
	require.Error(t, err, "the digest must name the bytes received")
	require.Nil(t, locations[0])

	wrongSize, err := p.StartBlob(ctx, did.Undef, 3)
	require.NoError(t, err)
	require.Error(t, p.PutBlob(ctx, wrongSize, bytes.NewReader(data)), "the body must be the allocated size")
}

func TestProvider_AbortDropsTheAllocation(t *testing.T) {
	ctx := context.Background()
	p := inmem.NewProvider()
	sb, err := p.StartBlob(ctx, did.Undef, 2)
	require.NoError(t, err)
	require.Equal(t, 1, p.Pending())
	require.NoError(t, p.AbortBlob(ctx, did.Undef, sb.AddTask))
	require.Zero(t, p.Pending())
}

func TestProvider_RefusesABlobThatDoesNotHashToItsDigest(t *testing.T) {
	p := inmem.NewProvider()
	digest := sha256Digest(t, []byte("what the digest names"))

	err := p.Put(digest, []byte("different bytes"))
	require.Error(t, err)
	require.False(t, p.Has(digest))
}

func TestProvider_UnknownBlobIsNotFound(t *testing.T) {
	ctx := context.Background()
	p := inmem.NewProvider()
	digest := sha256Digest(t, []byte("never stored"))

	_, err := p.OpenBlob(ctx, did.Undef, digest)
	require.True(t, errors.Is(err, blockstore.ErrNotFound))
	_, err = p.OpenBlobRange(ctx, did.Undef, digest, 0, 3)
	require.True(t, errors.Is(err, blockstore.ErrNotFound))
	_, err = p.GetBlock(ctx, did.Undef, cid.NewCidV1(cid.Raw, digest))
	require.True(t, errors.Is(err, blockstore.ErrNotFound))
}

func TestProvider_OpenBlobRange(t *testing.T) {
	ctx := context.Background()
	p := inmem.NewProvider()
	data := []byte("0123456789")
	digest := sha256Digest(t, data)
	require.NoError(t, p.Put(digest, data))

	for _, tc := range []struct {
		name       string
		start, end int64
		want       string
	}{
		{"inclusive end", 2, 5, "2345"},
		{"single byte", 9, 9, "9"},
		{"end past the blob yields what is there", 7, 100, "789"},
		{"start past the blob yields nothing", 10, 20, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rc, err := p.OpenBlobRange(ctx, did.Undef, digest, tc.start, tc.end)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(readAll(t, rc)))
		})
	}

	_, err := p.OpenBlobRange(ctx, did.Undef, digest, 5, 2)
	require.Error(t, err, "an inverted range is invalid")
}

func TestProvider_RemoveBlobDropsIt(t *testing.T) {
	ctx := context.Background()
	p := inmem.NewProvider()
	data := []byte("removed with its last claim")
	digest := sha256Digest(t, data)
	require.NoError(t, p.Put(digest, data))

	require.NoError(t, p.RemoveBlob(ctx, did.Undef, digest))
	require.False(t, p.Has(digest))
	_, err := p.OpenBlob(ctx, did.Undef, digest)
	require.True(t, errors.Is(err, blockstore.ErrNotFound))
}
