package blockstore

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/fil-forge/ucantone/did"
	"github.com/stretchr/testify/require"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// fakeBlobTier is a read tier that implements BlockReader (so it fits the
// Layered/Cached constructors) and BlobReader. data == nil means a miss.
type fakeBlobTier struct{ data []byte }

func (f fakeBlobTier) GetBlock(context.Context, did.DID, cid.Cid) (block.Block, error) {
	return nil, ErrNotFound
}

func (f fakeBlobTier) OpenBlob(context.Context, did.DID, mh.Multihash) (io.ReadCloser, error) {
	if f.data == nil {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

func readOpenBlob(t *testing.T, r BlobReader, digest mh.Multihash) (string, error) {
	t.Helper()
	rc, err := r.OpenBlob(context.Background(), did.Undef, digest)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	return string(b), err
}

// TestLayered_OpenBlob_Tiering covers the body-blob read fallthrough: cache first
// (read-after-write), then the network base after eviction, then ErrNotFound. The
// log is never consulted (it holds no body blobs).
func TestLayered_OpenBlob_Tiering(t *testing.T) {
	digest, _ := mh.Sum([]byte("digest"), mh.SHA2_256, -1)

	// cache hit — base is not consulted.
	l := NewLayered(fakeBlobTier{data: []byte("from-cache")}, nil, fakeBlobTier{data: []byte("from-base")})
	got, err := readOpenBlob(t, l, digest)
	require.NoError(t, err, "cache hit")
	require.Equal(t, "from-cache", got, "cache hit")

	// cache miss → base hit (the after-eviction path).
	l = NewLayered(fakeBlobTier{data: nil}, nil, fakeBlobTier{data: []byte("from-base")})
	got, err = readOpenBlob(t, l, digest)
	require.NoError(t, err, "cache miss → base")
	require.Equal(t, "from-base", got, "cache miss → base")

	// both miss → ErrNotFound.
	l = NewLayered(fakeBlobTier{data: nil}, nil, fakeBlobTier{data: nil})
	_, err = l.OpenBlob(context.Background(), did.Undef, digest)
	require.ErrorIs(t, err, ErrNotFound, "both miss")

	// nil cache is skipped, not panicked.
	l = NewLayered(nil, nil, fakeBlobTier{data: []byte("from-base")})
	got, err = readOpenBlob(t, l, digest)
	require.NoError(t, err, "nil cache → base")
	require.Equal(t, "from-base", got, "nil cache → base")
}

// TestCached_OpenBlob_BypassesCache confirms OpenBlob streams straight from the
// base reader (no LRU) — a large body blob must not be buffered into the cache.
func TestCached_OpenBlob_BypassesCache(t *testing.T) {
	digest, _ := mh.Sum([]byte("digest"), mh.SHA2_256, -1)
	c := NewCached(fakeBlobTier{data: []byte("streamed")}, 1<<20).(*Cached)
	got, err := readOpenBlob(t, c, digest)
	require.NoError(t, err, "cached OpenBlob")
	require.Equal(t, "streamed", got, "cached OpenBlob")
}

// TestBlobCache_OpenBlobRange: the section reader serves exactly the
// inclusive [start, end] of the cached file, and an end past the file's end
// yields a shorter stream.
func TestBlobCache_OpenBlobRange(t *testing.T) {
	ctx := context.Background()
	sp := newTestCache(t)
	digest := cacheTestBlob(t, sp, "0123456789abcdef")

	cases := []struct {
		name       string
		start, end int64
		want       string
	}{
		{"interior", 4, 9, "456789"},
		{"prefix", 0, 2, "012"},
		{"single byte", 15, 15, "f"},
		{"end past file clamps", 12, 100, "cdef"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rc, err := sp.OpenBlobRange(ctx, did.Undef, digest, c.start, c.end)
			require.NoError(t, err, "OpenBlobRange")
			defer rc.Close()
			got, err := io.ReadAll(rc)
			require.NoError(t, err, "read")
			require.Equal(t, c.want, string(got), "range [%d,%d]", c.start, c.end)
		})
	}

	_, err := sp.OpenBlobRange(ctx, did.Undef, mustDigest(t, []byte("absent")), 0, 1)
	require.ErrorIs(t, err, ErrNotFound, "missing blob err")
}

// TestOpenBlobRangeOf_Fallback: a tier without the BlobRangeReader capability
// is served through OpenBlob with the prefix discarded and the tail limited.
func TestOpenBlobRangeOf_Fallback(t *testing.T) {
	ctx := context.Background()
	tier := fakeBlobTier{data: []byte("0123456789abcdef")}

	rc, err := OpenBlobRangeOf(ctx, tier, did.Undef, nil, 4, 9)
	require.NoError(t, err, "OpenBlobRangeOf")
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err, "read")
	require.Equal(t, "456789", string(got), "fallback range")
}

// mustDigest hashes data into the multihash form blobs are keyed by.
func mustDigest(t *testing.T, data []byte) mh.Multihash {
	t.Helper()
	d, err := mh.Sum(data, mh.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	return d
}
