package blockstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// BlobCache holds local copies of blobs the provider already holds, keyed by
// digest: a read-after-write cache, so a just-written blob is served straight
// from disk, skipping the network read tier. A blob arrives only from the
// Spool (Take), once the S3 layer has recorded that the provider holds it, so
// every file here may be removed at any time; a read of a removed blob goes to
// the network. Network reads do not fill it yet: a cache that fills on reads
// is undecided and unbuilt, not ruled out.
//
// Like the Spool, it is pure file I/O, and at most one BlobCache may use a
// directory at a time. Its directory must be on the Spool's filesystem, so
// Take is a rename.
type BlobCache struct {
	*blobDir
}

// NewBlobCache opens (creating if needed) a cache rooted at dir and counts its
// blob files.
func NewBlobCache(dir string) (*BlobCache, error) {
	d, err := newBlobDir(dir, "cache")
	if err != nil {
		return nil, err
	}
	if err := d.Scan(func(BlobFile) {}); err != nil {
		return nil, err
	}
	return &BlobCache{blobDir: d}, nil
}

// Usage returns the byte count of the cache's blob files.
func (c *BlobCache) Usage() int64 {
	return c.finished()
}

// Take moves the blob with the given digest from spool into the cache and
// returns its size, moving the bytes between the two counts. Idempotent: a
// blob not in the spool moves nothing and is not an error. Only a blob the
// provider holds may be taken; the caller decides that.
func (c *BlobCache) Take(spool *Spool, digest mh.Multihash) (int64, error) {
	spool.mu.RLock()
	defer spool.mu.RUnlock()
	c.mu.RLock()
	defer c.mu.RUnlock()
	from := spool.Path(digest)
	info, err := os.Stat(from)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("blockstore: cache take: %w", err)
	}
	if err := os.Rename(from, c.Path(digest)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// A concurrent Remove or Take got there first.
			return 0, nil
		}
		return 0, fmt.Errorf("blockstore: cache take: %w", err)
	}
	spool.usage.Add(-info.Size())
	c.usage.Add(info.Size())
	return info.Size(), nil
}

// LocalBlobs reads a blob from local disk wherever it is: the cache, then the
// spool. A blob only ever moves from the spool to the cache, so one that moves
// between the two lookups is found by looking in the cache once more.
type LocalBlobs struct {
	Cache *BlobCache
	Spool *Spool
}

// OpenBlob streams the blob from local disk, or returns ErrNotFound.
func (l LocalBlobs) OpenBlob(ctx context.Context, space did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	return localLookup(func(d *blobDir) (io.ReadCloser, error) { return d.OpenBlob(ctx, space, digest) }, l)
}

// OpenBlobRange streams stored bytes [start, end] of the blob from local disk,
// or returns ErrNotFound.
func (l LocalBlobs) OpenBlobRange(ctx context.Context, space did.DID, digest mh.Multihash, start, end int64) (io.ReadCloser, error) {
	return localLookup(func(d *blobDir) (io.ReadCloser, error) {
		return d.OpenBlobRange(ctx, space, digest, start, end)
	}, l)
}

// GetBlock reads the blob stored under c's multihash from local disk, or
// returns ErrNotFound.
func (l LocalBlobs) GetBlock(ctx context.Context, space did.DID, c cid.Cid) (block.Block, error) {
	return localLookup(func(d *blobDir) (block.Block, error) { return d.GetBlock(ctx, space, c) }, l)
}

// localLookup tries the cache, the spool, then the cache again.
func localLookup[T any](get func(*blobDir) (T, error), l LocalBlobs) (T, error) {
	var zero T
	for _, d := range []*blobDir{l.Cache.blobDir, l.Spool.blobDir, l.Cache.blobDir} {
		v, err := get(d)
		if err == nil {
			return v, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return zero, err
		}
	}
	return zero, ErrNotFound
}

var (
	_ BlockReader     = (*BlobCache)(nil)
	_ BlobReader      = (*BlobCache)(nil)
	_ BlobRangeReader = (*BlobCache)(nil)
	_ BlockReader     = LocalBlobs{}
	_ BlobReader      = LocalBlobs{}
	_ BlobRangeReader = LocalBlobs{}
)
