package blockstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

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
// Like the Spool, it holds no policy: it knows a blob only as bytes under a
// digest, and the S3 layer decides what to remove. Besides the files and their
// byte count, it remembers in memory when each blob was last read from it,
// which the S3 layer's eviction consults. At most one BlobCache may use a
// directory at a time. Its directory must be on the Spool's filesystem, so
// Take is a rename.
type BlobCache struct {
	*blobDir
	reads *recencyMap
}

// NewBlobCache opens (creating if needed) a cache rooted at dir and counts its
// blob files.
func NewBlobCache(dir string) (*BlobCache, error) {
	d, err := newBlobDir(dir, "cache")
	if err != nil {
		return nil, err
	}
	if err := d.count(func(BlobFile) {}); err != nil {
		return nil, err
	}
	return &BlobCache{blobDir: d, reads: newRecencyMap(recencyCapacity)}, nil
}

// LastRead reports when the blob with the given digest was last served from
// the cache by this process, if it is still remembered.
func (c *BlobCache) LastRead(digest mh.Multihash) (time.Time, bool) {
	return c.reads.get(string(digest))
}

// OpenBlob streams the cached blob, or returns ErrNotFound, and records the
// read.
func (c *BlobCache) OpenBlob(ctx context.Context, space did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	rc, err := c.blobDir.OpenBlob(ctx, space, digest)
	if err == nil {
		c.reads.touch(string(digest), time.Now())
	}
	return rc, err
}

// OpenBlobRange streams stored bytes [start, end] of the cached blob, or
// returns ErrNotFound, and records the read.
func (c *BlobCache) OpenBlobRange(ctx context.Context, space did.DID, digest mh.Multihash, start, end int64) (io.ReadCloser, error) {
	rc, err := c.blobDir.OpenBlobRange(ctx, space, digest, start, end)
	if err == nil {
		c.reads.touch(string(digest), time.Now())
	}
	return rc, err
}

// GetBlock reads the cached blob stored under cid's multihash, or returns
// ErrNotFound, and records the read.
func (c *BlobCache) GetBlock(ctx context.Context, space did.DID, k cid.Cid) (block.Block, error) {
	b, err := c.blobDir.GetBlock(ctx, space, k)
	if err == nil {
		c.reads.touch(string(k.Hash()), time.Now())
	}
	return b, err
}

// CheckTake confirms that Take can move blobs from spool into the cache, which
// needs the two directories on one filesystem, by moving an empty probe file
// between them. Call it at startup: a cache mounted elsewhere would otherwise
// fail every Take, one warning per blob.
func (c *BlobCache) CheckTake(spool *Spool) error {
	f, err := os.CreateTemp(spool.dir, spoolTempPrefix+"probe-*")
	if err != nil {
		return fmt.Errorf("blockstore: cache probe: %w", err)
	}
	from := f.Name()
	_ = f.Close()
	to := filepath.Join(c.dir, filepath.Base(from))
	if err := os.Rename(from, to); err != nil {
		_ = os.Remove(from)
		return fmt.Errorf("blockstore: cache %s must be on the spool's filesystem (%s): %w", c.dir, spool.dir, err)
	}
	if err := os.Remove(to); err != nil {
		return fmt.Errorf("blockstore: cache probe: %w", err)
	}
	return nil
}

// Usage returns the byte count of the cache's blob files.
func (c *BlobCache) Usage() int64 {
	return c.finished()
}

// Take moves the blob with the given digest from spool into the cache and
// returns its size, moving the bytes between the two counts. Idempotent: a
// blob not in the spool moves nothing and is not an error. Only a blob the
// provider holds may be taken; the caller decides that. A copy already in the
// cache would be replaced but counted again; that cannot happen today,
// because every write encrypts under a fresh key and so has a digest of its
// own.
//
// Take holds both directories exclusively, so a move never overlaps a
// removal of the same blob: whether the removal finds the file in the spool
// or in the cache, the bytes come off one count, once.
func (c *BlobCache) Take(spool *Spool, digest mh.Multihash) (int64, error) {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	from := spool.Path(digest)
	info, err := os.Stat(from)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("blockstore: cache take: %w", err)
	}
	if err := os.Rename(from, c.Path(digest)); err != nil {
		if _, statErr := os.Stat(from); errors.Is(err, os.ErrNotExist) && errors.Is(statErr, os.ErrNotExist) {
			// Removed since the first Stat, by something outside this
			// process, which holding both directories rules out within it:
			// the spool no longer holds its bytes. (A missing cache
			// directory also fails the rename with ErrNotExist, but leaves
			// the file where it was, so it is an error below.)
			spool.usage.Add(-info.Size())
			spool.changes.Add(1)
			return 0, nil
		}
		return 0, fmt.Errorf("blockstore: cache take: %w", err)
	}
	spool.usage.Add(-info.Size())
	c.usage.Add(info.Size())
	spool.changes.Add(1)
	c.changes.Add(1)
	return info.Size(), nil
}

// LocalBlobs reads a blob from local disk wherever it is: the cache, then the
// spool. A blob only ever moves from the spool to the cache, so one that moves
// between the two lookups is found by looking in the cache once more.
type LocalBlobs struct {
	Cache *BlobCache
	Spool *Spool
}

// localTier is what LocalBlobs reads from each directory.
type localTier interface {
	BlockReader
	BlobReader
	BlobRangeReader
}

// OpenBlob streams the blob from local disk, or returns ErrNotFound.
func (l LocalBlobs) OpenBlob(ctx context.Context, space did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	return localLookup(func(d localTier) (io.ReadCloser, error) { return d.OpenBlob(ctx, space, digest) }, l)
}

// OpenBlobRange streams stored bytes [start, end] of the blob from local disk,
// or returns ErrNotFound.
func (l LocalBlobs) OpenBlobRange(ctx context.Context, space did.DID, digest mh.Multihash, start, end int64) (io.ReadCloser, error) {
	return localLookup(func(d localTier) (io.ReadCloser, error) {
		return d.OpenBlobRange(ctx, space, digest, start, end)
	}, l)
}

// GetBlock reads the blob stored under c's multihash from local disk, or
// returns ErrNotFound.
func (l LocalBlobs) GetBlock(ctx context.Context, space did.DID, c cid.Cid) (block.Block, error) {
	return localLookup(func(d localTier) (block.Block, error) { return d.GetBlock(ctx, space, c) }, l)
}

// localLookup tries the cache, the spool, then the cache again. A block that
// is never local, such as a catalog block on its way to the log, costs three
// failed opens.
func localLookup[T any](get func(localTier) (T, error), l LocalBlobs) (T, error) {
	var zero T
	for _, d := range []localTier{l.Cache, l.Spool, l.Cache} {
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
