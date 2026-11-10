package blockstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// BlobCache holds local copies of blobs the provider already holds, keyed by
// digest: a read-after-write cache, so a just-written blob is served straight
// from disk, skipping the network read tier. A write tees the blob into a
// temporary file here as it streams it to the provider (CreateTemp), and the
// S3 layer adopts the file under its digest once it has recorded that the
// provider holds the blob (Adopt), so every blob file here may be removed at
// any time; a read of a removed blob goes to the network. Network reads do
// not fill it yet: a cache that fills on reads is undecided and unbuilt, not
// ruled out.
//
// It holds no policy: it knows a blob only as bytes under a digest, and the
// S3 layer decides what to remove and when. It tells the S3 layer about each
// read it serves (OnRead), which extends the blob's time in the cache. At
// most one BlobCache may use a directory at a time: NewBlobCache deletes
// every temporary file it finds, which would destroy another live cache's
// writes in progress.
type BlobCache struct {
	*blobDir
	onRead func(context.Context, mh.Multihash)
	// inFlight is every byte written so far to temporary files not yet
	// adopted or discarded. It is kept apart from the blob files' count,
	// which a scan measures and may reset, so a scan never measures a write
	// mid-way.
	inFlight atomic.Int64
}

// tempPrefix names the files a write streams into before they are adopted
// under their digest.
const tempPrefix = ".tmp-"

// NewBlobCache opens (creating if needed) a cache rooted at dir, deletes
// every leftover temporary file it can (a write the previous process never
// finished; nothing can be writing one before the listener starts) and counts
// the blob files. A leftover it cannot delete is left for the sweeper's
// orphan pass, which retries and logs it, rather than failing startup.
// Entries that are neither, such as lost+found on a dedicated filesystem, are
// ignored.
func NewBlobCache(dir string) (*BlobCache, error) {
	d, err := newBlobDir(dir, "cache")
	if err != nil {
		return nil, err
	}
	var temps []string
	if err := d.count(func(f BlobFile) {
		if f.Digest == nil {
			temps = append(temps, f.Name)
		}
	}); err != nil {
		return nil, err
	}
	for _, name := range temps {
		_, _ = d.RemoveTemp(name)
	}
	return &BlobCache{blobDir: d}, nil
}

// OnRead sets fn to be called with each blob the cache serves, in the
// reader's goroutine, before the read returns. Call it before the cache
// serves any read.
func (c *BlobCache) OnRead(fn func(ctx context.Context, digest mh.Multihash)) {
	c.onRead = fn
}

// read reports a blob the cache served.
func (c *BlobCache) read(ctx context.Context, digest mh.Multihash) {
	if c.onRead != nil {
		c.onRead(ctx, digest)
	}
}

// OpenBlob streams the cached blob, or returns ErrNotFound.
func (c *BlobCache) OpenBlob(ctx context.Context, space did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	rc, err := c.blobDir.OpenBlob(ctx, space, digest)
	if err == nil {
		c.read(ctx, digest)
	}
	return rc, err
}

// OpenBlobRange streams stored bytes [start, end] of the cached blob, or
// returns ErrNotFound.
func (c *BlobCache) OpenBlobRange(ctx context.Context, space did.DID, digest mh.Multihash, start, end int64) (io.ReadCloser, error) {
	rc, err := c.blobDir.OpenBlobRange(ctx, space, digest, start, end)
	if err == nil {
		c.read(ctx, digest)
	}
	return rc, err
}

// GetBlock reads the cached blob stored under cid's multihash, or returns
// ErrNotFound.
func (c *BlobCache) GetBlock(ctx context.Context, space did.DID, k cid.Cid) (block.Block, error) {
	b, err := c.blobDir.GetBlock(ctx, space, k)
	if err == nil {
		c.read(ctx, k.Hash())
	}
	return b, err
}

// Touch sets the cached blob's modification time to at, which orders the
// budget pass's evictions, oldest first. It reports whether the blob is
// cached.
func (c *BlobCache) Touch(digest mh.Multihash, at time.Time) (bool, error) {
	err := os.Chtimes(c.Path(digest), at, at)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("blockstore: cache touch: %w", err)
	}
	return true, nil
}

// Usage returns the byte count of the cache: its blob files and the bytes
// written so far to temporary files.
func (c *BlobCache) Usage() int64 {
	return c.finished() + c.inFlight.Load()
}

// InFlight returns the bytes written so far to temporary files.
func (c *BlobCache) InFlight() int64 {
	return c.inFlight.Load()
}

// TempBlob is a blob being written into the cache, not yet under its digest.
// Each byte written counts toward the cache's usage as it lands. Close ends
// the writes; Adopt or Discard ends it. Not safe for concurrent use.
type TempBlob struct {
	c      *BlobCache
	f      *os.File
	n      int64
	closed bool
	done   bool
}

// CreateTemp starts a blob write into the cache.
func (c *BlobCache) CreateTemp() (*TempBlob, error) {
	f, err := os.CreateTemp(c.dir, tempPrefix+"*")
	if err != nil {
		return nil, fmt.Errorf("blockstore: cache tempfile: %w", err)
	}
	return &TempBlob{c: c, f: f}, nil
}

func (t *TempBlob) Write(p []byte) (int, error) {
	n, err := t.f.Write(p)
	t.n += int64(n)
	t.c.inFlight.Add(int64(n))
	return n, err
}

// Ended reports whether the temporary file has been adopted or discarded.
func (t *TempBlob) Ended() bool {
	return t.done
}

// Close ends the writes and closes the file, keeping it for Adopt or Discard,
// so a finished blob holds no file descriptor while the rest of its object
// streams. Closing it again does nothing.
func (t *TempBlob) Close() error {
	if t.closed {
		return nil
	}
	t.closed = true
	if err := t.f.Close(); err != nil {
		return fmt.Errorf("blockstore: cache tempfile close: %w", err)
	}
	return nil
}

// Discard deletes the temporary file and takes its bytes off the count.
// Discarding it again, once adopted, or a nil TempBlob, does nothing.
func (t *TempBlob) Discard() {
	if t == nil || t.done {
		return
	}
	t.done = true
	_ = t.Close()
	_ = os.Remove(t.f.Name())
	t.c.inFlight.Add(-t.n)
}

// Adopt moves a finished temporary file under digest and returns its size,
// moving its bytes from the in-flight count to the blob files'. Only a blob
// the provider holds may be adopted; the caller decides that. A copy already
// cached under the digest (the same bytes stored as received and written
// again) is replaced, and its bytes leave the count as the new copy's enter
// it. A temporary file that fails to adopt is discarded.
//
// Adopt holds the directory exclusively, so it never overlaps a removal of
// the same digest: the removal finds the old copy or the new one, and the
// bytes come off the count once.
func (c *BlobCache) Adopt(t *TempBlob, digest mh.Multihash) (int64, error) {
	if t.done {
		return 0, fmt.Errorf("blockstore: cache adopt: temporary file already ended")
	}
	defer t.Discard()
	if err := t.Close(); err != nil {
		return 0, fmt.Errorf("blockstore: cache adopt: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var replaced int64
	if prev, err := os.Stat(c.Path(digest)); err == nil {
		replaced = prev.Size()
	}
	if err := os.Rename(t.f.Name(), c.Path(digest)); err != nil {
		return 0, fmt.Errorf("blockstore: cache adopt: %w", err)
	}
	t.done = true
	c.inFlight.Add(-t.n)
	c.usage.Add(t.n - replaced)
	c.changes.Add(1)
	return t.n, nil
}

var (
	_ BlockReader     = (*BlobCache)(nil)
	_ BlobReader      = (*BlobCache)(nil)
	_ BlobRangeReader = (*BlobCache)(nil)
)
