package blockstore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// blobDir is a directory of finished blob files, each named by the hex of
// its digest, with a running byte count of them. Spool and BlobCache are
// both built on it. It knows a blob only as bytes under a digest: whether one
// may be removed is the caller's question.
//
// At most one blobDir may use a directory at a time: the byte count assumes
// it makes every change to the directory.
type blobDir struct {
	dir string
	// kind names the directory in errors.
	kind string
	// mu orders the changes that move the count: count holds it
	// exclusively while it measures, and each commit into the directory or
	// removal from it holds it shared, so the count sees every file either
	// before or after the change. A move between directories
	// (BlobCache.Take) holds both exclusively.
	mu sync.RWMutex
	// usage is the byte count of the finished blob files.
	usage atomic.Int64
}

func newBlobDir(dir, kind string) (*blobDir, error) {
	if dir == "" {
		return nil, fmt.Errorf("blockstore: %s dir is required", kind)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("blockstore: %s mkdir: %w", kind, err)
	}
	return &blobDir{dir: dir, kind: kind}, nil
}

// BlobFile is one file Scan found: a finished blob (Digest set) or a .tmp-*
// file (Digest nil), which only a Spool holds.
type BlobFile struct {
	Name    string
	Digest  mh.Multihash
	Size    int64
	ModTime time.Time
}

// Scan walks the directory and calls fn for each blob file and each .tmp-*
// file, skipping directories and names that are neither. It takes no lock, so
// writes, moves and removals go on while it runs: a file that changes during
// the scan may be seen before or after the change, or not at all. It stops,
// returning ctx's error, once ctx is done.
func (d *blobDir) Scan(ctx context.Context, fn func(BlobFile)) error {
	_, err := d.walk(ctx, fn)
	return err
}

// count sets the byte count to the blob files on disk, holding the directory
// exclusively while it measures, and calls fn for each file as Scan does. A
// Spool or BlobCache counts once, when it opens; after that every change it
// makes keeps the count itself. fn must not call back into this directory.
func (d *blobDir) count(fn func(BlobFile)) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	total, err := d.walk(context.Background(), fn)
	if err != nil {
		return err
	}
	d.usage.Store(total)
	return nil
}

// walk lists the directory for Scan and count and returns the total size of
// the blob files it saw. A file removed while it runs is skipped.
func (d *blobDir) walk(ctx context.Context, fn func(BlobFile)) (int64, error) {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return 0, fmt.Errorf("blockstore: %s scan: %w", d.kind, err)
	}
	var total int64
	for _, de := range entries {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		if !de.Type().IsRegular() {
			continue
		}
		name := de.Name()
		var digest mh.Multihash
		if !strings.HasPrefix(name, spoolTempPrefix) {
			digest = digestFromName(name)
			if digest == nil {
				continue
			}
		}
		info, err := de.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return total, fmt.Errorf("blockstore: %s stat %s: %w", d.kind, name, err)
		}
		if digest != nil {
			total += info.Size()
		}
		fn(BlobFile{Name: name, Digest: digest, Size: info.Size(), ModTime: info.ModTime()})
	}
	return total, nil
}

// RemoveTemp deletes one .tmp-* file by name and returns its size. Idempotent.
// It refuses any other name, so it cannot be used to delete a finished blob.
// It leaves every count alone: a temp file is counted only while a live write
// holds it (Spool's in-flight count), and only an abandoned write's file is
// safe to remove; a write that is still live takes its own bytes off when it
// fails.
func (d *blobDir) RemoveTemp(name string) (int64, error) {
	if !strings.HasPrefix(name, spoolTempPrefix) || filepath.Base(name) != name {
		return 0, fmt.Errorf("blockstore: %s remove temp: %q is not a temp file", d.kind, name)
	}
	path := filepath.Join(d.dir, name)
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("blockstore: %s remove temp: %w", d.kind, err)
	}
	err = os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("blockstore: %s remove temp: %w", d.kind, err)
	}
	return info.Size(), nil
}

// digestFromName decodes a blob file name back into the multihash it was
// written under, or nil when the name is not a hex multihash.
func digestFromName(name string) mh.Multihash {
	raw, err := hex.DecodeString(name)
	if err != nil {
		return nil
	}
	digest, err := mh.Cast(raw)
	if err != nil {
		return nil
	}
	return digest
}

// finished returns the byte count of the finished blob files.
func (d *blobDir) finished() int64 {
	return d.usage.Load()
}

// Path returns the on-disk path a blob with the given digest is stored at.
func (d *blobDir) Path(digest mh.Multihash) string {
	return filepath.Join(d.dir, hex.EncodeToString(digest))
}

// Remove deletes the blob with the given digest and returns how many bytes it
// freed. Idempotent: removing a blob that isn't here frees nothing and is not
// an error.
func (d *blobDir) Remove(digest mh.Multihash) (int64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	path := d.Path(digest)
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("blockstore: %s remove: %w", d.kind, err)
	}
	err = os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		// A concurrent Remove deleted it and counted it.
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("blockstore: %s remove: %w", d.kind, err)
	}
	d.usage.Add(-info.Size())
	return info.Size(), nil
}

// OpenBlob returns a streaming reader over the blob with the given digest, or
// ErrNotFound. Unlike GetBlock it does not read the blob into memory — the
// body read path serves bytes straight off disk. The caller owns the reader
// and must Close it. The returned *os.File is seekable, which the body reader
// uses to start a ranged read mid-blob without reading-and-discarding. An open
// file survives a later Remove.
func (d *blobDir) OpenBlob(_ context.Context, _ did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	f, err := os.Open(d.Path(digest))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blockstore: %s open %s: %w", d.kind, digest.B58String(), err)
	}
	return f, nil
}

// OpenBlobRange returns a reader over stored bytes [start, end] (inclusive)
// of the blob, or ErrNotFound — OpenBlob restricted to a section, for the
// decrypting read path, which fetches only the ciphertext span a plaintext
// range needs. An end past the file's end yields a shorter stream.
func (d *blobDir) OpenBlobRange(_ context.Context, _ did.DID, digest mh.Multihash, start, end int64) (io.ReadCloser, error) {
	f, err := os.Open(d.Path(digest))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blockstore: %s open %s: %w", d.kind, digest.B58String(), err)
	}
	return readerCloser{Reader: io.NewSectionReader(f, start, end-start+1), Closer: f}, nil
}

// GetBlock returns the blob stored under c's multihash, or ErrNotFound.
func (d *blobDir) GetBlock(_ context.Context, _ did.DID, c cid.Cid) (block.Block, error) {
	data, err := os.ReadFile(d.Path(c.Hash()))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blockstore: %s read %s: %w", d.kind, c, err)
	}
	return block.NewBlockWithCid(data, c)
}
