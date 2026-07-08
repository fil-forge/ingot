package blockstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// Spool is the local on-disk blob store (docs/architecture.md §5): each
// object-body blob is written here, keyed by its sha256 digest, before it is
// uploaded to Forge. The local copy does double duty as the read-after-write
// floor and the read cache — a just-written or hot blob is served straight from
// disk, skipping the network read tier.
//
// Spool is deliberately pure file I/O: a blockstore.BlockReader plus the
// streaming BlobReader/BlobWriter, and nothing more. The lifecycle of a blob
// (the upload_intents state machine, eviction policy) is owned by the caller
// that has the registry handle — blockstore cannot import registry without a
// cycle (registry imports
// blockstore for the segment-metadata types).
type Spool struct {
	dir string
}

// NewSpool opens (creating if needed) a spool rooted at dir.
func NewSpool(dir string) (*Spool, error) {
	if dir == "" {
		return nil, errors.New("blockstore: spool dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("blockstore: spool mkdir: %w", err)
	}
	return &Spool{dir: dir}, nil
}

// Path returns the on-disk path a blob with the given digest is stored at.
// Exposed so the caller can record it in upload_intents.local_path and hand it
// to the body uploader without re-deriving the layout.
func (s *Spool) Path(digest mh.Multihash) string {
	return filepath.Join(s.dir, hex.EncodeToString(digest))
}

// WriteBlob streams r to the spool, computing its sha256 digest as it writes so
// the blob is never held whole in memory (object-body blobs run up to
// max_blob_size = 256 MiB; buffering them would put that × concurrency in RAM).
// The write is atomic (temp file → rename to the digest path), so a crash leaves
// no partial blob readable under its digest. An empty r writes nothing and
// returns a nil digest with n == 0 (a zero-byte object has no blob). Re-writing
// an identical blob is idempotent (same digest, rename overwrites in place).
func (s *Spool) WriteBlob(_ context.Context, r io.Reader) (mh.Multihash, int64, error) {
	tmp, err := os.CreateTemp(s.dir, ".tmp-*")
	if err != nil {
		return nil, 0, fmt.Errorf("blockstore: spool tempfile: %w", err)
	}
	tmpName := tmp.Name()
	hasher := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(tmp, hasher), r)
	if closeErr := tmp.Close(); closeErr != nil && copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		_ = os.Remove(tmpName)
		return nil, n, fmt.Errorf("blockstore: spool write: %w", copyErr)
	}
	if n == 0 {
		_ = os.Remove(tmpName)
		return nil, 0, nil
	}
	digest, err := mh.Encode(hasher.Sum(nil), mh.SHA2_256)
	if err != nil {
		_ = os.Remove(tmpName)
		return nil, n, fmt.Errorf("blockstore: spool digest: %w", err)
	}
	if err := os.Rename(tmpName, s.Path(digest)); err != nil {
		_ = os.Remove(tmpName)
		return nil, n, fmt.Errorf("blockstore: spool rename: %w", err)
	}
	return digest, n, nil
}

// OpenBlob returns a streaming reader over the spooled blob with the given
// digest, or ErrNotFound. Unlike GetBlock it does not read the blob into memory —
// the body read path serves bytes straight off disk. The caller owns the reader
// and must Close it. The returned *os.File is seekable, which the body reader
// uses to start a ranged read mid-blob without reading-and-discarding.
func (s *Spool) OpenBlob(_ context.Context, _ did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	f, err := os.Open(s.Path(digest))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blockstore: spool open %s: %w", digest.B58String(), err)
	}
	return f, nil
}

// GetBlock returns the blob stored under c's multihash, or ErrNotFound. A miss
// is expected and cheap: it lets the layered read path fall through to the log
// (for catalog blocks, which are never spooled) or the network tier (for a body
// blob that has been evicted).
func (s *Spool) GetBlock(_ context.Context, _ did.DID, c cid.Cid) (block.Block, error) {
	data, err := os.ReadFile(s.Path(c.Hash()))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blockstore: spool read %s: %w", c, err)
	}
	return block.NewBlockWithCid(data, c)
}

// Compile-time assertions: Spool is a raw-block read tier and the streaming
// blob tier for object bodies.
var (
	_ BlockReader = (*Spool)(nil)
	_ BlobReader  = (*Spool)(nil)
	_ BlobWriter  = (*Spool)(nil)
)
