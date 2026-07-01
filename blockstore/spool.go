package blockstore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

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
// Spool is deliberately pure file I/O: it is a blockstore.BlockReader +
// BlockWriter and nothing more. The lifecycle of a blob (the upload_intents
// state machine, eviction policy) is owned by the caller that has the registry
// handle — blockstore cannot import registry without a cycle (registry imports
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

// PutBlock writes a raw block to the spool, keyed by its multihash. Writing is
// atomic (write to a temp file, then rename) so a crash mid-write never leaves a
// partial blob readable under its digest.
func (s *Spool) PutBlock(_ context.Context, blk block.Block) error {
	final := s.Path(blk.Cid().Hash())
	tmp, err := os.CreateTemp(s.dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("blockstore: spool tempfile: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(blk.RawData()); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("blockstore: spool write: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("blockstore: spool close: %w", err)
	}
	if err := os.Rename(tmpName, final); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("blockstore: spool rename: %w", err)
	}
	return nil
}

// GetBlock returns the blob stored under c's multihash, or ErrNotFound. A miss
// is expected and cheap: it lets the layered read path fall through to the log
// (for catalog blocks, which are never spooled) or the network tier (for a body
// blob that has been evicted).
func (s *Spool) GetBlock(_ context.Context, c cid.Cid) (block.Block, error) {
	data, err := os.ReadFile(s.Path(c.Hash()))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blockstore: spool read %s: %w", c, err)
	}
	return block.NewBlockWithCid(data, c)
}

// Has reports whether a blob with the given digest is on disk.
func (s *Spool) Has(digest mh.Multihash) bool {
	_, err := os.Stat(s.Path(digest))
	return err == nil
}

// Remove deletes a spooled blob (eviction / cleanup). Absent files are not an
// error.
func (s *Spool) Remove(digest mh.Multihash) error {
	if err := os.Remove(s.Path(digest)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("blockstore: spool remove: %w", err)
	}
	return nil
}

// Compile-time assertions: Spool is a read+write block tier.
var (
	_ BlockReader = (*Spool)(nil)
	_ BlockWriter = (*Spool)(nil)
)
