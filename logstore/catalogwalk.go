package logstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/cars"
)

// CatalogWalk is what a walk of one bucket's catalog found: how much space
// its segment files on disk take, and how much of the catalog the bucket's
// root still reaches.
type CatalogWalk struct {
	// FileBytes is the size of every catalog CAR of the bucket on disk,
	// headers included, in the same unit as a segment row's size_bytes.
	FileBytes int64
	// LiveBytes is the CAR frames of the distinct blocks the root reaches,
	// counting each block once however many segments hold it. LiveBlocks is
	// how many there are.
	LiveBytes  int64
	LiveBlocks int64
	// MissingBlocks counts the DAG-CBOR blocks the root reaches that the
	// walk could not read: no segment on disk holds them, and they were not
	// fetched from Forge. With any missing, LiveBytes is a lower bound.
	// Every catalog block is DAG-CBOR, so a missing link of another codec is
	// not a catalog block and is not counted.
	MissingBlocks int64
	// FetchedSegments counts the shipped segments read from Forge for blocks
	// no segment on disk holds, such as those of retired segments.
	FetchedSegments int64
}

// FetchSegment returns the blocks of the shipped segment holding c, read
// from Forge, for a block no segment on disk holds. It returns none when no
// shipped segment is known to hold c, or when the one that does was
// returned before.
type FetchSegment func(ctx context.Context, c cid.Cid) ([]block.Block, error)

// WalkCatalog walks bucket's catalog from root over the bucket's catalog
// segments under dir, the log manager's Dir, and, with fetch, over the
// shipped segments the walk needs that dir no longer holds. It holds every
// segment it reads in memory, so it suits a test stack, such as a benchmark
// run, rather than a production node. It only reads, and can run beside a
// live node: a frame still being appended is left out.
func WalkCatalog(ctx context.Context, dir, bucket string, root cid.Cid, fetch FetchSegment) (CatalogWalk, error) {
	var w CatalogWalk
	planeDir := filepath.Join(dir, bucket, blockstore.PlaneCatalog.String())
	entries, err := os.ReadDir(planeDir)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return w, fmt.Errorf("logstore: list %s: %w", planeDir, err)
	}
	blocks := make(map[cid.Cid]block.Block)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "seg-") || !strings.HasSuffix(name, ".car") {
			continue
		}
		path := filepath.Join(planeDir, name)
		info, err := e.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue // retired while we listed
			}
			return w, fmt.Errorf("logstore: stat %s: %w", path, err)
		}
		res, err := cars.ScanFile(path)
		switch {
		case err == nil, errors.Is(err, cars.ErrTorn):
			// A torn tail is a frame still being appended: the whole frames
			// before it count. ErrTorn can wrap an unexpected EOF too, so it
			// is checked first.
		case errors.Is(err, fs.ErrNotExist):
			continue // retired while we read
		case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
			// A segment just created, its header not yet whole: no blocks.
			w.FileBytes += info.Size()
			continue
		default:
			return w, fmt.Errorf("logstore: read %s: %w", path, err)
		}
		w.FileBytes += info.Size()
		for _, f := range res.Frames {
			blocks[f.Block.Cid()] = f.Block
		}
	}

	if !root.Defined() {
		return w, nil
	}
	seen := make(map[cid.Cid]struct{})
	pending := []cid.Cid{root}
	for len(pending) > 0 {
		c := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		b, ok := blocks[c]
		if !ok && fetch != nil && c.Prefix().Codec == cid.DagCBOR {
			fetched, err := fetch(ctx, c)
			if err != nil {
				return w, fmt.Errorf("logstore: fetch the segment holding %s: %w", c, err)
			}
			if len(fetched) > 0 {
				w.FetchedSegments++
			}
			for _, fb := range fetched {
				blocks[fb.Cid()] = fb
			}
			b, ok = blocks[c]
		}
		if !ok {
			if c.Prefix().Codec == cid.DagCBOR {
				w.MissingBlocks++
			}
			continue
		}
		w.LiveBytes += cars.FrameSize(c, uint64(len(b.RawData())))
		w.LiveBlocks++
		if c.Prefix().Codec != cid.DagCBOR {
			continue
		}
		if err := cbg.ScanForLinks(bytes.NewReader(b.RawData()), func(link cid.Cid) {
			pending = append(pending, link)
		}); err != nil {
			return w, fmt.Errorf("logstore: scan %s: %w", c, err)
		}
	}
	return w, nil
}
