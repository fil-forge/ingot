package logstore

import (
	"bytes"
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

// LocalCatalog is one bucket's catalog as the segment files on disk hold it:
// how much space they take, and how much of it the bucket's root still
// reaches.
type LocalCatalog struct {
	// FileBytes is the size of every catalog CAR of the bucket on disk,
	// headers included, in the same unit as a segment row's size_bytes.
	FileBytes int64
	// LiveBytes is the CAR frames of the distinct blocks the root reaches,
	// counting each block once however many segments hold it. LiveBlocks is
	// how many there are.
	LiveBytes  int64
	LiveBlocks int64
	// MissingBlocks counts the DAG-CBOR blocks the root reaches that no
	// segment on disk holds, such as those of a retired segment. With any
	// missing, LiveBytes is a lower bound. A link of another codec names an
	// object body, which the catalog never holds, so it is not counted.
	MissingBlocks int64
}

// MeasureLocalCatalog walks bucket's catalog from root over the bucket's
// catalog segments under dir, the log manager's Dir. It reads every segment
// into memory, so it suits a test stack that keeps all of them (a large
// Retain) rather than a production node. It only reads, and can run beside a
// live node: a frame still being appended is left out.
func MeasureLocalCatalog(dir, bucket string, root cid.Cid) (LocalCatalog, error) {
	var lc LocalCatalog
	planeDir := filepath.Join(dir, bucket, blockstore.PlaneCatalog.String())
	entries, err := os.ReadDir(planeDir)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return lc, fmt.Errorf("logstore: list %s: %w", planeDir, err)
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
			return lc, fmt.Errorf("logstore: stat %s: %w", path, err)
		}
		res, err := cars.ScanFile(path)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			continue
		case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
			// A segment just created, its header not yet whole: no blocks.
			lc.FileBytes += info.Size()
			continue
		case err != nil && !errors.Is(err, cars.ErrTorn):
			return lc, fmt.Errorf("logstore: read %s: %w", path, err)
		}
		lc.FileBytes += info.Size()
		for _, f := range res.Frames {
			blocks[f.Block.Cid()] = f.Block
		}
	}

	if !root.Defined() {
		return lc, nil
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
		if !ok {
			if c.Prefix().Codec == cid.DagCBOR {
				lc.MissingBlocks++
			}
			continue
		}
		lc.LiveBytes += cars.FrameSize(c, uint64(len(b.RawData())))
		lc.LiveBlocks++
		if c.Prefix().Codec != cid.DagCBOR {
			continue
		}
		if err := cbg.ScanForLinks(bytes.NewReader(b.RawData()), func(link cid.Cid) {
			pending = append(pending, link)
		}); err != nil {
			return lc, fmt.Errorf("logstore: scan %s: %w", c, err)
		}
	}
	return lc, nil
}
