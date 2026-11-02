package logstore

import (
	"context"
	"errors"
	"path/filepath"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/fil-forge/ingot/blockstore"
)

// Compile-time assertion that *Store satisfies blockstore.Log.
var _ blockstore.Log = (*Store)(nil)

// Store is the catalog log: a single LSM pipeline (its own seal trigger,
// transport, and retention) over the dag-cbor MST nodes + ObjectManifests.
// Object-body blobs are not journaled — they are spooled and uploaded per-blob
// (see blockstore.Spool), so the data plane is gone. Store is a thin wrapper
// over one PlaneLog, kept as the blockstore.Log seam the write/read paths hold.
type Store struct {
	catalog *PlaneLog
}

// Open initializes the catalog pipeline under <Dir>/catalog.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.defaults()

	catalog, err := openPlaneLog(ctx, blockstore.PlaneCatalog, cfg.Bucket, filepath.Join(cfg.Dir, "catalog"), cfg.Catalog, cfg.Meta, cfg.Logger)
	if err != nil {
		return nil, err
	}
	return &Store{catalog: catalog}, nil
}

// AppendBatch persists one op's catalog blocks plus the op-root, fsynced before
// returning. The block slice may be empty: an MST mutation can produce a new
// root that points only at nodes already materialized in a prior segment (e.g.
// trimTop after Delete), so AppendBatch is called with the OpRoot alone.
func (s *Store) AppendBatch(ctx context.Context, catalogBlocks []block.Block, opRoot blockstore.OpRoot) error {
	if !opRoot.Root.Defined() {
		return errors.New("logstore: AppendBatch: opRoot.Root must be defined")
	}
	return s.catalog.Append(ctx, catalogBlocks, opRoot)
}

// Get returns the catalog block holding c, or ErrNotFound. (Body blobs are
// never in the log; a raw-codec lookup simply misses and the caller falls
// through to the spool / network tier.)
func (s *Store) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	return s.catalog.Get(ctx, c)
}

// Close closes the catalog pipeline (sealing the open segment, draining the
// flush queue). Safe to call once.
func (s *Store) Close(ctx context.Context) error {
	return s.catalog.Close(ctx)
}
