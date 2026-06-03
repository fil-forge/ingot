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

// Store is the two-plane coordinator: it owns a data PlaneLog and a
// catalog PlaneLog, each an independent LSM pipeline (its own seal
// trigger, transport, and retention). The planes share nothing but this
// coordinator and the Meta seq allocator.
//
// AppendBatch is the only cross-plane operation. It persists one S3 op's
// data and catalog blocks, fsyncing DATA BEFORE CATALOG so a crash can
// never leave a durable catalog entry (op-root / MST node) referencing
// non-durable data. The caller's bucket-root CAS runs only after
// AppendBatch returns.
type Store struct {
	data    *PlaneLog
	catalog *PlaneLog
}

// Open initializes both plane pipelines under <Dir>/data and
// <Dir>/catalog.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.defaults()

	data, err := openPlaneLog(ctx, blockstore.PlaneData, filepath.Join(cfg.Dir, "data"), cfg.Data, cfg.Meta, cfg.Logger)
	if err != nil {
		return nil, err
	}
	catalog, err := openPlaneLog(ctx, blockstore.PlaneCatalog, filepath.Join(cfg.Dir, "catalog"), cfg.Catalog, cfg.Meta, cfg.Logger)
	if err != nil {
		_ = data.Close(ctx)
		return nil, err
	}
	return &Store{data: data, catalog: catalog}, nil
}

// AppendBatch persists one op's data + catalog blocks plus the op-root.
// Data is appended (and fsynced) BEFORE catalog: the op-root and MST nodes
// reference data chunks by CID, so catalog durability must imply data
// durability. Either block slice may be empty; the catalog always records
// the op-root (the bucket Root advanced).
func (s *Store) AppendBatch(ctx context.Context, dataBlocks, catalogBlocks []block.Block, opRoot blockstore.OpRoot) error {
	if !opRoot.Root.Defined() {
		return errors.New("logstore: AppendBatch: opRoot.Root must be defined")
	}
	if err := s.data.Append(ctx, dataBlocks); err != nil {
		return err
	}
	if err := s.catalog.Append(ctx, catalogBlocks, opRoot); err != nil {
		return err
	}
	return nil
}

// Get returns the block from whichever plane holds it, or ErrNotFound.
// Data blocks are raw-codec and catalog blocks dag-cbor, so the codec
// routes the lookup; a miss in the routed plane falls back to the other
// (defensive — keeps Get correct if classification ever drifts).
func (s *Store) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	first, second := s.catalog, s.data
	if c.Prefix().Codec == cid.Raw {
		first, second = s.data, s.catalog
	}
	blk, err := first.Get(ctx, c)
	if err == nil {
		return blk, nil
	}
	if !errors.Is(err, blockstore.ErrNotFound) {
		return nil, err
	}
	return second.Get(ctx, c)
}

// Close closes both plane pipelines (sealing open segments, draining
// flush queues). Safe to call once.
func (s *Store) Close(ctx context.Context) error {
	derr := s.data.Close(ctx)
	cerr := s.catalog.Close(ctx)
	return errors.Join(derr, cerr)
}
