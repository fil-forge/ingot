package blockstore

import (
	"context"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// OpRoot ties a single batch of block writes to the bucket Root
// they collectively materialize. Every AppendBatch on a Log
// records exactly one OpRoot; the flush pipeline replays these
// to advance per-bucket forge_root_cid as segments ship.
type OpRoot struct {
	// Bucket is the bucket whose Root this batch advances.
	Bucket string
	// Root is the new MST root the batch produces.
	Root cid.Cid
}

// BlockLoc points at a block's payload bytes inside a CAR file —
// the byte offset of the frame and the frame length. Logs
// populate one entry per block at append time; consumers (most
// notably the flush path that builds a ShardedDagIndexView) read
// the entries to avoid rescanning the file.
type BlockLoc struct {
	Offset uint64
	Length uint64
}

// Log is the journaling tier — an append-only block store with
// three levels of durability:
//
//   - Hot:  the open segment. AppendBatch fsyncs the batch into
//     the segment's CAR + ops sidecar before returning, so a
//     successful AppendBatch is durable on local disk before any
//     acked write becomes visible to clients.
//   - Warm: sealed segments retained on local disk. Reads hit
//     them via Get newest-first; Append never touches them.
//   - Cold: segments flushed off-host (to Forge in production).
//     Out of scope for the Log interface — the implementation
//     manages the flush pipeline outside this contract.
//
// Get is the seam Layered uses to consult the journal before
// falling through to the network base — it returns ErrNotFound
// when no local segment holds the requested CID. Close drains
// the flush pipeline at process shutdown.
//
// Implemented by *logstore.Store.
type Log interface {
	AppendBatch(ctx context.Context, blocks []block.Block, opRoot OpRoot) error
	Get(ctx context.Context, c cid.Cid) (block.Block, error)
	Close(ctx context.Context) error
}
