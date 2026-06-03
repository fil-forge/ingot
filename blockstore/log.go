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
// TODO(forrest): replace Offset and Length with Start and End to
// align with libforge semantics
type BlockLoc struct {
	Offset uint64
	Length uint64
}

// Plane distinguishes the two block planes a segment splits into.
// Each plane is its own CAR file with an independent ship/retain
// lifecycle:
//
//   - PlaneData: the data plane — raw-codec object-body chunks. The
//     actual bytes a client GETs.
//   - PlaneCatalog: the control plane — the dag-cbor MST nodes,
//     ObjectManifests, and chunk indexes that describe where the body
//     bytes live and how to reconstruct an object.
//
// Block classification is by CID codec: cid.Raw → PlaneData, anything
// else (dag-cbor) → PlaneCatalog. See OpStaging.Put.
type Plane int

const (
	// PlaneData is the object-body (raw chunk) plane.
	PlaneData Plane = iota
	// PlaneCatalog is the MST/manifest/index (dag-cbor) plane.
	PlaneCatalog
)

// Planes is the canonical iteration order over the two planes.
var Planes = [...]Plane{PlaneData, PlaneCatalog}

// String renders a Plane for logs and filenames.
func (p Plane) String() string {
	switch p {
	case PlaneData:
		return "data"
	case PlaneCatalog:
		return "catalog"
	default:
		return "unknown"
	}
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
// AppendBatch takes the batch split by plane: dataBlocks (raw chunks)
// land in the segment's data CAR, catalogBlocks (dag-cbor) in its
// catalog CAR. Both CARs plus the op-root record are fsynced before
// AppendBatch returns, so a successful append is durable across both
// planes before the bucket Root is allowed to advance.
//
// Implemented by *logstore.Store.
type Log interface {
	AppendBatch(ctx context.Context, dataBlocks, catalogBlocks []block.Block, opRoot OpRoot) error
	Get(ctx context.Context, c cid.Cid) (block.Block, error)
	Close(ctx context.Context) error
}
