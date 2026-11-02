package blockstore

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// OpStaging is a per-S3-op IpldBlockstore that captures every Put — MST nodes
// and ObjectManifests — in memory. On Commit it hands the entire ordered batch
// to the log store in one fsynced AppendBatch call, after which the new bucket
// Root may be safely advanced via the registry CAS.
//
// Only catalog (dag-cbor) blocks reach OpStaging: object-body blobs are spooled
// and uploaded per-blob before the commit, so they never pass through the
// per-op staging buffer or the log.
//
// Reads check the in-memory buffer first and fall through to the
// underlying read store on miss. This lets MST.GetPointer recompute
// path Put a node and immediately re-Read it during the same op.
//
// Single-shot per session: create at the start of an S3 op, Put any
// number of blocks, then call Commit(root) on success or Discard on
// failure. Failed ops never touch the log because nothing is written
// until Commit.
type OpStaging struct {
	underlying ReadStore
	log        Log
	bucket     string
	// space is the bucket's Forge space, bound in so the cbor-gen-shaped
	// Get below (fixed, space-less signature) can reach the network tier
	// with the right space on fallthrough.
	space did.DID

	mu sync.RWMutex
	// blocks holds every Put for the lifetime of the transaction so
	// read-your-writes (MST.GetPointer re-reading a freshly-put node) works.
	blocks map[string]block.Block // keyed by string(cid.Bytes())
	// order preserves Put order; on Commit it becomes the block slice handed to
	// Log.AppendBatch (the catalog plane).
	order []cid.Cid
}

// NewOpStaging constructs a per-op staging buffer. underlying is the
// read fallback (typically *Layered); log is the durable write
// target; bucket is the bucket whose root this op will advance and
// space its Forge space (for network fallthrough on reads).
func NewOpStaging(underlying ReadStore, log Log, bucket string, space did.DID) *OpStaging {
	return &OpStaging{
		underlying: underlying,
		log:        log,
		bucket:     bucket,
		space:      space,
		blocks:     map[string]block.Block{},
	}
}

func (b *OpStaging) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	b.mu.RLock()
	blk, ok := b.blocks[string(c.Bytes())]
	b.mu.RUnlock()
	if ok {
		return blk, nil
	}
	return b.underlying.GetBlock(ctx, b.space, c)
}

func (b *OpStaging) Put(_ context.Context, blk block.Block) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := string(blk.Cid().Bytes())
	if _, exists := b.blocks[key]; exists {
		return nil
	}
	b.blocks[key] = blk
	b.order = append(b.order, blk.Cid())
	return nil
}

// Commit hands every staged block + (bucket, root) to the log in one
// AppendBatch. After Commit returns nil, the blocks AND the op-root
// are durable on disk; the caller may advance the bucket's published
// Root.
//
// An empty blocks slice is legal: an MST mutation can produce a
// new root that points at a node already materialized in a prior
// segment (e.g., trimTop after Delete unwraps to an existing
// subtree). The bucket Root still needs to advance, so AppendBatch
// is called with an empty payload and the OpRoot record alone.
func (b *OpStaging) Commit(ctx context.Context, root cid.Cid) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !root.Defined() {
		return errors.New("opstaging: commit with undefined root")
	}

	cat := make([]block.Block, len(b.order))
	for i, c := range b.order {
		cat[i] = b.blocks[string(c.Bytes())]
	}
	if err := b.log.AppendBatch(ctx, cat, OpRoot{Bucket: b.bucket, Root: root}); err != nil {
		return fmt.Errorf("opstaging: append: %w", err)
	}

	b.blocks = map[string]block.Block{}
	b.order = nil
	return nil
}

// Discard drops any staged blocks without writing them. Use when the
// surrounding op has failed and the in-flight batch should be
// abandoned.
func (b *OpStaging) Discard() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocks = map[string]block.Block{}
	b.order = nil
}

// OpStaging is passed to CborStore in bucketop.Tx construction, so
// it must satisfy BaseStore (the IPFS-standard Get/Put-on-blocks
// shape). The two halves are: Get → check in-memory map then fall
// through to the underlying ReadStore's GetBlock; Put → append to
// the in-memory map.
var _ BaseStore = (*OpStaging)(nil)
