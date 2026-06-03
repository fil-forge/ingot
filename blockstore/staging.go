package blockstore

import (
	"context"
	"errors"
	"fmt"
	"sync"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// OpStaging is a per-S3-op IpldBlockstore that captures every Put —
// body chunks, MST nodes, ObjectManifests — in memory. On Commit it
// hands the entire ordered batch to the log store in one
// fsynced AppendBatch call, after which the new bucket Root may be
// safely advanced via the registry CAS.
//
// Reads check the in-memory buffer first and fall through to the
// underlying read store on miss. This lets MST.GetPointer recompute
// path Put a node and immediately re-Read it during the same op.
//
// Single-shot per session: create at the start of an S3 op, Put any
// number of blocks, then call Commit(root) on success or Discard on
// failure. Failed ops never touch the log because nothing is written
// until Commit.
//
// TODO(perf): the in-memory `blocks` map bounds the transaction's
// memory footprint at the size of the entire payload until Commit.
// For a multi-GB PutObject this means the full body — every chunk,
// every MST node, the manifest — sits in process memory until the
// log accepts the batch.
//
// An alternative OpStaging implementation could spool to a temp
// file (CAR-shaped, with an in-memory cid → (offset, length) index
// for read-your-writes) instead of an unbounded map, capping the
// per-transaction footprint to roughly one chunk plus the index.
// The interface (Get / Put / Commit / Discard) does not need to
// change — only the storage backend behind these methods.
//
// Discard would unlink the temp file; Commit could hand the file
// off to Log.AppendBatch (or a future SubmitCAR-style entry point
// that takes the path directly) to avoid materializing the batch
// as a Go slice in the hot path.
type OpStaging struct {
	underlying ReadStore
	log        Log
	bucket     string

	mu sync.RWMutex
	// blocks holds every Put for the lifetime of the transaction,
	// across BOTH planes, so read-your-writes (MST.GetPointer
	// re-reading a freshly-put node; a ranged GET re-reading a
	// freshly-put chunk) works regardless of plane. See the
	// TODO(perf) on OpStaging — this is the field a file-backed
	// implementation would replace.
	blocks map[string]block.Block // keyed by string(cid.Bytes())
	// dataOrder / catOrder preserve Put order within each plane.
	// On Commit they become the two block slices handed to
	// Log.AppendBatch. Classification is by CID codec — see
	// isDataBlock and the plane-split invariant below.
	dataOrder []cid.Cid
	catOrder  []cid.Cid
}

// isDataBlock reports whether a block belongs to the data plane.
//
// INVARIANT: the data plane is exactly the raw-codec leaf blocks the
// body codec emits (bucket.FixedChunker.putRawBlock uses cid.Raw);
// everything else — ObjectManifests, FixedChunkerIndex docs, MST
// nodes — is dag-cbor and belongs to the catalog plane. If a future
// body codec emits non-raw body DAG nodes, this seam must be replaced
// with an explicit per-write kind rather than a codec sniff.
func isDataBlock(c cid.Cid) bool {
	return c.Prefix().Codec == cid.Raw
}

// NewOpStaging constructs a per-op staging buffer. underlying is the
// read fallback (typically *Layered); log is the durable write
// target; bucket is the bucket whose root this op will advance.
func NewOpStaging(underlying ReadStore, log Log, bucket string) *OpStaging {
	return &OpStaging{
		underlying: underlying,
		log:        log,
		bucket:     bucket,
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
	return b.underlying.GetBlock(ctx, c)
}

func (b *OpStaging) Put(_ context.Context, blk block.Block) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := string(blk.Cid().Bytes())
	if _, exists := b.blocks[key]; exists {
		return nil
	}
	b.blocks[key] = blk
	if isDataBlock(blk.Cid()) {
		b.dataOrder = append(b.dataOrder, blk.Cid())
	} else {
		b.catOrder = append(b.catOrder, blk.Cid())
	}
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

	data := make([]block.Block, len(b.dataOrder))
	for i, c := range b.dataOrder {
		data[i] = b.blocks[string(c.Bytes())]
	}
	cat := make([]block.Block, len(b.catOrder))
	for i, c := range b.catOrder {
		cat[i] = b.blocks[string(c.Bytes())]
	}
	if err := b.log.AppendBatch(ctx, data, cat, OpRoot{Bucket: b.bucket, Root: root}); err != nil {
		return fmt.Errorf("opstaging: append: %w", err)
	}

	b.blocks = map[string]block.Block{}
	b.dataOrder = nil
	b.catOrder = nil
	return nil
}

// Discard drops any staged blocks without writing them. Use when the
// surrounding op has failed and the in-flight batch should be
// abandoned.
func (b *OpStaging) Discard() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocks = map[string]block.Block{}
	b.dataOrder = nil
	b.catOrder = nil
}

// OpStaging is passed to CborStore in bucketop.Tx construction, so
// it must satisfy BaseStore (the IPFS-standard Get/Put-on-blocks
// shape). The two halves are: Get → check in-memory map then fall
// through to the underlying ReadStore's GetBlock; Put → append to
// the in-memory map.
var _ BaseStore = (*OpStaging)(nil)
