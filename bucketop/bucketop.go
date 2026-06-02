// Package bucketop provides the per-bucket write-transaction
// primitive for ingot. Each Tx snapshots the bucket's published Root
// from the registry, exposes a per-op staging buffer that
// write-throughs to the LSM log, and on Commit fsyncs the buffer
// into one log.AppendBatch and CAS-advances the bucket Root from
// the snapshotted value to the caller-supplied newRoot — atomically
// from the caller's perspective.
//
// The package owns the four-way wiring (registry, log, layered read
// tier, MST CBOR view) that S3 verb implementations would otherwise
// compose by hand for every PUT/DELETE. It also owns the per-bucket
// lock map so concurrent transactions against the same bucket
// serialize within a single process and the CAS in Commit always
// sees a fresh snapshot.
//
// Read paths bypass bucketop. They only need the read-side
// blockstore directly; tx-style ceremony would be pure overhead.
package bucketop

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
)

// ErrBucketNotFound is returned by Begin when the bucket doesn't
// exist in the registry. Callers map this to NoSuchBucket at the
// protocol layer.
var ErrBucketNotFound = errors.New("bucketop: bucket not found")

// Deps wires the Coordinator to its three dependencies. Every field
// is an interface so tests can supply in-memory equivalents without
// standing up Postgres, an on-disk log, or a network blockstore.
type Deps struct {
	// Reg tracks per-bucket Root. Begin reads State; Commit
	// CAS-advances Root from the snapshot to newRoot.
	Reg registry.Registry

	// Log is the durability boundary. Tx.Commit calls
	// log.AppendBatch with the per-tx blocks plus an op-root
	// record of (bucket, newRoot).
	Log blockstore.Log

	// Reads is the read tier the staging buffer falls through to
	// on miss during the transaction.
	Reads blockstore.ReadStore
}

// Coordinator manages per-bucket transactions. One per ingot backend.
// Its job is to hand out Tx instances that share the same Deps,
// serialize concurrent transactions per bucket, and own the log's
// shutdown.
type Coordinator struct {
	deps Deps

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// NewCoordinator returns a Coordinator wired to the given deps.
func NewCoordinator(deps Deps) *Coordinator {
	return &Coordinator{
		deps:  deps,
		locks: map[string]*sync.Mutex{},
	}
}

// Begin starts a write transaction against bucket. Steps:
//  1. Acquire the per-bucket lock.
//  2. Snapshot the bucket's State from the registry. If the bucket
//     doesn't exist, release the lock and return ErrBucketNotFound.
//  3. Allocate a per-op staging buffer plus a CBOR view over it.
//
// Caller MUST defer tx.Discard() and call tx.Commit on success.
// Both Commit and Discard are idempotent against the lock — either
// one releases it; calling the other afterwards is a no-op.
//
// The bucket name is cloned defensively: protocol layers like
// versitygw/fiber return string headers that alias the request
// buffer (valid only inside the handler), and we persist the
// bucket name in Tx.bucket → OpRoot.Bucket → segment.opRoots,
// which the async flush path reads after the handler returns.
func (c *Coordinator) Begin(ctx context.Context, bucket string) (*Tx, error) {
	bucket = strings.Clone(bucket)
	release := c.Lock(bucket)

	state, err := c.deps.Reg.Get(ctx, bucket)
	if err != nil {
		release()
		if errors.Is(err, registry.ErrNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, fmt.Errorf("bucketop: get bucket %q: %w", bucket, err)
	}

	staging := blockstore.NewOpStaging(c.deps.Reads, c.deps.Log, bucket)
	return &Tx{
		deps:    c.deps,
		bucket:  bucket,
		state:   state,
		staging: staging,
		cst:     blockstore.CborStore(staging),
		release: release,
	}, nil
}

// Lock acquires the per-bucket lock without starting a transaction
// and returns a release func the caller MUST defer. Used by
// non-write operations that still need to serialize against
// concurrent writes — DeleteBucket, for example, walks the MST to
// confirm the bucket is empty and then deletes the registry row;
// without serialization a concurrent PUT could squeeze in between.
//
// Most callers should prefer WithLock, which removes the
// defer-or-leak hazard.
func (c *Coordinator) Lock(bucket string) func() {
	lock := c.lockFor(bucket)
	lock.Lock()
	return lock.Unlock
}

// MutateFn is the closure passed to WithTx. It receives the
// transaction's bucket-state snapshot and the per-op staging
// view, and returns the MST root the transaction should advance
// to.
//
//   - Returning (newRoot, nil) with newRoot.Defined() commits the
//     transaction: log.AppendBatch fsyncs the staging buffer and
//     reg.CASRoot advances the bucket Root.
//   - Returning (cid.Undef, nil) signals "no-op success": the
//     staging buffer is discarded with no log append and no Root
//     advance. Used by S3 DELETE-on-missing-key, which is
//     idempotent: the protocol wants a 200 even though the tree
//     didn't change.
//   - Returning (_, non-nil err) discards and propagates err.
type MutateFn func(ctx context.Context, tx *Tx) (newRoot cid.Cid, err error)

// WithTx runs fn against a fresh transaction. Begin/Commit/Discard
// happen automatically based on what fn returns; the caller can
// neither leak the bucket lock by forgetting Discard nor leak
// in-flight bytes by forgetting Commit.
//
// Errors mapped to the caller:
//   - ErrBucketNotFound from Begin propagates verbatim (fn is not
//     invoked).
//   - registry.ErrConflict from the inner CASRoot propagates
//     wrapped (only reachable in cross-process races; the
//     in-process bucket lock prevents it within one Coordinator).
//   - Any error fn returns propagates verbatim.
func (c *Coordinator) WithTx(ctx context.Context, bucket string, fn MutateFn) error {
	tx, err := c.Begin(ctx, bucket)
	if err != nil {
		return err
	}

	newRoot, fnErr := fn(ctx, tx)
	if fnErr != nil {
		tx.Discard()
		return fnErr
	}
	if !newRoot.Defined() {
		tx.Discard()
		return nil
	}
	return tx.Commit(ctx, newRoot)
}

// LockFn is the closure passed to WithLock. It runs while the
// per-bucket lock is held; the lock is released as soon as fn
// returns, regardless of whether fn errored.
type LockFn func(ctx context.Context) error

// WithLock runs fn while holding the per-bucket lock. Counterpart
// to WithTx for non-mutating bucket-level operations
// (DeleteBucket's empty-check + delete; future bucket-policy
// updates).
func (c *Coordinator) WithLock(ctx context.Context, bucket string, fn LockFn) error {
	release := c.Lock(bucket)
	defer release()
	return fn(ctx)
}

func (c *Coordinator) lockFor(bucket string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m, ok := c.locks[bucket]; ok {
		return m
	}
	m := &sync.Mutex{}
	c.locks[bucket] = m
	return m
}

// Close shuts down the underlying log: seals the open segment,
// drains the flush queue, and updates per-bucket forge_root_cid
// for every op_root contained in flushed segments. After Close
// returns cleanly, every acked write is durable in Forge or
// scheduled to ship. Close is one-shot at process shutdown;
// subsequent Begin/Lock calls are not safe.
func (c *Coordinator) Close(ctx context.Context) error {
	return c.deps.Log.Close(ctx)
}

// Tx is a single-bucket write transaction. It exposes four I/O
// methods (Get/Put for CBOR, GetBlock/PutBlock for raw bytes) so
// callers don't have to reach for the underlying blockstore /
// IpldStore views — the four interface assertions below pin the
// contracts the rest of pkg/ingot relies on.
type Tx struct {
	deps    Deps
	bucket  string
	state   *registry.State
	staging *blockstore.OpStaging
	cst     blockstore.Store

	// release is the bucket-lock release closure. Set by Begin;
	// nil-ed by finalize() so Commit and Discard mutually agree
	// that the lock has been released exactly once.
	release func()
}

// Compile-time assertions: Tx is the canonical handle through which
// the rest of pkg/ingot reaches into the per-op staging buffer, so
// it must satisfy each of the contracts at the call sites.
var (
	_ blockstore.Store       = (*Tx)(nil) // Get, Put     → manifest CBOR + MST.GetPointer
	_ blockstore.Reader      = (*Tx)(nil) // Get          → mst.LoadMST / NewEmptyMST
	_ blockstore.BlockReader = (*Tx)(nil) // GetBlock     → OpenBody / OpenBodyRange
	_ blockstore.BlockWriter = (*Tx)(nil) // PutBlock     → PutBody
)

// State returns the bucket's State as snapshotted at Begin. The
// reported Root is the value Commit will CAS against.
func (tx *Tx) State() *registry.State { return tx.state }

// Get fetches a CBOR-encoded value at c into out. Tx satisfies
// blockstore.Store (Get + Put) and blockstore.Reader (Get) so it
// can be passed directly to mst.LoadMST and
// MerkleSearchTree.GetPointer.
func (tx *Tx) Get(ctx context.Context, c cid.Cid, out any) error {
	return tx.cst.Get(ctx, c, out)
}

// Put CBOR-encodes v into the per-op staging buffer and returns
// its CID. Tx satisfies blockstore.Store via Get + Put.
func (tx *Tx) Put(ctx context.Context, v any) (cid.Cid, error) {
	return tx.cst.Put(ctx, v)
}

// GetBlock fetches a raw block from the per-tx view: staging
// buffer first, then the layered read store. Satisfies
// bucket.BlockReader so OpenBody / OpenBodyRange can read from
// freshly-staged chunks during the same op (rare but consistent).
func (tx *Tx) GetBlock(ctx context.Context, c cid.Cid) (block.Block, error) {
	return tx.staging.Get(ctx, c)
}

// PutBlock writes a raw block into the per-op staging buffer.
// Satisfies bucket.BlockWriter so PutBody can stream chunks
// directly through the Tx without the caller threading a separate
// blockstore.
func (tx *Tx) PutBlock(ctx context.Context, blk block.Block) error {
	return tx.staging.Put(ctx, blk)
}

// LoadTree returns the bucket's MST loaded from State().Root, or a
// fresh empty MST if the bucket has no objects yet. Mutations on
// the returned tree flow into the per-op staging buffer because
// the tree is loaded with Tx as its store (Tx satisfies blockstore.Reader,
// and MST writes only happen at GetPointer time, which takes its
// writer as an explicit argument).
func (tx *Tx) LoadTree() *mst.MerkleSearchTree {
	if tx.state.Root.Defined() {
		return mst.LoadMST(tx, tx.state.Root)
	}
	return mst.NewEmptyMST(tx)
}

// Commit finalizes the transaction:
//  1. log.AppendBatch fsyncs the staging buffer into the open log
//     segment with an op-root of (bucket, newRoot).
//  2. registry.CASRoot advances the bucket Root from State().Root
//     to newRoot.
//  3. The bucket lock is released.
//
// Returns registry.ErrConflict if another writer raced ahead of us
// (only possible across processes — within a single process the
// per-bucket lock prevents it). On any error the lock is still
// released; defer-Discard becomes a no-op.
//
// Failure mode worth knowing: if step 1 succeeds but step 2 fails
// (transient Postgres error, context cancellation between the two
// calls), the op_root is durable in the log even though the bucket
// Root never advanced. The flusher will eventually see this op_root
// and — today — blindly advance forge_root_cid to it, leaving
// forge_root_cid pointing at an orphan Root the bucket never
// published. See the TODO in pkg/ingot/registry/segments.go's
// MarkSegmentFlushed for the planned conditional-update fix.
func (tx *Tx) Commit(ctx context.Context, newRoot cid.Cid) error {
	if tx.release == nil {
		return errors.New("bucketop: tx already finalized")
	}
	defer tx.finalize()

	if err := tx.staging.Commit(ctx, newRoot); err != nil {
		return fmt.Errorf("bucketop: append: %w", err)
	}
	if err := tx.deps.Reg.CASRoot(ctx, tx.bucket, tx.state.Root, newRoot); err != nil {
		return fmt.Errorf("bucketop: advance root: %w", err)
	}
	return nil
}

// Discard rolls back the staging buffer (drops staged blocks
// without writing) and releases the bucket lock. Idempotent — safe
// to defer at the top of every operation regardless of whether
// Commit eventually runs.
func (tx *Tx) Discard() {
	if tx.release == nil {
		return
	}
	tx.staging.Discard()
	tx.finalize()
}

func (tx *Tx) finalize() {
	if tx.release != nil {
		tx.release()
		tx.release = nil
	}
}
