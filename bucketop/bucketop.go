// Package bucketop provides the per-bucket write-transaction primitive for
// ingot. A Tx snapshots the bucket's published Root from the registry and
// exposes a per-op staging buffer that writes through to the LSM log. A
// queue-bucket.commit applies its changes to one Tx in turn, each advancing
// the working root, and then appends the staged blocks to the log in one
// fsynced batch (Append); the root's advance and the rows the commit owes are
// written with the task. The bucket's subject on the queue serializes its
// commits, so a Tx needs no lock of its own.
//
// Read paths bypass bucketop. They only need the read-side blockstore
// directly; tx-style ceremony would be pure overhead.
package bucketop

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/fil-forge/ucantone/did"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// ErrBucketNotFound is returned by Begin when the bucket doesn't
// exist in the registry. Callers map this to NoSuchBucket at the
// protocol layer.
var ErrBucketNotFound = errors.New("bucketop: bucket not found")

// Deps wires the Coordinator to its three dependencies. Every field
// is an interface so tests can supply fakes without standing up an on-disk
// log or a network blockstore.
type Deps struct {
	// Reg tracks per-bucket Root. Begin reads State.
	Reg registry.Registry

	// Log is the durability boundary. Tx.Append calls
	// log.AppendBatch with the per-tx blocks plus an op-root
	// record of (bucket, newRoot). In production this is the
	// per-bucket *logstore.Manager: the op-root's bucket routes
	// each batch to that bucket's own log, whose sealed segments
	// ship to the bucket's Forge space.
	Log blockstore.Log

	// Reads is the read tier the staging buffer falls through to
	// on miss during the transaction.
	Reads blockstore.ReadStore

	// Ships, when set, is handed each commit's request just before its
	// blocks are appended: the segment they go into ships later, outside any
	// request, under authority captured from the bucket's writes. Nil
	// captures nothing.
	Ships ShipAuthority
}

// ShipAuthority records, from a write's request, the authority the catalog
// log's asynchronous ship to the bucket's space signs with
// (uploader.Uploader).
type ShipAuthority interface {
	CaptureShipAuthority(ctx context.Context, space did.DID)
}

// Coordinator hands out Tx instances that share the same Deps, and owns the
// log's shutdown. One per ingot backend.
type Coordinator struct {
	deps Deps
}

// NewCoordinator returns a Coordinator wired to the given deps.
func NewCoordinator(deps Deps) *Coordinator {
	return &Coordinator{deps: deps}
}

// Begin starts a write transaction against bucket: it snapshots the
// bucket's State from the registry and allocates a per-op staging buffer
// plus a CBOR view over it. It returns ErrBucketNotFound when the bucket
// doesn't exist. The caller runs under the bucket's subject on the bucket
// queue, and MUST defer tx.Discard().
//
// The bucket name is cloned defensively: protocol layers like
// versitygw/fiber return string headers that alias the request
// buffer (valid only inside the handler), and we persist the
// bucket name in Tx.bucket → OpRoot.Bucket → segment.opRoots,
// which the bucket's ship reads after the handler returns.
func (c *Coordinator) Begin(ctx context.Context, bucket string) (*Tx, error) {
	bucket = strings.Clone(bucket)
	state, err := c.deps.Reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, fmt.Errorf("bucketop: get bucket %q: %w", bucket, err)
	}
	staging := blockstore.NewOpStaging(c.deps.Reads, c.deps.Log, bucket, state.Space)
	return &Tx{
		deps:    c.deps,
		bucket:  bucket,
		state:   state,
		start:   state.Root,
		staging: staging,
		cst:     blockstore.CborStore(staging),
	}, nil
}

// Close shuts down the underlying log — with the per-bucket
// manager, every bucket's log: each seals its open segment and stops
// its seal ticker. Close is one-shot at process shutdown; subsequent
// Begin calls are not safe.
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
	start   cid.Cid
	staging *blockstore.OpStaging
	cst     cbor.IpldStore

	// commit is what the root's advance carries besides the root: the upload
	// registrations this commit queues and the reference index rows of the
	// versions it publishes and discards. They are written with the root,
	// so none can describe a version that did not commit, nor miss one that
	// did.
	commit registry.RootCommit
	// gc are the catalog blocks the commit's changes made garbage.
	gc []cid.Cid
}

// Mark is a point in a Tx to return to: the working root and what the
// commit carries at that point.
type Mark struct {
	root                    cid.Cid
	regs, claims, drops, gc int
}

// Mark returns the transaction's current point, for Restore.
func (tx *Tx) Mark() Mark {
	return Mark{tx.state.Root, len(tx.commit.Registrations), len(tx.commit.Claims), len(tx.commit.Drops), len(tx.gc)}
}

// Restore returns the transaction to m, dropping what the commit gained
// since: a change that failed on its own terms leaves nothing behind but the
// blocks it staged, which nothing references.
func (tx *Tx) Restore(m Mark) {
	tx.state.Root = m.root
	tx.commit.Registrations = tx.commit.Registrations[:m.regs]
	tx.commit.Claims = tx.commit.Claims[:m.claims]
	tx.commit.Drops = tx.commit.Drops[:m.drops]
	tx.gc = tx.gc[:m.gc]
}

// Advance makes root the working root, which the next change's LoadTree
// starts from.
func (tx *Tx) Advance(root cid.Cid) { tx.state.Root = root }

// AddGCCandidate records a catalog block the commit made garbage, for the
// commit to record as a GC candidate.
func (tx *Tx) AddGCCandidate(c cid.Cid) { tx.gc = append(tx.gc, c) }

// EnqueueUploadRegistration records a change to the space's content-entry
// list for this commit to carry. Commits run one at a time on the bucket's
// subject, so a bucket's registrations are queued in commit order, the order
// a key's changes have to replay in.
func (tx *Tx) EnqueueUploadRegistration(reg taskrunner.Spec) {
	tx.commit.Registrations = append(tx.commit.Registrations, reg)
}

// ClaimBlobs records the new generation's claims on its body digests for
// this commit to carry. A claim whose write's pending reference has expired
// fails the commit with registry.ErrReferenceExpired, and nothing is written.
func (tx *Tx) ClaimBlobs(claims []registry.BlobClaim) {
	tx.commit.Claims = append(tx.commit.Claims, claims...)
}

// DropClaims records the claims of a discarded generation for this commit
// to remove. A drop of the space's last claim on a digest queues the blob's
// release with the commit, due grace after it and carrying release's
// authority: the grace is measured once the catalog is fsynced, not here.
func (tx *Tx) DropClaims(drops []registry.ClaimDrop, grace time.Duration, release registry.ReleaseTask) {
	tx.commit.Drops = append(tx.commit.Drops, drops...)
	tx.commit.ReleaseGrace = grace
	tx.commit.Release = release
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

// State returns the bucket's State as snapshotted at Begin, with Root the
// working root.
func (tx *Tx) State() *registry.State { return tx.state }

// Get fetches a CBOR-encoded value at c into out. Tx satisfies
// blockstore.Store (Get + Put) and blockstore.Reader (Get) so it
// can be passed directly to mst.LoadMST and
// MerkleSearchTree.GetPointer. The space parameter is accepted for
// interface conformance; the Tx's own bucket space (bound into the
// staging buffer at Begin) is what reaches the network tier.
func (tx *Tx) Get(ctx context.Context, _ did.DID, c cid.Cid, out any) error {
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
func (tx *Tx) GetBlock(ctx context.Context, _ did.DID, c cid.Cid) (block.Block, error) {
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
		return mst.LoadMST(tx, tx.state.Space, tx.state.Root)
	}
	return mst.NewEmptyMST(tx, tx.state.Space)
}

// Appended is what Append made durable, for the commit's writes
// (registry.Postgres.RootCommitWrites): the root the transaction began from,
// the root it advances to, and what the advance carries.
type Appended struct {
	Expect, Next cid.Cid
	Commit       registry.RootCommit
	GC           []cid.Cid
}

// Append appends the staged blocks and an op-root of (bucket, root) to the
// log in one fsynced batch and returns what the commit writes, or false
// when no change moved the root. The append is what the segment will ship
// whatever becomes of the commit, so the ship's authority is captured first:
// an append that fills the segment can seal it at once. A write with no body
// (a delete, a tag change) captures none anywhere else.
//
// An append whose commit then fails leaves its op-root in the log with a
// root the bucket never published; a ship advances forge_root_cid through
// it, as MarkSegmentShipped's TODO describes.
func (tx *Tx) Append(ctx context.Context) (_ Appended, ok bool, err error) {
	if tx.state.Root == tx.start {
		return Appended{}, false, nil
	}
	ctx, span := tracing.Start(ctx, "bucket.append")
	defer func() { tracing.End(span, err) }()
	if tx.deps.Ships != nil {
		tx.deps.Ships.CaptureShipAuthority(ctx, tx.state.Space)
	}
	appended, err := tx.staging.Commit(ctx, tx.state.Root)
	if err != nil {
		return Appended{}, false, fmt.Errorf("bucketop: append: %w", err)
	}
	c := tx.commit
	c.Journal = appended
	return Appended{Expect: tx.start, Next: tx.state.Root, Commit: c, GC: tx.gc}, true, nil
}

// Discard drops the staged blocks without writing them. Idempotent, and a
// no-op after Append.
func (tx *Tx) Discard() {
	tx.staging.Discard()
}
