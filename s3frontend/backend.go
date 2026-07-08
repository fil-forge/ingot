// Package s3frontend implements versitygw's backend.Backend by
// orchestrating directly over the ingot domain primitives. It is the
// only S3 frontend ingot ships; it is wired into the process via
// pkg/ingot.Server.
//
// The Backend type is a thin protocol adapter:
//   - Read paths drive a single ReadStore that exposes both
//     CBOR-decoded reads (manifest, MST nodes) and raw block reads
//     (body chunks). The interface has no Put method, so write paths
//     can't accidentally route through it.
//   - Write paths drive a per-op bucketop.Tx, which owns the
//     staging buffer, MST CBOR view, bucket-Root CAS, and per-bucket
//     locking.
//
// Operations not implemented (multipart, lifecycle, locking,
// versioning, etc.) inherit ErrNotImplemented from the embedded
// backend.BackendUnsupported. The few unsupported-by-default
// methods that versitygw nevertheless calls on every request
// (GetBucketAcl, GetBucketPolicy, GetObjectLockConfiguration,
// GetBucketVersioning) are stubbed in bucket.go.
package s3frontend

import (
	"context"

	"github.com/fil-forge/versitygw/backend"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// Backend implements versitygw's backend.Backend directly over the
// ingot domain primitives. The embedded BackendUnsupported supplies
// ErrNotImplemented defaults for every operation; we override only
// the ones we actually serve.
type Backend struct {
	backend.BackendUnsupported

	read      blockstore.ReadStore
	reg       registry.Registry
	intents   registry.IntentStore
	locations registry.LocationStore
	blobRefs  registry.BlobRefStore
	gc        registry.GCStore
	multipart registry.MultipartStore
	txns      *bucketop.Coordinator
	log       blockstore.Log
	spool     *blockstore.Spool
	uploader  uploader.BodyUploader
	remover   uploader.BlobRemover
	logger    *zap.Logger

	maxBlobSize int64
}

// Deps wires a Backend over ingot's domain primitives.
type Deps struct {
	// Registry tracks per-bucket roots; IntentStore tracks the local spool's
	// upload_intents lifecycle; LocationStore records where each accepted body
	// blob can be retrieved from. Production passes one *registry.Postgres for
	// all three; the harness one *inmem.MemStore.
	Registry  registry.Registry
	Intents   registry.IntentStore
	Locations registry.LocationStore

	// BlobRefs is the reverse reference index (which versions reference each
	// blob); GC records superseded MST/manifest CIDs. Same instance as Registry.
	BlobRefs  registry.BlobRefStore
	GC        registry.GCStore
	Multipart registry.MultipartStore

	// Reads is the layered read tier (spool → log → forge). Log is the catalog
	// LSM write log driving the per-op staging buffer + commit — in production
	// the per-bucket *logstore.Manager, which routes each append to the
	// bucket's own log.
	Reads blockstore.ReadStore
	Log   blockstore.Log

	// Spool is the local blob store: SplitBody writes body blobs here on PUT,
	// and they are served back from here on GET (read-after-write / cache).
	Spool *blockstore.Spool

	// Uploader makes each spooled body blob durable on Forge (allocate→PUT→
	// accept) synchronously, before the manifest commits. Remover releases a
	// space's claim on a blob when its last reference is dropped.
	Uploader uploader.BodyUploader
	Remover  uploader.BlobRemover

	// MaxBlobSize is the coarse-split blob ceiling (0 → bucket default).
	MaxBlobSize int64

	// Logger is optional; defaults to zap.NewNop().
	Logger *zap.Logger
}

// Compile-time assertion that Backend satisfies versitygw's interface.
var _ backend.Backend = (*Backend)(nil)

// New constructs a Backend wired over ingot's domain primitives.
func New(d Deps) *Backend {
	logger := d.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Backend{
		read:        d.Reads,
		reg:         d.Registry,
		intents:     d.Intents,
		locations:   d.Locations,
		blobRefs:    d.BlobRefs,
		gc:          d.GC,
		multipart:   d.Multipart,
		txns:        bucketop.NewCoordinator(bucketop.Deps{Reg: d.Registry, Log: d.Log, Reads: d.Reads}),
		log:         d.Log,
		spool:       d.Spool,
		uploader:    d.Uploader,
		remover:     d.Remover,
		logger:      logger,
		maxBlobSize: d.MaxBlobSize,
	}
}

// String identifies this backend in versitygw logs.
func (*Backend) String() string { return "ingot" }

// Shutdown is a no-op; lifecycle for the underlying registry/log is
// owned by pkg/ingot.Server's Stop hook, not by versitygw.
func (*Backend) Shutdown() {}

// Recover is a no-op in the LSM design: logstore.Open already
// scanned the segment directory, reconciled with Postgres, and
// re-enqueued any pending segments for the background flusher.
// Recover is retained as the lifecycle seam in case future
// invariants need verifying before the listener accepts traffic.
func (b *Backend) Recover(_ context.Context) error { return nil }

// Drain shuts the log down via the Coordinator: seals the open
// segment, drains the flush queue, and updates per-bucket
// forge_root_cid for every op_root that landed in a flushed
// segment. After Drain returns cleanly, no acked write is
// unrepresented in Postgres.
func (b *Backend) Drain(ctx context.Context) error {
	if b.txns == nil {
		return nil
	}
	return b.txns.Close(ctx)
}
