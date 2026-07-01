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

	"github.com/versity/versitygw/backend"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/logstore"
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
	txns      *bucketop.Coordinator
	spool     *blockstore.Spool
	uploader  uploader.BodyUploader

	// space is the Forge space this instance owns; the key under which body
	// blobs' locations (and, later, reference claims) are recorded. Empty in
	// the in-memory harness, where reads are served from the spool.
	space       string
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

	// Reads is the layered read tier (spool → log → forge). Log is the catalog
	// LSM write log driving the per-op staging buffer + commit.
	Reads blockstore.ReadStore
	Log   *logstore.Store

	// Spool is the local blob store: SplitBody writes body blobs here on PUT,
	// and they are served back from here on GET (read-after-write / cache).
	Spool *blockstore.Spool

	// Uploader makes each spooled body blob durable on Forge (allocate→PUT→
	// accept) synchronously, before the manifest commits.
	Uploader uploader.BodyUploader

	// Space is the Forge space this instance owns (empty in the harness).
	Space string

	// MaxBlobSize is the coarse-split blob ceiling (0 → bucket default).
	MaxBlobSize int64
}

// Compile-time assertion that Backend satisfies versitygw's interface.
var _ backend.Backend = (*Backend)(nil)

// New constructs a Backend wired over ingot's domain primitives.
func New(d Deps) *Backend {
	return &Backend{
		read:        d.Reads,
		reg:         d.Registry,
		intents:     d.Intents,
		locations:   d.Locations,
		txns:        bucketop.NewCoordinator(bucketop.Deps{Reg: d.Registry, Log: d.Log, Reads: d.Reads}),
		spool:       d.Spool,
		uploader:    d.Uploader,
		space:       d.Space,
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
