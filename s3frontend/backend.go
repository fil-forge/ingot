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
// Operations not implemented (lifecycle, bucket policies, etc.)
// inherit ErrNotImplemented from the embedded
// backend.BackendUnsupported. The few unsupported-by-default
// methods that versitygw nevertheless calls on every request
// (GetBucketAcl, GetBucketPolicy, GetBucketCors) are stubbed in
// bucket.go. Object lock is implemented: the bucket configuration in
// bucket.go, the per-version retention / legal-hold methods in
// objectlock.go (docs/s3-object-lock.md). So is tagging: the
// per-version object methods in objecttag.go, the bucket-level tag
// set in buckettag.go (docs/s3-object-tagging.md).
package s3frontend

import (
	"context"
	"encoding/xml"
	"sync"
	"time"

	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/backend"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/regionkey"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/tenantkey"
	"github.com/fil-forge/ingot/uploader"
)

// Backend implements versitygw's backend.Backend directly over the
// ingot domain primitives. The embedded BackendUnsupported supplies
// ErrNotImplemented defaults for every operation; we override only
// the ones we actually serve.
type Backend struct {
	backend.BackendUnsupported

	read      blockstore.ReadStore
	authority bucketauthority.BucketAuthority
	reg       registry.Registry
	intents   registry.IntentStore
	locations registry.LocationStore
	blobRefs  registry.BlobRefStore
	gc        registry.GCStore
	multipart registry.MultipartStore
	txns      *bucketop.Coordinator
	log       blockstore.Log
	spool     *blockstore.Spool
	cache     *blockstore.BlobCache
	uploader  uploader.BodyUploader
	deferred  uploader.DeferredBodyUploader
	parks     registry.ParkStore
	remover   uploader.BlobRemover
	encParams registry.EncryptionParamsStore
	// streaming sends a body blob to its provider while it is spooled;
	// streams records each such upload until its park or acceptance is
	// recorded.
	streaming uploader.StreamingBodyUploader
	streams   registry.StreamStore
	// pendingReleases is the deferred-release queue; releaseGrace is how far
	// past the last-claim drop each release is scheduled (readers holding the
	// prior catalog root get at least this long to finish their prefetch).
	pendingReleases registry.PendingReleaseStore
	releaseGrace    time.Duration
	// Local blob sweeper knobs (see Deps). localBlobSweepMu serialises
	// SweepLocalBlobs; lastOrphanPass is when its orphan pass last ran.
	// localBlobSweepBatch overrides the rows a pass reads per query, for
	// tests; zero takes the default.
	localBlobMaxBytes   int64
	cacheMinResidency   time.Duration
	cacheReadRetention  time.Duration
	localBlobOrphanAge  time.Duration
	localBlobSweepMu    sync.Mutex
	lastOrphanPass      time.Time
	localBlobSweepBatch int
	// overBudgetWarned and timeLimitLogged are set once the sweeper has
	// logged that usage is over budget with nothing left to evict, or that an
	// eviction pass (budget or forced, whichever ran out first) ran out of
	// time, and cleared when usage is back under, so each message comes once
	// per episode. lastForcedWarn is when the forced pass last warned that it
	// evicted inside the retention windows; a forced pass usually brings
	// usage back under budget, so that warning is limited by time instead.
	// Guarded by localBlobSweepMu.
	overBudgetWarned bool
	timeLimitLogged  bool
	lastForcedWarn   time.Time
	// localBlobMetrics counts local blob removals; localBlobGauges is the
	// registration of the usage and budget gauges (nil when not registered).
	localBlobMetrics localBlobMetrics
	localBlobGauges  metric.Registration
	// regionKeys unwraps region-wrapped CEKs for the decrypting read path.
	regionKeys regionkey.Provider
	// tenantKeys yields the tenant wrap key each write encrypts to (the FEE
	// tenant recipient). Writes fail without it.
	tenantKeys tenantkey.Source
	logger     *zap.Logger

	maxBlobSize int64
	// cors is Deps.CORS marshalled once at construction — GetBucketCors
	// is on the per-request path, so the document is built here rather
	// than per call. Nil when CORS is disabled.
	cors []byte
}

// Deps wires a Backend over ingot's domain primitives.
type Deps struct {
	Authority bucketauthority.BucketAuthority
	// Registry tracks per-bucket roots; IntentStore tracks each local blob's
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

	// Reads is the layered read tier (local blobs → log → forge). Log is the
	// catalog LSM write log driving the per-op staging buffer + commit — in
	// production the per-bucket *logstore.Manager, which routes each append
	// to the bucket's own log.
	Reads blockstore.ReadStore
	Log   blockstore.Log

	// Spool is where SplitBody writes body blobs on PUT and where each waits
	// until the provider holds it; Cache then holds it as a read-after-write
	// copy until it is evicted. Reads serves both (blockstore.LocalBlobs).
	// Both are required.
	Spool *blockstore.Spool
	Cache *blockstore.BlobCache

	// Uploader makes each spooled body blob durable on Forge (allocate→PUT→
	// accept) synchronously, before the manifest commits. Remover releases a
	// space's claim on a blob when its last reference is dropped.
	Uploader uploader.BodyUploader
	// Deferred extends Uploader for multipart's deferred accept
	// (WithConclude(false), then ConcludeBlobs/AbortBlob); Parks persists
	// park state between UploadPart and Complete/Abort.
	Deferred uploader.DeferredBodyUploader
	Parks    registry.ParkStore
	Remover  uploader.BlobRemover

	// Streaming uploads each body blob while it is spooled,
	// allocating it by size and hash function before its digest is known;
	// Streams records each such upload until its park or acceptance is recorded.
	// Without Streaming every blob is spooled first and uploaded by digest.
	// Streams is required with it.
	Streaming uploader.StreamingBodyUploader
	Streams   registry.StreamStore

	// EncParams is the per-blob FEE encryption-parameter table: what the
	// decrypting read path needs to serve an encrypted blob. RegionKeys
	// unwraps its region-wrapped CEKs. Both required — which implementation
	// backs the provider (OpenBao in production, in-process for tests and
	// development) is configuration, but bucket encryption is not optional.
	EncParams  registry.EncryptionParamsStore
	RegionKeys regionkey.Provider
	// TenantKeys resolves the requesting tenant's wrap key: the X25519
	// public key every stored object is encrypted to as a COSE recipient (the
	// encryption RFC's insurance copy, recoverable without the region).
	// Required: a write that cannot obtain it fails.
	TenantKeys tenantkey.Source

	// PendingReleases is the deferred-release queue (blob_release_intents):
	// a last-claim drop enqueues here and the release sweeper executes after
	// ReleaseGrace. Same instance as Registry in production. Required.
	PendingReleases registry.PendingReleaseStore
	// ReleaseGrace schedules each release this far past its last-claim drop.
	// Zero means immediately due (the server applies the production default
	// before construction; tests use zero so a manual sweep drains).
	ReleaseGrace time.Duration

	// LocalBlobMaxBytes is the byte budget for the spool and the cache
	// together, writes in progress included, enforced by SweepLocalBlobs.
	// Zero turns the budget and forced passes off: eviction needs a network
	// read tier to serve evicted blobs, which the in-memory fakes do not
	// have.
	LocalBlobMaxBytes int64
	// CacheMinResidency is how long after its last state change (for a
	// committed blob, its commit) the budget pass leaves a blob alone, so a
	// client reading back what it just wrote reads from local disk. Zero
	// turns it off.
	CacheMinResidency time.Duration
	// CacheReadRetention is how long after a read from the cache the budget
	// pass leaves a blob alone, so objects read repeatedly stay local. Zero
	// turns it off.
	CacheReadRetention time.Duration
	// LocalBlobOrphanAge is the age at which SweepLocalBlobs deletes a .tmp-*
	// file in either directory, or a spool blob file with no intent row. Zero →
	// DefaultLocalBlobOrphanAge.
	LocalBlobOrphanAge time.Duration

	// MeterProvider supplies the spool's instruments: usage and budget
	// gauges, and removals by reason. Nil → the global provider, a no-op
	// until a host installs one.
	MeterProvider metric.MeterProvider

	// MaxBlobSize is the coarse-split blob ceiling (0 → bucket default).
	MaxBlobSize int64

	// CORS is the S3 CORS configuration GetBucketCors reports for every
	// bucket, rendered from config by internal/cors. New marshals it once
	// into the XML document the S3 API serves. Nil disables CORS.
	CORS *auth.CORSConfiguration

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
	// xml.Marshal cannot fail for CORSConfiguration's plain string and
	// int fields, but a failure must not leave the gateway serving a
	// half-configured document: log it and fall back to CORS disabled.
	var corsDoc []byte
	if d.CORS != nil {
		doc, err := xml.Marshal(d.CORS)
		if err != nil {
			logger.Error("marshalling CORS configuration; CORS disabled", zap.Error(err))
		} else {
			corsDoc = doc
		}
	}
	localBlobOrphanAge := d.LocalBlobOrphanAge
	if localBlobOrphanAge <= 0 {
		localBlobOrphanAge = DefaultLocalBlobOrphanAge
	}
	b := &Backend{
		authority:       d.Authority,
		read:            d.Reads,
		reg:             d.Registry,
		intents:         d.Intents,
		locations:       d.Locations,
		blobRefs:        d.BlobRefs,
		gc:              d.GC,
		multipart:       d.Multipart,
		txns:            bucketop.NewCoordinator(bucketop.Deps{Reg: d.Registry, Log: d.Log, Reads: d.Reads}),
		log:             d.Log,
		spool:           d.Spool,
		cache:           d.Cache,
		uploader:        d.Uploader,
		deferred:        d.Deferred,
		parks:           d.Parks,
		remover:         d.Remover,
		streaming:       d.Streaming,
		streams:         d.Streams,
		encParams:       d.EncParams,
		regionKeys:      d.RegionKeys,
		tenantKeys:      d.TenantKeys,
		pendingReleases: d.PendingReleases,
		releaseGrace:    d.ReleaseGrace,

		localBlobMaxBytes:  d.LocalBlobMaxBytes,
		cacheMinResidency:  d.CacheMinResidency,
		cacheReadRetention: d.CacheReadRetention,
		localBlobOrphanAge: localBlobOrphanAge,

		logger:      logger,
		maxBlobSize: d.MaxBlobSize,
		cors:        corsDoc,
	}
	mp := d.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	b.localBlobMetrics, b.localBlobGauges = newLocalBlobMetrics(mp, b, logger)
	return b
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
