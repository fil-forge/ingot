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
// Operations not implemented (lifecycle, etc.) inherit
// ErrNotImplemented from the embedded backend.BackendUnsupported. The
// few unsupported-by-default methods that versitygw nevertheless calls
// on every request are answered: GetBucketAcl is stubbed in bucket.go,
// and GetBucketCors (bucketcors.go) serves a bucket's own document or the
// gateway-wide one. The bucket policy operations forward to the bucket
// authority (policy.go), where GetBucketPolicy also answers versitygw's
// own access checks without it. Object lock is implemented: the bucket configuration in
// bucket.go, the per-version retention / legal-hold methods in
// objectlock.go (docs/s3-object-lock.md). So is tagging: the
// per-version object methods in objecttag.go, the bucket-level tag
// set in buckettag.go (docs/s3-object-tagging.md).
package s3frontend

import (
	"cmp"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/backend"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/internal/authority"
	"github.com/fil-forge/ingot/queue/shardqueue"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/queue/spaceuploadqueue"
	"github.com/fil-forge/ingot/regionkey"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
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
	gc        registry.GCStore
	multipart registry.MultipartStore
	txns      *bucketop.Coordinator
	log       blockstore.Log
	cache     *blockstore.BlobCache
	deferred  uploader.DeferredBodyUploader
	parks     registry.ParkStore
	remover   uploader.BlobRemover
	registrar uploader.UploadRegistrar
	// counts keeps each version's place in its space's object count.
	counts *spaceuploadqueue.Queue
	// shards keeps the cache's copies.
	shards *shardqueue.Queue
	// proofs finds the authority queued tasks make their network calls
	// with, and holds what requests lend for their spaces while they wait on
	// them.
	proofs    *authority.Resolver
	encParams registry.EncryptionParamsStore
	// streaming sends each body blob to its provider as the body arrives.
	streaming uploader.StreamingBodyUploader
	// claims counts a blob's claims; releaseGrace is how far past the
	// last-claim drop each release is due (readers holding the prior catalog
	// root get at least this long to finish their prefetch).
	claims registry.BlobRefStore
	// blobs runs the tasks on one blob in one space: a write's references,
	// its accept, its release.
	blobs *spaceshardqueue.Queue
	// buckets runs the queue-bucket tasks.
	buckets *taskrunner.Runner
	// uploads runs the queue-multipart-upload tasks.
	uploads *taskrunner.Runner
	// acceptDelay is how long after a part lands its upload's accept is due:
	// none, but tests delay it so that only they run it.
	acceptDelay time.Duration
	// multipartTTL is how long an upload stays open before its auto-abort,
	// and a completed one is kept (Deps.MultipartSessionTTL).
	multipartTTL time.Duration
	// shipMu guards the ship fields below. shipOwed are the buckets whose
	// ship could not be queued, which retryOwedShips queues once it can;
	// shipRetrying is set while it runs, and shipStop, closed by Drain, ends
	// it.
	shipMu   sync.Mutex
	shipOwed map[string]bool
	// shipAwaiting are the buckets whose ship found no authority to send
	// with, which the bucket's next commit queues again (awaitShipAuthority).
	shipAwaiting map[string]bool
	shipRetrying bool
	shipStop     chan struct{}
	shipStopped  bool
	releaseGrace time.Duration
	// Local blob sweeper knobs (see Deps). localBlobSweepMu serialises
	// SweepLocalBlobs; lastOrphanPass is when its orphan pass last ran.
	localBlobMaxBytes  int64
	localBlobOrphanAge time.Duration
	dropAcceptedBodies bool
	localBlobSweepMu   sync.Mutex
	lastOrphanPass     time.Time
	// overBudgetWarned and timeLimitLogged are set once the sweeper has
	// logged that usage is over budget with nothing left to evict, or that the
	// budget pass ran out of time, and cleared when usage falls back to the
	// low watermark, so each message comes once per episode. Guarded by
	// localBlobSweepMu.
	overBudgetWarned bool
	timeLimitLogged  bool
	// stalledUploading is the sweeper's latest sum of stalled uploads (see
	// SweepLocalBlobs), read by the stalled_bytes gauge; stalledKnown is set
	// once a sweep has computed it.
	stalledUploading atomic.Int64
	stalledKnown     atomic.Bool
	// localBlobMetrics counts local blob removals; localBlobGauges is the
	// registration of the usage and budget gauges (nil when not registered).
	localBlobMetrics localBlobMetrics
	localBlobGauges  metric.Registration
	// multipartMetrics counts part blobs' concludes and deferred Completes;
	// multipartGauges is the registration of the parked-age gauge (nil when
	// not registered).
	multipartMetrics multipartMetrics
	multipartGauges  metric.Registration
	// journalGauges is the registration of the catalog journal gauges (nil
	// when not registered).
	journalGauges metric.Registration
	// catalogUsage reports each bucket's catalog storage on Forge (nil: no
	// catalog usage gauges). catalogSnapshot is its latest report, which the
	// gauges registered as catalogGauges read.
	catalogUsage    registry.CatalogUsageStore
	catalogSnapshot atomic.Pointer[[]registry.CatalogUsage]
	catalogGauges   metric.Registration
	// regionKeys unwraps region-wrapped CEKs for the decrypting read path.
	regionKeys regionkey.Provider
	// tenantKeys yields the tenant wrap key each write encrypts to (the FEE
	// tenant recipient). Writes fail without it.
	tenantKeys tenantkey.Source
	logger     *zap.Logger

	maxBlobSize         int64
	allowNoneEncryption bool
	rehashBudget        int64
	// cors is Deps.CORS marshalled once at construction — GetBucketCors
	// is on the per-request path, so the document is built here rather
	// than per call. It is the default for a bucket without a document of
	// its own (bucketcors.go). Nil when CORS is disabled gateway-wide.
	cors []byte
}

// Deps wires a Backend over ingot's domain primitives.
type Deps struct {
	// Tasks runs the backend's task queues; New registers their handlers on
	// it, and the host starts it. Required.
	Tasks *taskrunner.Runner

	Authority bucketauthority.BucketAuthority
	// Registry tracks per-bucket roots; IntentStore tracks each local blob's
	// upload_intents lifecycle; LocationStore records where each accepted body
	// blob can be retrieved from. Production and tests pass one
	// *registry.Postgres for all three.
	Registry  registry.Registry
	Intents   registry.IntentStore
	Locations registry.LocationStore

	// GC records superseded MST/manifest CIDs. Same instance as Registry.
	GC        registry.GCStore
	Multipart registry.MultipartStore

	// Reads is the layered read tier (local blobs → log → forge). Log is the
	// catalog LSM write log driving the per-op staging buffer + commit — in
	// production the per-bucket *logstore.Manager, which routes each append
	// to the bucket's own log.
	Reads blockstore.ReadStore
	Log   blockstore.Log

	// Cache holds a read-after-write copy of each object-body blob written
	// here, from when the provider holds it until it is evicted. Reads serves
	// it first. Required.
	Cache *blockstore.BlobCache

	// Ships is the catalog log's ship uploader. Each commit hands it its
	// request's proof store as the bucket space's ship authority
	// (bucketop.Deps.Ships). Optional.
	Ships bucketop.ShipAuthority

	// Deferred concludes and aborts blobs parked on their providers
	// (ConcludeBlobs/AbortBlob); Parks persists park state between
	// UploadPart and Complete/Abort. Remover releases a space's claim on a
	// blob when its last reference is dropped.
	Deferred uploader.DeferredBodyUploader
	Parks    registry.ParkStore
	Remover  uploader.BlobRemover
	// Registrar keeps the upload service's content-entry list in step with the
	// catalog: one entry per committed object version, which is what the
	// service counts to report the space's object count.
	Registrar uploader.UploadRegistrar

	// Streaming uploads each body blob as the body arrives, allocating it by
	// size and hash function before its digest is known. Required.
	Streaming uploader.StreamingBodyUploader

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

	// Claims counts the claims on a blob, which a release rechecks before it
	// removes the blob. Same instance as Registry in production. Required.
	Claims registry.BlobRefStore
	// ReleaseGrace schedules each release this far past its last-claim drop.
	// Zero means immediately due (the server applies the production default
	// before construction; tests use zero so releases run as soon as they are
	// queued).
	ReleaseGrace time.Duration
	// MultipartSessionTTL bounds a multipart upload: an open one is aborted
	// this long after its creation, and a completed one kept this long after
	// its Complete. Zero means 7 days; negative, never.
	MultipartSessionTTL time.Duration

	// LocalBlobMaxBytes is the byte budget for the cache, writes in progress
	// included, enforced by SweepLocalBlobs.
	// Zero turns the budget pass off: eviction needs a network read tier to
	// serve evicted blobs, which the test fakes do not have.
	LocalBlobMaxBytes int64
	// CacheTTL is how long a blob stays in the cache after it was cached or
	// last read, unless the budget pass evicts it first. Zero →
	// shardqueue.DefaultTTL.
	CacheTTL time.Duration
	// LocalBlobOrphanAge is the age at which SweepLocalBlobs deletes a .tmp-*
	// file in the cache. Zero → DefaultLocalBlobOrphanAge.
	LocalBlobOrphanAge time.Duration
	// DropAcceptedBodies keeps no local copy of a body, so every read goes to
	// its provider. Off in tests whose base tier cannot serve a body. It
	// gives up the read-after-write copy, which is safe while reads resolve
	// through the local location table; an indexer-backed locator could miss
	// a digest not yet published.
	DropAcceptedBodies bool

	// MeterProvider supplies the cache's instruments: usage and budget
	// gauges, and removals by reason. Nil → the global provider, a no-op
	// until a host installs one.
	MeterProvider metric.MeterProvider

	// CatalogUsage, when set, is what the catalog usage gauges report each
	// bucket's catalog storage on Forge from, refreshed by
	// RefreshCatalogUsage. Typically the same instance as Registry.
	CatalogUsage registry.CatalogUsageStore

	// MaxBlobSize is the coarse-split blob ceiling (0 → bucket default).
	MaxBlobSize int64

	// AllowNoneEncryption lets PutBucketEncryption set a bucket's default
	// encryption to the non-standard "none", under which objects are stored
	// as received. Off, the deployment stores every object encrypted.
	AllowNoneEncryption bool

	// RehashBudget bounds the bytes CompleteMultipartUpload reads back to
	// hash non-final parts whose assumed object offset was wrong (0 →
	// DefaultRehashBudget); the final part is always re-hashed. A completion
	// needing more is refused with InvalidPartLayout.
	RehashBudget int64

	// CORS is the S3 CORS configuration GetBucketCors reports for a bucket
	// that has stored none of its own, rendered from config by
	// internal/cors. New marshals it once into the XML document the S3 API
	// serves. Nil disables CORS for such buckets.
	CORS *auth.CORSConfiguration

	// Logger is optional; defaults to zap.NewNop().
	Logger *zap.Logger

	// tuneBlobs adjusts the queue-space-shard dependencies, for tests.
	tuneBlobs func(*spaceshardqueue.Deps)
}

// Compile-time assertion that Backend satisfies versitygw's interface.
var _ backend.Backend = (*Backend)(nil)

// New constructs a Backend wired over ingot's domain primitives, and
// registers its task handlers on d.Tasks.
func New(d Deps) (*Backend, error) {
	if d.Tasks == nil {
		return nil, errors.New("s3frontend: no task runner")
	}
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
		authority:    d.Authority,
		read:         d.Reads,
		reg:          d.Registry,
		intents:      d.Intents,
		locations:    d.Locations,
		gc:           d.GC,
		multipart:    d.Multipart,
		txns:         bucketop.NewCoordinator(bucketop.Deps{Reg: d.Registry, Log: d.Log, Reads: d.Reads, Ships: d.Ships}),
		log:          d.Log,
		cache:        d.Cache,
		deferred:     d.Deferred,
		parks:        d.Parks,
		remover:      d.Remover,
		streaming:    d.Streaming,
		registrar:    d.Registrar,
		proofs:       authority.New(d.Registrar),
		encParams:    d.EncParams,
		regionKeys:   d.RegionKeys,
		tenantKeys:   d.TenantKeys,
		claims:       d.Claims,
		releaseGrace: d.ReleaseGrace,
		multipartTTL: d.MultipartSessionTTL,

		localBlobMaxBytes:  d.LocalBlobMaxBytes,
		localBlobOrphanAge: localBlobOrphanAge,
		dropAcceptedBodies: d.DropAcceptedBodies,

		logger:              logger,
		maxBlobSize:         d.MaxBlobSize,
		allowNoneEncryption: d.AllowNoneEncryption,
		rehashBudget:        cmp.Or(d.RehashBudget, DefaultRehashBudget),
		cors:                corsDoc,
		catalogUsage:        d.CatalogUsage,
	}
	mp := d.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	b.localBlobMetrics, b.localBlobGauges = newLocalBlobMetrics(mp, b, logger)
	b.multipartMetrics, b.multipartGauges = newMultipartMetrics(mp, b, logger)
	b.journalGauges = newJournalGauges(mp, b, logger)
	b.catalogGauges = newCatalogUsageGauges(mp, b, logger)
	var err error
	if b.shards, err = shardqueue.New(d.Tasks, shardqueue.Deps{Cache: d.Cache, TTL: d.CacheTTL, Removed: b.cacheRemoved, Logger: logger}); err == nil {
		b.counts, err = spaceuploadqueue.New(d.Tasks, spaceuploadqueue.Deps{Registrar: d.Registrar, Authority: b.proofs, Logger: logger})
	}
	if err == nil {
		bd := spaceshardqueue.Deps{
			Intents:   d.Intents,
			Parks:     d.Parks,
			Claims:    d.Claims,
			Locations: d.Locations,
			EncParams: d.EncParams,
			Remover:   d.Remover,
			Deferred:  d.Deferred,
			Streaming: d.Streaming,
			Shards:    b.shards,
			Authority: b.proofs,
			Concluded: b.multipartMetrics.pass,
			Logger:    logger,
		}
		if d.tuneBlobs != nil {
			d.tuneBlobs(&bd)
		}
		b.blobs, err = spaceshardqueue.New(d.Tasks, bd)
	}
	if err == nil {
		err = b.registerTasks(d.Tasks)
	}
	if err != nil {
		_ = b.CloseMetrics()
		return nil, fmt.Errorf("s3frontend: register tasks: %w", err)
	}
	return b, nil
}

// registerTasks registers every queue's handlers with r.
func (b *Backend) registerTasks(r *taskrunner.Runner) error {
	for _, register := range []func(*taskrunner.Runner) error{
		b.registerMultipartTasks,
		b.registerBucketTasks,
		b.registerAcceptTasks,
	} {
		if err := register(r); err != nil {
			return err
		}
	}
	return nil
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

// Drain shuts the log down via the Coordinator: it seals each bucket's
// open segment, which ships from the bucket's queue once the log is open
// again. After Drain returns cleanly, no acked write is unrepresented in
// Postgres.
func (b *Backend) Drain(ctx context.Context) error {
	b.stopShipRetries()
	if b.txns == nil {
		return nil
	}
	return b.txns.Close(ctx)
}
