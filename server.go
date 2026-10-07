package ingot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/did/web"
	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/metrics"
	"github.com/fil-forge/versitygw/s3api"
	"github.com/fil-forge/versitygw/s3event"
	"github.com/fil-forge/versitygw/s3log"
	"github.com/gofiber/fiber/v3"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/internal/fasthttputil"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/regionkey"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/s3frontend"
	"github.com/fil-forge/ingot/tenantkey"
	"github.com/fil-forge/ingot/uploader"
)

// ServerDeps bundles the runtime collaborators of an ingot Server
// behind interfaces. Production wiring uses Forge / Internal /
// Postgres; tests can substitute in-memory equivalents without
// standing up Postgres, piri, or the indexing-service.
type ServerDeps struct {
	// Logger is optional; defaults to zap.NewNop().
	Logger *zap.Logger

	// BaseBlockReader is the bottom tier of the layered read path —
	// what the log falls through to on misses. In production this is
	// *blockstore.Forge (network-backed via indexer + piri); in tests
	// it can be any IpldBlockstore.
	BaseBlockReader blockstore.BlockReader

	// Uploader is the destination for sealed catalog CAR segments.
	Uploader uploader.Uploader

	// BodyUploader makes each object-body blob durable on Forge by digest
	// (allocate→PUT→accept), synchronously during a PUT. Remover releases a
	// space's claim on a blob when its last reference is dropped. In tests both
	// are no-ops and reads are served from local disk.
	BodyUploader uploader.BodyUploader
	// Deferred extends BodyUploader for multipart's deferred accept:
	// park at UploadPart (WithConclude(false)), conclude at Complete,
	// abort at Abort.
	Deferred uploader.DeferredBodyUploader
	Remover  uploader.BlobRemover
	// Streaming, when set, uploads each body blob while it is
	// spooled, allocating it before its digest is known; Streams records
	// each such upload until its park or acceptance is recorded, and is
	// required with it. Without Streaming every blob is spooled first and
	// uploaded by digest.
	Streaming uploader.StreamingBodyUploader
	Streams   registry.StreamStore

	// Authority is the service that authorizes bucket creation and deletion.
	Authority bucketauthority.BucketAuthority

	// Registry tracks per-bucket roots. *registry.Postgres satisfies
	// both Registry and Meta in production; tests can supply two
	// separate implementations or one that does both.
	Registry registry.Registry

	// Intents tracks each local blob's upload_intents lifecycle; Locations
	// records where each accepted body blob (and shipped catalog shard) can be
	// retrieved from; Inclusions records each shipped shard's inner-block byte
	// ranges so retired catalog blocks stay resolvable; BlobRefs is the reverse
	// reference index; GC records superseded MST/manifest CIDs. Typically all
	// the same instance as Registry (*registry.Postgres / *inmem.MemStore).
	Intents    registry.IntentStore
	Locations  registry.LocationStore
	Inclusions registry.InclusionStore
	BlobRefs   registry.BlobRefStore
	GC         registry.GCStore
	Multipart  registry.MultipartStore
	// Parks persists deferred-accept park state between UploadPart and
	// Complete/Abort.
	Parks registry.ParkStore
	// PendingReleases is the deferred blob-release queue drained by the
	// release sweeper. Typically the same instance as Registry.
	PendingReleases registry.PendingReleaseStore

	// EncParams is the per-blob FEE encryption-parameter table the decrypting
	// read path consults; RegionKeys unwraps its region-wrapped CEKs. Both
	// required: which implementation backs the provider (OpenBao in
	// production, in-process for tests and development) is configuration,
	// but bucket encryption is not optional. EncParams is typically the same
	// instance as Registry.
	EncParams  registry.EncryptionParamsStore
	RegionKeys regionkey.Provider
	// TenantKeys resolves the requesting tenant's wrap key: the FEE tenant
	// recipient every write encrypts to. Required; writes fail without it.
	TenantKeys tenantkey.Source

	// Meta is the persistence backing for log-segment metadata.
	// Typically the same instance as Registry.
	Meta logstore.Meta

	// Identity is the agent identity (the issuer of every outbound
	// invocation). The listener serves its DID document at
	// /.well-known/did.json so peers can resolve a did:web agent to its
	// signing key (a did:key agent's document is served too; nothing needs
	// to fetch it). Required.
	Identity identity.Identity

	// IAM authenticates every access key (hilt/iam, which authorizes each
	// request against the Hilt tenant service). Required: versitygw's
	// built-in root account is disabled, so no request bypasses it.
	IAM auth.IAMService
}

// DeleteBucket reaches the catalog log's shipped-segment release through a
// runtime type assertion on this shape; pin *logstore.Manager to it at compile
// time so a signature drift is a build error rather than a silently skipped
// release.
var _ s3frontend.SegmentDigestLister = (*logstore.Manager)(nil)

// Server is a fully-wired ingot S3 listener. Use Start/Stop for
// lifecycle. fx callers wrap these in OnStart/OnStop hooks; tests
// call them directly.
type Server struct {
	cfg     config.ServerConfig
	logger  *zap.Logger
	log     blockstore.Log
	backend *s3frontend.Backend
	api     *s3api.S3ApiServer
	// sweepCtx is cancelled by Stop. Every background sweep derives its
	// context from it, and sweeps counts the sweeper goroutines, so Stop
	// can wait for them to exit before the registry closes.
	sweepCtx    context.Context
	sweepCancel context.CancelFunc
	sweeps      sync.WaitGroup
}

// New wires a ServerDeps + ServerConfig into a runnable Server. The
// caller is responsible for ensuring cfg.DataDir exists before
// calling.
func New(ctx context.Context, cfg config.ServerConfig, deps ServerDeps) (*Server, error) {
	if err := validateServerInputs(cfg, deps); err != nil {
		return nil, err
	}
	cfg = applyServerDefaults(cfg)

	logger := deps.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	// The log is segregated per bucket (each bucket's segments live under
	// segments/<bucket>/ and ship to that bucket's Forge space); the manager
	// stands in wherever the single global log stood.
	log, err := logstore.OpenManager(ctx, logstore.ManagerConfig{
		Dir:  filepath.Join(cfg.DataDir, "segments"),
		Meta: deps.Meta,
		Catalog: logstore.PlaneConfig{
			SealBytes: cfg.SealBytesCatalog,
			SealAge:   cfg.SealAgeCatalog,
			Ship:      cfg.ShipCatalog,
			Retain:    cfg.RetainCatalog,
		},
		FlushFor: func(bucket string) logstore.FlushFunc {
			return newBucketFlushFunc(deps.Uploader, deps.Registry, deps.Locations, deps.Inclusions, bucket, logger)
		},
		Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("ingot: logstore: %w", err)
	}

	// The spool and the cache share data_dir's filesystem, so moving a blob
	// from one to the other is a rename.
	spool, err := blockstore.NewSpool(filepath.Join(cfg.DataDir, "spool"))
	if err != nil {
		_ = log.Close(ctx)
		return nil, fmt.Errorf("ingot: spool: %w", err)
	}
	cache, err := blockstore.NewBlobCache(filepath.Join(cfg.DataDir, "cache"))
	if err != nil {
		_ = log.Close(ctx)
		return nil, fmt.Errorf("ingot: blob cache: %w", err)
	}
	if err := cache.CheckTake(spool); err != nil {
		_ = log.Close(ctx)
		return nil, fmt.Errorf("ingot: %w", err)
	}

	bs := blockstore.NewLayered(blockstore.LocalBlobs{Cache: cache, Spool: spool}, log, deps.BaseBlockReader)
	// The global meter provider is a no-op until a host (the daemon) installs
	// one, so counting costs nothing when nobody is listening.
	if err := bs.CountBlobReads(otel.Meter("github.com/fil-forge/ingot/blockstore")); err != nil {
		logger.Warn("local blob read metric not created", zap.Error(err))
	}
	backend := s3frontend.New(s3frontend.Deps{
		Authority:       deps.Authority,
		Registry:        deps.Registry,
		Intents:         deps.Intents,
		Locations:       deps.Locations,
		BlobRefs:        deps.BlobRefs,
		GC:              deps.GC,
		Multipart:       deps.Multipart,
		Parks:           deps.Parks,
		Reads:           bs,
		Log:             log,
		Spool:           spool,
		Cache:           cache,
		Uploader:        deps.BodyUploader,
		Deferred:        deps.Deferred,
		Remover:         deps.Remover,
		Streaming:       deps.Streaming,
		Streams:         deps.Streams,
		EncParams:       deps.EncParams,
		RegionKeys:      deps.RegionKeys,
		TenantKeys:      deps.TenantKeys,
		PendingReleases: deps.PendingReleases,
		ReleaseGrace:    cfg.ReleaseGrace,

		LocalBlobMaxBytes:  cfg.LocalBlobMaxBytes,
		CacheMinResidency:  cfg.CacheMinResidency,
		CacheReadRetention: cfg.CacheReadRetention,
		LocalBlobOrphanAge: cfg.LocalBlobOrphanAge,
		DropAcceptedBodies: cfg.DropAcceptedBodies,

		MaxBlobSize: cfg.MaxBlobSize,
		CORS:        cfg.CORSConfig,
		Logger:      logger,
	})

	api, err := buildS3API(ctx, backend, cfg, deps.IAM, deps.Identity, logger)
	if err != nil {
		// Best-effort cleanup if we got past the log open: the caller
		// has no Server handle to call Stop on.
		_ = backend.CloseMetrics()
		_ = log.Close(ctx)
		return nil, err
	}

	return &Server{
		cfg:     cfg,
		logger:  logger,
		log:     log,
		backend: backend,
		api:     api,
	}, nil
}

// Start runs Backend.Recover and spawns the S3 listener goroutine.
// Returns once the listener has been kicked off (does NOT wait for
// it to start serving on Addr). Call it at most once.
func (s *Server) Start(ctx context.Context) error {
	if err := s.backend.Recover(ctx); err != nil {
		// fx does not run OnStop for a hook whose OnStart failed, so the
		// gauges go now; Stop calling CloseMetrics again is harmless.
		_ = s.backend.CloseMetrics()
		return fmt.Errorf("ingot: recover: %w", err)
	}
	s.logger.Info("starting ingot S3 listener",
		zap.String("addr", s.cfg.Addr),
		zap.String("region", s.cfg.Region),
		zap.String("data_dir", s.cfg.DataDir),
		zap.Int64("max_blob_size", s.cfg.MaxBlobSize),
	)
	go func() {
		if err := s.api.ServeMultiPort([]string{s.cfg.Addr}); err != nil {
			s.logger.Error("ingot listener error", zap.Error(err))
		}
	}()
	s.sweepCtx, s.sweepCancel = context.WithCancel(context.Background())
	s.startMultipartSweeper()
	s.startReleaseSweeper()
	s.startLocalBlobSweeper()
	s.startReleasedPass()
	return nil
}

// goSweep runs fn in a sweeper goroutine that Stop waits for. fn must return
// promptly once s.sweepCtx is done. Its errors after that are the
// cancellation's, so the sweepers don't log them.
func (s *Server) goSweep(fn func()) {
	s.sweeps.Add(1)
	go func() {
		defer s.sweeps.Done()
		fn()
	}()
}

// startMultipartSweeper spawns the abandoned-multipart-session sweeper: open
// sessions older than MultipartSessionTTL are aborted (their parts, parked on
// their providers, released there) and terminal session rows reaped. Zero TTL → 7-day default;
// negative → disabled.
func (s *Server) startMultipartSweeper() {
	ttl := s.cfg.MultipartSessionTTL
	if ttl == 0 {
		ttl = 7 * 24 * time.Hour
	}
	if ttl < 0 {
		return
	}
	interval := ttl / 2
	if interval > 10*time.Minute {
		interval = 10 * time.Minute
	}
	s.goSweep(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.sweepCtx.Done():
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(s.sweepCtx, time.Minute)
				ctx, span := tracing.Start(ctx, "sweep.multipart_sessions")
				n, err := s.backend.SweepStaleMultipartSessions(ctx, ttl)
				s.endSweepSpan(span, err, attribute.Int("ingot.sweep.count", n))
				cancel()
				if err != nil && s.sweepCtx.Err() == nil {
					s.logger.Warn("multipart sweep", zap.Error(err))
				} else if n > 0 {
					s.logger.Info("multipart sweep reaped stale sessions", zap.Int("count", n))
				}
			}
		}
	})
}

// startReleaseSweeper spawns the deferred-release sweeper: release intents
// past their not_before (last-claim drop + ReleaseGrace) are executed —
// claim-count recheck, crypto-shred, location delete, network remove — with
// failures retried next tick. The interval floor keeps a tiny test grace
// from spinning a sub-second ticker.
func (s *Server) startReleaseSweeper() {
	interval := s.cfg.ReleaseGrace / 2
	if interval > 10*time.Minute {
		interval = 10 * time.Minute
	}
	if interval < time.Second {
		interval = time.Second
	}
	s.goSweep(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.sweepCtx.Done():
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(s.sweepCtx, time.Minute)
				ctx, span := tracing.Start(ctx, "sweep.releases")
				n, err := s.backend.SweepPendingReleases(ctx)
				s.endSweepSpan(span, err, attribute.Int("ingot.sweep.count", n))
				cancel()
				if err != nil && s.sweepCtx.Err() == nil {
					s.logger.Warn("release sweep", zap.Error(err))
				} else if n > 0 {
					s.logger.Info("release sweep executed deferred releases", zap.Int("count", n))
				}
				ctx, cancel = context.WithTimeout(s.sweepCtx, time.Minute)
				ctx, span = tracing.Start(ctx, "sweep.streams")
				n, err = s.backend.SweepStaleStreams(ctx)
				s.endSweepSpan(span, err, attribute.Int("ingot.sweep.count", n))
				cancel()
				if err != nil && s.sweepCtx.Err() == nil {
					s.logger.Warn("stream sweep", zap.Error(err))
				} else if n > 0 {
					s.logger.Info("stream sweep aborted abandoned uploads", zap.Int("count", n))
				}
			}
		}
	})
}

// localBlobSweepInterval is how often the local blob sweeper runs. The
// budget's 10% headroom must exceed ingest rate × this interval.
const localBlobSweepInterval = 30 * time.Second

// localBlobSweepLogInterval is the least time between the local blob
// sweeper's Info lines totalling its removals.
const localBlobSweepLogInterval = 10 * time.Minute

// startLocalBlobSweeper spawns the local blob sweeper: every
// localBlobSweepInterval it evicts blobs the provider holds down to
// LocalBlobMaxBytes (when set), and hourly it deletes orphan files (see
// Backend.SweepLocalBlobs).
func (s *Server) startLocalBlobSweeper() {
	s.goSweep(func() {
		// removed totals the removals since since, not yet logged.
		var removed s3frontend.LocalBlobSweepStats
		var lastLog time.Time
		since := time.Now()
		ticker := time.NewTicker(localBlobSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.sweepCtx.Done():
				return
			case <-ticker.C:
				// Each pass also caps itself; this bounds them together.
				ctx, cancel := context.WithTimeout(s.sweepCtx, 4*time.Minute)
				ctx, span := tracing.Start(ctx, "sweep.local_blobs")
				stats, err := s.backend.SweepLocalBlobs(ctx)
				span.SetAttributes(attribute.Int64("ingot.sweep.usage_bytes", s.backend.LocalBlobUsage()))
				s.endSweepSpan(span, err, stats.SpanAttributes()...)
				cancel()
				if err != nil && s.sweepCtx.Err() == nil {
					s.logger.Warn("local blob sweep", zap.Error(err))
				}
				// The removals are totalled and logged at most every
				// localBlobSweepLogInterval, so steady eviction shows at
				// Info without a line every sweep; the metrics carry each.
				removed.Add(stats)
				if removed.Removed() && time.Since(lastLog) >= localBlobSweepLogInterval {
					s.logger.Info("local blob sweeper removed files",
						append(removed.LogFields(),
							zap.Duration("over", time.Since(since)),
							zap.Int64("usage_bytes", s.backend.LocalBlobUsage()))...)
					removed, lastLog = s3frontend.LocalBlobSweepStats{}, time.Now()
				}
				if !removed.Removed() {
					since = time.Now()
				}
			}
		}
	})
}

// startReleasedPass removes the local copies and intents that releases kept
// before they freed local disk (see Backend.RemoveReleasedPublished). It runs
// at every startup, until it has gone through every candidate or Stop
// cancels it; on a node with none it is one scan.
func (s *Server) startReleasedPass() {
	s.goSweep(func() {
		ctx, span := tracing.Start(s.sweepCtx, "sweep.released_pass")
		files, bytes, err := s.backend.RemoveReleasedPublished(ctx)
		s.endSweepSpan(span, err,
			attribute.Int64("ingot.sweep.files", files),
			attribute.Int64("ingot.sweep.bytes", bytes))
		switch {
		case err != nil && s.sweepCtx.Err() == nil:
			s.logger.Warn("local blob released pass; the next restart tries again", zap.Error(err),
				zap.Int64("files", files), zap.Int64("bytes", bytes))
		case files > 0:
			s.logger.Info("local blob released pass removed copies earlier releases kept",
				zap.Int64("files", files), zap.Int64("bytes", bytes))
		}
	})
}

// Stop cancels the background sweeps and waits for them to exit (until ctx
// ends), then shuts the listener down and drains the log. Always returns the
// combined error of these steps so callers see all failure modes; none is
// fatal to the others.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("shutting down ingot S3 listener")

	var errs []error
	if s.sweepCancel != nil {
		s.sweepCancel()
		if err := s.waitSweeps(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if err := s.backend.CloseMetrics(); err != nil {
		errs = append(errs, fmt.Errorf("unregister local blob metrics: %w", err))
	}
	if err := s.api.ShutDown(); err != nil {
		errs = append(errs, fmt.Errorf("s3api shutdown: %w", err))
	}
	if err := s.backend.Drain(ctx); err != nil {
		errs = append(errs, fmt.Errorf("backend drain: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("ingot shutdown: %v", errs)
	}
	return nil
}

// endSweepSpan ends a background sweep's span with attrs. A sweep's error once
// sweepCtx is done is the cancellation's, as goSweep says, so it does not
// mark the span failed.
func (s *Server) endSweepSpan(span trace.Span, err error, attrs ...attribute.KeyValue) {
	span.SetAttributes(attrs...)
	if s.sweepCtx.Err() != nil {
		err = nil
	}
	tracing.End(span, err)
}

// waitSweeps waits for the sweeper goroutines to exit, or for ctx to end.
// A sweep stops at its next context check once sweepCtx is cancelled.
func (s *Server) waitSweeps(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.sweeps.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// An expired ctx and already-exited sweepers can both be ready.
		select {
		case <-done:
			return nil
		default:
		}
		return fmt.Errorf("waiting for background sweeps: %w", ctx.Err())
	}
}

// newBucketFlushFunc builds the logstore flush callback for one bucket's
// log: it ships a sealed catalog CAR to Forge via uploader.SubmitShard,
// then records the shard's location and every inner block's byte range
// in the local location/inclusion tables (the appliance mirror of the
// sharded-dag-index SubmitShard publishes). The store owns the
// ship-state transition (it stamps the segment's shipped timestamp and
// advances each affected bucket's forge_root_cid) once this returns nil —
// so a segment is only ever marked shipped (and thus eligible for
// retention) after its blocks are resolvable through the fallthrough read
// tier.
//
// A header-only CAR (a trimTop-to-existing-subtree writes no blocks) has
// no positions: nothing to ship, so the closure returns nil and the store
// still marks the segment shipped, letting retention reclaim the tiny CAR
// and advancing forge_root_cid for the recorded op-roots.
//
// The destination space is the bucket's, resolved from the registry at
// ship time (the log is segregated per bucket, so every segment this
// closure sees belongs to exactly this bucket). A bucket deleted while
// segments were still queued has nothing to ship to: the closure returns
// nil so the segment marks shipped and retires.
func newBucketFlushFunc(up uploader.Uploader, reg registry.Registry, locations registry.LocationStore, inclusions registry.InclusionStore, bucket string, logger *zap.Logger) logstore.FlushFunc {
	plane := blockstore.PlaneCatalog
	return func(ctx context.Context, seg *logstore.Segment) (multihash.Multihash, error) {
		positions := seg.Positions()
		if len(positions) == 0 {
			return nil, nil
		}
		st, err := reg.Get(ctx, bucket)
		if errors.Is(err, registry.ErrNotFound) {
			logger.Info("dropping segment for deleted bucket",
				zap.String("bucket", bucket), zap.Uint64("seq", seg.Seq()))
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("resolve space for %q: %w", bucket, err)
		}
		// Segment stores the raw 32-byte SHA-256 of the CAR file; the
		// uploader and ShardedDagIndexView want the multihash form.
		sha, err := multihash.Encode(seg.SHA256(), multihash.SHA2_256)
		if err != nil {
			return nil, fmt.Errorf("encode segment %d %s sha: %w", seg.Seq(), plane, err)
		}
		shard := uploader.CARShard{
			Path:      seg.CARPath(),
			Size:      seg.Size(),
			SHA256:    sha,
			Positions: positions,
		}
		carLoc, indexDigest, err := up.SubmitShard(ctx, plane, st.Space, shard)
		if err != nil {
			return nil, fmt.Errorf("submit segment %d %s for %q: %w", seg.Seq(), plane, bucket, err)
		}
		// No published location (the no-op uploader): nothing durable to
		// point reads at, so record nothing.
		if carLoc.Provider == "" || carLoc.URL == "" {
			return nil, nil
		}
		// Record the shard's location + every inner block's byte range
		// BEFORE returning: a flush error keeps the segment unshipped (and
		// retained), so retention can never retire blocks the read tier
		// can't resolve. Re-runs are idempotent (upserts) and the re-ship's
		// blob/add dedups on piri.
		if err := locations.PutLocation(ctx, registry.BlobLocation{
			Space:    st.Space,
			Digest:   sha,
			Provider: carLoc.Provider,
			URL:      carLoc.URL,
			Size:     seg.Size(),
		}); err != nil {
			return nil, fmt.Errorf("record segment %d %s location for %q: %w", seg.Seq(), plane, bucket, err)
		}
		incs := make([]registry.BlobInclusion, 0, len(positions))
		for c, loc := range positions {
			incs = append(incs, registry.BlobInclusion{
				Space:       st.Space,
				Digest:      c.Hash(),
				ShardDigest: sha,
				RangeStart:  int64(loc.Offset),
				RangeEnd:    int64(loc.Offset + loc.Length - 1),
			})
		}
		if err := inclusions.PutInclusions(ctx, incs); err != nil {
			return nil, fmt.Errorf("record segment %d %s inclusions for %q: %w", seg.Seq(), plane, bucket, err)
		}
		return indexDigest, nil
	}
}

// buildS3API constructs the versitygw S3ApiServer with the wiring ingot
// needs: no event sink, generous concurrency limits, and an audit-log sink
// that reports unexpected request failures through zap. Every access key
// authenticates through iam, which is required: versitygw's built-in root
// account is disabled (an empty RootUserConfig), since a root request never
// reaches hilt and so carries none of the delegations the Forge-facing
// handlers need. The server also publishes id's DID document at
// /.well-known/did.json.
func buildS3API(ctx context.Context, backend *s3frontend.Backend, cfg config.ServerConfig, iam auth.IAMService, id identity.Identity, logger *zap.Logger) (*s3api.S3ApiServer, error) {
	if iam == nil {
		return nil, fmt.Errorf("ingot: IAMService is required")
	}
	loggers, err := s3log.InitLogger(&s3log.LogConfig{})
	if err != nil {
		return nil, fmt.Errorf("ingot: loggers: %w", err)
	}
	loggers.S3Logger = &auditLogger{logger: logger}
	evSender, err := s3event.InitEventSender(&s3event.EventConfig{})
	if err != nil {
		return nil, fmt.Errorf("ingot: event sender: %w", err)
	}
	mm, err := metrics.NewManager(ctx, metrics.Config{})
	if err != nil {
		return nil, fmt.Errorf("ingot: metrics: %w", err)
	}

	opts := []s3api.Option{
		s3api.WithQuiet(),
		s3api.WithHealth("/health"),
		s3api.WithConcurrencyLimiter(cfg.MaxConnections, cfg.MaxRequests),
		// Without this the part-number ceiling defaults to 0 and every
		// UploadPart is rejected. 10000 is the S3 maximum.
		s3api.WithMpMaxParts(10000),
		// Ingot stores keys as opaque byte strings in an MST, not as
		// filesystem paths, so the path-traversal key guard does not apply:
		// keys like "../file.txt" are legal literal S3 keys and must round-trip.
		s3api.WithDisableObjNameTraversalCheck(),
		// Stash the signed S3 request on the context for every request. The
		// bucket-authority seam (Create/Delete/ListBuckets) recovers it to
		// forward to Hilt; doing it here, ahead of auth, covers the presigned
		// and POST-form auth paths as well as the header-signed one.
		// The request's server span. First, so it times everything after
		// the rate limiter: auth, routing and the backend call.
		s3api.WithMiddleware("/", tracing.Middleware()),
		s3api.WithMiddleware("/", func(c fiber.Ctx) error {
			c.Locals(reqscope.RequestKey(), fasthttputil.RequestFromHTTPContext(c.RequestCtx()))
			return c.Next()
		}),
	}
	// Public DID document for did:web resolution of the agent identity, so
	// hilt/sprue/piri can verify ingot's UCAN signatures. WithRoute mounts
	// ahead of the S3 route table, so this path is never read as the bucket
	// ".well-known" / key "did.json"; it is also outside auth (a DID document
	// is public by definition).
	doc, err := id.DIDDocument()
	if err != nil {
		return nil, fmt.Errorf("ingot: building the agent DID document: %w", err)
	}
	opts = append(opts, s3api.WithRoute(http.MethodGet, web.WellKnownDIDPath, didDocumentHandler(doc)))

	// No s3api.WithRootUser: the gateway has no root account, so every access
	// key resolves through iam.
	api, err := s3api.New(backend,
		cfg.Region, iam, loggers.S3Logger, loggers.AdminLogger, evSender, mm,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("ingot: s3api: %w", err)
	}
	return api, nil
}

// didDocumentHandler serves a fixed DID document as JSON. The document is
// built once at construction: the agent identity never changes while the
// server runs.
func didDocumentHandler(doc did.Document) fiber.Handler {
	return func(c fiber.Ctx) error {
		return c.JSON(doc)
	}
}

func validateServerInputs(cfg config.ServerConfig, deps ServerDeps) error {
	if cfg.Addr == "" {
		return errors.New("ingot: ServerConfig.Addr is required")
	}
	if cfg.DataDir == "" {
		return errors.New("ingot: ServerConfig.DataDir is required")
	}
	if deps.BaseBlockReader == nil {
		return errors.New("ingot: ServerDeps.BaseBlockReader is required")
	}
	if deps.Uploader == nil {
		return errors.New("ingot: ServerDeps.Uploader is required")
	}
	if deps.BodyUploader == nil {
		return errors.New("ingot: ServerDeps.BodyUploader is required")
	}
	if deps.Deferred == nil {
		return errors.New("ingot: ServerDeps.Deferred is required")
	}
	if deps.Parks == nil {
		return errors.New("ingot: ServerDeps.Parks is required")
	}
	if deps.Streaming != nil && deps.Streams == nil {
		return errors.New("ingot: ServerDeps.Streams is required with Streaming")
	}
	if deps.EncParams == nil {
		return errors.New("ingot: ServerDeps.EncParams is required")
	}
	if deps.RegionKeys == nil {
		return errors.New("ingot: ServerDeps.RegionKeys is required")
	}
	if deps.TenantKeys == nil {
		return errors.New("ingot: ServerDeps.TenantKeys is required")
	}
	if deps.Registry == nil {
		return errors.New("ingot: ServerDeps.Registry is required")
	}
	if deps.Intents == nil {
		return errors.New("ingot: ServerDeps.Intents is required")
	}
	if deps.Locations == nil {
		return errors.New("ingot: ServerDeps.Locations is required")
	}
	if deps.BlobRefs == nil {
		return errors.New("ingot: ServerDeps.BlobRefs is required")
	}
	if deps.GC == nil {
		return errors.New("ingot: ServerDeps.GC is required")
	}
	if deps.Multipart == nil {
		return errors.New("ingot: ServerDeps.Multipart is required")
	}
	if deps.PendingReleases == nil {
		return errors.New("ingot: ServerDeps.PendingReleases is required")
	}
	if deps.Remover == nil {
		return errors.New("ingot: ServerDeps.Remover is required")
	}
	if deps.Meta == nil {
		return errors.New("ingot: ServerDeps.Meta is required")
	}
	if deps.Identity.Issuer == nil {
		return errors.New("ingot: ServerDeps.Identity is required")
	}
	return nil
}

func applyServerDefaults(cfg config.ServerConfig) config.ServerConfig {
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.MaxBlobSize <= 0 {
		cfg.MaxBlobSize = msbucket.DefaultMaxBlobSize
	}
	// SealBytes / SealAge / Retain pass through to logstore.Open
	// untouched; logstore.Config.defaults handles its own fallbacks.

	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 4096
	}
	if cfg.MaxRequests <= 0 {
		cfg.MaxRequests = 4096
	}
	return cfg
}
