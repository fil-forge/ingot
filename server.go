package ingot

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/metrics"
	"github.com/fil-forge/versitygw/s3api"
	"github.com/fil-forge/versitygw/s3api/middlewares"
	"github.com/fil-forge/versitygw/s3event"
	"github.com/fil-forge/versitygw/s3log"
	"github.com/gofiber/fiber/v3"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/internal/cors"
	"github.com/fil-forge/ingot/internal/fasthttputil"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/s3frontend"
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
	// are no-ops and reads are served from the local spool.
	BodyUploader uploader.BodyUploader
	Remover      uploader.BlobRemover

	// Authority is the service that authorizes bucket creation and deletion.
	Authority bucketauthority.BucketAuthority

	// Registry tracks per-bucket roots. *registry.Postgres satisfies
	// both Registry and Meta in production; tests can supply two
	// separate implementations or one that does both.
	Registry registry.Registry

	// Intents tracks the local spool's upload_intents lifecycle; Locations
	// records where each accepted body blob can be retrieved from; BlobRefs is
	// the reverse reference index; GC records superseded MST/manifest CIDs.
	// Typically all the same instance as Registry (*registry.Postgres /
	// *inmem.MemStore).
	Intents   registry.IntentStore
	Locations registry.LocationStore
	BlobRefs  registry.BlobRefStore
	GC        registry.GCStore
	Multipart registry.MultipartStore

	// Meta is the persistence backing for log-segment metadata.
	// Typically the same instance as Registry.
	Meta logstore.Meta

	// IAM authenticates non-root access keys (e.g. hilt/iam, which
	// authorizes each request against the Hilt tenant service). Optional:
	// nil leaves the gateway with only the single root account, as in
	// standalone mode and the test harness.
	IAM auth.IAMService
}

// Server is a fully-wired ingot S3 listener. Use Start/Stop for
// lifecycle. fx callers wrap these in OnStart/OnStop hooks; tests
// call them directly.
type Server struct {
	cfg     config.ServerConfig
	logger  *zap.Logger
	log     blockstore.Log
	backend *s3frontend.Backend
	api     *s3api.S3ApiServer
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
			return newBucketFlushFunc(deps.Uploader, deps.Registry, bucket, logger)
		},
		Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("ingot: logstore: %w", err)
	}

	spool, err := blockstore.NewSpool(filepath.Join(cfg.DataDir, "spool"))
	if err != nil {
		_ = log.Close(ctx)
		return nil, fmt.Errorf("ingot: spool: %w", err)
	}

	bs := blockstore.NewLayered(spool, log, deps.BaseBlockReader)
	backend := s3frontend.New(s3frontend.Deps{
		Authority:   deps.Authority,
		Registry:    deps.Registry,
		Intents:     deps.Intents,
		Locations:   deps.Locations,
		BlobRefs:    deps.BlobRefs,
		GC:          deps.GC,
		Multipart:   deps.Multipart,
		Reads:       bs,
		Log:         log,
		Spool:       spool,
		Uploader:    deps.BodyUploader,
		Remover:     deps.Remover,
		MaxBlobSize: cfg.MaxBlobSize,
		Logger:      logger,
	})

	api, err := buildS3API(ctx, backend, cfg, deps.IAM)
	if err != nil {
		// Best-effort cleanup if we got past the log open: the caller
		// has no Server handle to call Stop on.
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
// it to start serving on Addr).
func (s *Server) Start(ctx context.Context) error {
	if err := s.backend.Recover(ctx); err != nil {
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
	return nil
}

// Stop shuts the listener down and drains the log. Always returns
// the combined error of the two operations so callers see all
// failure modes; either alone is non-fatal to the other.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("shutting down ingot S3 listener")

	var errs []error
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

// newPlaneFlushFunc builds the logstore flush callback for ONE plane:
// it ships that plane's sealed CAR to Forge via uploader.SubmitShard.
// The store owns the ship-state transition (it stamps the per-plane
// shipped timestamp and, for the catalog plane, advances each affected
// bucket's forge_root_cid) once this returns nil — so the closure is
// purely the network ship.
//
// A header-only CAR (e.g. an MST-only op writes no data blocks; a
// trimTop-to-existing-subtree writes neither) has no positions: nothing
// to ship, so the closure returns nil and the store still marks the
// plane shipped, letting retention reclaim the tiny CAR and (for the
// catalog plane) advancing forge_root_cid for the recorded op-roots.
//
// The destination space is the bucket's, resolved from the registry at
// ship time (the log is segregated per bucket, so every segment this
// closure sees belongs to exactly this bucket). A bucket deleted while
// segments were still queued has nothing to ship to: the closure returns
// nil so the segment marks shipped and retires.
func newBucketFlushFunc(up uploader.Uploader, reg registry.Registry, bucket string, logger *zap.Logger) logstore.FlushFunc {
	plane := blockstore.PlaneCatalog
	return func(ctx context.Context, seg *logstore.Segment) error {
		positions := seg.Positions()
		if len(positions) == 0 {
			return nil
		}
		st, err := reg.Get(ctx, bucket)
		if errors.Is(err, registry.ErrNotFound) {
			logger.Info("dropping segment for deleted bucket",
				zap.String("bucket", bucket), zap.Uint64("seq", seg.Seq()))
			return nil
		}
		if err != nil {
			return fmt.Errorf("resolve space for %q: %w", bucket, err)
		}
		// Segment stores the raw 32-byte SHA-256 of the CAR file; the
		// uploader and ShardedDagIndexView want the multihash form.
		sha, err := multihash.Encode(seg.SHA256(), multihash.SHA2_256)
		if err != nil {
			return fmt.Errorf("encode segment %d %s sha: %w", seg.Seq(), plane, err)
		}
		shard := uploader.CARShard{
			Path:      seg.CARPath(),
			Size:      seg.Size(),
			SHA256:    sha,
			Positions: positions,
		}
		if err := up.SubmitShard(ctx, plane, st.Space, shard); err != nil {
			return fmt.Errorf("submit segment %d %s for %q: %w", seg.Seq(), plane, bucket, err)
		}
		return nil
	}
}

// buildS3API constructs the versitygw S3ApiServer with the wiring ingot
// needs: no audit / event sinks, generous concurrency limits. Non-root
// access keys authenticate through iam when provided (the root account is
// checked before the IAM lookup either way).
func buildS3API(ctx context.Context, backend *s3frontend.Backend, cfg config.ServerConfig, iam auth.IAMService) (*s3api.S3ApiServer, error) {
	if iam == nil {
		return nil, fmt.Errorf("ingot: IAMService is required")
	}
	loggers, err := s3log.InitLogger(&s3log.LogConfig{})
	if err != nil {
		return nil, fmt.Errorf("ingot: loggers: %w", err)
	}
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
		// Stash the signed S3 request on the context for every request. The
		// bucket-authority seam (Create/Delete/ListBuckets) recovers it to
		// forward to Hilt; doing it here — ahead of auth — covers all auth
		// paths (root included), not just the Hilt-backed IAM lookup, so a
		// root request can still drive bucket operations.
		s3api.WithMiddleware("/", func(c fiber.Ctx) error {
			c.Locals(reqscope.RequestKey(), fasthttputil.RequestFromHTTPContext(c.RequestCtx()))
			return c.Next()
		}),
	}
	if len(cfg.CORSAllowedOrigins) > 0 {
		matcher, err := cors.NewMatcher(cfg.CORSAllowedOrigins)
		if err != nil {
			return nil, fmt.Errorf("ingot: cors_allowed_origins: %w", err)
		}
		opts = append(opts, s3api.WithMiddleware("/", corsMiddleware(matcher)))
	}

	api, err := s3api.New(backend,
		middlewares.RootUserConfig{Access: cfg.RootAccess, Secret: cfg.RootSecret},
		cfg.Region, iam, loggers.S3Logger, loggers.AdminLogger, evSender, mm,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("ingot: s3api: %w", err)
	}
	return api, nil
}

func validateServerInputs(cfg config.ServerConfig, deps ServerDeps) error {
	if cfg.Addr == "" {
		return errors.New("ingot: ServerConfig.Addr is required")
	}
	if cfg.DataDir == "" {
		return errors.New("ingot: ServerConfig.DataDir is required")
	}
	if cfg.RootAccess == "" || cfg.RootSecret == "" {
		return errors.New("ingot: ServerConfig.RootAccess and ServerConfig.RootSecret are required")
	}
	if cfg.RootAccess == "" || cfg.RootSecret == "" {
		return errors.New("ingot: ServerConfig.RootAccess and ServerConfig.RootSecret are required")
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
	if deps.Remover == nil {
		return errors.New("ingot: ServerDeps.Remover is required")
	}
	if deps.Meta == nil {
		return errors.New("ingot: ServerDeps.Meta is required")
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
