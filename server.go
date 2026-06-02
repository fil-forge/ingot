package ingot

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/metrics"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3api/middlewares"
	"github.com/versity/versitygw/s3event"
	"github.com/versity/versitygw/s3log"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/s3frontend"
	"github.com/fil-forge/ingot/uploader"
)

// ServerConfig captures the user-facing knobs of an ingot S3 listener.
// New() applies defaults for any zero-valued knobs. SealAge is in
// time.Duration form because callers parse the string config field
// once before constructing the server.
type ServerConfig struct {
	// Addr is the host:port to bind the S3 listener to. Required.
	Addr string

	// DataDir is where the log writes its segments dir; the caller
	// is responsible for creating this directory before calling New.
	// Required.
	DataDir string

	// Region is the AWS region advertised over sigv4. Defaults to
	// "us-east-1".
	Region string

	// RootAccess / RootSecret configure the single-account IAM root
	// user for the embedded S3 listener. Both required.
	RootAccess string
	RootSecret string

	// ChunkSize is the body chunk size for new objects, in bytes.
	// 0 → bucket.DefaultChunkSize.
	ChunkSize int64

	// SealBytes / SealAge / Retain are passed through to logstore.Open.
	// Zero values pick logstore defaults (64 MiB / 5 s / 6 segments).
	SealBytes int64
	SealAge   time.Duration
	Retain    int

	// MaxConnections / MaxRequests configure versitygw's hard
	// concurrency limit. Zero is unsafe (yields 503 SlowDown on every
	// request), so New substitutes a sensible default.
	MaxConnections int
	MaxRequests    int
}

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

	// Uploader is the destination for sealed segments.
	Uploader uploader.Uploader

	// Registry tracks per-bucket roots. *registry.Postgres satisfies
	// both Registry and Meta in production; tests can supply two
	// separate implementations or one that does both.
	Registry registry.Registry

	// Meta is the persistence backing for log-segment metadata.
	// Typically the same instance as Registry.
	Meta logstore.Meta
}

// Server is a fully-wired ingot S3 listener. Use Start/Stop for
// lifecycle. fx callers wrap these in OnStart/OnStop hooks; tests
// call them directly.
type Server struct {
	cfg     ServerConfig
	logger  *zap.Logger
	log     blockstore.Log
	backend *s3frontend.Backend
	api     *s3api.S3ApiServer
}

// New wires a ServerDeps + ServerConfig into a runnable Server. The
// caller is responsible for ensuring cfg.DataDir exists before
// calling.
func New(ctx context.Context, cfg ServerConfig, deps ServerDeps) (*Server, error) {
	if err := validateServerInputs(cfg, deps); err != nil {
		return nil, err
	}
	cfg = applyServerDefaults(cfg)

	logger := deps.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	flush := newFlushFunc(deps.Uploader, deps.Meta)

	log, err := logstore.Open(ctx, logstore.Config{
		Dir:       filepath.Join(cfg.DataDir, "segments"),
		Meta:      deps.Meta,
		SealBytes: cfg.SealBytes,
		SealAge:   cfg.SealAge,
		Retain:    cfg.Retain,
		Flush:     flush,
		Logger:    logger,
	})
	if err != nil {
		return nil, fmt.Errorf("ingot: logstore: %w", err)
	}

	bs := blockstore.NewLayered(log, deps.BaseBlockReader)
	codec := &msbucket.FixedChunker{ChunkSize: cfg.ChunkSize}
	backend := s3frontend.New(deps.Registry, bs, log, codec)

	api, err := buildS3API(ctx, backend, cfg)
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
		zap.Int64("chunk_size", s.cfg.ChunkSize),
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

// newFlushFunc captures uploader + meta into the closure passed to
// logstore.Open. Each sealed segment becomes one Forge round trip
// (CAR + index + indexer claim) plus one Postgres tx that flips the
// segment row to flushed and advances each affected bucket's
// forge_root_cid.
//
// The sealed CAR file is the wire payload — uploader.SubmitCAR
// streams it directly into the HTTP PUT, and the segment's
// already-computed digest and append-time position table feed
// allocate/accept and the index view without rescanning the file.
func newFlushFunc(up uploader.Uploader, meta logstore.Meta) logstore.FlushFunc {
	return func(ctx context.Context, seg *logstore.Segment) error {
		opRoots := seg.OpRoots()
		positions := seg.BlockPositions()
		if len(positions) == 0 || len(opRoots) == 0 {
			// Empty or no-op segment (e.g., force-sealed during a
			// quiet startup). Mark flushed so retention can sweep
			// it; no Forge ship and no forge_root advance are
			// needed.
			return meta.MarkSegmentFlushed(ctx, seg.Seq(), time.Now().Unix(), nil)
		}
		// Segment stores the raw 32-byte SHA-256 of the CAR file;
		// the uploader and ShardedDagIndexView want the multihash
		// form (varint code + length + digest).
		sha, err := multihash.Encode(seg.SHA256(), multihash.SHA2_256)
		if err != nil {
			return fmt.Errorf("encode segment %d sha: %w", seg.Seq(), err)
		}
		rootCids := make([]cid.Cid, len(opRoots))
		for i, opr := range opRoots {
			rootCids[i] = opr.Root
		}
		src := uploader.CARSource{
			Path:      seg.CARPath(),
			Size:      seg.Size(),
			SHA256:    sha,
			Positions: positions,
		}
		if err := up.SubmitCAR(ctx, rootCids, src); err != nil {
			return fmt.Errorf("submit segment %d: %w", seg.Seq(), err)
		}
		return meta.MarkSegmentFlushed(ctx, seg.Seq(), time.Now().Unix(), opRoots)
	}
}

// buildS3API constructs the versitygw S3ApiServer with the wiring
// ingot needs: single-account IAM, no audit / event sinks, generous
// concurrency limits.
func buildS3API(ctx context.Context, backend *s3frontend.Backend, cfg ServerConfig) (*s3api.S3ApiServer, error) {
	rootAcc := auth.Account{
		Access: cfg.RootAccess,
		Secret: cfg.RootSecret,
		Role:   auth.RoleAdmin,
	}
	iam := auth.NewIAMServiceSingle(rootAcc)

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

	api, err := s3api.New(backend,
		middlewares.RootUserConfig{Access: rootAcc.Access, Secret: rootAcc.Secret},
		cfg.Region, iam, loggers.S3Logger, loggers.AdminLogger, evSender, mm,
		s3api.WithQuiet(),
		s3api.WithHealth("/health"),
		s3api.WithConcurrencyLimiter(cfg.MaxConnections, cfg.MaxRequests),
	)
	if err != nil {
		return nil, fmt.Errorf("ingot: s3api: %w", err)
	}
	return api, nil
}

func validateServerInputs(cfg ServerConfig, deps ServerDeps) error {
	if cfg.Addr == "" {
		return errors.New("ingot: ServerConfig.Addr is required")
	}
	if cfg.DataDir == "" {
		return errors.New("ingot: ServerConfig.DataDir is required")
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
	if deps.Registry == nil {
		return errors.New("ingot: ServerDeps.Registry is required")
	}
	if deps.Meta == nil {
		return errors.New("ingot: ServerDeps.Meta is required")
	}
	return nil
}

func applyServerDefaults(cfg ServerConfig) ServerConfig {
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = msbucket.DefaultChunkSize
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
