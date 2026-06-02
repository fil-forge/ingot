// Package ingot exposes the embedded S3 listener as both a low-level Server type
// (see server.go) and an fx module (see Module).
//
// The S3 protocol layer is provided by github.com/versity/versitygw; the
// storage backend is the LSM-style log in logstore in front of a Forge-backed
// read tier, with versitygw -> logstore translation in s3frontend.
//
// # Using ingot as an fx module
//
// A host (piri, guppy, ...) includes ingot with ingot.Module(cfg) and provides
// the following in its own graph:
//
//   - *zap.Logger
//   - *pgxpool.Pool          — ingot owns its schema and runs its own goose
//     migrations against this pool at startup
//   - ingot.ServiceIdentity   — the host's upload-service signer
//   - uploader.ProviderSelector — chooses which piri a blob is allocated to;
//     uploader.NewStaticProviderSelector covers the single home-piri case
//
// Module manages the embedded S3 Server's lifecycle and provides nothing to the
// host graph. When cfg.Enabled is false it is an empty option, so a host can
// always include it and toggle ingot purely through config.
//
// # ServerModule: the composable core
//
// Module is a thin production wrapper around [ServerModule], which is the
// reusable core: it consumes a [ServerConfig], a *zap.Logger, and the four
// collaborator interfaces (registry.Registry, logstore.Meta,
// blockstore.BlockReader, uploader.Uploader) from the graph, then manages
// New -> Start -> Stop over the fx lifecycle. Module layers the production
// providers (Postgres registry, Forge reader/uploader, space signer) and a
// migration pre-start hook on top. A test harness, by contrast, includes
// ServerModule directly and supplies in-memory fakes for the same four
// interfaces — so both paths construct the Server through identical wiring.
//
// # Using ingot without fx
//
// Construct the collaborators yourself and call New(ctx, ServerConfig,
// ServerDeps); drive Server.Start / Server.Stop directly. The fx module is a
// thin convenience over exactly that path.
package ingot

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/principal"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/migrations"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// ServiceIdentity carries the host's upload-service signer into the module. It
// is a named wrapper rather than a bare ucan.Signer so it can't be confused
// with any other signer the host has in its fx graph.
type ServiceIdentity struct {
	Signer ucan.Signer
}

// PreStartHook runs once during the server's OnStart, before the listener is
// constructed. Production registers the goose migration step as one of these
// (the registry/log recovery in New depends on the schema existing). Hosts and
// tests that need no pre-start work register none. Provide implementations into
// the "ingot_prestart" fx group.
type PreStartHook func(ctx context.Context) error

// Module returns the ingot fx module wired for cfg. When cfg.Enabled is false it
// returns an empty option, so a host can always include Module and toggle ingot
// purely through configuration (mirroring how sprue's AppModule conditionally
// includes a storage backend).
//
// See the package doc for the dependencies the host must provide.
func Module(cfg Config) fx.Option {
	if !cfg.Enabled {
		return fx.Options()
	}
	opts := []fx.Option{
		fx.Supply(cfg),
		// Derive the low-level ServerConfig from Config (parses SealAge once)
		// and feed it to the shared ServerModule.
		fx.Provide(Config.ServerConfig),
		fx.Provide(
			provideSpaceSigner,
			provideRegistry,
			provideForgeReader,
			provideIndexPublisher,
			provideUploader,
			provideMigrationHook,
		),
		ServerModule,
	}
	// When the host configured a single home piri, build the provider selector
	// from config so it needn't supply one. Otherwise the host provides a
	// uploader.ProviderSelector in its own graph (e.g. a routing-backed one).
	if cfg.HomeProviderDID != "" && cfg.HomeProviderURL != "" {
		opts = append(opts, fx.Provide(provideHomeSelector))
	}
	return fx.Module("ingot", opts...)
}

// ServerModule is the composable core: it constructs and lifecycles the
// embedded S3 Server from a ServerConfig, a *zap.Logger, the four collaborator
// interfaces, and any registered PreStartHooks. It is exported so a test
// harness (or an alternative host) can supply its own implementations of the
// collaborators — in-memory fakes, say — and still construct the Server through
// exactly the wiring production uses. Module supplies the production
// implementations on top of this.
var ServerModule = fx.Module("ingot-server",
	fx.Invoke(registerServerLifecycle),
)

// serverParams are the inputs ServerModule binds the server's start/stop hooks
// over. The four collaborators are interfaces, so any provider satisfying them
// (production's Postgres + Forge, or a harness's in-memory fakes) wires in.
type serverParams struct {
	fx.In

	Config    ServerConfig
	Logger    *zap.Logger
	Reader    blockstore.BlockReader
	Uploader  uploader.Uploader
	Registry  registry.Registry
	Meta      logstore.Meta
	PreStarts []PreStartHook `group:"ingot_prestart"`
}

// registerServerLifecycle hooks the embedded server into the fx lifecycle. All
// side-effecting startup work — the pre-start hooks (e.g. migrations), then log
// recovery (in New), then the listener — happens in OnStart, in that order,
// against the lifecycle context; OnStop drains and shuts down.
func registerServerLifecycle(lc fx.Lifecycle, p serverParams) {
	logger := p.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	var srv *Server
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			for _, hook := range p.PreStarts {
				if hook == nil {
					continue
				}
				if err := hook(ctx); err != nil {
					return err
				}
			}
			s, err := New(ctx, p.Config, ServerDeps{
				Logger:          logger,
				BaseBlockReader: p.Reader,
				Uploader:        p.Uploader,
				Registry:        p.Registry,
				Meta:            p.Meta,
			})
			if err != nil {
				return err
			}
			srv = s
			return srv.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			if srv == nil {
				return nil
			}
			return srv.Stop(ctx)
		},
	})
}

// provideHomeSelector builds a single-home-piri ProviderSelector from config.
// Only registered when both HomeProvider fields are set (see Module).
func provideHomeSelector(cfg Config) (uploader.ProviderSelector, error) {
	id, err := did.Parse(cfg.HomeProviderDID)
	if err != nil {
		return nil, fmt.Errorf("ingot: parse home_provider_did: %w", err)
	}
	endpoint, err := url.Parse(cfg.HomeProviderURL)
	if err != nil {
		return nil, fmt.Errorf("ingot: parse home_provider_url: %w", err)
	}
	return uploader.NewStaticProviderSelector(id, *endpoint), nil
}

// spaceSigner is an internal wrapper around the persisted space key so it is a
// distinct type in the fx graph from the host-provided ServiceIdentity.
type spaceSigner struct{ principal.Signer }

// provideSpaceSigner loads or creates the space key under cfg.DataDir. ingot IS
// the space owner (root UCAN authority); the key is generated on first run and
// persisted at data_dir/space.key.
func provideSpaceSigner(cfg Config, logger *zap.Logger) (spaceSigner, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return spaceSigner{}, fmt.Errorf("ingot: mkdir data dir: %w", err)
	}
	keyPath := filepath.Join(cfg.DataDir, "space.key")
	s, err := LoadOrCreateSigner(keyPath)
	if err != nil {
		return spaceSigner{}, fmt.Errorf("ingot: space signer: %w", err)
	}
	logger.Info("ingot space loaded",
		zap.String("space_did", s.DID().String()),
		zap.String("key_file", keyPath),
	)
	return spaceSigner{Signer: s}, nil
}

// registryResult exposes the one *registry.Postgres as both collaborator
// interfaces ServerModule needs: registry.Registry (bucket state) and
// logstore.Meta (segment metadata). Production wires a single instance to both,
// matching how New is called directly.
type registryResult struct {
	fx.Out

	Registry registry.Registry
	Meta     logstore.Meta
}

// provideRegistry wraps the host's pool in the postgres-backed registry and
// exposes it under both interfaces ServerModule consumes.
func provideRegistry(pool *pgxpool.Pool) registryResult {
	pg := registry.NewPostgres(pool)
	return registryResult{Registry: pg, Meta: pg}
}

// migrationHookOut feeds the migration PreStartHook into the "ingot_prestart"
// group serverParams collects.
type migrationHookOut struct {
	fx.Out

	Hook PreStartHook `group:"ingot_prestart"`
}

// provideMigrationHook registers ingot's goose migrations as a pre-start hook.
// ingot owns its schema; goose tracks its version at ingot.goose_db_version, so
// this never collides with the host's own migrations on the same database. It
// must run before New (whose log recovery queries the registry), which the
// PreStartHook ordering guarantees.
func provideMigrationHook(pool *pgxpool.Pool, logger *zap.Logger) migrationHookOut {
	return migrationHookOut{Hook: func(ctx context.Context) error {
		if err := migrations.Up(ctx, pool, logger); err != nil {
			return fmt.Errorf("ingot: migrations: %w", err)
		}
		return nil
	}}
}

// provideForgeReader builds the network-backed read tier (indexer + piri),
// fronted by a bounded in-memory block cache (see Config.ReadCacheBytes).
func provideForgeReader(cfg Config, id ServiceIdentity, space spaceSigner, logger *zap.Logger) (blockstore.BlockReader, error) {
	forge, err := blockstore.NewForge(blockstore.ForgeConfig{
		IndexerEndpoint: cfg.IndexerEndpoint,
		IndexerDID:      cfg.IndexerDID,
		Spaces:          []did.DID{space.DID()},
		Signer:          id.Signer,
		SpaceSigner:     space.Signer,
		Logger:          logger,
	})
	if err != nil {
		return nil, err
	}
	return blockstore.NewCached(forge, cfg.readCacheBytes()), nil
}

// provideIndexPublisher builds the /assert/index publisher against the indexer.
func provideIndexPublisher(cfg Config, id ServiceIdentity, logger *zap.Logger) (uploader.IndexPublisher, error) {
	endpoint, err := url.Parse(cfg.IndexerEndpoint)
	if err != nil {
		return nil, fmt.Errorf("ingot: parse indexer endpoint: %w", err)
	}
	indexerDID, err := did.Parse(cfg.IndexerDID)
	if err != nil {
		return nil, fmt.Errorf("ingot: parse indexer DID: %w", err)
	}
	return uploader.NewIndexPublisher(endpoint, indexerDID, id.Signer, logger)
}

// provideUploader builds the segment-flush uploader (allocate/PUT/accept +
// index publish) over the host-supplied provider selector.
func provideUploader(id ServiceIdentity, space spaceSigner, sel uploader.ProviderSelector, pub uploader.IndexPublisher, logger *zap.Logger) (uploader.Uploader, error) {
	return uploader.NewForge(uploader.ForgeConfig{
		Selector:       sel,
		IndexPublisher: pub,
		Signer:         id.Signer,
		SpaceSigner:    space.Signer,
		Logger:         logger,
	})
}

// emptyDefault returns def when s is the empty string.
func emptyDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
