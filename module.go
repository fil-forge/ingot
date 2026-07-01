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

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	contentcmds "github.com/fil-forge/libforge/commands/content"
	indexcmds "github.com/fil-forge/libforge/commands/index"
	"github.com/fil-forge/libforge/receipt"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/principal"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/forgeclient"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/migrations"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/tokenstore"
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
			provideServerSpace,
			provideRegistry,
			provideForgeReader,
			provideTokenStore,
			provideForgeClient,
			provideUploader,
			provideMigrationHook,
			provideTokenSeedHook,
		),
		ServerModule,
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

	Config       ServerConfig
	Logger       *zap.Logger
	Reader       blockstore.BlockReader
	Uploader     uploader.Uploader
	BodyUploader uploader.BodyUploader
	Registry     registry.Registry
	Intents      registry.IntentStore
	Locations    registry.LocationStore
	Meta         logstore.Meta
	// Space is the Forge space this instance owns. Optional: standalone / the
	// harness don't provide it (reads come from the spool), so it defaults to "".
	Space     ServerSpace    `optional:"true"`
	PreStarts []PreStartHook `group:"ingot_prestart"`
}

// ServerSpace is the Forge space DID (as a string) the embedded server records
// body-blob locations under. A named type so it is unambiguous in the fx graph.
type ServerSpace string

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
				BodyUploader:    p.BodyUploader,
				Registry:        p.Registry,
				Intents:         p.Intents,
				Locations:       p.Locations,
				Meta:            p.Meta,
				Space:           string(p.Space),
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

// provideTokenStore opens the filesystem-backed token store that holds
// ingot's space→agent delegation chain (login-derived in a real
// deployment, or self-issued by provideTokenSeedHook for the
// single-operator case).
func provideTokenStore(cfg Config) (tokenstore.Store, error) {
	dir := emptyDefault(cfg.TokenStoreDir, cfg.DataDir)
	return tokenstore.NewFsStore(dir)
}

// provideForgeClient builds the edge-client to the upload service (sprue):
// the agent identity issues invocations, the token store supplies proofs.
func provideForgeClient(cfg Config, id ServiceIdentity, store tokenstore.Store, logger *zap.Logger) (*forgeclient.Client, error) {
	sprueURL, err := url.Parse(cfg.UploadServiceURL)
	if err != nil {
		return nil, fmt.Errorf("ingot: parse upload_service_url: %w", err)
	}
	sprueDID, err := did.Parse(cfg.UploadServiceDID)
	if err != nil {
		return nil, fmt.Errorf("ingot: parse upload_service_did: %w", err)
	}
	opts := []forgeclient.Option{
		forgeclient.WithTokenStore(store),
		forgeclient.WithLogger(logger),
	}
	if cfg.UploadReceiptsURL != "" {
		rcptURL, err := url.Parse(cfg.UploadReceiptsURL)
		if err != nil {
			return nil, fmt.Errorf("ingot: parse upload_receipts_url: %w", err)
		}
		opts = append(opts, forgeclient.WithReceiptsClient(receipt.NewClient(rcptURL)))
	}
	return forgeclient.New(id.Signer, sprueDID, *sprueURL, opts...)
}

// tokenSeedHookOut feeds the delegation-seed PreStartHook into the group.
type tokenSeedHookOut struct {
	fx.Out

	Hook PreStartHook `group:"ingot_prestart"`
}

// provideTokenSeedHook seeds the token store with self-issued
// space→agent delegations for /blob/add, /index/add, and
// /content/retrieve when it has none, so the single-operator appliance
// can ship without an interactive login. A real multi-tenant deployment
// instead populates the store via `ingot login` (sprue-issued proofs);
// the seed is skipped once any chain exists.
func provideTokenSeedHook(space spaceSigner, id ServiceIdentity, store tokenstore.Store, logger *zap.Logger) tokenSeedHookOut {
	return tokenSeedHookOut{Hook: func(ctx context.Context) error {
		return seedSpaceDelegations(ctx, space.Signer, id.Signer.DID(), store, logger)
	}}
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

	Registry  registry.Registry
	Intents   registry.IntentStore
	Locations registry.LocationStore
	Meta      logstore.Meta
}

// provideRegistry wraps the host's pool in the postgres-backed registry and
// exposes it under every interface ServerModule consumes. One *registry.Postgres
// satisfies bucket state (Registry), the spool's upload_intents (IntentStore),
// blob locations (LocationStore), and segment metadata (Meta).
func provideRegistry(pool *pgxpool.Pool) registryResult {
	pg := registry.NewPostgres(pool)
	return registryResult{Registry: pg, Intents: pg, Locations: pg, Meta: pg}
}

// provideServerSpace exposes the owned space DID (as a string) to the server.
func provideServerSpace(space spaceSigner) ServerSpace {
	return ServerSpace(space.DID().String())
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

// uploaderResult exposes the one *uploader.Forge under both upload seams:
// Uploader (catalog CAR segments) and BodyUploader (per-blob body uploads).
type uploaderResult struct {
	fx.Out

	Uploader     uploader.Uploader
	BodyUploader uploader.BodyUploader
}

// provideUploader builds the guppy-style edge client that ships to Forge via
// the upload service (/blob/add → /ucan/conclude → /blob/accept → /index/add).
// The same client both ships sealed catalog shards (Uploader) and uploads
// individual body blobs by digest (BodyUploader).
func provideUploader(c *forgeclient.Client, space spaceSigner, logger *zap.Logger) (uploaderResult, error) {
	f, err := uploader.NewForge(uploader.ForgeConfig{
		Client: c,
		Space:  space.DID(),
		Logger: logger,
	})
	if err != nil {
		return uploaderResult{}, err
	}
	return uploaderResult{Uploader: f, BodyUploader: f}, nil
}

// seedSpaceDelegations self-issues no-expiry space→agent delegations for
// the capabilities the edge client invokes, unless the store already
// holds a /blob/add chain for the agent (idempotent across restarts).
func seedSpaceDelegations(ctx context.Context, spaceSigner ucan.Signer, agent did.DID, store tokenstore.Store, logger *zap.Logger) error {
	space := spaceSigner.DID()
	// Sentinel on /blob/allocate: it is the cap the upload service re-invokes
	// against piri on the space's behalf, so a store missing it cannot ship.
	// (An older store seeded before allocate/accept were added re-seeds here.)
	if proofs, _, err := store.ProofChain(ctx, agent, blobcmds.Allocate.Command, space); err == nil && len(proofs) > 0 {
		return nil // already seeded (or login-provisioned)
	}
	// The edge-client flow needs space->agent authority over: /blob/add (the
	// add invocation), /blob/allocate + /blob/accept (re-delegated to sprue so
	// it can allocate/accept against piri), /index/add (the index publish), and
	// /content/retrieve (the read path + the index retrieval-auth).
	type cap struct {
		name string
		dlg  func() (ucan.Delegation, error)
	}
	caps := []cap{
		{"/blob/add", func() (ucan.Delegation, error) {
			return blobcmds.Add.Delegate(spaceSigner, agent, space, delegation.WithNoExpiration())
		}},
		{"/blob/allocate", func() (ucan.Delegation, error) {
			return blobcmds.Allocate.Delegate(spaceSigner, agent, space, delegation.WithNoExpiration())
		}},
		{"/blob/accept", func() (ucan.Delegation, error) {
			return blobcmds.Accept.Delegate(spaceSigner, agent, space, delegation.WithNoExpiration())
		}},
		{"/index/add", func() (ucan.Delegation, error) {
			return indexcmds.Add.Delegate(spaceSigner, agent, space, delegation.WithNoExpiration())
		}},
		{"/content/retrieve", func() (ucan.Delegation, error) {
			return contentcmds.Retrieve.Delegate(spaceSigner, agent, space, delegation.WithNoExpiration())
		}},
	}
	dlgs := make([]ucan.Delegation, 0, len(caps))
	for _, c := range caps {
		d, err := c.dlg()
		if err != nil {
			return fmt.Errorf("ingot: delegate %s: %w", c.name, err)
		}
		dlgs = append(dlgs, d)
	}
	if err := store.AddDelegations(ctx, dlgs...); err != nil {
		return fmt.Errorf("ingot: seed delegations: %w", err)
	}
	logger.Info("ingot seeded self-issued space delegations",
		zap.String("space_did", space.String()),
		zap.String("agent_did", agent.String()),
	)
	return nil
}

// emptyDefault returns def when s is the empty string.
func emptyDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
