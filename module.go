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

	"github.com/fil-forge/libforge/receipt"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/versitygw/auth"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/zap"

	hiltclient "github.com/fil-forge/hilt/pkg/client"
	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/forgeclient"
	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/migrations"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/tokenstore"
	"github.com/fil-forge/ingot/uploader"
)

// ServiceIdentity carries the host's upload-service agent into the module. It
// is a named wrapper rather than a bare ucan.Issuer so it can't be confused
// with any other issuer the host has in its fx graph. The agent is a
// [ucan.Issuer] (a signer tied to a DID): it issues every invocation to sprue.
type ServiceIdentity struct {
	Signer ucan.Issuer
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
func Module(cfg config.Config) fx.Option {
	if !cfg.Enabled {
		return fx.Options()
	}
	opts := []fx.Option{
		fx.Supply(cfg),
		// Derive the low-level ServerConfig from Config (parses SealAge once)
		// and feed it to the shared ServerModule.
		fx.Provide(cfg.ServerConfig),
		fx.Provide(
			provideRegistry,
			provideForgeReader,
			provideTokenStore,
			provideForgeClient,
			provideAuthServiceClient,
			provideUploader,
			provideMigrationHook,
			provideKeyProofs,
			provideIAMService,
			fx.Annotate(bucketauthority.New, fx.As(new(bucketauthority.BucketAuthority))),
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

	Config          config.ServerConfig
	Logger          *zap.Logger
	Reader          blockstore.BlockReader
	Uploader        uploader.Uploader
	BodyUploader    uploader.BodyUploader
	Deferred        uploader.DeferredBodyUploader
	Remover         uploader.BlobRemover
	BucketAuthority bucketauthority.BucketAuthority
	Registry        registry.Registry
	Intents         registry.IntentStore
	Locations       registry.LocationStore
	Inclusions      registry.InclusionStore
	BlobRefs        registry.BlobRefStore
	GC              registry.GCStore
	Multipart       registry.MultipartStore
	Parks           registry.ParkStore
	Meta            logstore.Meta
	// IAM authenticates non-root access keys.
	IAM       auth.IAMService `optional:"true"`
	PreStarts []PreStartHook  `group:"ingot_prestart"`
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
				BodyUploader:    p.BodyUploader,
				Deferred:        p.Deferred,
				Remover:         p.Remover,
				Authority:       p.BucketAuthority,
				Registry:        p.Registry,
				Intents:         p.Intents,
				Locations:       p.Locations,
				Inclusions:      p.Inclusions,
				BlobRefs:        p.BlobRefs,
				GC:              p.GC,
				Multipart:       p.Multipart,
				Parks:           p.Parks,
				Meta:            p.Meta,
				IAM:             p.IAM,
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

// provideTokenStore opens the filesystem-backed token store the sprue
// edge-client reads its proof chains from. Nothing populates it since the
// self-issued space key was removed — the write path's /blob/add and
// /index/add chains are a known gap; the delegation cache the IAM service
// fills is the likely future source.
func provideTokenStore(cfg config.Config) (tokenstore.Store, error) {
	dir := config.EmptyDefault(cfg.TokenStoreDir, cfg.DataDir)
	return tokenstore.NewFsStore(dir)
}

// provideForgeClient builds the edge-client to the upload service (sprue):
// the agent identity issues invocations, the token store supplies proofs.
func provideForgeClient(cfg config.Config, id ServiceIdentity, store tokenstore.Store, logger *zap.Logger) (*forgeclient.Client, error) {
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

// provideAuthServiceClient builds the UCAN RPC client to the Hilt tenant-
// management service (/s3/request/authorize, /s3/bucket/*). The agent identity
// issues invocations; HiltProofs (optional) supplies the Hilt→agent proof
// chains. The provider is lazy, so an unconfigured Hilt only errors if a
// consumer actually needs the client.
func provideAuthServiceClient(cfg config.Config, id ServiceIdentity, logger *zap.Logger) (*hiltclient.Client, error) {
	if cfg.AuthServiceURL == "" || cfg.AuthServiceDID == "" {
		return nil, fmt.Errorf("ingot: auth_service_url and auth_service_did are required for the auth service client")
	}
	authServiceURL, err := url.Parse(cfg.AuthServiceURL)
	if err != nil {
		return nil, fmt.Errorf("ingot: parse auth_service_url: %w", err)
	}
	authServiceDID, err := did.Parse(cfg.AuthServiceDID)
	if err != nil {
		return nil, fmt.Errorf("ingot: parse auth_service_did: %w", err)
	}
	var proofs ucanlib.ProofStore
	if cfg.AuthServiceProofs != "" {
		ct, err := config.LoadProofsContainer(cfg.AuthServiceProofs)
		if err != nil {
			return nil, fmt.Errorf("ingot: auth_service_proofs: %w", err)
		}
		proofs = ucanlib.NewContainerProofStore(ct)
	}
	return hiltclient.New(authServiceDID, *authServiceURL, id.Signer, proofs, hiltclient.WithLogger(logger))
}

// provideKeyProofs is the per-access-key delegation store registry: the IAM
// service deposits each key's Hilt-issued chains into its own store and
// stashes that store on the request context, from which the network read
// tier resolves per-space /content/retrieve proof chains scoped to the
// requesting key.
func provideKeyProofs() *iam.KeyProofs {
	return iam.NewKeyProofs()
}

// provideIAMService adapts the hilt client to versitygw's IAM seam: a request
// signed with a non-root access key is authorized locally when the caches
// hold its verification key + covering delegation chains, else by Hilt's
// /s3/request/authorize — whose response replenishes the caches. Either way
// the gateway verifies the signature with the derived key.
func provideIAMService(c *hiltclient.Client, proofs *iam.KeyProofs, reg registry.Registry, id ServiceIdentity, logger *zap.Logger) auth.IAMService {
	return iam.New(c, proofs, iam.NewVerificationKeyCache(),
		iam.WithLocalAuthorization(id.Signer.DID(), reg),
		iam.WithLogger(logger))
}

// registryResult exposes the one *registry.Postgres as both collaborator
// interfaces ServerModule needs: registry.Registry (bucket state) and
// logstore.Meta (segment metadata). Production wires a single instance to both,
// matching how New is called directly.
type registryResult struct {
	fx.Out

	Registry   registry.Registry
	Intents    registry.IntentStore
	Locations  registry.LocationStore
	Inclusions registry.InclusionStore
	BlobRefs   registry.BlobRefStore
	GC         registry.GCStore
	Multipart  registry.MultipartStore
	Parks      registry.ParkStore
	Meta       logstore.Meta
}

// provideRegistry wraps the host's pool in the postgres-backed registry and
// exposes it under every interface ServerModule consumes. One *registry.Postgres
// satisfies bucket state (Registry), the spool's upload_intents (IntentStore),
// blob locations (LocationStore), the reference index (BlobRefStore), the GC
// candidate log (GCStore), and segment metadata (Meta). The hilt client is
// required: bucket create/delete/list are forwarded to Hilt, so forge mode
// needs hilt_url/hilt_did configured.
func provideRegistry(pool *pgxpool.Pool) registryResult {
	pg := registry.NewPostgres(pool)
	return registryResult{Registry: pg, Intents: pg, Locations: pg, Inclusions: pg, BlobRefs: pg, GC: pg, Multipart: pg, Parks: pg, Meta: pg}
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

// provideForgeReader builds the network-backed read tier (piri retrieval),
// fronted by a bounded in-memory block cache (see Config.ReadCacheBytes). Blob
// locations resolve from the local blob_locations + shard_inclusions tables
// (the appliance read tier, registry.LocalLocator) rather than the
// indexing-service — same retrieval path, no indexer dependency for reads
// (docs/architecture.md §8). Whole blobs resolve directly; catalog blocks in
// retention-retired segments resolve through their inclusion row to a ranged
// read of the shipped shard. Per-space retrieval authority is the
// request-scoped proof store the IAM service stashes on the context; Forge
// reads it from there, so this reader takes no proof dependency.
func provideForgeReader(cfg config.Config, id ServiceIdentity, locations registry.LocationStore, inclusions registry.InclusionStore, logger *zap.Logger) (blockstore.BlockReader, error) {
	forge, err := blockstore.NewForge(blockstore.ForgeConfig{
		Locator: registry.NewLocalLocator(locations, inclusions),
		Signer:  id.Signer,
		Logger:  logger,
	})
	if err != nil {
		return nil, err
	}
	return blockstore.NewCached(forge, config.ResolveReadCacheBytes(cfg.ReadCacheBytes)), nil
}

// uploaderResult exposes the one *uploader.Forge under both upload seams:
// Uploader (catalog CAR segments) and BodyUploader (per-blob body uploads).
type uploaderResult struct {
	fx.Out

	Uploader     uploader.Uploader
	BodyUploader uploader.BodyUploader
	Deferred     uploader.DeferredBodyUploader
	Remover      uploader.BlobRemover
}

// provideUploader builds the guppy-style edge client that ships to Forge via
// the upload service (/blob/add → /ucan/conclude → /blob/accept → /index/add).
// The same client both ships sealed catalog shards (Uploader) and uploads
// individual body blobs by digest (BodyUploader).
func provideUploader(c *forgeclient.Client, logger *zap.Logger) (uploaderResult, error) {
	f, err := uploader.NewForge(uploader.ForgeConfig{Client: c, Logger: logger})
	if err != nil {
		return uploaderResult{}, err
	}
	return uploaderResult{Uploader: f, BodyUploader: f, Deferred: f, Remover: f}, nil
}
