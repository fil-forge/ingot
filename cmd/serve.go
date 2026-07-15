package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot"
	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the ingot S3 gateway daemon",
		Long: `Run the embedded S3 gateway as a daemon.

Mode is selected by config (mode: standalone|forge):
  - standalone: no Forge dependency; in-memory registry; both planes are
    retained on local disk and never shipped. Mirrors the test harness.
  - forge: Postgres-backed registry/meta, network reads, and per-plane
    upload pipelines that ship to the Forge upload service (sprue).`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := Load(cfgFile)
			if err != nil {
				return err
			}
			if err := cfg.Validate(); err != nil {
				return err
			}
			logger, err := buildLogger(cfg.LogLevel)
			if err != nil {
				return err
			}
			defer func() { _ = logger.Sync() }()

			if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
				return fmt.Errorf("creating data dir %s: %w", cfg.DataDir, err)
			}

			var app *fx.App
			switch cfg.Mode {
			case ModeStandalone:
				app, err = standaloneApp(cfg, logger)
			case ModeForge:
				app, err = forgeApp(cmd.Context(), cfg, logger)
			default:
				return fmt.Errorf("unknown mode %q", cfg.Mode)
			}
			if err != nil {
				return err
			}

			logger.Info("starting ingot daemon",
				zap.String("mode", cfg.Mode),
				zap.String("addr", cfg.Addr),
				zap.String("data_dir", cfg.DataDir),
			)
			app.Run() // blocks until SIGINT/SIGTERM, then runs OnStop hooks
			return nil
		},
	}
}

func fxLogger(logger *zap.Logger) fx.Option {
	return fx.WithLogger(func() fxevent.Logger { return &fxevent.ZapLogger{Logger: logger} })
}

// standaloneApp builds the no-Forge app: ingot.ServerModule with the
// in-memory fakes, both planes configured never to ship (so their CARs
// are retained on local disk and serve every read).
func standaloneApp(cfg *DaemonConfig, logger *zap.Logger) (*fx.App, error) {
	sc, err := cfg.Config.ServerConfig()
	if err != nil {
		return nil, err
	}
	sc.ShipCatalog = false

	mem := inmem.NewMemStore()
	app := fx.New(
		fxLogger(logger),
		fx.Supply(logger),
		fx.Supply(sc),
		fx.Provide(
			fx.Annotate(func() *inmem.MemStore { return mem }, fx.As(new(registry.Registry))),
			fx.Annotate(func() *inmem.MemStore { return mem }, fx.As(new(registry.IntentStore))),
			fx.Annotate(func() *inmem.MemStore { return mem }, fx.As(new(registry.LocationStore))),
			fx.Annotate(func() *inmem.MemStore { return mem }, fx.As(new(registry.BlobRefStore))),
			fx.Annotate(func() *inmem.MemStore { return mem }, fx.As(new(registry.GCStore))),
			fx.Annotate(func() *inmem.MemStore { return mem }, fx.As(new(registry.MultipartStore))),
			fx.Annotate(func() *inmem.MemStore { return mem }, fx.As(new(registry.ParkStore))),
			fx.Annotate(func() *inmem.MemStore { return mem }, fx.As(new(logstore.Meta))),
			fx.Annotate(func() inmem.NopBaseReader { return inmem.NopBaseReader{} }, fx.As(new(blockstore.BlockReader))),
			fx.Annotate(func() inmem.NopUploader { return inmem.NopUploader{} }, fx.As(new(uploader.Uploader))),
			fx.Annotate(func() inmem.NopUploader { return inmem.NopUploader{} }, fx.As(new(uploader.BodyUploader))),
			fx.Annotate(func() inmem.NopUploader { return inmem.NopUploader{} }, fx.As(new(uploader.DeferredBodyUploader))),
			fx.Annotate(func() inmem.NopUploader { return inmem.NopUploader{} }, fx.As(new(uploader.BlobRemover))),
		),
		ingot.ServerModule,
	)
	return app, app.Err()
}

// forgeApp builds the Forge-connected app: ingot.Module plus the host
// providers it documents — logger, Postgres pool, and the agent service
// identity.
func forgeApp(ctx context.Context, cfg *DaemonConfig, logger *zap.Logger) (*fx.App, error) {
	id, err := loadAgentIdentity(cfg.Identity.KeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := openPool(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, err
	}

	mcfg := cfg.Config
	mcfg.Enabled = true

	app := fx.New(
		fxLogger(logger),
		fx.Supply(logger),
		fx.Supply(pool),
		fx.Supply(id),
		ingot.Module(mcfg),
		fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.Hook{OnStop: func(context.Context) error { pool.Close(); return nil }})
		}),
	)
	if err := app.Err(); err != nil {
		pool.Close()
		return nil, err
	}
	return app, nil
}
