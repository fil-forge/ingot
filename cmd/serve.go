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
	"github.com/fil-forge/ingot/config"
)

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the ingot S3 gateway daemon",
		Long:  `Run the embedded S3 gateway as a daemon.`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(cfgFile)
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

			app, err := buildApp(cmd.Context(), cfg, logger)
			if err != nil {
				return err
			}

			logger.Info("starting ingot daemon",
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

// buildApp builds the Forge-connected app: ingot.Module plus the host
// providers it documents — logger, Postgres pool, and the agent service
// identity.
func buildApp(ctx context.Context, cfg *config.Config, logger *zap.Logger) (*fx.App, error) {
	id, err := loadAgentIdentity(cfg.Identity)
	if err != nil {
		return nil, err
	}
	// The issuer's String() prints the service DID and, for a did:web agent,
	// the underlying key: "did:web:… (key: z6Mk…)".
	logger.Info("loaded agent identity", zap.Stringer("agent", id))
	pool, err := openPool(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, err
	}

	mcfg := *cfg
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
