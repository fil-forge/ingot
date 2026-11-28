package ingot_test

import (
	"testing"

	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot"
)

// TestModuleValidate_Enabled asserts that an enabled ingot.Module, given the
// dependencies a host is expected to provide (logger, pool, service identity),
// forms a complete and acyclic fx graph. fx.ValidateApp checks wireability
// without running constructors, lifecycle hooks, or touching the (nil) pool.
// The module itself provides the token store, the sprue edge-client, and the
// per-plane uploader.
func TestModuleValidate_Enabled(t *testing.T) {
	signer, err := ed25519.GenerateIssuer()
	require.NoError(t, err, "generate signer")

	cfg := config.Config{
		Enabled:          true,
		Addr:             "127.0.0.1:0",
		DataDir:          t.TempDir(),
		UploadServiceURL: "http://127.0.0.1:8000",
		UploadServiceDID: "did:web:upload.example",
	}

	err = fx.ValidateApp(
		fx.NopLogger,
		ingot.Module(cfg),
		// Dependencies the host supplies:
		fx.Supply(zap.NewNop()),
		fx.Supply((*pgxpool.Pool)(nil)),
		fx.Supply(identity.Identity{Issuer: signer}),
	)
	require.NoError(t, err, "ingot.Module graph does not validate")
}

// TestModuleValidate_HiltEnabled asserts the graph also validates with Hilt
// configured, the service behind the hilt-backed IAM provider the server
// requires.
func TestModuleValidate_HiltEnabled(t *testing.T) {
	signer, err := ed25519.GenerateIssuer()
	require.NoError(t, err, "generate signer")

	cfg := config.Config{
		Enabled:          true,
		Addr:             "127.0.0.1:0",
		DataDir:          t.TempDir(),
		UploadServiceURL: "http://127.0.0.1:8000",
		UploadServiceDID: "did:web:upload.example",
		AuthServiceURL:   "http://127.0.0.1:7000",
		AuthServiceDID:   "did:web:auth.example",
	}

	err = fx.ValidateApp(
		fx.NopLogger,
		ingot.Module(cfg),
		fx.Supply(zap.NewNop()),
		fx.Supply((*pgxpool.Pool)(nil)),
		fx.Supply(identity.Identity{Issuer: signer}),
	)
	require.NoError(t, err, "ingot.Module graph with hilt does not validate")
}

// TestModuleValidate_RevocationEnabled asserts the graph also validates with
// the revocation service configured, which adds the firehose consumer
// provider and its lifecycle invoke.
func TestModuleValidate_RevocationEnabled(t *testing.T) {
	signer, err := ed25519.GenerateIssuer()
	require.NoError(t, err, "generate signer")

	cfg := config.Config{
		Enabled:              true,
		Addr:                 "127.0.0.1:0",
		DataDir:              t.TempDir(),
		UploadServiceURL:     "http://127.0.0.1:8000",
		UploadServiceDID:     "did:web:upload.example",
		AuthServiceURL:       "http://127.0.0.1:7000",
		AuthServiceDID:       "did:web:auth.example",
		RevocationServiceURL: "http://127.0.0.1:6000",
		RevocationServiceDID: "did:web:swarf.example",
	}

	err = fx.ValidateApp(
		fx.NopLogger,
		ingot.Module(cfg),
		fx.Supply(zap.NewNop()),
		fx.Supply((*pgxpool.Pool)(nil)),
		fx.Supply(identity.Identity{Issuer: signer}),
	)
	require.NoError(t, err, "ingot.Module graph with revocation service does not validate")
}

// TestModuleValidate_Disabled asserts that a disabled module is an inert empty
// option that needs no host inputs, so a host can always include it.
func TestModuleValidate_Disabled(t *testing.T) {
	require.NoError(t, fx.ValidateApp(fx.NopLogger, ingot.Module(config.Config{})), "disabled ingot.Module should validate as a no-op")
}
