package ingot_test

import (
	"testing"

	"github.com/fil-forge/ucantone/principal/ed25519"
	"github.com/jackc/pgx/v5/pgxpool"
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
	signer, err := ed25519.Generate()
	if err != nil {
		t.Fatalf("generate signer: %v", err)
	}

	cfg := ingot.Config{
		Enabled:          true,
		Addr:             "127.0.0.1:0",
		DataDir:          t.TempDir(),
		RootAccess:       "key",
		RootSecret:       "secret",
		IndexerEndpoint:  "http://127.0.0.1:9000",
		IndexerDID:       "did:web:indexer.example",
		UploadServiceURL: "http://127.0.0.1:8000",
		UploadServiceDID: "did:web:upload.example",
	}

	err = fx.ValidateApp(
		fx.NopLogger,
		ingot.Module(cfg),
		// Dependencies the host supplies:
		fx.Supply(zap.NewNop()),
		fx.Supply((*pgxpool.Pool)(nil)),
		fx.Supply(ingot.ServiceIdentity{Signer: signer}),
	)
	if err != nil {
		t.Fatalf("ingot.Module graph does not validate: %v", err)
	}
}

// TestModuleValidate_Disabled asserts that a disabled module is an inert empty
// option that needs no host inputs, so a host can always include it.
func TestModuleValidate_Disabled(t *testing.T) {
	if err := fx.ValidateApp(fx.NopLogger, ingot.Module(ingot.Config{})); err != nil {
		t.Fatalf("disabled ingot.Module should validate as a no-op: %v", err)
	}
}
