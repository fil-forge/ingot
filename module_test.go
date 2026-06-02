package ingot_test

import (
	"net/url"
	"testing"

	"github.com/fil-forge/ucantone/principal/ed25519"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot"
	"github.com/fil-forge/ingot/uploader"
)

// TestModuleValidate_Enabled asserts that an enabled ingot.Module, given the
// dependencies a host is expected to provide, forms a complete and acyclic fx
// graph. fx.ValidateApp checks wireability without running constructors,
// lifecycle hooks, or touching the (nil) pool.
func TestModuleValidate_Enabled(t *testing.T) {
	signer, err := ed25519.Generate()
	if err != nil {
		t.Fatalf("generate signer: %v", err)
	}
	endpoint, err := url.Parse("http://127.0.0.1:9000")
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}

	cfg := ingot.Config{
		Enabled:         true,
		Addr:            "127.0.0.1:0",
		DataDir:         t.TempDir(),
		RootAccess:      "key",
		RootSecret:      "secret",
		IndexerEndpoint: "http://127.0.0.1:9000",
		IndexerDID:      "did:web:indexer.example",
	}

	err = fx.ValidateApp(
		fx.NopLogger,
		ingot.Module(cfg),
		// Dependencies the host supplies:
		fx.Supply(zap.NewNop()),
		fx.Supply((*pgxpool.Pool)(nil)),
		fx.Supply(ingot.ServiceIdentity{Signer: signer}),
		fx.Provide(func() uploader.ProviderSelector {
			return uploader.NewStaticProviderSelector(signer.DID(), *endpoint)
		}),
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

// TestModuleValidate_HomeSelector asserts that, with the HomeProvider config
// set, the module builds its own ProviderSelector and the host does NOT need to
// supply one — the single-home-piri convenience path.
func TestModuleValidate_HomeSelector(t *testing.T) {
	signer, err := ed25519.Generate()
	if err != nil {
		t.Fatalf("generate signer: %v", err)
	}
	cfg := ingot.Config{
		Enabled:         true,
		Addr:            "127.0.0.1:0",
		DataDir:         t.TempDir(),
		RootAccess:      "key",
		RootSecret:      "secret",
		IndexerEndpoint: "http://127.0.0.1:9000",
		IndexerDID:      "did:web:indexer.example",
		HomeProviderDID: signer.DID().String(),
		HomeProviderURL: "http://127.0.0.1:8000",
	}

	// No uploader.ProviderSelector supplied — the module provides one from cfg.
	err = fx.ValidateApp(
		fx.NopLogger,
		ingot.Module(cfg),
		fx.Supply(zap.NewNop()),
		fx.Supply((*pgxpool.Pool)(nil)),
		fx.Supply(ingot.ServiceIdentity{Signer: signer}),
	)
	if err != nil {
		t.Fatalf("home-selector ingot.Module graph does not validate: %v", err)
	}
}
