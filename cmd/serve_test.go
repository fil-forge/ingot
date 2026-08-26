package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/config"
)

// TestApp_GraphValidates ensures the fx graph is complete — every collaborator
// ingot.ServerModule consumes is provided. fx resolves the lifecycle invoke
// eagerly at fx.New, so app.Err() surfaces any missing provider without
// binding a listener or running migrations. Guards cmd/serve.go's provider
// list from drifting away from what ServerModule needs.
//
// buildApp dials and pings Postgres before constructing the graph, so this
// runs only when INGOT_TEST_DSN names a reachable database (same gate as
// registry's live test). The always-on, DB-free graph validation lives in the
// root package's TestModuleValidate_* (fx.ValidateApp over ingot.Module).
func TestApp_GraphValidates(t *testing.T) {
	dsn := os.Getenv("INGOT_TEST_DSN")
	if dsn == "" {
		t.Skip("set INGOT_TEST_DSN to run the buildApp graph test (buildApp pings Postgres)")
	}

	signer, err := ed25519.Generate()
	if err != nil {
		t.Fatalf("generate agent key: %v", err)
	}
	pemBytes, err := identity.EncodeSignerToPEM(signer)
	if err != nil {
		t.Fatalf("encode agent key: %v", err)
	}
	keyFile := filepath.Join(t.TempDir(), "agent.pem")
	if err := os.WriteFile(keyFile, pemBytes, 0o600); err != nil {
		t.Fatalf("write agent key: %v", err)
	}

	cfg := &config.Config{
		Addr:             "127.0.0.1:0",
		DataDir:          t.TempDir(),
		PostgresDSN:      dsn,
		Identity:         config.IdentityConfig{KeyFile: keyFile},
		UploadServiceURL: "http://127.0.0.1:8000",
		UploadServiceDID: "did:web:upload.example",
		AuthServiceURL:   "http://127.0.0.1:7000",
		AuthServiceDID:   "did:web:auth.example",
		// The region key provider is a required dependency; inprocess needs
		// no external service (the constructor generates a throwaway KEK).
		RegionKey: config.RegionKeyConfig{Provider: "inprocess"},
	}

	app, err := buildApp(t.Context(), cfg, zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("fx graph does not validate: %v", err)
	}
	if app == nil {
		t.Fatal("buildApp returned a nil app")
	}
}
