package cmd

import (
	"os"
	"path/filepath"
	"testing"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/ucantone/ucan/delegation"
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
		// A hilt-backed client refuses to build without proof chains, and the
		// provider runs eagerly here; one delegation satisfies it.
		AuthServiceProofs: encodedProofs(t, mintDelegation(t)),
		// The region key provider is a required dependency; inprocess needs
		// no external service (the constructor generates a throwaway KEK).
		RegionKey: config.RegionKeyConfig{Provider: "inprocess"},
		// The tenant key source is required too; it makes no network calls at
		// construction, so an unreachable directory is fine here.
		TenantKey: config.TenantKeyConfig{PLCDirectoryURL: "http://127.0.0.1:2582"},
	}

	app, err := buildApp(t.Context(), cfg, zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("fx graph does not validate: %v", err)
	}
	if app == nil {
		t.Fatal("buildApp returned a nil app")
	}
}

// mintDelegation returns one space→agent /blob/add delegation.
func mintDelegation(t *testing.T) ucan.Delegation {
	t.Helper()
	space, err := ed25519.GenerateIssuer()
	if err != nil {
		t.Fatalf("generate space: %v", err)
	}
	agent, err := ed25519.GenerateIssuer()
	if err != nil {
		t.Fatalf("generate agent: %v", err)
	}
	d, err := blobcmds.Add.Delegate(space, agent.DID(), space.DID(), delegation.WithNoExpiration())
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	return d
}

// encodedProofs renders the delegations as a base64 container, the inline
// form an auth_service_proofs config value accepts.
func encodedProofs(t *testing.T, dlgs ...ucan.Delegation) string {
	t.Helper()
	encoded, err := container.Encode(container.Base64, container.New(container.WithDelegations(dlgs...)))
	if err != nil {
		t.Fatalf("encode proofs container: %v", err)
	}
	return string(encoded)
}
