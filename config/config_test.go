package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/ucantone/ucan/delegation"
)

// validConfig returns a Config that passes Validate: every required field
// set, with an agent key file that exists (Validate stats it, but does not
// parse it) and an inline proofs container holding one delegation.
func validConfig(t *testing.T) Config {
	t.Helper()
	keyFile := filepath.Join(t.TempDir(), "agent.pem")
	if err := os.WriteFile(keyFile, []byte("stat-only"), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	return Config{
		Addr:             "127.0.0.1:9000",
		DataDir:          "/data",
		RootAccess:       "root-access",
		RootSecret:       "root-secret",
		PostgresDSN:      "postgres://ingot@127.0.0.1:5432/ingot",
		Identity:         IdentityConfig{KeyFile: keyFile},
		UploadServiceURL: "http://127.0.0.1:8000",
		UploadServiceDID: "did:web:upload.example",
		AuthServiceURL:   "http://127.0.0.1:7000",
		AuthServiceDID:   "did:web:auth.example",

		AuthServiceProofs: encodedProofs(t, mintDelegation(t)),
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

func TestValidate_OK(t *testing.T) {
	cfg := validConfig(t)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got: %v", err)
	}
}

// TestValidate_RequiredFields drops each required field in turn and asserts
// the aggregated error names it.
func TestValidate_RequiredFields(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"addr", func(c *Config) { c.Addr = "" }, "addr is required"},
		{"data_dir", func(c *Config) { c.DataDir = "" }, "data_dir is required"},
		{"postgres_dsn", func(c *Config) { c.PostgresDSN = "" }, "postgres_dsn is required"},
		{"identity key unset", func(c *Config) { c.Identity.KeyFile = "" }, "identity.key_file (agent PEM) is required"},
		{"identity key missing", func(c *Config) { c.Identity.KeyFile = "/nonexistent/agent.pem" }, "identity.key_file"},
		{"upload service", func(c *Config) { c.UploadServiceURL = "" }, "upload_service_url and upload_service_did are required"},
		{"auth service", func(c *Config) { c.AuthServiceDID = "" }, "auth_service_url and auth_service_did are required"},
		{"revocation url without did", func(c *Config) { c.RevocationServiceURL = "http://127.0.0.1:6000" }, "revocation_service_url and revocation_service_did must be set together"},
		{"revocation did without url", func(c *Config) { c.RevocationServiceDID = "did:web:swarf.example" }, "revocation_service_url and revocation_service_did must be set together"},
		{"bad seal_age", func(c *Config) { c.SealAge = "not-a-duration" }, "parse seal_age"},
		{"bad cors origin", func(c *Config) { c.CORSAllowedOrigins = []string{"app.example"} }, "cors_allowed_origins"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig(t)
			tc.mutate(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

// TestValidate_RevocationServicePair: the revocation service is optional, but
// URL and DID come as a pair.
func TestValidate_RevocationServicePair(t *testing.T) {
	cfg := validConfig(t)
	cfg.RevocationServiceURL = "http://127.0.0.1:6000"
	cfg.RevocationServiceDID = "did:web:swarf.example"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config with revocation pair set, got: %v", err)
	}
}

// TestValidate_AuthServiceProofs: the proofs a configured auth service needs
// are resolved eagerly, so a missing, unreadable, or empty value fails at
// startup instead of on the first authorized request.
func TestValidate_AuthServiceProofs(t *testing.T) {
	cases := []struct {
		name    string
		proofs  string
		wantErr string
	}{
		{"unset", "", "auth_service_proofs is required when auth_service_url is set"},
		{"missing file", "/nonexistent/proofs.cbor", "auth_service_proofs: ingot: decode proofs container"},
		{"no delegations", encodedProofs(t), "auth_service_proofs: the container holds no delegations"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig(t)
			cfg.AuthServiceProofs = tc.proofs
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}
