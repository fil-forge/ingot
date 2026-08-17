package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// validConfig returns a Config that passes Validate: every required field
// set, with an agent key file that exists (Validate stats it, but does not
// parse it).
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
	}
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

// TestValidate_AuthServiceProofs: the optional proofs value is loaded eagerly
// so a bad path or encoding fails at startup.
func TestValidate_AuthServiceProofs(t *testing.T) {
	cfg := validConfig(t)
	cfg.AuthServiceProofs = "/nonexistent/proofs.cbor"
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "auth_service_proofs") {
		t.Fatalf("expected error containing %q, got: %v", "auth_service_proofs", err)
	}
}
