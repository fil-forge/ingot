package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/fil-forge/ingot/bucket"
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
		Addr:              "127.0.0.1:9000",
		DataDir:           "/data",
		PostgresDSN:       "postgres://ingot@127.0.0.1:5432/ingot",
		Identity:          IdentityConfig{KeyFile: keyFile},
		UploadServiceURL:  "http://127.0.0.1:8000",
		UploadServiceDID:  "did:web:upload.example",
		AuthServiceURL:    "http://127.0.0.1:7000",
		AuthServiceDID:    "did:web:auth.example",
		AuthServiceProofs: encodedProofs(t, mintDelegation(t)),
		RegionKey:         RegionKeyConfig{Provider: "inprocess"},
		TenantKey:         TenantKeyConfig{PLCDirectoryURL: "http://plc.example:3000"},
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
		{"identity service id not a DID", func(c *Config) { c.Identity.ServiceID = "ingot.example" }, "identity.service_id"},
		{"upload service", func(c *Config) { c.UploadServiceURL = "" }, "upload_service_url and upload_service_did are required"},
		{"auth service", func(c *Config) { c.AuthServiceDID = "" }, "auth_service_url and auth_service_did are required"},
		{"revocation url without did", func(c *Config) { c.RevocationServiceURL = "http://127.0.0.1:6000" }, "revocation_service_url and revocation_service_did must be set together"},
		{"revocation did without url", func(c *Config) { c.RevocationServiceDID = "did:web:swarf.example" }, "revocation_service_url and revocation_service_did must be set together"},
		{"bad seal_age", func(c *Config) { c.SealAge = "not-a-duration" }, "parse seal_age"},
		{"bad release_grace", func(c *Config) { c.ReleaseGrace = "soon" }, "parse release_grace"},
		{"negative local_blob_max_bytes", func(c *Config) { c.LocalBlobMaxBytes = -1 }, "local_blob_max_bytes -1: must not be negative"},
		{"bad cache_min_residency", func(c *Config) { c.CacheMinResidency = "soon" }, "parse cache_min_residency"},
		{"negative cache_min_residency", func(c *Config) { c.CacheMinResidency = "-1m" }, `cache_min_residency "-1m": must not be negative`},
		{"negative cache_read_retention", func(c *Config) { c.CacheReadRetention = "-1m" }, `cache_read_retention "-1m": must not be negative`},
		{"bad local_blob_orphan_age", func(c *Config) { c.LocalBlobOrphanAge = "soon" }, "parse local_blob_orphan_age"},
		{"short local_blob_orphan_age", func(c *Config) { c.LocalBlobOrphanAge = "59m" }, `local_blob_orphan_age "59m": must be at least 1h`},
		{"bad cors origin", func(c *Config) { c.CORSAllowedOrigins = []string{"app.example"} }, "cors_allowed_origins"},
		{"regionkey provider unset", func(c *Config) { c.RegionKey.Provider = "" }, "regionkey.provider is required"},
		{"tenantkey url unset", func(c *Config) { c.TenantKey.PLCDirectoryURL = "" }, "tenantkey.plc_directory_url is required"},
		{"tenantkey url relative", func(c *Config) { c.TenantKey.PLCDirectoryURL = "plc:3000" }, "not an absolute URL"},
		{"tenantkey bad ttl", func(c *Config) { c.TenantKey.CacheTTL = "soon" }, "tenantkey.cache_ttl"},
		{"tenantkey zero ttl", func(c *Config) { c.TenantKey.CacheTTL = "0s" }, "must be positive"},
		{"unknown regionkey provider", func(c *Config) { c.RegionKey.Provider = "hsm" }, `regionkey.provider "hsm" is not one of openbao, inprocess`},
		{"openbao without key", func(c *Config) { c.RegionKey.Provider = "openbao" }, "regionkey.openbao.key"},
		{"inprocess kek not base64", func(c *Config) {
			c.RegionKey.Provider = "inprocess"
			c.RegionKey.InProcess.KEK = "not-base64!!"
		}, "regionkey.inprocess.kek"},
		{"inprocess kek wrong length", func(c *Config) {
			c.RegionKey.Provider = "inprocess"
			c.RegionKey.InProcess.KEK = "c2hvcnQ=" // "short"
		}, "must decode to 32 bytes"},
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

// localBlobKnobs is the local blob storage subset of ServerConfig, for
// comparing it whole.
type localBlobKnobs struct {
	MaxBytes      int64
	MinResidency  time.Duration
	ReadRetention time.Duration
	OrphanAge     time.Duration
}

func TestServerConfig_LocalBlobKnobs(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
		want   localBlobKnobs
	}{
		{
			name:   "defaults",
			mutate: func(*Config) {},
			want:   localBlobKnobs{MaxBytes: 0, MinResidency: 10 * time.Minute, ReadRetention: time.Hour, OrphanAge: 24 * time.Hour},
		},
		{
			name: "explicit values",
			mutate: func(c *Config) {
				c.LocalBlobMaxBytes = 1 << 40
				c.CacheMinResidency = "0s"
				c.CacheReadRetention = "15m"
				c.LocalBlobOrphanAge = "2h"
			},
			want: localBlobKnobs{MaxBytes: 1 << 40, MinResidency: 0, ReadRetention: 15 * time.Minute, OrphanAge: 2 * time.Hour},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig(t)
			tc.mutate(&cfg)
			sc, err := cfg.ServerConfig()
			if err != nil {
				t.Fatalf("ServerConfig: %v", err)
			}
			got := localBlobKnobs{sc.LocalBlobMaxBytes, sc.CacheMinResidency, sc.CacheReadRetention, sc.LocalBlobOrphanAge}
			if got != tc.want {
				t.Fatalf("local blob knobs = %+v, want %+v", got, tc.want)
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

// TestValidate_RegionKey: both providers validate with their required
// settings present. The provider itself is required (see the required-fields
// table); only the implementation choice is configuration.
func TestValidate_RegionKey(t *testing.T) {
	cfg := validConfig(t)
	cfg.RegionKey.Provider = "openbao"
	cfg.RegionKey.OpenBao.Key = "region-kek"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid openbao regionkey config, got: %v", err)
	}

	cfg = validConfig(t)
	cfg.RegionKey.Provider = "inprocess"
	cfg.RegionKey.InProcess.KEK = base64.StdEncoding.EncodeToString(make([]byte, 32))
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid inprocess regionkey config, got: %v", err)
	}

	cfg = validConfig(t)
	cfg.RegionKey.Provider = "inprocess" // empty KEK: generated at startup
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid inprocess config with no KEK, got: %v", err)
	}
}

// TestValidate_MaxBlobSize: a max_blob_size whose encrypted envelope cannot
// ship to a default-configured piri fails at startup, not at the first PUT's
// BlobSizeLimitExceeded. The default (and zero) pass.
func TestValidate_MaxBlobSize(t *testing.T) {
	cfg := validConfig(t)
	cfg.MaxBlobSize = bucket.DefaultMaxBlobSize
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected the default max_blob_size to validate, got: %v", err)
	}

	cfg = validConfig(t)
	cfg.MaxBlobSize = 256 << 20 // over the ceiling once the envelope framing is added
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "piece cap") {
		t.Fatalf("expected a piece-cap error for a 256 MiB max_blob_size, got: %v", err)
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

// TestValidate_IdentityServiceID: the optional service DID must parse as a
// DID; a did:web is the expected shape but the method is not enforced.
func TestValidate_IdentityServiceID(t *testing.T) {
	cfg := validConfig(t)
	cfg.Identity.ServiceID = "did:web:ingot.example"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config with a did:web service id, got: %v", err)
	}
}

// TestLoad_IdentityEnv: INGOT_IDENTITY_* env vars apply even when the YAML
// omits the identity block.
func TestLoad_IdentityEnv(t *testing.T) {
	t.Setenv("INGOT_IDENTITY_KEY_FILE", "/keys/agent.pem")
	t.Setenv("INGOT_IDENTITY_SERVICE_ID", "did:web:ingot.example")
	cfgFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfgFile, []byte("addr: \"127.0.0.1:9000\"\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := Load(cfgFile)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Identity.KeyFile != "/keys/agent.pem" {
		t.Fatalf("identity.key_file = %q, want /keys/agent.pem", cfg.Identity.KeyFile)
	}
	if cfg.Identity.ServiceID != "did:web:ingot.example" {
		t.Fatalf("identity.service_id = %q, want did:web:ingot.example", cfg.Identity.ServiceID)
	}
}

// TestLoad_EnvWithoutYAMLKey checks that an INGOT_* variable sets a key the
// config file leaves out, which viper's AutomaticEnv alone would ignore,
// and that it overrides a key the file sets.
func TestLoad_EnvWithoutYAMLKey(t *testing.T) {
	t.Setenv("INGOT_LOCAL_BLOB_MAX_BYTES", "1073741824")
	t.Setenv("INGOT_LOCAL_BLOB_ORPHAN_AGE", "2h")
	t.Setenv("INGOT_CORS_ALLOWED_ORIGINS", "https://a.example,https://b.example")
	t.Setenv("INGOT_CATALOG_PLANE_RETAIN", "3")
	t.Setenv("INGOT_CATALOG_PLANE_SHIP", "false")
	t.Setenv("INGOT_REGIONKEY_OPENBAO_ADDRESS", "https://bao.example:8200")
	t.Setenv("INGOT_REGIONKEY_OPENBAO_MOUNT", "transit-env")
	t.Setenv("INGOT_DATA_DIR", "/from-env")
	cfgFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfgFile, []byte("data_dir: /from-yaml\nregionkey:\n  openbao:\n    key: from-yaml\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := Load(cfgFile)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.LocalBlobMaxBytes != 1<<30 {
		t.Errorf("local_blob_max_bytes = %d, want %d", cfg.LocalBlobMaxBytes, 1<<30)
	}
	if cfg.LocalBlobOrphanAge != "2h" {
		t.Errorf("local_blob_orphan_age = %q, want 2h", cfg.LocalBlobOrphanAge)
	}
	if want := []string{"https://a.example", "https://b.example"}; !slices.Equal(cfg.CORSAllowedOrigins, want) {
		t.Errorf("cors_allowed_origins = %q, want %q", cfg.CORSAllowedOrigins, want)
	}
	if cfg.CatalogPlane.Retain != 3 {
		t.Errorf("catalog_plane.retain = %d, want 3", cfg.CatalogPlane.Retain)
	}
	if cfg.CatalogPlane.Ship == nil || *cfg.CatalogPlane.Ship {
		t.Errorf("catalog_plane.ship = %v, want a non-nil false", cfg.CatalogPlane.Ship)
	}
	if got := cfg.RegionKey.OpenBao; got.Address != "https://bao.example:8200" || got.Key != "from-yaml" {
		t.Errorf("regionkey.openbao address, key = %q, %q, want the env address beside the YAML key", got.Address, got.Key)
	}
	if got := cfg.RegionKey.OpenBao.Mount; got != "transit-env" {
		t.Errorf("regionkey.openbao.mount = %q, want the env value over the default", got)
	}
	if cfg.DataDir != "/from-env" {
		t.Errorf("data_dir = %q, want /from-env", cfg.DataDir)
	}
	if cfg.Addr != "0.0.0.0:8080" {
		t.Errorf("addr = %q, want the default 0.0.0.0:8080", cfg.Addr)
	}
}

// TestLoad_CacheWritesEnv: INGOT_CACHE_WRITES binds even when the YAML omits
// cache_writes, and leaving both unset caches writes.
func TestLoad_CacheWritesEnv(t *testing.T) {
	for _, tc := range []struct {
		env  string
		drop bool
	}{{"", false}, {"false", true}, {"true", false}} {
		t.Run("env="+tc.env, func(t *testing.T) {
			if tc.env != "" {
				t.Setenv("INGOT_CACHE_WRITES", tc.env)
			}
			cfgFile := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(cfgFile, []byte("addr: \"127.0.0.1:9000\"\n"), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			cfg, err := Load(cfgFile)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if drop := cfg.CacheWrites != nil && !*cfg.CacheWrites; drop != tc.drop {
				t.Fatalf("drops accepted bodies = %v, want %v", drop, tc.drop)
			}
		})
	}
}

func TestServerConfig_CacheWrites(t *testing.T) {
	no, yes := false, true
	for name, tc := range map[string]struct {
		set  *bool
		drop bool
	}{
		"unset caches": {set: nil, drop: false},
		"true caches":  {set: &yes, drop: false},
		"false drops":  {set: &no, drop: true},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig(t)
			cfg.CacheWrites = tc.set
			sc, err := cfg.ServerConfig()
			if err != nil {
				t.Fatalf("ServerConfig: %v", err)
			}
			if sc.DropAcceptedBodies != tc.drop {
				t.Fatalf("DropAcceptedBodies = %v, want %v", sc.DropAcceptedBodies, tc.drop)
			}
		})
	}
}

func TestLoad_EncryptionAllowNoneEnv(t *testing.T) {
	for _, tc := range []struct {
		env   string
		allow bool
	}{{"", false}, {"true", true}, {"false", false}} {
		t.Run("env="+tc.env, func(t *testing.T) {
			if tc.env != "" {
				t.Setenv("INGOT_ENCRYPTION_ALLOW_NONE", tc.env)
			}
			cfgFile := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(cfgFile, []byte("addr: \"127.0.0.1:9000\"\n"), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			cfg, err := Load(cfgFile)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Encryption.AllowNone != tc.allow {
				t.Fatalf("encryption.allow_none = %v, want %v", cfg.Encryption.AllowNone, tc.allow)
			}
		})
	}
}

func TestServerConfig_AllowNoneEncryption(t *testing.T) {
	for _, allow := range []bool{false, true} {
		cfg := validConfig(t)
		cfg.Encryption.AllowNone = allow
		sc, err := cfg.ServerConfig()
		if err != nil {
			t.Fatalf("ServerConfig: %v", err)
		}
		if sc.AllowNoneEncryption != allow {
			t.Fatalf("AllowNoneEncryption = %v, want %v", sc.AllowNoneEncryption, allow)
		}
	}
}
