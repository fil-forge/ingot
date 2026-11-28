package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/fil-forge/ingot/bucket"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validConfig returns a Config that passes Validate: every required field
// set, with an agent key file that exists (Validate stats it, but does not
// parse it) and an inline proofs container holding one delegation.
func validConfig(t *testing.T) Config {
	t.Helper()
	keyFile := filepath.Join(t.TempDir(), "agent.pem")
	require.NoError(t, os.WriteFile(keyFile, []byte("stat-only"), 0o600), "write key file")
	return Config{
		TaskWorkers:       TaskWorkerConfig{Bucket: 2},
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
	require.NoError(t, err, "generate space")
	agent, err := ed25519.GenerateIssuer()
	require.NoError(t, err, "generate agent")
	d, err := blobcmds.Add.Delegate(space, agent.DID(), space.DID(), delegation.WithNoExpiration())
	require.NoError(t, err, "delegate")
	return d
}

// encodedProofs renders the delegations as a base64 container, the inline
// form an auth_service_proofs config value accepts.
func encodedProofs(t *testing.T, dlgs ...ucan.Delegation) string {
	t.Helper()
	encoded, err := container.Encode(container.Base64, container.New(container.WithDelegations(dlgs...)))
	require.NoError(t, err, "encode proofs container")
	return string(encoded)
}

func TestValidate_OK(t *testing.T) {
	cfg := validConfig(t)
	require.NoError(t, cfg.Validate())
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
		{"bad complete_multipart_keepalive", func(c *Config) { c.CompleteMultipartKeepalive = "soon" }, "parse complete_multipart_keepalive"},
		{"negative local_blob_max_bytes", func(c *Config) { c.LocalBlobMaxBytes = -1 }, "local_blob_max_bytes -1: must not be negative"},
		{"bad usage_report_interval", func(c *Config) { c.UsageReportInterval = "often" }, "parse usage_report_interval"},
		{"negative usage_report_interval", func(c *Config) { c.UsageReportInterval = "-1m" }, `usage_report_interval "-1m": must not be negative`},
		{"zero usage_report_interval", func(c *Config) { c.UsageReportInterval = "0s" }, `usage_report_interval "0s": must be positive`},
		{"bad cache_ttl", func(c *Config) { c.CacheTTL = "soon" }, "parse cache_ttl"},
		{"negative cache_ttl", func(c *Config) { c.CacheTTL = "-1m" }, `cache_ttl "-1m": must not be negative`},
		{"zero cache_ttl", func(c *Config) { c.CacheTTL = "0s" }, `cache_ttl "0s": must be positive`},
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
			require.Error(t, err, "expected error containing %q, got: %v", tc.wantErr, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// localBlobKnobs is the local blob storage subset of ServerConfig, for
// comparing it whole.
type localBlobKnobs struct {
	MaxBytes  int64
	TTL       time.Duration
	OrphanAge time.Duration
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
			want:   localBlobKnobs{MaxBytes: 0, TTL: time.Hour, OrphanAge: 24 * time.Hour},
		},
		{
			name: "explicit values",
			mutate: func(c *Config) {
				c.LocalBlobMaxBytes = 1 << 40
				c.CacheTTL = "15m"
				c.LocalBlobOrphanAge = "2h"
			},
			want: localBlobKnobs{MaxBytes: 1 << 40, TTL: 15 * time.Minute, OrphanAge: 2 * time.Hour},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig(t)
			tc.mutate(&cfg)
			sc, err := cfg.ServerConfig()
			require.NoError(t, err, "ServerConfig")
			got := localBlobKnobs{sc.LocalBlobMaxBytes, sc.CacheTTL, sc.LocalBlobOrphanAge}
			require.Equal(t, tc.want, got, "local blob knobs")
		})
	}
}

// TestValidate_RevocationServicePair: the revocation service is optional, but
// URL and DID come as a pair.
func TestValidate_RevocationServicePair(t *testing.T) {
	cfg := validConfig(t)
	cfg.RevocationServiceURL = "http://127.0.0.1:6000"
	cfg.RevocationServiceDID = "did:web:swarf.example"
	require.NoError(t, cfg.Validate(), "expected valid config with revocation pair set")
}

// TestValidate_RegionKey: both providers validate with their required
// settings present. The provider itself is required (see the required-fields
// table); only the implementation choice is configuration.
func TestValidate_RegionKey(t *testing.T) {
	cfg := validConfig(t)
	cfg.RegionKey.Provider = "openbao"
	cfg.RegionKey.OpenBao.Key = "region-kek"
	require.NoError(t, cfg.Validate(), "expected valid openbao regionkey config")

	cfg = validConfig(t)
	cfg.RegionKey.Provider = "inprocess"
	cfg.RegionKey.InProcess.KEK = base64.StdEncoding.EncodeToString(make([]byte, 32))
	require.NoError(t, cfg.Validate(), "expected valid inprocess regionkey config")

	cfg = validConfig(t)
	cfg.RegionKey.Provider = "inprocess" // empty KEK: generated at startup
	require.NoError(t, cfg.Validate(), "expected valid inprocess config with no KEK")
}

// TestValidate_MaxBlobSize: a max_blob_size whose encrypted envelope cannot
// ship to a default-configured piri fails at startup, not at the first PUT's
// BlobSizeLimitExceeded. The default (and zero) pass.
func TestValidate_MaxBlobSize(t *testing.T) {
	cfg := validConfig(t)
	cfg.MaxBlobSize = bucket.DefaultMaxBlobSize
	require.NoError(t, cfg.Validate(), "expected the default max_blob_size to validate")

	cfg = validConfig(t)
	cfg.MaxBlobSize = 256 << 20 // over the ceiling once the envelope framing is added
	err := cfg.Validate()
	require.Error(t, err, "expected a piece-cap error for a 256 MiB max_blob_size")
	require.Contains(t, err.Error(), "piece cap", "expected a piece-cap error for a 256 MiB max_blob_size")
}

// TestValidate_MinMaxBlobSize: a max_blob_size small enough that a single
// 5 GiB request could exceed one manifest's blob list is refused at startup;
// the minimum itself is accepted, and at the minimum a 5 GiB body fits.
func TestValidate_MinMaxBlobSize(t *testing.T) {
	blobs := (int64(5<<30) + MinMaxBlobSize - 1) / MinMaxBlobSize
	require.LessOrEqual(t, blobs, int64(bucket.MaxBodyBlobs), "a 5 GiB body at the %d-byte minimum splits into %d blobs, over the %d a manifest lists", MinMaxBlobSize, blobs, bucket.MaxBodyBlobs)

	cfg := validConfig(t)
	cfg.MaxBlobSize = MinMaxBlobSize
	require.NoError(t, cfg.Validate(), "expected the minimum max_blob_size to validate")

	cfg = validConfig(t)
	cfg.MaxBlobSize = MinMaxBlobSize - 1
	require.ErrorContains(t, cfg.Validate(), "below the", "expected a minimum error for max_blob_size %d", MinMaxBlobSize-1)
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
			require.Error(t, err, "expected error containing %q, got: %v", tc.wantErr, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestValidate_IdentityServiceID: the optional service DID must parse as a
// DID; a did:web is the expected shape but the method is not enforced.
func TestValidate_IdentityServiceID(t *testing.T) {
	cfg := validConfig(t)
	cfg.Identity.ServiceID = "did:web:ingot.example"
	require.NoError(t, cfg.Validate(), "expected valid config with a did:web service id")
}

// TestLoad_IdentityEnv: INGOT_IDENTITY_* env vars apply even when the YAML
// omits the identity block.
func TestLoad_IdentityEnv(t *testing.T) {
	t.Setenv("INGOT_IDENTITY_KEY_FILE", "/keys/agent.pem")
	t.Setenv("INGOT_IDENTITY_SERVICE_ID", "did:web:ingot.example")
	cfgFile := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte("addr: \"127.0.0.1:9000\"\n"), 0o600), "write config")
	cfg, err := Load(cfgFile)
	require.NoError(t, err, "Load")
	require.Equal(t, "/keys/agent.pem", cfg.Identity.KeyFile, "identity.key_file")
	require.Equal(t, "did:web:ingot.example", cfg.Identity.ServiceID, "identity.service_id")
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
	require.NoError(t, os.WriteFile(cfgFile, []byte("data_dir: /from-yaml\nregionkey:\n  openbao:\n    key: from-yaml\n"), 0o600), "write config")
	cfg, err := Load(cfgFile)
	require.NoError(t, err, "Load")
	assert.Equal(t, int64(1<<30), cfg.LocalBlobMaxBytes, "local_blob_max_bytes")
	assert.Equal(t, "2h", cfg.LocalBlobOrphanAge, "local_blob_orphan_age")
	want := []string{"https://a.example", "https://b.example"}
	assert.True(t, slices.Equal(cfg.CORSAllowedOrigins, want), "cors_allowed_origins = %q, want %q", cfg.CORSAllowedOrigins, want)
	assert.Equal(t, 3, cfg.CatalogPlane.Retain, "catalog_plane.retain")
	// Equal compares pointees: a non-nil false.
	assert.Equal(t, new(bool), cfg.CatalogPlane.Ship, "catalog_plane.ship")
	// The env address beside the YAML key.
	assert.Equal(t, "https://bao.example:8200", cfg.RegionKey.OpenBao.Address, "regionkey.openbao.address")
	assert.Equal(t, "from-yaml", cfg.RegionKey.OpenBao.Key, "regionkey.openbao.key")
	assert.Equal(t, "transit-env", cfg.RegionKey.OpenBao.Mount, "regionkey.openbao.mount: the env value over the default")
	assert.Equal(t, "/from-env", cfg.DataDir, "data_dir")
	assert.Equal(t, "0.0.0.0:8080", cfg.Addr, "addr = %q, want the default 0.0.0.0:8080", cfg.Addr)
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
			require.NoError(t, os.WriteFile(cfgFile, []byte("addr: \"127.0.0.1:9000\"\n"), 0o600), "write config")
			cfg, err := Load(cfgFile)
			require.NoError(t, err, "Load")
			drop := cfg.CacheWrites != nil && !*cfg.CacheWrites
			require.Equal(t, tc.drop, drop, "drops accepted bodies")
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
			require.NoError(t, err, "ServerConfig")
			require.Equal(t, tc.drop, sc.DropAcceptedBodies, "DropAcceptedBodies")
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
			require.NoError(t, os.WriteFile(cfgFile, []byte("addr: \"127.0.0.1:9000\"\n"), 0o600), "write config")
			cfg, err := Load(cfgFile)
			require.NoError(t, err, "Load")
			require.Equal(t, tc.allow, cfg.Encryption.AllowNone, "encryption.allow_none")
		})
	}
}

func TestServerConfig_AllowNoneEncryption(t *testing.T) {
	for _, allow := range []bool{false, true} {
		cfg := validConfig(t)
		cfg.Encryption.AllowNone = allow
		sc, err := cfg.ServerConfig()
		require.NoError(t, err, "ServerConfig")
		require.Equal(t, allow, sc.AllowNoneEncryption, "AllowNoneEncryption")
	}
}

func TestServerConfig_CompleteMultipartKeepalive(t *testing.T) {
	for name, tc := range map[string]struct {
		set  string
		want time.Duration
	}{
		"unset is the default":  {set: "", want: 5 * time.Second},
		"explicit":              {set: "2s", want: 2 * time.Second},
		"negative turns it off": {set: "-1s", want: 0},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig(t)
			cfg.CompleteMultipartKeepalive = tc.set
			sc, err := cfg.ServerConfig()
			require.NoError(t, err, "ServerConfig")
			require.Equal(t, tc.want, sc.CompleteMultipartKeepalive, "CompleteMultipartKeepalive")
		})
	}
}

func TestLoad_BucketQueueWorkersDefaultToTwo(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte("data_dir: /any-dir\n"), 0o600), "write config")
	cfg, err := Load(cfgFile)
	require.NoError(t, err, "Load")
	require.Equal(t, 2, cfg.TaskWorkers.Bucket, "task_workers.bucket")
}

func TestLoad_BucketQueueWorkersFromEnv(t *testing.T) {
	t.Setenv("INGOT_TASK_WORKERS_BUCKET", "16")
	cfgFile := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte("data_dir: /any-dir\n"), 0o600), "write config")
	cfg, err := Load(cfgFile)
	require.NoError(t, err, "Load")
	require.Equal(t, 16, cfg.TaskWorkers.Bucket, "task_workers.bucket")
}

func TestValidate_RejectsNegativeBucketQueueWorkers(t *testing.T) {
	cfg := validConfig(t)
	cfg.TaskWorkers.Bucket = -1
	want := "task_workers.bucket -1: must not be negative"
	err := cfg.Validate()
	require.Error(t, err, "got %v, want an error containing %q", err, want)
	require.Contains(t, err.Error(), want, "got %v, want an error containing %q", err, want)
}

func TestValidate_AcceptsZeroBucketQueueWorkersAsTheDefault(t *testing.T) {
	cfg := validConfig(t)
	cfg.TaskWorkers.Bucket = 0
	require.NoError(t, cfg.Validate(), "Validate")
}
