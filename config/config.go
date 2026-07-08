package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config is ingot's own configuration.
type Config struct {
	// Enabled toggles the fx module; when false Module is an empty option so
	// the module is safe to always include in a host's app graph.
	Enabled bool `mapstructure:"enabled" yaml:"enabled"`
	// Addr is the host:port to bind the S3 listener to.
	Addr string `mapstructure:"addr" yaml:"addr"`
	// DataDir is where the log writes its segments dir and the space key lives.
	DataDir string `mapstructure:"data_dir" yaml:"data_dir"`
	// Region is the AWS region advertised over sigv4 (default "us-east-1").
	Region string `mapstructure:"region" yaml:"region"`
	// MaxBlobSize is the blob ceiling for new objects, in bytes (0 -> default
	// 256 MiB). An object larger than this is coarsely split into ≤ max blobs.
	MaxBlobSize int64 `mapstructure:"max_blob_size" yaml:"max_blob_size"`
	// SealBytes / SealAge / Retain tune the logstore (zero -> logstore defaults).
	SealBytes int64  `mapstructure:"seal_bytes" yaml:"seal_bytes"`
	SealAge   string `mapstructure:"seal_age" yaml:"seal_age"`
	Retain    int    `mapstructure:"retain" yaml:"retain"`
	// ReadCacheBytes bounds the in-memory block cache fronting the
	// network-backed read tier. 0 -> default (256 MiB); negative -> disabled.
	ReadCacheBytes int64 `mapstructure:"read_cache_bytes" yaml:"read_cache_bytes"`
	// UploadServiceURL / UploadServiceDID address the Forge upload service
	// (sprue): the control-plane peer ingot invokes /blob/add, /ucan/conclude,
	// and /index/add against as a guppy-style edge client.
	UploadServiceURL string `mapstructure:"upload_service_url" yaml:"upload_service_url"`
	UploadServiceDID string `mapstructure:"upload_service_did" yaml:"upload_service_did"`
	// UploadReceiptsURL is the receipts-polling base URL. Optional; defaults to
	// UploadServiceURL + "/receipt/".
	UploadReceiptsURL string `mapstructure:"upload_receipts_url" yaml:"upload_receipts_url"`
	// TokenStoreDir is where login-derived delegations persist (tokens.cbor).
	// Optional; defaults to DataDir.
	TokenStoreDir string `mapstructure:"token_store_dir" yaml:"token_store_dir"`
	// AuthServiceURL / AuthServiceDID address the S3 authorization service:
	// the UCAN RPC service that ingot invokes /s3/request/authorize and
	// /s3/bucket/* against (see the Forge S3 tenant-management RFC). AKA Hilt.
	AuthServiceURL string `mapstructure:"auth_service_url" yaml:"auth_service_url"`
	AuthServiceDID string `mapstructure:"auth_service_did" yaml:"auth_service_did"`
	// AuthServiceProofs supplies the Hilt→Ingot delegation chains the hilt client
	// attaches to its invocations: either a path to a file containing a UCAN
	// container of proofs, or the string-encoded UCAN container itself.
	// Optional; when empty the client sends invocations with no proofs (Hilt
	// may authorize registered provider DIDs directly).
	AuthServiceProofs string `mapstructure:"auth_service_proofs" yaml:"auth_service_proofs"`

	// CatalogPlane overrides the catalog logstore pipeline knobs. Any field
	// left zero/unset falls back to the top-level SealBytes / SealAge / Retain
	// (and Ship defaults to true) — e.g. to configure the catalog never to ship.
	CatalogPlane PlaneSettings `mapstructure:"catalog_plane" yaml:"catalog_plane"`

	// LogLevel is the zap level (debug|info|warn|error).
	LogLevel string `mapstructure:"log_level" yaml:"log_level"`
	// PostgresDSN is the registry/meta database.
	PostgresDSN string `mapstructure:"postgres_dsn" yaml:"postgres_dsn"`
	// Identity holds the agent (service) identity key.
	Identity IdentityConfig `mapstructure:"identity" yaml:"identity"`
}

// PlaneSettings are the per-plane logstore overrides (data or catalog).
// Zero-valued fields fall back to the top-level Config defaults; Ship is
// a pointer so an explicit `ship: false` is distinguishable from unset
// (which defaults to shipping).
type PlaneSettings struct {
	SealBytes int64  `mapstructure:"seal_bytes" yaml:"seal_bytes"`
	SealAge   string `mapstructure:"seal_age" yaml:"seal_age"`
	Ship      *bool  `mapstructure:"ship" yaml:"ship"`
	Retain    int    `mapstructure:"retain" yaml:"retain"`
}

// defaultReadCacheBytes is the block cache budget when ReadCacheBytes is 0.
const defaultReadCacheBytes int64 = 256 << 20

// readCacheBytes resolves the configured block cache budget: 0 picks the
// default, a negative value disables caching.
func ResolveReadCacheBytes(n int64) int64 {
	switch {
	case n == 0:
		return defaultReadCacheBytes
	case n < 0:
		return 0
	default:
		return n
	}
}

// ServerConfig derives the low-level ServerConfig from c, parsing SealAge once.
// It is the single mapping site between the host-facing Config and the
// constructor-facing ServerConfig (used by the fx module and available to
// non-fx callers that want the same defaults).
func (c Config) ServerConfig() (ServerConfig, error) {
	catAge, err := planeSealAge(c.CatalogPlane.SealAge, c.SealAge)
	if err != nil {
		return ServerConfig{}, err
	}
	return ServerConfig{
		Addr:        c.Addr,
		DataDir:     c.DataDir,
		Region:      c.Region,
		MaxBlobSize: c.MaxBlobSize,

		// A per-plane override wins, else the top-level value, else the logstore
		// default. Ship defaults to true unless the catalog block sets
		// `ship: false`.
		SealBytesCatalog: firstNonZero64(c.CatalogPlane.SealBytes, c.SealBytes),
		SealAgeCatalog:   catAge,
		ShipCatalog:      shipDefault(c.CatalogPlane.Ship),
		RetainCatalog:    firstNonZeroInt(c.CatalogPlane.Retain, c.Retain),
	}, nil
}

// planeSealAge resolves a plane's SealAge: the per-plane value if set,
// else the top-level value, else 0 (logstore applies its own default).
func planeSealAge(planeStr, topStr string) (time.Duration, error) {
	s := EmptyDefault(planeStr, topStr)
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("ingot: parse seal_age %q: %w", s, err)
	}
	return d, nil
}

// shipDefault treats an unset (nil) per-plane Ship flag as true.
func shipDefault(p *bool) bool {
	if p == nil {
		return true
	}
	return *p
}

func firstNonZero64(a, b int64) int64 {
	if a != 0 {
		return a
	}
	return b
}

func firstNonZeroInt(a, b int) int {
	if a != 0 {
		return a
	}
	return b
}

// EmptyDefault returns def when s is the empty string.
func EmptyDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// IdentityConfig points at the agent's PEM-encoded ed25519 key — the
// signer that issues invocations to sprue.
type IdentityConfig struct {
	KeyFile string `mapstructure:"key_file" yaml:"key_file"`
}

// Load reads daemon config from configFile (or the default search path)
// with env override (INGOT_* / nested keys via "_").
func Load(configFile string) (*Config, error) {
	v := viper.New()
	setDefaults(v)
	v.SetEnvPrefix("INGOT")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("reading config %s: %w", configFile, err)
		}
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("/etc/ingot")
		if err := v.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return nil, fmt.Errorf("reading config: %w", err)
			}
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}
	if cfg.TokenStoreDir == "" {
		cfg.TokenStoreDir = cfg.DataDir
	}
	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("log_level", "info")
	v.SetDefault("addr", "0.0.0.0:9000")
	v.SetDefault("region", "us-east-1")
	v.SetDefault("data_dir", "/data")
}

// Validate checks the config for the selected mode, aggregating every
// problem into one error.
func (c *Config) Validate() error {
	var errs []string

	if c.Addr == "" {
		errs = append(errs, "addr is required")
	}
	if c.DataDir == "" {
		errs = append(errs, "data_dir is required")
	}
	if _, err := c.ServerConfig(); err != nil {
		errs = append(errs, err.Error())
	}

	if c.PostgresDSN == "" {
		errs = append(errs, "postgres_dsn is required")
	}
	if c.Identity.KeyFile == "" {
		errs = append(errs, "identity.key_file (agent PEM) is required")
	} else if _, err := os.Stat(c.Identity.KeyFile); err != nil {
		errs = append(errs, fmt.Sprintf("identity.key_file %q: %v", c.Identity.KeyFile, err))
	}
	if c.UploadServiceURL == "" || c.UploadServiceDID == "" {
		errs = append(errs, "upload_service_url and upload_service_did are required")
	}
	if c.AuthServiceURL == "" || c.AuthServiceDID == "" {
		errs = append(errs, "auth_service_url and auth_service_did are required")
	}
	if c.AuthServiceProofs != "" {
		// Load eagerly so a bad path or encoding fails at startup rather than
		// on the first authorized request.
		if _, err := LoadProofsContainer(c.AuthServiceProofs); err != nil {
			errs = append(errs, fmt.Sprintf("auth_service_proofs: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid config:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}
