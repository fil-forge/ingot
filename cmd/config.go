package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"

	"github.com/fil-forge/ingot"
)

// Run modes.
const (
	ModeStandalone = "standalone"
	ModeForge      = "forge"
)

// DaemonConfig is the ingot daemon's configuration. It embeds the
// library-facing ingot.Config (squashed, so every existing yaml/env key
// is reused verbatim) and adds the daemon-only fields.
type DaemonConfig struct {
	ingot.Config `mapstructure:",squash" yaml:",inline"`

	// Mode selects the run mode: "standalone" (no Forge; in-memory
	// registry; both planes retained locally) or "forge" (Postgres +
	// sprue + network reads).
	Mode string `mapstructure:"mode" yaml:"mode"`
	// LogLevel is the zap level (debug|info|warn|error).
	LogLevel string `mapstructure:"log_level" yaml:"log_level"`
	// PostgresDSN is the registry/meta database (forge mode only).
	PostgresDSN string `mapstructure:"postgres_dsn" yaml:"postgres_dsn"`
	// Identity holds the agent (service) identity key (forge mode only).
	Identity IdentityConfig `mapstructure:"identity" yaml:"identity"`
}

// IdentityConfig points at the agent's PEM-encoded ed25519 key — the
// signer that issues invocations to sprue.
type IdentityConfig struct {
	KeyFile string `mapstructure:"key_file" yaml:"key_file"`
}

// Load reads daemon config from configFile (or the default search path)
// with env override (INGOT_* / nested keys via "_").
func Load(configFile string) (*DaemonConfig, error) {
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

	var cfg DaemonConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}
	cfg.Mode = strings.ToLower(strings.TrimSpace(cfg.Mode))
	if cfg.TokenStoreDir == "" {
		cfg.TokenStoreDir = cfg.DataDir
	}
	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("mode", ModeStandalone)
	v.SetDefault("log_level", "info")
	v.SetDefault("addr", "0.0.0.0:9000")
	v.SetDefault("region", "us-east-1")
	v.SetDefault("data_dir", "/data")
}

// Validate checks the config for the selected mode, aggregating every
// problem into one error.
func (c *DaemonConfig) Validate() error {
	var errs []string

	switch c.Mode {
	case ModeStandalone, ModeForge:
	default:
		errs = append(errs, fmt.Sprintf("mode must be %q or %q, got %q", ModeStandalone, ModeForge, c.Mode))
	}
	if c.Addr == "" {
		errs = append(errs, "addr is required")
	}
	if c.DataDir == "" {
		errs = append(errs, "data_dir is required")
	}
	if c.RootAccess == "" || c.RootSecret == "" {
		errs = append(errs, "root_access and root_secret are required")
	}
	if _, err := c.Config.ServerConfig(); err != nil {
		errs = append(errs, err.Error())
	}

	if c.Mode == ModeForge {
		if c.PostgresDSN == "" {
			errs = append(errs, "postgres_dsn is required in forge mode")
		}
		if c.Identity.KeyFile == "" {
			errs = append(errs, "identity.key_file (agent PEM) is required in forge mode")
		} else if _, err := os.Stat(c.Identity.KeyFile); err != nil {
			errs = append(errs, fmt.Sprintf("identity.key_file %q: %v", c.Identity.KeyFile, err))
		}
		if c.IndexerEndpoint == "" || c.IndexerDID == "" {
			errs = append(errs, "indexer_endpoint and indexer_did are required in forge mode")
		}
		if c.UploadServiceURL == "" || c.UploadServiceDID == "" {
			errs = append(errs, "upload_service_url and upload_service_did are required in forge mode")
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid config:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}
