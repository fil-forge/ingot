package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/ucantone/did"
	"github.com/filecoin-project/go-fee/aesstream"
	"github.com/spf13/viper"
	"go.uber.org/multierr"

	"github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/internal/cors"
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
	// MaxBlobSize is the blob ceiling for new objects, in bytes (0 -> the
	// default, bucket.DefaultMaxBlobSize: the largest split whose encrypted
	// envelope fits piri's ~254 MiB piece cap). An object larger than this is
	// coarsely split into ≤ max blobs. Validate rejects values whose envelope
	// a default-configured piri would refuse.
	MaxBlobSize int64 `mapstructure:"max_blob_size" yaml:"max_blob_size"`
	// CORSAllowedOrigins lists the browser origins the S3 listener answers
	// CORS for. Each entry is an exact origin ("https://app.example"), a
	// wildcard origin ("https://*.dev.example" — one '*' standing for any
	// run of characters, S3's own matching), or the lone "*" (any origin —
	// avoid outside development). Empty disables CORS (the default).
	//
	// The list is rendered into an S3 CORS configuration document that
	// every bucket reports, which is what drives versitygw's CORS
	// middlewares and preflight handler. Two consequences worth knowing:
	// a matched origin is answered with Access-Control-Allow-Credentials:
	// true (AWS behaviour — harmless here because ingot authenticates the
	// Authorization header and presigned URLs, never cookies), and the
	// service-level routes (ListBuckets, "GET /") are not covered, so
	// cross-origin bucket listing is unsupported.
	CORSAllowedOrigins []string `mapstructure:"cors_allowed_origins" yaml:"cors_allowed_origins"`
	// SealBytes / SealAge / Retain tune every bucket's catalog log (zero ->
	// logstore defaults; catalog_plane overrides them): when a segment seals
	// and how many shipped segments stay on disk. Object bodies are not in
	// the log; local_blob_max_bytes bounds their local storage.
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
	// Required whenever AuthServiceURL is set. Hilt rejects unproven
	// invocations, so an ingot started without them accepts connections and
	// then fails every S3 request; startup refuses instead.
	AuthServiceProofs string `mapstructure:"auth_service_proofs" yaml:"auth_service_proofs"`
	// RevocationServiceURL / RevocationServiceDID address the UCAN revocation
	// service (Swarf): ingot subscribes to its revocation firehose so that
	// Hilt's access-key deletions (published as UCAN revocations) clear the
	// per-key authorization caches. Optional; when unset the firehose consumer
	// is not started and cache entries age out on their own TTLs. Set both or
	// neither.
	RevocationServiceURL string `mapstructure:"revocation_service_url" yaml:"revocation_service_url"`
	RevocationServiceDID string `mapstructure:"revocation_service_did" yaml:"revocation_service_did"`

	// MultipartSessionTTL bounds abandoned multipart uploads (Go duration
	// string, e.g. "168h"): a session whose state has not changed for this
	// long is torn down by a background sweeper and its parts released.
	// Empty → default 7 days; a negative duration disables the sweeper.
	// A part keeps no local copy once it parks on its provider, so Complete
	// relies on the provider keeping the parked allocation: a session
	// completed after the provider has expired it fails, and the client must
	// upload those parts again. The TTL counts from the session's last state
	// change, and a Complete that fails returns the session to open, which
	// restarts it; so it bounds a parked part's age only while no Complete
	// fails. Keep it well under how long the provider keeps parked
	// allocations.
	MultipartSessionTTL string `mapstructure:"multipart_session_ttl" yaml:"multipart_session_ttl"`

	// ReleaseGrace delays each blob release (crypto-shred + location delete +
	// network remove + local-copy removal) this long past the drop of its
	// last reference claim (Go duration string), so in-flight readers holding
	// the prior catalog root finish first. It bounds how long such a reader
	// may take: a GET, or a copy reading the object as its source, still
	// streaming a deleted or overwritten object once the grace has passed
	// fails when it reaches a blob it has not yet opened.
	// Empty → default 60s; a negative duration makes releases due
	// immediately.
	ReleaseGrace string `mapstructure:"release_grace" yaml:"release_grace"`

	// LocalBlobMaxBytes is the byte budget for local blob storage: the spool
	// (<data_dir>/spool: writes in progress, counted as their bytes land, and
	// bodies waiting for upload) plus the cache (<data_dir>/cache: copies of
	// bodies the provider holds). It does not cover the catalog log. A
	// sweeper checks it every 30 seconds and evicts blobs the provider
	// holds, oldest first, down to 90% of the budget; reads of an evicted blob go to the
	// provider. Usage can exceed the budget by ingest rate × 30 seconds
	// between sweeps, and by files that must stay (the spool's, and orphans
	// younger than LocalBlobOrphanAge). 0 → no budget (the default); negative
	// is an error.
	LocalBlobMaxBytes int64 `mapstructure:"local_blob_max_bytes" yaml:"local_blob_max_bytes"`
	// CacheMinResidency is the read-after-write window (Go duration string):
	// the sweeper leaves alone a cached blob whose upload state changed less
	// than this long ago (for a committed blob, its commit time) unless usage
	// stays over budget without it. Costs ingest rate × residency in disk.
	// Empty → default 10m; "0s" turns it off; negative is an error.
	CacheMinResidency string `mapstructure:"cache_min_residency" yaml:"cache_min_residency"`
	// CacheReadRetention is the read-cache window (Go duration string): the
	// sweeper leaves alone a blob served from the cache within this long,
	// unless usage stays over budget without it. Reads are remembered in
	// memory, for a bounded number of blobs, and forgotten on restart. Empty
	// → default 1h; "0s" turns it off; negative is an error.
	CacheReadRetention string `mapstructure:"cache_read_retention" yaml:"cache_read_retention"`
	// LocalBlobOrphanAge is the age (file modification time) at which the
	// sweeper deletes a .tmp-* file in the spool or the cache, or a spool blob
	// file with no upload intent (Go duration string), hourly, whether or not a
	// budget is set. It must exceed the longest time one request body takes to
	// stream: the server sets no body read timeout, and a body still streaming
	// past it can have its first blobs deleted, failing the request. Empty →
	// default 24h; under 1h is an error.
	LocalBlobOrphanAge string `mapstructure:"local_blob_orphan_age" yaml:"local_blob_orphan_age"`

	// CatalogPlane overrides the catalog logstore pipeline knobs. Any field
	// left zero/unset falls back to the top-level SealBytes / SealAge / Retain
	// (and Ship defaults to true) — e.g. to configure the catalog never to ship.
	CatalogPlane PlaneSettings `mapstructure:"catalog_plane" yaml:"catalog_plane"`

	// RegionKey selects and configures the region CEK wrap provider
	// (regionkey.Provider): the component that wraps each object's
	// content-encryption key under the region KEK for the read path (the
	// FilOne encryption design's region wrap). Required: the provider choice
	// (openbao in production, inprocess for tests/dev) is configuration, but
	// bucket encryption is not optional.
	RegionKey RegionKeyConfig `mapstructure:"regionkey" yaml:"regionkey"`

	// TenantKey configures resolution of the tenant wrap key (tenantkey): the
	// X25519 key, published in each tenant's did:plc document, that every
	// stored object is encrypted to as the FEE tenant recipient. Required:
	// writes fail without a recipient.
	TenantKey TenantKeyConfig `mapstructure:"tenantkey" yaml:"tenantkey"`

	// LogLevel is the zap level (debug|info|warn|error).
	LogLevel string `mapstructure:"log_level" yaml:"log_level"`
	// PostgresDSN is the registry/meta database.
	PostgresDSN string `mapstructure:"postgres_dsn" yaml:"postgres_dsn"`
	// Identity holds the agent (service) identity key.
	Identity IdentityConfig `mapstructure:"identity" yaml:"identity"`
}

// PlaneSettings are the catalog plane's logstore overrides.
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
	var mpTTL time.Duration
	if c.MultipartSessionTTL != "" {
		mpTTL, err = time.ParseDuration(c.MultipartSessionTTL)
		if err != nil {
			return ServerConfig{}, fmt.Errorf("ingot: parse multipart_session_ttl %q: %w", c.MultipartSessionTTL, err)
		}
	}
	releaseGrace := 60 * time.Second
	if c.ReleaseGrace != "" {
		releaseGrace, err = time.ParseDuration(c.ReleaseGrace)
		if err != nil {
			return ServerConfig{}, fmt.Errorf("ingot: parse release_grace %q: %w", c.ReleaseGrace, err)
		}
		if releaseGrace < 0 {
			releaseGrace = 0
		}
	}
	cacheMinResidency, err := parseDurationKnob("cache_min_residency", c.CacheMinResidency, 10*time.Minute)
	if err != nil {
		return ServerConfig{}, err
	}
	cacheReadRetention, err := parseDurationKnob("cache_read_retention", c.CacheReadRetention, time.Hour)
	if err != nil {
		return ServerConfig{}, err
	}
	localBlobOrphanAge, err := parseDurationKnob("local_blob_orphan_age", c.LocalBlobOrphanAge, 24*time.Hour)
	if err != nil {
		return ServerConfig{}, err
	}
	if localBlobOrphanAge < time.Hour {
		return ServerConfig{}, fmt.Errorf("ingot: local_blob_orphan_age %q: must be at least 1h, longer than any request body takes to stream", c.LocalBlobOrphanAge)
	}
	if c.LocalBlobMaxBytes < 0 {
		return ServerConfig{}, fmt.Errorf("ingot: local_blob_max_bytes %d: must not be negative (0 means no budget)", c.LocalBlobMaxBytes)
	}
	// Render the CORS configuration here — the single place it is built —
	// so a typo fails at startup (via Validate) rather than from New.
	corsCfg, err := cors.Build(c.CORSAllowedOrigins)
	if err != nil {
		return ServerConfig{}, fmt.Errorf("ingot: cors_allowed_origins: %w", err)
	}
	return ServerConfig{
		Addr:        c.Addr,
		DataDir:     c.DataDir,
		Region:      c.Region,
		MaxBlobSize: c.MaxBlobSize,

		CORSConfig: corsCfg,

		// The catalog_plane override wins, else the top-level value, else the logstore
		// default. Ship defaults to true unless the catalog block sets
		// `ship: false`.
		SealBytesCatalog: firstNonZero64(c.CatalogPlane.SealBytes, c.SealBytes),
		SealAgeCatalog:   catAge,
		ShipCatalog:      shipDefault(c.CatalogPlane.Ship),
		RetainCatalog:    firstNonZeroInt(c.CatalogPlane.Retain, c.Retain),

		MultipartSessionTTL: mpTTL,
		ReleaseGrace:        releaseGrace,

		LocalBlobMaxBytes:  c.LocalBlobMaxBytes,
		CacheMinResidency:  cacheMinResidency,
		CacheReadRetention: cacheReadRetention,
		LocalBlobOrphanAge: localBlobOrphanAge,
	}, nil
}

// parseDurationKnob parses a duration knob: empty takes def,
// negative is an error.
func parseDurationKnob(name, value string, def time.Duration) (time.Duration, error) {
	if value == "" {
		return def, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("ingot: parse %s %q: %w", name, value, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("ingot: %s %q: must not be negative", name, value)
	}
	return d, nil
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

// IdentityConfig describes the agent (service) identity: the signer that
// issues every outbound invocation (to hilt, sprue, and piri).
type IdentityConfig struct {
	// KeyFile is the path to the agent's PEM-encoded ed25519 private key.
	// Required.
	KeyFile string `mapstructure:"key_file" yaml:"key_file"`
	// ServiceID is an optional did:web identity to wrap the key with (e.g.
	// "did:web:ingot.example.com"). When set, ingot issues invocations as that
	// DID; peers resolve it through the DID document the S3 listener serves at
	// /.well-known/did.json. When empty, the key's did:key is used.
	ServiceID string `mapstructure:"service_id" yaml:"service_id"`
}

// TenantKeyConfig locates the did:plc directory tenant DID documents are
// resolved from.
type TenantKeyConfig struct {
	// PLCDirectoryURL is the did:plc directory endpoint, e.g.
	// "https://plc.directory". Required.
	PLCDirectoryURL string `mapstructure:"plc_directory_url" yaml:"plc_directory_url"`
	// CacheTTL is how long a resolved tenant document is reused before the
	// directory is consulted again (a Go duration). It bounds both the
	// directory traffic on the write path and how long a wrap-key rotation
	// goes unseen. Empty means "10m".
	CacheTTL string `mapstructure:"cache_ttl" yaml:"cache_ttl"`
}

// DefaultTenantKeyCacheTTL is the tenant document cache TTL when
// tenantkey.cache_ttl is unset.
const DefaultTenantKeyCacheTTL = 10 * time.Minute

// CacheTTLDuration parses CacheTTL, returning DefaultTenantKeyCacheTTL when
// it is empty.
func (c TenantKeyConfig) CacheTTLDuration() (time.Duration, error) {
	if c.CacheTTL == "" {
		return DefaultTenantKeyCacheTTL, nil
	}
	d, err := time.ParseDuration(c.CacheTTL)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf("must be positive, got %s", d)
	}
	return d, nil
}

// RegionKeyConfig selects the region CEK wrap provider and carries each
// implementation's settings.
type RegionKeyConfig struct {
	// Provider names the implementation: "openbao" (production — the wrap
	// runs inside the region's OpenBao transit engine and the KEK never
	// enters ingot's process) or "inprocess" (AES-256-GCM in ingot's own
	// process; tests and development only). Required.
	Provider string `mapstructure:"provider" yaml:"provider"`
	// OpenBao configures the "openbao" provider.
	OpenBao OpenBaoConfig `mapstructure:"openbao" yaml:"openbao"`
	// InProcess configures the "inprocess" provider.
	InProcess InProcessConfig `mapstructure:"inprocess" yaml:"inprocess"`
}

// OpenBaoConfig is the "openbao" region-key provider's connection and key
// settings.
type OpenBaoConfig struct {
	// Address of the OpenBao server, e.g. "https://bao.region.internal:8200"
	// or a unix socket "unix:///run/openbao/api.sock". Empty falls back to
	// the client's environment (BAO_ADDR, or upstream VAULT_ADDR).
	Address string `mapstructure:"address" yaml:"address"`
	// Token authenticates ingot to OpenBao; it needs encrypt/decrypt/rewrap
	// on the transit key and nothing else. Empty falls back to the client's
	// environment (BAO_TOKEN, or upstream VAULT_TOKEN).
	Token string `mapstructure:"token" yaml:"token"`
	// Mount is the transit engine's mount path. Empty means "transit".
	Mount string `mapstructure:"mount" yaml:"mount"`
	// Key is the transit key name holding the region KEK (provisioned with
	// type aes256-gcm96 and derived=true). Required when provider=openbao.
	Key string `mapstructure:"key" yaml:"key"`
}

// InProcessConfig is the "inprocess" region-key provider's settings.
type InProcessConfig struct {
	// KEK is the region key, base64-encoded 32 bytes. Empty generates a
	// random key at startup — development only: wraps made under a generated
	// key are unreadable after a restart.
	KEK string `mapstructure:"kek" yaml:"kek"`
	// Version tags wraps with the KEK's version. Empty means "v1".
	Version string `mapstructure:"version" yaml:"version"`
}

// Load reads daemon config from configFile (or the default search path)
// with env override (INGOT_* / nested keys via "_").
//
// Viper's AutomaticEnv alone applies an environment variable only to a key
// viper already knows from the config file or a default, so a key absent
// from the YAML would ignore its INGOT_* variable. ExperimentalBindStruct
// makes Unmarshal also look up every key of Config, so each field can be
// set from the environment.
func Load(configFile string) (*Config, error) {
	v := viper.NewWithOptions(viper.ExperimentalBindStruct())
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
	v.SetDefault("addr", "0.0.0.0:8080")
	v.SetDefault("regionkey.openbao.mount", "transit")
	v.SetDefault("regionkey.inprocess.version", "v1")
}

// Validate checks the config for the selected mode, aggregating every
// problem into one error.
func (c *Config) Validate() error {
	var errs error

	if c.Addr == "" {
		errs = multierr.Append(errs, errors.New("addr is required"))
	}
	if c.DataDir == "" {
		errs = multierr.Append(errs, errors.New("data_dir is required"))
	}
	if c.MaxBlobSize > 0 {
		// What ships to piri is the FEE envelope, not the plaintext: per-chunk
		// GCM tags plus the COSE header (budgeted generously). Fail at startup
		// rather than at the first PUT's BlobSizeLimitExceeded.
		const envelopeHeaderBudget = 1024
		if enc := aesstream.EncryptedSize(c.MaxBlobSize, aesstream.DefaultChunkSize) + envelopeHeaderBudget; enc > blobcmds.MaxBlobSize {
			errs = multierr.Append(errs, fmt.Errorf("max_blob_size %d: its encrypted envelope (~%d bytes) exceeds the %d-byte network blob ceiling (the piece cap of a default-configured piri) — use at most the default %d, or raise every piri in the region above piri's default piece size", c.MaxBlobSize, enc, int64(blobcmds.MaxBlobSize), bucket.DefaultMaxBlobSize))
		}
	}
	if _, err := c.ServerConfig(); err != nil {
		errs = multierr.Append(errs, err)
	}

	if c.PostgresDSN == "" {
		errs = multierr.Append(errs, errors.New("postgres_dsn is required"))
	}
	if c.Identity.KeyFile == "" {
		errs = multierr.Append(errs, errors.New("identity.key_file (agent PEM) is required"))
	} else if _, err := os.Stat(c.Identity.KeyFile); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("identity.key_file %q: %w", c.Identity.KeyFile, err))
	}
	if c.Identity.ServiceID != "" {
		if _, err := did.Parse(c.Identity.ServiceID); err != nil {
			errs = multierr.Append(errs, fmt.Errorf("identity.service_id %q: %w", c.Identity.ServiceID, err))
		}
	}
	if c.UploadServiceURL == "" || c.UploadServiceDID == "" {
		errs = multierr.Append(errs, errors.New("upload_service_url and upload_service_did are required"))
	}
	if c.AuthServiceURL == "" || c.AuthServiceDID == "" {
		errs = multierr.Append(errs, errors.New("auth_service_url and auth_service_did are required"))
	}
	if c.AuthServiceURL != "" && c.AuthServiceProofs == "" {
		errs = multierr.Append(errs, errors.New("auth_service_proofs is required when auth_service_url is set: without the Hilt delegation chains every S3 request fails authorization"))
	}
	if c.AuthServiceProofs != "" {
		// Load eagerly so a bad path, a bad encoding, or a container carrying
		// no delegations fails at startup rather than on the first authorized
		// request.
		ct, err := LoadProofsContainer(c.AuthServiceProofs)
		switch {
		case err != nil:
			errs = multierr.Append(errs, fmt.Errorf("auth_service_proofs: %w", err))
		case len(ct.Delegations()) == 0:
			errs = multierr.Append(errs, errors.New("auth_service_proofs: the container holds no delegations"))
		}
	}
	if (c.RevocationServiceURL == "") != (c.RevocationServiceDID == "") {
		errs = multierr.Append(errs, errors.New("revocation_service_url and revocation_service_did must be set together"))
	}

	switch c.RegionKey.Provider {
	case "":
		errs = multierr.Append(errs, errors.New("regionkey.provider is required (openbao or inprocess)"))
	case "openbao":
		if c.RegionKey.OpenBao.Key == "" {
			errs = multierr.Append(errs, errors.New("regionkey.openbao.key (transit key name) is required when regionkey.provider is openbao"))
		}
	case "inprocess":
		if c.RegionKey.InProcess.KEK != "" {
			kek, err := base64.StdEncoding.DecodeString(c.RegionKey.InProcess.KEK)
			if err != nil {
				errs = multierr.Append(errs, fmt.Errorf("regionkey.inprocess.kek: %w", err))
			} else if len(kek) != 32 {
				errs = multierr.Append(errs, fmt.Errorf("regionkey.inprocess.kek must decode to 32 bytes (AES-256), got %d", len(kek)))
			}
		}
	default:
		errs = multierr.Append(errs, fmt.Errorf("regionkey.provider %q is not one of openbao, inprocess", c.RegionKey.Provider))
	}

	if c.TenantKey.PLCDirectoryURL == "" {
		errs = multierr.Append(errs, errors.New("tenantkey.plc_directory_url is required"))
	} else if u, err := url.Parse(c.TenantKey.PLCDirectoryURL); err != nil || u.Scheme == "" || u.Host == "" {
		errs = multierr.Append(errs, fmt.Errorf("tenantkey.plc_directory_url %q is not an absolute URL", c.TenantKey.PLCDirectoryURL))
	}
	if _, err := c.TenantKey.CacheTTLDuration(); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("tenantkey.cache_ttl: %w", err))
	}

	if errs != nil {
		return fmt.Errorf("invalid config: %w", errs)
	}
	return nil
}
