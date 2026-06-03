package ingot

import (
	"fmt"
	"time"
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
	// RootAccess / RootSecret configure the single-account IAM root user.
	RootAccess string `mapstructure:"root_access" yaml:"root_access"`
	RootSecret string `mapstructure:"root_secret" yaml:"root_secret"`
	// ChunkSize is the body chunk size for new objects, in bytes (0 -> default).
	ChunkSize int64 `mapstructure:"chunk_size" yaml:"chunk_size"`
	// SealBytes / SealAge / Retain tune the logstore (zero -> logstore defaults).
	SealBytes int64  `mapstructure:"seal_bytes" yaml:"seal_bytes"`
	SealAge   string `mapstructure:"seal_age" yaml:"seal_age"`
	Retain    int    `mapstructure:"retain" yaml:"retain"`
	// IndexerEndpoint / IndexerDID address the Forge indexing-service.
	IndexerEndpoint string `mapstructure:"indexer_endpoint" yaml:"indexer_endpoint"`
	IndexerDID      string `mapstructure:"indexer_did" yaml:"indexer_did"`
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

	// DataPlane / CatalogPlane override the per-plane logstore pipeline
	// knobs. Any field left zero/unset falls back to the top-level
	// SealBytes / SealAge / Retain (and Ship defaults to true). This is
	// what lets, e.g., the catalog plane be configured never to ship.
	DataPlane    PlaneSettings `mapstructure:"data_plane" yaml:"data_plane"`
	CatalogPlane PlaneSettings `mapstructure:"catalog_plane" yaml:"catalog_plane"`
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
func (c Config) readCacheBytes() int64 {
	switch {
	case c.ReadCacheBytes == 0:
		return defaultReadCacheBytes
	case c.ReadCacheBytes < 0:
		return 0
	default:
		return c.ReadCacheBytes
	}
}

// ServerConfig derives the low-level ServerConfig from c, parsing SealAge once.
// It is the single mapping site between the host-facing Config and the
// constructor-facing ServerConfig (used by the fx module and available to
// non-fx callers that want the same defaults).
func (c Config) ServerConfig() (ServerConfig, error) {
	dataAge, err := planeSealAge(c.DataPlane.SealAge, c.SealAge)
	if err != nil {
		return ServerConfig{}, err
	}
	catAge, err := planeSealAge(c.CatalogPlane.SealAge, c.SealAge)
	if err != nil {
		return ServerConfig{}, err
	}
	return ServerConfig{
		Addr:       c.Addr,
		DataDir:    c.DataDir,
		Region:     c.Region,
		RootAccess: c.RootAccess,
		RootSecret: c.RootSecret,
		ChunkSize:  c.ChunkSize,

		// Per-plane: a per-plane override wins, else the top-level value,
		// else the logstore default. Ship defaults to true unless a plane
		// block sets `ship: false`.
		SealBytesData: firstNonZero64(c.DataPlane.SealBytes, c.SealBytes),
		SealAgeData:   dataAge,
		ShipData:      shipDefault(c.DataPlane.Ship),
		RetainData:    firstNonZeroInt(c.DataPlane.Retain, c.Retain),

		SealBytesCatalog: firstNonZero64(c.CatalogPlane.SealBytes, c.SealBytes),
		SealAgeCatalog:   catAge,
		ShipCatalog:      shipDefault(c.CatalogPlane.Ship),
		RetainCatalog:    firstNonZeroInt(c.CatalogPlane.Retain, c.Retain),
	}, nil
}

// planeSealAge resolves a plane's SealAge: the per-plane value if set,
// else the top-level value, else 0 (logstore applies its own default).
func planeSealAge(planeStr, topStr string) (time.Duration, error) {
	s := emptyDefault(planeStr, topStr)
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
