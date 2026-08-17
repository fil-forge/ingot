package config

import (
	"time"

	"github.com/fil-forge/versitygw/auth"
)

// ServerConfig captures the user-facing knobs of an ingot S3 listener.
// New() applies defaults for any zero-valued knobs. SealAge is in
// time.Duration form because callers parse the string config field
// once before constructing the server.
type ServerConfig struct {
	// Addr is the host:port to bind the S3 listener to. Required.
	Addr string

	// DataDir is where the log writes its segments dir; the caller
	// is responsible for creating this directory before calling New.
	// Required.
	DataDir string

	// Region is the AWS region advertised over sigv4. Defaults to
	// "us-east-1".
	Region string

	// RootAccess / RootSecret configure the single-account IAM root
	// user for the embedded S3 listener. Both required.
	RootAccess string
	RootSecret string

	// MaxBlobSize is the blob ceiling for new objects, in bytes.
	// 0 → bucket.DefaultMaxBlobSize.
	MaxBlobSize int64

	// Catalog plane seal threshold, ship gate, and retention. Zero
	// SealBytes/SealAge pick logstore defaults (64 MiB / 5 s); zero Retain → 6
	// (ignored for a non-shipping plane, which is retained indefinitely).
	SealBytesCatalog int64
	SealAgeCatalog   time.Duration
	ShipCatalog      bool
	RetainCatalog    int

	// MaxConnections / MaxRequests configure versitygw's hard
	// concurrency limit. Zero is unsafe (yields 503 SlowDown on every
	// request), so New substitutes a sensible default.
	MaxConnections int
	MaxRequests    int

	// MultipartSessionTTL bounds abandoned multipart uploads: open sessions
	// older than this are aborted by a background sweeper (dropping their
	// spooled parts), and completed-session rows retained for Complete
	// idempotency are reaped past the same age. Zero → default 7 days;
	// negative → sweeper disabled.
	MultipartSessionTTL time.Duration

	// CORSConfig is the S3 CORS configuration the backend reports for
	// every bucket, rendered from Config.CORSAllowedOrigins by
	// internal/cors. Nil disables CORS entirely (the default).
	CORSConfig *auth.CORSConfiguration
}
