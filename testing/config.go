package testing

import (
	"github.com/fil-forge/versitygw/tests/integration"
)

// Config carries the connection details for a running ingot S3 listener.
// Callers own the server lifecycle; this package is only client-side glue.
type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string

	// Region must match the listener's configured region. Empty
	// defaults to "us-east-1".
	Region string

	// HostStyle uses host-style bucket addressing (bucket.host)
	// instead of path-style (host/bucket).
	HostStyle bool

	// VersioningEnabled tells the upstream suite the bucket-versioning
	// feature is on; case functions branch on this flag.
	VersioningEnabled bool

	// SkipTLSVerify accepts self-signed certs.
	SkipTLSVerify bool
}

// NewS3Conf builds the upstream versitygw *integration.S3Conf for the
// listener described by c. Individual upstream cases have signature
// func(*integration.S3Conf) error, so this is the bridge for invoking them
// directly — see ingot's itest package for the curated conformance
// partition that does exactly that.
func NewS3Conf(c Config) *integration.S3Conf {
	region := c.Region
	if region == "" {
		region = "us-east-1"
	}
	opts := []integration.Option{
		integration.WithEndpoint(c.Endpoint),
		integration.WithAccess(c.AccessKey),
		integration.WithSecret(c.SecretKey),
		integration.WithRegion(region),
		integration.WithTLSStatus(c.SkipTLSVerify),
	}
	if c.HostStyle {
		opts = append(opts, integration.WithHostStyle())
	}
	if c.VersioningEnabled {
		opts = append(opts, integration.WithVersioningEnabled())
	}
	return integration.NewS3Conf(opts...)
}
