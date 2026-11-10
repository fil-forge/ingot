package preload

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ClientConfig says how to reach the gateway.
type ClientConfig struct {
	// Endpoint is the gateway's URL. Empty takes the profile's
	// endpoint_url.
	Endpoint string
	// Region is the region requests are signed for, which must be the
	// gateway's. Empty takes the profile's or the environment's.
	Region string
	// Profile names the shared config profile holding the credentials;
	// empty uses the SDK's default chain (AWS_ACCESS_KEY_ID and friends,
	// then the default profile).
	Profile string
	// Conns is the number of connections to keep open: at least the
	// number of requests in flight.
	Conns int
}

// NewClient returns an S3 client for the gateway: path-style addressing,
// credentials from the SDK's usual sources, and no retries of its own, so
// that the load counts and paces every retry.
func NewClient(ctx context.Context, c ClientConfig) (*s3.Client, error) {
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.MaxIdleConns = max(c.Conns, tr.MaxIdleConns)
		tr.MaxIdleConnsPerHost = max(c.Conns, tr.MaxIdleConnsPerHost)
	})
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithHTTPClient(httpClient),
		awsconfig.WithRetryMaxAttempts(1),
	}
	if c.Region != "" {
		opts = append(opts, awsconfig.WithRegion(c.Region))
	}
	if c.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(c.Profile))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	if cfg.Region == "" {
		return nil, errors.New("no region: pass one, or use a profile or environment that sets it")
	}
	if c.Endpoint == "" && cfg.BaseEndpoint == nil {
		return nil, errors.New("no endpoint: pass one, or use a profile that sets endpoint_url")
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
		o.UsePathStyle = true
	}), nil
}
