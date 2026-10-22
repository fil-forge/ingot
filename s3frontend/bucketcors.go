package s3frontend

import (
	"context"
	"errors"
	"fmt"

	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/s3err"

	"github.com/fil-forge/ingot/registry"
)

// CORS (docs/s3-cors.md). versitygw drives all of its CORS behaviour off
// GetBucketCors: ApplyBucketCORS is attached to every bucket/object route
// and ctrl.CORSOptions answers preflights from the document it returns. A
// bucket answers with its own document when PutBucketCors has stored one,
// and otherwise with the gateway-wide document rendered from
// cors_allowed_origins (internal/cors), so the operator's list is the
// default and a tenant's document replaces it for that bucket. With neither,
// the sentinel NoSuchCORSConfiguration makes versitygw's CORS middlewares
// fall through untouched.

const (
	// maxCORSRules is S3's limit on the rules one configuration holds.
	// versitygw's validator checks each rule and never the count, so the
	// count is checked here.
	maxCORSRules = 100
	// maxCORSConfig bounds the document PutBucketCors stores. A hundred
	// validated rules is a few KiB, so the cap only keeps a registry row
	// from carrying an unbounded body.
	maxCORSConfig = 64 << 10
)

// GetBucketCors returns the CORS document the bucket answers with: its own,
// else the gateway-wide one, else ErrNoSuchCORSConfiguration. Called on
// every request that carries an Origin header, so it is one registry Get.
func (b *Backend) GetBucketCors(ctx context.Context, bucket string) ([]byte, error) {
	st, err := b.reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, err
	}
	if len(st.CORSConfig) > 0 {
		return st.CORSConfig, nil
	}
	if len(b.cors) > 0 {
		return b.cors, nil
	}
	return nil, s3err.GetAPIError(s3err.ErrNoSuchCORSConfiguration)
}

// PutBucketCors stores the bucket's own CORS document. The controller has
// already parsed and validated the XML (CORSConfiguration.Validate) and
// hands us the body to store verbatim, the document GetBucketCors returns
// and the controller parses back. The rule count is the one check the
// validator leaves out: past S3's hundred the document is refused as
// MalformedXML, the schema's own refusal, and the stored one stands.
func (b *Backend) PutBucketCors(ctx context.Context, bucket string, cors []byte) error {
	if len(cors) > maxCORSConfig {
		return s3err.GetAPIError(s3err.ErrEntityTooLarge)
	}
	cfg, err := auth.ParseCORSOutput(cors)
	if err != nil || len(cfg.Rules) > maxCORSRules {
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	return b.setCORSConfig(ctx, bucket, cors)
}

// DeleteBucketCors clears the bucket's own CORS document, so it answers
// with the gateway-wide one again. Idempotent: a bucket without one is a
// success no-op, as it is in S3.
func (b *Backend) DeleteBucketCors(ctx context.Context, bucket string) error {
	return b.setCORSConfig(ctx, bucket, nil)
}

// setCORSConfig stores doc as the bucket's own CORS document. The registry
// reports a missing row itself, so both writers reach the S3 sentinel
// without a separate existence check.
func (b *Backend) setCORSConfig(ctx context.Context, bucket string, doc []byte) error {
	if err := b.reg.SetCORSConfig(ctx, bucket, doc); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return fmt.Errorf("s3frontend: set bucket cors %q: %w", bucket, err)
	}
	return nil
}
