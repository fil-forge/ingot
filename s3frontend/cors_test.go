package s3frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/ucantone/did"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/internal/cors"
)

// GetBucketCors is what makes versitygw's CORS middlewares fire at all:
// they only proceed past it on NoSuchCORSConfiguration/NoSuchBucket, and
// serve the document otherwise.
func TestGetBucketCors(t *testing.T) {
	ctx := context.Background()
	const origin = "https://app.example"
	cfg, err := cors.Build([]string{origin})
	if err != nil {
		t.Fatalf("cors.Build: %v", err)
	}

	newBackend := func(t *testing.T, corsCfg *auth.CORSConfiguration) *Backend {
		t.Helper()
		mem := inmem.NewMemStore()
		if err := mem.Create(ctx, "bk", did.Undef); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		return New(Deps{Registry: mem, CORS: corsCfg})
	}

	// The document must survive New's marshalling into something
	// versitygw parses back to the same rules — that round trip is the
	// whole contract between internal/cors and the S3 API.
	t.Run("configured", func(t *testing.T) {
		got, err := newBackend(t, cfg).GetBucketCors(ctx, "bk")
		if err != nil {
			t.Fatalf("GetBucketCors: %v", err)
		}
		parsed, err := auth.ParseCORSOutput(got)
		if err != nil {
			t.Fatalf("ParseCORSOutput(%q): %v", got, err)
		}
		if _, err := parsed.IsAllowed(origin, "PUT", nil, s3err.ResourceType("")); err != nil {
			t.Errorf("IsAllowed(%q) = %v, want allowed", origin, err)
		}
	})

	t.Run("unconfigured", func(t *testing.T) {
		_, err := newBackend(t, nil).GetBucketCors(ctx, "bk")
		if !errors.Is(err, s3err.GetAPIError(s3err.ErrNoSuchCORSConfiguration)) {
			t.Errorf("GetBucketCors = %v, want NoSuchCORSConfiguration", err)
		}
	})

	t.Run("unknown bucket", func(t *testing.T) {
		_, err := newBackend(t, cfg).GetBucketCors(ctx, "missing")
		if !errors.Is(err, s3err.GetAPIError(s3err.ErrNoSuchBucket)) {
			t.Errorf("GetBucketCors = %v, want NoSuchBucket", err)
		}
	})
}
