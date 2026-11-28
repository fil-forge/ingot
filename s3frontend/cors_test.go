package s3frontend

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/cors"
	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/registry"
)

// GetBucketCors is what makes versitygw's CORS middlewares fire at all:
// they only proceed past it on NoSuchCORSConfiguration/NoSuchBucket, and
// serve the document otherwise.
func TestGetBucketCors(t *testing.T) {
	ctx := context.Background()
	const origin = "https://app.example"
	cfg, err := cors.Build([]string{origin})
	require.NoError(t, err, "cors.Build")

	newBackend := func(t *testing.T, corsCfg *auth.CORSConfiguration) *Backend {
		t.Helper()
		mem := newTestStore(t)
		require.NoError(t, mem.Create(ctx, "bk", did.Undef, registry.CreateState{}), "create bucket")
		cache, err := blockstore.NewBlobCache(filepath.Join(t.TempDir(), "cache"))
		require.NoError(t, err, "cache")
		b, err := New(Deps{Tasks: mem.tasks, Capturer: testfakes.NopUploader{}, Registry: mem, Intents: mem, Cache: cache, CORS: corsCfg})
		require.NoError(t, err, "backend")
		return b
	}

	// The document must survive New's marshalling into something
	// versitygw parses back to the same rules — that round trip is the
	// whole contract between internal/cors and the S3 API.
	t.Run("configured", func(t *testing.T) {
		got, err := newBackend(t, cfg).GetBucketCors(ctx, "bk")
		require.NoError(t, err, "GetBucketCors")
		parsed, err := auth.ParseCORSOutput(got)
		require.NoError(t, err, "ParseCORSOutput(%q)", got)
		_, err = parsed.IsAllowed(origin, "PUT", nil, s3err.ResourceType(""))
		assert.NoError(t, err, "IsAllowed(%q)", origin)
	})

	t.Run("unconfigured", func(t *testing.T) {
		_, err := newBackend(t, nil).GetBucketCors(ctx, "bk")
		assert.ErrorIs(t, err, s3err.GetAPIError(s3err.ErrNoSuchCORSConfiguration), "GetBucketCors")
	})

	t.Run("unknown bucket", func(t *testing.T) {
		_, err := newBackend(t, cfg).GetBucketCors(ctx, "missing")
		assert.ErrorIs(t, err, s3err.GetAPIError(s3err.ErrNoSuchBucket), "GetBucketCors")
	})
}
