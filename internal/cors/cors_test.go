package cors

import (
	"testing"

	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// build renders origins, failing the test on error. Assertions run
// through versitygw's own IsAllowed — the code that will evaluate this
// configuration on every request — rather than over the struct fields,
// so they cover what the rules actually permit.
func build(t *testing.T, origins []string) *auth.CORSConfiguration {
	t.Helper()
	cfg, err := Build(origins)
	require.NoError(t, err, "Build")
	return cfg
}

func TestBuild_EmptyDisablesCORS(t *testing.T) {
	cfg, err := Build(nil)
	require.NoError(t, err, "Build(nil)")
	assert.Nil(t, cfg, "Build(nil)")
}

func TestBuild_RejectsBadOrigins(t *testing.T) {
	cases := []struct {
		name    string
		origin  string
		wantErr string
	}{
		{"empty", "", "empty origin"},
		{"no scheme", "app.example", "must start with http:// or https://"},
		{"bad scheme", "ftp://app.example", "must start with http:// or https://"},
		{"trailing slash", "https://app.example/", "bare origin"},
		{"path", "https://app.example/bucket", "bare origin"},
		{"bare scheme", "https://", "bare origin"},
		{"userinfo", "https://user@app.example", "bare origin"},
		{"userinfo with password", "https://user:pass@app.example", "bare origin"},
		{"two stars", "https://*.*.example", "at most one '*'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Build([]string{tc.origin})
			require.Error(t, err, "expected error containing %q, got: %v", tc.wantErr, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestBuild_OriginMatching(t *testing.T) {
	cfg := build(t, []string{
		"https://app.example",
		"https://*.dev.example",
		"http://localhost:3000",
	})

	allow := []string{
		"https://app.example",
		"https://x.dev.example",
		"https://feature-42.dev.example",
		"https://a.b.dev.example", // the wildcard spans nested subdomains
		"http://localhost:3000",
	}
	deny := []string{
		"https://evil.example",
		"http://app.example",       // scheme must match
		"https://app.example:8443", // port not in the configured origin
		"https://dev.example",      // the wildcard does not match the apex
	}
	for _, o := range allow {
		_, err := cfg.IsAllowed(o, "PUT", nil, s3err.ResourceType(""))
		assert.NoError(t, err, "IsAllowed(%q)", o)
	}
	for _, o := range deny {
		_, err := cfg.IsAllowed(o, "PUT", nil, s3err.ResourceType(""))
		assert.Error(t, err, "IsAllowed(%q) = allowed, want denied", o)
	}
}

func TestBuild_AllowanceHeaders(t *testing.T) {
	cfg := build(t, []string{"https://app.example"})

	// The header set a browser sends on an S3 preflight: SigV4 travels in
	// authorization plus x-amz-*, so AllowedHeaders must cover them.
	headers, err := auth.ParseCORSHeaders("authorization, content-type, x-amz-content-sha256, x-amz-date")
	require.NoError(t, err, "ParseCORSHeaders")

	allowance, err := cfg.IsAllowed("https://app.example", "PUT", headers, s3err.ResourceType(""))
	require.NoError(t, err, "IsAllowed")
	assert.Equal(t, "https://app.example", allowance.Origin, "Origin = %q, want the request origin echoed", allowance.Origin)
	for _, m := range []string{"GET", "HEAD", "PUT", "POST", "DELETE"} {
		assert.Contains(t, allowance.Methods, m, "Methods")
	}
	for _, h := range []string{"ETag", "x-cid", "Accept-Ranges", "Content-Range"} {
		assert.Contains(t, allowance.ExposedHeaders, h, "ExposedHeaders")
	}
	if assert.NotNil(t, allowance.MaxAge, "MaxAge") {
		assert.Equal(t, maxAgeSeconds, *allowance.MaxAge, "MaxAge")
	}
}

// A lone "*" allows any origin, and versitygw answers it with the
// wildcard rather than the request's origin — which is also what turns
// Allow-Credentials off.
func TestBuild_WildcardOrigin(t *testing.T) {
	cfg := build(t, []string{"*"})

	allowance, err := cfg.IsAllowed("https://anything.example", "GET", nil, s3err.ResourceType(""))
	require.NoError(t, err, "IsAllowed")
	assert.Equal(t, "*", allowance.Origin, "Origin")
	assert.Equal(t, "false", allowance.AllowCredentials, "AllowCredentials = %q, want \"false\" for a wildcard origin", allowance.AllowCredentials)
}
