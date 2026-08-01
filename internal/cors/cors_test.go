package cors

import (
	"strings"
	"testing"

	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/s3err"
)

// build renders origins, failing the test on error. Assertions run
// through versitygw's own IsAllowed — the code that will evaluate this
// configuration on every request — rather than over the struct fields,
// so they cover what the rules actually permit.
func build(t *testing.T, origins []string) *auth.CORSConfiguration {
	t.Helper()
	cfg, err := Build(origins)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return cfg
}

func TestBuild_EmptyDisablesCORS(t *testing.T) {
	cfg, err := Build(nil)
	if err != nil {
		t.Fatalf("Build(nil): %v", err)
	}
	if cfg != nil {
		t.Errorf("Build(nil) = %+v, want nil (CORS disabled)", cfg)
	}
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
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
			}
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
		if _, err := cfg.IsAllowed(o, "PUT", nil, s3err.ResourceType("")); err != nil {
			t.Errorf("IsAllowed(%q) = %v, want allowed", o, err)
		}
	}
	for _, o := range deny {
		if _, err := cfg.IsAllowed(o, "PUT", nil, s3err.ResourceType("")); err == nil {
			t.Errorf("IsAllowed(%q) = allowed, want denied", o)
		}
	}
}

func TestBuild_AllowanceHeaders(t *testing.T) {
	cfg := build(t, []string{"https://app.example"})

	// The header set a browser sends on an S3 preflight: SigV4 travels in
	// authorization plus x-amz-*, so AllowedHeaders must cover them.
	headers, err := auth.ParseCORSHeaders("authorization, content-type, x-amz-content-sha256, x-amz-date")
	if err != nil {
		t.Fatalf("ParseCORSHeaders: %v", err)
	}

	allowance, err := cfg.IsAllowed("https://app.example", "PUT", headers, s3err.ResourceType(""))
	if err != nil {
		t.Fatalf("IsAllowed: %v", err)
	}
	if allowance.Origin != "https://app.example" {
		t.Errorf("Origin = %q, want the request origin echoed", allowance.Origin)
	}
	for _, m := range []string{"GET", "HEAD", "PUT", "POST", "DELETE"} {
		if !strings.Contains(allowance.Methods, m) {
			t.Errorf("Methods = %q, want it to include %s", allowance.Methods, m)
		}
	}
	if !strings.Contains(allowance.ExposedHeaders, "ETag") {
		t.Errorf("ExposedHeaders = %q, want it to include ETag", allowance.ExposedHeaders)
	}
	if allowance.MaxAge == nil || *allowance.MaxAge != maxAgeSeconds {
		t.Errorf("MaxAge = %v, want %d", allowance.MaxAge, maxAgeSeconds)
	}
}

// A lone "*" allows any origin, and versitygw answers it with the
// wildcard rather than the request's origin — which is also what turns
// Allow-Credentials off.
func TestBuild_WildcardOrigin(t *testing.T) {
	cfg := build(t, []string{"*"})

	allowance, err := cfg.IsAllowed("https://anything.example", "GET", nil, s3err.ResourceType(""))
	if err != nil {
		t.Fatalf("IsAllowed: %v", err)
	}
	if allowance.Origin != "*" {
		t.Errorf("Origin = %q, want \"*\"", allowance.Origin)
	}
	if allowance.AllowCredentials != "false" {
		t.Errorf("AllowCredentials = %q, want \"false\" for a wildcard origin", allowance.AllowCredentials)
	}
}
