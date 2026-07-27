package cors

import (
	"strings"
	"testing"
)

func TestNewMatcher_RejectsBadPatterns(t *testing.T) {
	cases := []struct {
		name    string
		pattern string
		wantErr string
	}{
		{"empty", "", "empty origin pattern"},
		{"no scheme", "app.example", "must start with http:// or https://"},
		{"bad scheme", "ftp://app.example", "must start with http:// or https://"},
		{"trailing slash", "https://app.example/", "bare origin"},
		{"path", "https://app.example/bucket", "bare origin"},
		{"bare scheme", "https://", "bare origin"},
		{"star mid-label", "https://foo*.dev.example", "leftmost host label"},
		{"star not a label", "https://*dev.example", "leftmost host label"},
		{"star alone in host", "https://*.", "leftmost host label"},
		{"two stars", "https://*.*.example", "at most one '*'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewMatcher([]string{tc.pattern})
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestMatcher_Allows(t *testing.T) {
	m, err := NewMatcher([]string{
		"https://app.example",
		"https://staging.example",
		"https://*.dev.example",
		"http://localhost:3000",
	})
	if err != nil {
		t.Fatalf("NewMatcher: %v", err)
	}

	allow := []string{
		"https://app.example",
		"HTTPS://APP.EXAMPLE", // origins match case-insensitively
		"https://staging.example",
		"https://x.dev.example",
		"https://feature-42.dev.example",
		"https://a.b.dev.example", // wildcard spans nested subdomains
		"http://localhost:3000",
	}
	deny := []string{
		"",
		"null", // opaque origin (sandboxed iframe, file://)
		"https://evil.example",
		"http://app.example",           // scheme must match
		"https://app.example:8443",     // port not in the pattern
		"https://app.example.evil.com", // suffix trick on an exact origin
		"https://dev.example",          // wildcard does not match the apex
		"https://evil.com/x.dev.example",
		"https://xdev.example",
		"https://evil.com?x=.dev.example",
	}
	for _, o := range allow {
		if !m.Allows(o) {
			t.Errorf("Allows(%q) = false, want true", o)
		}
	}
	for _, o := range deny {
		if m.Allows(o) {
			t.Errorf("Allows(%q) = true, want false", o)
		}
	}
}

func TestMatcher_Wildcard_AllowsAnyOrigin(t *testing.T) {
	m, err := NewMatcher([]string{"*"})
	if err != nil {
		t.Fatalf("NewMatcher: %v", err)
	}
	if !m.Allows("https://anything.example") {
		t.Error(`"*" should allow any origin`)
	}
	if m.Allows("") {
		t.Error("empty origin must never match")
	}
}
