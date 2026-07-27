// Package cors implements the allowed-origin matching behind the S3
// listener's CORS support. It is pure string logic — the fiber middleware
// that consumes it lives in the root package (cors.go), and the config
// package reuses NewMatcher to validate patterns at startup.
package cors

import (
	"fmt"
	"strings"
)

// wildcard is one parsed subdomain pattern: "https://*.dev.example" splits
// into prefix "https://" and suffix ".dev.example".
type wildcard struct {
	prefix string
	suffix string
}

// Matcher decides whether a request's Origin header is allowed. Patterns
// are either exact origins ("https://app.example"), subdomain wildcards
// ("https://*.dev.example", any depth of subdomain), or the lone "*"
// (any origin). Matching is case-insensitive; a non-default port must be
// spelled out in the pattern because browsers include it in the Origin.
type Matcher struct {
	any       bool
	exact     map[string]struct{}
	wildcards []wildcard
}

// NewMatcher parses and validates the configured origin patterns. It
// rejects anything that isn't a well-formed origin — a scheme other than
// http/https, a path or trailing slash, or a '*' anywhere but the leftmost
// host label — so a typo fails at startup rather than silently never
// matching.
func NewMatcher(patterns []string) (*Matcher, error) {
	m := &Matcher{exact: make(map[string]struct{}, len(patterns))}
	for _, raw := range patterns {
		p := strings.ToLower(strings.TrimSpace(raw))
		if p == "" {
			return nil, fmt.Errorf("cors: empty origin pattern")
		}
		if p == "*" {
			m.any = true
			continue
		}
		rest, ok := strings.CutPrefix(p, "https://")
		if !ok {
			rest, ok = strings.CutPrefix(p, "http://")
		}
		if !ok {
			return nil, fmt.Errorf("cors: origin pattern %q must start with http:// or https://", raw)
		}
		if rest == "" || strings.ContainsAny(rest, "/?#") {
			return nil, fmt.Errorf("cors: origin pattern %q must be a bare origin (scheme://host[:port], no path)", raw)
		}
		switch strings.Count(p, "*") {
		case 0:
			m.exact[p] = struct{}{}
		case 1:
			// The '*' must be the whole leftmost host label:
			// scheme://*.rest-of-host[:port].
			tail, ok := strings.CutPrefix(rest, "*.")
			if !ok || tail == "" {
				return nil, fmt.Errorf("cors: origin pattern %q: '*' must replace the leftmost host label (e.g. https://*.dev.example)", raw)
			}
			i := strings.Index(p, "*")
			m.wildcards = append(m.wildcards, wildcard{prefix: p[:i], suffix: p[i+1:]})
		default:
			return nil, fmt.Errorf("cors: origin pattern %q: at most one '*' is allowed", raw)
		}
	}
	return m, nil
}

// Allows reports whether origin (the request's Origin header, verbatim)
// matches one of the configured patterns.
func (m *Matcher) Allows(origin string) bool {
	o := strings.ToLower(strings.TrimSpace(origin))
	if o == "" {
		return false
	}
	if m.any {
		return true
	}
	if _, ok := m.exact[o]; ok {
		return true
	}
	for _, w := range m.wildcards {
		if len(o) <= len(w.prefix)+len(w.suffix) {
			continue
		}
		if !strings.HasPrefix(o, w.prefix) || !strings.HasSuffix(o, w.suffix) {
			continue
		}
		// The part the '*' stood for must look like host labels — bare
		// prefix/suffix checks would let "https://evil.com/x.dev.example"
		// through on a ".dev.example" suffix.
		if hostLabels(o[len(w.prefix) : len(o)-len(w.suffix)]) {
			return true
		}
	}
	return false
}

// hostLabels reports whether s is one or more dot-separated DNS labels:
// alphanumerics and '-' only, no empty labels, no leading/trailing dot.
func hostLabels(s string) bool {
	for _, label := range strings.Split(s, ".") {
		if label == "" {
			return false
		}
		for _, r := range label {
			switch {
			case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			default:
				return false
			}
		}
	}
	return true
}
