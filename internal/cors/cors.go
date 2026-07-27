// Package cors renders ingot's cors_allowed_origins into the S3 CORS
// configuration the listener reports for every bucket.
//
// versitygw drives all of its CORS behaviour off the backend's
// GetBucketCors — ApplyBucketCORS is attached to every bucket/object
// route and ctrl.CORSOptions answers preflights from the same document
// (s3api/router.go, s3api/controllers/options.go) — so ingot's whole job
// is producing a valid configuration. s3frontend marshals and serves it;
// nothing here matches origins or touches a request.
package cors

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/fil-forge/versitygw/auth"
)

// allowedMethods are the S3 verbs the rule permits. These are exactly
// the methods auth.CORSHTTPMethod.IsValid accepts.
var allowedMethods = []auth.CORSHTTPMethod{
	http.MethodGet,
	http.MethodHead,
	http.MethodPut,
	http.MethodPost,
	http.MethodDelete,
}

// exposeHeaders are the response headers browser JavaScript may read on a
// cross-origin response. ETag is the one S3 clients can't live without
// (PUT/multipart verification) and is listed explicitly because the
// preflight controller doesn't apply versitygw's ensureExposeETag
// fallback; the x-amz-* set covers request tracing and versioning.
var exposeHeaders = []auth.CORSHeader{
	"ETag",
	"x-amz-storage-class",
	"x-amz-request-id",
	"x-amz-id-2",
	"x-amz-version-id",
}

// maxAgeSeconds caps how long a browser may cache a preflight result.
// Without it browsers fall back to ~5s and re-preflight almost every
// request — an extra round trip per PUT for a browser client.
const maxAgeSeconds int32 = 600

// Build renders origins as a single-rule S3 CORS configuration. An empty
// list yields (nil, nil): CORS disabled, which s3frontend reports as
// NoSuchCORSConfiguration so versitygw's CORS middlewares fall through
// untouched.
//
// Origins are matched by versitygw at request time with S3 semantics
// (auth.wildcardMatch): an exact origin, or one '*' standing for any run
// of characters ("https://*.dev.example"). Matching is over the raw
// Origin header, so a non-default port must be spelled out.
func Build(origins []string) (*auth.CORSConfiguration, error) {
	if len(origins) == 0 {
		return nil, nil
	}

	allowed := make([]auth.CORSOrigin, 0, len(origins))
	for _, raw := range origins {
		o := strings.ToLower(strings.TrimSpace(raw))
		if err := validateOrigin(raw, o); err != nil {
			return nil, err
		}
		allowed = append(allowed, auth.CORSOrigin(o))
	}

	maxAge := maxAgeSeconds
	cfg := &auth.CORSConfiguration{
		Rules: []auth.CORSRule{{
			AllowedOrigins: allowed,
			AllowedMethods: allowedMethods,
			// Every requested header must match an entry or
			// CORSRule.Match rejects the preflight, and S3 clients send
			// an open-ended x-amz-* set alongside authorization.
			AllowedHeaders: []auth.CORSHeader{"*"},
			ExposeHeaders:  exposeHeaders,
			MaxAgeSeconds:  &maxAge,
		}},
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("cors: %w", err)
	}
	return cfg, nil
}

// validateOrigin rejects anything that couldn't be a browser Origin.
// versitygw's own CORSOrigin.Validate only rejects a second '*', so
// without this a typo like "app.example" would be accepted and then
// silently never match; raw is carried through for the error message.
func validateOrigin(raw, o string) error {
	if o == "" {
		return fmt.Errorf("cors: empty origin")
	}
	if o == "*" {
		return nil
	}
	rest, ok := strings.CutPrefix(o, "https://")
	if !ok {
		rest, ok = strings.CutPrefix(o, "http://")
	}
	if !ok {
		return fmt.Errorf("cors: origin %q must start with http:// or https://", raw)
	}
	// '@' rejects userinfo (https://user@host): an Origin header is only
	// scheme+host(+port), so such an entry could never match a request.
	if rest == "" || strings.ContainsAny(rest, "/?#@") {
		return fmt.Errorf("cors: origin %q must be a bare origin (scheme://host[:port], no path or userinfo)", raw)
	}
	if strings.Count(o, "*") > 1 {
		return fmt.Errorf("cors: origin %q: at most one '*' is allowed", raw)
	}
	return nil
}
