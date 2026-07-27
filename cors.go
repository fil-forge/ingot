package ingot

import (
	"strings"

	"github.com/fil-forge/versitygw/s3api/middlewares"
	"github.com/gofiber/fiber/v3"

	"github.com/fil-forge/ingot/internal/cors"
)

// corsExposeHeaders are the response headers browser JavaScript may read
// on cross-origin responses. ETag is the one S3 clients can't live
// without (PUT/multipart verification); the x-amz-* set covers request
// tracing and versioning metadata.
const corsExposeHeaders = "ETag, x-amz-storage-class, x-amz-request-id, x-amz-id-2, x-amz-version-id"

// corsMiddleware answers CORS for the whole listener from a static
// allow-list. Access-Control-Allow-Origin only carries a single exact
// origin, so wildcard-subdomain patterns can't be expressed as a static
// header value: the middleware instead matches each request's Origin
// against the configured patterns and echoes it back when allowed (with
// Vary: Origin for caches).
//
// An allowed preflight (OPTIONS + Access-Control-Request-Method) is
// answered directly with 204 and never reaches the router — browsers
// send preflights without SigV4 credentials, so letting one through
// would just be rejected by auth. Everything else falls through
// untouched; a disallowed Origin gets no CORS headers, which is how a
// browser is told "no".
//
// Access-Control-Allow-Credentials is deliberately never set: S3
// authentication travels in the Authorization header and presigned query
// strings, not cookies, and reflecting allow-listed origins with
// credentials enabled would widen the blast radius of a bad pattern.
func corsMiddleware(m *cors.Matcher) fiber.Handler {
	return func(c fiber.Ctx) error {
		origin := strings.TrimSpace(c.Get(fiber.HeaderOrigin))
		if origin == "" || !m.Allows(origin) {
			return c.Next()
		}
		h := &c.Response().Header
		h.Set(fiber.HeaderAccessControlAllowOrigin, origin)
		h.Set(fiber.HeaderVary, middlewares.VaryHdr)
		h.Set(fiber.HeaderAccessControlExposeHeaders, corsExposeHeaders)

		if c.Method() == fiber.MethodOptions {
			if reqMethod := strings.TrimSpace(c.Get(fiber.HeaderAccessControlRequestMethod)); reqMethod != "" {
				h.Set(fiber.HeaderAccessControlAllowMethods, reqMethod)
				if reqHeaders := strings.TrimSpace(c.Get(fiber.HeaderAccessControlRequestHeaders)); reqHeaders != "" {
					h.Set(fiber.HeaderAccessControlAllowHeaders, reqHeaders)
				}
				return c.SendStatus(fiber.StatusNoContent)
			}
		}
		return c.Next()
	}
}
