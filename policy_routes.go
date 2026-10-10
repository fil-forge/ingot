package ingot

import (
	"errors"
	"net/http"

	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/internal/fasthttputil"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/s3frontend"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3api"
	"github.com/fil-forge/versitygw/s3api/middlewares"
	"github.com/fil-forge/versitygw/s3api/utils"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"
)

// policyRoutes answers the S3 bucket policy operations, GET, PUT and DELETE
// on /{bucket}?policy, by forwarding the signed request to Hilt's
// /s3/bucket/policy. A request on the bucket path without ?policy falls
// through to the S3 route table.
//
// These routes mount ahead of every s3api.WithMiddleware entry and of the S3
// route table's own steps, so a ?policy request gets its server span here and
// the bucket CORS headers in the handler. A request without ?policy skips
// both, to be traced (once) by the gateway middleware.
func policyRoutes(be backend.Backend, authority bucketauthority.BucketAuthority, logger *zap.Logger) []s3api.Option {
	trace := forPolicyRequests(tracing.Middleware())
	handler := policyHandler(be, authority, logger)
	return []s3api.Option{
		s3api.WithRoute(http.MethodGet, "/:bucket", trace, handler),
		s3api.WithRoute(http.MethodPut, "/:bucket", trace, handler),
		s3api.WithRoute(http.MethodDelete, "/:bucket", trace, handler),
	}
}

// isPolicyRequest reports whether the request addresses the bucket's
// ?policy subresource.
func isPolicyRequest(c fiber.Ctx) bool {
	return c.RequestCtx().QueryArgs().Has("policy")
}

// forPolicyRequests runs h for a ?policy request and passes any other
// request on.
func forPolicyRequests(h fiber.Handler) fiber.Handler {
	return func(c fiber.Ctx) error {
		if !isPolicyRequest(c) {
			return c.Next()
		}
		return h(c)
	}
}

func policyHandler(be backend.Backend, authority bucketauthority.BucketAuthority, logger *zap.Logger) fiber.Handler {
	applyCORS := middlewares.ApplyBucketCORS(be, middlewares.BucketFromPath, "")
	return func(c fiber.Ctx) error {
		if !isPolicyRequest(c) {
			return c.Next()
		}
		req := fasthttputil.RequestFromHTTPContext(c.RequestCtx())
		var ok *s3bkt.PolicyOK
		err := applyCORS(c)
		if err == nil {
			err = bucketauthority.ErrUnsupported
			if authority != nil {
				ok, err = authority.BucketPolicy(c.RequestCtx(), req, c.Body())
			}
		}
		if err != nil {
			apiErr := policyError(err)
			if apiErr.HTTPStatusCode >= http.StatusInternalServerError {
				logger.Error("bucket policy operation failed", zap.String("method", req.Method), zap.Error(err))
			}
			c.Set(fiber.HeaderContentType, "application/xml")
			return c.Status(apiErr.HTTPStatusCode).Send(apiErr.XMLBody(utils.EnsureRequestIDs(c)))
		}
		if ok.ETag != "" {
			c.Set("ETag", ok.ETag)
		}
		switch c.Method() {
		case http.MethodGet:
			c.Set(fiber.HeaderContentType, "application/json")
			return c.Status(http.StatusOK).Send(ok.Policy)
		default:
			return c.SendStatus(http.StatusNoContent)
		}
	}
}

// policyError renders a bucket policy operation's failure as the S3 error the
// RFC names: the policy rejections carried by the bucket authority, Hilt's
// authorization rejections through the same mapping every operation uses,
// and anything else as an internal error.
func policyError(err error) s3err.APIError {
	// A request the bucket CORS rules reject renders as their own S3 error.
	var corsErr s3err.APIError
	if errors.As(err, &corsErr) {
		return corsErr
	}
	switch {
	case errors.Is(err, bucketauthority.ErrNotFound):
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	case errors.Is(err, bucketauthority.ErrNoPolicy):
		return s3err.GetAPIError(s3err.ErrNoSuchBucketPolicy)
	case errors.Is(err, bucketauthority.ErrMalformedPolicy):
		return s3frontend.MalformedPolicy(err.Error())
	case errors.Is(err, bucketauthority.ErrPreconditionFailed):
		return s3err.GetAPIError(s3err.ErrPreconditionFailed)
	case errors.Is(err, bucketauthority.ErrInvalidPrecondition):
		e := s3err.GetAPIError(s3err.ErrInvalidRequest)
		e.Description = err.Error()
		return e
	case errors.Is(err, bucketauthority.ErrUnsupported):
		return s3err.GetAPIError(s3err.ErrNotImplemented)
	case errors.Is(err, bucketauthority.ErrConcurrentChange):
		return s3err.APIError{
			Code:           "OperationAborted",
			Description:    "A conflicting conditional operation is currently in progress against this resource. Try again.",
			HTTPStatusCode: http.StatusConflict,
		}
	}
	if mapped, ok := iam.MapAuthError(err); ok {
		var s3e s3err.S3Error
		if errors.As(mapped, &s3e) {
			return s3e.BaseError()
		}
		// An unknown, invalid or expired key: versitygw's auth middleware turns
		// ErrNoSuchUser into InvalidAccessKeyId, but these routes mount ahead
		// of it.
		if errors.Is(mapped, auth.ErrNoSuchUser) {
			return s3err.GetAPIError(s3err.ErrInvalidAccessKeyID)
		}
		return s3err.GetAPIError(s3err.ErrAccessDenied)
	}
	return s3err.GetAPIError(s3err.ErrInternalError)
}
