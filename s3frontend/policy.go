package s3frontend

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/s3err"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
)

// The bucket policy operations forward the signed request to the bucket
// authority (Hilt's /s3/bucket/policy), which authenticates and authorizes
// it, checks its preconditions and keeps the document. versitygw's
// controllers have validated a PUT body's AWS shape first. The response ETag
// is computed here from the document, with the same [s3bkt.ETag] Hilt stores
// and checks If-Match against, and travels back through the request's
// reqscope ETag holder, since the backend seam returns no headers.

// PutBucketPolicy stores the policy document through the bucket authority.
func (b *Backend) PutBucketPolicy(ctx context.Context, _ string, policy []byte) error {
	_, err := b.bucketPolicy(ctx, policy)
	return err
}

// DeleteBucketPolicy removes the bucket's policy through the bucket authority.
func (b *Backend) DeleteBucketPolicy(ctx context.Context, _ string) error {
	_, err := b.bucketPolicy(ctx, nil)
	return err
}

// GetBucketPolicy returns the bucket's policy document from the bucket
// authority, but only to the GetBucketPolicy operation itself: a signed GET
// /{bucket}?policy. versitygw also calls it from its own access checks,
// auth.VerifyPublicAccess for an anonymous request and auth.VerifyAccess for
// accounts below the admin role (Hilt-authorized accounts carry the admin
// role and skip it). Those must neither reach Hilt nor get a document to
// evaluate, so they get the stub answer: NoSuchBucket for an unknown bucket,
// else NoSuchBucketPolicy, which leaves access to Hilt.
func (b *Backend) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	if req, ok := reqscope.Request(ctx); ok && isSignedPolicyGet(req) {
		return b.bucketPolicy(ctx, nil)
	}
	if _, err := b.reg.Get(ctx, bucket); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, err
	}
	return nil, s3err.GetAPIError(s3err.ErrNoSuchBucketPolicy)
}

// bucketPolicy forwards the request on ctx, with body on a PUT, and returns
// the stored document on a GET. Its errors are all S3 errors: versitygw
// renders anything else as InternalError.
func (b *Backend) bucketPolicy(ctx context.Context, body []byte) ([]byte, error) {
	if b.authority == nil {
		return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
	}
	req, ok := reqscope.Request(ctx)
	if !ok {
		b.logger.Error("bucket policy operation without a request in context")
		return nil, s3err.GetAPIError(s3err.ErrInternalError)
	}
	res, err := b.authority.BucketPolicy(ctx, req, body)
	if err != nil {
		apiErr := policyError(err)
		if apiErr.HTTPStatusCode >= http.StatusInternalServerError {
			b.logger.Error("bucket policy operation failed", zap.String("method", req.Method), zap.Error(err))
		}
		return nil, apiErr
	}
	switch {
	case res.Policy != nil: // GET
		reqscope.SetETag(ctx, s3bkt.ETag(*res.Policy))
		// The policy types encode as the AWS document Hilt stores; their
		// encoding cannot fail.
		doc, _ := json.Marshal(res.Policy)
		return doc, nil
	case body != nil: // PUT
		// Hilt stores the document this body decodes to, normalized, so its tag
		// is the stored tag. The body passed Hilt's own decoder a moment ago;
		// a failure here means the two sides run different libforge versions.
		var doc s3bkt.BucketPolicy
		if err := json.Unmarshal(body, &doc); err != nil {
			b.logger.Error("tagging an accepted bucket policy", zap.Error(err))
			return nil, nil
		}
		reqscope.SetETag(ctx, s3bkt.ETag(doc))
	}
	return nil, nil
}

// isSignedPolicyGet reports whether req is a GetBucketPolicy request carrying
// a signature, in its Authorization header or a presigned URL.
func isSignedPolicyGet(req s3.Request) bool {
	if req.Method != http.MethodGet {
		return false
	}
	u, err := url.ParseRequestURI(req.URL)
	if err != nil {
		return false
	}
	q := u.Query()
	if !q.Has("policy") {
		return false
	}
	if q.Get("X-Amz-Signature") != "" {
		return true
	}
	for k, v := range req.Headers {
		if strings.EqualFold(k, "Authorization") && v != "" {
			return true
		}
	}
	return false
}

// policyError renders a bucket policy operation's failure as the S3 error the
// RFC names: the policy rejections carried by the bucket authority, Hilt's
// authorization rejections through the same mapping every operation uses,
// and anything else as an internal error.
func policyError(err error) s3err.APIError {
	switch {
	case errors.Is(err, bucketauthority.ErrNotFound):
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	case errors.Is(err, bucketauthority.ErrNoPolicy):
		return s3err.GetAPIError(s3err.ErrNoSuchBucketPolicy)
	case errors.Is(err, bucketauthority.ErrMalformedPolicy):
		return MalformedPolicy(err.Error())
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
		// An unknown, revoked or expired key: versitygw's auth middleware
		// turns ErrNoSuchUser into InvalidAccessKeyId, but this error comes
		// from the backend, after it.
		if errors.Is(mapped, auth.ErrNoSuchUser) {
			return s3err.GetAPIError(s3err.ErrInvalidAccessKeyID)
		}
		return s3err.GetAPIError(s3err.ErrAccessDenied)
	}
	return s3err.GetAPIError(s3err.ErrInternalError)
}
