package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"

	"github.com/fil-forge/ingot/bucketop"
)

// copySourceConditionPrefix is what S3 prepends to the failed header's name
// in a CopyObject / UploadPartCopy 412 body: the <Condition> reads
// "x-amz-copy-source-If-None-Match", never the bare GET-style "If-None-Match".
// versitygw's PreconditionFailedError carries the bare names, so the copy path
// rewrites them (and defines If-Modified-Since, which on a GET can only
// produce a bodiless 304 and so has no upstream constant).
const copySourceConditionPrefix = "x-amz-copy-source-"

const conditionIfModifiedSince s3err.Condition = "If-Modified-Since"

// evaluateCopySourcePreconditions evaluates the x-amz-copy-source-if-*
// headers against the source version's ETag and mtime. Every failed copy
// precondition is a 412 PreconditionFailed (verified against S3):
// backend.EvaluatePreconditions implements the RFC 7232 read semantics, where
// a matched If-None-Match or an unsatisfied If-Modified-Since is a 304 Not
// Modified telling the client its cached copy is still fresh. A copy has no
// client-side representation to reuse, so S3 reports those two the same way
// as the other failures, and this remaps them. The condition named in the
// body is If-None-Match when that header was sent (the helper only yields 304
// for If-None-Match when it matched); otherwise If-Modified-Since is the
// header that failed. A future If-Modified-Since never reaches here: the
// controller drops it, as S3 ignores it.
func evaluateCopySourcePreconditions(etag string, modTime time.Time, pc backend.PreConditions) error {
	err := backend.EvaluatePreconditions(etag, modTime, pc)
	var apiErr s3err.APIError
	if errors.As(err, &apiErr) && apiErr.HTTPStatusCode == http.StatusNotModified {
		if pc.IfNoneMatch != nil {
			return copySourcePreconditionFailed(s3err.ConditionIfNoneMatch)
		}
		return copySourcePreconditionFailed(conditionIfModifiedSince)
	}
	var pf s3err.PreconditionFailedError
	if errors.As(err, &pf) {
		return copySourcePreconditionFailed(pf.Condition)
	}
	return err
}

// copySourcePreconditionFailed is the 412 for a failed x-amz-copy-source-if-*
// header, naming the header the way S3 does.
func copySourcePreconditionFailed(c s3err.Condition) s3err.PreconditionFailedError {
	return s3err.GetPreconditionFailedErr(s3err.Condition(copySourceConditionPrefix + string(c)))
}

// currentObjectETag resolves the committed ETag of (bucket, key) for evaluating
// put/copy preconditions before ingest. It distinguishes "no such key" (exists
// = false, no error) from real errors; a delete-marker current counts as "no
// object" for precondition purposes.
func (b *Backend) currentObjectETag(ctx context.Context, bucket, key string) (etag string, exists bool, err error) {
	rv, err := b.resolveVersion(ctx, bucket, key, "")
	if err != nil {
		if isNoSuchKey(err) {
			return "", false, nil
		}
		return "", false, err
	}
	if rv.mf.DeleteMarker {
		return "", false, nil
	}
	return etagOf(rv.mf), true, nil
}

// isNoSuchKey reports whether err is the NoSuchKey API error resolution
// returns for a missing object.
func isNoSuchKey(err error) bool {
	return errors.Is(err, s3err.GetAPIError(s3err.ErrNoSuchKey))
}

// mapCommitError maps an error from a bucketop.WithTx commit closure to the
// S3-facing error: a missing bucket becomes NoSuchBucket, a versitygw API error
// (e.g. a precondition failure raised inside the closure) is surfaced verbatim
// so its HTTP status is preserved, and anything else is wrapped with op for the
// server log.
func mapCommitError(err error, op string) error {
	if errors.Is(err, bucketop.ErrBucketNotFound) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	// PreconditionFailedError, NoSuchVersionError, and InvalidArgumentError
	// embed APIError but are distinct concrete types, so errors.As against
	// APIError below won't match them. Surface each verbatim so its status
	// and XML body reach the versitygw error renderer (which type-asserts
	// s3err.S3Error without unwrapping — a generic %w wrap would degrade it
	// to InternalError).
	var pf s3err.PreconditionFailedError
	if errors.As(err, &pf) {
		return pf
	}
	var nsv s3err.NoSuchVersionError
	if errors.As(err, &nsv) {
		return nsv
	}
	var inv s3err.InvalidArgumentError
	if errors.As(err, &inv) {
		return inv
	}
	var apiErr s3err.APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return fmt.Errorf("s3frontend: %s: %w", op, err)
}
