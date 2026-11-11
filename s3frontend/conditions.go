package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"

	"github.com/fil-forge/ingot/registry"

	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/taskrunner"
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
	return rv.mf.QuotedETag(), true, nil
}

// isNoSuchKey reports whether err is the NoSuchKey API error resolution
// returns for a missing object.
func isNoSuchKey(err error) bool {
	return errors.Is(err, s3err.GetAPIError(s3err.ErrNoSuchKey))
}

// mapBucketError maps an error from a bucket queue task, or from one of a
// commit's changes, to the S3 error that answers it; nil stays nil.
// Anything else is wrapped with op for the server log, and answers
// InternalError: a superseded reservation is among them, since the client
// retries it.
func mapBucketError(err error, op string) error {
	if err == nil {
		return nil
	}
	var ce *bucketqueue.ChangeError
	if errors.As(err, &ce) {
		switch ce.Kind {
		case bucketqueue.KindPreconditionFailed:
			if ce.Condition == "" {
				return s3err.GetAPIError(s3err.ErrPreconditionFailed)
			}
			return s3err.GetPreconditionFailedErr(s3err.Condition(ce.Condition))
		case bucketqueue.KindNoSuchKey:
			return s3err.GetAPIError(s3err.ErrNoSuchKey)
		case bucketqueue.KindNoSuchVersion:
			return s3err.GetNoSuchVersionErr(ce.Key, ce.VersionID)
		case bucketqueue.KindInvalidVersionID:
			return errInvalidVersionID(ce.VersionID)
		case bucketqueue.KindDeleteMarker:
			return s3err.GetAPIError(s3err.ErrMethodNotAllowed)
		case bucketqueue.KindNoObjectLock:
			return s3err.GetAPIError(s3err.ErrMissingObjectLockConfiguration)
		}
	}
	switch {
	case errors.Is(err, bucketop.ErrBucketNotFound):
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	case errors.Is(err, bucketqueue.ErrBucketClosing):
		return errBucketClosing
	case errors.Is(err, bucketqueue.ErrBucketExists):
		return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
	case errors.Is(err, bucketqueue.ErrVersionedBucketNotEmpty):
		return s3err.GetAPIError(s3err.ErrVersionedBucketNotEmpty)
	case errors.Is(err, bucketqueue.ErrBucketNotEmpty):
		return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
	case errors.Is(err, bucketqueue.ErrLockedVersioning):
		return s3err.GetAPIError(s3err.ErrSuspendedVersioningNotAllowed)
	case errors.Is(err, bucketqueue.ErrLockNeedsVersioning):
		return s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotAllowed)
	case errors.Is(err, registry.ErrReferenceExpired):
		// The write's reference to one of the body's blobs expired before
		// the commit, and the blob may be released: the swap wrote nothing,
		// and the client sends the write again.
		return s3err.GetAPIError(s3err.ErrSlowDown)
	}
	return fmt.Errorf("s3frontend: %s: %w", op, err)
}

// deletePreconditions converts S3's delete preconditions for a change;
// nil for none.
func deletePreconditions(p *backend.ObjectDeletePreconditions) *bucketqueue.DeletePreconditions {
	if p == nil || (p.IfMatch == nil && p.IfMatchSize == nil && p.IfMatchLastModTime == nil) {
		return nil
	}
	var out bucketqueue.DeletePreconditions
	if p.IfMatch != nil {
		out.HasIfMatch, out.IfMatch = true, *p.IfMatch
	}
	if p.IfMatchSize != nil {
		out.HasSize, out.Size = true, *p.IfMatchSize
	}
	if p.IfMatchLastModTime != nil {
		out.HasModTime, out.ModTime = true, p.IfMatchLastModTime.UnixNano()
	}
	return &out
}

// blobRefTasks converts a write's pending references for a change.
func blobRefTasks(refs map[string]taskrunner.TaskID) []bucketqueue.BlobRefTask {
	out := make([]bucketqueue.BlobRefTask, 0, len(refs))
	for d, id := range refs {
		out = append(out, bucketqueue.BlobRefTask{Digest: []byte(d), Ref: append([]byte(nil), id[:]...)})
	}
	return out
}
