package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"
	mh "github.com/multiformats/go-multihash"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/queue/multipartqueue"
	"github.com/fil-forge/ingot/registry"
)

// abortUpload runs sess's queue-multipart-upload.abort for a request and
// then the releases it queued, within releaseNow's budget.
func (b *Backend) abortUpload(ctx context.Context, sess registry.MultipartSession) error {
	released, err := b.uploads.Abort(ctx, sess)
	if err != nil {
		return mapUploadError(err, "abort multipart upload")
	}
	b.blobs.ReleaseNow(ctx, sess.Space, released)
	return nil
}

// errCompleteInProgress is S3's OperationAborted, a 409: another Complete of
// this upload is still running past the loser's wait, or the upload's
// accepts did not finish within their wait. The AWS CLI and other
// botocore-based clients treat it as final (botocore retries neither 409 nor
// OperationAborted), report the upload failed and abort it. Only a client
// that retries on its own gets the replay of the winner's result. (Not in the
// versitygw error table, hence built here.)
var errCompleteInProgress = s3err.APIError{
	Code:           "OperationAborted",
	Description:    "A conflicting conditional operation is currently in progress against this resource. Please try again.",
	HTTPStatusCode: http.StatusConflict,
}

// errPartLayoutOverBudget refuses a Complete whose re-hash would exceed the
// budget: a 400 the client resolves by uploading parts of one size, or in
// part-number order, and completing again. (Not in the versitygw error
// table, hence built here.)
func errPartLayoutOverBudget(need, budget int64) s3err.APIError {
	return s3err.APIError{
		Code: "InvalidPartLayout",
		Description: fmt.Sprintf("The parts were uploaded out of order with varying sizes, so %d bytes would have to be read back to compute the object's digest, over this server's limit of %d bytes. "+
			"Upload parts of one size (the last part may be shorter), or upload them in part-number order, then complete the upload again.", need, budget),
		HTTPStatusCode: http.StatusBadRequest,
	}
}

// mapUploadError maps an error from a multipart upload's change to the S3
// error that answers it; nil stays nil. A Complete's commit and its bucket
// fail with the bucket queue's errors, which mapBucketError maps, as it
// wraps anything else with op.
func mapUploadError(err error, op string) error {
	if err == nil {
		return nil
	}
	var ce *bucketqueue.ChangeError
	if errors.As(err, &ce) {
		return mapBucketError(err, op)
	}
	var (
		typeErr   *multipartqueue.ChecksumTypeError
		sizeErr   *multipartqueue.ObjectSizeError
		sumErr    *multipartqueue.ChecksumError
		layoutErr *multipartqueue.PartLayoutError
		partErr   *multipartqueue.PartError
	)
	switch {
	case errors.Is(err, multipartqueue.ErrNoSuchUpload):
		return s3err.GetAPIError(s3err.ErrNoSuchUpload)
	case errors.Is(err, multipartqueue.ErrBusy):
		return s3err.GetAPIError(s3err.ErrSlowDown)
	case errors.Is(err, multipartqueue.ErrInProgress):
		return errCompleteInProgress
	case errors.Is(err, multipartqueue.ErrConditionUnsupported):
		return s3err.GetAPIError(s3err.ErrNotImplemented)
	case errors.Is(err, multipartqueue.ErrNoSuchKey):
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	case errors.Is(err, multipartqueue.ErrPreconditionFailed):
		return s3err.GetAPIError(s3err.ErrPreconditionFailed)
	case errors.Is(err, multipartqueue.ErrInvalidPart):
		return s3err.GetAPIError(s3err.ErrInvalidPart)
	case errors.Is(err, multipartqueue.ErrMalformedPartList):
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	case errors.Is(err, multipartqueue.ErrInvalidPartOrder):
		return s3err.GetAPIError(s3err.ErrInvalidPartOrder)
	case errors.Is(err, multipartqueue.ErrPartTooSmall):
		return s3err.GetAPIError(s3err.ErrEntityTooSmall)
	case errors.Is(err, multipartqueue.ErrTooManyBlobs):
		return s3err.GetAPIError(s3err.ErrEntityTooLarge)
	case errors.Is(err, bucketqueue.ErrNoObjectLock):
		// The session's lock headers, against a bucket that has lost its
		// lock configuration since: the creation-time variant.
		return s3err.GetAPIError(s3err.ErrMissingObjectLockConfigurationNoSpaces)
	case errors.As(err, &typeErr):
		// A session without a checksum type reports the literal "null".
		t := typeErr.Type
		if t == "" {
			t = types.ChecksumType("null")
		}
		return s3err.GetChecksumTypeMismatchOnMpErr(t)
	case errors.As(err, &sizeErr):
		if sizeErr.Stated < 0 {
			return s3err.GetNegatvieMpObjectSizeErr(sizeErr.Stated)
		}
		return s3err.GetIncorrectMpObjectSizeErr(sizeErr.Total, sizeErr.Stated)
	case errors.As(err, &sumErr):
		return s3err.GetChecksumBadDigestErr(sumErr.Algorithm)
	case errors.As(err, &layoutErr):
		return errPartLayoutOverBudget(layoutErr.Need, layoutErr.Budget)
	case errors.As(err, &partErr):
		return partError(partErr)
	}
	return mapBucketError(err, op)
}

// partError is the S3 error for one listed part's fault.
func partError(e *multipartqueue.PartError) error {
	switch e.Kind {
	case multipartqueue.PartNumberInvalid:
		return s3err.GetInvalidArgumentErr(s3err.InvalidArgCompleteMpPartNumber, e.Arg)
	case multipartqueue.PartChecksumInvalid:
		return s3err.GetInvalidArgumentErr(s3err.InvalidArgChecksumPart, e.Arg)
	case multipartqueue.PartChecksumMissing:
		return s3err.APIError{
			Code:           "InvalidRequest",
			Description:    fmt.Sprintf("The upload was created using a %v checksum. The complete request must include the checksum for each part. It was missing for part %v in the request.", strings.ToLower(string(e.Algorithm)), e.PartNumber),
			HTTPStatusCode: http.StatusBadRequest,
		}
	case multipartqueue.PartChecksumMismatch:
		return s3err.APIError{
			Code:           "BadDigest",
			Description:    fmt.Sprintf("The %v you specified for part %v did not match what we received.", strings.ToLower(string(e.Algorithm)), e.PartNumber),
			HTTPStatusCode: http.StatusBadRequest,
		}
	}
	return s3err.GetInvalidPartErr(e.UploadID, e.PartNumber, e.ETag)
}

// uploadBodies reads a completing upload's bytes back, for the parts a
// Complete hashes again.
type uploadBodies struct{ b *Backend }

func (u uploadBodies) BodyOpener(ctx context.Context, space did.DID, blobs msbucket.BlobIndex, plaintext bool, start, end int64) (msbucket.BlobRangeOpener, error) {
	return u.b.bodyOpener(ctx, space, blobs, plaintext, start, end)
}

func (u uploadBodies) PlaintextLen(ctx context.Context, space did.DID, digest mh.Multihash, storedSize int64) (int64, error) {
	return u.b.blobPlaintextLen(ctx, space, digest, storedSize)
}
