package s3frontend

import (
	"context"
	"errors"
	"fmt"

	"github.com/fil-forge/versitygw/s3err"

	"github.com/fil-forge/ingot/bucketop"
)

// currentObjectETag resolves the committed ETag of (bucket, key) for evaluating
// put/copy preconditions before ingest. It distinguishes "no such key" (exists
// = false, no error) from real errors.
func (b *Backend) currentObjectETag(ctx context.Context, bucket, key string) (etag string, exists bool, err error) {
	mf, _, err := b.lookupManifest(ctx, bucket, key)
	if err != nil {
		if isNoSuchKey(err) {
			return "", false, nil
		}
		return "", false, err
	}
	return etagOf(mf), true, nil
}

// isNoSuchKey reports whether err is the NoSuchKey API error lookupManifest
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
	var apiErr s3err.APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return fmt.Errorf("s3frontend: %s: %w", op, err)
}
