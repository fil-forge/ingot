package s3frontend

import (
	"context"
	"errors"
	"net/http"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"

	"github.com/fil-forge/ingot/registry"
)

// setBucket changes bucket with set, on the bucket's subject: a bucket
// deleted and recreated under the name since the request resolved it is not
// the bucket the request was for.
func (b *Backend) setBucket(ctx context.Context, bucket string, set func(ctx context.Context, space did.DID, name string) error) error {
	st, err := b.reg.Get(ctx, bucket)
	if errors.Is(err, registry.ErrNotFound) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return err
	}
	if err := set(ctx, st.Space, bucket); err != nil {
		return mapBucketError(err, "set bucket")
	}
	return nil
}

// errBucketClosing refuses a write to a bucket a DeleteBucket has closed.
var errBucketClosing = s3err.APIError{
	Code:           "OperationAborted",
	Description:    "A conflicting conditional operation is currently in progress against this resource. Please try again.",
	HTTPStatusCode: http.StatusConflict,
}
