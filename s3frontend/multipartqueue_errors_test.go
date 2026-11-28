package s3frontend

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/queue/multipartqueue"
	"github.com/fil-forge/ingot/registry"
)

// Each error an upload's change fails with answers its S3 error, and a
// commit's or a bucket's answers as mapBucketError has it.
func TestMapUploadError(t *testing.T) {
	part := func(kind string) *multipartqueue.PartError {
		return &multipartqueue.PartError{Kind: kind, UploadID: "u", PartNumber: 2, ETag: `"e"`, Arg: "x", Algorithm: types.ChecksumAlgorithmSha256}
	}
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"no such upload", multipartqueue.ErrNoSuchUpload, s3err.GetAPIError(s3err.ErrNoSuchUpload)},
		{"busy", multipartqueue.ErrBusy, s3err.GetAPIError(s3err.ErrSlowDown)},
		{"in progress", multipartqueue.ErrInProgress, errCompleteInProgress},
		{"unsupported condition", multipartqueue.ErrConditionUnsupported, s3err.GetAPIError(s3err.ErrNotImplemented)},
		{"no such key", multipartqueue.ErrNoSuchKey, s3err.GetAPIError(s3err.ErrNoSuchKey)},
		{"precondition", multipartqueue.ErrPreconditionFailed, s3err.GetAPIError(s3err.ErrPreconditionFailed)},
		{"invalid part", multipartqueue.ErrInvalidPart, s3err.GetAPIError(s3err.ErrInvalidPart)},
		{"malformed part list", multipartqueue.ErrMalformedPartList, s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"part order", multipartqueue.ErrInvalidPartOrder, s3err.GetAPIError(s3err.ErrInvalidPartOrder)},
		{"part too small", multipartqueue.ErrPartTooSmall, s3err.GetAPIError(s3err.ErrEntityTooSmall)},
		{"too many blobs", multipartqueue.ErrTooManyBlobs, s3err.GetAPIError(s3err.ErrEntityTooLarge)},
		{"lock headers without lock", bucketqueue.ErrNoObjectLock, s3err.GetAPIError(s3err.ErrMissingObjectLockConfigurationNoSpaces)},
		{"checksum type", &multipartqueue.ChecksumTypeError{Type: types.ChecksumTypeComposite},
			s3err.GetChecksumTypeMismatchOnMpErr(types.ChecksumTypeComposite)},
		{"no checksum type", &multipartqueue.ChecksumTypeError{}, s3err.GetChecksumTypeMismatchOnMpErr("null")},
		{"negative size", &multipartqueue.ObjectSizeError{Stated: -1, Total: 5}, s3err.GetNegatvieMpObjectSizeErr(-1)},
		{"wrong size", &multipartqueue.ObjectSizeError{Stated: 4, Total: 5}, s3err.GetIncorrectMpObjectSizeErr(5, 4)},
		{"final checksum", &multipartqueue.ChecksumError{Algorithm: types.ChecksumAlgorithmCrc32}, s3err.GetChecksumBadDigestErr(types.ChecksumAlgorithmCrc32)},
		{"part layout", &multipartqueue.PartLayoutError{Need: 7, Budget: 3}, errPartLayoutOverBudget(7, 3)},
		{"part number", part(multipartqueue.PartNumberInvalid), s3err.GetInvalidArgumentErr(s3err.InvalidArgCompleteMpPartNumber, "x")},
		{"part checksum", part(multipartqueue.PartChecksumInvalid), s3err.GetInvalidArgumentErr(s3err.InvalidArgChecksumPart, "x")},
		{"part not matched", part(multipartqueue.PartNotMatched), s3err.GetInvalidPartErr("u", 2, `"e"`)},
		{"commit precondition", &bucketqueue.ChangeError{Kind: bucketqueue.KindPreconditionFailed},
			s3err.GetAPIError(s3err.ErrPreconditionFailed)},
		{"commit without lock", &bucketqueue.ChangeError{Kind: bucketqueue.KindNoObjectLock},
			s3err.GetAPIError(s3err.ErrMissingObjectLockConfiguration)},
		{"bucket gone", bucketop.ErrBucketNotFound, s3err.GetAPIError(s3err.ErrNoSuchBucket)},
		{"bucket closing", fmt.Errorf("admit: %w", bucketqueue.ErrBucketClosing), errBucketClosing},
		{"reference expired", registry.ErrReferenceExpired, s3err.GetAPIError(s3err.ErrSlowDown)},
	} {
		got := mapUploadError(tc.err, "complete")
		assert.Equal(t, tc.want, got, "%s", tc.name)
	}
	for kind, code := range map[string]string{
		multipartqueue.PartChecksumMissing:  "InvalidRequest",
		multipartqueue.PartChecksumMismatch: "BadDigest",
	} {
		var api s3err.APIError
		ok := errors.As(mapUploadError(part(kind), "complete"), &api)
		assert.True(t, ok && api.Code == code, "%s answered %v, want %s", kind, api, code)
	}
	err := mapUploadError(nil, "complete")
	require.NoError(t, err, "no error answered %v", err)
	got, cause := mapUploadError(errors.New("disk full"), "complete"), "disk full"
	require.Error(t, got, "a storage error answered %v, want it wrapped", got)
	require.Equal(t, "s3frontend: complete: "+cause, got.Error(), "a storage error answered %v", got)
}
