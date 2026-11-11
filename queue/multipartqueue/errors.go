package multipartqueue

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// The errors an upload's changes fail with. A Complete also fails with the
// bucket queue's errors: its commit's, and bucketop.ErrBucketNotFound for a
// bucket that is gone.
var (
	// ErrNoSuchUpload is an upload that does not exist, belongs to another
	// bucket or key, or is no longer open.
	ErrNoSuchUpload = errors.New("no such upload")
	// ErrBusy refuses a part that waited too long behind the upload's other
	// changes.
	ErrBusy = errors.New("the upload is busy")
	// ErrInProgress refuses a Complete that waited too long behind the
	// upload's other changes, another Complete included, or for its parts'
	// accepts. The upload stays as it was.
	ErrInProgress = errors.New("the upload is being completed")
	// ErrConditionUnsupported refuses a Complete with an If-None-Match other
	// than "*", or with both If-None-Match and If-Match.
	ErrConditionUnsupported = errors.New("unsupported condition")
	// ErrNoSuchKey refuses an If-Match Complete of a key with no object.
	ErrNoSuchKey = errors.New("no such key")
	// ErrPreconditionFailed refuses a Complete whose If-Match or
	// If-None-Match fails against the key's current object.
	ErrPreconditionFailed = errors.New("precondition failed")
	// ErrInvalidPart refuses a Complete naming no parts, or a part the
	// upload does not hold as named.
	ErrInvalidPart = errors.New("invalid part")
	// ErrMalformedPartList refuses a Complete listing a part without its
	// number or its ETag.
	ErrMalformedPartList = errors.New("malformed part list")
	// ErrInvalidPartOrder refuses a Complete whose parts are not in
	// ascending order.
	ErrInvalidPartOrder = errors.New("parts out of order")
	// ErrPartTooSmall refuses a Complete with a part, other than the last,
	// under S3's minimum part size.
	ErrPartTooSmall = errors.New("part too small")
	// ErrTooManyBlobs refuses a Complete whose object would hold more blobs
	// than a manifest does.
	ErrTooManyBlobs = errors.New("too many blobs for one object")
)

// ChecksumTypeError refuses a Complete whose checksum type is not the one the
// upload was created with.
type ChecksumTypeError struct {
	// Type is the upload's checksum type; empty for none.
	Type types.ChecksumType
}

func (e *ChecksumTypeError) Error() string {
	return fmt.Sprintf("the upload's checksum type is %q", e.Type)
}

// ObjectSizeError refuses a Complete whose stated object size is negative, or
// is not the size of the parts it lists.
type ObjectSizeError struct {
	// Stated is the size the request stated.
	Stated int64
	// Total is the size of the listed parts.
	Total int64
}

func (e *ObjectSizeError) Error() string {
	return fmt.Sprintf("stated object size %d, parts total %d", e.Stated, e.Total)
}

// ChecksumError refuses a Complete whose stated final checksum is not the
// one its parts make.
type ChecksumError struct {
	// Algorithm is the upload's checksum algorithm.
	Algorithm types.ChecksumAlgorithm
}

func (e *ChecksumError) Error() string {
	return fmt.Sprintf("the %s checksum does not match the parts'", e.Algorithm)
}

// PartLayoutError refuses a Complete whose parts would need more re-reading,
// to compute the object's digest, than the budget allows: variable-size
// parts uploaded out of order. The upload stays open.
type PartLayoutError struct {
	// Need is the bytes the re-read would take.
	Need int64
	// Budget is the most the Complete allows itself.
	Budget int64
}

func (e *PartLayoutError) Error() string {
	return fmt.Sprintf("re-hash of %d bytes over the budget of %d", e.Need, e.Budget)
}

// The kinds of PartError.
const (
	// PartNumberInvalid is a part number below 1; Arg is the number.
	PartNumberInvalid = "part-number-invalid"
	// PartChecksumInvalid is a part listed with more than one checksum, or
	// with a checksum that is not one; Arg is the offending value.
	PartChecksumInvalid = "part-checksum-invalid"
	// PartChecksumMissing is a part listed without the checksum the upload
	// declared.
	PartChecksumMissing = "part-checksum-missing"
	// PartChecksumMismatch is a part listed with a checksum of another
	// algorithm than the upload's.
	PartChecksumMismatch = "part-checksum-mismatch"
	// PartNotMatched is a part listed with a checksum that is not the
	// recorded part's, or with one the upload declared none for.
	PartNotMatched = "part-not-matched"
)

// PartError refuses a Complete for one of the parts it lists.
type PartError struct {
	// Kind says what is wrong with the part.
	Kind string
	// UploadID is the upload's ID.
	UploadID string
	// PartNumber is the part's number as listed.
	PartNumber int32
	// ETag is the part's ETag as listed.
	ETag string
	// Arg is the listed value a PartNumberInvalid or PartChecksumInvalid
	// names.
	Arg string
	// Algorithm is the checksum algorithm a PartChecksumMissing or
	// PartChecksumMismatch names.
	Algorithm types.ChecksumAlgorithm
}

func (e *PartError) Error() string {
	return fmt.Sprintf("part %d: %s", e.PartNumber, e.Kind)
}
