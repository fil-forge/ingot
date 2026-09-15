package s3frontend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
)

// maxCopySize is S3's ceiling on the bytes one copy request may move: the
// whole object for CopyObject, the range for UploadPartCopy. A larger object
// is copied as multipart parts.
const maxCopySize = 5 << 30

// UploadPartCopy ingests a part whose bytes are a range of an existing object:
// the source's plaintext streams through the decrypting read path into the
// same ingest UploadPart uses, so the part is spooled, parked and recorded
// exactly like an uploaded one, in new blobs under the destination's space.
// Nothing is shared with the source, so the source may live in any bucket of
// the tenant, in any space.
//
// The source is vetted before any of its bytes are read: the bucket must be
// the destination tenant's (copySourceBucket), the version must resolve, the
// range must lie within the object, and the x-amz-copy-source-if-*
// preconditions must hold (every failure a 412, as S3 answers for copies).
// The part checksum is computed over the copied bytes with the session's
// declared algorithm, which for a whole-object copy of a source checksummed
// the same way reproduces the source's value; a session that declared none
// records the internal CRC64NVME and echoes nothing. The part ETag is the hex
// md5 of the copied bytes.
func (b *Backend) UploadPartCopy(ctx context.Context, input *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	if input.Bucket == nil || input.Key == nil || input.UploadId == nil || input.PartNumber == nil || input.CopySource == nil {
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	if req, ok := reqscope.Request(ctx); ok && requestsServerSideEncryption(req.Headers) {
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
	}
	sess, err := b.openSession(ctx, *input.UploadId, input.Bucket, input.Key)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	srcBucket, srcKey, srcVersionID, err := backend.ParseCopySource(*input.CopySource)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	dst, err := b.reg.Get(ctx, sess.Bucket)
	if err != nil {
		return s3response.CopyPartResult{}, fmt.Errorf("s3frontend: upload part copy: destination bucket: %w", err)
	}
	srcSt, err := b.copySourceBucket(ctx, dst, srcBucket)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	if err := expectedSourceOwner(input.ExpectedSourceBucketOwner, srcSt); err != nil {
		return s3response.CopyPartResult{}, err
	}
	srcRv, err := b.resolveVersionIn(ctx, srcSt, srcKey, srcVersionID)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	srcMf := srcRv.mf
	if srcMf.DeleteMarker {
		if srcVersionID == "" {
			return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	start, end, err := parseCopySourceRange(srcMf.Body.Size, backend.GetStringFromPtr(input.CopySourceRange))
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	if end-start+1 > maxCopySize {
		return s3response.CopyPartResult{}, s3err.GetCopySourceObjectTooLargeErr(maxCopySize)
	}
	if err := evaluateCopySourcePreconditions(etagOf(srcMf), time.Unix(srcMf.Created, 0), backend.PreConditions{
		IfMatch:       input.CopySourceIfMatch,
		IfNoneMatch:   input.CopySourceIfNoneMatch,
		IfModSince:    input.CopySourceIfModifiedSince,
		IfUnmodeSince: input.CopySourceIfUnmodifiedSince,
	}); err != nil {
		return s3response.CopyPartResult{}, err
	}

	rc, err := b.openCopySource(ctx, srcRv, start, end)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	defer rc.Close()
	// The session's algorithm is the part's: passing it as the requested one
	// satisfies the negotiation (a COMPOSITE session needs a checksum on every
	// part) with no value to validate, so it is computed over the copied bytes.
	rec, err := b.ingestPart(ctx, sess, int(*input.PartNumber), rc, types.ChecksumAlgorithm(sess.ChecksumAlgorithm), "")
	if err != nil {
		return s3response.CopyPartResult{}, err
	}

	result := s3response.CopyPartResult{ETag: &rec.etag, LastModified: time.Now().UTC()}
	setCopyPartChecksum(&result, rec.echoAlgo, rec.echoSum)
	if srcRv.versioned() {
		result.CopySourceVersionId = srcRv.node.VersionID
	}
	return result, nil
}

// openCopySource opens the plaintext of the resolved source version over the
// inclusive byte range [start, end], through the decrypting read path GetObject
// uses. An empty range (end < start, the only case being an empty object) reads
// nothing.
func (b *Backend) openCopySource(ctx context.Context, src *resolvedVersion, start, end int64) (io.ReadCloser, error) {
	if end < start {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	opener, err := b.bodyOpener(ctx, src.st.Space, src.mf.Body)
	if err != nil {
		return nil, err
	}
	return msbucket.OpenBodyRange(ctx, opener, src.st.Space, src.mf.Body, start, end), nil
}

// copySourceRangeForm is the only shape S3 accepts for x-amz-copy-source-range:
// bytes=first-last, both zero-based inclusive offsets. Open-ended and suffix
// forms, legal on a GET Range, are not.
var copySourceRangeForm = regexp.MustCompile(`^bytes=(\d+)-(\d+)$`)

// errCopySourceRangeBeyondObject is S3's answer for a range that starts past
// the end of the source object (a range that merely ends past it, or starts at
// its size, is the InvalidArgument exceeding-range error).
var errCopySourceRangeBeyondObject = s3err.APIError{
	Code:           "InvalidRequest",
	Description:    "The specified copy range is invalid for the source object size",
	HTTPStatusCode: http.StatusBadRequest,
}

// parseCopySourceRange resolves an x-amz-copy-source-range against the source
// object's size to an inclusive [start, end]. An absent header selects the
// whole object (end = size-1, so -1 for an empty object). The errors follow S3:
// a malformed or reversed range is InvalidArgument naming the header; a start
// past the object is InvalidRequest; a start at the object's size or an end
// past it is the exceeding-range InvalidArgument.
func parseCopySourceRange(size int64, header string) (start, end int64, err error) {
	if header == "" {
		return 0, size - 1, nil
	}
	m := copySourceRangeForm.FindStringSubmatch(header)
	if m == nil {
		return 0, 0, s3err.GetInvalidArgumentErr(s3err.InvalidArgCopySourceRange, header)
	}
	start, err1 := strconv.ParseInt(m[1], 10, 64)
	end, err2 := strconv.ParseInt(m[2], 10, 64)
	if err1 != nil || err2 != nil || end < start {
		return 0, 0, s3err.GetInvalidArgumentErr(s3err.InvalidArgCopySourceRange, header)
	}
	if start > size {
		return 0, 0, errCopySourceRangeBeyondObject
	}
	if start >= size || end >= size {
		return 0, 0, s3err.GetInvalidArgExceedingRange(size)
	}
	return start, end, nil
}

// expectedSourceOwner applies x-amz-source-expected-bucket-owner: ingot models a
// bucket's owner as its tenant, so the header must name the source bucket's
// tenant DID. Absent, it constrains nothing.
func expectedSourceOwner(header *string, src *registry.State) error {
	if want := backend.GetStringFromPtr(header); want != "" && want != src.Tenant.String() {
		return s3err.GetAPIError(s3err.ErrAccessDenied)
	}
	return nil
}
