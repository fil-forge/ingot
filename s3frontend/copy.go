package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3api/utils"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
)

// CopyObject copies an object as a metadata-only operation under dedup: it
// resolves the source manifest — the current version, or the one named by the
// copy-source `?versionId` — and writes a new destination version pinning the
// SAME body blobs (same digests), adding a reference-index claim per digest. No
// bytes move and no Forge upload happens — the blobs already exist. Honors
// MetadataDirective (COPY = inherit source metadata; REPLACE = take it from the
// request) and the x-amz-copy-source-if-* preconditions, and supports a
// cross-bucket source in the same space.
func (b *Backend) CopyObject(ctx context.Context, input s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil || input.CopySource == nil {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	srcBucket, srcKey, srcVersionID, err := backend.ParseCopySource(*input.CopySource)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	dstBucket, dstKey := *input.Bucket, *input.Key
	if !mst.IsValidKey(dstKey) {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}

	replace := input.MetadataDirective == types.MetadataDirectiveReplace
	// Copy to self is only legal when the metadata is being replaced — unless
	// the source names an older version (restoring a version onto its own key).
	if srcBucket == dstBucket && srcKey == dstKey && !replace && srcVersionID == "" {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidCopyDest)
	}

	// Destination bucket must exist.
	bucketState, err := b.reg.Get(ctx, dstBucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.CopyObjectOutput{}, fmt.Errorf("s3frontend: copy: %w", err)
	}

	// Resolve the source version (NoSuchBucket / NoSuchKey / NoSuchVersion /
	// InvalidArgument map from resolution). A delete marker cannot be a copy
	// source: the current-marker case is a missing key; naming a marker's
	// versionId is an invalid request (docs/s3-versioning.md §6.2).
	srcRv, err := b.resolveVersion(ctx, srcBucket, srcKey, srcVersionID)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	srcMf := srcRv.mf
	if srcMf.DeleteMarker {
		if srcVersionID == "" {
			return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	// A copy-source versionId naming the CURRENT version is still an illegal
	// self-copy without metadata replacement; only restoring a noncurrent
	// version is exempt from the check at the top.
	if srcBucket == dstBucket && srcKey == dstKey && !replace && srcVersionID != "" && srcRv.isLatest {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidCopyDest)
	}
	if err := backend.EvaluatePreconditions(etagOf(srcMf), time.Unix(srcMf.Created, 0), backend.PreConditions{
		IfMatch:       input.CopySourceIfMatch,
		IfNoneMatch:   input.CopySourceIfNoneMatch,
		IfModSince:    input.CopySourceIfModifiedSince,
		IfUnmodeSince: input.CopySourceIfUnmodifiedSince,
	}); err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	// Destination checksum: same bytes → the source's checksum (and type)
	// carries over. A request naming a DIFFERENT x-amz-checksum-algorithm
	// replaces it: the shared body streams through the new algorithm once and
	// the result is a full-object value — the sole per-object checksum, never
	// accumulated alongside the source's.
	ckAlgo, ckVal, ckType := srcMf.ChecksumAlgorithm, srcMf.Checksum, srcMf.ChecksumType
	if ckVal != "" && ckType == "" {
		ckType = string(types.ChecksumTypeFullObject)
	}
	if reqAlgo := input.ChecksumAlgorithm; reqAlgo != "" && string(reqAlgo) != srcMf.ChecksumAlgorithm {
		ht, err := hashTypeForAlgo(reqAlgo)
		if err != nil {
			return s3response.CopyObjectOutput{}, err
		}
		rc := msbucket.OpenBody(ctx, b.read, srcRv.st.Space, srcMf.Body)
		defer rc.Close()
		hr, err := utils.NewHashReader(rc, "", ht)
		if err != nil {
			return s3response.CopyObjectOutput{}, fmt.Errorf("s3frontend: copy checksum reader: %w", err)
		}
		if _, err := io.Copy(io.Discard, hr); err != nil {
			return s3response.CopyObjectOutput{}, fmt.Errorf("s3frontend: copy checksum: %w", err)
		}
		ckAlgo, ckVal, ckType = string(reqAlgo), hr.Sum(), string(types.ChecksumTypeFullObject)
	}

	// Destination manifest: the SAME body (size/sha/md5/blobs) and ETag, since
	// the content is identical. Metadata per the directive.
	dstMf := &msbucket.ObjectManifest{
		Key:               dstKey,
		Created:           time.Now().Unix(),
		Body:              srcMf.Body,
		ETag:              srcMf.ETag,
		ChecksumAlgorithm: ckAlgo,
		Checksum:          ckVal,
		ChecksumType:      ckType,
	}
	if replace {
		ct := backend.GetStringFromPtr(input.ContentType)
		if ct == "" {
			ct = "application/octet-stream"
		}
		dstMf.ContentType = ct
		dstMf.ContentEncoding = backend.GetStringFromPtr(input.ContentEncoding)
		dstMf.ContentDisposition = backend.GetStringFromPtr(input.ContentDisposition)
		dstMf.ContentLanguage = backend.GetStringFromPtr(input.ContentLanguage)
		dstMf.CacheControl = backend.GetStringFromPtr(input.CacheControl)
		dstMf.Expires = backend.GetStringFromPtr(input.Expires)
		dstMf.WebsiteRedirectLocation = backend.GetStringFromPtr(input.WebsiteRedirectLocation)
		dstMf.Metadata = input.Metadata
	} else {
		dstMf.ContentType = srcMf.ContentType
		dstMf.ContentEncoding = srcMf.ContentEncoding
		dstMf.ContentDisposition = srcMf.ContentDisposition
		dstMf.ContentLanguage = srcMf.ContentLanguage
		dstMf.CacheControl = srcMf.CacheControl
		dstMf.Expires = srcMf.Expires
		// WebsiteRedirectLocation is intentionally NOT inherited on a
		// metadata-COPY: S3 drops it unless the copy uses MetadataDirective=REPLACE
		// and supplies a new value (see the REPLACE branch above).
		dstMf.Metadata = srcMf.Metadata
	}

	// Commit to the destination via the write rule: splice + reference index.
	// The new claims use the DESTINATION bucket/space; the same digests gain
	// another reference.
	node, effState, err := b.commitVersion(ctx, bucketState, dstKey, dstMf, nil)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	lastMod := time.Unix(dstMf.Created, 0)
	etag := etagOf(dstMf)
	result := &s3response.CopyObjectResult{
		ETag:         &etag,
		LastModified: &lastMod,
	}
	result.ChecksumCRC32, result.ChecksumCRC32C, result.ChecksumSHA1, result.ChecksumSHA256, result.ChecksumCRC64NVME, result.ChecksumSHA512, result.ChecksumMD5, result.ChecksumXXHASH64, result.ChecksumXXHASH3, result.ChecksumXXHASH128, result.ChecksumType = checksumFields(dstMf.ChecksumAlgorithm, dstMf.Checksum, dstMf.ChecksumType)
	out := s3response.CopyObjectOutput{
		CopyObjectResult: result,
	}
	// Version ids in the response, per each side's bucket state (§4.3).
	if srcRv.versioned() {
		out.CopySourceVersionId = &srcRv.node.VersionID
	}
	if effState.Configured() {
		out.VersionId = &node.VersionID
	}
	return out, nil
}
