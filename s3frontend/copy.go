package s3frontend

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3api/utils"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
)

// CopyObject copies an object. Within one space it is a metadata-only
// operation under dedup: it resolves the source manifest — the current version,
// or the one named by the copy-source `?versionId` — and writes a new
// destination version pinning the SAME body blobs (same digests), adding a
// reference-index claim per digest; no bytes move and no Forge upload happens.
// Across spaces (every bucket has its own) the blobs cannot be shared, because
// each blob's key is wrapped bound to (space, digest): the source's plaintext
// instead streams through the decrypting read path into new blobs under the
// destination's space, exactly as a PUT of those bytes would, and the copy has
// its own digests and claims. Either way the copy's ETag is the md5 of its
// bytes (so a multipart source's "-N" ETag is not carried over, as on S3) and
// its checksum is a full-object value. Honors MetadataDirective (COPY = inherit
// source metadata; REPLACE = take it from the request) and the
// x-amz-copy-source-if-* preconditions. The source bucket must belong to the
// destination's tenant (copySourceBucket), backing up hilt's own decision on
// the source.
func (b *Backend) CopyObject(ctx context.Context, input s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil || input.CopySource == nil {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	srcBucket, srcKey, srcVersionID, err := backend.ParseCopySource(*input.CopySource)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	dstBucket, dstKey := *input.Bucket, *input.Key
	if err := objectKeyError(dstKey); err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	if unsupportedObjectACL(input.ACL, input.GrantFullControl, input.GrantRead, input.GrantReadACP, input.GrantWriteACP) {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
	}
	if req, ok := reqscope.Request(ctx); ok && requestsServerSideEncryption(req.Headers) {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
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

	// x-amz-object-lock-* headers stamp the DESTINATION version; lock state
	// is never inherited from the source (docs/s3-object-lock.md §7).
	initState, err := lockStateFromHeaders(bucketState, input.ObjectLockMode, input.ObjectLockRetainUntilDate, input.ObjectLockLegalHoldStatus)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	// Destination tags per x-amz-tagging-directive (the controller defaults
	// an absent header to COPY, so only COPY or REPLACE arrives): REPLACE
	// parses the request's own header, failing before any further work; COPY
	// inherits the source version's tags once it resolves below
	// (docs/s3-object-tagging.md §4).
	var dstTags map[string]string
	if input.TaggingDirective == types.TaggingDirectiveReplace {
		if dstTags, err = backend.ParseObjectTags(backend.GetStringFromPtr(input.Tagging)); err != nil {
			return s3response.CopyObjectOutput{}, err
		}
	}

	// Vet the source bucket, then resolve the source version (NoSuchKey /
	// NoSuchVersion / InvalidArgument map from resolution). A delete marker
	// cannot be a copy source: the current-marker case is a missing key;
	// naming a marker's versionId is an invalid request
	// (docs/s3-versioning.md §6.2).
	srcSt, err := b.copySourceBucket(ctx, bucketState, srcBucket)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	if err := expectedSourceOwner(input.ExpectedSourceBucketOwner, srcSt); err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	srcRv, err := b.resolveVersionIn(ctx, srcSt, srcKey, srcVersionID)
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
	// S3 copies at most 5 GiB in one CopyObject; a larger object is copied as
	// multipart parts. Checked before any bytes are read, and on the pinned
	// path too, for parity.
	if srcMf.Body.Size > maxCopySize {
		return s3response.CopyObjectOutput{}, s3err.GetCopySourceObjectTooLargeErr(maxCopySize)
	}
	// A copy-source versionId naming the CURRENT version is still an illegal
	// self-copy without metadata replacement; only restoring a noncurrent
	// version is exempt from the check at the top.
	if srcBucket == dstBucket && srcKey == dstKey && !replace && srcVersionID != "" && srcRv.isLatest {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidCopyDest)
	}
	if err := evaluateCopySourcePreconditions(etagOf(srcMf), time.Unix(srcMf.Created, 0), backend.PreConditions{
		IfMatch:       input.CopySourceIfMatch,
		IfNoneMatch:   input.CopySourceIfNoneMatch,
		IfModSince:    input.CopySourceIfModifiedSince,
		IfUnmodeSince: input.CopySourceIfUnmodifiedSince,
	}); err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	if input.TaggingDirective == types.TaggingDirectiveCopy {
		srcVs, err := b.versionStateOf(ctx, srcRv)
		if err != nil {
			return s3response.CopyObjectOutput{}, err
		}
		if srcVs != nil {
			dstTags = srcVs.Tags
		}
	}

	// The copy's body, ETag and checksum. The checksum algorithm is the one the
	// request names, else the source's, else the CRC64NVME every stored object
	// carries; the value is always a full-object one for the copy's bytes, never
	// a composite carried over. Three shapes:
	//
	//   - same space, single-part source, same algorithm: pin the source's body
	//     and ETag verbatim, no read;
	//   - same space, but a multipart source (its ETag is md5-of-md5s + "-N",
	//     its checksum possibly composite) or a different algorithm requested:
	//     pin the body, stream it once to compute the md5 ETag and checksum;
	//   - another space: stream it once through ingestBody into new blobs
	//     under the destination space; the ETag and checksum come from that
	//     pass.
	//
	// The pinned body drops its part geometry: the copy is a single-part
	// object, as on S3.
	crossSpace := srcRv.st.Space != bucketState.Space
	multipartSrc := len(srcMf.Body.PartSizes) > 0 || isMultipartETag(srcMf.ETag)
	ckAlgo := srcMf.ChecksumAlgorithm
	if input.ChecksumAlgorithm != "" {
		ckAlgo = string(input.ChecksumAlgorithm)
	}
	ckVal, ckType := srcMf.Checksum, srcMf.ChecksumType
	if ckVal != "" && ckType == "" {
		ckType = string(types.ChecksumTypeFullObject)
	}
	body, etag := srcMf.Body, srcMf.ETag
	body.PartSizes, body.PartChecksums = nil, nil
	// A source without any checksum (a manifest from before every object
	// carried one) also takes the pass, so the copy gets the default.
	if crossSpace || multipartSrc || ckAlgo == "" || ckAlgo != srcMf.ChecksumAlgorithm {
		if ckAlgo == "" {
			ckAlgo = string(types.ChecksumAlgorithmCrc64nvme)
		}
		ht, err := hashTypeForAlgo(types.ChecksumAlgorithm(ckAlgo))
		if err != nil {
			return s3response.CopyObjectOutput{}, err
		}
		rc, err := b.openCopySource(ctx, srcRv, 0, srcMf.Body.Size-1)
		if err != nil {
			return s3response.CopyObjectOutput{}, err
		}
		defer rc.Close()
		hr, err := utils.NewHashReader(rc, "", ht)
		if err != nil {
			return s3response.CopyObjectOutput{}, fmt.Errorf("s3frontend: copy checksum reader: %w", err)
		}
		if crossSpace {
			if body, err = b.ingestBody(ctx, bucketState, hr); err != nil {
				var apiErr s3err.APIError
				if errors.As(err, &apiErr) {
					return s3response.CopyObjectOutput{}, apiErr
				}
				return s3response.CopyObjectOutput{}, fmt.Errorf("s3frontend: copy ingest: %w", err)
			}
			etag = hex.EncodeToString(body.MD5)
		} else if multipartSrc {
			// The pinned body keeps its bytes; only the md5 the source never
			// recorded (its ETag is md5-of-md5s) is computed alongside the
			// checksum.
			sum := md5.New()
			if _, err := io.Copy(sum, hr); err != nil {
				return s3response.CopyObjectOutput{}, fmt.Errorf("s3frontend: copy checksum: %w", err)
			}
			etag = hex.EncodeToString(sum.Sum(nil))
		} else {
			// A single-part source's ETag already is the md5; only the newly
			// requested checksum needs the pass.
			if _, err := io.Copy(io.Discard, hr); err != nil {
				return s3response.CopyObjectOutput{}, fmt.Errorf("s3frontend: copy checksum: %w", err)
			}
		}
		ckVal, ckType = hr.Sum(), string(types.ChecksumTypeFullObject)
	}

	// Destination manifest: the body and ETag chosen above, metadata per the
	// directive.
	dstMf := &msbucket.ObjectManifest{
		Key:               dstKey,
		Created:           time.Now().Unix(),
		Body:              body,
		ETag:              etag,
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
		dstMf.ContentEncoding = normalizeContentEncoding(backend.GetStringFromPtr(input.ContentEncoding))
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
	// The claims use the DESTINATION bucket/space: for a pinned body the same
	// digests gain another reference, for a re-ingested one its new digests
	// gain their first.
	node, effState, err := b.commitVersion(ctx, bucketState, dstKey, dstMf, applyTagsIfPresent(initState, dstTags), nil)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	lastMod := time.Unix(dstMf.Created, 0)
	quotedETag := etagOf(dstMf)
	result := &s3response.CopyObjectResult{
		ETag:         &quotedETag,
		LastModified: &lastMod,
	}
	result.ChecksumCRC32, result.ChecksumCRC32C, result.ChecksumSHA1, result.ChecksumSHA256, result.ChecksumCRC64NVME, result.ChecksumSHA512, result.ChecksumMD5, result.ChecksumXXHASH64, result.ChecksumXXHASH3, result.ChecksumXXHASH128, result.ChecksumType = checksumFields(dstMf.ChecksumAlgorithm, dstMf.Checksum, dstMf.ChecksumType)
	out := s3response.CopyObjectOutput{
		CopyObjectResult: result,
	}
	// Version ids in the response, per each side's bucket state (§4.3). Only an
	// enabled destination echoes the new version id; a suspended destination
	// stores the "null" version but omits it from the response, matching AWS.
	if srcRv.versioned() {
		out.CopySourceVersionId = &srcRv.node.VersionID
	}
	if effState == registry.VersioningEnabled {
		out.VersionId = &node.VersionID
	}
	return out, nil
}

// multipartETag is the ETag shape a completed multipart upload records: the hex
// md5 of the parts' md5s and the part count. Manifests written before part
// geometry was recorded carry only this to say they were assembled from parts.
var multipartETag = regexp.MustCompile(`^"?[0-9a-f]{32}-[0-9]+"?$`)

// isMultipartETag reports whether etag has the multipart shape.
func isMultipartETag(etag string) bool { return multipartETag.MatchString(etag) }

// copySourceBucket resolves the copy source's bucket and requires it to belong
// to the destination bucket's tenant. hilt authorizes a copy as a write to the
// destination and never reads x-amz-copy-source, so the source bucket's tenant
// is checked here, before any key lookup: a foreign source is AccessDenied
// whether or not the key exists, which is what S3 returns for another
// account's bucket. The check compares the two bucket rows, so it needs no
// tenant on the request. A row whose owner was never
// recorded (registry.UnknownTenant) matches no tenant, its own sentinel
// included: two such rows prove nothing about each other. UploadPartCopy
// shares this rule.
func (b *Backend) copySourceBucket(ctx context.Context, dst *registry.State, srcBucket string) (*registry.State, error) {
	// A copy within one bucket reads the bucket it writes; no lookup or
	// comparison is needed.
	if srcBucket == dst.Name {
		return dst, nil
	}
	src, err := b.reg.Get(ctx, srcBucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, fmt.Errorf("s3frontend: copy source bucket: %w", err)
	}
	known := src.Tenant.Defined() && src.Tenant != registry.UnknownTenant
	if !known || src.Tenant != dst.Tenant {
		return nil, s3err.GetAPIError(s3err.ErrAccessDenied)
	}
	return src, nil
}
