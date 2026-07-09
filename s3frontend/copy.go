package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
)

// CopyObject copies an object as a metadata-only operation under dedup: it
// resolves the source manifest and writes a new destination manifest pinning the
// SAME body blobs (same digests), adding a reference-index claim per digest. No
// bytes move and no Forge upload happens — the blobs already exist. Honors
// MetadataDirective (COPY = inherit source metadata; REPLACE = take it from the
// request) and the x-amz-copy-source-if-* preconditions, and supports a
// cross-bucket source in the same space.
func (b *Backend) CopyObject(ctx context.Context, input s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil || input.CopySource == nil {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	srcBucket, srcKey, _, err := backend.ParseCopySource(*input.CopySource)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	dstBucket, dstKey := *input.Bucket, *input.Key
	if !mst.IsValidKey(dstKey) {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}

	replace := input.MetadataDirective == types.MetadataDirectiveReplace
	// Copy to self is only legal when the metadata is being replaced.
	if srcBucket == dstBucket && srcKey == dstKey && !replace {
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

	// Resolve the source manifest (NoSuchBucket / NoSuchKey map from lookup).
	srcMf, _, err := b.lookupManifest(ctx, srcBucket, srcKey)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	if err := backend.EvaluatePreconditions(etagOf(srcMf), time.Unix(srcMf.Created, 0), backend.PreConditions{
		IfMatch:       input.CopySourceIfMatch,
		IfNoneMatch:   input.CopySourceIfNoneMatch,
		IfModSince:    input.CopySourceIfModifiedSince,
		IfUnmodeSince: input.CopySourceIfUnmodifiedSince,
	}); err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	// Destination manifest: the SAME body (size/sha/md5/blobs) and ETag, since
	// the content is identical. Metadata per the directive.
	dstMf := &msbucket.ObjectManifest{
		Key:     dstKey,
		Created: time.Now().Unix(),
		Body:    srcMf.Body,
		ETag:    srcMf.ETag,
		// Same content → same additional checksum, regardless of directive.
		ChecksumAlgorithm: srcMf.ChecksumAlgorithm,
		Checksum:          srcMf.Checksum,
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
		dstMf.Metadata = input.Metadata
	} else {
		dstMf.ContentType = srcMf.ContentType
		dstMf.ContentEncoding = srcMf.ContentEncoding
		dstMf.ContentDisposition = srcMf.ContentDisposition
		dstMf.ContentLanguage = srcMf.ContentLanguage
		dstMf.CacheControl = srcMf.CacheControl
		dstMf.Expires = srcMf.Expires
		dstMf.Metadata = srcMf.Metadata
	}

	// Commit to the destination: splice + reference index. The new claims use
	// the DESTINATION bucket/space; the same digests gain another reference.
	if err := b.commitManifest(ctx, bucketState, dstKey, dstMf, bodyDigests(dstMf.Body)); err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	lastMod := time.Unix(dstMf.Created, 0)
	etag := etagOf(dstMf)
	return s3response.CopyObjectOutput{
		CopyObjectResult: &s3response.CopyObjectResult{
			ETag:         &etag,
			LastModified: &lastMod,
		},
	}, nil
}
