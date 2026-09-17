package s3frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fil-forge/versitygw/s3err"

	"github.com/fil-forge/ingot/registry"
)

// This file implements S3 bucket tagging (docs/s3-object-tagging.md §9): the
// three bucket-level methods over the registry's bucket row, beside the
// object-lock configuration in bucket.go. A tag set is bucket metadata, not
// catalog content, so it never touches the MST. versitygw's controller owns
// validation and rendering — utils.ParseTagging with TagLimitBucket caps the
// set at 50 tags and checks key/value lengths, characters and duplicates —
// and hands the backend a clean map.
//
// Unlike object tagging, absence here is a 404: a bucket with no tag set
// answers NoSuchTagSet, which is what DeleteBucketTagging restores.

// GetBucketTagging returns the bucket's tag set, or ErrBucketTaggingNotFound
// when it carries none.
func (b *Backend) GetBucketTagging(ctx context.Context, bucket string) (map[string]string, error) {
	st, err := b.reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, err
	}
	if len(st.BucketTagging) == 0 {
		return nil, s3err.GetAPIError(s3err.ErrBucketTaggingNotFound)
	}
	var tags map[string]string
	if err := json.Unmarshal(st.BucketTagging, &tags); err != nil {
		return nil, fmt.Errorf("s3frontend: get bucket tagging %q: %w", bucket, err)
	}
	// A stored empty object reads as absent too — PutBucketTagging never
	// writes one, but a row that carries it answers like a cleared bucket
	// rather than rendering an empty TagSet.
	if len(tags) == 0 {
		return nil, s3err.GetAPIError(s3err.ErrBucketTaggingNotFound)
	}
	return tags, nil
}

// PutBucketTagging replaces the bucket's tag set. An empty set stores
// nothing: like DeleteBucketTagging it leaves the bucket without a tag set,
// so the next read is the not-found sentinel rather than an empty TagSet.
func (b *Backend) PutBucketTagging(ctx context.Context, bucket string, tags map[string]string) error {
	var doc []byte
	if len(tags) > 0 {
		var err error
		if doc, err = json.Marshal(tags); err != nil {
			return fmt.Errorf("s3frontend: put bucket tagging %q: %w", bucket, err)
		}
	}
	return b.setBucketTagging(ctx, bucket, doc)
}

// DeleteBucketTagging clears the bucket's tag set. Idempotent: a bucket
// without one is a success no-op.
func (b *Backend) DeleteBucketTagging(ctx context.Context, bucket string) error {
	return b.setBucketTagging(ctx, bucket, nil)
}

// setBucketTagging stores doc as the bucket's tag set. The registry reports
// a missing row itself, so both writers reach the S3 sentinel without a
// separate existence check.
func (b *Backend) setBucketTagging(ctx context.Context, bucket string, doc []byte) error {
	if err := b.reg.SetBucketTagging(ctx, bucket, doc); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return fmt.Errorf("s3frontend: set bucket tagging %q: %w", bucket, err)
	}
	return nil
}
