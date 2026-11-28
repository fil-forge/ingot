package s3frontend

import (
	"context"
	"maps"
	"testing"

	"github.com/fil-forge/versitygw/s3err"
	"github.com/stretchr/testify/require"
)

// Bucket-tagging tests over the refindex_test.go harness, covering
// docs/s3-object-tagging.md §9: the not-found sentinel for a bucket without
// a tag set, whole-set replacement, the delete round-trip, and the missing
// bucket outranking both.

func TestBucketTagging_RoundTrip(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	// No tag set yet: the 404 sentinel, not an empty map.
	_, err := b.GetBucketTagging(ctx, "bk")
	require.Error(t, err, "GetBucketTagging on an untagged bucket")
	wantAPIErr(t, err, s3err.ErrBucketTaggingNotFound)

	tags := map[string]string{"env": "prod", "team": "storage"}
	require.NoError(t, b.PutBucketTagging(ctx, "bk", tags), "PutBucketTagging")
	got, err := b.GetBucketTagging(ctx, "bk")
	require.NoError(t, err, "GetBucketTagging")
	require.True(t, maps.Equal(got, tags), "tags = %v, want %v", got, tags)

	// A second Put replaces the whole set rather than merging into it.
	replacement := map[string]string{"env": "staging"}
	require.NoError(t, b.PutBucketTagging(ctx, "bk", replacement), "PutBucketTagging (replace)")
	got, err = b.GetBucketTagging(ctx, "bk")
	require.NoError(t, err, "GetBucketTagging (after replace)")
	require.True(t, maps.Equal(got, replacement), "tags = %v, want %v", got, replacement)

	// Delete restores the unset state, and is idempotent from there.
	require.NoError(t, b.DeleteBucketTagging(ctx, "bk"), "DeleteBucketTagging")
	_, err = b.GetBucketTagging(ctx, "bk")
	require.Error(t, err, "GetBucketTagging after delete")
	wantAPIErr(t, err, s3err.ErrBucketTaggingNotFound)
	require.NoError(t, b.DeleteBucketTagging(ctx, "bk"), "DeleteBucketTagging (second)")
}

func TestBucketTagging_EmptySetClears(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	require.NoError(t, b.PutBucketTagging(ctx, "bk", map[string]string{"env": "prod"}), "PutBucketTagging")
	require.NoError(t, b.PutBucketTagging(ctx, "bk", map[string]string{}), "PutBucketTagging (empty)")
	_, err := b.GetBucketTagging(ctx, "bk")
	require.Error(t, err, "GetBucketTagging after an empty Put")
	wantAPIErr(t, err, s3err.ErrBucketTaggingNotFound)
}

func TestBucketTagging_MissingBucket(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	_, err := b.GetBucketTagging(ctx, "nope")
	require.Error(t, err, "GetBucketTagging on a missing bucket")
	wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	err = b.PutBucketTagging(ctx, "nope", map[string]string{"env": "prod"})
	require.Error(t, err, "PutBucketTagging on a missing bucket")
	wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	err = b.DeleteBucketTagging(ctx, "nope")
	require.Error(t, err, "DeleteBucketTagging on a missing bucket")
	wantAPIErr(t, err, s3err.ErrNoSuchBucket)
}
