package s3frontend

import (
	"context"
	"maps"
	"testing"

	"github.com/fil-forge/versitygw/s3err"
)

// Bucket-tagging tests over the refindex_test.go harness, covering
// docs/s3-object-tagging.md §9: the not-found sentinel for a bucket without
// a tag set, whole-set replacement, the delete round-trip, and the missing
// bucket outranking both.

func TestBucketTagging_RoundTrip(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	// No tag set yet: the 404 sentinel, not an empty map.
	if _, err := b.GetBucketTagging(ctx, "bk"); err == nil {
		t.Fatal("GetBucketTagging on an untagged bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrBucketTaggingNotFound)
	}

	tags := map[string]string{"env": "prod", "team": "storage"}
	if err := b.PutBucketTagging(ctx, "bk", tags); err != nil {
		t.Fatalf("PutBucketTagging: %v", err)
	}
	got, err := b.GetBucketTagging(ctx, "bk")
	if err != nil {
		t.Fatalf("GetBucketTagging: %v", err)
	}
	if !maps.Equal(got, tags) {
		t.Fatalf("tags = %v, want %v", got, tags)
	}

	// A second Put replaces the whole set rather than merging into it.
	replacement := map[string]string{"env": "staging"}
	if err := b.PutBucketTagging(ctx, "bk", replacement); err != nil {
		t.Fatalf("PutBucketTagging (replace): %v", err)
	}
	if got, err = b.GetBucketTagging(ctx, "bk"); err != nil {
		t.Fatalf("GetBucketTagging (after replace): %v", err)
	}
	if !maps.Equal(got, replacement) {
		t.Fatalf("tags = %v, want %v", got, replacement)
	}

	// Delete restores the unset state, and is idempotent from there.
	if err := b.DeleteBucketTagging(ctx, "bk"); err != nil {
		t.Fatalf("DeleteBucketTagging: %v", err)
	}
	if _, err := b.GetBucketTagging(ctx, "bk"); err == nil {
		t.Fatal("GetBucketTagging after delete: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrBucketTaggingNotFound)
	}
	if err := b.DeleteBucketTagging(ctx, "bk"); err != nil {
		t.Fatalf("DeleteBucketTagging (second): %v", err)
	}
}

func TestBucketTagging_EmptySetClears(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	if err := b.PutBucketTagging(ctx, "bk", map[string]string{"env": "prod"}); err != nil {
		t.Fatalf("PutBucketTagging: %v", err)
	}
	if err := b.PutBucketTagging(ctx, "bk", map[string]string{}); err != nil {
		t.Fatalf("PutBucketTagging (empty): %v", err)
	}
	if _, err := b.GetBucketTagging(ctx, "bk"); err == nil {
		t.Fatal("GetBucketTagging after an empty Put: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrBucketTaggingNotFound)
	}
}

func TestBucketTagging_MissingBucket(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	if _, err := b.GetBucketTagging(ctx, "nope"); err == nil {
		t.Fatal("GetBucketTagging on a missing bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	}
	if err := b.PutBucketTagging(ctx, "nope", map[string]string{"env": "prod"}); err == nil {
		t.Fatal("PutBucketTagging on a missing bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	}
	if err := b.DeleteBucketTagging(ctx, "nope"); err == nil {
		t.Fatal("DeleteBucketTagging on a missing bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	}
}
