package s3frontend

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func i32(n int32) *int32   { return &n }
func str(s string) *string { return &s }

// TestListObjectsV2_ExactFitNotTruncated locks the look-ahead: a page whose
// element count exactly equals MaxKeys is complete, not truncated, and carries
// no continuation token.
func TestListObjectsV2_ExactFitNotTruncated(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	bucket := "bk"
	for _, k := range []string{"k0", "k1", "k2", "k3", "k4"} {
		putObj(t, b, k, []byte("x"))
	}

	res, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, MaxKeys: i32(5)})
	require.NoError(t, err, "ListObjectsV2")
	require.Len(t, res.Contents, 5, "Contents")
	require.NotNil(t, res.IsTruncated, "IsTruncated")
	require.False(t, *res.IsTruncated, "IsTruncated (exact fit)")
	require.Nil(t, res.NextContinuationToken, "NextContinuationToken")
}

// TestListObjectsV2_GenuineTruncationAndResume confirms a real truncation still
// reports IsTruncated + a token that resumes to exactly the remaining keys.
func TestListObjectsV2_GenuineTruncationAndResume(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	bucket := "bk"
	for _, k := range []string{"k0", "k1", "k2", "k3", "k4"} {
		putObj(t, b, k, []byte("x"))
	}

	res, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, MaxKeys: i32(4)})
	require.NoError(t, err, "ListObjectsV2 page1")
	require.Len(t, res.Contents, 4, "page1: Contents=%d truncated", len(res.Contents))
	require.NotNil(t, res.IsTruncated, "page1: Contents=%d truncated", len(res.Contents))
	require.True(t, *res.IsTruncated, "page1: Contents=%d truncated=%v, want 4/true", len(res.Contents), res.IsTruncated)
	require.NotNil(t, res.NextContinuationToken, "page1 missing NextContinuationToken")

	res2, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucket, MaxKeys: i32(4), ContinuationToken: res.NextContinuationToken,
	})
	require.NoError(t, err, "ListObjectsV2 page2")
	require.Len(t, res2.Contents, 1, "page2 Contents")
	require.Equal(t, "k4", *res2.Contents[0].Key, "page2 Contents")
	require.NotNil(t, res2.IsTruncated, "page2 IsTruncated")
	require.False(t, *res2.IsTruncated, "page2 IsTruncated")
}

// TestListObjectsV2_DelimiterExactFit covers the delimiter case where contents
// plus a new common prefix exactly fill the page (listing-0023/0008 shape).
func TestListObjectsV2_DelimiterExactFit(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	bucket := "bk"
	for _, k := range []string{"a", "boo/bar", "boo/baz", "cquux/x"} {
		putObj(t, b, k, []byte("x"))
	}

	// Top level with delimiter "/": Contents [a] + CommonPrefixes [boo/, cquux/] = 3.
	res, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucket, Delimiter: str("/"), MaxKeys: i32(3),
	})
	require.NoError(t, err, "ListObjectsV2")
	require.Len(t, res.Contents, 1, "Contents")
	require.Len(t, res.CommonPrefixes, 2, "CommonPrefixes")
	require.NotNil(t, res.IsTruncated, "IsTruncated")
	require.False(t, *res.IsTruncated, "IsTruncated (exact fit)")
}

// TestListObjectsV2_DelimiterGenuineTruncation confirms more common prefixes
// than MaxKeys still truncates and resumes correctly (listing-0103 shape).
func TestListObjectsV2_DelimiterGenuineTruncation(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	bucket := "bk"
	for _, k := range []string{"p0/x", "p1/x", "p2/x"} {
		putObj(t, b, k, []byte("x"))
	}

	res, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucket, Delimiter: str("/"), MaxKeys: i32(2),
	})
	require.NoError(t, err, "ListObjectsV2 page1")
	require.Len(t, res.CommonPrefixes, 2, "page1: CommonPrefixes=%d truncated", len(res.CommonPrefixes))
	require.NotNil(t, res.IsTruncated, "page1: CommonPrefixes=%d truncated", len(res.CommonPrefixes))
	require.True(t, *res.IsTruncated, "page1: CommonPrefixes=%d truncated=%v, want 2/true", len(res.CommonPrefixes), res.IsTruncated)
	require.NotNil(t, res.NextContinuationToken, "page1 missing NextContinuationToken")

	res2, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucket, Delimiter: str("/"), MaxKeys: i32(2), ContinuationToken: res.NextContinuationToken,
	})
	require.NoError(t, err, "ListObjectsV2 page2")
	require.Len(t, res2.CommonPrefixes, 1, "page2 CommonPrefixes")
	require.Equal(t, "p2/", *res2.CommonPrefixes[0].Prefix, "page2 CommonPrefixes")
	require.NotNil(t, res2.IsTruncated, "page2 IsTruncated")
	require.False(t, *res2.IsTruncated, "page2 IsTruncated")
}

// TestListObjectsV1_DelimiterNextMarker covers V1's delimiter-gated NextMarker
// under genuine truncation, and that resuming by Marker returns the remainder.
func TestListObjectsV1_DelimiterNextMarker(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	bucket := "bk"
	for _, k := range []string{"p0/x", "p1/x", "p2/x"} {
		putObj(t, b, k, []byte("x"))
	}

	res, err := b.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: &bucket, Delimiter: str("/"), MaxKeys: i32(2),
	})
	require.NoError(t, err, "ListObjects page1")
	require.Len(t, res.CommonPrefixes, 2, "page1: CommonPrefixes=%d truncated", len(res.CommonPrefixes))
	require.NotNil(t, res.IsTruncated, "page1: CommonPrefixes=%d truncated", len(res.CommonPrefixes))
	require.True(t, *res.IsTruncated, "page1: CommonPrefixes=%d truncated=%v, want 2/true", len(res.CommonPrefixes), res.IsTruncated)
	require.NotNil(t, res.NextMarker, "NextMarker")
	require.Equal(t, "p1/", *res.NextMarker, "NextMarker")

	res2, err := b.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: &bucket, Delimiter: str("/"), MaxKeys: i32(2), Marker: res.NextMarker,
	})
	require.NoError(t, err, "ListObjects page2")
	require.Len(t, res2.CommonPrefixes, 1, "page2 CommonPrefixes")
	require.Equal(t, "p2/", *res2.CommonPrefixes[0].Prefix, "page2 CommonPrefixes")
	require.NotNil(t, res2.IsTruncated, "page2 IsTruncated")
	require.False(t, *res2.IsTruncated, "page2 IsTruncated")
}
