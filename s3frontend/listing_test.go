package s3frontend

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(res.Contents) != 5 {
		t.Fatalf("Contents = %d, want 5", len(res.Contents))
	}
	if res.IsTruncated == nil || *res.IsTruncated {
		t.Fatalf("IsTruncated = %v, want false (exact fit)", res.IsTruncated)
	}
	if res.NextContinuationToken != nil {
		t.Fatalf("NextContinuationToken = %q, want nil", *res.NextContinuationToken)
	}
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
	if err != nil {
		t.Fatalf("ListObjectsV2 page1: %v", err)
	}
	if len(res.Contents) != 4 || res.IsTruncated == nil || !*res.IsTruncated {
		t.Fatalf("page1: Contents=%d truncated=%v, want 4/true", len(res.Contents), res.IsTruncated)
	}
	if res.NextContinuationToken == nil {
		t.Fatal("page1 missing NextContinuationToken")
	}

	res2, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucket, MaxKeys: i32(4), ContinuationToken: res.NextContinuationToken,
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 page2: %v", err)
	}
	if len(res2.Contents) != 1 || *res2.Contents[0].Key != "k4" {
		t.Fatalf("page2 Contents = %d (%v), want 1 [k4]", len(res2.Contents), res2.Contents)
	}
	if res2.IsTruncated == nil || *res2.IsTruncated {
		t.Fatalf("page2 IsTruncated = %v, want false", res2.IsTruncated)
	}
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
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(res.Contents) != 1 || len(res.CommonPrefixes) != 2 {
		t.Fatalf("Contents=%d CommonPrefixes=%d, want 1/2", len(res.Contents), len(res.CommonPrefixes))
	}
	if res.IsTruncated == nil || *res.IsTruncated {
		t.Fatalf("IsTruncated = %v, want false (exact fit)", res.IsTruncated)
	}
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
	if err != nil {
		t.Fatalf("ListObjectsV2 page1: %v", err)
	}
	if len(res.CommonPrefixes) != 2 || res.IsTruncated == nil || !*res.IsTruncated {
		t.Fatalf("page1: CommonPrefixes=%d truncated=%v, want 2/true", len(res.CommonPrefixes), res.IsTruncated)
	}
	if res.NextContinuationToken == nil {
		t.Fatal("page1 missing NextContinuationToken")
	}

	res2, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucket, Delimiter: str("/"), MaxKeys: i32(2), ContinuationToken: res.NextContinuationToken,
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 page2: %v", err)
	}
	if len(res2.CommonPrefixes) != 1 || *res2.CommonPrefixes[0].Prefix != "p2/" {
		t.Fatalf("page2 CommonPrefixes = %v, want [p2/]", res2.CommonPrefixes)
	}
	if res2.IsTruncated == nil || *res2.IsTruncated {
		t.Fatalf("page2 IsTruncated = %v, want false", res2.IsTruncated)
	}
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
	if err != nil {
		t.Fatalf("ListObjects page1: %v", err)
	}
	if len(res.CommonPrefixes) != 2 || res.IsTruncated == nil || !*res.IsTruncated {
		t.Fatalf("page1: CommonPrefixes=%d truncated=%v, want 2/true", len(res.CommonPrefixes), res.IsTruncated)
	}
	if res.NextMarker == nil || *res.NextMarker != "p1/" {
		t.Fatalf("NextMarker = %v, want p1/", res.NextMarker)
	}

	res2, err := b.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: &bucket, Delimiter: str("/"), MaxKeys: i32(2), Marker: res.NextMarker,
	})
	if err != nil {
		t.Fatalf("ListObjects page2: %v", err)
	}
	if len(res2.CommonPrefixes) != 1 || *res2.CommonPrefixes[0].Prefix != "p2/" {
		t.Fatalf("page2 CommonPrefixes = %v, want [p2/]", res2.CommonPrefixes)
	}
	if res2.IsTruncated == nil || *res2.IsTruncated {
		t.Fatalf("page2 IsTruncated = %v, want false", res2.IsTruncated)
	}
}
