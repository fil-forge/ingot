package testing

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/tests/integration"
)

// Order-independent replacements for versitygw's ListBuckets_success,
// ListBuckets_truncated and ListBuckets_with_prefix.
//
// The upstream cases build their expected slice in bucket-*creation* order and
// compare it positionally (compareBuckets, no sort) against the ListBuckets
// response. ingot returns buckets in lexicographic order — the correct S3
// contract, matching versitygw's own posix backend (os.ReadDir sorts by name)
// and AWS's paginated ListBuckets. Upstream names buckets "test-bucket-N" from
// a process-global counter, so under `go test -shuffle` (which the CI Go-test
// workflow enables) the offset at which a case runs is random; when a case's
// names straddle a digit-width boundary (...-9 -> ...-10, sorting as
// 10,11,6,7,8,9) creation order diverges from lexicographic order and the
// positional compare fails — non-deterministically.
//
// These local equivalents keep the same coverage but are deterministic: each
// lists only its own buckets via a unique Prefix and asserts the result equals
// the lexicographically-sorted set it created, independent of creation order.
// The suffixes deliberately straddle a digit boundary so a regression that
// stopped sorting would still be caught.

const lbOpTimeout = 30 * time.Second

// uniqueBucketPrefix returns a fresh, DNS-compliant bucket-name prefix so a
// case sees only the buckets it created, regardless of registry state.
func uniqueBucketPrefix() string {
	return fmt.Sprintf("lb-%d-", time.Now().UnixNano())
}

func lbCreateBuckets(ctx context.Context, c *s3.Client, names []string) error {
	for _, n := range names {
		if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(n)}); err != nil {
			return fmt.Errorf("create bucket %q: %w", n, err)
		}
	}
	return nil
}

// lbDeleteBuckets is best-effort cleanup on its own context so it still runs if
// the caller's context is spent.
func lbDeleteBuckets(c *s3.Client, names []string) {
	ctx, cancel := context.WithTimeout(context.Background(), lbOpTimeout)
	defer cancel()
	for _, n := range names {
		_, _ = c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(n)})
	}
}

func lbListNames(ctx context.Context, c *s3.Client, in *s3.ListBucketsInput) ([]string, *string, error) {
	out, err := c.ListBuckets(ctx, in)
	if err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, len(out.Buckets))
	for _, b := range out.Buckets {
		names = append(names, aws.ToString(b.Name))
	}
	return names, out.ContinuationToken, nil
}

func lbSorted(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func lbEqual(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// smokeListBucketsSuccess: ListBuckets returns every bucket created under our
// prefix, in lexicographic order, regardless of the order they were created.
func smokeListBucketsSuccess(s *integration.S3Conf) error {
	ctx, cancel := context.WithTimeout(context.Background(), lbOpTimeout)
	defer cancel()
	client := s.GetClient()

	prefix := uniqueBucketPrefix()
	var created []string
	for i := 1; i <= 12; i++ {
		created = append(created, fmt.Sprintf("%s%d", prefix, i))
	}
	if err := lbCreateBuckets(ctx, client, created); err != nil {
		return err
	}
	defer lbDeleteBuckets(client, created)

	got, _, err := lbListNames(ctx, client, &s3.ListBucketsInput{Prefix: aws.String(prefix)})
	if err != nil {
		return err
	}
	want := lbSorted(created)
	if !lbEqual(got, want) {
		return fmt.Errorf("ListBuckets returned %v, want %v", got, want)
	}
	return nil
}

// smokeListBucketsTruncated: MaxBuckets pages through the buckets in
// lexicographic order, the continuation token points at the last bucket of
// each page, and the pages concatenate back to the full sorted set.
func smokeListBucketsTruncated(s *integration.S3Conf) error {
	ctx, cancel := context.WithTimeout(context.Background(), lbOpTimeout)
	defer cancel()
	client := s.GetClient()

	prefix := uniqueBucketPrefix()
	var created []string
	for i := 1; i <= 5; i++ {
		created = append(created, fmt.Sprintf("%s%d", prefix, i))
	}
	if err := lbCreateBuckets(ctx, client, created); err != nil {
		return err
	}
	defer lbDeleteBuckets(client, created)
	want := lbSorted(created) // 5 names, lexicographic

	// Page 1: first two.
	page1, tok1, err := lbListNames(ctx, client, &s3.ListBucketsInput{
		Prefix: aws.String(prefix), MaxBuckets: aws.Int32(2),
	})
	if err != nil {
		return err
	}
	if !lbEqual(page1, want[:2]) {
		return fmt.Errorf("page 1 = %v, want %v", page1, want[:2])
	}
	if aws.ToString(tok1) != want[1] {
		return fmt.Errorf("page 1 token = %q, want %q", aws.ToString(tok1), want[1])
	}

	// Page 2: next two, continuing from page 1.
	page2, tok2, err := lbListNames(ctx, client, &s3.ListBucketsInput{
		Prefix: aws.String(prefix), MaxBuckets: aws.Int32(2), ContinuationToken: tok1,
	})
	if err != nil {
		return err
	}
	if !lbEqual(page2, want[2:4]) {
		return fmt.Errorf("page 2 = %v, want %v", page2, want[2:4])
	}
	if aws.ToString(tok2) != want[3] {
		return fmt.Errorf("page 2 token = %q, want %q", aws.ToString(tok2), want[3])
	}

	// Page 3: the remainder; no further pages.
	page3, tok3, err := lbListNames(ctx, client, &s3.ListBucketsInput{
		Prefix: aws.String(prefix), ContinuationToken: tok2,
	})
	if err != nil {
		return err
	}
	if !lbEqual(page3, want[4:]) {
		return fmt.Errorf("page 3 = %v, want %v", page3, want[4:])
	}
	if aws.ToString(tok3) != "" {
		return fmt.Errorf("expected no continuation token after final page, got %q", aws.ToString(tok3))
	}
	return nil
}

// smokeListBucketsWithPrefix: ListBuckets with a Prefix returns only the
// matching buckets, in lexicographic order, and echoes the prefix.
func smokeListBucketsWithPrefix(s *integration.S3Conf) error {
	ctx, cancel := context.WithTimeout(context.Background(), lbOpTimeout)
	defer cancel()
	client := s.GetClient()

	base := uniqueBucketPrefix()
	prefixA, prefixB := base+"a-", base+"b-"
	var aBuckets, bBuckets []string
	for i := 8; i <= 11; i++ { // straddles the 9 -> 10 boundary
		aBuckets = append(aBuckets, fmt.Sprintf("%s%d", prefixA, i))
	}
	for i := 1; i <= 3; i++ {
		bBuckets = append(bBuckets, fmt.Sprintf("%s%d", prefixB, i))
	}
	all := append(append([]string{}, aBuckets...), bBuckets...)
	if err := lbCreateBuckets(ctx, client, all); err != nil {
		return err
	}
	defer lbDeleteBuckets(client, all)

	out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{Prefix: aws.String(prefixA)})
	if err != nil {
		return err
	}
	got := make([]string, 0, len(out.Buckets))
	for _, b := range out.Buckets {
		got = append(got, aws.ToString(b.Name))
	}
	if want := lbSorted(aBuckets); !lbEqual(got, want) {
		return fmt.Errorf("ListBuckets(prefix=%q) = %v, want %v", prefixA, got, want)
	}
	if aws.ToString(out.Prefix) != prefixA {
		return fmt.Errorf("echoed prefix = %q, want %q", aws.ToString(out.Prefix), prefixA)
	}
	return nil
}
