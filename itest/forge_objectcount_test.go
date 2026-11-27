//go:build itest

package itest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/fil-forge/smelt/pkg/stack"

	ingottest "github.com/fil-forge/ingot/testing"
)

// TestForgeObjectCount proves the object count the gateway reports for
// billing: a bucket gains an object with every object version it commits and
// loses one with every version it discards, in the commit itself, and the
// hour's usage count under the bucket's space holds the totals for the report
// to the auth service. Nothing is registered with the upload service for it.
func TestForgeObjectCount(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t)
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "objcount")
	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)

	const bucket = "object-count"
	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	space := bucketSpace(t, ctx, s, bucket)

	// One PUT, one object. The count moves with the commit, so it is read
	// as soon as the write returns.
	if err := ingottest.PutBytes(ctx, cfg, bucket, "a", patternBytes(4<<10)); err != nil {
		t.Fatalf("put a: %v", err)
	}
	wantBucketObjects(t, ctx, s, bucket, "1 0", "after one PUT")

	// A second key adds one more.
	if err := ingottest.PutBytes(ctx, cfg, bucket, "b", patternBytes(4<<10)); err != nil {
		t.Fatalf("put b: %v", err)
	}
	wantBucketObjects(t, ctx, s, bucket, "2 0", "after two PUTs")

	// Overwriting a key in an unversioned bucket discards the prior version,
	// so the bucket gains one and loses one, and holds two.
	if err := ingottest.PutBytes(ctx, cfg, bucket, "a", patternBytes(8<<10)); err != nil {
		t.Fatalf("overwrite a: %v", err)
	}
	wantBucketObjects(t, ctx, s, bucket, "3 1", "after overwriting a key")

	// Deleting takes one away.
	if err := ingottest.DeleteObject(ctx, cfg, bucket, "a"); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	wantBucketObjects(t, ctx, s, bucket, "3 2", "after a delete")

	// The usage count the reporter sends holds the same totals under the
	// bucket's space, in the latest hour it changed.
	if got := spaceObjectTotals(t, ctx, s, space); got != "3 2" {
		t.Fatalf("the space's latest usage count is %q, want \"3 2\"", got)
	}

	// The upload service was told of no object.
	if n := sprueSQL(t, ctx, s, fmt.Sprintf("SELECT count(*) FROM upload WHERE space = '%s'", space)); n != "0" {
		t.Fatalf("the bucket's space holds %s content entries in the upload service, want none", n)
	}

	// Emptied and deleted back to back — what a script does. The bucket's
	// row goes, and its space's usage count stays for the reporter, saying
	// the bucket ended empty.
	if err := ingottest.DeleteObject(ctx, cfg, bucket, "b"); err != nil {
		t.Fatalf("delete b: %v", err)
	}
	if err := ingottest.DeleteBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}
	if got := spaceObjectTotals(t, ctx, s, space); got != "3 3" {
		t.Fatalf("the deleted bucket's latest usage count is %q, want \"3 3\"", got)
	}
}

// wantBucketObjects asserts the objects the bucket has gained and lost, as
// "gained lost".
func wantBucketObjects(t *testing.T, ctx context.Context, s *stack.Stack, bucket, want, stage string) {
	t.Helper()
	got := ingotSQL(t, ctx, s, fmt.Sprintf(
		"SELECT objects_added || ' ' || objects_removed FROM ingot.buckets WHERE name = '%s'", bucket))
	if got != want {
		t.Fatalf("%s: the bucket has gained and lost %q objects, want %q", stage, got, want)
	}
}

// spaceObjectTotals returns the totals of the space's latest usage count, as
// "gained lost".
func spaceObjectTotals(t *testing.T, ctx context.Context, s *stack.Stack, space string) string {
	t.Helper()
	return ingotSQL(t, ctx, s, fmt.Sprintf(
		"SELECT added || ' ' || removed FROM ingot.object_usage WHERE space = '%s' ORDER BY hour DESC LIMIT 1", space))
}

// sprueSQL runs one SQL statement against the upload service's Postgres and
// returns the bare psql output.
func sprueSQL(t *testing.T, ctx context.Context, s *stack.Stack, q string) string {
	t.Helper()
	out, errOut, err := s.Exec(ctx, "postgres", "psql", "-U", "sprue", "-d", "sprue", "-tAc", q)
	if err != nil {
		t.Fatalf("sprue sql %q: %v (stderr=%s)", q, err, errOut)
	}
	return strings.TrimSpace(out)
}

// bucketSpace reads the DID of the space backing a bucket from ingot's own
// registry.
func bucketSpace(t *testing.T, ctx context.Context, s *stack.Stack, bucket string) string {
	t.Helper()
	space := ingotSQL(t, ctx, s, fmt.Sprintf("SELECT space FROM ingot.buckets WHERE name = '%s'", bucket))
	if space == "" {
		t.Fatalf("no space recorded for bucket %q", bucket)
	}
	return space
}
