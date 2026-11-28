//go:build itest

package itest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/fil-forge/smelt/pkg/stack"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, ingottest.CreateBucket(ctx, cfg, bucket), "create bucket")
	space := bucketSpace(t, ctx, s, bucket)

	// One PUT, one object. The count moves with the commit, so it is read
	// as soon as the write returns.
	require.NoError(t, ingottest.PutBytes(ctx, cfg, bucket, "a", patternBytes(4<<10)), "put a")
	wantBucketObjects(t, ctx, s, bucket, "1 0", "after one PUT")

	// A second key adds one more.
	require.NoError(t, ingottest.PutBytes(ctx, cfg, bucket, "b", patternBytes(4<<10)), "put b")
	wantBucketObjects(t, ctx, s, bucket, "2 0", "after two PUTs")

	// Overwriting a key in an unversioned bucket discards the prior version,
	// so the bucket gains one and loses one, and holds two.
	require.NoError(t, ingottest.PutBytes(ctx, cfg, bucket, "a", patternBytes(8<<10)), "overwrite a")
	wantBucketObjects(t, ctx, s, bucket, "3 1", "after overwriting a key")

	// Deleting takes one away.
	require.NoError(t, ingottest.DeleteObject(ctx, cfg, bucket, "a"), "delete a")
	wantBucketObjects(t, ctx, s, bucket, "3 2", "after a delete")

	// The usage count the reporter sends holds the same totals under the
	// bucket's space, in the latest hour it changed.
	got := spaceObjectTotals(t, ctx, s, space)
	require.Equal(t, "3 2", got, "the space's latest usage count")

	// The upload service was told of no object.
	n := sprueSQL(t, ctx, s, fmt.Sprintf("SELECT count(*) FROM upload WHERE space = '%s'", space))
	require.Equal(t, "0", n, "the bucket's space holds %s content entries in the upload service", n)

	// Emptied and deleted back to back — what a script does. The bucket's
	// row goes, and its space's usage count stays for the reporter, saying
	// the bucket ended empty.
	require.NoError(t, ingottest.DeleteObject(ctx, cfg, bucket, "b"), "delete b")
	require.NoError(t, ingottest.DeleteBucket(ctx, cfg, bucket), "delete bucket")
	require.Equal(t, "3 3", spaceObjectTotals(t, ctx, s, space), "the deleted bucket's latest usage count")
}

// wantBucketObjects asserts the objects the bucket has gained and lost, as
// "gained lost".
func wantBucketObjects(t *testing.T, ctx context.Context, s *stack.Stack, bucket, want, stage string) {
	t.Helper()
	got := ingotSQL(t, ctx, s, fmt.Sprintf(
		"SELECT objects_added || ' ' || objects_removed FROM ingot.buckets WHERE name = '%s'", bucket))
	require.Equal(t, want, got, "%s: the bucket has gained and lost %q objects", stage, got)
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
	require.NoError(t, err, "sprue sql %q: %v (stderr=%s)", q, err, errOut)
	return strings.TrimSpace(out)
}

// bucketSpace reads the DID of the space backing a bucket from ingot's own
// registry.
func bucketSpace(t *testing.T, ctx context.Context, s *stack.Stack, bucket string) string {
	t.Helper()
	space := ingotSQL(t, ctx, s, fmt.Sprintf("SELECT space FROM ingot.buckets WHERE name = '%s'", bucket))
	require.NotEmpty(t, space, "no space recorded for bucket %q", bucket)
	return space
}
