//go:build itest

package itest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/fil-forge/smelt/pkg/stack"

	ingottest "github.com/fil-forge/ingot/testing"
)

// TestForgeObjectCount proves the object count FilOne bills on: every
// committed object version registers a content entry with the upload service,
// and a retired one retracts, so the count sprue derives — /upload/add-total
// minus /upload/remove-total on the bucket's space — tracks the bucket.
//
// Needs two services ahead of their published images: an upload service that
// records the upload metrics and carries the upload_diff table, and a hilt
// that grants /upload/remove for s3:PutObject (without it an overwrite
// registers the new version and cannot retract the superseded one, so the
// count climbs). Until both are published, point the stack at local builds:
//
//	INGOT_ITEST_UPLOAD_IMAGE=ghcr.io/fil-forge/upload-service:local \
//	INGOT_ITEST_HILT_IMAGE=ghcr.io/fil-forge/hilt:local \
//	  go test -tags itest ./itest -run TestForgeObjectCount -v -timeout 900s
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

	// One PUT, one object.
	if err := ingottest.PutBytes(ctx, cfg, bucket, "a", patternBytes(4<<10)); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if got := spaceObjectCount(t, ctx, s, space); got != 1 {
		t.Fatalf("after one PUT: object count %d, want 1", got)
	}

	// A second key adds one more.
	if err := ingottest.PutBytes(ctx, cfg, bucket, "b", patternBytes(4<<10)); err != nil {
		t.Fatalf("put b: %v", err)
	}
	if got := spaceObjectCount(t, ctx, s, space); got != 2 {
		t.Fatalf("after two PUTs: object count %d, want 2", got)
	}

	// Overwriting a key in an unversioned bucket discards the prior version,
	// so the count holds: the new root registers and the old one retracts.
	if err := ingottest.PutBytes(ctx, cfg, bucket, "a", patternBytes(8<<10)); err != nil {
		t.Fatalf("overwrite a: %v", err)
	}
	if got := spaceObjectCount(t, ctx, s, space); got != 2 {
		t.Fatalf("after overwriting a key: object count %d, want 2", got)
	}

	// Deleting returns the count.
	if err := ingottest.DeleteObject(ctx, cfg, bucket, "a"); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	if got := spaceObjectCount(t, ctx, s, space); got != 1 {
		t.Fatalf("after a delete: object count %d, want 1", got)
	}

	// The diff log carries the same history with timestamps, which is what a
	// windowed object-count series is reconstructed from. Four adds (a, b, a
	// again) and two removes (the superseded a, the deleted a) net to 1.
	if got := spaceUploadDiffSum(t, ctx, s, space); got != 1 {
		t.Fatalf("upload_diff deltas sum to %d, want 1", got)
	}
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
// registry — the subject every /upload/add for the bucket is issued against.
func bucketSpace(t *testing.T, ctx context.Context, s *stack.Stack, bucket string) string {
	t.Helper()
	space := ingotSQL(t, ctx, s, fmt.Sprintf("SELECT space FROM ingot.buckets WHERE name = '%s'", bucket))
	if space == "" {
		t.Fatalf("no space recorded for bucket %q", bucket)
	}
	return space
}

// spaceObjectCount is the count sprue reports for a space: the adds it has
// counted less the removes, from the cumulative per-space metric counters.
func spaceObjectCount(t *testing.T, ctx context.Context, s *stack.Stack, space string) int {
	t.Helper()
	q := fmt.Sprintf(`SELECT COALESCE(SUM(CASE WHEN name = '/upload/add-total' THEN value
	                                           WHEN name = '/upload/remove-total' THEN -value
	                                           ELSE 0 END), 0)
	                  FROM space_metrics WHERE space = '%s'`, space)
	n, err := strconv.Atoi(sprueSQL(t, ctx, s, q))
	if err != nil {
		t.Fatalf("parse object count: %v", err)
	}
	return n
}

// spaceUploadDiffSum is the same count derived the other way, by summing the
// signed deltas of the append-only log a windowed series is built from.
func spaceUploadDiffSum(t *testing.T, ctx context.Context, s *stack.Stack, space string) int {
	t.Helper()
	q := fmt.Sprintf("SELECT COALESCE(SUM(delta), 0) FROM upload_diff WHERE space = '%s'", space)
	n, err := strconv.Atoi(sprueSQL(t, ctx, s, q))
	if err != nil {
		t.Fatalf("parse upload_diff sum: %v", err)
	}
	return n
}
