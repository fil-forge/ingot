//go:build itest

package itest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

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
// that grants /upload/remove for s3:PutObject. The test skips itself against
// an upload service without the table (see requireUploadMetrics), so it stays
// green on the published images and starts running on its own once they carry
// the support — no gate to remember to remove. To run it now, point the stack
// at local builds:
//
//	INGOT_ITEST_UPLOAD_IMAGE=ghcr.io/fil-forge/upload-service:local \
//	INGOT_ITEST_HILT_IMAGE=ghcr.io/fil-forge/hilt:local \
//	  go test -tags itest ./itest -run TestForgeObjectCount -v -timeout 900s
func TestForgeObjectCount(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t)
	requireUploadMetrics(t, ctx, s)
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "objcount")
	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)

	const bucket = "object-count"
	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	space := bucketSpace(t, ctx, s, bucket)

	// One PUT, one object. The write queues the change and the registration
	// sweeper applies it, so every count below is awaited rather than read
	// once.
	if err := ingottest.PutBytes(ctx, cfg, bucket, "a", patternBytes(4<<10)); err != nil {
		t.Fatalf("put a: %v", err)
	}
	awaitObjectCount(t, ctx, s, space, 1, "after one PUT")

	// A second key adds one more.
	if err := ingottest.PutBytes(ctx, cfg, bucket, "b", patternBytes(4<<10)); err != nil {
		t.Fatalf("put b: %v", err)
	}
	awaitObjectCount(t, ctx, s, space, 2, "after two PUTs")

	// Overwriting a key in an unversioned bucket discards the prior version,
	// so the count holds: the new root registers and the old one retracts.
	if err := ingottest.PutBytes(ctx, cfg, bucket, "a", patternBytes(8<<10)); err != nil {
		t.Fatalf("overwrite a: %v", err)
	}
	// A count that climbs to 3 and stays there is the authorization half
	// rather than ingot: a put that supersedes retracts the replaced version
	// with /upload/remove, which hilt grants through s3:PutObject. Without the
	// grant the retraction is refused and requeued forever.
	awaitObjectCount(t, ctx, s, space, 2, "after overwriting a key "+
		"(a count stuck at 3 means hilt is not granting /upload/remove for s3:PutObject)")

	// Deleting returns the count.
	if err := ingottest.DeleteObject(ctx, cfg, bucket, "a"); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	awaitObjectCount(t, ctx, s, space, 1, "after a delete")

	// The diff log carries the same history with timestamps, which is what a
	// windowed object-count series is reconstructed from. Three adds (a, b, a
	// again) and two removes (the superseded a, the deleted a) net to 1.
	if got := spaceUploadDiffSum(t, ctx, s, space); got != 1 {
		t.Fatalf("upload_diff deltas sum to %d, want 1", got)
	}

	// Nothing is left owing: the queue drained rather than stalling.
	if left := ingotSQL(t, ctx, s, "SELECT count(*) FROM ingot.upload_registrations"); left != "0" {
		t.Fatalf("upload_registrations still holds %s rows; the sweeper is not draining", left)
	}

	// Deleting the bucket takes its queued changes with it. A row left behind
	// would be retried against a space that no longer exists, keep its bearer
	// proofs on disk, and sit in front of a new bucket of the same name.
	if err := ingottest.DeleteObject(ctx, cfg, bucket, "b"); err != nil {
		t.Fatalf("delete b: %v", err)
	}
	if err := ingottest.DeleteBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}
	q := fmt.Sprintf("SELECT count(*) FROM ingot.upload_registrations WHERE space = '%s'", space)
	if left := ingotSQL(t, ctx, s, q); left != "0" {
		t.Fatalf("the deleted bucket's space still holds %s queued changes", left)
	}
}

// awaitObjectCount waits for the space's object count to reach want. The
// registration sweeper applies the queued changes off the request path, so the
// count trails the write by up to a sweep interval.
func awaitObjectCount(t *testing.T, ctx context.Context, s *stack.Stack, space string, want int, stage string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var got int
	for time.Now().Before(deadline) {
		got = spaceObjectCount(t, ctx, s, space)
		if got == want {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("%s: object count settled at %d, want %d", stage, got, want)
}

// requireUploadMetrics skips the test unless the upload service records object
// counts — probed by the presence of the upload_diff table, which arrives with
// the metric counters in the same change. Probing the capability rather than
// gating on the image override env vars means the test runs by itself once the
// published image carries the support.
func requireUploadMetrics(t *testing.T, ctx context.Context, s *stack.Stack) {
	t.Helper()
	if sprueSQL(t, ctx, s, "SELECT to_regclass('public.upload_diff') IS NOT NULL") != "t" {
		t.Skip("upload service does not record object counts (no upload_diff table); " +
			"set INGOT_ITEST_UPLOAD_IMAGE to a build that does")
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
