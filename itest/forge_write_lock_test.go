//go:build itest

package itest

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/smelt/pkg/stack"
)

// TestForgeWriteLockedTenantRefusesMutations is the falsifiable invariant behind
// FIL-1101: a tenant whose status is write-locked must not be able to mutate
// through the S3 data plane, and must still be able to read.
//
// Each probe uses a key minted after the status change, so the decision is
// hilt's and not ingot's cached fast path. The Disabled arm is the control: it
// proves the status field is read on every request and only its value differs.
//
// The refusal is asserted as 403 AccessDenied, not merely as an error. The
// first version of this test asserted err != nil and passed while hilt's
// TenantWriteLocked arrived here unmapped and came back 500 InternalError — a
// retryable server fault for what is a policy decision. mapAuthError now knows
// the name; this is what holds it there.
//
// It needs a hilt that enforces the lock (fil-forge/hilt#52): run it against
// that build with INGOT_ITEST_HILT_BINARY.
//
//	go test -tags itest ./itest -run TestForgeWriteLockedTenantRefusesMutations -v -timeout 900s
func TestForgeWriteLockedTenantRefusesMutations(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t)

	const tenant, bucket = "writelock", "writelock-bkt"
	svcAK, svcSK := hiltProvisionTenant(t, ctx, s, tenant)
	if _, err := bigObjectClient(t, endpoint, svcAK, svcSK).CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("create %s: %v", bucket, err)
	}

	// coldClient mints a key AFTER the status is set, so ingot holds no cache
	// for it and hilt must decide the request.
	coldClient := func(t *testing.T, name string) *s3.Client {
		t.Helper()
		ak, sk := hiltCreateAccessKey(t, ctx, s, tenant, name, hiltAllPermissions, nil)
		return bigObjectClient(t, endpoint, ak, sk)
	}
	mutations := func(t *testing.T, c *s3.Client, suffix string) []error {
		t.Helper()
		_, putErr := c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("probe-" + suffix),
			Body: bytes.NewReader(patternBytes(512)),
		})
		_, createErr := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket + "-" + suffix)})
		_, deleteErr := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket + "-" + suffix)})
		return []error{putErr, createErr, deleteErr}
	}
	names := []string{"PutObject", "CreateBucket", "DeleteBucket"}

	// requireAccessDenied pins how the refusal reaches the client: a policy
	// decision is 403 AccessDenied. A 5xx here is the bug this assertion
	// exists for — SDKs retry it, so a write lock that lands as 500 looks to
	// the caller like the service is broken rather than like it said no.
	requireAccessDenied := func(t *testing.T, err error, what string) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s succeeded, want a refusal", what)
		}
		code, status := apiErrorOf(t, err)
		if status != http.StatusForbidden || code != "AccessDenied" {
			t.Errorf("%s: got HTTP %d %s, want 403 AccessDenied (err=%v)", what, status, code, err)
			return
		}
		t.Logf("%s refused: HTTP %d %s", what, status, code)
	}

	// Control: Disabled must refuse all three, on the same cold path.
	hiltSetTenantStatus(t, ctx, s, tenant, "disabled")
	for i, err := range mutations(t, coldClient(t, "cold-disabled"), "disabled") {
		requireAccessDenied(t, err, "control: "+names[i]+" on a disabled tenant")
	}

	hiltSetTenantStatus(t, ctx, s, tenant, "write-locked")
	locked := coldClient(t, "cold-writelocked")
	for i, err := range mutations(t, locked, "wl") {
		requireAccessDenied(t, err, names[i]+" on a write-locked tenant")
	}

	// "Done when #2" on FIL-1101: a write-locked tenant can still read. The
	// three the ticket names, plus two of the bucket-configuration reads,
	// which hilt refused under the lock for as long as it enumerated its
	// read operations by hand.
	if _, err := locked.ListBuckets(ctx, &s3.ListBucketsInput{}); err != nil {
		t.Errorf("ListBuckets on a write-locked tenant: %v", err)
	}
	if _, err := locked.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}); err != nil {
		t.Errorf("ListObjectsV2 on a write-locked tenant: %v", err)
	}
	if _, err := locked.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)}); err != nil {
		t.Errorf("GetBucketVersioning on a write-locked tenant: %v", err)
	}
	if _, err := locked.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(bucket)}); err != nil {
		t.Errorf("GetBucketLocation on a write-locked tenant: %v", err)
	}

	hiltSetTenantStatus(t, ctx, s, tenant, "active")
}

// hiltCreatePrincipal registers a principal on an already-provisioned tenant.

// hiltSetTenantStatus moves a tenant to one of active, write-locked, disabled.
func hiltSetTenantStatus(t *testing.T, ctx context.Context, s *stack.Stack, tenantID, status string) {
	t.Helper()
	if out, errOut, err := s.Exec(ctx, "hilt", "curl", "-sS", "-f", "-X", "POST",
		"http://localhost:80/tenants/"+tenantID+"/status",
		"-H", "Authorization: Bearer dev-partner-key", "-H", "Content-Type: application/json",
		"-d", fmt.Sprintf(`{"status":%q}`, status)); err != nil {
		t.Fatalf("hilt set tenant %q status %q: %v (stdout=%s stderr=%s)", tenantID, status, err, out, errOut)
	}
	t.Logf("tenant %q status set to %q", tenantID, status)
}
