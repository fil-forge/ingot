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

// TestForgeWriteLockedTenantIsReadOnly checks the tenant states through the S3
// data plane, as fil-one's Service Orchestrator integration guide defines
// them: a write-locked tenant is refused uploads and bucket creation and
// keeps its reads, listings and deletes, and a disabled tenant is refused
// everything.
//
// Each status is exercised with a key that has not been used before, so
// ingot holds nothing cached for it and the decision is the auth service's.
// The keys are created while the tenant is active, because a disabled tenant
// gets no new keys.
//
// A refusal is asserted as 403 AccessDenied and not merely as an error: an
// unmapped refusal reaches the client as 500 InternalError, which every SDK
// retries as a server fault.
//
//	go test -tags itest ./itest -run TestForgeWriteLockedTenantIsReadOnly -v -timeout 900s
func TestForgeWriteLockedTenantIsReadOnly(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t)

	const tenant, bucket, spare = "writelock", "writelock-bkt", "writelock-spare"
	svcAK, svcSK := hiltProvisionTenant(t, ctx, s, tenant)
	owner := bigObjectClient(t, endpoint, svcAK, svcSK)
	for _, b := range []string{bucket, spare} {
		if _, err := owner.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil {
			t.Fatalf("create %s: %v", b, err)
		}
	}
	for _, key := range []string{"kept", "deleted"} {
		if _, err := owner.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(patternBytes(512)),
		}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	newClient := func(t *testing.T, name string) *s3.Client {
		t.Helper()
		ak, sk := hiltCreateAccessKey(t, ctx, s, tenant, name, hiltAllPermissions, nil)
		return bigObjectClient(t, endpoint, ak, sk)
	}
	disabled, locked := newClient(t, "for-disabled"), newClient(t, "for-write-locked")

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
	requireOK := func(t *testing.T, err error, what string) {
		t.Helper()
		if err != nil {
			t.Errorf("%s: %v", what, err)
			return
		}
		t.Logf("%s succeeded", what)
	}
	put := func(c *s3.Client, key string) error {
		_, err := c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(patternBytes(512)),
		})
		return err
	}
	createBucket := func(c *s3.Client, name string) error {
		_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)})
		return err
	}
	get := func(c *s3.Client, key string) error {
		out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err == nil {
			out.Body.Close()
		}
		return err
	}

	// A disabled tenant is refused everything, reads included.
	hiltSetTenantStatus(t, ctx, s, tenant, "disabled")
	requireAccessDenied(t, put(disabled, "probe-disabled"), "PutObject on a disabled tenant")
	requireAccessDenied(t, createBucket(disabled, bucket+"-disabled"), "CreateBucket on a disabled tenant")
	requireAccessDenied(t, get(disabled, "kept"), "GetObject on a disabled tenant")

	// A write-locked tenant is refused uploads and bucket creation.
	hiltSetTenantStatus(t, ctx, s, tenant, "write-locked")
	requireAccessDenied(t, put(locked, "probe-locked"), "PutObject on a write-locked tenant")
	requireAccessDenied(t, createBucket(locked, bucket+"-locked"), "CreateBucket on a write-locked tenant")

	// It keeps its reads and listings.
	_, err := locked.ListBuckets(ctx, &s3.ListBucketsInput{})
	requireOK(t, err, "ListBuckets on a write-locked tenant")
	_, err = locked.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	requireOK(t, err, "ListObjectsV2 on a write-locked tenant")
	requireOK(t, get(locked, "kept"), "GetObject on a write-locked tenant")
	_, err = locked.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	requireOK(t, err, "GetBucketVersioning on a write-locked tenant")
	_, err = locked.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(bucket)})
	requireOK(t, err, "GetBucketLocation on a write-locked tenant")

	// And its deletes, so a tenant over its storage limit can free space.
	_, err = locked.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("deleted")})
	requireOK(t, err, "DeleteObject on a write-locked tenant")
	_, err = locked.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(spare)})
	requireOK(t, err, "DeleteBucket on a write-locked tenant")

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
