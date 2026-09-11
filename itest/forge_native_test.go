//go:build itest

package itest

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/smelt/pkg/stack"
	"github.com/filecoin-project/go-fee/cose"

	ingottest "github.com/fil-forge/ingot/testing"
)

// TestForgeNativeProvision proves the hilt onboarding path end-to-end on a
// fresh stack: a tenant provisioned through hilt's Tenant API (which mints
// the tenant's did:plc space, registers it as an upload-service customer,
// and issues SigV4 credentials) can immediately create a bucket and
// round-trip a PUT/GET through the forge-mode listener — exercising the
// full ship path (sprue -> piri -> indexer) on a hilt-provisioned space.
// (This replaced the retired `ingot login` + `ingot space generate`
// self-provisioning flow when hilt took ownership of tenancy.)
//
//	go test -tags itest ./itest -run TestForgeNativeProvision -v -timeout 900s
func TestForgeNativeProvision(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t)

	// 1. Provision a tenant + access key through hilt (the sole onboarding
	// path — ingot itself no longer logs in or generates spaces).
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "native")

	// 2. PUT/GET round-trip against the forge-mode ingot — exercises the full
	// blob ship path (sprue -> piri -> indexer) on the hilt-provisioned
	// space. The exhaustive S3 surface is TestForgeVersity's job.
	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)
	const bucket, key = "native-provision", "obj"
	data := patternBytes(512 << 10)
	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := ingottest.PutBytes(ctx, cfg, bucket, key, data); err != nil {
		t.Fatalf("put object: %v", err)
	}
	got, err := ingottest.GetBytes(ctx, cfg, bucket, key)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(data))
	}
	t.Logf("hilt-provisioned round-trip OK: %d bytes through sprue/piri", len(data))

	// 3. Every stored blob is a COSE_Encrypt whose one recipient is the
	// tenant's wrap key: the kid in the envelope is the fingerprint hilt
	// registered for this tenant's active wrap key (its wrap_key row), so the
	// envelope is recoverable from hilt's custody alone.
	wantKID := hiltActiveWrapKID(t, ctx, s, "native")
	env := spooledEnvelope(t, ctx, s)
	if len(env.Recipients) != 1 {
		t.Fatalf("stored envelope has %d recipients, want 1 (the tenant)", len(env.Recipients))
	}
	kid, ok := env.Recipients[0].Headers.Unprotected.Bytes(cose.HeaderLabelKID)
	if !ok || string(kid) != wantKID {
		t.Fatalf("envelope recipient kid = %q, want the tenant's active wrap key %q", kid, wantKID)
	}
	t.Logf("stored envelope carries the tenant recipient %s", wantKID)

	// 4. A second tenant cannot use the first tenant's bucket as a copy
	// source, nor reach it directly: hilt refuses another tenant's bucket as
	// AccessDenied on both paths (S3's answer for another account's bucket),
	// and ingot's own tenant comparison on the bucket rows backs the copy
	// path up. A bucket that exists nowhere is still NoSuchBucket.
	accessKeyB, secretKeyB := hiltProvisionTenant(t, ctx, s, "native-b")
	clientB := bigObjectClient(t, ingotEndpoint, accessKeyB, secretKeyB)
	const bucketB = "native-provision-b"
	if _, err := clientB.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketB)}); err != nil {
		t.Fatalf("tenant B create bucket: %v", err)
	}
	copyInto := func(source string) error {
		_, err := clientB.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket: aws.String(bucketB), Key: aws.String("copied"), CopySource: aws.String(source),
		})
		return err
	}
	if code, status := apiErrorOf(t, copyInto(bucket+"/"+key)); code != "AccessDenied" || status != http.StatusForbidden {
		t.Fatalf("cross-tenant copy of an existing key: %s/%d, want AccessDenied/403", code, status)
	}
	if code, status := apiErrorOf(t, copyInto(bucket+"/no-such-key")); code != "AccessDenied" || status != http.StatusForbidden {
		t.Fatalf("cross-tenant copy of a missing key: %s/%d, want AccessDenied/403 (key existence must not leak)", code, status)
	}
	if code, status := apiErrorOf(t, copyInto("native-provision-nowhere/"+key)); code != "NoSuchBucket" || status != http.StatusNotFound {
		t.Fatalf("copy from a nonexistent bucket: %s/%d, want NoSuchBucket/404", code, status)
	}
	if _, err := clientB.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err == nil {
		t.Fatalf("tenant B could HEAD tenant A's bucket %q", bucket)
	} else if _, status := apiErrorOf(t, err); status != http.StatusForbidden {
		t.Fatalf("tenant B HEAD of tenant A's bucket: status %d, want 403", status)
	}
	t.Logf("cross-tenant copy source refused")
}

// hiltActiveWrapKID reads the active wrap-key fingerprint hilt registered for
// the tenant with the given external id, straight from hilt's Postgres.
func hiltActiveWrapKID(t *testing.T, ctx context.Context, s *stack.Stack, externalID string) string {
	t.Helper()
	q := fmt.Sprintf(`SELECT w.kid FROM wrap_key w JOIN tenant t ON t.id = w.tenant_id WHERE t.external_id = '%s' AND w.status = 'active'`, externalID)
	out, errOut, err := s.Exec(ctx, "hilt-postgres", "psql", "-U", "hilt", "-d", "hilt", "-tAc", q)
	if err != nil {
		t.Fatalf("query hilt wrap key: %v (stderr=%s)", err, errOut)
	}
	kid := strings.TrimSpace(out)
	if kid == "" {
		t.Fatalf("hilt has no active wrap key for tenant %q", externalID)
	}
	return kid
}

// spooledEnvelope pulls one body blob out of the ingot container's spool and
// decodes its COSE envelope header.
func spooledEnvelope(t *testing.T, ctx context.Context, s *stack.Stack) *cose.Envelope {
	t.Helper()
	out, errOut, err := s.Exec(ctx, "ingot", "sh", "-c",
		`f=$(find /data/spool -maxdepth 1 -type f ! -name '.tmp*' | head -1); [ -n "$f" ] && base64 < "$f"`)
	if err != nil {
		t.Fatalf("read a spooled blob: %v (stderr=%s)", err, errOut)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(out), ""))
	if err != nil {
		t.Fatalf("decode spooled blob: %v", err)
	}
	env, _, err := cose.Decode(raw)
	if err != nil {
		t.Fatalf("decode COSE envelope: %v", err)
	}
	return env
}
