//go:build itest

package itest

import (
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
	"github.com/stretchr/testify/require"

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
	require.NoError(t, ingottest.CreateBucket(ctx, cfg, bucket), "create bucket")
	require.NoError(t, ingottest.PutBytes(ctx, cfg, bucket, key, data), "put object")
	got, err := ingottest.GetBytes(ctx, cfg, bucket, key)
	require.NoError(t, err, "get object")
	require.Equal(t, data, got, "round-trip mismatch")
	t.Logf("hilt-provisioned round-trip OK: %d bytes through sprue/piri", len(data))

	// 3. Every stored blob is a COSE_Encrypt whose one recipient is the
	// tenant's wrap key: the kid in the envelope is the fingerprint hilt
	// registered for this tenant's active wrap key (its wrap_key row), so the
	// envelope is recoverable from hilt's custody alone.
	wantKID := hiltActiveWrapKID(t, ctx, s, "native")
	env := localEnvelope(t, ctx, s)
	require.Len(t, env.Recipients, 1, "stored envelope has %d recipients", len(env.Recipients))
	kid, ok := env.Recipients[0].Headers.Unprotected.Bytes(cose.HeaderLabelKID)
	require.True(t, ok, "envelope recipient kid = %q, want the tenant's active wrap key %q", kid, wantKID)
	require.Equal(t, wantKID, string(kid), "envelope recipient kid = %q, want the tenant's active wrap key %q", kid, wantKID)
	t.Logf("stored envelope carries the tenant recipient %s", wantKID)

	// 4. A second tenant cannot use the first tenant's bucket as a copy
	// source, nor reach it directly: hilt refuses another tenant's bucket as
	// AccessDenied on both paths (S3's answer for another account's bucket),
	// and ingot's own tenant comparison on the bucket rows backs the copy
	// path up. A bucket that exists nowhere is still NoSuchBucket.
	accessKeyB, secretKeyB := hiltProvisionTenant(t, ctx, s, "native-b")
	clientB := bigObjectClient(t, ingotEndpoint, accessKeyB, secretKeyB)
	const bucketB = "native-provision-b"
	_, err = clientB.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketB)})
	require.NoError(t, err, "tenant B create bucket")
	copyInto := func(source string) error {
		_, err := clientB.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket: aws.String(bucketB), Key: aws.String("copied"), CopySource: aws.String(source),
		})
		return err
	}
	code, status := apiErrorOf(t, copyInto(bucket+"/"+key))
	require.Equal(t, "AccessDenied", code, "cross-tenant copy of an existing key")
	require.Equal(t, http.StatusForbidden, status, "cross-tenant copy of an existing key")
	// Key existence must not leak.
	code, status = apiErrorOf(t, copyInto(bucket+"/no-such-key"))
	require.Equal(t, "AccessDenied", code, "cross-tenant copy of a missing key")
	require.Equal(t, http.StatusForbidden, status, "cross-tenant copy of a missing key")
	code, status = apiErrorOf(t, copyInto("native-provision-nowhere/"+key))
	require.Equal(t, "NoSuchBucket", code, "copy from a nonexistent bucket")
	require.Equal(t, http.StatusNotFound, status, "copy from a nonexistent bucket")
	_, err = clientB.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	require.Error(t, err, "tenant B could HEAD tenant A's bucket %q", bucket)
	_, status = apiErrorOf(t, err)
	require.Equal(t, http.StatusForbidden, status, "tenant B HEAD of tenant A's bucket")
	t.Logf("cross-tenant copy source refused")
}

// hiltActiveWrapKID reads the active wrap-key fingerprint hilt registered for
// the tenant with the given external id, straight from hilt's Postgres.
func hiltActiveWrapKID(t *testing.T, ctx context.Context, s *stack.Stack, externalID string) string {
	t.Helper()
	q := fmt.Sprintf(`SELECT w.kid FROM wrap_key w JOIN tenant t ON t.id = w.tenant_id WHERE t.external_id = '%s' AND w.status = 'active'`, externalID)
	out, errOut, err := s.Exec(ctx, "hilt-postgres", "psql", "-U", "hilt", "-d", "hilt", "-tAc", q)
	require.NoError(t, err, "query hilt wrap key: %v (stderr=%s)", err, errOut)
	kid := strings.TrimSpace(out)
	require.NotEmpty(t, kid, "hilt has no active wrap key for tenant %q", externalID)
	return kid
}

// localEnvelope pulls one body blob out of the ingot container's
// decodes its COSE envelope header.
func localEnvelope(t *testing.T, ctx context.Context, s *stack.Stack) *cose.Envelope {
	t.Helper()
	out, errOut, err := s.Exec(ctx, "ingot", "sh", "-c",
		`f=$(find /data/cache -maxdepth 1 -type f ! -name '.tmp*' | head -1); [ -n "$f" ] && base64 < "$f"`)
	require.NoError(t, err, "read a cached blob: %v (stderr=%s)", err, errOut)
	raw, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(out), ""))
	require.NoError(t, err, "decode cached blob")
	env, _, err := cose.Decode(raw)
	require.NoError(t, err, "decode COSE envelope")
	return env
}
