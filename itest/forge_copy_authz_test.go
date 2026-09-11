//go:build itest

package itest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/smelt/pkg/stack"

	ingottest "github.com/fil-forge/ingot/testing"
)

// TestForgeCopyAuthorization proves a copy is authorized on both ends through
// the real hilt: the caller needs write access to the destination AND read
// access to the source, the source resolving within the caller's tenant and
// the key's bucket scope. Each denial is asserted twice with the same key, so
// the second request runs after ingot has cached whatever delegations the
// first one earned: the local fast path must refuse what hilt refuses.
//
//	go test -tags itest ./itest -run TestForgeCopyAuthorization -v -timeout 900s
func TestForgeCopyAuthorization(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t)

	// Tenant A: two buckets, an object in the first, and three keys: every
	// permission on every bucket; PutObject only; every permission but scoped
	// to the second bucket.
	akA, skA := hiltProvisionTenant(t, ctx, s, "copyauthz-a")
	full := bigObjectClient(t, endpoint, akA, skA)
	const srcBucket, dstBucket, key = "copyauthz-src", "copyauthz-dst", "obj"
	cfgA := forgeConfig(endpoint, akA, skA)
	for _, b := range []string{srcBucket, dstBucket} {
		if err := ingottest.CreateBucket(ctx, cfgA, b); err != nil {
			t.Fatalf("create %s: %v", b, err)
		}
	}
	data := patternBytes(64 << 10)
	if err := ingottest.PutBytes(ctx, cfgA, srcBucket, key, data); err != nil {
		t.Fatalf("put source: %v", err)
	}
	akPut, skPut := hiltCreateAccessKey(t, ctx, s, "copyauthz-a", "put-only", []string{"s3:PutObject"}, nil)
	putOnly := bigObjectClient(t, endpoint, akPut, skPut)
	akScoped, skScoped := hiltCreateAccessKey(t, ctx, s, "copyauthz-a", "dst-scoped", hiltAllPermissions, []string{dstBucket})
	dstScoped := bigObjectClient(t, endpoint, akScoped, skScoped)

	// Tenant B with its own bucket.
	akB, skB := hiltProvisionTenant(t, ctx, s, "copyauthz-b")
	clientB := bigObjectClient(t, endpoint, akB, skB)
	const bucketB = "copyauthz-b"
	if err := ingottest.CreateBucket(ctx, forgeConfig(endpoint, akB, skB), bucketB); err != nil {
		t.Fatalf("tenant B create bucket: %v", err)
	}

	copyObj := func(c *s3.Client, dst, source string) error {
		_, err := c.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket: aws.String(dst), Key: aws.String("copied"), CopySource: aws.String(source),
		})
		return err
	}
	// twice runs the request once against hilt and once more with whatever the
	// first attempt left in ingot's caches, asserting the same denial both times.
	// An empty wantCode checks the status alone (a HEAD's error has no body, so
	// the SDK invents its code).
	twice := func(t *testing.T, what string, do func() error, wantCode string, wantStatus int) {
		t.Helper()
		for attempt := 1; attempt <= 2; attempt++ {
			code, status := apiErrorOf(t, do())
			if (wantCode != "" && code != wantCode) || status != wantStatus {
				t.Fatalf("%s (attempt %d): %s/%d, want %s/%d", what, attempt, code, status, wantCode, wantStatus)
			}
		}
	}

	// The full key copies within the source bucket: read and write authorized
	// on one bucket, and the copy-source header was signed by the SDK.
	if err := copyObj(full, srcBucket, srcBucket+"/"+key); err != nil {
		t.Fatalf("same-bucket copy with a full key: %v", err)
	}
	got, err := ingottest.GetBytes(ctx, cfgA, srcBucket, "copied")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("copied object: %d bytes, err %v; want %d bytes", len(got), err, len(data))
	}

	// Within the tenant, a key that can write the destination but cannot read
	// the source is refused: PutObject only, or scoped away from the source.
	// (The scoped key can write dst; the put-only key can read nothing.)
	twice(t, "put-only key copying", func() error { return copyObj(putOnly, dstBucket, srcBucket+"/"+key) },
		"AccessDenied", http.StatusForbidden)
	twice(t, "destination-scoped key copying from the source", func() error { return copyObj(dstScoped, dstBucket, srcBucket+"/"+key) },
		"AccessDenied", http.StatusForbidden)
	// The scoped key's copy within its own bucket is fine as far as hilt is
	// concerned; it must not have been the source scope that let it through.
	if _, err := dstScoped.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(dstBucket), Key: aws.String("own"), Body: bytes.NewReader(data)}); err != nil {
		t.Fatalf("destination-scoped key writing its own bucket: %v", err)
	}
	if err := copyObj(dstScoped, dstBucket, dstBucket+"/own"); err != nil {
		t.Fatalf("destination-scoped key copying within its bucket: %v", err)
	}

	// Across tenants, another tenant's bucket is AccessDenied as a copy source
	// and on the direct path alike — S3's answer for another account's bucket
	// — while a bucket that exists nowhere stays NoSuchBucket.
	twice(t, "tenant B copying from tenant A", func() error { return copyObj(clientB, bucketB, srcBucket+"/"+key) },
		"AccessDenied", http.StatusForbidden)
	twice(t, "tenant B heading tenant A's bucket", func() error {
		_, err := clientB.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(srcBucket)})
		return err
	}, "", http.StatusForbidden)
	twice(t, "tenant B copying from a nonexistent bucket", func() error { return copyObj(clientB, bucketB, "copyauthz-nowhere/"+key) },
		"NoSuchBucket", http.StatusNotFound)
	t.Logf("copy authorization holds on both ends, through hilt and the cached fast path")
}

// hiltCreateAccessKey issues an access key for an already-provisioned tenant
// with the given permissions and bucket scope (names; nil = every bucket),
// returning its SigV4 credentials.
func hiltCreateAccessKey(t *testing.T, ctx context.Context, s *stack.Stack, tenantID, name string, permissions, buckets []string) (accessKey, secretKey string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"name": name, "permissions": permissions, "buckets": buckets})
	if err != nil {
		t.Fatalf("marshal access-key request: %v", err)
	}
	out, errOut, err := s.Exec(ctx, "hilt", "curl", "-sS", "-f", "-X", "POST",
		"http://localhost:80/tenants/"+tenantID+"/access-keys",
		"-H", "Authorization: Bearer dev-partner-key", "-H", "Content-Type: application/json",
		"-d", string(body))
	if err != nil {
		t.Fatalf("hilt create access key %q for %q: %v (stdout=%s stderr=%s)", name, tenantID, err, out, errOut)
	}
	var created struct {
		AccessKeyID     string `json:"accessKeyId"`
		SecretAccessKey string `json:"secretAccessKey"`
	}
	if err := json.Unmarshal([]byte(out), &created); err != nil || created.AccessKeyID == "" || created.SecretAccessKey == "" {
		t.Fatalf("parse access-key response %q: %v", out, err)
	}
	return created.AccessKeyID, created.SecretAccessKey
}
