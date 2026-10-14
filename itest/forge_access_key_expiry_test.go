//go:build itest

package itest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/smelt/pkg/stack"

	ingottest "github.com/fil-forge/ingot/testing"
)

// TestForgeAccessKeyExpiry proves an access key stops working when it
// expires, on the Hilt path and on the cached fast path alike. The key is
// used twice before expiry, so ingot holds its verification key and
// delegation chains. Ingot caches the verification key until the next UTC
// midnight whatever the key's expiry; what refuses the expired key locally
// is that Hilt caps the delegations it issues at the key's expiry, so the
// fast path finds no live chain and asks Hilt, which rejects the key.
//
//	go test -tags itest ./itest -run TestForgeAccessKeyExpiry -v -timeout 900s
func TestForgeAccessKeyExpiry(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t)

	const tenant, bucket, key = "keyexpiry", "keyexpiry", "obj"
	ak, sk := hiltProvisionTenant(t, ctx, s, tenant)
	if err := ingottest.CreateBucket(ctx, forgeConfig(endpoint, ak, sk), bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// Long enough for the requests before expiry, short enough to wait out.
	// The host and the containers share a clock, so the host decides when
	// the key has expired.
	expiresAt := time.Now().Add(30 * time.Second).Truncate(time.Second)
	akExp, skExp := hiltCreateExpiringAccessKey(t, ctx, s, tenant, "expiring", hiltAllPermissions, expiresAt)
	c := bigObjectClient(t, endpoint, akExp, skExp)
	data := patternBytes(64 << 10)

	put := func() error {
		_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data)})
		return err
	}
	get := func() error {
		out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			return err
		}
		defer out.Body.Close()
		got, err := io.ReadAll(out.Body)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("read back %d bytes, want %d", len(got), len(data))
		}
		return nil
	}

	// Before expiry: the write goes to Hilt and fills ingot's caches, so the
	// second read is served by the fast path.
	if err := put(); err != nil {
		t.Fatalf("put before expiry: %v", err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if err := get(); err != nil {
			t.Fatalf("get before expiry (attempt %d): %v", attempt, err)
		}
	}
	if time.Now().After(expiresAt) {
		t.Fatalf("requests before expiry ran past %s; lengthen the key's life", expiresAt.Format(time.RFC3339))
	}

	// A few seconds' margin past expiry absorbs rounding to whole seconds.
	wait := time.Until(expiresAt) + 3*time.Second
	t.Logf("waiting %s for the key to expire", wait.Round(time.Second))
	select {
	case <-time.After(wait):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// After expiry: Hilt's AccessKeyExpired maps to InvalidAccessKeyId, the
	// same answer as for a key that does not exist. Each request runs twice,
	// as a refusal caches nothing and the second attempt must not find a
	// way through either.
	for _, op := range []struct {
		name string
		do   func() error
	}{{"get", get}, {"put", put}} {
		for attempt := 1; attempt <= 2; attempt++ {
			code, status := apiErrorOf(t, op.do())
			if code != "InvalidAccessKeyId" || status != http.StatusForbidden {
				t.Fatalf("%s after expiry (attempt %d): %s/%d, want InvalidAccessKeyId/403", op.name, attempt, code, status)
			}
		}
	}

	// The tenant's other key is untouched.
	if _, err := ingottest.GetBytes(ctx, forgeConfig(endpoint, ak, sk), bucket, key); err != nil {
		t.Fatalf("get with the tenant's unexpiring key: %v", err)
	}
	t.Logf("expired key refused on the Hilt path and the cached fast path")
}

// hiltCreateExpiringAccessKey issues an access key for an already-provisioned
// tenant with the given permissions over every bucket, expiring at expiresAt,
// returning its SigV4 credentials.
func hiltCreateExpiringAccessKey(t *testing.T, ctx context.Context, s *stack.Stack, tenantID, name string, permissions []string, expiresAt time.Time) (accessKey, secretKey string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"name": name, "permissions": permissions, "expiresAt": expiresAt.UTC()})
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
		AccessKeyID     string     `json:"accessKeyId"`
		SecretAccessKey string     `json:"secretAccessKey"`
		ExpiresAt       *time.Time `json:"expiresAt"`
	}
	if err := json.Unmarshal([]byte(out), &created); err != nil || created.AccessKeyID == "" || created.SecretAccessKey == "" {
		t.Fatalf("parse access-key response %q: %v", out, err)
	}
	if created.ExpiresAt == nil || !created.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("hilt stored expiry %v, want %s", created.ExpiresAt, expiresAt.Format(time.RFC3339))
	}
	return created.AccessKeyID, created.SecretAccessKey
}
