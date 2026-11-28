//go:build itest

package itest

import (
	"bytes"
	"net/http"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// TestForgeBucketEncryptionPermissions proves the bucket encryption
// configuration is guarded by its own permissions through the real hilt: a
// key holding every other permission, bucket create and delete included, is
// refused all three operations; s3:PutEncryptionConfiguration writes and
// removes the configuration without reading it; s3:GetEncryptionConfiguration
// reads it and nothing more. Each denied key first makes an allowed request,
// so ingot holds its verification key and delegation chains, and each denial
// is then asserted twice: the second request runs against those caches, so
// the local fast path must refuse what hilt refuses.
//
//	go test -tags itest ./itest -run TestForgeBucketEncryptionPermissions -v -timeout 900s
func TestForgeBucketEncryptionPermissions(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t)
	const tenant, bucket = "encryption-perms", "guarded"
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, tenant)
	full := sdkClient(forgeS3Conf(endpoint, accessKey, secretKey))
	_, err := full.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "CreateBucket")

	client := func(name string, permissions []string) *s3.Client {
		ak, sk := hiltCreateAccessKey(t, ctx, s, tenant, name, permissions, nil)
		return sdkClient(forgeS3Conf(endpoint, ak, sk))
	}
	aes256 := &types.ServerSideEncryptionConfiguration{Rules: []types.ServerSideEncryptionRule{{
		ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{SSEAlgorithm: types.ServerSideEncryptionAes256},
	}}}
	get := func(c *s3.Client) error {
		_, err := c.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
		return err
	}
	put := func(c *s3.Client) error {
		_, err := c.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{Bucket: aws.String(bucket), ServerSideEncryptionConfiguration: aes256})
		return err
	}
	del := func(c *s3.Client) error {
		_, err := c.DeleteBucketEncryption(ctx, &s3.DeleteBucketEncryptionInput{Bucket: aws.String(bucket)})
		return err
	}
	// denied runs the request once against hilt and once more with whatever
	// the first attempt left in ingot's caches, asserting AccessDenied both
	// times.
	denied := func(t *testing.T, what string, do func() error) {
		t.Helper()
		for attempt := 1; attempt <= 2; attempt++ {
			code, status := apiErrorOf(t, do())
			require.Equal(t, "AccessDenied", code, "%s (attempt %d): %s/%d", what, attempt, code, status)
			require.Equal(t, http.StatusForbidden, status, "%s (attempt %d): %s/%d", what, attempt, code, status)
		}
	}

	// Every permission but the two encryption ones, so the bucket's creator
	// and deleter is still refused its configuration. The allowed write
	// first seeds ingot's caches for the key.
	admin := client("bucket-admin", slices.DeleteFunc(slices.Clone(hiltAllPermissions), func(p string) bool {
		return p == "s3:GetEncryptionConfiguration" || p == "s3:PutEncryptionConfiguration"
	}))
	_, err = admin.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("seed"), Body: bytes.NewReader(patternBytes(1 << 10))})
	require.NoError(t, err, "seed write")
	// Refused before the configuration is looked up: an authorized read of an
	// unconfigured bucket answers 404, a denied one 403.
	denied(t, "bucket-admin key reading the configuration", func() error { return get(admin) })
	denied(t, "bucket-admin key writing the configuration", func() error { return put(admin) })
	denied(t, "bucket-admin key removing the configuration", func() error { return del(admin) })

	// The write permission writes and removes the configuration, as in S3,
	// and does not read it.
	writer := client("encryption-writer", []string{"s3:PutEncryptionConfiguration"})
	require.NoError(t, put(writer), "PutBucketEncryption with the write permission")
	denied(t, "write-only key reading the configuration", func() error { return get(writer) })

	// The read permission reads the configuration and nothing more.
	reader := client("encryption-reader", []string{"s3:GetEncryptionConfiguration"})
	got, err := reader.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "GetBucketEncryption with the read permission")
	rules := got.ServerSideEncryptionConfiguration.Rules
	require.Len(t, rules, 1, "GetBucketEncryption = %+v, want one rule naming AES256", rules)
	require.Equal(t, types.ServerSideEncryptionAes256, rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm, "GetBucketEncryption = %+v, want one rule naming AES256", rules)
	denied(t, "read-only key writing the configuration", func() error { return put(reader) })
	denied(t, "read-only key removing the configuration", func() error { return del(reader) })

	require.NoError(t, del(writer), "DeleteBucketEncryption with the write permission")
	code, status := apiErrorOf(t, get(full))
	require.Equal(t, "ServerSideEncryptionConfigurationNotFoundError", code, "GetBucketEncryption after the removal = %s/%d", code, status)
	require.Equal(t, http.StatusNotFound, status, "GetBucketEncryption after the removal = %s/%d", code, status)
}
