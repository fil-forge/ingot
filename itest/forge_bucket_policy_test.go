//go:build itest

package itest

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	"github.com/stretchr/testify/require"
)

// TestForgeBucketPolicy drives the S3 bucket policy operations end to end: a
// service key writes an AWS-shaped policy naming a principal by its user ARN
// with PutBucketPolicy, reads it back canonical with GetBucketPolicy (both
// answering the document's ETag), replaces it with a wildcard document, is
// refused a document versitygw rejects (a bare principal name) and one Hilt
// rejects (no Version) with MalformedPolicy, and removes it with
// DeleteBucketPolicy.
func TestForgeBucketPolicy(t *testing.T) {
	s, endpoint := forgeStack(t)
	ctx := t.Context()
	const tenant, principal, bucket = "bucketpolicy", "member", "policied"
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, tenant)
	client := sdkClient(forgeS3Conf(endpoint, accessKey, secretKey))

	// The policy names a principal, which the tenant must hold.
	out, errOut, err := s.Exec(ctx, "hilt", "curl", "-sS", "-f", "-X", "PUT",
		"http://localhost:80/tenants/"+tenant+"/principals/"+principal,
		"-H", "Authorization: Bearer dev-partner-key")
	require.NoError(t, err, "hilt create principal (stdout=%s stderr=%s)", out, errOut)
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "CreateBucket")

	_, err = client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	require.True(t, isS3Code(err, "NoSuchBucketPolicy"), "GetBucketPolicy before any write: got %v, want NoSuchBucketPolicy", err)

	// Written out of order and with a duplicate; read back sorted and deduplicated.
	objects := s3bkt.ObjectsARN(bucket)
	written := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":[%q]},"Action":["s3:PutObject","s3:GetObject","s3:PutObject"],"Resource":%q}]}`,
		s3bkt.UserARN(principal), objects)
	put, err := client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(written)})
	require.NoError(t, err, "PutBucketPolicy")
	putETag := responseETag(put.ResultMetadata)
	require.NotEmpty(t, putETag, "PutBucketPolicy answered no ETag")
	got, err := client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "GetBucketPolicy")
	want := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":[%q]},"Action":["s3:GetObject","s3:PutObject"],"Resource":[%q]}]}`,
		s3bkt.UserARN(principal), objects)
	require.Equal(t, want, aws.ToString(got.Policy), "GetBucketPolicy")
	require.Equal(t, putETag, responseETag(got.ResultMetadata), "GetBucketPolicy ETag against the PutBucketPolicy ETag")

	wildcard := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":%q}]}`, objects)
	_, err = client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(wildcard)})
	require.NoError(t, err, "PutBucketPolicy with a wildcard principal")

	for name, doc := range map[string]string{
		// versitygw's validation finds no account for a bare name.
		"a bare principal name": fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":[%q]},"Action":"s3:GetObject","Resource":%q}]}`, principal, objects),
		// versitygw defaults the Version; Hilt requires it.
		"no Version": fmt.Sprintf(`{"Statement":[{"Effect":"Allow","Principal":{"AWS":[%q]},"Action":"s3:GetObject","Resource":%q}]}`, s3bkt.UserARN(principal), objects),
	} {
		_, err = client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(doc)})
		require.True(t, isS3Code(err, "MalformedPolicy"), "PutBucketPolicy with %s: got %v, want MalformedPolicy", name, err)
	}

	_, err = client.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "DeleteBucketPolicy")
	_, err = client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	require.True(t, isS3Code(err, "NoSuchBucketPolicy"), "GetBucketPolicy after the delete: got %v, want NoSuchBucketPolicy", err)
	_, err = client.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)})
	require.True(t, isS3Code(err, "NoSuchBucketPolicy"), "DeleteBucketPolicy twice: got %v, want NoSuchBucketPolicy", err)
}

// responseETag returns the ETag header of the response an SDK output came from.
func responseETag(md middleware.Metadata) string {
	if res, ok := awsmiddleware.GetRawResponse(md).(*smithyhttp.Response); ok {
		return res.Header.Get("ETag")
	}
	return ""
}

// isS3Code reports whether err is an S3 API error with the given code.
func isS3Code(err error, code string) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && strings.EqualFold(apiErr.ErrorCode(), code)
}
