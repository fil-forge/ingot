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
)

// TestForgeBucketPolicy drives the S3 bucket policy operations end to end: a
// service key writes an AWS-shaped policy naming a principal by its user ARN
// with PutBucketPolicy, reads it back canonical with GetBucketPolicy (both
// answering the document's ETag), replaces it with a wildcard document, is
// refused a document versitygw rejects (a bare principal name) and one Hilt
// rejects (no Version) with MalformedPolicy, and removes it with
// DeleteBucketPolicy. The published hilt image lacks /s3/bucket/policy until
// fil-forge/hilt#89 merges, so the scenario runs only against a hilt override
// (or when INGOT_ITEST_IAM=1 says the image carries it); drop the guard then.
func TestForgeBucketPolicy(t *testing.T) {
	if !iamScenariosEnabled() {
		t.Skip("the bucket policy scenario needs a hilt override (INGOT_ITEST_HILT_BINARY or INGOT_ITEST_HILT_IMAGE), or INGOT_ITEST_IAM=1, until the :main image serves /s3/bucket/policy")
	}
	s, endpoint := forgeStack(t)
	ctx := t.Context()
	const tenant, principal, bucket = "bucketpolicy", "member", "policied"
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, tenant)
	client := sdkClient(forgeS3Conf(endpoint, accessKey, secretKey))

	// The policy names a principal, which the tenant must hold.
	if out, errOut, err := s.Exec(ctx, "hilt", "curl", "-sS", "-f", "-X", "PUT",
		"http://localhost:80/tenants/"+tenant+"/principals/"+principal,
		"-H", "Authorization: Bearer dev-partner-key"); err != nil {
		t.Fatalf("hilt create principal: %v (stdout=%s stderr=%s)", err, out, errOut)
	}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	if _, err := client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)}); !isS3Code(err, "NoSuchBucketPolicy") {
		t.Fatalf("GetBucketPolicy before any write: got %v, want NoSuchBucketPolicy", err)
	}

	// Written out of order and with a duplicate; read back sorted and deduplicated.
	objects := s3bkt.ObjectsARN(bucket)
	written := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":[%q]},"Action":["s3:PutObject","s3:GetObject","s3:PutObject"],"Resource":%q}]}`,
		s3bkt.UserARN(principal), objects)
	put, err := client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(written)})
	if err != nil {
		t.Fatalf("PutBucketPolicy: %v", err)
	}
	putETag := responseETag(put.ResultMetadata)
	if putETag == "" {
		t.Fatal("PutBucketPolicy answered no ETag")
	}
	got, err := client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketPolicy: %v", err)
	}
	want := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":[%q]},"Action":["s3:GetObject","s3:PutObject"],"Resource":[%q]}]}`,
		s3bkt.UserARN(principal), objects)
	if aws.ToString(got.Policy) != want {
		t.Fatalf("GetBucketPolicy = %s, want %s", aws.ToString(got.Policy), want)
	}
	if getETag := responseETag(got.ResultMetadata); getETag != putETag {
		t.Fatalf("GetBucketPolicy ETag = %q, want the PutBucketPolicy ETag %q", getETag, putETag)
	}

	wildcard := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":%q}]}`, objects)
	if _, err := client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(wildcard)}); err != nil {
		t.Fatalf("PutBucketPolicy with a wildcard principal: %v", err)
	}

	for name, doc := range map[string]string{
		// versitygw's validation finds no account for a bare name.
		"a bare principal name": fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":[%q]},"Action":"s3:GetObject","Resource":%q}]}`, principal, objects),
		// versitygw defaults the Version; Hilt requires it.
		"no Version": fmt.Sprintf(`{"Statement":[{"Effect":"Allow","Principal":{"AWS":[%q]},"Action":"s3:GetObject","Resource":%q}]}`, s3bkt.UserARN(principal), objects),
	} {
		if _, err := client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(doc)}); !isS3Code(err, "MalformedPolicy") {
			t.Fatalf("PutBucketPolicy with %s: got %v, want MalformedPolicy", name, err)
		}
	}

	if _, err := client.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucketPolicy: %v", err)
	}
	if _, err := client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)}); !isS3Code(err, "NoSuchBucketPolicy") {
		t.Fatalf("GetBucketPolicy after the delete: got %v, want NoSuchBucketPolicy", err)
	}
	if _, err := client.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)}); !isS3Code(err, "NoSuchBucketPolicy") {
		t.Fatalf("DeleteBucketPolicy twice: got %v, want NoSuchBucketPolicy", err)
	}
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
