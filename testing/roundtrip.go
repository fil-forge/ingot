package testing

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// These helpers do controlled single-object S3 round-trips against a running
// ingot listener, for tests that need to drive a specific PUT/GET sequence
// (e.g. the forge read-after-eviction e2e) rather than the whole versity suite.
// They keep the AWS SDK contained here so callers (incl. smelt) need only this
// package.

func s3Client(ctx context.Context, c Config) (*s3.Client, error) {
	region := c.Region
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(c.Endpoint)
		o.UsePathStyle = true
	}), nil
}

// CreateBucket creates bucket on the ingot listener at c.
func CreateBucket(ctx context.Context, c Config, bucket string) error {
	cl, err := s3Client(ctx, c)
	if err != nil {
		return err
	}
	_, err = cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket})
	return err
}

// PutBytes uploads body as object key in bucket.
func PutBytes(ctx context.Context, c Config, bucket, key string, body []byte) error {
	cl, err := s3Client(ctx, c)
	if err != nil {
		return err
	}
	_, err = cl.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(body),
	})
	return err
}

// GetBytes downloads object key from bucket.
func GetBytes(ctx context.Context, c Config, bucket, key string) ([]byte, error) {
	cl, err := s3Client(ctx, c)
	if err != nil {
		return nil, err
	}
	out, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}
