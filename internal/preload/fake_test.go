package preload

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// statusError is an error response with an HTTP status, as the SDK reports
// one.
type statusError struct{ code int }

func (e *statusError) Error() string       { return fmt.Sprintf("status %d", e.code) }
func (e *statusError) HTTPStatusCode() int { return e.code }

// fakeS3 is an in-memory S3 that checks each PUT the way a load must send
// it: create-only, with the body's SHA-256.
type fakeS3 struct {
	mu      sync.Mutex
	buckets map[string]*fakeBucket
	creates int
	// fail, when set, may fail a PUT before it is applied.
	fail func(ctx context.Context, bucket, key string) error
	// after, when set, runs after each stored PUT.
	after  func(stored int)
	stored int
}

type fakeBucket struct {
	sse     string
	objects map[string]int64 // key → size
}

func newFakeS3() *fakeS3 {
	return &fakeS3{buckets: make(map[string]*fakeBucket)}
}

func (f *fakeS3) CreateBucket(_ context.Context, in *s3.CreateBucketInput, _ ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates++
	if _, ok := f.buckets[*in.Bucket]; ok {
		return nil, &smithy.GenericAPIError{Code: "BucketAlreadyOwnedByYou"}
	}
	f.buckets[*in.Bucket] = &fakeBucket{objects: make(map[string]int64)}
	return &s3.CreateBucketOutput{}, nil
}

func (f *fakeS3) PutBucketEncryption(_ context.Context, in *s3.PutBucketEncryptionInput, _ ...func(*s3.Options)) (*s3.PutBucketEncryptionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &statusError{404}
	}
	b.sse = string(in.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm)
	return &s3.PutBucketEncryptionOutput{}, nil
}

func (f *fakeS3) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if in.IfNoneMatch == nil || *in.IfNoneMatch != "*" {
		return nil, fmt.Errorf("PUT of %s is not create-only", *in.Key)
	}
	h := sha256.New()
	n, err := io.Copy(h, in.Body)
	if err != nil {
		return nil, err
	}
	if in.ContentLength == nil || *in.ContentLength != n {
		return nil, fmt.Errorf("PUT of %s: content length %v, body %d bytes", *in.Key, in.ContentLength, n)
	}
	if got := base64.StdEncoding.EncodeToString(h.Sum(nil)); in.ChecksumSHA256 == nil || *in.ChecksumSHA256 != got {
		return nil, fmt.Errorf("PUT of %s: checksum %v, body %s", *in.Key, in.ChecksumSHA256, got)
	}
	if f.fail != nil {
		if err := f.fail(ctx, *in.Bucket, *in.Key); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		f.mu.Unlock()
		return nil, &statusError{404}
	}
	if _, ok := b.objects[*in.Key]; ok {
		f.mu.Unlock()
		return nil, &statusError{412}
	}
	b.objects[*in.Key] = n
	f.stored++
	stored := f.stored
	f.mu.Unlock()
	if f.after != nil {
		f.after(stored)
	}
	return &s3.PutObjectOutput{}, nil
}

// objects returns a copy of bucket's objects.
func (f *fakeS3) objects(bucket string) map[string]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int64)
	if b, ok := f.buckets[bucket]; ok {
		for k, v := range b.objects {
			out[k] = v
		}
	}
	return out
}
