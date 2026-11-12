package preload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	objects map[string]int64  // key → size
	bodies  map[string][]byte // key → body, for objects that have one
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
	f.buckets[*in.Bucket] = &fakeBucket{objects: make(map[string]int64), bodies: make(map[string][]byte)}
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
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	n := int64(len(body))
	h := sha256.New()
	h.Write(body)
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
	if n > 0 {
		b.bodies[*in.Key] = body
	}
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

// object returns bucket's object at key, or a 404.
func (f *fakeS3) object(bucket, key string) (int64, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.buckets[bucket]
	if !ok {
		return 0, nil, &statusError{404}
	}
	size, ok := b.objects[key]
	if !ok {
		return 0, nil, &statusError{404}
	}
	return size, b.bodies[key], nil
}

func (f *fakeS3) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	size, _, err := f.object(*in.Bucket, *in.Key)
	if err != nil {
		return nil, err
	}
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(size)}, nil
}

// GetObject serves a range of the form bytes=first-last, as a validation
// asks for.
func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	size, body, err := f.object(*in.Bucket, *in.Key)
	if err != nil {
		return nil, err
	}
	spec, ok := strings.CutPrefix(aws.ToString(in.Range), "bytes=")
	first, last, ok2 := strings.Cut(spec, "-")
	lo, err1 := strconv.ParseInt(first, 10, 64)
	hi, err2 := strconv.ParseInt(last, 10, 64)
	if !ok || !ok2 || err1 != nil || err2 != nil || lo > hi || hi >= size {
		return nil, &statusError{416}
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(body[lo : hi+1])),
		ContentLength: aws.Int64(hi - lo + 1),
		ContentRange:  aws.String(fmt.Sprintf("bytes %d-%d/%d", lo, hi, size)),
	}, nil
}

// fakeListPage is how many keys a fake listing returns at a time.
const fakeListPage = 7

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &statusError{404}
	}
	var keys []string
	for k := range b.objects {
		if strings.HasPrefix(k, aws.ToString(in.Prefix)) && k > aws.ToString(in.ContinuationToken) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(len(keys) > fakeListPage)}
	if len(keys) > fakeListPage {
		keys = keys[:fakeListPage]
		out.NextContinuationToken = aws.String(keys[len(keys)-1])
	}
	for _, k := range keys {
		out.Contents = append(out.Contents, types.Object{Key: aws.String(k), Size: aws.Int64(b.objects[k])})
	}
	return out, nil
}

func (f *fakeS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if b, ok := f.buckets[*in.Bucket]; ok {
		delete(b.objects, *in.Key)
		delete(b.bodies, *in.Key)
	}
	return &s3.DeleteObjectOutput{}, nil
}
