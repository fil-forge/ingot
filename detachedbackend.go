package ingot

import (
	"bufio"
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3response"

	"github.com/fil-forge/ingot/internal/fasthttputil"
)

// requestBackend is what versitygw calls on the backend: its methods, and the
// Complete that reports its headers early.
type requestBackend interface {
	backend.Backend
	backend.CompleteMultipartUploadReporter
}

// detachedBackend hands b each request's context detached from fasthttp's
// request value (fasthttputil.DetachContext), which fasthttp reuses once the
// handler returns. It implements every method itself rather than embedding b,
// so a method versitygw adds fails to compile here instead of reaching b with
// fasthttp's value.
type detachedBackend struct {
	b requestBackend
}

var (
	_ backend.Backend                         = detachedBackend{}
	_ backend.CompleteMultipartUploadReporter = detachedBackend{}
)

// detachRequest is fasthttputil.DetachContext.
func detachRequest(ctx context.Context) context.Context {
	return fasthttputil.DetachContext(ctx)
}

func (d detachedBackend) String() string {
	return d.b.String()
}

func (d detachedBackend) CompleteMultipartUploadReportingHeaders(ctx context.Context, a1 *s3.CompleteMultipartUploadInput, a2 func(backend.CompleteMultipartUploadHeaders)) (s3response.CompleteMultipartUploadResult, string, error) {
	return d.b.CompleteMultipartUploadReportingHeaders(detachRequest(ctx), a1, a2)
}

func (d detachedBackend) Shutdown() {
	d.b.Shutdown()
}

func (d detachedBackend) NormalizeObjectKey(a0 string, a1 string) string {
	return d.b.NormalizeObjectKey(a0, a1)
}

func (d detachedBackend) ListBuckets(ctx context.Context, a1 s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	return d.b.ListBuckets(detachRequest(ctx), a1)
}

func (d detachedBackend) HeadBucket(ctx context.Context, a1 *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	return d.b.HeadBucket(detachRequest(ctx), a1)
}

func (d detachedBackend) GetBucketAcl(ctx context.Context, a1 *s3.GetBucketAclInput) ([]byte, error) {
	return d.b.GetBucketAcl(detachRequest(ctx), a1)
}

func (d detachedBackend) CreateBucket(ctx context.Context, a1 *s3.CreateBucketInput, a2 []byte) error {
	return d.b.CreateBucket(detachRequest(ctx), a1, a2)
}

func (d detachedBackend) PutBucketAcl(ctx context.Context, a1 string, a2 []byte) error {
	return d.b.PutBucketAcl(detachRequest(ctx), a1, a2)
}

func (d detachedBackend) DeleteBucket(ctx context.Context, a1 string) error {
	return d.b.DeleteBucket(detachRequest(ctx), a1)
}

func (d detachedBackend) PutBucketVersioning(ctx context.Context, a1 string, a2 types.BucketVersioningStatus) error {
	return d.b.PutBucketVersioning(detachRequest(ctx), a1, a2)
}

func (d detachedBackend) GetBucketVersioning(ctx context.Context, a1 string) (s3response.GetBucketVersioningOutput, error) {
	return d.b.GetBucketVersioning(detachRequest(ctx), a1)
}

func (d detachedBackend) PutBucketPolicy(ctx context.Context, a1 string, a2 []byte) error {
	return d.b.PutBucketPolicy(detachRequest(ctx), a1, a2)
}

func (d detachedBackend) GetBucketPolicy(ctx context.Context, a1 string) ([]byte, error) {
	return d.b.GetBucketPolicy(detachRequest(ctx), a1)
}

func (d detachedBackend) DeleteBucketPolicy(ctx context.Context, a1 string) error {
	return d.b.DeleteBucketPolicy(detachRequest(ctx), a1)
}

func (d detachedBackend) PutBucketOwnershipControls(ctx context.Context, a1 string, a2 types.ObjectOwnership) error {
	return d.b.PutBucketOwnershipControls(detachRequest(ctx), a1, a2)
}

func (d detachedBackend) GetBucketOwnershipControls(ctx context.Context, a1 string) (types.ObjectOwnership, error) {
	return d.b.GetBucketOwnershipControls(detachRequest(ctx), a1)
}

func (d detachedBackend) DeleteBucketOwnershipControls(ctx context.Context, a1 string) error {
	return d.b.DeleteBucketOwnershipControls(detachRequest(ctx), a1)
}

func (d detachedBackend) PutBucketCors(ctx context.Context, a1 string, a2 []byte) error {
	return d.b.PutBucketCors(detachRequest(ctx), a1, a2)
}

func (d detachedBackend) GetBucketCors(ctx context.Context, a1 string) ([]byte, error) {
	return d.b.GetBucketCors(detachRequest(ctx), a1)
}

func (d detachedBackend) DeleteBucketCors(ctx context.Context, a1 string) error {
	return d.b.DeleteBucketCors(detachRequest(ctx), a1)
}

func (d detachedBackend) PutBucketWebsite(ctx context.Context, a1 string, a2 []byte) error {
	return d.b.PutBucketWebsite(detachRequest(ctx), a1, a2)
}

func (d detachedBackend) GetBucketWebsite(ctx context.Context, a1 string) ([]byte, error) {
	return d.b.GetBucketWebsite(detachRequest(ctx), a1)
}

func (d detachedBackend) DeleteBucketWebsite(ctx context.Context, a1 string) error {
	return d.b.DeleteBucketWebsite(detachRequest(ctx), a1)
}

func (d detachedBackend) CreateMultipartUpload(ctx context.Context, a1 s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	return d.b.CreateMultipartUpload(detachRequest(ctx), a1)
}

func (d detachedBackend) CompleteMultipartUpload(ctx context.Context, a1 *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	return d.b.CompleteMultipartUpload(detachRequest(ctx), a1)
}

func (d detachedBackend) AbortMultipartUpload(ctx context.Context, a1 *s3.AbortMultipartUploadInput) error {
	return d.b.AbortMultipartUpload(detachRequest(ctx), a1)
}

func (d detachedBackend) ListMultipartUploads(ctx context.Context, a1 *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	return d.b.ListMultipartUploads(detachRequest(ctx), a1)
}

func (d detachedBackend) ListParts(ctx context.Context, a1 *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	return d.b.ListParts(detachRequest(ctx), a1)
}

func (d detachedBackend) UploadPart(ctx context.Context, a1 *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	return d.b.UploadPart(detachRequest(ctx), a1)
}

func (d detachedBackend) UploadPartCopy(ctx context.Context, a1 *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	return d.b.UploadPartCopy(detachRequest(ctx), a1)
}

func (d detachedBackend) PutObject(ctx context.Context, a1 s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	return d.b.PutObject(detachRequest(ctx), a1)
}

func (d detachedBackend) HeadObject(ctx context.Context, a1 *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	return d.b.HeadObject(detachRequest(ctx), a1)
}

func (d detachedBackend) GetObject(ctx context.Context, a1 *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return d.b.GetObject(detachRequest(ctx), a1)
}

func (d detachedBackend) GetObjectAcl(ctx context.Context, a1 *s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
	return d.b.GetObjectAcl(detachRequest(ctx), a1)
}

func (d detachedBackend) GetObjectAttributes(ctx context.Context, a1 *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	return d.b.GetObjectAttributes(detachRequest(ctx), a1)
}

func (d detachedBackend) CopyObject(ctx context.Context, a1 s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	return d.b.CopyObject(detachRequest(ctx), a1)
}

func (d detachedBackend) ListObjects(ctx context.Context, a1 *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	return d.b.ListObjects(detachRequest(ctx), a1)
}

func (d detachedBackend) ListObjectsV2(ctx context.Context, a1 *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	return d.b.ListObjectsV2(detachRequest(ctx), a1)
}

func (d detachedBackend) DeleteObject(ctx context.Context, a1 *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	return d.b.DeleteObject(detachRequest(ctx), a1)
}

func (d detachedBackend) DeleteObjects(ctx context.Context, a1 *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	return d.b.DeleteObjects(detachRequest(ctx), a1)
}

func (d detachedBackend) PutObjectAcl(ctx context.Context, a1 *s3.PutObjectAclInput) error {
	return d.b.PutObjectAcl(detachRequest(ctx), a1)
}

func (d detachedBackend) ListObjectVersions(ctx context.Context, a1 *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {
	return d.b.ListObjectVersions(detachRequest(ctx), a1)
}

func (d detachedBackend) RestoreObject(ctx context.Context, a1 *s3.RestoreObjectInput) error {
	return d.b.RestoreObject(detachRequest(ctx), a1)
}

func (d detachedBackend) SelectObjectContent(ctx context.Context, a1 *s3.SelectObjectContentInput) func(w *bufio.Writer) {
	return d.b.SelectObjectContent(detachRequest(ctx), a1)
}

func (d detachedBackend) GetBucketTagging(ctx context.Context, a1 string) (map[string]string, error) {
	return d.b.GetBucketTagging(detachRequest(ctx), a1)
}

func (d detachedBackend) PutBucketTagging(ctx context.Context, a1 string, a2 map[string]string) error {
	return d.b.PutBucketTagging(detachRequest(ctx), a1, a2)
}

func (d detachedBackend) DeleteBucketTagging(ctx context.Context, a1 string) error {
	return d.b.DeleteBucketTagging(detachRequest(ctx), a1)
}

func (d detachedBackend) GetObjectTagging(ctx context.Context, a1 string, a2 string, a3 string) (map[string]string, error) {
	return d.b.GetObjectTagging(detachRequest(ctx), a1, a2, a3)
}

func (d detachedBackend) PutObjectTagging(ctx context.Context, a1 string, a2 string, a3 string, a4 map[string]string) error {
	return d.b.PutObjectTagging(detachRequest(ctx), a1, a2, a3, a4)
}

func (d detachedBackend) DeleteObjectTagging(ctx context.Context, a1 string, a2 string, a3 string) error {
	return d.b.DeleteObjectTagging(detachRequest(ctx), a1, a2, a3)
}

func (d detachedBackend) PutBucketEncryption(ctx context.Context, a1 string, a2 s3response.ServerSideEncryptionConfiguration) error {
	return d.b.PutBucketEncryption(detachRequest(ctx), a1, a2)
}

func (d detachedBackend) GetBucketEncryption(ctx context.Context, a1 string) (s3response.ServerSideEncryptionConfiguration, error) {
	return d.b.GetBucketEncryption(detachRequest(ctx), a1)
}

func (d detachedBackend) DeleteBucketEncryption(ctx context.Context, a1 string) error {
	return d.b.DeleteBucketEncryption(detachRequest(ctx), a1)
}

func (d detachedBackend) PutObjectLockConfiguration(ctx context.Context, a1 string, a2 []byte) error {
	return d.b.PutObjectLockConfiguration(detachRequest(ctx), a1, a2)
}

func (d detachedBackend) GetObjectLockConfiguration(ctx context.Context, a1 string) ([]byte, error) {
	return d.b.GetObjectLockConfiguration(detachRequest(ctx), a1)
}

func (d detachedBackend) PutObjectRetention(ctx context.Context, a1 string, a2 string, a3 string, a4 []byte) error {
	return d.b.PutObjectRetention(detachRequest(ctx), a1, a2, a3, a4)
}

func (d detachedBackend) GetObjectRetention(ctx context.Context, a1 string, a2 string, a3 string) ([]byte, error) {
	return d.b.GetObjectRetention(detachRequest(ctx), a1, a2, a3)
}

func (d detachedBackend) PutObjectLegalHold(ctx context.Context, a1 string, a2 string, a3 string, a4 bool) error {
	return d.b.PutObjectLegalHold(detachRequest(ctx), a1, a2, a3, a4)
}

func (d detachedBackend) GetObjectLegalHold(ctx context.Context, a1 string, a2 string, a3 string) (*bool, error) {
	return d.b.GetObjectLegalHold(detachRequest(ctx), a1, a2, a3)
}

func (d detachedBackend) ChangeBucketOwner(ctx context.Context, a1 string, a2 string) error {
	return d.b.ChangeBucketOwner(detachRequest(ctx), a1, a2)
}

func (d detachedBackend) ListBucketsAndOwners(ctx context.Context) ([]s3response.Bucket, error) {
	return d.b.ListBucketsAndOwners(detachRequest(ctx))
}
