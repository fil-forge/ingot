package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
)

// This file is the S3 face of bucket encryption: the bucket-encryption API
// (Get/Put/DeleteBucketEncryption) and the x-amz-server-side-encryption
// request and response headers of the object operations.
//
// Every object is stored encrypted (encrypt.go) unless its bucket's default
// encryption is the non-standard SSEAlgorithm "none", which a deployment
// opts into (Deps.AllowNoneEncryption) for tenants whose data is encrypted
// before it reaches Ingot. S3's own vocabulary maps onto that: SSE-S3
// (AES256) is the encryption every object gets anyway, so a request or
// bucket asking for it changes nothing on an encrypting bucket and turns
// encryption on for one object of a "none" bucket; SSE-KMS and SSE-C are not
// implemented. A stored object reports AES256 when encrypted and no
// encryption header at all when stored as received.

// sseNone is the non-standard SSEAlgorithm under which objects are stored as
// received.
const sseNone types.ServerSideEncryption = "none"

// errSSEArgument is S3's InvalidArgument for an x-amz-server-side-encryption
// header set it does not accept.
func errSSEArgument(description string) error {
	return s3err.APIError{
		Code:           "InvalidArgument",
		Description:    description,
		HTTPStatusCode: http.StatusBadRequest,
	}
}

// requestedEncryption reads a write request's server-side-encryption
// headers and returns the algorithm the request asks for: "" when it asks
// for none, else AES256. The request's other possibilities each end here:
// an algorithm S3 does not know is an InvalidArgument, as is a KMS key id or
// context without aws:kms, or SSE-C headers beside an algorithm; aws:kms and
// SSE-C on their own are NotImplemented. Only header presence and the
// algorithm value are inspected; the (sensitive) SSE-C key value is never
// read or logged.
func requestedEncryption(ctx context.Context) (types.ServerSideEncryption, error) {
	req, ok := reqscope.Request(ctx)
	if !ok {
		return "", nil
	}
	var algo, kmsKeyID, kmsContext string
	var customer, copySourceCustomer bool
	for k, v := range req.Headers {
		switch lk := strings.ToLower(k); {
		case lk == "x-amz-server-side-encryption":
			algo = v
		case lk == "x-amz-server-side-encryption-aws-kms-key-id":
			kmsKeyID = v
		case lk == "x-amz-server-side-encryption-context":
			kmsContext = v
		case strings.HasPrefix(lk, "x-amz-server-side-encryption-customer-"):
			customer = true
		case strings.HasPrefix(lk, "x-amz-copy-source-server-side-encryption-customer-"):
			copySourceCustomer = true
		}
	}
	sse := types.ServerSideEncryption(algo)
	kms := sse == types.ServerSideEncryptionAwsKms || sse == types.ServerSideEncryptionAwsKmsDsse
	switch {
	case customer && algo != "":
		return "", errSSEArgument("Server Side Encryption with Customer provided key is incompatible with the encryption method specified")
	case algo != "" && sse != types.ServerSideEncryptionAes256 && !kms:
		return "", errSSEArgument("The encryption method specified is not supported")
	case (kmsKeyID != "" || kmsContext != "") && !kms:
		return "", errSSEArgument("x-amz-server-side-encryption-aws-kms-key-id and x-amz-server-side-encryption-context require x-amz-server-side-encryption: aws:kms")
	case kms || customer || copySourceCustomer:
		return "", s3err.GetAPIError(s3err.ErrNotImplemented)
	}
	return sse, nil
}

// rejectEncryptionHeaders fails a read request that carries any
// x-amz-server-side-encryption header: a GET or HEAD has no encryption to
// choose, as on S3.
func rejectEncryptionHeaders(ctx context.Context) error {
	req, ok := reqscope.Request(ctx)
	if !ok {
		return nil
	}
	for k := range req.Headers {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-server-side-encryption") {
			return errSSEArgument("x-amz-server-side-encryption header is not supported for this operation")
		}
	}
	return nil
}

// storesPlaintext decides how a new object of bucket is stored: as received
// only when the bucket's default encryption is "none" and the request did
// not ask for AES256 itself.
func storesPlaintext(bucket *registry.State, requested types.ServerSideEncryption) bool {
	return bucket.Encryption == registry.BucketEncryptionNone && requested == ""
}

// encryptionOf is the x-amz-server-side-encryption a stored object reports:
// AES256 for an encrypted body, nothing for one stored as received.
func encryptionOf(mf *msbucket.ObjectManifest) types.ServerSideEncryption {
	return encryptionFor(mf.Plaintext)
}

// encryptionFor is encryptionOf for a body not yet committed, such as a
// multipart session's.
func encryptionFor(plaintext bool) types.ServerSideEncryption {
	if plaintext {
		return ""
	}
	return types.ServerSideEncryptionAes256
}

// GetBucketEncryption reports the bucket's default encryption as
// PutBucketEncryption stored it. A bucket that was never configured, or
// whose configuration was deleted, answers
// ServerSideEncryptionConfigurationNotFoundError, as on S3 — its objects are
// encrypted all the same.
func (b *Backend) GetBucketEncryption(ctx context.Context, bucket string) (s3response.ServerSideEncryptionConfiguration, error) {
	st, err := b.reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.ServerSideEncryptionConfiguration{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.ServerSideEncryptionConfiguration{}, err
	}
	if st.Encryption == registry.BucketEncryptionUnset {
		return s3response.ServerSideEncryptionConfiguration{}, s3err.GetAPIError(s3err.ErrServerSideEncryptionConfigurationNotFound)
	}
	return s3response.ServerSideEncryptionConfiguration{
		Rules: []s3response.ServerSideEncryptionRule{{
			ApplyServerSideEncryptionByDefault: &s3response.ServerSideEncryptionByDefault{
				SSEAlgorithm: types.ServerSideEncryption(st.Encryption),
			},
		}},
	}, nil
}

// PutBucketEncryption sets the bucket's default encryption. The controller
// has checked the document's shape (one rule with an SSEAlgorithm); the
// algorithm is judged here: AES256 is accepted and changes nothing, "none"
// is accepted only on a deployment that allows it, aws:kms is not
// implemented, and anything else is an InvalidArgument. A KMS key id beside
// AES256 is rejected as on S3. The setting applies to objects written after
// it; objects already stored keep the encryption they were written with.
func (b *Backend) PutBucketEncryption(ctx context.Context, bucket string, config s3response.ServerSideEncryptionConfiguration) error {
	if len(config.Rules) != 1 || config.Rules[0].ApplyServerSideEncryptionByDefault == nil {
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	byDefault := config.Rules[0].ApplyServerSideEncryptionByDefault
	var enc registry.BucketEncryption
	switch byDefault.SSEAlgorithm {
	case types.ServerSideEncryptionAes256:
		if byDefault.KMSMasterKeyID != nil {
			return errSSEArgument("KMSMasterKeyID can only be specified with aws:kms")
		}
		enc = registry.BucketEncryptionAES256
	case types.ServerSideEncryptionAwsKms, types.ServerSideEncryptionAwsKmsDsse:
		return s3err.GetAPIError(s3err.ErrNotImplemented)
	case sseNone:
		if !b.allowNoneEncryption {
			return errSSEArgument(fmt.Sprintf("SSEAlgorithm %q is not enabled on this deployment", sseNone))
		}
		enc = registry.BucketEncryptionNone
	default:
		return errSSEArgument("The encryption method specified is not supported")
	}
	if err := b.reg.SetEncryption(ctx, bucket, enc); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return fmt.Errorf("s3frontend: put bucket encryption: %w", err)
	}
	return nil
}

// DeleteBucketEncryption clears the bucket's default encryption, returning
// it to the unconfigured state: objects written from here on are encrypted,
// and GetBucketEncryption answers not found. Idempotent.
func (b *Backend) DeleteBucketEncryption(ctx context.Context, bucket string) error {
	if err := b.reg.SetEncryption(ctx, bucket, registry.BucketEncryptionUnset); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return fmt.Errorf("s3frontend: delete bucket encryption: %w", err)
	}
	return nil
}
