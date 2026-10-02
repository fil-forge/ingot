package bucketauthority

import (
	"context"
	"errors"
	"fmt"

	"github.com/fil-forge/hilt/pkg/bucketpolicy"
	hiltclient "github.com/fil-forge/hilt/pkg/client"
	bucketrpc "github.com/fil-forge/hilt/pkg/rpc/service/bucket"
	"github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	"github.com/fil-forge/ucantone/did"
	ucanerr "github.com/fil-forge/ucantone/errors"
)

var (
	ErrNotFound        = errors.New("bucketauthority: bucket not found")
	ErrNotEmpty        = errors.New("bucketauthority: bucket not empty")
	ErrExists          = errors.New("bucketauthority: bucket already exists")
	ErrAlreadyOwned    = errors.New("bucketauthority: bucket already owned by you")
	ErrInvalidArgument = errors.New("bucketauthority: invalid argument")

	// The bucket policy operations' rejections, from Hilt's /s3/bucket/policy.
	ErrNoPolicy            = errors.New("bucketauthority: bucket has no policy")
	ErrMalformedPolicy     = errors.New("bucketauthority: malformed policy")
	ErrPreconditionFailed  = errors.New("bucketauthority: precondition failed")
	ErrInvalidPrecondition = errors.New("bucketauthority: invalid precondition")
	ErrConcurrentChange    = errors.New("bucketauthority: policy is being changed concurrently")
	// ErrUnsupported is a policy write on an authority that keeps no policies.
	ErrUnsupported = errors.New("bucketauthority: bucket policies are not supported")
)

type BucketAuthority interface {
	CreateBucket(ctx context.Context, req s3.Request) (did.DID, error)
	DeleteBucket(ctx context.Context, req s3.Request) error
	ListBuckets(ctx context.Context, req s3.Request) (*s3bkt.ListOK, error)
	// BucketPolicy forwards a GetBucketPolicy, PutBucketPolicy or
	// DeleteBucketPolicy request; body is the policy document on a PUT.
	BucketPolicy(ctx context.Context, req s3.Request, body []byte) (*s3bkt.PolicyOK, error)
}

type Service struct {
	client *hiltclient.Client
}

var _ BucketAuthority = (*Service)(nil)

// New returns a BucketAuthority that forwards requests to Hilt.
func New(client *hiltclient.Client) *Service {
	return &Service{client: client}
}

func (s *Service) CreateBucket(ctx context.Context, req s3.Request) (did.DID, error) {
	createOK, _, err := s.client.CreateBucket(ctx, req)
	if err != nil {
		var namedErr ucanerr.Named
		if errors.As(err, &namedErr) {
			switch namedErr.Name() {
			case bucketrpc.BucketExistsErrorName:
				return did.Undef, ErrExists
			case bucketrpc.BucketAlreadyOwnedErrorName:
				return did.Undef, ErrAlreadyOwned
			case bucketpolicy.InvalidPolicyErrorName:
				// The policy the create carried in x-bucket-policy was refused;
				// no bucket exists.
				return did.Undef, fmt.Errorf("%w: %s", ErrMalformedPolicy, namedErr.Error())
			}
		}
		return did.Undef, err
	}
	if createOK.Bucket == nil {
		return did.Undef, errors.New("bucketauthority: bucket creation succeeded but no bucket info returned")
	}
	return *createOK.Bucket, nil
}

func (s *Service) DeleteBucket(ctx context.Context, req s3.Request) error {
	if err := s.client.DeleteBucket(ctx, req); err != nil {
		var namedErr ucanerr.Named
		if errors.As(err, &namedErr) {
			switch namedErr.Name() {
			case bucketrpc.UnknownBucketErrorName:
				return ErrNotFound
			case bucketrpc.BucketNotEmptyErrorName:
				return ErrNotEmpty
			}
		}
		return err
	}
	return nil
}

func (s *Service) BucketPolicy(ctx context.Context, req s3.Request, body []byte) (*s3bkt.PolicyOK, error) {
	ok, err := s.client.Policy(ctx, req, body)
	if err != nil {
		var namedErr ucanerr.Named
		if errors.As(err, &namedErr) {
			// The names are Hilt's policy service's (hilt/pkg/api/service/bucketpolicy)
			// and its policy package's; matched literally so the gateway does
			// not import the service's store and vault dependencies.
			switch namedErr.Name() {
			case bucketrpc.UnknownBucketErrorName:
				return nil, ErrNotFound
			case "PolicyNotFound":
				return nil, ErrNoPolicy
			case bucketpolicy.InvalidPolicyErrorName:
				return nil, fmt.Errorf("%w: %s", ErrMalformedPolicy, namedErr.Error())
			case "PreconditionFailed":
				return nil, ErrPreconditionFailed
			case "InvalidPrecondition":
				return nil, fmt.Errorf("%w: %s", ErrInvalidPrecondition, namedErr.Error())
			case "ConcurrentChange":
				return nil, ErrConcurrentChange
			}
		}
		return nil, err
	}
	return ok, nil
}

func (s *Service) ListBuckets(ctx context.Context, req s3.Request) (*s3bkt.ListOK, error) {
	listOK, err := s.client.ListBuckets(ctx, req)
	if err != nil {
		var namedErr ucanerr.Named
		if errors.As(err, &namedErr) && namedErr.Name() == bucketrpc.InvalidArgumentErrorName {
			return nil, ErrInvalidArgument
		}
		return nil, fmt.Errorf("bucketauthority: list: %w", err)
	}
	return listOK, nil
}
