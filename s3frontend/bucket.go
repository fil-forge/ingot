package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
)

func (b *Backend) ListBuckets(ctx context.Context, input s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	req, ok := reqscope.Request(ctx)
	if !ok {
		return s3response.ListAllMyBucketsResult{}, errors.New("s3frontend: create bucket: no request in context")
	}
	page, err := b.authority.ListBuckets(ctx, req)
	if err != nil {
		if errors.Is(err, bucketauthority.ErrInvalidArgument) {
			return s3response.ListAllMyBucketsResult{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
		}
		return s3response.ListAllMyBucketsResult{}, err
	}

	entries := make([]s3response.ListAllMyBucketsEntry, 0, len(page.Buckets))
	for _, st := range page.Buckets {
		creationDate, err := time.Parse(time.RFC3339, st.CreationDate)
		if err != nil {
			return s3response.ListAllMyBucketsResult{}, fmt.Errorf("parsing creation date: %w", err)
		}
		entries = append(entries, s3response.ListAllMyBucketsEntry{
			Name:         st.Name,
			BucketRegion: st.Region,
			CreationDate: creationDate,
		})
	}

	return s3response.ListAllMyBucketsResult{
		Buckets: s3response.ListAllMyBucketsList{Bucket: entries},
		Owner: s3response.CanonicalUser{
			ID:          page.Owner.ID,
			DisplayName: page.Owner.DisplayName,
		},
		Prefix:            input.Prefix,
		ContinuationToken: page.ContinuationToken,
	}, nil
}

// GetBucketAcl is invoked on every object op via versitygw's ParseAcl
// middleware to capture the bucket owner before the controller runs
// (acl-parser.go:30). We don't model ACLs — but returning the
// BackendUnsupported default (ErrNotImplemented) propagates as
// "header you provided implies functionality that is not implemented"
// for *every* PUT/GET/DELETE. Returning empty bytes for a known
// bucket lets ParseACL produce ACL{}, after which the middleware
// substitutes the configured root access key as the owner.
func (b *Backend) GetBucketAcl(ctx context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if _, err := b.reg.Get(ctx, *input.Bucket); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, err
	}
	return nil, nil
}

// GetObjectLockConfiguration is called from auth.CheckObjectAccess
// (object_lock.go:223) on every object PUT/DELETE. The caller only
// tolerates ErrObjectLockConfigurationNotFound; ErrNotImplemented
// propagates as "header you provided implies functionality not
// implemented" — ingot doesn't model object lock today, so the
// honest answer is "no configuration."
func (b *Backend) GetObjectLockConfiguration(ctx context.Context, bucket string) ([]byte, error) {
	if _, err := b.reg.Get(ctx, bucket); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, err
	}
	return nil, s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)
}

// GetBucketPolicy is called from auth.VerifyAccess (access-control.go:103)
// for non-root requests and from auth.VerifyPublicAccess for anonymous
// ones. Authenticated root requests short-circuit before this is hit
// today, but stubbing it now keeps non-root authz paths from tripping
// the same NotImplemented trap.
func (b *Backend) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	if _, err := b.reg.Get(ctx, bucket); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, err
	}
	return nil, s3err.GetAPIError(s3err.ErrNoSuchBucketPolicy)
}

// GetBucketVersioning is called from auth.CheckObjectAccess
// (object_lock.go:220, 257). Both call sites tolerate any error by
// treating versioning as disabled, so we could leave the default
// ErrNotImplemented — but returning a clean "Suspended" status is
// less noisy in logs and makes the no-op intent explicit.
func (b *Backend) GetBucketVersioning(ctx context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	if _, err := b.reg.Get(ctx, bucket); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.GetBucketVersioningOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.GetBucketVersioningOutput{}, err
	}
	return s3response.GetBucketVersioningOutput{}, nil
}

func (b *Backend) HeadBucket(ctx context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if _, err := b.reg.Get(ctx, *input.Bucket); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, err
	}
	return &s3.HeadBucketOutput{}, nil
}

func (b *Backend) CreateBucket(ctx context.Context, input *s3.CreateBucketInput, _ []byte) error {
	if input.Bucket == nil {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	// strings.Clone: versitygw passes us a fiber.Ctx.Params() string
	// whose backing buffer is recycled when the request completes.
	// Storing it directly in the registry produces map-key corruption
	// once the buffer is reused for the next request.
	name := strings.Clone(*input.Bucket)
	if !validBucketName(name) {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	req, ok := reqscope.Request(ctx)
	if !ok {
		return errors.New("s3frontend: create bucket: no request in context")
	}
	id, err := b.authority.CreateBucket(ctx, req)
	if err != nil {
		if errors.Is(err, bucketauthority.ErrExists) {
			return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
		}
		return err
	}
	if err := b.reg.Create(ctx, name, id); err != nil {
		if errors.Is(err, registry.ErrExists) {
			return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
		}
		return err
	}
	return nil
}

func (b *Backend) DeleteBucket(ctx context.Context, name string) error {
	return b.txns.WithLock(ctx, name, func(ctx context.Context) error {
		st, err := b.reg.Get(ctx, name)
		if err != nil {
			if errors.Is(err, registry.ErrNotFound) {
				return s3err.GetAPIError(s3err.ErrNoSuchBucket)
			}
			return err
		}

		// S3 forbids deleting non-empty buckets. Walk the MST until
		// we see any leaf, then bail.
		if st.Root.Defined() {
			t := mst.LoadMST(b.read, st.Space, st.Root)
			var seen bool
			walkErr := t.WalkLeavesFromNocache(ctx, "", func(string, cid.Cid) error {
				seen = true
				return mst.ErrStopWalk
			})
			if walkErr != nil {
				return walkErr
			}
			if seen {
				return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
			}
		}

		req, ok := reqscope.Request(ctx)
		if !ok {
			return errors.New("s3frontend: delete bucket: no request in context")
		}
		if err := b.authority.DeleteBucket(ctx, req); err != nil {
			if errors.Is(err, bucketauthority.ErrNotFound) {
				return s3err.GetAPIError(s3err.ErrNoSuchBucket)
			} else if errors.Is(err, bucketauthority.ErrNotEmpty) {
				return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
			}
			return err
		}

		if err := b.reg.Delete(ctx, name); err != nil {
			if errors.Is(err, registry.ErrNotFound) {
				return s3err.GetAPIError(s3err.ErrNoSuchBucket)
			} else if errors.Is(err, registry.ErrNotEmpty) {
				return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
			}
			return err
		}

		// The bucket's segment history dies with it. Best-effort: the
		// bucket is already deleted, so a cleanup failure must not fail
		// the API call.
		if remover, ok := b.log.(interface {
			RemoveBucketLog(ctx context.Context, bucket string) error
		}); ok {
			if rerr := remover.RemoveBucketLog(ctx, name); rerr != nil {
				b.logger.Warn("bucket log cleanup failed",
					zap.String("bucket", name), zap.Error(rerr))
			}
		}
		return nil
	})
}

// validBucketName mirrors the rules from the prior bucket.Service:
// 3-63 chars, lowercase letters, digits, dots, dashes; cannot begin
// with a dot or dash. This is the S3 DNS-compliant subset.
func validBucketName(s string) bool {
	if len(s) < 3 || len(s) > 63 {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '.':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}
