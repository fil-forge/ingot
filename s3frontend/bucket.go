package s3frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
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

// GetBucketCors is the single seam versitygw drives all of its CORS
// behaviour off: ApplyBucketCORS is attached to every bucket/object
// route (s3api/router.go:188) and ctrl.CORSOptions answers preflights
// from the same document (s3api/controllers/options.go:29) — both of
// which fall through untouched while the BackendUnsupported default
// (ErrNotImplemented) stands, which is why serving a document here is
// all CORS support takes.
//
// The document is gateway-wide, rendered from cors_allowed_origins
// (internal/cors) and marshalled once in New: ingot doesn't model
// per-bucket CORS, so every bucket reports the same rules and
// PutBucketCors/DeleteBucketCors stay unimplemented. The disabled check
// comes first so the common case — no CORS configured — costs no
// registry lookup on requests that carry an Origin header.
func (b *Backend) GetBucketCors(ctx context.Context, bucket string) ([]byte, error) {
	if len(b.cors) == 0 {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchCORSConfiguration)
	}
	if _, err := b.reg.Get(ctx, bucket); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, err
	}
	return b.cors, nil
}

// GetObjectLockConfiguration returns the bucket's stored lock configuration
// document (the controller's auth.BucketLockConfig JSON, verbatim), or
// ErrObjectLockConfigurationNotFound when the bucket has never been
// configured. Called from auth.CheckObjectAccess (object_lock.go:223) on
// every object PUT/DELETE, which tolerates exactly that sentinel — so this
// stays one registry Get (docs/s3-object-lock.md §5).
func (b *Backend) GetObjectLockConfiguration(ctx context.Context, bucket string) ([]byte, error) {
	st, err := b.reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, err
	}
	if st.ObjectLockConfig == nil {
		return nil, s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)
	}
	return st.ObjectLockConfig, nil
}

// PutObjectLockConfiguration stores the bucket's lock configuration. The
// controller has already parsed and validated the XML (mode, days/years
// bounds, Enabled status) and hands us its BucketLockConfig JSON to store
// verbatim. Versioning must be Enabled — enabling lock on an existing
// versioned bucket is allowed, a suspended or unversioned one is a 409
// (docs/s3-object-lock.md §5).
func (b *Backend) PutObjectLockConfiguration(ctx context.Context, bucket string, config []byte) error {
	st, err := b.reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return err
	}
	if st.Versioning != registry.VersioningEnabled {
		return s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotAllowed)
	}
	if err := b.reg.SetObjectLockConfig(ctx, bucket, config); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return fmt.Errorf("s3frontend: put object lock configuration: %w", err)
	}
	return nil
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

// GetBucketVersioning reports the bucket's versioning configuration. A bucket
// that has never been configured returns an empty <VersioningConfiguration/>
// (nil Status); once configured, the status is Enabled or Suspended. Also
// called from auth.CheckObjectAccess (object_lock.go:220, 257).
func (b *Backend) GetBucketVersioning(ctx context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	st, err := b.reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.GetBucketVersioningOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.GetBucketVersioningOutput{}, err
	}
	out := s3response.GetBucketVersioningOutput{}
	switch st.Versioning {
	case registry.VersioningEnabled:
		status := types.BucketVersioningStatusEnabled
		out.Status = &status
	case registry.VersioningSuspended:
		status := types.BucketVersioningStatusSuspended
		out.Status = &status
	}
	return out, nil
}

// PutBucketVersioning sets the bucket's versioning state. The controller
// rejects anything but Enabled/Suspended before we're called; there is no way
// back to unversioned (matching S3). Suspending a bucket that carries an
// object-lock configuration is refused — the guard that makes "a lock bucket
// is always versioning-Enabled" an invariant (docs/s3-object-lock.md §5).
func (b *Backend) PutBucketVersioning(ctx context.Context, bucket string, status types.BucketVersioningStatus) error {
	st, err := b.reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return err
	}
	var v registry.VersioningState
	switch status {
	case types.BucketVersioningStatusEnabled:
		v = registry.VersioningEnabled
	case types.BucketVersioningStatusSuspended:
		if st.ObjectLockConfig != nil {
			return s3err.GetAPIError(s3err.ErrSuspendedVersioningNotAllowed)
		}
		v = registry.VersioningSuspended
	default:
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	if err := b.reg.SetVersioning(ctx, bucket, v); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return fmt.Errorf("s3frontend: put bucket versioning: %w", err)
	}
	return nil
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
	// x-amz-bucket-object-lock-enabled: the bucket is born versioned and
	// locked in one Create, so there is no window in which it exists
	// unlocked (docs/s3-object-lock.md §5). The stored document mirrors
	// posix: Enabled with the creation time, no default retention.
	var init registry.CreateState
	if input.ObjectLockEnabledForBucket != nil && *input.ObjectLockEnabledForBucket {
		now := time.Now()
		cfg, err := json.Marshal(auth.BucketLockConfig{Enabled: true, CreatedAt: &now})
		if err != nil {
			return fmt.Errorf("s3frontend: create bucket: marshal lock config: %w", err)
		}
		init = registry.CreateState{Versioning: registry.VersioningEnabled, ObjectLockConfig: cfg}
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
	if err := b.reg.Create(ctx, name, id, init); err != nil {
		if errors.Is(err, registry.ErrExists) {
			return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
		}
		return err
	}
	return nil
}

// SegmentDigestLister is the slice of the catalog log DeleteBucket needs to
// release shipped-segment blob registrations: quiesce the bucket's log, then
// enumerate every blob its shipped segments registered. It is matched by a
// runtime type assertion (b.log is a blockstore.Log), which fails SILENTLY on
// a signature drift — the compile-time assertion in the root package pins
// *logstore.Manager to this shape so a drift is a build error instead.
type SegmentDigestLister interface {
	QuiesceBucketLog(ctx context.Context, bucket string) error
	ShippedSegmentDigests(ctx context.Context, bucket string) ([]multihash.Multihash, error)
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

		// S3 forbids deleting non-empty buckets. Walk the MST until we see any
		// leaf, then bail. Any leaf counts — a versioned bucket holding only
		// delete markers is still non-empty, and reports the versioned error
		// ("you must delete all versions").
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
				if st.Versioning.Configured() {
					return s3err.GetAPIError(s3err.ErrVersionedBucketNotEmpty)
				}
				return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
			}
		}

		// In-flight multipart uploads do not block deletion (the upstream
		// conformance contract's teardown deletes buckets without aborting
		// them): abort any open sessions, releasing their parked part blobs
		// from the space, before asking hilt to delete the space — which
		// refuses while the space still holds blob registrations.
		sessions, err := b.multipart.ListSessions(ctx, name)
		if err != nil {
			return fmt.Errorf("s3frontend: delete bucket: list mp sessions: %w", err)
		}
		aborted := 0
		for _, s := range sessions {
			if s.State == registry.SessionOpen {
				b.abortOpenSession(ctx, st.Space, s)
				aborted++
			}
		}
		if aborted > 0 {
			b.logger.Info("delete bucket: aborted in-flight multipart sessions",
				zap.String("bucket", name), zap.Int("aborted", aborted), zap.Int("total", len(sessions)))
		}

		// Shipped catalog segments registered blobs in the bucket's space
		// (each sealed CAR plus its sharded-dag-index blob), and hilt
		// refuses to delete a space that still holds registrations — so
		// release them first. Quiescing the bucket's log comes before the
		// enumeration: it joins any in-flight ship, so a segment can't
		// register its blobs after the release pass has already read the
		// rows (the delete would race the flush goroutine and be refused).
		// The release is idempotent (removing an unregistered blob is a
		// no-op), so a retried DeleteBucket is safe.
		if log, ok := b.log.(SegmentDigestLister); ok {
			if err := log.QuiesceBucketLog(ctx, name); err != nil {
				return fmt.Errorf("s3frontend: delete bucket: %w", err)
			}
			digests, err := log.ShippedSegmentDigests(ctx, name)
			if err != nil {
				return fmt.Errorf("s3frontend: delete bucket: %w", err)
			}
			for _, d := range digests {
				if err := b.remover.RemoveBlob(ctx, st.Space, d); err != nil {
					return fmt.Errorf("s3frontend: delete bucket: release shipped segment: %w", err)
				}
			}
		}

		// Deleted objects' blobs may still be registered in the space: their
		// releases sit in the deferred queue behind the reader grace, and
		// hilt refuses to delete a space that still holds registrations.
		// The bucket is provably empty here and its deletion explicit, so
		// no grace is owed — execute the space's pending releases now.
		if err := b.drainSpaceReleases(ctx, st.Space); err != nil {
			return fmt.Errorf("s3frontend: delete bucket: %w", err)
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
