package s3frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	"github.com/fil-forge/ingot/taskrunner"
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
// bucket lets ParseACL produce ACL{} with an empty owner (no root
// account is configured to substitute), so the ACL layer has nothing to
// deny on and hilt's per-request authorization stands alone.
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
	return b.runBucket(ctx, st.Space, registry.BucketSetLockConfigAction, &BucketSetLockConfigTask{Name: bucket, Config: config})
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
		v = registry.VersioningSuspended
	default:
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	return b.runBucket(ctx, st.Space, registry.BucketSetVersioningAction, &BucketSetVersioningTask{Name: bucket, State: string(v)})
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

// unsupportedBucketACL reports whether the request asks for a bucket ACL ingot
// cannot honor: any canned ACL other than the default "private", or any grant
// header. An absent canned ACL (the default) and an explicit "private" pass.
// The controller forwards these from the x-amz-acl / x-amz-grant-* headers.
func unsupportedBucketACL(input *s3.CreateBucketInput) bool {
	if input.ACL != "" && input.ACL != types.BucketCannedACLPrivate {
		return true
	}
	return grantSet(input.GrantFullControl) ||
		grantSet(input.GrantRead) ||
		grantSet(input.GrantReadACP) ||
		grantSet(input.GrantWrite) ||
		grantSet(input.GrantWriteACP)
}

func grantSet(g *string) bool { return g != nil && *g != "" }

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
	// ingot does not model ACLs, so the only bucket ACL it accepts is the
	// default: no ACL header, or a "private" canned ACL. Any other canned ACL,
	// or an explicit grant header, asks for functionality we do not implement.
	if unsupportedBucketACL(input) {
		return s3err.GetAPIError(s3err.ErrNotImplemented)
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
	// The owning tenant is the caller's: iam stashes it from hilt's authorize
	// response on every request, and the same access key is about to create
	// the bucket through hilt. (hilt's create reply does not carry the
	// tenant.)
	tenant, ok := reqscope.Tenant(ctx)
	if !ok {
		return errors.New("s3frontend: create bucket: no tenant in context")
	}
	init.Tenant = tenant
	id, err := b.authority.CreateBucket(ctx, req)
	if err != nil {
		if errors.Is(err, bucketauthority.ErrAlreadyOwned) {
			return s3err.GetAPIError(s3err.ErrBucketAlreadyOwnedByYou)
		}
		if errors.Is(err, bucketauthority.ErrExists) {
			return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
		}
		return err
	}
	return b.runBucket(ctx, id, registry.BucketCreateAction, &BucketCreateTask{
		Name:       name,
		Tenant:     init.Tenant.String(),
		Versioning: string(init.Versioning),
		LockConfig: init.ObjectLockConfig,
	})
}

// SegmentDigestLister is the slice of the catalog log DeleteBucket needs to
// release shipped-segment blob registrations: quiesce the bucket's log, then
// release every blob its retired segments registered and enumerate the ones
// its other segments did. It is matched by a runtime type assertion (b.log is
// a blockstore.Log), which fails SILENTLY on a signature drift — the
// compile-time assertion in the root package pins *logstore.Manager to this
// shape so a drift is a build error instead.
type SegmentDigestLister interface {
	QuiesceBucketLog(ctx context.Context, bucket string) error
	ReleaseRetiredSegments(ctx context.Context, bucket string, release func(multihash.Multihash) error) error
	ShippedSegmentDigests(ctx context.Context, bucket string) ([]multihash.Multihash, error)
}

func (b *Backend) DeleteBucket(ctx context.Context, name string) error {
	st, err := b.reg.Get(ctx, name)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return err
	}
	// Closed first, so the bucket stays empty and gains no multipart upload
	// while the ones in flight are aborted. In-flight uploads do not block
	// deletion, as on AWS (and versitygw's conformance teardown deletes
	// buckets without aborting them). The aborts run from here: the
	// multipart queue is above the bucket queue, so no bucket task may run
	// them. Each runs on the request's own proofs: hilt delegates blob.Abort
	// and blob.Remove with s3:DeleteBucket, so a bucket that never saw a
	// write has the authority to release what it holds.
	if err := b.runBucket(ctx, st.Space, registry.BucketCloseAction, &BucketTask{Name: name}); err != nil {
		return err
	}
	sessions, err := b.multipart.ListSessions(ctx, name, st.Space)
	if err != nil {
		return fmt.Errorf("s3frontend: delete bucket: list mp sessions: %w", err)
	}
	for _, s := range sessions {
		switch s.State {
		case registry.SessionOpen:
			if err := b.abortUpload(ctx, s); err != nil && !errors.Is(err, s3err.GetAPIError(s3err.ErrNoSuchUpload)) {
				return fmt.Errorf("s3frontend: delete bucket: abort multipart upload %s: %w", s.UploadID, err)
			}
		case registry.SessionCompleted:
			if err := b.reapNow(ctx, s.UploadID); err != nil {
				return fmt.Errorf("s3frontend: delete bucket: reap multipart upload %s: %w", s.UploadID, err)
			}
		}
	}
	if len(sessions) > 0 {
		b.logger.Info("delete bucket: tore down multipart sessions",
			zap.String("bucket", name), zap.Int("sessions", len(sessions)))
	}
	ctx = context.WithValue(ctx, deleteKey{}, name)
	return b.runBucket(ctx, st.Space, registry.BucketDeleteAction, &BucketTask{Name: name})
}

// deleteKey is the context key a DeleteBucket request marks its context
// with: the name of the bucket it deletes.
type deleteKey struct{}

// refuseNonEmpty refuses st's bucket when it holds any object. S3 forbids
// deleting non-empty buckets. It walks the MST until it sees any leaf, then
// bails. Any leaf counts — a versioned bucket holding only delete markers is
// still non-empty, and reports the versioned error ("you must delete all
// versions").
func (b *Backend) refuseNonEmpty(ctx context.Context, st *registry.State) error {
	if !st.Root.Defined() {
		return nil
	}
	t := mst.LoadMST(b.read, st.Space, st.Root)
	var seen bool
	if err := t.WalkLeavesFromNocache(ctx, "", func(string, cid.Cid) error {
		seen = true
		return mst.ErrStopWalk
	}); err != nil {
		return err
	}
	if !seen {
		return nil
	}
	if st.Versioning.Configured() {
		return s3err.GetAPIError(s3err.ErrVersionedBucketNotEmpty)
	}
	return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
}

// runDeleteBucket is queue-bucket.delete, the last step of DeleteBucket,
// after queue-bucket.close and the aborts of the bucket's multipart uploads.
// It fails on a bucket that holds objects or an upload, which the close
// makes impossible. Hilt authorizes the delete with the
// signed S3 request itself, which reaches the task only through its caller's
// context, marked by the DeleteBucket request for this bucket (deleteKey): a
// delete run without that mark, resumed by a worker or by another request's
// task, fails without acting, and the client, whose request failed with it,
// retries with a freshly signed one. Every step before the Hilt call is safe
// to repeat.
func (b *Backend) runDeleteBucket(ctx context.Context, _ *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args BucketTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode delete: %w", err))
	}
	name := args.Name
	if _, ok := reqscope.Request(ctx); !ok || ctx.Value(deleteKey{}) != name {
		return nil, taskrunner.Permanent(errors.New("s3frontend: delete bucket: its request has gone"))
	}
	return nil, func() error {
		st, err := b.bucketOn(ctx, name, t.Subject)
		if err != nil {
			return err
		}

		if err := b.refuseNonEmpty(ctx, st); err != nil {
			return err
		}
		sessions, err := b.multipart.ListSessions(ctx, name, st.Space)
		if err != nil {
			return fmt.Errorf("s3frontend: delete bucket: list mp sessions: %w", err)
		}
		if len(sessions) > 0 {
			return fmt.Errorf("s3frontend: delete bucket: %d multipart uploads remain after the close: %w", len(sessions), errBucketClosing)
		}

		// Everything below releases blobs, and the catalog's own segments
		// among them. An empty bucket's catalog describes nothing it still
		// needs, so commit the empty tree first, ahead of this task's own
		// commit: the bucket then reads no catalog block, and a delete that
		// fails past this point (hilt refusing, a release erroring) leaves a
		// bucket that lists empty and deletes on retry, not one whose root
		// names released blocks. The same write clears forge_root_cid and
		// drops the bucket's journaled blocks: the quiesce below leaves the
		// last segment unshipped until the bucket's next write, and its rows
		// would otherwise wait with it.
		if err := b.reg.EmptyRoot(ctx, name, st.Root); err != nil {
			return fmt.Errorf("s3frontend: delete bucket: empty the root: %w", err)
		}

		// Shipped catalog segments registered blobs in the bucket's space
		// (each sealed CAR plus its sharded-dag-index blob, retired segments
		// included), and hilt refuses to delete a space that still holds
		// registrations — so release them first. The root is already empty,
		// so nothing the bucket reads goes with them. Quiescing the bucket's
		// log comes before the enumeration, so no segment seals after the
		// release pass has read the rows. A ship runs on this subject, so
		// none is in flight to register its blobs behind the pass. The
		// release is idempotent (removing an unregistered blob is a no-op),
		// so a retried DeleteBucket is safe; retired segments, which
		// accumulate for the bucket's life, drop their rows as they are
		// released, so a retry doesn't release them again.
		if log, ok := b.log.(SegmentDigestLister); ok {
			if err := log.QuiesceBucketLog(ctx, name); err != nil {
				return fmt.Errorf("s3frontend: delete bucket: %w", err)
			}
			if err := log.ReleaseRetiredSegments(ctx, name, func(d multihash.Multihash) error {
				return b.remover.RemoveBlob(ctx, st.Space, d)
			}); err != nil {
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
		// no grace is owed — execute the space's pending releases now. The
		// workers that run them, and the object-count changes below, read
		// this request's authority, lent for the space while it waits: what
		// the tasks captured may have lapsed.
		defer b.proofs.Lend(ctx, st.Space)()
		if err := b.drainSpaceReleases(ctx, st.Space); err != nil {
			return fmt.Errorf("s3frontend: delete bucket: %w", err)
		}

		// The object-count changes the emptying owed are drained for the same
		// reason and at the same point: the bucket is provably empty, so the
		// queue holds its deletes' retractions, and the cascade below would
		// throw them away. Nothing else would retract those roots — hilt's
		// delete checks the space's blobs, not its content entries — so every
		// object of a bucket emptied and deleted in one go would stay counted.
		if err := b.DrainSpaceRegistrations(ctx, st.Space); err != nil {
			return fmt.Errorf("s3frontend: delete bucket: %w", err)
		}

		req, ok := reqscope.Request(ctx)
		if !ok {
			return errors.New("s3frontend: delete bucket: no request in context")
		}
		// Only an answer that the bucket stands reopens it. Not found means
		// an earlier delete removed the space, so this one finishes the job
		// here. A call that failed on its way may have deleted the space all
		// the same, so the bucket stays closing until a retry settles it.
		if err := b.authority.DeleteBucket(ctx, req); err != nil && !errors.Is(err, bucketauthority.ErrNotFound) {
			if errors.Is(err, bucketauthority.ErrNotEmpty) || errors.Is(err, bucketauthority.ErrRefused) {
				if cerr := b.reg.SetClosing(ctx, name, false); cerr != nil {
					b.logger.Warn("delete bucket: clear the closing mark after the refusal", zap.String("bucket", name), zap.Error(cerr))
				}
			}
			if errors.Is(err, bucketauthority.ErrNotEmpty) {
				return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
			}
			return err
		}

		// The bucket's segment history dies with it, while the row still
		// holds the name: the log is the name's, and a bucket created under
		// the name once the row goes runs under its own space, apart from
		// this task, and writes a log this must not remove. Best-effort: Hilt
		// has deleted the space, so a cleanup failure must not fail the API
		// call, and a create under the name removes a log left behind.
		if remover, ok := b.log.(bucketLogRemover); ok {
			if rerr := remover.RemoveBucketLog(ctx, name); rerr != nil {
				b.logger.Warn("bucket log cleanup failed",
					zap.String("bucket", name), zap.Error(rerr))
			}
		}

		// The space's object-count changes went out in the drain above; one
		// that could not be sent stays in the queue as a failed task, which
		// outlives the bucket as the record of the count it leaves wrong.
		if err := b.reg.Delete(ctx, name, st.Space); err != nil {
			if errors.Is(err, registry.ErrNotFound) {
				return s3err.GetAPIError(s3err.ErrNoSuchBucket)
			} else if errors.Is(err, registry.ErrNotEmpty) {
				return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
			}
			return err
		}
		return nil
	}()
}

// validBucketName enforces AWS's general-purpose bucket naming rules
// (https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html):
// 3-63 characters; only lowercase letters, digits, dots and hyphens; must
// begin and end with a letter or digit; no consecutive dots and no dot
// adjacent to a hyphen (every dot-separated label begins and ends
// alphanumeric); not formatted as an IPv4 address; and none of the reserved
// prefixes/suffixes. A false result maps to S3 InvalidBucketName.
func validBucketName(s string) bool {
	if len(s) < 3 || len(s) > 63 {
		return false
	}
	// Reserved prefixes and suffixes (AWS).
	if strings.HasPrefix(s, "xn--") || strings.HasPrefix(s, "sthree-") {
		return false
	}
	if strings.HasSuffix(s, "-s3alias") || strings.HasSuffix(s, "--ol-s3") {
		return false
	}
	last := len(s) - 1
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			// alphanumeric always allowed
		case r == '-':
			// hyphen may not be first or last, nor adjacent to a dot
			if i == 0 || i == last || s[i-1] == '.' || s[i+1] == '.' {
				return false
			}
		case r == '.':
			// dot may not be first or last, nor adjacent to a dot or hyphen
			if i == 0 || i == last {
				return false
			}
			if next := s[i+1]; next == '.' || next == '-' {
				return false
			}
			if prev := s[i-1]; prev == '-' {
				return false
			}
		default:
			return false
		}
	}
	// Must not be formatted as an IPv4 address (e.g. 192.168.1.1).
	if isIPv4(s) {
		return false
	}
	return true
}

// isIPv4 reports whether s is four dot-separated groups of 1-3 digits — the
// IP-address form S3 forbids as a bucket name.
func isIPv4(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return false
	}
	for _, p := range parts {
		if len(p) < 1 || len(p) > 3 {
			return false
		}
		for _, r := range p {
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}

// MalformedPolicy is the S3 error for a PutBucketPolicy body the storage
// system refuses (fil-one/RFC#30, "Failures").
func MalformedPolicy(description string) s3err.APIError {
	return s3err.APIError{Code: "MalformedPolicy", Description: description, HTTPStatusCode: http.StatusBadRequest}
}
