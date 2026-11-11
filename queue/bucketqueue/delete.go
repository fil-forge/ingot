package bucketqueue

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// deleteKey is the context key Delete marks its request's context with: the
// name of the bucket it deletes.
type deleteKey struct{}

// refuseNonEmpty refuses st's bucket when it holds any object. S3 forbids
// deleting non-empty buckets. It walks the MST until it sees any leaf, then
// bails. Any leaf counts — a versioned bucket holding only delete markers is
// still non-empty, and reports the versioned error ("you must delete all
// versions").
func (q *Queue) refuseNonEmpty(ctx context.Context, st *registry.State) error {
	if !st.Root.Defined() {
		return nil
	}
	t := mst.LoadMST(q.read, st.Space, st.Root)
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
		return ErrVersionedBucketNotEmpty
	}
	return ErrBucketNotEmpty
}

// runDeleteBucket is queue-bucket.delete, the last step of DeleteBucket,
// after queue-bucket.close and the aborts of the bucket's multipart uploads.
// It fails on a bucket that holds objects or an upload, which the close
// makes impossible. Hilt authorizes the delete with the
// signed S3 request itself, which reaches the task only through its caller's
// context, marked by Delete for this bucket (deleteKey): a
// delete run without that mark, resumed by a worker or by another request's
// task, fails without acting, and the client, whose request failed with it,
// retries with a freshly signed one. Every step before the Hilt call is safe
// to repeat.
func (q *Queue) runDeleteBucket(ctx context.Context, _ *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args BucketTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode delete: %w", err))
	}
	name := args.Name
	if _, ok := reqscope.Request(ctx); !ok || ctx.Value(deleteKey{}) != name {
		return nil, taskrunner.Permanent(errors.New("delete bucket: its request has gone"))
	}
	return nil, func() error {
		st, err := q.bucketOn(ctx, name, t.Subject)
		if err != nil {
			return err
		}

		if err := q.refuseNonEmpty(ctx, st); err != nil {
			return err
		}
		sessions, err := q.sessions.ListSessions(ctx, name, st.Space)
		if err != nil {
			return fmt.Errorf("delete bucket: list mp sessions: %w", err)
		}
		if len(sessions) > 0 {
			return fmt.Errorf("delete bucket: %d multipart uploads remain after the close: %w", len(sessions), ErrBucketClosing)
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
		if err := q.reg.EmptyRoot(ctx, name, st.Root); err != nil {
			return fmt.Errorf("delete bucket: empty the root: %w", err)
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
		if log, ok := q.log.(SegmentDigestLister); ok {
			if err := log.QuiesceBucketLog(ctx, name); err != nil {
				return fmt.Errorf("delete bucket: %w", err)
			}
			if err := log.ReleaseRetiredSegments(ctx, name, func(d multihash.Multihash) error {
				return q.remover.RemoveBlob(ctx, st.Space, d)
			}); err != nil {
				return fmt.Errorf("delete bucket: %w", err)
			}
			digests, err := log.ShippedSegmentDigests(ctx, name)
			if err != nil {
				return fmt.Errorf("delete bucket: %w", err)
			}
			for _, d := range digests {
				if err := q.remover.RemoveBlob(ctx, st.Space, d); err != nil {
					return fmt.Errorf("delete bucket: release shipped segment: %w", err)
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
		defer q.proofs.Lend(ctx, st.Space)()
		if err := q.blobs.DrainSpace(ctx, st.Space); err != nil {
			return fmt.Errorf("delete bucket: %w", err)
		}

		// The object-count changes the emptying owed are drained for the same
		// reason and at the same point: the bucket is provably empty, so the
		// queue holds its deletes' retractions, and the cascade below would
		// throw them away. Nothing else would retract those roots — hilt's
		// delete checks the space's blobs, not its content entries — so every
		// object of a bucket emptied and deleted in one go would stay counted.
		if err := q.counts.Drain(ctx, st.Space); err != nil {
			return fmt.Errorf("delete bucket: %w", err)
		}

		req, ok := reqscope.Request(ctx)
		if !ok {
			return errors.New("delete bucket: no request in context")
		}
		// Only an answer that the bucket stands reopens it. Not found means
		// an earlier delete removed the space, so this one finishes the job
		// here. A call that failed on its way may have deleted the space all
		// the same, so the bucket stays closing until a retry settles it.
		if err := q.buckets.DeleteBucket(ctx, req); err != nil && !errors.Is(err, bucketauthority.ErrNotFound) {
			if errors.Is(err, bucketauthority.ErrNotEmpty) || errors.Is(err, bucketauthority.ErrRefused) {
				if cerr := q.reg.SetClosing(ctx, name, false); cerr != nil {
					q.logger.Warn("delete bucket: clear the closing mark after the refusal", zap.String("bucket", name), zap.Error(cerr))
				}
			}
			if errors.Is(err, bucketauthority.ErrNotEmpty) {
				return ErrBucketNotEmpty
			}
			return err
		}

		// The bucket's segment history dies with it, while the row still
		// holds the name: the log is the name's, and a bucket created under
		// the name once the row goes runs under its own space, apart from
		// this task, and writes a log this must not remove. Best-effort: Hilt
		// has deleted the space, so a cleanup failure must not fail the API
		// call, and a create under the name removes a log left behind.
		if remover, ok := q.log.(bucketLogRemover); ok {
			if rerr := remover.RemoveBucketLog(ctx, name); rerr != nil {
				q.logger.Warn("bucket log cleanup failed",
					zap.String("bucket", name), zap.Error(rerr))
			}
		}

		// The space's object-count changes went out in the drain above; one
		// that could not be sent stays in the queue as a failed task, which
		// outlives the bucket as the record of the count it leaves wrong.
		if err := q.reg.Delete(ctx, name, st.Space); err != nil {
			if errors.Is(err, registry.ErrNotFound) {
				return bucketop.ErrBucketNotFound
			} else if errors.Is(err, registry.ErrNotEmpty) {
				return ErrBucketNotEmpty
			}
			return err
		}
		return nil
	}()
}

// SegmentDigestLister is the slice of the catalog log DeleteBucket needs to
// release shipped-segment blob registrations: quiesce the bucket's log, then
// release every blob its retired segments registered and enumerate the ones
// its other segments did. It is matched by a runtime type assertion (the log is
// a blockstore.Log), which fails SILENTLY on a signature drift — the
// compile-time assertion in the root package pins *logstore.Manager to this
// shape so a drift is a build error instead.
type SegmentDigestLister interface {
	QuiesceBucketLog(ctx context.Context, bucket string) error
	ReleaseRetiredSegments(ctx context.Context, bucket string, release func(multihash.Multihash) error) error
	ShippedSegmentDigests(ctx context.Context, bucket string) ([]multihash.Multihash, error)
}
