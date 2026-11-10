package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/google/uuid"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/uploader"
)

// A write's use of a blob is a pending reference, taken by a sync
// queue-space-shard.add on the blob's release subject (registry.ReleaseSubject)
// and held until the write's commit claims it or its part row takes it over.
// The add runs under the subject's lease, so it never overlaps a release of
// the same blob: it waits for one running, and for its retries, and then
// finds the blob unlocated and uses its own stream's allocation, which the
// space can take once the release's removal has its receipt. A release that
// runs after the add counts the reference and does nothing. A reference
// nothing takes over is dropped at its deadline by a queue-space-shard.expire,
// which releases the blob at once if nothing else in the space references it:
// a blob nothing claimed was never readable, so no reader grace is owed.

// The ways a write uses a blob (ShardAddTask.Mode).
const (
	// addStreamed is an object blob the write streamed: concluded, unless
	// the space holds the blob already.
	addStreamed = "streamed"
	// addPart is a part blob the write streamed: its park is recorded, for
	// the upload's accept to conclude, unless the space has the blob parked
	// or accepted already.
	addPart = "part"
	// addCopy is a same-space copy of a blob the space holds: there is
	// nothing to send, and a blob the space no longer holds fails the copy.
	addCopy = "copy"
)

// pendingRefTTL is how long a pending reference lasts unless the write takes
// it over: the provider expires an allocation that is never concluded within
// about the same time.
const pendingRefTTL = 24 * time.Hour

// addWaitLimit bounds how long an add waits for a release of the same blob,
// running or waiting to retry; past it the request is refused with S3's
// SlowDown, which the client retries. Package-level so tests can shrink it.
var addWaitLimit = 30 * time.Second

// expireRetry retries an expiry that could not finish, with no attempt
// limit: dropping it would leave the blob referenced for good.
var expireRetry = taskrunner.RetryPolicy{First: time.Second, Max: 5 * time.Minute, Multiplier: 2}

// errBlobGone fails a same-space copy whose source blob the space no longer
// holds: its release has run.
var errBlobGone = errors.New("s3frontend: the space no longer holds the blob")

// ShardAddTask is a queue-space-shard.add task's arguments.
type ShardAddTask struct {
	// Mode is how the write uses the blob: addStreamed, addPart or addCopy.
	Mode string
	// Bucket is the bucket the write is to, recorded with the blob's intent.
	Bucket string
	// Size is the blob's stored byte count.
	Size int64
	// Release is the authority the release an expiry queues removes the blob
	// with.
	Release registry.ReleaseTask
}

// addStreamKey carries a streamed blob's allocation to its add, which runs in
// the caller's goroutine. The allocation's put invocation embeds derived
// signer keys, so it is never stored in the task's arguments.
type addStreamKey struct{}

// addResult is an add's result: the reference it took, which is the add's
// task ID, and whether the write reused a blob the space already held, so
// its stream's allocation is surplus.
type addResult struct {
	ref    taskrunner.TaskID
	reused bool
}

func (r addResult) encode() []byte {
	out := append([]byte(nil), r.ref[:]...)
	if r.reused {
		return append(out, 1)
	}
	return append(out, 0)
}

func decodeAddResult(b []byte) (addResult, error) {
	if len(b) != 17 {
		return addResult{}, fmt.Errorf("add result %x", b)
	}
	var r addResult
	copy(r.ref[:], b[:16])
	r.reused = b[16] == 1
	return r, nil
}

// registerShardRefTasks registers the add and expire handlers on the release
// queue (see registerReleaseTasks).
func (b *Backend) registerShardRefTasks(r *taskrunner.Runner) error {
	if err := r.Handle(registry.ReleaseQueue, registry.AddAction, b.runAdd); err != nil {
		return err
	}
	return r.Handle(registry.ReleaseQueue, registry.ExpireAction, b.runExpire, taskrunner.WithRetry(expireRetry))
}

// addBlob runs a queue-space-shard.add for a blob of a write to bucket in
// space, and returns the reference it took. sb is the blob's stream, for the
// streamed and part modes; when the space already held the blob, the stream's
// allocation is given up. release is the authority an expiry of the
// reference releases the blob with.
func (b *Backend) addBlob(ctx context.Context, space did.DID, bucket string, digest multihash.Multihash, size int64, mode string, sb *uploader.StreamedBlob, release registry.ReleaseTask) (taskrunner.TaskID, error) {
	var buf bytes.Buffer
	if err := (&ShardAddTask{Mode: mode, Bucket: bucket, Size: size, Release: release}).MarshalCBOR(&buf); err != nil {
		return uuid.Nil, fmt.Errorf("encode add: %w", err)
	}
	if sb != nil {
		ctx = context.WithValue(ctx, addStreamKey{}, sb)
	}
	expires := time.Now().Add(addWaitLimit)
	out, err := b.releases.Run(ctx, taskrunner.Spec{
		Queue:   registry.ReleaseQueue,
		Subject: registry.ReleaseSubject(space, digest),
		Action:  registry.AddAction,
		Args:    buf.Bytes(),
		Expires: &expires,
	})
	switch {
	case errors.Is(err, taskrunner.ErrExpired):
		// A release of the blob ran, or waited to retry, past the limit.
		return uuid.Nil, s3err.GetAPIError(s3err.ErrSlowDown)
	case err != nil:
		return uuid.Nil, err
	}
	res, err := decodeAddResult(out)
	if err != nil {
		return uuid.Nil, err
	}
	if res.reused && sb != nil {
		b.abandonStream(ctx, space, *sb)
	}
	return res.ref, nil
}

// runAdd is queue-space-shard.add: it makes the write's blob durable for the
// space and takes the write's pending reference to it, due to expire at
// pendingRefTTL. The reference and its expiry commit with the task; the
// blob's rows are recorded as the steps that make them true finish.
func (b *Backend) runAdd(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	space, digest, err := registry.ParseReleaseSubject(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	var args ShardAddTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode add: %w", err))
	}
	sb, _ := ctx.Value(addStreamKey{}).(*uploader.StreamedBlob)
	if sb == nil && args.Mode != addCopy {
		// The caller that held the stream has gone.
		return nil, taskrunner.Permanent(fmt.Errorf("add of %x: no stream", digest))
	}
	reused, err := b.addUse(ctx, space, digest, args, sb)
	if err != nil {
		return nil, err
	}
	var release bytes.Buffer
	if err := args.Release.MarshalCBOR(&release); err != nil {
		return nil, fmt.Errorf("encode expiry: %w", err)
	}
	deadline := time.Now().Add(pendingRefTTL)
	expire, err := ex.Submit(ctx, taskrunner.Spec{
		Queue:     registry.ReleaseQueue,
		Subject:   t.Subject,
		Action:    registry.ExpireAction,
		Args:      release.Bytes(),
		NotBefore: &deadline,
	})
	if err != nil {
		return nil, err
	}
	st := registry.PendingRef(t.ID, expire, space, digest, deadline)
	if err := ex.Write(ctx, st.SQL, st.Args...); err != nil {
		return nil, err
	}
	return addResult{ref: t.ID, reused: reused}.encode(), nil
}

// addUse makes the blob durable for the space as the mode says, recording its
// rows, and reports whether the space held it already.
func (b *Backend) addUse(ctx context.Context, space did.DID, digest multihash.Multihash, args ShardAddTask, sb *uploader.StreamedBlob) (bool, error) {
	if loc, err := b.locations.GetLocation(ctx, space, digest); err == nil && loc != nil {
		// Accepted for the space: the blob is reused. A copy's source
		// already has its intent.
		if args.Mode == addCopy {
			return true, nil
		}
		if err := b.putIntent(ctx, space, digest, args, registry.IntentAccepted); err != nil {
			return false, err
		}
		if args.Mode == addPart {
			// A located blob has no use for a park. One is still here when
			// another write accepted the same bytes while a part's park
			// stood; the upload's accept drops it otherwise, so a failure
			// here is logged.
			if err := b.parks.DeletePark(ctx, space, digest); err != nil {
				b.logger.Warn("drop stale park row failed; the upload's accept will retry",
					zap.Stringer("space", space), zap.Error(err))
			}
		}
		return true, nil
	} else if err != nil && !errors.Is(err, registry.ErrNotFound) {
		return false, fmt.Errorf("lookup location: %w", err)
	}
	switch args.Mode {
	case addCopy:
		return false, errBlobGone
	case addPart:
		return b.addPark(ctx, space, digest, args, *sb)
	case addStreamed:
		return false, b.addConclude(ctx, space, digest, args, *sb)
	}
	return false, taskrunner.Permanent(fmt.Errorf("add mode %q", args.Mode))
}

// addPark records the park a part blob's stream left on its provider, traced
// as a blob.park span, unless the space has the blob parked already: that
// park stands, and the stream is surplus.
func (b *Backend) addPark(ctx context.Context, space did.DID, digest multihash.Multihash, args ShardAddTask, sb uploader.StreamedBlob) (_ bool, err error) {
	if _, err := b.parks.GetPark(ctx, space, digest); err == nil {
		return true, b.putIntent(ctx, space, digest, args, registry.IntentParked)
	} else if !errors.Is(err, registry.ErrNotFound) {
		return false, fmt.Errorf("lookup park: %w", err)
	}
	ctx, span := tracing.Start(ctx, "blob.park",
		attribute.String("ingot.blob.result", "streamed"),
		attribute.Int64("ingot.blob.bytes", sb.Size),
	)
	defer func() { tracing.End(span, err) }()
	parked := sb.Parked(digest)
	recorded, err := b.parks.PutPark(ctx, registry.BlobPark{
		Space:         space,
		Digest:        digest,
		AddTask:       parked.AddTask.Bytes(),
		AcceptTask:    parked.AcceptTask.Bytes(),
		PutInvocation: parked.PutInvocation,
		Size:          parked.Size,
	})
	if err != nil {
		return false, fmt.Errorf("record park: %w", err)
	}
	// Not recorded: a write in another instance parked the same bytes for
	// the space first; its park stands.
	return !recorded, b.putIntent(ctx, space, digest, args, registry.IntentParked)
}

// addConclude accepts an object blob that went to its provider as the body
// arrived, traced as a blob.upload span, and records its location.
func (b *Backend) addConclude(ctx context.Context, space did.DID, digest multihash.Multihash, args ShardAddTask, sb uploader.StreamedBlob) (err error) {
	ctx, span := tracing.Start(ctx, "blob.upload",
		attribute.String("ingot.blob.result", "streamed"),
		attribute.Int64("ingot.blob.bytes", sb.Size),
	)
	defer func() { tracing.End(span, err) }()
	// The blob may be accepted from here on.
	if err := b.putIntent(ctx, space, digest, args, registry.IntentUploading); err != nil {
		return err
	}
	locations, err := b.streaming.ConcludeBlobs(ctx, space, []uploader.UploadedBlob{sb.Parked(digest)})
	// An acceptance that came back is recorded before any error is acted on,
	// as concludeBlobs does: the upload service ran the accept whether or not
	// the batch then failed.
	if len(locations) == 1 && locations[0] != nil {
		if rerr := b.recordAccepted(ctx, space, digest, *locations[0]); rerr != nil {
			return errors.Join(err, rerr)
		}
	}
	if err != nil {
		return fmt.Errorf("conclude streamed blob: %w", err)
	}
	if len(locations) != 1 || locations[0] == nil {
		return fmt.Errorf("conclude streamed blob %x: no location", digest)
	}
	return nil
}

// putIntent records the space's upload intent for the blob in state.
func (b *Backend) putIntent(ctx context.Context, space did.DID, digest multihash.Multihash, args ShardAddTask, state string) error {
	if err := b.intents.PutIntent(ctx, registry.UploadIntent{
		Space:  space,
		Digest: digest,
		Size:   args.Size,
		State:  state,
		Bucket: args.Bucket,
	}); err != nil {
		return fmt.Errorf("record intent: %w", err)
	}
	return nil
}

// runExpire is queue-space-shard.expire: it drops the pending reference whose
// expiry it is, unless the write has taken it over since, and queues the
// blob's release, due at once, if nothing else in the space references it.
func (b *Backend) runExpire(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	space, digest, err := registry.ParseReleaseSubject(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	args, err := registry.ReleaseTaskFrom(t)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	add, err := b.claims.PendingRefByExpiry(ctx, t.ID)
	if errors.Is(err, registry.ErrNotFound) {
		return nil, nil // taken over
	}
	if err != nil {
		return nil, fmt.Errorf("lookup pending reference: %w", err)
	}
	st := registry.DroppedPendingRef(t.ID)
	if err := ex.Write(ctx, st.SQL, st.Args...); err != nil {
		return nil, err
	}
	// A reference taken since this count is the release's to find: it counts
	// again under the same lease.
	n, err := b.claims.CountReferences(ctx, space, digest, add)
	if err != nil {
		return nil, fmt.Errorf("count references: %w", err)
	}
	if n > 0 {
		return nil, nil
	}
	spec, err := args.Spec(space, digest, registry.DueNow())
	if err != nil {
		return nil, err
	}
	_, err = ex.Submit(ctx, spec)
	return nil, err
}

// dropRefsNow expires refs, the write's pending references to digests in
// space, at once, for a write that failed before anything took them over, and
// waits for the releases they leave due, best effort, as releaseNow does. It
// brings forward only these references' expiries: another write's references
// to the same blobs keep their deadlines.
func (b *Backend) dropRefsNow(ctx context.Context, space did.DID, refs []taskrunner.TaskID, digests []multihash.Multihash) {
	expiries, err := b.claims.ExpiriesOf(ctx, refs)
	if err == nil {
		err = b.releases.ExpediteTasks(ctx, registry.ReleaseQueue, expiries...)
	}
	if err != nil {
		b.logger.Warn("expire a failed write's references; they expire at their deadline",
			zap.Stringer("space", space), zap.Error(err))
		return
	}
	// The first pass waits for the expiries, the second for the releases
	// they queue.
	b.releaseNow(ctx, space, digests)
	b.releaseNow(ctx, space, digests)
}
