// Package spaceshardqueue is queue-space-shard: the tasks on one blob in one
// space, on the subject of the space and the digest (registry.ReleaseSubject).
//
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
package spaceshardqueue

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/internal/authority"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/queue/shardqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/uploader"
)

// abandonTimeLimit bounds giving up a surplus stream's allocation, which may
// run after the request's own context has ended. An abort cut short is left to
// the provider's expiry.
const abandonTimeLimit = 10 * time.Second

// Deps is what the queue's handlers need.
type Deps struct {
	Intents   registry.IntentStore
	Parks     registry.ParkStore
	Claims    registry.BlobRefStore
	Locations registry.LocationStore
	// EncParams holds the blobs' encryption parameters, which a release
	// deletes first (the crypto-shred).
	EncParams registry.EncryptionParamsStore
	Remover   uploader.BlobRemover
	// Deferred concludes and aborts parked blobs.
	Deferred uploader.DeferredBodyUploader
	// Streaming concludes streamed object blobs and aborts surplus streams.
	Streaming uploader.StreamingBodyUploader
	// Shards evicts a released blob's cached copy.
	Shards *shardqueue.Queue
	// Authority finds the authority a release is sent with.
	Authority *authority.Resolver
	// Concluded, when set, is told the outcome of each parked blob's
	// conclude.
	Concluded func(ctx context.Context, err error)
	// Logger is optional; defaults to zap.NewNop().
	Logger *zap.Logger

	// ReleaseRetry replaces the release retry policy (zero keeps it).
	ReleaseRetry taskrunner.RetryPolicy
	// AddWait, AttemptLimit and ConcludeTimeout replace defaultAddWait,
	// defaultAttemptLimit and defaultConcludeTimeout (zero keeps each).
	AddWait         time.Duration
	AttemptLimit    time.Duration
	ConcludeTimeout time.Duration
}

// Queue runs queue-space-shard.
type Queue struct {
	tasks           *taskrunner.Runner
	intents         registry.IntentStore
	parks           registry.ParkStore
	claims          registry.BlobRefStore
	locations       registry.LocationStore
	encParams       registry.EncryptionParamsStore
	remover         uploader.BlobRemover
	deferred        uploader.DeferredBodyUploader
	streaming       uploader.StreamingBodyUploader
	shards          *shardqueue.Queue
	proofs          *authority.Resolver
	concluded       func(ctx context.Context, err error)
	logger          *zap.Logger
	addWait         time.Duration
	attemptLimit    time.Duration
	concludeTimeout time.Duration
}

// New registers the queue-space-shard handlers with r: a write's add and its
// expiry, the release, a parked blob's accept, and the no-op flush. Call it
// before r starts.
func New(r *taskrunner.Runner, d Deps) (*Queue, error) {
	q := &Queue{
		tasks:           r,
		intents:         d.Intents,
		parks:           d.Parks,
		claims:          d.Claims,
		locations:       d.Locations,
		encParams:       d.EncParams,
		remover:         d.Remover,
		deferred:        d.Deferred,
		streaming:       d.Streaming,
		shards:          d.Shards,
		proofs:          d.Authority,
		concluded:       d.Concluded,
		logger:          cmp.Or(d.Logger, zap.NewNop()),
		addWait:         cmp.Or(d.AddWait, defaultAddWait),
		attemptLimit:    cmp.Or(d.AttemptLimit, defaultAttemptLimit),
		concludeTimeout: cmp.Or(d.ConcludeTimeout, defaultConcludeTimeout),
	}
	if q.concluded == nil {
		q.concluded = func(context.Context, error) {}
	}
	for _, h := range []struct {
		action string
		run    taskrunner.Handler
		opts   []taskrunner.HandleOption
	}{
		{registry.AddAction, q.runAdd, nil},
		{registry.ExpireAction, q.runExpire, []taskrunner.HandleOption{taskrunner.WithRetry(expireRetry)}},
		{registry.ReleaseAction, q.runRelease, []taskrunner.HandleOption{taskrunner.WithRetry(cmp.Or(d.ReleaseRetry, defaultReleaseRetry))}},
		{registry.BlobAcceptAction, q.runAccept, []taskrunner.HandleOption{taskrunner.WithRetry(acceptRetry)}},
		{releaseFlush, func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) { return nil, nil }, nil},
	} {
		if err := r.Handle(registry.ReleaseQueue, h.action, h.run, h.opts...); err != nil {
			return nil, err
		}
	}
	return q, nil
}

// ExpediteAccepts makes the space's pending blob accepts due now, and stops
// their being replaced, returning them by subject.
func (q *Queue) ExpediteAccepts(ctx context.Context, space did.DID) (map[string][]taskrunner.TaskID, error) {
	return q.tasks.Expedite(ctx, registry.ReleaseQueue, registry.SpaceReleases(space), registry.BlobAcceptAction)
}

// The ways a write uses a blob (AddTask.Mode).
const (
	// Streamed is an object blob the write streamed: concluded, unless
	// the space holds the blob already.
	Streamed = "streamed"
	// Part is a part blob the write streamed: its park is recorded, for
	// the upload's accept to conclude, unless the space has the blob parked
	// or accepted already.
	Part = "part"
	// Copy is a same-space copy of a blob the space holds: there is
	// nothing to send, and a blob the space no longer holds fails the copy.
	Copy = "copy"
)

// pendingRefTTL is how long a pending reference lasts unless the write takes
// it over: the provider expires an allocation that is never concluded within
// about the same time.
const pendingRefTTL = 24 * time.Hour

// defaultAddWait bounds how long an add waits for a release of the same
// blob, running or waiting to retry; past it the add fails with ErrBusy,
// which the client is told to retry.
const defaultAddWait = 30 * time.Second

// expireRetry retries an expiry that could not finish, with no attempt
// limit: dropping it would leave the blob referenced for good.
var expireRetry = taskrunner.RetryPolicy{First: time.Second, Max: 5 * time.Minute, Multiplier: 2}

// ErrBlobGone fails a same-space copy whose source blob the space no longer
// holds: its release has run.
var ErrBlobGone = errors.New("spaceshardqueue: the space no longer holds the blob")

// ErrBusy fails an add that waited defaultAddWait for a release of the same
// blob.
var ErrBusy = errors.New("spaceshardqueue: a release of the blob is still running")

// AddTask is a queue-space-shard.add task's arguments.
type AddTask struct {
	// Mode is how the write uses the blob: Streamed, Part or Copy.
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

// A blob nothing in its space references any more is released by a
// queue-space-shard.release task, whose subject is the space and the digest
// (registry.ReleaseSubject): the commit that drops the last claim queues it,
// due after the reader grace, in its own transaction; a session teardown, a
// superseded part or an expired pending reference queues it due at once.
// Every reference a write takes enters through a queue-space-shard.add on the
// same subject, so the subject's lease keeps a release and a
// write of the same blob apart: a release finds the reference, or the write's
// add runs after it and finds the blob gone. A parked part blob's
// queue-space-shard.accept runs under it too, so a release
// never runs while the blob is being concluded.

// defaultAttemptLimit bounds one release attempt's network calls.
const defaultAttemptLimit = 2 * time.Minute

// defaultReleaseRetry retries a release that could not finish, with no attempt
// limit: dropping it would leave a blob nothing references on the network.
// A retry holds the subject, so an add of the same blob waits behind it: a
// failed attempt may already have removed the blob from the provider, and
// the add must then find it unlocated rather than reuse it.
var defaultReleaseRetry = taskrunner.RetryPolicy{
	First:        time.Second,
	Max:          5 * time.Minute,
	Multiplier:   2,
	HoldsSubject: true,
}

// releaseFlush is the queue's sync no-op: running it on a blob's subject waits
// for a running release to finish, and leaves a pending one standing.
const releaseFlush = "flush"

// releasedResult is a release's result once it has removed the blob; a
// release that found it claimed again returns none.
var releasedResult = []byte("released")

// runRelease is queue-space-shard.release: it unwinds one blob for one space,
// whatever state the blob is in, unless something in the space still uses it.
//
// Every check and read comes before the network step, and after it nothing
// can fail but the commit, which the runner retries: the blob's rows go
// through the task's writes. The crypto-shred goes first, so a blob nothing
// references becomes unreadable at once; only a plaintext blob can be
// referenced again, and it has no keys.
//
// Any reference in the space (a claim, a write's pending reference, a part of
// an in-flight session) makes the release stale, and it does nothing. Every
// reference enters through an add on this subject, which waits behind a
// running release and behind its retries, so a release that may already have
// acted never finds one. Each way a reference goes queues a release again: a
// drop of the last claim, an expiry, a superseded part, a session's teardown.
// Two of those can queue a blob's release while an earlier one runs; the
// later finds the blob's upload intent gone, which only a release's commit
// removes, and does nothing.
// A conclude of the blob in flight here defers it, without holding the
// subject.
func (q *Queue) runRelease(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	space, digest, err := registry.ParseReleaseSubject(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	args, err := registry.ReleaseTaskFrom(t)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}

	if n, err := q.claims.CountReferences(ctx, space, digest, uuid.Nil); err != nil {
		return nil, fmt.Errorf("count references: %w", err)
	} else if n > 0 {
		return nil, nil // referenced again since it was queued: stale
	}
	if _, err := q.intents.GetIntent(ctx, space, digest); errors.Is(err, registry.ErrNotFound) {
		return nil, nil // released in full since it was queued: stale
	} else if err != nil {
		return nil, fmt.Errorf("lookup intent: %w", err)
	}

	located := false
	if loc, err := q.locations.GetLocation(ctx, space, digest); err == nil && loc != nil {
		located = true
	} else if err != nil && !errors.Is(err, registry.ErrNotFound) {
		return nil, fmt.Errorf("lookup location: %w", err)
	}
	park, err := q.parks.GetPark(ctx, space, digest)
	if err != nil && !errors.Is(err, registry.ErrNotFound) {
		return nil, fmt.Errorf("lookup park: %w", err)
	}
	others, err := q.intents.CountOtherIntents(ctx, space, digest)
	if err != nil {
		return nil, fmt.Errorf("count other intents: %w", err)
	}
	netCtx, err := q.proofs.WithProofs(ctx, space, args.RemoveProofs, args.AbortProofs)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	// A stalled provider would otherwise hold a release worker for good; a
	// call cut short fails the attempt, which retries.
	netCtx, cancel := context.WithTimeout(netCtx, q.attemptLimit)
	defer cancel()

	if err := q.encParams.DeleteEncryptionParams(ctx, space, digest); err != nil {
		return nil, fmt.Errorf("crypto-shred (delete encryption params): %w", err)
	}
	switch {
	case located:
		// Accepted: drop the space's claim on the network.
		if err := q.remover.RemoveBlob(netCtx, space, digest); err != nil {
			return nil, fmt.Errorf("network remove: %w", err)
		}
	case park != nil:
		// Parked: release the allocation on the provider. A refusal because
		// the space has accepted the blob means a conclude ran that this
		// node has not recorded (an upload's accept still running, or one
		// that failed after the upload service accepted); the blob is
		// removed as an accepted one instead.
		cause, err := cid.Cast(park.AddTask)
		if err != nil {
			return nil, taskrunner.Permanent(fmt.Errorf("decode park add task: %w", err))
		}
		aerr := q.deferred.AbortBlob(netCtx, space, cause)
		switch {
		case aerr == nil:
		case errors.Is(aerr, uploader.ErrBlobAccepted):
			if err := q.remover.RemoveBlob(netCtx, space, digest); err != nil {
				return nil, fmt.Errorf("network remove of an accepted blob: %w", err)
			}
		default:
			return nil, fmt.Errorf("abort parked blob: %w", aerr)
		}
	default:
		// No row says the blob is on the network, but it may be: a conclude
		// can be accepted and then fail to record its location, leaving
		// neither row. The upload
		// service treats a remove of a blob it never registered as success,
		// so the remove costs one round trip and closes that leak.
		if err := q.remover.RemoveBlob(netCtx, space, digest); err != nil {
			return nil, fmt.Errorf("network remove of a blob with no rows: %w", err)
		}
	}

	// The network holds nothing of the blob for this space now. Its rows go
	// with the task's commit.
	for _, st := range registry.ReleasedRows(space, digest) {
		if err := ex.Write(ctx, st.SQL, st.Args...); err != nil {
			return nil, err
		}
	}
	// The cached copy is shared with every other space whose intent names
	// the digest (the same bytes stored as received in two buckets), so
	// while another space holds an intent it stays, for that space's reads.
	// A copy both spaces' releases leave at once is reclaimed by the cache's
	// time to live.
	if others == 0 {
		evict, err := q.shards.ReleasedSpec(digest)
		if err != nil {
			return nil, err
		}
		if _, err := ex.Submit(ctx, evict); err != nil {
			return nil, err
		}
	}
	st := registry.ReleasedIntent(space, digest)
	return releasedResult, ex.Write(ctx, st.SQL, st.Args...)
}

// releaseWaitLimit bounds how long an S3 verb spends running the releases it
// queued due at once, for the promptness it owes its caller: an Abort releases
// parked blobs on their providers before it returns. A release left when it
// passes runs in the background.
var releaseWaitLimit = 10 * time.Second

// ReleaseNow waits, best effort and within releaseWaitLimit, for the queue's
// workers to run the releases of digests in space that are due now: a sync
// no-op on each blob's subject runs once the due tasks before it, the release
// among them, have run. A release not yet due, as one a commit queued after
// its reader grace, stays for its time.
func (q *Queue) ReleaseNow(ctx context.Context, space did.DID, digests []multihash.Multihash) {
	if len(digests) == 0 {
		return
	}
	expires := time.Now().Add(releaseWaitLimit)
	for _, d := range digests {
		if err := q.Flush(ctx, registry.ReleaseSubject(space, d), expires); err != nil {
			return // the rest run in the background
		}
	}
}

// Flush runs the queue's no-op on subject, which waits for the due
// tasks before it to run on the queue's workers, until expires.
func (q *Queue) Flush(ctx context.Context, subject string, expires time.Time) error {
	ctx, cancel := context.WithDeadline(ctx, expires)
	defer cancel()
	_, err := q.tasks.Run(ctx, taskrunner.Spec{Queue: registry.ReleaseQueue, Subject: subject, Action: releaseFlush, Expires: &expires})
	return err
}

// DrainSpace runs a space's pending releases now, whatever their
// grace: DeleteBucket calls it before asking hilt to delete the space, which
// refuses while blobs remain registered. No reader grace is owed — the bucket
// is provably empty at that point and its deletion is the operator's explicit
// intent. The reference recheck still applies, and a release that still waits,
// as one a conclude in flight defers does, fails the delete for the client to
// retry.
func (q *Queue) DrainSpace(ctx context.Context, space did.DID) error {
	due, err := q.tasks.Expedite(ctx, registry.ReleaseQueue, registry.SpaceReleases(space), registry.ReleaseAction)
	if err != nil {
		return fmt.Errorf("expedite the space's releases: %w", err)
	}
	if len(due) == 0 {
		return nil
	}
	expires := time.Now().Add(releaseDrainTimeout)
	subjects := make([]string, 0, len(due))
	for subject := range due {
		subjects = append(subjects, subject)
		if err := q.Flush(ctx, subject, expires); err != nil {
			if errors.Is(err, taskrunner.ErrExpired) || (errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil) {
				return fmt.Errorf("the space's releases did not finish in time; retry")
			}
			return fmt.Errorf("release the space's blobs: %w", err)
		}
	}
	left, err := q.tasks.Pending(ctx, registry.ReleaseQueue, subjects, registry.ReleaseAction)
	if err != nil {
		return fmt.Errorf("check the space's releases: %w", err)
	}
	if len(left) > 0 {
		return fmt.Errorf("a release of the space's blobs waits, as a multipart session in flight still references it or a conclude of it is in flight; retry")
	}
	return nil
}

// releaseDrainTimeout is how long a bucket delete waits for its space's
// releases to finish before it fails, for the client to retry.
const releaseDrainTimeout = 30 * time.Second

// Add runs a queue-space-shard.add for a blob of a write to bucket in
// space, and returns the reference it took. sb is the blob's stream, for the
// streamed and part modes; when the space already held the blob, the stream's
// allocation is given up. release is the authority an expiry of the
// reference releases the blob with.
func (q *Queue) Add(ctx context.Context, space did.DID, bucket string, digest multihash.Multihash, size int64, mode string, sb *uploader.StreamedBlob, release registry.ReleaseTask) (taskrunner.TaskID, error) {
	var buf bytes.Buffer
	if err := (&AddTask{Mode: mode, Bucket: bucket, Size: size, Release: release}).MarshalCBOR(&buf); err != nil {
		return uuid.Nil, fmt.Errorf("encode add: %w", err)
	}
	if sb != nil {
		ctx = context.WithValue(ctx, addStreamKey{}, sb)
	}
	expires := time.Now().Add(q.addWait)
	out, err := q.tasks.Run(ctx, taskrunner.Spec{
		Queue:   registry.ReleaseQueue,
		Subject: registry.ReleaseSubject(space, digest),
		Action:  registry.AddAction,
		Args:    buf.Bytes(),
		Expires: &expires,
	})
	switch {
	case errors.Is(err, taskrunner.ErrExpired):
		// A release of the blob ran, or waited to retry, past the limit.
		return uuid.Nil, ErrBusy
	case err != nil:
		return uuid.Nil, err
	}
	res, err := decodeAddResult(out)
	if err != nil {
		return uuid.Nil, err
	}
	if res.reused && sb != nil {
		q.abandon(ctx, space, *sb)
	}
	return res.ref, nil
}

// runAdd is queue-space-shard.add: it makes the write's blob durable for the
// space and takes the write's pending reference to it, due to expire at
// pendingRefTTL. The reference and its expiry commit with the task; the
// blob's rows are recorded as the steps that make them true finish.
func (q *Queue) runAdd(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	space, digest, err := registry.ParseReleaseSubject(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	var args AddTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode add: %w", err))
	}
	sb, _ := ctx.Value(addStreamKey{}).(*uploader.StreamedBlob)
	if sb == nil && args.Mode != Copy {
		// The caller that held the stream has gone.
		return nil, taskrunner.Permanent(fmt.Errorf("add of %x: no stream", digest))
	}
	reused, err := q.addUse(ctx, space, digest, args, sb)
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
func (q *Queue) addUse(ctx context.Context, space did.DID, digest multihash.Multihash, args AddTask, sb *uploader.StreamedBlob) (bool, error) {
	if loc, err := q.locations.GetLocation(ctx, space, digest); err == nil && loc != nil {
		// Accepted for the space: the blob is reused. A copy's source
		// already has its intent.
		if args.Mode == Copy {
			return true, nil
		}
		if err := q.putIntent(ctx, space, digest, args, registry.IntentAccepted); err != nil {
			return false, err
		}
		if args.Mode == Part {
			// A located blob has no use for a park. One is still here when
			// another write accepted the same bytes while a part's park
			// stood; the upload's accept drops it otherwise, so a failure
			// here is logged.
			if err := q.parks.DeletePark(ctx, space, digest); err != nil {
				q.logger.Warn("drop stale park row failed; the upload's accept will retry",
					zap.Stringer("space", space), zap.Error(err))
			}
		}
		return true, nil
	} else if err != nil && !errors.Is(err, registry.ErrNotFound) {
		return false, fmt.Errorf("lookup location: %w", err)
	}
	switch args.Mode {
	case Copy:
		return false, ErrBlobGone
	case Part:
		return q.addPark(ctx, space, digest, args, *sb)
	case Streamed:
		return false, q.addConclude(ctx, space, digest, args, *sb)
	}
	return false, taskrunner.Permanent(fmt.Errorf("add mode %q", args.Mode))
}

// addPark records the park a part blob's stream left on its provider, traced
// as a blob.park span, unless the space has the blob parked already: that
// park stands, and the stream is surplus.
func (q *Queue) addPark(ctx context.Context, space did.DID, digest multihash.Multihash, args AddTask, sb uploader.StreamedBlob) (_ bool, err error) {
	if _, err := q.parks.GetPark(ctx, space, digest); err == nil {
		return true, q.putIntent(ctx, space, digest, args, registry.IntentParked)
	} else if !errors.Is(err, registry.ErrNotFound) {
		return false, fmt.Errorf("lookup park: %w", err)
	}
	ctx, span := tracing.Start(ctx, "blob.park",
		attribute.String("ingot.blob.result", "streamed"),
		attribute.Int64("ingot.blob.bytes", sb.Size),
	)
	defer func() { tracing.End(span, err) }()
	parked := sb.Parked(digest)
	recorded, err := q.parks.PutPark(ctx, registry.BlobPark{
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
	return !recorded, q.putIntent(ctx, space, digest, args, registry.IntentParked)
}

// addConclude accepts an object blob that went to its provider as the body
// arrived, traced as a blob.upload span, and records its location.
func (q *Queue) addConclude(ctx context.Context, space did.DID, digest multihash.Multihash, args AddTask, sb uploader.StreamedBlob) (err error) {
	ctx, span := tracing.Start(ctx, "blob.upload",
		attribute.String("ingot.blob.result", "streamed"),
		attribute.Int64("ingot.blob.bytes", sb.Size),
	)
	defer func() { tracing.End(span, err) }()
	// The blob may be accepted from here on.
	if err := q.putIntent(ctx, space, digest, args, registry.IntentUploading); err != nil {
		return err
	}
	locations, err := q.streaming.ConcludeBlobs(ctx, space, []uploader.UploadedBlob{sb.Parked(digest)})
	// An acceptance that came back is recorded before any error is acted on,
	// as a part's accept does: the upload service ran the accept whether or not
	// the batch then failed.
	if len(locations) == 1 && locations[0] != nil {
		if rerr := q.recordAccepted(ctx, space, digest, *locations[0]); rerr != nil {
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
func (q *Queue) putIntent(ctx context.Context, space did.DID, digest multihash.Multihash, args AddTask, state string) error {
	if err := q.intents.PutIntent(ctx, registry.UploadIntent{
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
func (q *Queue) runExpire(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	space, digest, err := registry.ParseReleaseSubject(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	args, err := registry.ReleaseTaskFrom(t)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	add, err := q.claims.PendingRefByExpiry(ctx, t.ID)
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
	n, err := q.claims.CountReferences(ctx, space, digest, add)
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

// DropRefsNow expires refs, the write's pending references to digests in
// space, at once, for a write that failed before anything took them over, and
// waits for the releases they leave due, best effort, as ReleaseNow does. It
// brings forward only these references' expiries: another write's references
// to the same blobs keep their deadlines.
func (q *Queue) DropRefsNow(ctx context.Context, space did.DID, refs []taskrunner.TaskID, digests []multihash.Multihash) {
	expiries, err := q.claims.ExpiriesOf(ctx, refs)
	if err == nil {
		err = q.tasks.ExpediteTasks(ctx, registry.ReleaseQueue, expiries...)
	}
	if err != nil {
		q.logger.Warn("expire a failed write's references; they expire at their deadline",
			zap.Stringer("space", space), zap.Error(err))
		return
	}
	// The first pass waits for the expiries, the second for the releases
	// they queue.
	q.ReleaseNow(ctx, space, digests)
	q.ReleaseNow(ctx, space, digests)
}

// defaultConcludeTimeout bounds one blob's conclude. The Forge client sets no
// timeout of its own, so without it a conclude whose answer never came would
// hold the blob's subject for good. A conclude that runs out fails the task,
// and the retry concludes it again, which the upload service treats as a
// repeat.
const defaultConcludeTimeout = 2 * time.Minute

// acceptRetry retries a failed accept a few times. A later part's accept, or
// Complete, queues whatever is left again.
var acceptRetry = taskrunner.RetryPolicy{First: 5 * time.Second, Max: time.Minute, Multiplier: 2, Attempts: 5}

// runAccept is queue-space-shard.accept: it concludes the blob its
// subject names, if it is still parked, and marks accepted, with its commit,
// the parts of the space's open sessions it completes. A blob already located
// needs nothing concluded (a dedup); a parked one concludes its deferred
// /http/put receipt, firing /blob/accept. A blob with neither has been
// released, and its parts are left parked. So is a parked blob nothing in the
// space references any more, as a dropped part's is: its release aborts the
// allocation instead.
func (q *Queue) runAccept(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) (_ []byte, err error) {
	space, digest, err := registry.ParseReleaseSubject(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	ctx, span := tracing.Start(ctx, "blobs.conclude", attribute.Int("ingot.blobs.total", 1))
	defer func() { tracing.End(span, err) }()

	var writes []registry.Statement
	if loc, err := q.locations.GetLocation(ctx, space, digest); err == nil && loc != nil {
		writes = registry.LocatedBlob(space, digest)
	} else if err != nil && !errors.Is(err, registry.ErrNotFound) {
		return nil, fmt.Errorf("lookup location: %w", err)
	} else {
		park, err := q.parks.GetPark(ctx, space, digest)
		if errors.Is(err, registry.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("lookup park: %w", err)
		}
		if n, err := q.claims.CountReferences(ctx, space, digest, uuid.Nil); err != nil {
			return nil, fmt.Errorf("count references: %w", err)
		} else if n == 0 {
			return nil, nil
		}
		blob, err := parkedBlob(digest, park)
		if err != nil {
			return nil, err
		}
		cctx, cancel := context.WithTimeout(ctx, q.concludeTimeout)
		locations, err := q.deferred.ConcludeBlobs(cctx, space, []uploader.UploadedBlob{blob})
		cancel()
		q.concluded(ctx, err)
		if err != nil {
			return nil, fmt.Errorf("conclude blob: %w", err)
		}
		if len(locations) == 0 || locations[0] == nil {
			return nil, fmt.Errorf("conclude blob %x: no location returned", digest)
		}
		span.SetAttributes(attribute.Int("ingot.blobs.concluded", 1))
		writes = registry.ConcludedBlob(space, digest, blob.AddTask.Bytes(), registry.BlobLocation{
			Provider: locations[0].Provider,
			URL:      locations[0].URL,
			Size:     locations[0].Size,
		})
	}
	writes = append(writes, registry.AcceptedParts(space, digest)...)
	for _, st := range writes {
		if err := ex.Write(ctx, st.SQL, st.Args...); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// parkedBlob is the blob a park left on its provider, ready to conclude.
func parkedBlob(digest multihash.Multihash, park *registry.BlobPark) (uploader.UploadedBlob, error) {
	addTask, err := cid.Cast(park.AddTask)
	if err != nil {
		return uploader.UploadedBlob{}, fmt.Errorf("decode park add task: %w", err)
	}
	acceptTask, err := cid.Cast(park.AcceptTask)
	if err != nil {
		return uploader.UploadedBlob{}, fmt.Errorf("decode park accept task: %w", err)
	}
	return uploader.UploadedBlob{
		Digest:        digest,
		Size:          park.Size,
		AddTask:       addTask,
		AcceptTask:    acceptTask,
		PutInvocation: park.PutInvocation,
	}, nil
}

// recordAccepted persists an accepted blob's location and marks its intent.
// The location is the proof eviction requires that the provider holds the
// blob, and where reads go once the local copy is gone.
//
// The location is written first, so a failure between the two writes leaves
// the intent in its earlier state with a location row, never accepted
// without one. An intent left uploading counts as a stalled upload, and a
// release of it, if one runs, finds the location and removes the blob from
// the provider. An accepted intent with no location is neither evictable nor
// counted as stalled.
func (q *Queue) recordAccepted(ctx context.Context, space did.DID, digest multihash.Multihash, loc uploader.BlobLocation) error {
	if err := q.locations.PutLocation(ctx, registry.BlobLocation{
		Space:    space,
		Digest:   digest,
		Provider: loc.Provider,
		URL:      loc.URL,
		Size:     loc.Size,
	}); err != nil {
		return fmt.Errorf("record location: %w", err)
	}
	if err := q.intents.SetIntentState(ctx, space, digest, registry.IntentAccepted); err != nil {
		return fmt.Errorf("mark accepted: %w", err)
	}
	return nil
}

// abandon gives up a streamed upload whose blob the space holds
// already (see Add): the allocation is aborted on its provider,
// best effort.
func (q *Queue) abandon(ctx context.Context, space did.DID, sb uploader.StreamedBlob) {
	ctx, cancel := context.WithTimeout(ctx, abandonTimeLimit)
	defer cancel()
	if err := q.streaming.AbortBlob(ctx, space, sb.AddTask); err != nil {
		q.logger.Warn("abandoning streamed upload failed; the provider expires it", zap.Stringer("add", sb.AddTask), zap.Error(err))
	}
}
