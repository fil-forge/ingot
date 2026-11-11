// Package multipartqueue runs every change to a multipart upload, as
// queue-multipart-upload tasks on the upload's subject, one at a time: its
// creation, its parts, the scheduling of their accepts, its Complete and its
// Abort. A part therefore never lands beside a Complete or an Abort, and a
// Complete and an Abort never overlap, without a lock or a latch on the
// session row. The slow part of an UploadPart, streaming its blobs and taking
// their references, happens in the request beforehand.
//
// Create, add-part and Complete run sync, and take what they write from
// their request through its context: a task abandoned by its caller can
// answer nobody, and fails without acting. The auto-abort a create schedules
// carries its own arguments, and so does the reap a Complete schedules.
package multipartqueue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fil-forge/ucantone/did"
	mh "github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// abortRetry retries an abort with no attempt limit: dropping the auto-abort
// would leave an abandoned upload open, holding its parts' blobs.
var abortRetry = taskrunner.RetryPolicy{First: time.Second, Max: 5 * time.Minute, Multiplier: 2}

// acceptRetry retries a failed accept a few times. A later part's accept, or
// Complete, queues whatever is left again.
var acceptRetry = taskrunner.RetryPolicy{First: 5 * time.Second, Max: time.Minute, Multiplier: 2, Attempts: 5}

// DefaultSessionTTL is how long an upload stays open before its auto-abort,
// and a completed one is kept for a repeated Complete before its reap.
const DefaultSessionTTL = 7 * 24 * time.Hour

// DefaultRehashBudget is the re-read Complete allows itself for non-final
// parts hashed at a wrong assumed offset: 5 GiB, S3's maximum part size. The
// final part is exempt: in a uniform-size upload it is the only part whose
// assumption can fail (it lands before any predecessor and its own size
// stands in), so it is always re-hashed when wrong, bounded by the part
// maximum. The budget therefore governs only variable-size parts uploaded out
// of order, and bounds how long Complete spends reading parts back from their
// storage nodes. A completion that would need more is refused
// (PartLayoutError); an object is never committed without its digest.
const DefaultRehashBudget = 5 << 30 // 5 GiB

// The waits a Complete allows itself, by default.
const (
	// DefaultCompleteWait bounds how long a Complete waits for its upload's
	// other changes, another Complete included, before giving up with
	// ErrInProgress.
	DefaultCompleteWait = 10 * time.Second
	// DefaultAcceptWait bounds how long Complete waits for its parts' blobs
	// to be accepted, behind accepts and releases already queued on them,
	// under the 60 s read timeout clients such as the AWS CLI apply.
	DefaultAcceptWait = 30 * time.Second
)

// addPartWaitLimit bounds how long an add-part waits for the upload's other
// changes before it is refused.
const addPartWaitLimit = 30 * time.Second

// acceptListPage is how many parked parts an upload's accept reads at a time.
const acceptListPage = 1000

// acceptWaits bounds how many blobs Complete waits on the accepts of at once.
const acceptWaits = 16

// Deps are what a Queue's tasks need.
type Deps struct {
	// Registry holds the buckets' state.
	Registry registry.Registry
	// Multipart holds the uploads and their parts.
	Multipart registry.MultipartStore
	// Intents says which of an upload's blobs are still there to release.
	Intents registry.IntentStore
	// Parks says whether a part's blob is still waiting for its accept.
	Parks registry.ParkStore
	// Read reads the buckets' catalogs, for a conditional Complete's check
	// of the key's current version.
	Read blockstore.ReadStore
	// Bodies reads a completing object's bytes back, for the parts whose
	// trees must be hashed again.
	Bodies BodyReader
	// Buckets admits a new upload to its bucket, and commits a Complete's
	// object.
	Buckets *bucketqueue.Queue
	// Blobs accepts the parts' blobs, and releases them.
	Blobs *spaceshardqueue.Queue
	// Capture captures a request's authority in space for the tasks a change
	// queues, which run after the request has gone.
	Capture func(ctx context.Context, space did.DID) bucketqueue.Authority
	// ReleaseGrace is how long after an abort a blob an object published is
	// released.
	ReleaseGrace time.Duration
	// SessionTTL is how long an upload stays open before its auto-abort, and
	// a completed one is kept; zero for DefaultSessionTTL, negative for
	// never.
	SessionTTL time.Duration
	// Limits bounds the queue's waits and a Complete's work; nil for the
	// defaults.
	Limits *Limits
	// CompleteDeferred counts the Completes answered ErrInProgress because
	// the parts' accepts did not finish in time; nil for none.
	CompleteDeferred metric.Int64Counter
	// Logger defaults to a no-op logger.
	Logger *zap.Logger
}

// Limits bounds a queue's waits and a Complete's work. New sets its zero
// fields to their defaults, and the queue reads them as it goes, so a test
// may change them once the queue runs.
type Limits struct {
	// AcceptDelay is how long after a part lands its upload's accept is due.
	AcceptDelay time.Duration
	// CompleteWait is the Complete's wait for the upload's other changes;
	// zero for DefaultCompleteWait.
	CompleteWait time.Duration
	// AcceptWait is the Complete's wait for its parts' accepts; zero for
	// DefaultAcceptWait.
	AcceptWait time.Duration
	// RehashBudget bounds a Complete's re-read; zero for
	// DefaultRehashBudget.
	RehashBudget int64
	// MaxBodyBlobs is the most blobs a Complete assembles into one object;
	// zero for msbucket.MaxBodyBlobs.
	MaxBodyBlobs int
}

// BodyReader reads an object's bytes back from its blobs.
type BodyReader interface {
	// BodyOpener opens the blobs that hold bytes [start, end] of a body in
	// space, decrypting unless plaintext.
	BodyOpener(ctx context.Context, space did.DID, blobs msbucket.BlobIndex, plaintext bool, start, end int64) (msbucket.BlobRangeOpener, error)
	// PlaintextLen is the length of the span an encrypted blob of
	// storedSize bytes covers.
	PlaintextLen(ctx context.Context, space did.DID, digest mh.Multihash, storedSize int64) (int64, error)
}

// Queue is the queue-multipart-upload queue.
type Queue struct {
	runner           *taskrunner.Runner
	reg              registry.Registry
	multipart        registry.MultipartStore
	intents          registry.IntentStore
	parks            registry.ParkStore
	read             blockstore.ReadStore
	bodies           BodyReader
	buckets          *bucketqueue.Queue
	blobs            *spaceshardqueue.Queue
	capture          func(ctx context.Context, space did.DID) bucketqueue.Authority
	releaseGrace     time.Duration
	sessionTTL       time.Duration
	limits           *Limits
	completeDeferred metric.Int64Counter
	logger           *zap.Logger
}

// New returns the queue and registers the queue-multipart-upload handlers
// with r. Call it before r starts.
func New(r *taskrunner.Runner, d Deps) (*Queue, error) {
	if d.Capture == nil {
		return nil, errors.New("multipartqueue: no authority capture")
	}
	q := &Queue{
		runner:           r,
		reg:              d.Registry,
		multipart:        d.Multipart,
		intents:          d.Intents,
		parks:            d.Parks,
		read:             d.Read,
		bodies:           d.Bodies,
		buckets:          d.Buckets,
		blobs:            d.Blobs,
		capture:          d.Capture,
		releaseGrace:     d.ReleaseGrace,
		sessionTTL:       d.SessionTTL,
		limits:           d.Limits,
		completeDeferred: d.CompleteDeferred,
		logger:           d.Logger,
	}
	if q.sessionTTL == 0 {
		q.sessionTTL = DefaultSessionTTL
	}
	if q.limits == nil {
		q.limits = &Limits{}
	}
	if q.limits.CompleteWait == 0 {
		q.limits.CompleteWait = DefaultCompleteWait
	}
	if q.limits.AcceptWait == 0 {
		q.limits.AcceptWait = DefaultAcceptWait
	}
	if q.limits.RehashBudget == 0 {
		q.limits.RehashBudget = DefaultRehashBudget
	}
	if q.limits.MaxBodyBlobs == 0 {
		q.limits.MaxBodyBlobs = msbucket.MaxBodyBlobs
	}
	if q.completeDeferred == nil {
		q.completeDeferred = noop.Int64Counter{}
	}
	if q.logger == nil {
		q.logger = zap.NewNop()
	}
	for action, h := range map[string]taskrunner.Handler{
		CreateAction:   q.runCreate,
		AddPartAction:  q.runAddPart,
		CompleteAction: q.runComplete,
		ReapAction:     q.runReap,
	} {
		if err := r.Handle(Name, action, h); err != nil {
			return nil, err
		}
	}
	if err := r.Handle(Name, AbortAction, q.runAbort, taskrunner.WithRetry(abortRetry)); err != nil {
		return nil, err
	}
	if err := r.Handle(Name, AcceptAction, q.runAccept, taskrunner.WithRetry(acceptRetry)); err != nil {
		return nil, err
	}
	return q, nil
}

// AbortTask is a queue-multipart-upload.abort task's arguments.
type AbortTask struct {
	// Release is the authority the releases of the parts' blobs remove them
	// with: the request's, or the create request's for the auto-abort.
	Release spaceshardqueue.ReleaseTask
	// Auto marks the auto-abort, which leaves an upload already completed or
	// gone alone instead of failing with ErrNoSuchUpload.
	Auto bool
}

// The context keys a sync queue-multipart-upload task finds its request
// under.
type (
	createKey   struct{}
	addPartKey  struct{}
	completeKey struct{}
	abortKey    struct{}
)

// addPartCall is an UploadPart's part, for its add-part, and what the add-part
// tells it back.
type addPartCall struct {
	part     registry.MultipartPart
	tree     *registry.PartTree
	release  spaceshardqueue.ReleaseTask
	released []mh.Multihash
}

// abortCall is what an abort tells its request back: the blobs whose
// releases it queued.
type abortCall struct {
	released []mh.Multihash
}

// acceptSpec returns the queue-multipart-upload.accept an UploadPart queues
// for uploadID, due at: a replacement, so an upload has at most one pending.
func acceptSpec(uploadID string, at time.Time) taskrunner.Spec {
	return taskrunner.Spec{
		Queue:     Name,
		Subject:   uploadID,
		Action:    AcceptAction,
		NotBefore: &at,
		Replace:   true,
	}
}

// uploadSpec is a queue-multipart-upload task on uploadID.
func uploadSpec(uploadID, action string, args []byte) taskrunner.Spec {
	return taskrunner.Spec{Queue: Name, Subject: uploadID, Action: action, Args: args}
}

// Create runs sess's queue-multipart-upload.create. It fails with the bucket
// queue's errors when the bucket does not admit the upload.
func (q *Queue) Create(ctx context.Context, sess *registry.MultipartSession) error {
	_, err := q.runner.Run(context.WithValue(ctx, createKey{}, sess), uploadSpec(sess.UploadID, CreateAction, nil))
	return err
}

// runCreate is queue-multipart-upload.create. Its bucket must admit it
// (queue-bucket.admit-upload, run as a child): the create holds the bucket's
// subject until it commits, so the upload exists before a DeleteBucket's
// close or is refused. It schedules the upload's auto-abort, with the create
// request's authority to release the parts' blobs.
func (q *Queue) runCreate(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	sess, _ := ctx.Value(createKey{}).(*registry.MultipartSession)
	if sess == nil {
		return nil, taskrunner.Permanent(errors.New("create: its request has gone"))
	}
	if err := q.buckets.AdmitUpload(ctx, ex, sess.Space, sess.Bucket); err != nil {
		return nil, err
	}
	st, err := registry.CreatedSession(*sess)
	if err != nil {
		return nil, err
	}
	if err := ex.Write(ctx, st.SQL, st.Args...); err != nil {
		return nil, err
	}
	if q.sessionTTL >= 0 {
		var buf bytes.Buffer
		if err := (&AbortTask{Release: q.capture(ctx, sess.Space).Release, Auto: true}).MarshalCBOR(&buf); err != nil {
			return nil, fmt.Errorf("encode auto-abort: %w", err)
		}
		spec := uploadSpec(t.Subject, AbortAction, buf.Bytes())
		at := time.Now().Add(q.sessionTTL)
		spec.NotBefore = &at
		if _, err := ex.Submit(ctx, spec); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// AddPart runs part's queue-multipart-upload.add-part, and returns the
// blobs of the part it replaced that it queued releases for. One waiting
// behind a Complete or another upload of the upload's parts past
// addPartWaitLimit fails with ErrBusy. An upload no longer open is
// ErrNoSuchUpload, and a reference to one of the part's blobs already gone
// registry.ErrReferenceExpired.
func (q *Queue) AddPart(ctx context.Context, part registry.MultipartPart, tree *registry.PartTree, release spaceshardqueue.ReleaseTask) ([]mh.Multihash, error) {
	call := &addPartCall{part: part, tree: tree, release: release}
	spec := uploadSpec(part.UploadID, AddPartAction, nil)
	expires := time.Now().Add(addPartWaitLimit)
	spec.Expires = &expires
	if _, err := q.runner.Run(context.WithValue(ctx, addPartKey{}, call), spec); err != nil {
		if errors.Is(err, taskrunner.ErrExpired) {
			return nil, ErrBusy
		}
		return nil, err
	}
	return call.released, nil
}

// runAddPart is queue-multipart-upload.add-part: it records the part, taking
// its blobs' pending references over, and queues the releases of the blobs
// the part it replaces named and it does not (registry.Postgres.PartWrites),
// and the upload's queue-multipart-upload.accept, due after the accept delay. An
// upload no longer open refuses it.
func (q *Queue) runAddPart(ctx context.Context, ex *taskrunner.Exec, _ taskrunner.Task) ([]byte, error) {
	call, _ := ctx.Value(addPartKey{}).(*addPartCall)
	if call == nil {
		return nil, taskrunner.Permanent(errors.New("add-part: its request has gone"))
	}
	w, err := q.multipart.PartWrites(ctx, call.part, call.tree)
	switch {
	case errors.Is(err, registry.ErrNotFound):
		return nil, ErrNoSuchUpload
	case err != nil:
		return nil, err
	}
	w.Spawns = append(w.Spawns, acceptSpec(call.part.UploadID, time.Now().Add(q.limits.AcceptDelay)))
	if err := spaceshardqueue.ApplyWrites(ctx, ex, w, call.release); err != nil {
		return nil, err
	}
	var released []mh.Multihash
	for _, r := range w.Releases {
		released = append(released, r.Digest)
	}
	call.released = released
	return nil, nil
}

// Abort runs sess's queue-multipart-upload.abort for a request, and returns
// the blobs whose releases it queued, due now. An upload already completed or
// gone is ErrNoSuchUpload.
func (q *Queue) Abort(ctx context.Context, sess registry.MultipartSession) ([]mh.Multihash, error) {
	var buf bytes.Buffer
	if err := (&AbortTask{Release: q.capture(ctx, sess.Space).Release}).MarshalCBOR(&buf); err != nil {
		return nil, fmt.Errorf("multipartqueue: encode abort: %w", err)
	}
	call := &abortCall{}
	if _, err := q.runner.Run(context.WithValue(ctx, abortKey{}, call), uploadSpec(sess.UploadID, AbortAction, buf.Bytes())); err != nil {
		return nil, err
	}
	return call.released, nil
}

// runAbort is queue-multipart-upload.abort: it deletes an open upload and
// its parts and queues the release of each of their blobs still there to
// release (sessionReleases): due at once, or after the reader grace for a
// blob an object published. An upload already completed or gone is
// ErrNoSuchUpload for a request's abort, and nothing for the auto-abort.
func (q *Queue) runAbort(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args AbortTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode abort: %w", err))
	}
	sess, err := q.multipart.GetSession(ctx, t.Subject)
	if err != nil && !errors.Is(err, registry.ErrNotFound) {
		return nil, fmt.Errorf("get session: %w", err)
	}
	if err != nil || sess.State != registry.SessionOpen {
		if args.Auto {
			return nil, nil
		}
		return nil, ErrNoSuchUpload
	}
	digests, published, err := q.sessionReleases(ctx, sess.Space, t.Subject)
	if err != nil {
		return nil, err
	}
	if err := spawnReleases(ctx, ex, sess.Space, digests, args.Release, registry.DueNow()); err != nil {
		return nil, err
	}
	graceDue := time.Now().Add(q.releaseGrace)
	if q.releaseGrace <= 0 {
		graceDue = registry.DueNow()
	}
	if err := spawnReleases(ctx, ex, sess.Space, published, args.Release, graceDue); err != nil {
		return nil, err
	}
	st := registry.DeletedSession(t.Subject)
	if err := ex.Write(ctx, st.SQL, st.Args...); err != nil {
		return nil, err
	}
	if call, _ := ctx.Value(abortKey{}).(*abortCall); call != nil {
		call.released = digests
	}
	return nil, nil
}

// sessionReleases returns the blobs of uploadID's parts that are still there
// to release, each once, for an open upload's abort to queue their releases,
// as every reference that goes does. The blob's upload intent says which
// those are. A published intent is a committed blob — the state its first
// reference claim wrote — so it may have been readable through an object:
// those are returned apart, as published, for releases after the reader
// grace. While the upload was open its parts were references, so an object's
// release that ran then found the blob referenced and did nothing, and this
// release is the one that frees it. No intent at all is a blob already
// released in full: every written blob gets an intent, and only a release's
// commit removes one. Releasing such a blob again would ask to remove a blob
// that may never have left this node, which nothing authorizes and no retry
// can complete. Whether a blob is in fact free to go is the release's
// question, answered when it runs: it does nothing for a digest anything in
// the space still references.
func (q *Queue) sessionReleases(ctx context.Context, space did.DID, uploadID string) (digests, published []mh.Multihash, err error) {
	parts, err := q.multipart.ListParts(ctx, uploadID)
	if err != nil {
		return nil, nil, fmt.Errorf("list parts: %w", err)
	}
	seen := map[string]bool{}
	for _, p := range parts {
		for _, d := range p.BlobDigests {
			if seen[string(d)] {
				continue
			}
			seen[string(d)] = true
			in, err := q.intents.GetIntent(ctx, space, d)
			switch {
			case errors.Is(err, registry.ErrNotFound):
				// Already released in full, intent and all.
				continue
			case err != nil:
				return nil, nil, fmt.Errorf("lookup intent: %w", err)
			case in.State == registry.IntentPublished:
				// Committed, so possibly read: released after the grace.
				published = append(published, d)
				continue
			}
			digests = append(digests, d)
		}
	}
	return digests, published, nil
}

// Reap runs uploadID's queue-multipart-upload.reap at once, for a
// DeleteBucket that clears the bucket's completed uploads.
func (q *Queue) Reap(ctx context.Context, uploadID string) error {
	_, err := q.runner.Run(ctx, uploadSpec(uploadID, ReapAction, nil))
	return err
}

// runReap is queue-multipart-upload.reap: it deletes a completed upload and
// its part rows, kept until now so a repeated Complete got the same answer.
// It releases nothing: the Complete queued the releases of the parts it left
// out with its own commit, and the object's claims hold the rest.
func (q *Queue) runReap(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	st := registry.ReapedSession(t.Subject)
	return nil, ex.Write(ctx, st.SQL, st.Args...)
}

// spawnReleases queues, with the task, a release due at of each of digests
// in space, carrying release's authority.
func spawnReleases(ctx context.Context, ex *taskrunner.Exec, space did.DID, digests []mh.Multihash, release spaceshardqueue.ReleaseTask, at time.Time) error {
	specs := make([]taskrunner.Spec, len(digests))
	for i, d := range digests {
		var err error
		if specs[i], err = release.Spec(space, d, at); err != nil {
			return err
		}
	}
	_, err := ex.SubmitAll(ctx, specs)
	return err
}
