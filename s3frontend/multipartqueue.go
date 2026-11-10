package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// Every change to a multipart upload runs on the upload's subject on
// queue-multipart-upload, one at a time: its creation, its parts, its
// Complete and its Abort. A part therefore never lands beside a Complete or
// an Abort, and a Complete and an Abort never overlap, without a lock or a
// latch on the session row. The slow part of an UploadPart, streaming its
// blobs and taking their references, happens in the request beforehand.
//
// Create, add-part and Complete run sync, and take what they write from
// their request through its context: a task abandoned by its caller can
// answer nobody, and fails without acting. The auto-abort a create schedules
// carries its own arguments, and so does the reap a Complete schedules.

// abortRetry retries an abort with no attempt limit: dropping the auto-abort
// would leave an abandoned upload open, holding its parts' blobs.
var abortRetry = taskrunner.RetryPolicy{First: time.Second, Max: 5 * time.Minute, Multiplier: 2}

// defaultSessionTTL is how long an upload stays open before its auto-abort,
// and a completed one is kept for a repeated Complete before its reap.
const defaultSessionTTL = 7 * 24 * time.Hour

// MultipartAbortTask is a queue-multipart-upload.abort task's arguments.
type MultipartAbortTask struct {
	// Release is the authority the releases of the parts' blobs remove them
	// with: the request's, or the create request's for the auto-abort.
	Release registry.ReleaseTask
	// Auto marks the auto-abort, which leaves an upload already completed or
	// gone alone instead of answering NoSuchUpload.
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
	release  registry.ReleaseTask
	released []mh.Multihash
}

// abortCall is what an abort tells its request back: the blobs whose
// releases it queued, for releaseNow.
type abortCall struct {
	released []mh.Multihash
}

// registerMultipartTasks registers the queue-multipart-upload handlers with r.
// Call it before r starts.
func (b *Backend) registerMultipartTasks(r *taskrunner.Runner) error {
	b.uploads = r
	for action, h := range map[string]taskrunner.Handler{
		registry.MultipartCreateAction:   b.runCreateUpload,
		registry.MultipartAddPartAction:  b.runAddPart,
		registry.MultipartCompleteAction: b.runComplete,
		registry.MultipartReapAction:     b.runReap,
	} {
		if err := r.Handle(registry.MultipartQueue, action, h); err != nil {
			return err
		}
	}
	return r.Handle(registry.MultipartQueue, registry.MultipartAbortAction, b.runAbort, taskrunner.WithRetry(abortRetry))
}

// uploadSpec is a queue-multipart-upload task on uploadID.
func uploadSpec(uploadID, action string, args []byte) taskrunner.Spec {
	return taskrunner.Spec{Queue: registry.MultipartQueue, Subject: uploadID, Action: action, Args: args}
}

// sessionTTL is how long an upload stays open before its auto-abort, and a
// completed one is kept; negative for never.
func (b *Backend) sessionTTL() time.Duration {
	if b.multipartTTL == 0 {
		return defaultSessionTTL
	}
	return b.multipartTTL
}

// createUpload runs sess's queue-multipart-upload.create.
func (b *Backend) createUpload(ctx context.Context, sess *registry.MultipartSession) error {
	_, err := b.uploads.Run(context.WithValue(ctx, createKey{}, sess), uploadSpec(sess.UploadID, registry.MultipartCreateAction, nil))
	return err
}

// runCreateUpload is queue-multipart-upload.create. Its bucket must admit
// it (queue-bucket.admit-upload, run as a child): the create holds the
// bucket's subject until it commits, so the upload exists before a
// DeleteBucket's close or is refused. It schedules the upload's auto-abort,
// with the create request's authority to release the parts' blobs.
func (b *Backend) runCreateUpload(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	sess, _ := ctx.Value(createKey{}).(*registry.MultipartSession)
	if sess == nil {
		return nil, taskrunner.Permanent(errors.New("create: its request has gone"))
	}
	admit, err := encodeBucketTask(&BucketTask{Name: sess.Bucket})
	if err != nil {
		return nil, err
	}
	if _, err := ex.Run(ctx, b.bucketSpec(sess.Space, registry.BucketAdmitUploadAction, admit)); err != nil {
		return nil, err
	}
	st, err := registry.CreatedSession(*sess)
	if err != nil {
		return nil, err
	}
	if err := ex.Write(ctx, st.SQL, st.Args...); err != nil {
		return nil, err
	}
	if ttl := b.sessionTTL(); ttl >= 0 {
		var buf bytes.Buffer
		if err := (&MultipartAbortTask{Release: b.releaseAuthority(ctx, sess.Space), Auto: true}).MarshalCBOR(&buf); err != nil {
			return nil, fmt.Errorf("encode auto-abort: %w", err)
		}
		spec := uploadSpec(t.Subject, registry.MultipartAbortAction, buf.Bytes())
		at := time.Now().Add(ttl)
		spec.NotBefore = &at
		if _, err := ex.Submit(ctx, spec); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// addPartWaitLimit bounds how long an add-part waits for the upload's other
// changes before it is refused.
const addPartWaitLimit = 30 * time.Second

// addPart runs part's queue-multipart-upload.add-part, and returns the
// blobs of the part it replaced that it queued releases for. One waiting
// behind a Complete or another upload of the upload's parts past
// addPartWaitLimit is refused with SlowDown, which the client retries.
func (b *Backend) addPart(ctx context.Context, part registry.MultipartPart, tree *registry.PartTree, release registry.ReleaseTask) ([]mh.Multihash, error) {
	call := &addPartCall{part: part, tree: tree, release: release}
	spec := uploadSpec(part.UploadID, registry.MultipartAddPartAction, nil)
	expires := time.Now().Add(addPartWaitLimit)
	spec.Expires = &expires
	if _, err := b.uploads.Run(context.WithValue(ctx, addPartKey{}, call), spec); err != nil {
		if errors.Is(err, taskrunner.ErrExpired) {
			return nil, s3err.GetAPIError(s3err.ErrSlowDown)
		}
		return nil, err
	}
	return call.released, nil
}

// runAddPart is queue-multipart-upload.add-part: it records the part, taking
// its blobs' pending references over, and queues the releases of the blobs
// the part it replaces named and it does not (registry.Postgres.PartWrites),
// and the upload's queue-multipart-upload.accept, due after acceptDelay. An
// upload no longer open refuses it.
func (b *Backend) runAddPart(ctx context.Context, ex *taskrunner.Exec, _ taskrunner.Task) ([]byte, error) {
	call, _ := ctx.Value(addPartKey{}).(*addPartCall)
	if call == nil {
		return nil, taskrunner.Permanent(errors.New("add-part: its request has gone"))
	}
	w, released, err := b.multipart.PartWrites(ctx, call.part, call.tree, call.release)
	switch {
	case errors.Is(err, registry.ErrNotFound):
		return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	case errors.Is(err, registry.ErrReferenceExpired):
		return nil, s3err.GetAPIError(s3err.ErrSlowDown)
	case err != nil:
		return nil, err
	}
	w.Spawns = append(w.Spawns, registry.AcceptSpec(call.part.UploadID, time.Now().Add(b.acceptDelay)))
	if err := applyCommitWrites(ctx, ex, w); err != nil {
		return nil, err
	}
	call.released = released
	return nil, nil
}

// abortUpload runs sess's queue-multipart-upload.abort for a request and
// then the releases it queued, within releaseNow's budget.
func (b *Backend) abortUpload(ctx context.Context, sess registry.MultipartSession) error {
	var buf bytes.Buffer
	if err := (&MultipartAbortTask{Release: b.releaseAuthority(ctx, sess.Space)}).MarshalCBOR(&buf); err != nil {
		return fmt.Errorf("s3frontend: encode abort: %w", err)
	}
	call := &abortCall{}
	if _, err := b.uploads.Run(context.WithValue(ctx, abortKey{}, call), uploadSpec(sess.UploadID, registry.MultipartAbortAction, buf.Bytes())); err != nil {
		return err
	}
	b.blobs.ReleaseNow(ctx, sess.Space, call.released)
	return nil
}

// runAbort is queue-multipart-upload.abort: it deletes an open upload and
// its parts and queues the release of each of their blobs still there to
// release (sessionReleases): due at once, or after the reader grace for a
// blob an object published. An upload already completed or
// gone is NoSuchUpload for a request's abort, and nothing for the
// auto-abort.
func (b *Backend) runAbort(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args MultipartAbortTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode abort: %w", err))
	}
	sess, err := b.multipart.GetSession(ctx, t.Subject)
	if err != nil && !errors.Is(err, registry.ErrNotFound) {
		return nil, fmt.Errorf("get session: %w", err)
	}
	if err != nil || sess.State != registry.SessionOpen {
		if args.Auto {
			return nil, nil
		}
		return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	digests, published, err := b.sessionReleases(ctx, sess.Space, t.Subject)
	if err != nil {
		return nil, err
	}
	if err := spawnReleases(ctx, ex, sess.Space, digests, args.Release, registry.DueNow()); err != nil {
		return nil, err
	}
	graceDue := time.Now().Add(b.releaseGrace)
	if b.releaseGrace <= 0 {
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

// reapNow runs uploadID's queue-multipart-upload.reap at once, for a
// DeleteBucket that clears the bucket's completed uploads.
func (b *Backend) reapNow(ctx context.Context, uploadID string) error {
	_, err := b.uploads.Run(ctx, uploadSpec(uploadID, registry.MultipartReapAction, nil))
	return err
}

// runReap is queue-multipart-upload.reap: it deletes a completed upload and
// its part rows, kept until now so a repeated Complete got the same answer.
// It releases nothing: the Complete queued the releases of the parts it left
// out with its own commit, and the object's claims hold the rest.
func (b *Backend) runReap(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	st := registry.ReapedSession(t.Subject)
	return nil, ex.Write(ctx, st.SQL, st.Args...)
}

// spawnReleases queues, with the task, a release due at of each of digests
// in space, carrying release's authority.
func spawnReleases(ctx context.Context, ex *taskrunner.Exec, space did.DID, digests []mh.Multihash, release registry.ReleaseTask, at time.Time) error {
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

// applyCommitWrites adds w to the task's commit: its guards, then its
// statements (a part's tree row follows the part row its guard writes), its
// spawns and the expiries it cancels.
func applyCommitWrites(ctx context.Context, ex *taskrunner.Exec, w registry.CommitWrites) error {
	for _, st := range w.Guards {
		if err := ex.WriteConditional(ctx, st.SQL, st.Args...); err != nil {
			return err
		}
	}
	for _, st := range w.Statements {
		if err := ex.Write(ctx, st.SQL, st.Args...); err != nil {
			return err
		}
	}
	if _, err := ex.SubmitAll(ctx, w.Spawns); err != nil {
		return err
	}
	return ex.Cancel(ctx, registry.ReleaseQueue, w.Expiries...)
}
