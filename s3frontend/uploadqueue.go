package s3frontend

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	uploadcmds "github.com/fil-forge/libforge/commands/upload"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/uploader"
)

// A version's place in its space's object count is kept by queue-space-upload
// tasks: a commit queues an add for the version it creates and a remove for
// one it retires, in its own transaction, and workers send them to the upload
// service off the request path. The object count is a reporting number, so a
// round trip to the upload service does not belong in a PUT, and an outage
// should delay the count rather than fail writes or lose it.
//
// The queue's subject is the space, so a space's changes go out in commit
// order. That order is what keeps the count honest: a root is registered by
// the commit that creates its version and retracted by a later one, and a
// retraction that overtook its addition would leave the root counted for good.
// A batch takes only consecutive changes of one kind, so adds and removes stay
// in sequence, and a change that has to be retried holds back the ones behind
// it.

// uploadRegistrationBatch is the most changes one round trip carries.
const uploadRegistrationBatch = 256

// uploadRegistrationDeadLetterAfter is how many attempts a change gets once
// its authority is spent before it is failed for good. Retrying costs nothing
// for a while, in case the space is written again and leaves fresh authority
// behind; past that the change is set aside, because one that can never be
// made must not hold back the ones behind it for good.
const uploadRegistrationDeadLetterAfter = 10

// uploadRegistrationRetry is how a refused or unsent change is retried: with
// no attempt limit, since dropping it would leave the space's object count
// wrong, backing off to a few minutes, and holding its space so the changes
// behind it keep their order.
var uploadRegistrationRetry = taskrunner.RetryPolicy{
	First:        time.Second,
	Max:          5 * time.Minute,
	Multiplier:   2,
	HoldsSubject: true,
}

// uploadFlush is the queue's sync no-op: running it on a space waits for the
// space's due changes to be sent, so a caller that needs them sent before it
// goes on can wait for them.
const uploadFlush = "flush"

// uploadDrainTimeout is how long a bucket delete waits for its space's changes
// to be sent before it fails, for the client to retry.
const uploadDrainTimeout = 30 * time.Second

// registerUploadTasks registers the queue-space-upload handlers with r, and
// keeps r for draining a space's changes when its bucket is deleted. Call it
// before r starts.
func (b *Backend) registerUploadTasks(r *taskrunner.Runner) error {
	b.tasks = r
	for _, op := range []registry.UploadRegistrationOp{registry.UploadRegistrationAdd, registry.UploadRegistrationRemove} {
		if err := r.HandleBatch(registry.UploadQueue, string(op), uploadRegistrationBatch,
			func(ctx context.Context, _ *taskrunner.Exec, tasks []taskrunner.Task) ([]taskrunner.Outcome, error) {
				return b.sendUploadTasks(ctx, op, tasks)
			}, taskrunner.WithRetry(b.uploadRetry)); err != nil {
			return err
		}
	}
	return r.Handle(registry.UploadQueue, uploadFlush, func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		return nil, nil
	})
}

// sendUploadTasks sends a batch of one op's queued changes and reports each
// one's outcome. A change that goes back for another try with authority
// renewed for it keeps the renewal in its arguments, so a restart before the
// retry does not leave it with the spent chain it started with.
func (b *Backend) sendUploadTasks(ctx context.Context, op registry.UploadRegistrationOp, tasks []taskrunner.Task) ([]taskrunner.Outcome, error) {
	out := make([]taskrunner.Outcome, len(tasks))
	regs := make([]registry.UploadRegistration, 0, len(tasks))
	attempts := make([]int, 0, len(tasks))
	index := make([]int, 0, len(tasks))
	for i, t := range tasks {
		reg, err := registry.UploadRegistrationFromTask(t)
		if err != nil {
			// Not retryable: a change nothing can decode would hold its space
			// back for good.
			out[i].Err = taskrunner.Permanent(err)
			continue
		}
		regs = append(regs, reg)
		attempts = append(attempts, t.Attempt)
		index = append(index, i)
	}
	errs, renewed := b.sendRegistrations(ctx, op, regs, attempts)
	for j, i := range index {
		out[i].Err = errs[j]
		if errs[j] == nil || renewed[j] == nil {
			continue
		}
		reg := regs[j]
		reg.Proofs = renewed[j]
		if spec, err := reg.Spec(); err == nil {
			out[i].Args = spec.Args
		}
	}
	return out, nil
}

// sendRegistrations sends one op's changes in one round trip, after making
// sure each carries usable authority, and returns each change's outcome in
// order: nil when the upload service took it, otherwise the error to retry it
// on, which is permanent once a change whose authority is spent has used its
// attempts. A transport failure, where nothing was answered, is every sent
// change's error. attempts holds how many earlier attempts each change
// failed. renewed holds, for each change, the authority renewed for it this
// time, or nil if its own was still usable or could not be renewed.
//
// Within a call the changes are independent: each names the manifest of a
// distinct version.
func (b *Backend) sendRegistrations(ctx context.Context, op registry.UploadRegistrationOp, regs []registry.UploadRegistration, attempts []int) (errs []error, renewed [][]byte) {
	errs = make([]error, len(regs))
	renewed = make([][]byte, len(regs))
	batch := make([]uploader.QueuedUpload, 0, len(regs))
	sent := make([]int, 0, len(regs))
	for i, reg := range regs {
		proofs, fresh, err := b.proofs.Prepare(ctx, reg.Space, registrationCommand(op), reg.Proofs)
		if err != nil {
			errs[i] = b.spentAuthority(reg, op, attempts[i], err)
			continue
		}
		if fresh {
			renewed[i] = proofs
		}
		batch = append(batch, uploader.QueuedUpload{Space: reg.Space, Root: reg.Root, Proofs: proofs})
		sent = append(sent, i)
	}
	if len(batch) == 0 {
		return errs, renewed
	}
	send := b.registrar.RegisterUploads
	if op == registry.UploadRegistrationRemove {
		send = b.registrar.RetractUploads
	}
	results, err := send(ctx, batch)
	if err != nil {
		b.logger.Warn("upload registrations: upload service unreachable; batch will be retried",
			zap.String("op", string(op)), zap.Int("changes", len(batch)), zap.Error(err))
		err = fmt.Errorf("s3frontend: send upload registrations: %w", err)
		for _, i := range sent {
			errs[i] = err
		}
		return errs, renewed
	}
	for j, i := range sent {
		if results[j] == nil {
			continue
		}
		reg := regs[i]
		b.logger.Warn("upload registrations: upload service refused a change; will retry",
			zap.String("bucket", reg.Bucket),
			zap.String("op", string(op)),
			zap.Stringer("space", reg.Space),
			zap.Stringer("root", reg.Root),
			zap.Int("attempts", attempts[i]+1),
			zap.Error(results[j]),
		)
		errs[i] = results[j]
	}
	return errs, renewed
}

// spentAuthority is the outcome of a change whose stored authority has run out
// with nothing in the space to renew it from: retried while attempts remain,
// in case the space is written again, and failed for good after that.
func (b *Backend) spentAuthority(reg registry.UploadRegistration, op registry.UploadRegistrationOp, attempts int, err error) error {
	if attempts+1 < uploadRegistrationDeadLetterAfter {
		b.logger.Warn("upload registrations: authority spent; will retry in case the space is written again",
			zap.String("bucket", reg.Bucket),
			zap.Stringer("space", reg.Space),
			zap.Int("attempts", attempts+1),
			zap.Error(err),
		)
		return err
	}
	// Which way the count is wrong depends on the op, and the two are not the
	// same mistake: an addition never made leaves the object uncounted, while
	// a retraction never made leaves a version counted after it is gone. Say
	// which, so the number can be reconciled from the failed task rather than
	// guessed at.
	effect := "object count will be short by one"
	if op == registry.UploadRegistrationRemove {
		effect = "object count will stay high by one, the retired version still counted"
	}
	b.logger.Error("upload registrations: failing a change whose authority cannot be renewed; "+effect,
		zap.String("bucket", reg.Bucket),
		zap.String("key", reg.ObjectKey),
		zap.String("op", string(op)),
		zap.Stringer("space", reg.Space),
		zap.Stringer("root", reg.Root),
		zap.Error(err),
	)
	return taskrunner.Permanent(fmt.Errorf("authority expired and could not be renewed: %w", err))
}

// registrationCommand is the capability a queued change invokes.
func registrationCommand(op registry.UploadRegistrationOp) ucan.Command {
	if op == registry.UploadRegistrationRemove {
		return uploadcmds.Remove.Command
	}
	return uploadcmds.Add.Command
}

// DrainSpaceRegistrations sends a space's queued object-count changes before
// its bucket is deleted, rather than leaving them to the workers.
//
// It is needed because the two ends of a deletion are cleaned up by different
// people. Emptying a bucket queues a retraction per version, and nothing else
// retracts those roots once the space is gone: hilt's delete checks that the
// space holds no blobs and never touches its content entries, and the upload
// service keeps a space's entries and counters after the space is gone. So a
// bucket emptied and deleted in one go would leave every one of its objects
// counted for good if its retractions had not gone out.
//
// It also resends the space's retractions that failed for good, with the
// authority this request carries in its own right, since this is the last
// chance anyone has to send them. A failed addition is not resent: one that
// never landed leaves nothing behind to retract.
//
// Failing here fails the delete, which is safe: it runs before hilt has
// removed anything, so the bucket is still whole and the retry is clean.
func (b *Backend) DrainSpaceRegistrations(ctx context.Context, space did.DID) error {
	if err := b.flushSpaceRegistrations(ctx, space); err != nil {
		return err
	}

	// Every failed retraction is listed before any is resent: one that fails
	// again comes back as a new failed task, which a listing still paging
	// would reach and resend for as long as the service kept refusing it.
	failed, err := b.failedRetractions(ctx, space)
	if err != nil {
		return fmt.Errorf("s3frontend: list failed upload registrations: %w", err)
	}
	var resent int
	for _, f := range failed {
		reg, err := registry.UploadRegistrationFromTask(f.Task)
		if err != nil {
			continue
		}
		proofs, err := b.proofs.Capture(ctx, space, uploadcmds.Remove.Command)
		if err != nil {
			return fmt.Errorf("s3frontend: authority to resend a retraction: %w", err)
		}
		reg.Proofs = proofs
		spec, err := reg.Spec()
		if err != nil {
			return err
		}
		if _, err := b.tasks.Resubmit(ctx, registry.UploadQueue, f.Task.ID, spec.Args); err != nil && !errors.Is(err, taskrunner.ErrNotFound) {
			return fmt.Errorf("s3frontend: resend a failed retraction: %w", err)
		}
		resent++
	}
	if resent == 0 {
		return nil
	}
	if err := b.flushSpaceRegistrations(ctx, space); err != nil {
		return err
	}
	// A resent retraction the upload service refuses is retried, which holds
	// the space, so the flush above runs out of time and the delete is
	// retried. One can only be failed again here by a permanent error, which
	// every retry of the delete would meet too, so the delete goes ahead
	// rather than leave the bucket undeletable. The failed task outlives the
	// bucket, so the count it leaves behind can be reconciled from it.
	left, err := b.failedRetractions(ctx, space)
	if err != nil {
		b.logger.Warn("delete bucket: could not check for retractions that failed again", zap.Stringer("space", space), zap.Error(err))
		return nil
	}
	for _, f := range left {
		b.logger.Error("delete bucket: a retraction could not be sent; the upload service keeps this root and its count",
			zap.Stringer("space", space),
			zap.Stringer("task", f.Task.ID),
			zap.String("reason", f.Error),
		)
	}
	return nil
}

// failedRetractions lists all of the space's retractions that failed for
// good, a page at a time, passing over its failed additions.
func (b *Backend) failedRetractions(ctx context.Context, space did.DID) ([]taskrunner.FailedTask, error) {
	limit := cmp.Or(b.failedPage, uploadRegistrationBatch)
	var out []taskrunner.FailedTask
	var after taskrunner.TaskID
	for {
		page, err := b.tasks.ListFailed(ctx, registry.UploadQueue, space.String(), after, limit)
		if err != nil {
			return nil, err
		}
		for _, f := range page {
			if f.Task.Action == string(registry.UploadRegistrationRemove) {
				out = append(out, f)
			}
		}
		if len(page) < limit {
			return out, nil
		}
		after = page[len(page)-1].Task.ID
	}
}

// flushSpaceRegistrations waits for the space's due changes to be sent, by
// running the queue's no-op sync task on it, which runs once the workers have
// run the older tasks. A change waiting out a retry holds the space, so the
// flush expires and the delete asks the client to retry.
func (b *Backend) flushSpaceRegistrations(ctx context.Context, space did.DID) error {
	expires := time.Now().Add(b.drainTimeout)
	_, err := b.tasks.Run(ctx, taskrunner.Spec{Queue: registry.UploadQueue, Subject: space.String(), Action: uploadFlush, Expires: &expires})
	if errors.Is(err, taskrunner.ErrExpired) {
		return fmt.Errorf("s3frontend: the upload service has not taken this space's object-count changes; retry")
	}
	if err != nil {
		return fmt.Errorf("s3frontend: drain space registrations: %w", err)
	}
	return nil
}
