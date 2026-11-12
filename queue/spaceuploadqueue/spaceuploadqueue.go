// Package spaceuploadqueue is queue-space-upload, which keeps each version's
// place in its space's object count: a commit queues an add for the version
// it creates and a remove for one it retires, in its own transaction, and workers send them to the upload
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
package spaceuploadqueue

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	uploadcmds "github.com/fil-forge/libforge/commands/upload"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/internal/authority"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/uploader"
)

// Op is what a queued registration asks the upload service to do with a
// version's content entry. It is the registration task's action.
type Op string

const (
	// Add records a committed version as a content entry, so the space
	// counts it.
	Add Op = "add"
	// Remove drops a retired version's entry, so the space stops counting
	// it. It does not touch the version's blobs — those are
	// released per digest by the reference index.
	Remove Op = "remove"
)

// Name is the task queue upload registrations run on. Its subject is the
// space, so a space's changes are sent in commit order: out of order, an add
// can land after the remove that retires it and leave the root counted for
// good.
const Name = "queue-space-upload"

// Registration is one change to the upload service's content-entry list for
// a space. A commit queues it as a task in its own transaction
// (registry.RootCommit.Registrations), so the change exists exactly when the
// version it describes committed.
type Registration struct {
	// Bucket and ObjectKey name the object the change belongs to, for logs.
	Bucket    string
	ObjectKey string
	// Space is the space whose count changes, and the task's subject.
	Space did.DID
	// Root is the version's manifest, the content entry's root.
	Root cid.Cid
	// Op says whether the entry is added or removed.
	Op Op
	// Proofs is the encoded UCAN container holding the delegation chain that
	// authorizes the invocation, captured from the request that committed the
	// version. The task runs with no request to borrow authority from, and
	// the space may never be written again, so it carries its own. It is
	// bearer authority for one space and one command.
	Proofs []byte
}

// Task is a queued registration's arguments: everything but the space, which
// is the task's subject, and the op, which is its action.
type Task struct {
	Bucket    string
	ObjectKey string
	Root      []byte
	Proofs    []byte
}

// Spec returns the task that carries reg.
func (reg Registration) Spec() (taskrunner.Spec, error) {
	var buf bytes.Buffer
	args := Task{Bucket: reg.Bucket, ObjectKey: reg.ObjectKey, Root: reg.Root.Bytes(), Proofs: reg.Proofs}
	if err := args.MarshalCBOR(&buf); err != nil {
		return taskrunner.Spec{}, fmt.Errorf("spaceuploadqueue: encode upload registration: %w", err)
	}
	return taskrunner.Spec{Queue: Name, Subject: reg.Space.String(), Action: string(reg.Op), Args: buf.Bytes()}, nil
}

// registrationFromTask decodes the registration a task carries.
func registrationFromTask(t taskrunner.Task) (Registration, error) {
	var args Task
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return Registration{}, fmt.Errorf("spaceuploadqueue: decode upload registration: %w", err)
	}
	space, err := did.Parse(t.Subject)
	if err != nil {
		return Registration{}, fmt.Errorf("spaceuploadqueue: upload registration space: %w", err)
	}
	_, root, err := cid.CidFromBytes(args.Root)
	if err != nil {
		return Registration{}, fmt.Errorf("spaceuploadqueue: upload registration root: %w", err)
	}
	return Registration{
		Bucket:    args.Bucket,
		ObjectKey: args.ObjectKey,
		Space:     space,
		Root:      root,
		Op:        Op(t.Action),
		Proofs:    args.Proofs,
	}, nil
}

// batchSize is the most changes one round trip carries.
const batchSize = 256

// deadLetterAfter is how many attempts a change gets once its authority is
// spent before it is failed for good. Retrying costs nothing
// for a while, in case the space is written again and leaves fresh authority
// behind; past that the change is set aside, because one that can never be
// made must not hold back the ones behind it for good.
const deadLetterAfter = 10

// defaultRetry is how a refused or unsent change is retried: with no attempt
// limit, since dropping it would leave the space's object count
// wrong, backing off to a few minutes, and holding its space so the changes
// behind it keep their order.
var defaultRetry = taskrunner.RetryPolicy{
	First:        time.Second,
	Max:          5 * time.Minute,
	Multiplier:   2,
	HoldsSubject: true,
}

// flushAction is the queue's sync no-op: running it on a space waits for the
// space's due changes to be sent, so a caller that needs them sent before it
// goes on can wait for them.
const flushAction = "flush"

// defaultDrainTimeout is how long a bucket delete waits for its space's
// changes to be sent before it fails, for the client to retry.
const defaultDrainTimeout = 30 * time.Second

// Deps is what the queue's handlers need.
type Deps struct {
	// Registrar sends the changes to the upload service. Required.
	Registrar uploader.UploadRegistrar
	// Authority finds the authority each change is sent with. Required.
	Authority *authority.Resolver
	// Logger is optional; defaults to zap.NewNop().
	Logger *zap.Logger

	// retry replaces defaultRetry, for tests.
	retry taskrunner.RetryPolicy
	// drainTimeout replaces defaultDrainTimeout, for tests.
	drainTimeout time.Duration
	// failedPage is how many failed tasks a drain lists per query, for
	// tests; zero takes batchSize.
	failedPage int
}

// Queue runs queue-space-upload.
type Queue struct {
	tasks        *taskrunner.Runner
	registrar    uploader.UploadRegistrar
	proofs       *authority.Resolver
	logger       *zap.Logger
	drainTimeout time.Duration
	failedPage   int
}

// New registers the queue-space-upload handlers with r, and keeps r for
// draining a space's changes when its bucket is deleted. Call it before r
// starts.
func New(r *taskrunner.Runner, d Deps) (*Queue, error) {
	if d.Registrar == nil || d.Authority == nil {
		return nil, errors.New("spaceuploadqueue: no registrar or authority")
	}
	q := &Queue{
		tasks:        r,
		registrar:    d.Registrar,
		proofs:       d.Authority,
		logger:       cmp.Or(d.Logger, zap.NewNop()),
		drainTimeout: cmp.Or(d.drainTimeout, defaultDrainTimeout),
		failedPage:   cmp.Or(d.failedPage, batchSize),
	}
	retry := cmp.Or(d.retry, defaultRetry)
	for _, op := range []Op{Add, Remove} {
		if err := r.HandleBatch(Name, string(op), batchSize,
			func(ctx context.Context, _ *taskrunner.Exec, tasks []taskrunner.Task) ([]taskrunner.Outcome, error) {
				return q.sendTasks(ctx, op, tasks)
			}, taskrunner.WithRetry(retry)); err != nil {
			return nil, err
		}
	}
	if err := r.Handle(Name, flushAction, func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		return nil, nil
	}); err != nil {
		return nil, err
	}
	return q, nil
}

// sendTasks sends a batch of one op's queued changes and reports each one's
// outcome. A change that goes back for another try with authority
// renewed for it keeps the renewal in its arguments, so a restart before the
// retry does not leave it with the spent chain it started with.
func (q *Queue) sendTasks(ctx context.Context, op Op, tasks []taskrunner.Task) ([]taskrunner.Outcome, error) {
	out := make([]taskrunner.Outcome, len(tasks))
	regs := make([]Registration, 0, len(tasks))
	attempts := make([]int, 0, len(tasks))
	index := make([]int, 0, len(tasks))
	for i, t := range tasks {
		reg, err := registrationFromTask(t)
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
	errs, renewed := q.sendRegistrations(ctx, op, regs, attempts)
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
func (q *Queue) sendRegistrations(ctx context.Context, op Op, regs []Registration, attempts []int) (errs []error, renewed [][]byte) {
	errs = make([]error, len(regs))
	renewed = make([][]byte, len(regs))
	batch := make([]uploader.QueuedUpload, 0, len(regs))
	sent := make([]int, 0, len(regs))
	for i, reg := range regs {
		proofs, fresh, err := q.proofs.Prepare(ctx, reg.Space, registrationCommand(op), reg.Proofs)
		if err != nil {
			errs[i] = q.spentAuthority(reg, op, attempts[i], err)
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
	send := q.registrar.RegisterUploads
	if op == Remove {
		send = q.registrar.RetractUploads
	}
	results, err := send(ctx, batch)
	if err != nil {
		q.logger.Warn("upload registrations: upload service unreachable; batch will be retried",
			zap.String("op", string(op)), zap.Int("changes", len(batch)), zap.Error(err))
		err = fmt.Errorf("spaceuploadqueue: send upload registrations: %w", err)
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
		q.logger.Warn("upload registrations: upload service refused a change; will retry",
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
func (q *Queue) spentAuthority(reg Registration, op Op, attempts int, err error) error {
	if attempts+1 < deadLetterAfter {
		q.logger.Warn("upload registrations: authority spent; will retry in case the space is written again",
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
	if op == Remove {
		effect = "object count will stay high by one, the retired version still counted"
	}
	q.logger.Error("upload registrations: failing a change whose authority cannot be renewed; "+effect,
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
func registrationCommand(op Op) ucan.Command {
	if op == Remove {
		return uploadcmds.Remove.Command
	}
	return uploadcmds.Add.Command
}

// Drain sends a space's queued object-count changes before its bucket is deleted, rather than leaving them to the workers.
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
func (q *Queue) Drain(ctx context.Context, space did.DID) error {
	if err := q.flush(ctx, space); err != nil {
		return err
	}

	// Every failed retraction is listed before any is resent: one that fails
	// again comes back as a new failed task, which a listing still paging
	// would reach and resend for as long as the service kept refusing it.
	failed, err := q.failedRetractions(ctx, space)
	if err != nil {
		return fmt.Errorf("spaceuploadqueue: list failed upload registrations: %w", err)
	}
	var resent int
	for _, f := range failed {
		reg, err := registrationFromTask(f.Task)
		if err != nil {
			continue
		}
		proofs, err := q.proofs.Capture(ctx, space, uploadcmds.Remove.Command)
		if err != nil {
			return fmt.Errorf("spaceuploadqueue: authority to resend a retraction: %w", err)
		}
		reg.Proofs = proofs
		spec, err := reg.Spec()
		if err != nil {
			return err
		}
		if _, err := q.tasks.Resubmit(ctx, Name, f.Task.ID, spec.Args); err != nil && !errors.Is(err, taskrunner.ErrNotFound) {
			return fmt.Errorf("spaceuploadqueue: resend a failed retraction: %w", err)
		}
		resent++
	}
	if resent == 0 {
		return nil
	}
	if err := q.flush(ctx, space); err != nil {
		return err
	}
	// A resent retraction the upload service refuses is retried, which holds
	// the space, so the flush above runs out of time and the delete is
	// retried. One can only be failed again here by a permanent error, which
	// every retry of the delete would meet too, so the delete goes ahead
	// rather than leave the bucket undeletable. The failed task outlives the
	// bucket, so the count it leaves behind can be reconciled from it.
	left, err := q.failedRetractions(ctx, space)
	if err != nil {
		q.logger.Warn("delete bucket: could not check for retractions that failed again", zap.Stringer("space", space), zap.Error(err))
		return nil
	}
	for _, f := range left {
		q.logger.Error("delete bucket: a retraction could not be sent; the upload service keeps this root and its count",
			zap.Stringer("space", space),
			zap.Stringer("task", f.Task.ID),
			zap.String("reason", f.Error),
		)
	}
	return nil
}

// failedRetractions lists all of the space's retractions that failed for
// good, a page at a time, passing over its failed additions.
func (q *Queue) failedRetractions(ctx context.Context, space did.DID) ([]taskrunner.FailedTask, error) {
	limit := q.failedPage
	var out []taskrunner.FailedTask
	var after taskrunner.TaskID
	for {
		page, err := q.tasks.ListFailed(ctx, Name, space.String(), after, limit)
		if err != nil {
			return nil, err
		}
		for _, f := range page {
			if f.Task.Action == string(Remove) {
				out = append(out, f)
			}
		}
		if len(page) < limit {
			return out, nil
		}
		after = page[len(page)-1].Task.ID
	}
}

// flush waits for the space's due changes to be sent, by
// running the queue's no-op sync task on it, which runs once the workers have
// run the older tasks. A change waiting out a retry holds the space, so the
// flush expires and the delete asks the client to retry.
func (q *Queue) flush(ctx context.Context, space did.DID) error {
	expires := time.Now().Add(q.drainTimeout)
	_, err := q.tasks.Run(ctx, taskrunner.Spec{Queue: Name, Subject: space.String(), Action: flushAction, Expires: &expires})
	if errors.Is(err, taskrunner.ErrExpired) {
		return fmt.Errorf("spaceuploadqueue: the upload service has not taken this space's object-count changes; retry")
	}
	if err != nil {
		return fmt.Errorf("spaceuploadqueue: drain space registrations: %w", err)
	}
	return nil
}
