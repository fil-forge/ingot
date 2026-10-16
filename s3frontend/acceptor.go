package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	mh "github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/forgeclient"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/registry"
)

// A multipart upload's blobs are accepted on Forge while the client is still
// uploading, so Complete has only the last few parts left to wait for. The
// part rows are the queue: UploadPart moves a part to 'parked' once its blobs
// are parked and nudges the upload's acceptor, which concludes the upload's
// parked parts in batches and moves them to 'accepted'.
//
// Each upload has at most one acceptor, a goroutine that runs while there is
// work and exits when a pass finds none, so a large upload never delays a
// small one's accepts. Ownership is in-process: Ingot runs as one instance.
// Running several would need a lease on the session row, so that one
// instance's acceptor owns an upload at a time.

// acceptBatchBlobs caps the blobs one acceptor pass concludes: one
// /ucan/conclude's worth. A part is never split across passes, so a pass
// takes at least one part however many blobs it has.
var acceptBatchBlobs = forgeclient.MaxConcludeReceipts

// acceptPassTimeout bounds one acceptor pass. The Forge client sets no
// timeout of its own, so without it a conclude whose answer never came would
// hold the acceptor in one pass for good. A pass that runs out fails like any
// other and is retried; accepts the upload service ran before the timeout are
// concluded again on the retry, which it treats as a repeat. A full pass
// takes well under a minute. Package-level so tests can shrink it.
var acceptPassTimeout = 2 * time.Minute

// acceptBackoffFirst and acceptBackoffMax bound the wait between an
// acceptor's failed pass and its retry. Package-level so tests can shrink
// them.
var (
	acceptBackoffFirst = time.Second
	acceptBackoffMax   = 30 * time.Second
)

// acceptors is the Backend's set of running acceptors, by upload ID.
type acceptors struct {
	mu sync.Mutex
	// ctx is the acceptors' lifetime, set by StartAcceptors: nil before it,
	// and once it is done no acceptor starts.
	ctx context.Context
	// closed is set by WaitAcceptors, so no acceptor starts behind its wait.
	closed  bool
	wg      sync.WaitGroup
	running map[string]*acceptor
}

// acceptor is one upload's running acceptor. Its fields are guarded by
// acceptors.mu.
type acceptor struct {
	// more is set by a nudge, or by a pass that did work, so the loop makes
	// another pass before it exits.
	more bool
	// lastErr is the last pass's error, nil once a pass succeeds.
	lastErr error
	// waiters are signalled with the outcome of the next pass that fails,
	// or with nil when the acceptor exits with nothing left to do.
	waiters []chan error
}

// signal hands err to every waiter and forgets them.
func (a *acceptor) signal(err error) {
	for _, w := range a.waiters {
		w <- err
	}
	a.waiters = nil
}

// StartAcceptors lets acceptors run until ctx is done. Before it is called a
// nudge does nothing; the restart sweep (NudgeParkedUploads) picks up the
// parts that queued meanwhile.
func (b *Backend) StartAcceptors(ctx context.Context) {
	b.acceptors.mu.Lock()
	defer b.acceptors.mu.Unlock()
	b.acceptors.ctx = ctx
}

// WaitAcceptors stops new acceptors from starting and waits for the running
// ones to exit. Call it once the context given to StartAcceptors is done.
func (b *Backend) WaitAcceptors() {
	b.acceptors.mu.Lock()
	b.acceptors.closed = true
	b.acceptors.mu.Unlock()
	b.acceptors.wg.Wait()
}

// NudgeParkedUploads nudges the acceptor of every open or completing upload
// with parked parts, and returns how many it nudged. It is the restart sweep:
// the part rows are the queue, so nothing is lost when a process stops with
// work outstanding or a nudge goes missing.
func (b *Backend) NudgeParkedUploads(ctx context.Context) (int, error) {
	ids, err := b.multipart.ListUploadsWithParkedParts(ctx)
	if err != nil {
		return 0, fmt.Errorf("s3frontend: list uploads with parked parts: %w", err)
	}
	for _, id := range ids {
		b.nudgeAcceptor(id)
	}
	return len(ids), nil
}

// nudgeAcceptor starts uploadID's acceptor, or asks the running one for
// another pass.
func (b *Backend) nudgeAcceptor(uploadID string) {
	b.acceptors.mu.Lock()
	defer b.acceptors.mu.Unlock()
	b.nudgeLocked(uploadID)
}

// nudgeLocked is nudgeAcceptor with acceptors.mu held. It returns the
// upload's acceptor, nil when acceptors are not running.
func (b *Backend) nudgeLocked(uploadID string) *acceptor {
	m := &b.acceptors
	if m.ctx == nil || m.ctx.Err() != nil || m.closed {
		return nil
	}
	if a, ok := m.running[uploadID]; ok {
		a.more = true
		return a
	}
	if m.running == nil {
		m.running = map[string]*acceptor{}
	}
	a := &acceptor{more: true}
	m.running[uploadID] = a
	m.wg.Add(1)
	go func(ctx context.Context) {
		defer m.wg.Done()
		b.runAcceptor(ctx, uploadID, a)
	}(m.ctx)
	return a
}

// errAcceptorsNotRunning is drainAcceptor's answer before StartAcceptors, or
// once the acceptors have stopped.
var errAcceptorsNotRunning = errors.New("acceptors are not running")

// drainAcceptor nudges uploadID's acceptor and waits until it has nothing
// left to do, its next pass fails, or ctx ends. An acceptor backing off after
// a failure returns that failure at once rather than waiting out the backoff.
// A nil result says only that the acceptor stopped; the part states are the
// answer to what was accepted.
func (b *Backend) drainAcceptor(ctx context.Context, uploadID string) error {
	b.acceptors.mu.Lock()
	a := b.nudgeLocked(uploadID)
	if a == nil {
		b.acceptors.mu.Unlock()
		return errAcceptorsNotRunning
	}
	if a.lastErr != nil {
		err := a.lastErr
		b.acceptors.mu.Unlock()
		return err
	}
	done := make(chan error, 1)
	a.waiters = append(a.waiters, done)
	b.acceptors.mu.Unlock()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// The acceptor signals into a buffered channel, so leaving the
		// waiter behind costs nothing.
		return ctx.Err()
	}
}

// runAcceptor is uploadID's acceptor: it makes passes while there is work,
// backs off after a failed pass, and exits once a pass finds nothing to do or
// ctx is done.
func (b *Backend) runAcceptor(ctx context.Context, uploadID string, a *acceptor) {
	m := &b.acceptors
	backoff := acceptBackoffFirst
	for {
		m.mu.Lock()
		if !a.more || ctx.Err() != nil {
			delete(m.running, uploadID)
			a.signal(nil)
			m.mu.Unlock()
			return
		}
		a.more = false
		m.mu.Unlock()

		passCtx, cancel := context.WithTimeout(ctx, acceptPassTimeout)
		did, err := b.acceptPass(passCtx, uploadID)
		cancel()

		m.mu.Lock()
		a.lastErr = err
		if err != nil {
			// Retried after the backoff; the parts stay parked meanwhile.
			a.more = true
			a.signal(err)
		} else if did {
			a.more = true
		}
		m.mu.Unlock()

		if err == nil {
			backoff = acceptBackoffFirst
			continue
		}
		if ctx.Err() == nil {
			b.logger.Warn("multipart accept pass failed; retrying",
				zap.String("upload_id", uploadID), zap.Duration("backoff", backoff), zap.Error(err))
		}
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, acceptBackoffMax)
	}
}

// acceptPass accepts one batch of uploadID's parked parts and reports whether
// it found any. A session that is gone, or no longer open or completing, has
// nothing to accept: an aborting session releases its parts instead, and a
// completed one accepted them all.
func (b *Backend) acceptPass(ctx context.Context, uploadID string) (did bool, err error) {
	sess, err := b.multipart.GetSession(ctx, uploadID)
	if errors.Is(err, registry.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get session: %w", err)
	}
	if sess.State != registry.SessionOpen && sess.State != registry.SessionCompleting {
		return false, nil
	}
	queued, err := b.multipart.ListParkedParts(ctx, uploadID, acceptBatchBlobs)
	if err != nil {
		return false, fmt.Errorf("list parked parts: %w", err)
	}
	if len(queued) == 0 {
		return false, nil
	}

	ctx, span := tracing.Start(ctx, "multipart.accept_pass")
	defer func() { tracing.End(span, err) }()

	var batch []registry.MultipartPart
	var digests []mh.Multihash
	for _, p := range queued {
		if len(batch) > 0 && len(digests)+len(p.BlobDigests) > acceptBatchBlobs {
			break
		}
		batch = append(batch, p)
		digests = append(digests, p.BlobDigests...)
	}
	span.SetAttributes(attribute.Int("ingot.parts", len(batch)), attribute.Int("ingot.blobs.total", len(digests)))

	left, err := b.acceptParked(ctx, sess.Space, digests, acceptInBackground)
	if err != nil {
		return false, err
	}
	// A part holding a blob the acceptor left is not its to finish, and goes
	// back to 'spooled' for Complete: a blob with neither a park nor a
	// location is uploaded from its spool copy, and one with a release
	// record is settled by Complete's commit, which claims it. The move is
	// guarded by the part's digests, so a part a later upload replaced, or a
	// session's teardown deleted, is left as it is and its release runs.
	missing := digestSet(left)
	var accepted, respooled []registry.MultipartPart
	for _, p := range batch {
		whole := true
		for _, d := range p.BlobDigests {
			if _, ok := missing[string(d)]; ok {
				whole = false
				break
			}
		}
		if whole {
			accepted = append(accepted, p)
		} else {
			respooled = append(respooled, p)
		}
	}
	if len(respooled) > 0 {
		b.logger.Info("multipart accept: parts left for Complete (a blob unparked or being released)",
			zap.String("upload_id", uploadID), zap.Int("parts", len(respooled)))
		if _, err := b.multipart.MoveParts(ctx, respooled, registry.PartParked, registry.PartSpooled); err != nil {
			return false, fmt.Errorf("return parts to spooled: %w", err)
		}
	}
	if _, err := b.multipart.MoveParts(ctx, accepted, registry.PartParked, registry.PartAccepted); err != nil {
		return false, fmt.Errorf("mark parts accepted: %w", err)
	}
	return true, nil
}
