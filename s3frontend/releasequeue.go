package s3frontend

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/tokenstore"
	"github.com/fil-forge/ingot/uploader"
)

// A blob nothing in its space references any more is released by a
// queue-space-shard.release task, whose subject is the space and the digest
// (registry.ReleaseSubject): the commit that drops the last claim queues it,
// due after the reader grace, in its own transaction; a session teardown, a
// superseded part or an expired pending reference queues it due at once.
// Every reference a write takes enters through a queue-space-shard.add on the
// same subject (shardref.go), so the subject's lease keeps a release and a
// write of the same blob apart: a release finds the reference, or the write's
// add runs after it and finds the blob gone.

// releaseAttemptTimeLimit bounds one release attempt's network calls.
// Package-level so tests can shrink it.
var releaseAttemptTimeLimit = 2 * time.Minute

// releaseRetry retries a release that could not finish, with no attempt
// limit: dropping it would leave a blob nothing references on the network.
// A retry holds the subject, so an add of the same blob waits behind it: a
// failed attempt may already have removed the blob from the provider, and
// the add must then find it unlocated rather than reuse it.
var releaseRetry = taskrunner.RetryPolicy{
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

// errReleaseDeferred is a release that must wait and has done nothing. It is
// retried without holding the subject (taskrunner.Defer).
var errReleaseDeferred = errors.New("s3frontend: release deferred")

// RegisterReleaseTasks registers the queue-space-shard handlers with r: the
// release, a write's add and its expiry (shardref.go), and the no-op flush.
// Call it before r starts.
func (b *Backend) RegisterReleaseTasks(r *taskrunner.Runner) error {
	b.releases = r
	if err := r.Handle(registry.ReleaseQueue, registry.ReleaseAction, b.runRelease, taskrunner.WithRetry(b.releaseRetry)); err != nil {
		return err
	}
	if err := b.registerShardRefTasks(r); err != nil {
		return err
	}
	return r.Handle(registry.ReleaseQueue, releaseFlush, func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		return nil, nil
	})
}

// releaseAuthority captures the space's authority to remove and abort blobs
// from the request, for the releases a write queues, which run after the
// request has gone. A command the request holds no chain for is left empty,
// and the release then borrows authority the space captured since.
func (b *Backend) releaseAuthority(ctx context.Context, space did.DID) registry.ReleaseTask {
	var args registry.ReleaseTask
	var err error
	if args.RemoveProofs, err = b.registrar.CaptureAuthority(ctx, space, blobcmds.Remove.Command); err != nil {
		b.logger.Debug("no /blob/remove authority to queue with a release", zap.Stringer("space", space), zap.Error(err))
	}
	if args.AbortProofs, err = b.registrar.CaptureAuthority(ctx, space, blobcmds.Abort.Command); err != nil {
		b.logger.Debug("no /blob/abort authority to queue with a release", zap.Stringer("space", space), zap.Error(err))
	}
	return args
}

// withReleaseAuthority returns ctx carrying a proof store holding the
// release's captured chains, which the network calls read before any
// authority captured since. A store ctx carries already, one a bucket delete
// lent for the space while it waits on the release (lendAuthority), is read
// first, since it is fresh, and the captured chains only for a command it
// holds none for. With none captured, ctx is returned as is.
func withReleaseAuthority(ctx context.Context, args registry.ReleaseTask) (context.Context, error) {
	store := tokenstore.NewMemStore()
	found := false
	for _, proofs := range [][]byte{args.RemoveProofs, args.AbortProofs} {
		if len(proofs) == 0 {
			continue
		}
		ct, err := container.Decode(proofs)
		if err != nil {
			return nil, fmt.Errorf("decode release authority: %w", err)
		}
		if dlgs := ct.Delegations(); len(dlgs) > 0 {
			if err := store.AddDelegations(ctx, dlgs...); err != nil {
				return nil, err
			}
			found = true
		}
	}
	if !found {
		return ctx, nil
	}
	var proofs ucanlib.ProofStore = store
	if live, ok := ctx.Value(reqscope.ProofStoreKey()).(ucanlib.ProofStore); ok && live != nil {
		proofs = proofStores{live, store}
	}
	return context.WithValue(ctx, reqscope.ProofStoreKey(), proofs), nil
}

// proofStores reads each store in turn, and returns the first chain found.
type proofStores []ucanlib.ProofStore

func (ps proofStores) ProofChain(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) ([]ucan.Delegation, []cid.Cid, error) {
	var errs []error
	for _, p := range ps {
		dlgs, links, err := p.ProofChain(ctx, aud, cmd, sub)
		if err == nil && len(dlgs) > 0 {
			return dlgs, links, nil
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return nil, nil, errors.Join(errs...)
}

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
// A conclude of the blob in flight here defers it, without holding the
// subject.
func (b *Backend) runRelease(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	space, digest, err := registry.ParseReleaseSubject(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	args, err := registry.ReleaseTaskFrom(t)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}

	if n, err := b.claims.CountReferences(ctx, space, digest, uuid.Nil); err != nil {
		return nil, fmt.Errorf("count references: %w", err)
	} else if n > 0 {
		return nil, nil // referenced again since it was queued: stale
	}
	if b.isConcluding(space, digest) {
		// Its accept may be registered after a remove sent now (see
		// markConcluding). The release has done nothing, so its retry
		// holds up none of the blob's adds meanwhile.
		return nil, taskrunner.Defer(fmt.Errorf("%w: a conclude of the blob is in flight", errReleaseDeferred))
	}

	located := false
	if loc, err := b.locations.GetLocation(ctx, space, digest); err == nil && loc != nil {
		located = true
	} else if err != nil && !errors.Is(err, registry.ErrNotFound) {
		return nil, fmt.Errorf("lookup location: %w", err)
	}
	park, err := b.parks.GetPark(ctx, space, digest)
	if err != nil && !errors.Is(err, registry.ErrNotFound) {
		return nil, fmt.Errorf("lookup park: %w", err)
	}
	others, err := b.intents.CountOtherIntents(ctx, space, digest)
	if err != nil {
		return nil, fmt.Errorf("count other intents: %w", err)
	}
	netCtx, err := withReleaseAuthority(b.withLentAuthority(ctx, space), args)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	// A stalled provider would otherwise hold a release worker for good; a
	// call cut short fails the attempt, which retries.
	netCtx, cancel := context.WithTimeout(netCtx, releaseAttemptTimeLimit)
	defer cancel()

	if err := b.encParams.DeleteEncryptionParams(ctx, space, digest); err != nil {
		return nil, fmt.Errorf("crypto-shred (delete encryption params): %w", err)
	}
	switch {
	case located:
		// Accepted: drop the space's claim on the network.
		if err := b.remover.RemoveBlob(netCtx, space, digest); err != nil {
			return nil, fmt.Errorf("network remove: %w", err)
		}
	case park != nil:
		// Parked: release the allocation on the provider. A refusal because
		// the space has accepted the blob means a conclude ran and this node
		// never learned of it (the response was lost, or Complete died
		// between the accept and recording it); the blob is removed as an
		// accepted one instead.
		cause, err := cid.Cast(park.AddTask)
		if err != nil {
			return nil, taskrunner.Permanent(fmt.Errorf("decode park add task: %w", err))
		}
		aerr := b.deferred.AbortBlob(netCtx, space, cause)
		switch {
		case aerr == nil:
		case errors.Is(aerr, uploader.ErrBlobAccepted):
			if err := b.remover.RemoveBlob(netCtx, space, digest); err != nil {
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
		if err := b.remover.RemoveBlob(netCtx, space, digest); err != nil {
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
		var evict bytes.Buffer
		if err := (&ShardEvictTask{Released: true}).MarshalCBOR(&evict); err != nil {
			return nil, err
		}
		if _, err := ex.Submit(ctx, taskrunner.Spec{
			Queue:   ShardQueue,
			Subject: hex.EncodeToString(digest),
			Action:  shardEvict,
			Args:    evict.Bytes(),
			Node:    b.shardNode(),
		}); err != nil {
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

// releaseNow waits, best effort and within releaseWaitLimit, for the queue's
// workers to run the releases of digests in space that are due now: a sync
// no-op on each blob's subject runs once the due tasks before it, the release
// among them, have run. A release not yet due, as one a commit queued after
// its reader grace, stays for its time.
func (b *Backend) releaseNow(ctx context.Context, space did.DID, digests []multihash.Multihash) {
	if len(digests) == 0 {
		return
	}
	expires := time.Now().Add(releaseWaitLimit)
	for _, d := range digests {
		if err := b.flushRelease(ctx, registry.ReleaseSubject(space, d), expires); err != nil {
			return // the rest run in the background
		}
	}
}

// flushRelease runs the queue's no-op on subject, which waits for the due
// tasks before it to run on the queue's workers, until expires.
func (b *Backend) flushRelease(ctx context.Context, subject string, expires time.Time) error {
	ctx, cancel := context.WithDeadline(ctx, expires)
	defer cancel()
	_, err := b.releases.Run(ctx, taskrunner.Spec{Queue: registry.ReleaseQueue, Subject: subject, Action: releaseFlush, Expires: &expires})
	return err
}

// drainSpaceReleases runs a space's pending releases now, whatever their
// grace: DeleteBucket calls it before asking hilt to delete the space, which
// refuses while blobs remain registered. No reader grace is owed — the bucket
// is provably empty at that point and its deletion is the operator's explicit
// intent. The reference recheck still applies, and a release that still waits,
// as one a conclude in flight defers does, fails the delete for the client to
// retry.
func (b *Backend) drainSpaceReleases(ctx context.Context, space did.DID) error {
	due, err := b.releases.Expedite(ctx, registry.ReleaseQueue, registry.SpaceReleases(space), registry.ReleaseAction)
	if err != nil {
		return fmt.Errorf("expedite the space's releases: %w", err)
	}
	if len(due) == 0 {
		return nil
	}
	expires := time.Now().Add(drainTimeoutOr(b.drainTimeout))
	subjects := make([]string, 0, len(due))
	for subject := range due {
		subjects = append(subjects, subject)
		if err := b.flushRelease(ctx, subject, expires); err != nil {
			if errors.Is(err, taskrunner.ErrExpired) || (errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil) {
				return fmt.Errorf("the space's releases did not finish in time; retry")
			}
			return fmt.Errorf("release the space's blobs: %w", err)
		}
	}
	left, err := b.releases.Pending(ctx, registry.ReleaseQueue, subjects, registry.ReleaseAction)
	if err != nil {
		return fmt.Errorf("check the space's releases: %w", err)
	}
	if len(left) > 0 {
		return fmt.Errorf("a release of the space's blobs waits, as a multipart session in flight still references it or a conclude of it is in flight; retry")
	}
	return nil
}

// drainTimeoutOr returns d, or the upload drain's timeout when d is zero.
func drainTimeoutOr(d time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return uploadDrainTimeout
}
