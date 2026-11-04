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
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/tokenstore"
	"github.com/fil-forge/ingot/uploader"
)

// A blob nothing in its space claims any more is released by a
// queue-space-shard.release task, whose subject is the space and the digest
// (registry.ReleaseSubject): the commit that drops the last claim queues it,
// due after the reader grace, in its own transaction, and a session teardown
// or a superseded part queues it due at once. The task's lease is what keeps a
// release and a write of the same blob apart. A write about to reuse the blob
// cancels a pending release (settleRelease) or waits out a running one; a
// commit claiming it cancels a pending one in its transaction and refuses
// while one runs (registry.ErrReleaseInProgress).

// releaseRetry retries a release that could not finish, with no attempt
// limit: dropping it would leave a blob nothing references on the network.
var releaseRetry = taskrunner.RetryPolicy{
	First:      time.Second,
	Max:        5 * time.Minute,
	Multiplier: 2,
}

// releaseFlush is the queue's sync no-op: running it on a blob's subject waits
// for a running release to finish, and leaves a pending one standing.
const releaseFlush = "flush"

// releasedResult is a release's result once it has removed the blob; a
// release that found it claimed again returns none.
var releasedResult = []byte("released")

// errReleaseDeferred is a release that must wait: something in the space still
// means to use the blob. It is retried.
var errReleaseDeferred = errors.New("s3frontend: release deferred")

// RegisterReleaseTasks registers the queue-space-shard handlers with r. Call
// it before r starts.
func (b *Backend) RegisterReleaseTasks(r *taskrunner.Runner) error {
	b.releases = r
	if err := r.Handle(registry.ReleaseQueue, registry.ReleaseAction, b.runRelease, taskrunner.WithRetry(b.releaseRetry)); err != nil {
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
// authority captured since. With none captured, ctx is returned as is.
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
	return context.WithValue(ctx, reqscope.ProofStoreKey(), ucanlib.ProofStore(store)), nil
}

// runRelease is queue-space-shard.release: it unwinds one blob for one space,
// whatever state the blob is in, unless something in the space still uses it.
//
// Every check and read comes before the network step, and after it nothing
// can fail but the commit, which the runner retries: the blob's rows go
// through the task's writes. A release that fails before then has changed
// nothing outside Postgres but the crypto-shred, and a commit may cancel its
// retry and claim the blob, which is still on the network. The crypto-shred
// goes first, so a blob nothing references becomes unreadable at once; only a
// plaintext blob can be claimed again, and it has no keys.
//
// The checks run in this order on purpose. A live multipart part naming the
// blob defers it: its session means to claim it. A Complete adds its claims
// while its session is still in flight and only then marks it completed, so
// reading the parts before the claims means a release finds one or the other.
// A claim means the release is stale and it does nothing. A write of the same
// bytes in this space holding the spool file, or a conclude of the blob in
// flight here, defers it too.
func (b *Backend) runRelease(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	space, digest, err := registry.ParseReleaseSubject(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	args, err := registry.ReleaseTaskFrom(t)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	log := b.logger.With(zap.Stringer("space", space), zap.String("digest", hex.EncodeToString(digest)))

	if n, err := b.multipart.CountLivePartRefs(ctx, space, digest); err != nil {
		return nil, fmt.Errorf("count live part references: %w", err)
	} else if n > 0 {
		return nil, fmt.Errorf("%w: a multipart part of the space names the blob", errReleaseDeferred)
	}
	if n, err := b.claims.CountClaims(ctx, space, digest); err != nil {
		return nil, fmt.Errorf("count claims: %w", err)
	} else if n > 0 {
		return nil, nil // claimed again since it was queued: stale
	}
	if b.spool.HeldBy(space, digest) {
		return nil, fmt.Errorf("%w: a write of the same bytes holds the spool file", errReleaseDeferred)
	}
	if b.isConcluding(space, digest) {
		// Its accept may be registered after a remove sent now (see
		// markConcluding).
		return nil, fmt.Errorf("%w: a conclude of the blob is in flight", errReleaseDeferred)
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
	in, err := b.intents.GetIntent(ctx, space, digest)
	if err != nil && !errors.Is(err, registry.ErrNotFound) {
		return nil, fmt.Errorf("lookup upload intent: %w", err)
	}
	if err != nil {
		in = nil
	}
	others, err := b.intents.CountOtherIntents(ctx, space, digest)
	if err != nil {
		return nil, fmt.Errorf("count other intents: %w", err)
	}
	netCtx, err := withReleaseAuthority(ctx, args)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}

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
	case in != nil && in.State == registry.IntentSpooled:
		// No row says the blob is on the network, and the intent says it
		// never went: every upload moves the intent off 'spooled' before its
		// first network call. Nothing is owed there, which matters for the
		// retry: a blob that was never uploaded captured no authority a
		// background remove could use.
	default:
		// No row says the blob is on the network, but the intent says it may
		// be: a never-parked blob can be uploaded and accepted at Complete and
		// then fail to record its location, leaving neither row. The upload
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
	// The local copy is shared with every other space whose intent names the
	// digest (the same bytes stored as received in two buckets), so while
	// another space holds an intent it stays, for that space's upload or
	// reads. A write of the same bytes in this space that spooled them again
	// since owns the intent and the file. A copy both spaces' releases leave
	// at once is reclaimed by the cache's time to live or the orphan pass.
	if b.spool.HeldBy(space, digest) {
		return releasedResult, nil
	}
	if others == 0 && !b.spool.Held(digest) {
		freed, err := b.spool.Remove(digest)
		switch {
		case errors.Is(err, blockstore.ErrHeld) && b.spool.HeldBy(space, digest):
			return releasedResult, nil // a write of this space took the file since the check above
		case errors.Is(err, blockstore.ErrHeld):
			// Another space's write took the file: it stays.
		case err != nil:
			log.Warn("release: remove the spool copy failed; the orphan pass takes it", zap.Error(err))
		default:
			b.localBlobMetrics.removedFile(ctx, removedReleased, freed)
		}
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

// settleRelease settles, before a write uploads, parks, concludes or reuses a
// blob, whether a release of the same digest in the space stands in the way,
// and reports whether the blob may be reused as located. A blob stored as
// received has the digest of its bytes, so a write of the same bytes can follow
// the deletion of the object that stored them, with that object's release
// pending or running. A pending release is cancelled, and the blob is the
// write's: the claim its commit adds would have made the release stale anyway.
// A running one means the blob is going; the write waits for it to finish,
// then uploads afresh (false). No release at all leaves a located blob reusable
// (true). A release queued after this and before the commit is cancelled by
// the claim itself, or refuses it while it runs (registry.ErrReleaseInProgress).
// The wait is bounded by reuseWaitLimit and then refused with S3's SlowDown,
// which the client retries. An encrypted blob has a digest of its own and is
// never released under a write.
func (b *Backend) settleRelease(ctx context.Context, space did.DID, digest multihash.Multihash) (bool, error) {
	subject := []string{registry.ReleaseSubject(space, digest)}
	return b.settle(ctx, func() (releaseStanding, error) {
		cancelled, running, err := b.releases.CancelPending(ctx, registry.ReleaseQueue, subject, registry.ReleaseAction)
		switch {
		case err != nil:
			return 0, fmt.Errorf("cancel release: %w", err)
		case len(running) > 0:
			return releaseStarted, nil
		case cancelled > 0:
			return releaseTaken, nil
		}
		return releaseAbsent, nil
	})
}

// awaitRelease is settleRelease for a part blob, whose part row already names
// the digest. A pending release is left standing rather than cancelled: the
// row defers it for as long as the row stands (runRelease reads the live part
// references first), and it runs once a later upload of the part number
// replaces the row, which is exactly what it is for. Cancelled, the obligation
// would rest on this write alone, which can die before it queues the release
// again. A running release is waited out as settleRelease waits.
func (b *Backend) awaitRelease(ctx context.Context, space did.DID, digest multihash.Multihash) (bool, error) {
	subject := registry.ReleaseSubject(space, digest)
	pending, err := b.releases.Pending(ctx, registry.ReleaseQueue, []string{subject}, registry.ReleaseAction)
	if err != nil {
		return false, fmt.Errorf("lookup release: %w", err)
	}
	if len(pending[subject]) == 0 {
		return true, nil
	}
	// Running the queue's no-op on the subject waits for the lease, so for a
	// running release to finish; a pending one, not yet due, stays.
	expires := time.Now().Add(reuseWaitLimit)
	if _, err := b.releases.Run(ctx, taskrunner.Spec{Queue: registry.ReleaseQueue, Subject: subject, Action: releaseFlush, Expires: &expires}); err != nil {
		if errors.Is(err, taskrunner.ErrExpired) {
			return false, s3err.GetAPIError(s3err.ErrSlowDown)
		}
		return false, fmt.Errorf("wait for release: %w", err)
	}
	// Still located means the release that stood was pending and stays: the
	// blob is reusable. Gone means a running one took it.
	loc, err := b.locations.GetLocation(ctx, space, digest)
	if err != nil && !errors.Is(err, registry.ErrNotFound) {
		return false, fmt.Errorf("lookup location: %w", err)
	}
	return err == nil && loc != nil, nil
}

// releaseStanding is what a write's probe of a release found.
type releaseStanding int

const (
	// releaseAbsent: no release, or the one waited out has run.
	releaseAbsent releaseStanding = iota
	// releaseTaken: a pending release the write cancelled; the blob is the
	// write's.
	releaseTaken
	// releaseStarted: the blob is going; the write waits.
	releaseStarted
)

// settle is settleRelease's wait: probe reports the release as the write
// settles it, and a running one is polled until it has finished, up to
// reuseWaitLimit.
func (b *Backend) settle(ctx context.Context, probe func() (releaseStanding, error)) (bool, error) {
	deadline := time.Now().Add(reuseWaitLimit)
	backoff := reuseWaitFirst
	waited := false
	for {
		standing, err := probe()
		if err != nil {
			return false, err
		}
		switch standing {
		case releaseAbsent:
			// No release, or the one waited out has run and the location
			// went with it.
			return !waited, nil
		case releaseTaken:
			return true, nil
		}
		waited = true
		if time.Now().After(deadline) {
			return false, s3err.GetAPIError(s3err.ErrSlowDown)
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < reuseWaitMax {
			backoff *= 2
		}
	}
}

// settleRelease's wait on a running release: the first poll interval, its
// cap, and how long in all before the write is refused. Package-level so
// tests can shrink them.
var (
	reuseWaitFirst = 20 * time.Millisecond
	reuseWaitMax   = 500 * time.Millisecond
	reuseWaitLimit = 30 * time.Second
)

// releaseWaitLimit bounds how long an S3 verb spends running the releases it
// queued due at once, for the promptness it owes its caller: an Abort releases
// parked blobs on their providers before it returns. A release left when it
// passes runs in the background.
var releaseWaitLimit = 10 * time.Second

// releaseNow runs the releases of digests in space now, in this goroutine,
// best effort, within releaseWaitLimit. A sync no-op on each blob's subject
// runs the due tasks before it, the release among them, whether or not a
// worker is free. A release not yet due, as one a commit queued after its
// reader grace, stays for its time.
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

// flushRelease runs the queue's no-op on subject, which first runs the due
// tasks before it, until expires.
func (b *Backend) flushRelease(ctx context.Context, subject string, expires time.Time) error {
	_, err := b.releases.Run(ctx, taskrunner.Spec{Queue: registry.ReleaseQueue, Subject: subject, Action: releaseFlush, Expires: &expires})
	return err
}

// drainSpaceReleases runs a space's pending releases now, whatever their
// grace: DeleteBucket calls it before asking hilt to delete the space, which
// refuses while blobs remain registered. No reader grace is owed — the bucket
// is provably empty at that point and its deletion is the operator's explicit
// intent. The claim recheck still applies, and a release that still waits, as
// one a live multipart part defers does, fails the delete for the client to
// retry: the bucket is not empty after all.
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
			if errors.Is(err, taskrunner.ErrExpired) {
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
