package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/queue/spaceuploadqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	uploadcmds "github.com/fil-forge/libforge/commands/upload"
)

// Every change to a bucket's objects runs as a queue-bucket.commit on the
// bucket's subject, one at a time: what a write does depends on the bucket's
// state (its versioning decides the new version's id and what it
// supersedes), and every write rewrites the bucket's one root. The commit
// applies its changes to one transaction in turn, appends the catalog blocks
// to the bucket's log in one fsynced batch, and writes the root's advance
// and the rows it owes with the task. A change that fails on its own terms
// (a precondition, a missing version, a version under lock) fails only
// itself; the rest still apply. The slow part of a write, streaming its body
// and taking its references, happens in the request beforehand.

// registerBucketTasks registers the queue-bucket handlers with r. Call it
// before r starts.
func (b *Backend) registerBucketTasks(r *taskrunner.Runner) error {
	b.buckets = r
	if err := r.Handle(registry.BucketQueue, registry.BucketCommitAction, b.runCommit); err != nil {
		return err
	}
	return b.registerBucketAdmin(r)
}

// commitOutcome is what a commit reports to the request that ran it.
type commitOutcome struct {
	// versioning is the bucket's versioning state under the commit, which
	// decided its versions' ids: response shaping gates on it rather than
	// on the request's earlier read.
	versioning registry.VersioningState
	results    []ChangeResult
}

// syncRunner runs a sync task: the runner, for a request, or a task's Exec,
// for a child.
type syncRunner interface {
	Run(ctx context.Context, s taskrunner.Spec) ([]byte, error)
}

// commit runs a queue-bucket.commit of changes to st's bucket and waits for
// it. The request's authority for the tasks the commit spawns goes with it:
// the releases of the claims it drops and the upload registrations it
// queues.
func (b *Backend) commit(ctx context.Context, st *registry.State, changes ...KeyChange) (commitOutcome, error) {
	return b.commitWith(ctx, b.buckets, st, changes...)
}

// commitWith is commit run through run: a Complete runs its commit as a
// child, so the commit's writes land with its own.
func (b *Backend) commitWith(ctx context.Context, run syncRunner, st *registry.State, changes ...KeyChange) (commitOutcome, error) {
	args := BucketCommitTask{Bucket: st.Name, Changes: changes, Release: b.releaseAuthority(ctx, st.Space)}
	var err error
	if args.RegisterAdd, err = b.proofs.Capture(ctx, st.Space, uploadcmds.Add.Command); err != nil {
		b.logger.Debug("commit: no /upload/add authority to queue registrations with", zap.Stringer("space", st.Space), zap.Error(err))
	}
	if args.RegisterRemove, err = b.proofs.Capture(ctx, st.Space, uploadcmds.Remove.Command); err != nil {
		b.logger.Debug("commit: no /upload/remove authority to queue retractions with", zap.Stringer("space", st.Space), zap.Error(err))
	}
	var buf bytes.Buffer
	if err := args.MarshalCBOR(&buf); err != nil {
		return commitOutcome{}, fmt.Errorf("s3frontend: encode commit: %w", err)
	}
	out, err := run.Run(ctx, b.bucketSpec(st.Space, registry.BucketCommitAction, buf.Bytes()))
	if err != nil {
		return commitOutcome{}, mapCommitError(err, "commit")
	}
	var res BucketCommitResult
	if err := res.UnmarshalCBOR(bytes.NewReader(out)); err != nil {
		return commitOutcome{}, fmt.Errorf("s3frontend: decode commit result: %w", err)
	}
	return commitOutcome{versioning: registry.VersioningState(res.Versioning), results: res.Results}, nil
}

// commitOne is commit of one change, returning its result or the error it
// failed with.
func (b *Backend) commitOne(ctx context.Context, st *registry.State, change KeyChange, op string) (ChangeResult, registry.VersioningState, error) {
	return b.commitOneWith(ctx, b.buckets, st, change, op)
}

// commitOneWith is commitOne run through run (commitWith).
func (b *Backend) commitOneWith(ctx context.Context, run syncRunner, st *registry.State, change KeyChange, op string) (ChangeResult, registry.VersioningState, error) {
	out, err := b.commitWith(ctx, run, st, change)
	if err != nil {
		return ChangeResult{}, "", err
	}
	res := out.results[0]
	if res.Err != nil {
		return res, out.versioning, mapCommitError(res.Err.err(), op)
	}
	return res, out.versioning, nil
}

// bucketSpec is a queue-bucket task on space's bucket, placed on this
// instance, which holds the bucket's catalog log.
func (b *Backend) bucketSpec(space did.DID, action string, args []byte) taskrunner.Spec {
	spec := taskrunner.Spec{
		Queue:   registry.BucketQueue,
		Subject: registry.BucketSubject(space),
		Action:  action,
		Args:    args,
	}
	if node := b.buckets.Node(); node != "" {
		spec.Node = &node
	}
	return spec
}

// commitScope is one commit's state while its changes apply.
type commitScope struct {
	tx   *bucketop.Tx
	args *BucketCommitTask
}

// runCommit is queue-bucket.commit.
func (b *Backend) runCommit(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) (_ []byte, err error) {
	var args BucketCommitTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode commit: %w", err))
	}
	ctx, span := tracing.Start(ctx, "bucket.commit")
	defer func() { tracing.End(span, err) }()

	tx, err := b.txns.Begin(ctx, args.Bucket)
	if err != nil {
		if errors.Is(err, bucketop.ErrBucketNotFound) {
			return nil, taskrunner.Permanent(err)
		}
		return nil, err
	}
	defer tx.Discard()
	// A bucket recreated under the name lives in another space, and is not
	// the bucket the commit was queued for (bucketOn).
	if tx.State().Space.String() != t.Subject {
		return nil, taskrunner.Permanent(bucketop.ErrBucketNotFound)
	}
	if tx.State().Closing {
		return nil, errBucketClosing
	}
	sc := &commitScope{tx: tx, args: &args}
	res := BucketCommitResult{Versioning: string(tx.State().Versioning), Results: make([]ChangeResult, len(args.Changes))}
	for i, ch := range args.Changes {
		mark := tx.Mark()
		r, root, err := b.applyChange(ctx, sc, ch)
		if err != nil {
			ce, ok := changeError(err)
			if !ok {
				return nil, err
			}
			tx.Restore(mark)
			res.Results[i] = ChangeResult{Err: ce}
			continue
		}
		if root.Defined() {
			tx.Advance(root)
		}
		res.Results[i] = r
	}

	appended, ok, err := tx.Append(ctx)
	if err != nil {
		return nil, err
	}
	if ok {
		w, err := b.reg.RootCommitWrites(ctx, args.Bucket, appended.Expect, appended.Next, appended.Commit, appended.GC)
		if err != nil {
			return nil, err
		}
		if err := applyCommitWrites(ctx, ex, w); err != nil {
			return nil, err
		}
		// The append captured fresh ship authority: a ship that found none
		// is queued again with this commit.
		if b.awaitingShip(args.Bucket) {
			raw, err := encodeBucketTask(&BucketTask{Name: args.Bucket})
			if err != nil {
				return nil, err
			}
			spec := b.bucketSpec(tx.State().Space, registry.BucketShipAction, raw)
			now := registry.DueNow()
			spec.NotBefore, spec.Replace = &now, true
			if _, err := ex.Submit(ctx, spec); err != nil {
				return nil, err
			}
		}
	}
	var buf bytes.Buffer
	if err := res.MarshalCBOR(&buf); err != nil {
		return nil, fmt.Errorf("encode commit result: %w", err)
	}
	return buf.Bytes(), nil
}

// applyChange applies one change to the commit's transaction and returns its
// result and the root it leaves, or cid.Undef when it changed nothing.
func (b *Backend) applyChange(ctx context.Context, sc *commitScope, ch KeyChange) (ChangeResult, cid.Cid, error) {
	switch {
	case ch.Put != nil:
		return b.applyPut(ctx, sc, ch.Key, ch.Put)
	case ch.Delete != nil:
		return b.applyDelete(ctx, sc, ch.Key, ch.Delete)
	case ch.SetAttributes != nil:
		root, err := b.applySetAttributes(ctx, sc, ch.Key, ch.SetAttributes)
		return ChangeResult{}, root, err
	}
	return ChangeResult{}, cid.Undef, taskrunner.Permanent(fmt.Errorf("change to %q sets nothing", ch.Key))
}

// applyDelete removes the version d names, or the key's current one as the
// bucket's versioning state says: a delete marker when versioning is
// configured, the key when it is not.
func (b *Backend) applyDelete(ctx context.Context, sc *commitScope, key string, d *DeleteChange) (ChangeResult, cid.Cid, error) {
	if d.HasETag {
		etag, exists, err := b.currentETagIn(ctx, sc.tx, key)
		if err != nil {
			return ChangeResult{}, cid.Undef, err
		}
		if !exists || !etagsEqual(d.ETag, etag) {
			return ChangeResult{}, cid.Undef, s3err.GetAPIError(s3err.ErrPreconditionFailed)
		}
	}
	if d.VersionID != "" {
		return b.applyDeleteVersion(ctx, sc, key, d.VersionID, d.Pre)
	}
	if !sc.tx.State().Versioning.Configured() {
		root, err := b.applyDeleteKey(ctx, sc, key, d.Pre)
		return ChangeResult{}, root, err
	}
	marker := &PutChange{
		Manifest:  deleteMarkerManifest(key),
		DeletePre: d.Pre,
	}
	r, root, err := b.applyPut(ctx, sc, key, marker)
	r.DeleteMarker = true
	return r, root, err
}

// register queues one change to the space's content-entry list with the
// commit, under the request's captured authority. A request with none
// cannot produce authority for the registration to run with later, and
// failing the write over the object count would fail a write that otherwise
// succeeded: the registration is skipped and logged, and the count
// under-reports until something registers that root again.
func (b *Backend) register(sc *commitScope, st *registry.State, key string, root cid.Cid, op spaceuploadqueue.Op) {
	if !root.Defined() {
		return
	}
	proofs := sc.args.RegisterAdd
	if op == spaceuploadqueue.Remove {
		proofs = sc.args.RegisterRemove
	}
	if len(proofs) == 0 {
		b.logger.Warn("commit: no authority to queue an upload registration; object count will drift",
			zap.String("bucket", st.Name),
			zap.String("op", string(op)),
			zap.Stringer("space", st.Space),
			zap.Stringer("root", root),
		)
		return
	}
	spec, err := spaceuploadqueue.Registration{
		Bucket:    st.Name,
		ObjectKey: key,
		Space:     st.Space,
		Root:      root,
		Op:        op,
		Proofs:    proofs,
	}.Spec()
	if err != nil {
		b.logger.Warn("commit: could not encode an upload registration; object count will drift",
			zap.String("bucket", st.Name), zap.Stringer("root", root), zap.Error(err))
		return
	}
	sc.tx.EnqueueUploadRegistration(spec)
}

// putPreCheck returns the check a put runs against the version it would
// supersede (nil when the key is new): a delete marker's delete
// preconditions, a Complete's If-Match and If-None-Match, or a PutObject's.
func (b *Backend) putPreCheck(_ context.Context, p *PutChange) (func(superseded *msbucket.ObjectManifest) error, error) {
	return func(superseded *msbucket.ObjectManifest) error {
		// A delete-marker current means "no object" for precondition
		// purposes.
		oldETag, oldExists := "", false
		if superseded != nil && !superseded.DeleteMarker {
			oldETag, oldExists = etagOf(superseded), true
		}
		switch {
		case p.DeletePre != nil:
			if !oldExists {
				return nil
			}
			return backend.EvaluateObjectDeletePreconditions(oldETag, time.Unix(superseded.Created, 0), superseded.Body.Size, p.DeletePre.preconditions())
		case p.Completes:
			if p.HasIfMatch {
				if !oldExists {
					return s3err.GetAPIError(s3err.ErrNoSuchKey)
				}
				if !etagsEqual(p.IfMatch, oldETag) {
					return s3err.GetAPIError(s3err.ErrPreconditionFailed)
				}
			}
			if p.HasIfNoneMatch && oldExists {
				return s3err.GetAPIError(s3err.ErrPreconditionFailed)
			}
			return nil
		}
		var ifMatch, ifNoneMatch *string
		if p.HasIfMatch {
			ifMatch = &p.IfMatch
		}
		if p.HasIfNoneMatch {
			ifNoneMatch = &p.IfNoneMatch
		}
		return backend.EvaluateObjectPutPreconditions(oldETag, ifMatch, ifNoneMatch, oldExists)
	}, nil
}

// deleteMarkerManifest is the manifest of a delete marker for key: no body,
// and no claims.
func deleteMarkerManifest(key string) *msbucket.ObjectManifest {
	return &msbucket.ObjectManifest{Key: key, Created: time.Now().Unix(), DeleteMarker: true}
}

// currentETagIn returns the ETag of key's current version as the commit's
// transaction sees it, and whether there is one: a delete marker current is
// none.
func (b *Backend) currentETagIn(ctx context.Context, tx *bucketop.Tx, key string) (string, bool, error) {
	st := tx.State()
	if !st.Root.Defined() {
		return "", false, nil
	}
	valCid, err := tx.LoadTree().Get(ctx, key)
	if errors.Is(err, mst.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("mst get: %w", err)
	}
	var val msbucket.ObjectValue
	if err := tx.Get(ctx, st.Space, valCid, &val); err != nil {
		return "", false, fmt.Errorf("load value: %w", err)
	}
	mf := val.Manifest
	if val.Leaf != nil {
		var em msbucket.EnvelopedManifest
		if err := tx.Get(ctx, st.Space, val.Leaf.Current.Manifest, &em); err != nil {
			return "", false, fmt.Errorf("load manifest: %w", err)
		}
		mf = em.Manifest
	}
	if mf.DeleteMarker {
		return "", false, nil
	}
	return etagOf(mf), true, nil
}
