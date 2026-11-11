// Package bucketqueue runs the changes to a bucket's objects, as
// queue-bucket.commit tasks on the bucket's subject, one at a time.
//
// What a write does depends on the bucket's state (its versioning decides
// the new version's id and what it supersedes), and every write rewrites
// the bucket's one root. A commit applies its changes to one transaction in
// turn, appends the catalog blocks to the bucket's log in one fsynced batch,
// and writes the root's advance and the rows it owes with the task. A change
// that fails on its own terms (a precondition, a missing version) fails only
// itself, with a *ChangeError in its result; the rest still apply. The slow
// part of a write, streaming its body and taking its references, happens in
// the request beforehand.
package bucketqueue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/internal/authority"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/queue/spaceuploadqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/uploader"
)

// Deps are what a Queue's tasks need.
type Deps struct {
	// Registry holds the buckets' state, their version seqs and the
	// writes' pending references.
	Registry registry.Registry
	// Txns opens a commit's transaction on a bucket's catalog.
	Txns *bucketop.Coordinator
	// Read reads the buckets' catalogs, for a close's and a delete's check
	// that the bucket is empty.
	Read blockstore.ReadStore
	// Log is the catalog log, which ships each bucket's sealed segments;
	// a delete releases them and removes the bucket's log.
	Log blockstore.Log
	// Sessions lists a bucket's multipart uploads, which a delete refuses.
	Sessions SessionLister
	// Remover releases a deleted bucket's catalog segments from its space.
	Remover uploader.BlobRemover
	// Buckets deletes a bucket's space.
	Buckets bucketauthority.BucketAuthority
	// Authority lends a delete's request authority to the tasks it drains.
	Authority *authority.Resolver
	// Blobs drains a deleted bucket's space of its blobs' releases.
	Blobs *spaceshardqueue.Queue
	// Counts drains a deleted bucket's space of its object-count changes.
	Counts *spaceuploadqueue.Queue
	// ReleaseGrace is how long after a commit a blob it drops the last
	// claim on is released.
	ReleaseGrace time.Duration
	// ShipRetryFirst is the first wait before a ship that could not be
	// queued is queued again; zero for a second.
	ShipRetryFirst time.Duration
	// ShipRetryMax bounds the doubling waits after it; zero for a minute.
	ShipRetryMax time.Duration
	// Logger defaults to a no-op logger.
	Logger *zap.Logger
}

// SessionLister lists a bucket's multipart uploads.
type SessionLister interface {
	ListSessions(ctx context.Context, bucket string, space did.DID) ([]registry.MultipartSession, error)
}

// Queue is the queue-bucket queue: every change to a bucket, its objects,
// its settings and its life, one at a time on the bucket's subject.
type Queue struct {
	runner       *taskrunner.Runner
	reg          registry.Registry
	txns         *bucketop.Coordinator
	read         blockstore.ReadStore
	log          blockstore.Log
	sessions     SessionLister
	remover      uploader.BlobRemover
	buckets      bucketauthority.BucketAuthority
	proofs       *authority.Resolver
	blobs        *spaceshardqueue.Queue
	counts       *spaceuploadqueue.Queue
	releaseGrace time.Duration
	logger       *zap.Logger
	ships        ships
}

// New returns the queue and registers the queue-bucket handlers with r.
// Call it before r starts.
func New(r *taskrunner.Runner, d Deps) (*Queue, error) {
	if d.Txns == nil {
		return nil, errors.New("bucketqueue: no catalog transactions")
	}
	q := &Queue{
		runner:       r,
		reg:          d.Registry,
		txns:         d.Txns,
		read:         d.Read,
		log:          d.Log,
		sessions:     d.Sessions,
		remover:      d.Remover,
		buckets:      d.Buckets,
		proofs:       d.Authority,
		blobs:        d.Blobs,
		counts:       d.Counts,
		releaseGrace: d.ReleaseGrace,
		logger:       d.Logger,
		ships:        ships{retryFirst: d.ShipRetryFirst, retryMax: d.ShipRetryMax},
	}
	if q.logger == nil {
		q.logger = zap.NewNop()
	}
	if q.ships.retryFirst == 0 {
		q.ships.retryFirst = defaultShipRetryFirst
	}
	if q.ships.retryMax == 0 {
		q.ships.retryMax = defaultShipRetryMax
	}
	if err := r.Handle(registry.BucketQueue, registry.BucketCommitAction, q.runCommit); err != nil {
		return nil, err
	}
	if err := q.registerAdmin(r); err != nil {
		return nil, err
	}
	if seals, ok := q.log.(SealReporter); ok {
		seals.OnSealed(q.QueueShip)
	}
	return q, nil
}

// Authority is what the tasks a commit spawns run with, captured from the
// request that queued it.
type Authority struct {
	// Release is the authority the releases of dropped claims remove their
	// blobs with.
	Release registry.ReleaseTask
	// RegisterAdd is the /upload/add chain the upload registrations a commit
	// queues run with; empty when the request held none, and the
	// registration is then skipped.
	RegisterAdd []byte
	// RegisterRemove is RegisterAdd's /upload/remove twin, for the
	// retractions.
	RegisterRemove []byte
}

// Outcome is what a commit reports to the request that ran it.
type Outcome struct {
	// Versioning is the bucket's versioning state under the commit, which
	// decided its versions' ids: response shaping gates on it rather than
	// on the request's earlier read.
	Versioning registry.VersioningState
	// Results are the changes' outcomes, in order. A change that failed on
	// its own terms has its Err set.
	Results []ChangeResult
}

// Commit runs a queue-bucket.commit of changes to st's bucket and waits for
// it. The commit fails as a whole only for a reason that is not one
// change's: the bucket gone (bucketop.ErrBucketNotFound) or closing
// (ErrBucketClosing), a reference expired as the commit wrote
// (registry.ErrReferenceExpired), or a storage error.
func (q *Queue) Commit(ctx context.Context, st *registry.State, auth Authority, changes ...KeyChange) (Outcome, error) {
	return q.commitWith(ctx, q.runner, st, auth, changes...)
}

// CommitChild is Commit run as a child of the task ex runs, so the commit's
// writes land with the parent's.
func (q *Queue) CommitChild(ctx context.Context, ex *taskrunner.Exec, st *registry.State, auth Authority, changes ...KeyChange) (Outcome, error) {
	return q.commitWith(ctx, ex, st, auth, changes...)
}

// syncRunner runs a sync task: the runner, for a request, or a task's Exec,
// for a child.
type syncRunner interface {
	Run(ctx context.Context, s taskrunner.Spec) ([]byte, error)
}

// commitWith runs the commit through run and decodes its result.
func (q *Queue) commitWith(ctx context.Context, run syncRunner, st *registry.State, auth Authority, changes ...KeyChange) (Outcome, error) {
	args := BucketCommitTask{Bucket: st.Name, Changes: changes, Release: auth.Release, RegisterAdd: auth.RegisterAdd, RegisterRemove: auth.RegisterRemove}
	var buf bytes.Buffer
	if err := args.MarshalCBOR(&buf); err != nil {
		return Outcome{}, fmt.Errorf("bucketqueue: encode commit: %w", err)
	}
	out, err := run.Run(ctx, q.bucketSpec(st.Space, registry.BucketCommitAction, buf.Bytes()))
	if err != nil {
		return Outcome{}, err
	}
	var res BucketCommitResult
	if err := res.UnmarshalCBOR(bytes.NewReader(out)); err != nil {
		return Outcome{}, fmt.Errorf("bucketqueue: decode commit result: %w", err)
	}
	return Outcome{Versioning: registry.VersioningState(res.Versioning), Results: res.Results}, nil
}

// commitScope is one commit's state while its changes apply.
type commitScope struct {
	tx   *bucketop.Tx
	args *BucketCommitTask
}

// runCommit is queue-bucket.commit.
func (q *Queue) runCommit(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) (_ []byte, err error) {
	var args BucketCommitTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode commit: %w", err))
	}
	ctx, span := tracing.Start(ctx, "bucket.commit")
	defer func() { tracing.End(span, err) }()

	tx, err := q.txns.Begin(ctx, args.Bucket)
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
		return nil, ErrBucketClosing
	}
	sc := &commitScope{tx: tx, args: &args}
	res := BucketCommitResult{Versioning: string(tx.State().Versioning), Results: make([]ChangeResult, len(args.Changes))}
	for i, ch := range args.Changes {
		mark := tx.Mark()
		r, root, err := q.applyChange(ctx, sc, ch)
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
		w, err := q.reg.RootCommitWrites(ctx, args.Bucket, appended.Expect, appended.Next, appended.Commit, appended.GC)
		if err != nil {
			return nil, err
		}
		if err := w.Apply(ctx, ex); err != nil {
			return nil, err
		}
		if err := q.queueAwaitedShip(ctx, ex, tx.State()); err != nil {
			return nil, err
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
func (q *Queue) applyChange(ctx context.Context, sc *commitScope, ch KeyChange) (ChangeResult, cid.Cid, error) {
	switch {
	case ch.Put != nil:
		return q.applyPut(ctx, sc, ch.Key, ch.Put)
	case ch.Delete != nil:
		return q.applyDelete(ctx, sc, ch.Key, ch.Delete)
	case ch.SetAttributes != nil:
		root, err := q.applySetAttributes(ctx, sc, ch.Key, ch.SetAttributes)
		return ChangeResult{}, root, err
	}
	return ChangeResult{}, cid.Undef, taskrunner.Permanent(fmt.Errorf("change to %q sets nothing", ch.Key))
}

// applyDelete removes the version d names, or the key's current one as the
// bucket's versioning state says: a delete marker when versioning is
// configured, the key when it is not.
func (q *Queue) applyDelete(ctx context.Context, sc *commitScope, key string, d *DeleteChange) (ChangeResult, cid.Cid, error) {
	if d.HasETag {
		etag, exists, err := q.currentETagIn(ctx, sc.tx, key)
		if err != nil {
			return ChangeResult{}, cid.Undef, err
		}
		if !exists || !etagsEqual(d.ETag, etag) {
			return ChangeResult{}, cid.Undef, preconditionFailed("")
		}
	}
	if d.VersionID != "" {
		return q.applyDeleteVersion(ctx, sc, key, d.VersionID, d.Pre)
	}
	if !sc.tx.State().Versioning.Configured() {
		root, err := q.applyDeleteKey(ctx, sc, key, d.Pre)
		return ChangeResult{}, root, err
	}
	marker := &PutChange{
		Manifest:  deleteMarkerManifest(key),
		DeletePre: d.Pre,
	}
	r, root, err := q.applyPut(ctx, sc, key, marker)
	r.DeleteMarker = true
	return r, root, err
}

// register queues one change to the space's content-entry list with the
// commit, under the request's captured authority. A request with none
// cannot produce authority for the registration to run with later, and
// failing the write over the object count would fail a write that otherwise
// succeeded: the registration is skipped and logged, and the count
// under-reports until something registers that root again.
func (q *Queue) register(sc *commitScope, st *registry.State, key string, root cid.Cid, op spaceuploadqueue.Op) {
	if !root.Defined() {
		return
	}
	proofs := sc.args.RegisterAdd
	if op == spaceuploadqueue.Remove {
		proofs = sc.args.RegisterRemove
	}
	if len(proofs) == 0 {
		q.logger.Warn("commit: no authority to queue an upload registration; object count will drift",
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
		q.logger.Warn("commit: could not encode an upload registration; object count will drift",
			zap.String("bucket", st.Name), zap.Stringer("root", root), zap.Error(err))
		return
	}
	sc.tx.EnqueueUploadRegistration(spec)
}

// putPreCheck returns the check a put runs against the version it would
// supersede (nil when the key is new): a delete marker's delete
// preconditions, a Complete's If-Match and If-None-Match, or a PutObject's.
func (q *Queue) putPreCheck(_ context.Context, p *PutChange) (func(superseded *msbucket.ObjectManifest) error, error) {
	return func(superseded *msbucket.ObjectManifest) error {
		// A delete-marker current means "no object" for precondition
		// purposes.
		oldETag, oldExists := "", false
		if superseded != nil && !superseded.DeleteMarker {
			oldETag, oldExists = superseded.QuotedETag(), true
		}
		switch {
		case p.DeletePre != nil:
			if !oldExists {
				return nil
			}
			return p.DeletePre.check(superseded)
		case p.Completes:
			if p.HasIfMatch {
				if !oldExists {
					return errNoSuchKey
				}
				if !etagsEqual(p.IfMatch, oldETag) {
					return preconditionFailed("")
				}
			}
			if p.HasIfNoneMatch && oldExists {
				return preconditionFailed("")
			}
			return nil
		}
		// A PutObject's If-None-Match is "*", and it carries at most one of
		// the two; the request checked both.
		switch {
		case p.HasIfNoneMatch && oldExists:
			return preconditionFailed(ConditionIfNoneMatch)
		case p.HasIfMatch && !oldExists:
			return errNoSuchKey
		case p.HasIfMatch && p.IfMatch != strings.Trim(oldETag, `"`):
			return preconditionFailed(ConditionIfMatch)
		}
		return nil
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
func (q *Queue) currentETagIn(ctx context.Context, tx *bucketop.Tx, key string) (string, bool, error) {
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
	return mf.QuotedETag(), true, nil
}
