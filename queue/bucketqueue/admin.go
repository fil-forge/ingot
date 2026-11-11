package bucketqueue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/fil-forge/ucantone/did"

	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// A bucket's settings and lifecycle change on its subject too, so a commit
// never runs with them half-changed, and the versioning state and the object
// lock configuration, each of which the other's change checks, cannot race.

// BucketTask is the arguments of a queue-bucket action that names only its
// bucket: delete and ship.
type BucketTask struct {
	// Name is the bucket's name.
	Name string
}

// BucketCreateTask is a queue-bucket.create task's arguments.
type BucketCreateTask struct {
	// Name is the bucket's name.
	Name string
	// Tenant is the owning tenant's DID.
	Tenant string
	// Versioning and LockConfig are the bucket's initial versioning state
	// and object lock configuration: a bucket created with object lock is
	// born versioned and locked.
	Versioning string
	LockConfig []byte
}

// BucketSetVersioningTask is a queue-bucket.set-versioning task's arguments.
type BucketSetVersioningTask struct {
	// Name is the bucket's name.
	Name string
	// State is Enabled or Suspended.
	State string
}

// BucketSetLockConfigTask is a queue-bucket.set-object-lock-config task's
// arguments.
type BucketSetLockConfigTask struct {
	// Name is the bucket's name.
	Name string
	// Config is the lock configuration, validated by the S3 layer.
	Config []byte
}

// BucketSetConfigTask is the arguments of a queue-bucket action that stores
// one of the bucket's settings: set-tagging, set-cors and set-encryption.
type BucketSetConfigTask struct {
	// Name is the bucket's name.
	Name string
	// Value is the setting, validated by the S3 layer: the tag set or CORS
	// document, or the encryption algorithm. Empty clears it.
	Value []byte
}

// cborArgs is a task's arguments.
type cborArgs interface {
	MarshalCBOR(w io.Writer) error
}

// encodeBucketTask encodes a queue-bucket task's arguments.
func encodeBucketTask(args cborArgs) ([]byte, error) {
	var buf bytes.Buffer
	if err := args.MarshalCBOR(&buf); err != nil {
		return nil, fmt.Errorf("bucketqueue: encode bucket task: %w", err)
	}
	return buf.Bytes(), nil
}

// registerAdmin registers the queue-bucket actions besides commit.
func (q *Queue) registerAdmin(r *taskrunner.Runner) error {
	for action, h := range map[string]taskrunner.Handler{
		registry.BucketCreateAction:        q.runCreateBucket,
		registry.BucketSetVersioningAction: q.runSetVersioning,
		registry.BucketSetLockConfigAction: q.runSetLockConfig,
		registry.BucketSetTaggingAction:    q.runSetConfig(registry.TaggingSet),
		registry.BucketSetCORSAction:       q.runSetConfig(registry.CORSSet),
		registry.BucketSetEncryptionAction: q.runSetConfig(func(name string, v []byte) registry.Statement {
			return registry.EncryptionSet(name, registry.BucketEncryption(v))
		}),
		registry.BucketCloseAction:       q.runCloseBucket,
		registry.BucketAdmitUploadAction: q.runAdmitUpload,
		registry.BucketDeleteAction:      q.runDeleteBucket,
	} {
		if err := r.Handle(registry.BucketQueue, action, h); err != nil {
			return err
		}
	}
	return q.registerShip(r)
}

// bucketSpec is a queue-bucket task on space's bucket, placed on this
// instance, which holds the bucket's catalog log.
func (q *Queue) bucketSpec(space did.DID, action string, args []byte) taskrunner.Spec {
	spec := taskrunner.Spec{
		Queue:   registry.BucketQueue,
		Subject: registry.BucketSubject(space),
		Action:  action,
		Args:    args,
	}
	if node := q.runner.Node(); node != "" {
		spec.Node = &node
	}
	return spec
}

// runBucket runs a queue-bucket action on space's bucket for the request
// waiting on it.
func (q *Queue) runBucket(ctx context.Context, space did.DID, action string, args cborArgs) error {
	raw, err := encodeBucketTask(args)
	if err != nil {
		return err
	}
	_, err = q.runner.Run(ctx, q.bucketSpec(space, action, raw))
	return err
}

// Create records bucket args.Name in space, whose space the bucket's
// authority created for the request beforehand, with its empty root.
func (q *Queue) Create(ctx context.Context, space did.DID, args BucketCreateTask) error {
	return q.runBucket(ctx, space, registry.BucketCreateAction, &args)
}

// SetVersioning sets the versioning state of space's bucket name.
func (q *Queue) SetVersioning(ctx context.Context, space did.DID, name string, state registry.VersioningState) error {
	return q.runBucket(ctx, space, registry.BucketSetVersioningAction, &BucketSetVersioningTask{Name: name, State: string(state)})
}

// SetLockConfig sets the object lock configuration of space's bucket name.
func (q *Queue) SetLockConfig(ctx context.Context, space did.DID, name string, config []byte) error {
	return q.runBucket(ctx, space, registry.BucketSetLockConfigAction, &BucketSetLockConfigTask{Name: name, Config: config})
}

// SetTagging sets the tag set of space's bucket name; empty clears it.
func (q *Queue) SetTagging(ctx context.Context, space did.DID, name string, tags []byte) error {
	return q.runBucket(ctx, space, registry.BucketSetTaggingAction, &BucketSetConfigTask{Name: name, Value: tags})
}

// SetCORS sets the CORS document of space's bucket name; empty clears it.
func (q *Queue) SetCORS(ctx context.Context, space did.DID, name string, doc []byte) error {
	return q.runBucket(ctx, space, registry.BucketSetCORSAction, &BucketSetConfigTask{Name: name, Value: doc})
}

// SetEncryption sets the default encryption of space's bucket name.
func (q *Queue) SetEncryption(ctx context.Context, space did.DID, name string, enc registry.BucketEncryption) error {
	return q.runBucket(ctx, space, registry.BucketSetEncryptionAction, &BucketSetConfigTask{Name: name, Value: []byte(enc)})
}

// Close marks space's bucket name closing, the first step of its delete: it
// then stays empty and gains no multipart upload while the request aborts
// the ones in flight. It fails on a bucket that holds objects.
func (q *Queue) Close(ctx context.Context, space did.DID, name string) error {
	return q.runBucket(ctx, space, registry.BucketCloseAction, &BucketTask{Name: name})
}

// AdmitUpload refuses a multipart upload to space's bucket name while it is
// closing, as a child of the task ex runs: the upload's create.
func (q *Queue) AdmitUpload(ctx context.Context, ex *taskrunner.Exec, space did.DID, name string) error {
	raw, err := encodeBucketTask(&BucketTask{Name: name})
	if err != nil {
		return err
	}
	_, err = ex.Run(ctx, q.bucketSpec(space, registry.BucketAdmitUploadAction, raw))
	return err
}

// Delete deletes space's bucket name, the last step of its delete, after
// Close and the aborts of its multipart uploads, with the authority of the
// S3 request in ctx.
func (q *Queue) Delete(ctx context.Context, space did.DID, name string) error {
	return q.runBucket(context.WithValue(ctx, deleteKey{}, name), space, registry.BucketDeleteAction, &BucketTask{Name: name})
}

// runSetConfig returns the handler of a queue-bucket action that stores one
// of the bucket's settings with write, with the task's commit.
func (q *Queue) runSetConfig(write func(name string, value []byte) registry.Statement) taskrunner.Handler {
	return func(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
		var args BucketSetConfigTask
		if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
			return nil, taskrunner.Permanent(fmt.Errorf("decode %s: %w", t.Action, err))
		}
		if _, err := q.bucketOn(ctx, args.Name, t.Subject); err != nil {
			return nil, err
		}
		st := write(args.Name, args.Value)
		return nil, ex.WriteConditional(ctx, st.SQL, st.Args...)
	}
}

// bucketOn returns bucket name's state when it still lives in the space
// subject, the bucket task's subject. A bucket deleted and recreated under the
// same name lives in another space, and is not the bucket a task queued for
// the first was meant for: that is NoSuchBucket, so the task acts on no
// bucket but its own while it holds its own space's lease.
func (q *Queue) bucketOn(ctx context.Context, name, subject string) (*registry.State, error) {
	st, err := q.reg.Get(ctx, name)
	if errors.Is(err, registry.ErrNotFound) || (err == nil && st.Space.String() != subject) {
		return nil, bucketop.ErrBucketNotFound
	}
	return st, err
}

// bucketLogRemover is a catalog log that can remove a bucket's log entirely.
type bucketLogRemover interface {
	RemoveBucketLog(ctx context.Context, bucket string) error
}

// runCreateBucket is queue-bucket.create: it records the bucket, whose space
// Hilt created for the request beforehand, with its empty root. It is safe to
// run again: a bucket already recorded in the task's space is this one.
func (q *Queue) runCreateBucket(ctx context.Context, _ *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args BucketCreateTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode create: %w", err))
	}
	space, err := did.Parse(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("create subject: %w", err))
	}
	tenant, err := did.Parse(args.Tenant)
	if err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("create tenant: %w", err))
	}
	init := registry.CreateState{Tenant: tenant, Versioning: registry.VersioningState(args.Versioning), ObjectLockConfig: args.LockConfig}
	// A bucket deleted under this name may have left its log behind, since
	// the delete removes it best-effort, and a new bucket must not ship the
	// old one's catalog. A run of this task that recorded the bucket already
	// finds its row below and leaves the log alone.
	if _, err := q.reg.Get(ctx, args.Name); errors.Is(err, registry.ErrNotFound) {
		if remover, ok := q.log.(bucketLogRemover); ok {
			if err := remover.RemoveBucketLog(ctx, args.Name); err != nil {
				return nil, fmt.Errorf("remove a deleted bucket's log: %w", err)
			}
		}
	} else if err != nil {
		return nil, err
	}
	if err := q.reg.Create(ctx, args.Name, space, init); err != nil {
		if !errors.Is(err, registry.ErrExists) {
			return nil, err
		}
		// A run of this task that created the bucket and then lost its
		// lease finds its own row: a bucket is its space, so a row in this
		// task's space is this create's.
		if _, err := q.bucketOn(ctx, args.Name, t.Subject); err == nil {
			return nil, nil
		}
		return nil, ErrBucketExists
	}
	return nil, nil
}

// runSetVersioning is queue-bucket.set-versioning. Suspending a bucket that
// carries an object-lock configuration is refused, which makes "a lock
// bucket is always versioning-Enabled" an invariant (docs/s3-object-lock.md
// §5): the configuration changes on this subject too, so the check holds
// until the state is written.
func (q *Queue) runSetVersioning(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args BucketSetVersioningTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode set-versioning: %w", err))
	}
	st, err := q.bucketOn(ctx, args.Name, t.Subject)
	if err != nil {
		return nil, err
	}
	v := registry.VersioningState(args.State)
	if v == registry.VersioningSuspended && st.ObjectLockConfig != nil {
		return nil, ErrLockedVersioning
	}
	st2, err := registry.VersioningSet(args.Name, v)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	// Written with the task's commit, under its lease, so a handler that lost
	// its lease cannot overwrite a newer setting.
	return nil, ex.WriteConditional(ctx, st2.SQL, st2.Args...)
}

// runSetLockConfig is queue-bucket.set-object-lock-config: it stores the
// bucket's lock configuration, which needs versioning Enabled — enabling lock
// on an existing versioned bucket is allowed, a suspended or unversioned one
// is a 409 (docs/s3-object-lock.md §5).
func (q *Queue) runSetLockConfig(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args BucketSetLockConfigTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode set-object-lock-config: %w", err))
	}
	st, err := q.bucketOn(ctx, args.Name, t.Subject)
	if err != nil {
		return nil, err
	}
	if st.Versioning != registry.VersioningEnabled {
		return nil, ErrLockNeedsVersioning
	}
	st2 := registry.LockConfigSet(args.Name, args.Config)
	// Written with the task's commit, as the versioning state is.
	return nil, ex.WriteConditional(ctx, st2.SQL, st2.Args...)
}

// runCloseBucket is queue-bucket.close, the first step of DeleteBucket: it
// refuses a bucket that holds objects and otherwise marks it closing, so it
// stays empty (queue-bucket.commit refuses it) and gains no multipart upload
// (queue-bucket.admit-upload refuses it) while the request aborts the ones in
// flight. A bucket already closing, by a delete that stopped, closes again.
func (q *Queue) runCloseBucket(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args BucketTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode close: %w", err))
	}
	st, err := q.bucketOn(ctx, args.Name, t.Subject)
	if err != nil {
		return nil, err
	}
	if err := q.refuseNonEmpty(ctx, st); err != nil {
		return nil, err
	}
	// Written with the task's commit, under its lease, as the bucket's
	// settings are.
	st2 := registry.ClosingSet(args.Name)
	return nil, ex.WriteConditional(ctx, st2.SQL, st2.Args...)
}

// runAdmitUpload is queue-bucket.admit-upload, which
// queue-multipart-upload.create runs as a child: it refuses a closing
// bucket. The create holds this subject until it commits, so its upload
// either exists before a queue-bucket.close, and the bucket's delete aborts
// it, or is refused here.
func (q *Queue) runAdmitUpload(ctx context.Context, _ *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args BucketTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode admit-upload: %w", err))
	}
	st, err := q.bucketOn(ctx, args.Name, t.Subject)
	if err != nil {
		return nil, err
	}
	if st.Closing {
		return nil, ErrBucketClosing
	}
	return nil, nil
}
