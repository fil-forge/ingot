package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"

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

// cborArgs is a task's arguments.
type cborArgs interface {
	MarshalCBOR(w io.Writer) error
}

// encodeBucketTask encodes a queue-bucket task's arguments.
func encodeBucketTask(args cborArgs) ([]byte, error) {
	var buf bytes.Buffer
	if err := args.MarshalCBOR(&buf); err != nil {
		return nil, fmt.Errorf("s3frontend: encode bucket task: %w", err)
	}
	return buf.Bytes(), nil
}

// registerBucketAdmin registers the queue-bucket actions besides commit.
func (b *Backend) registerBucketAdmin(r *taskrunner.Runner) error {
	for action, h := range map[string]taskrunner.Handler{
		registry.BucketCreateAction:        b.runCreateBucket,
		registry.BucketSetVersioningAction: b.runSetVersioning,
		registry.BucketSetLockConfigAction: b.runSetLockConfig,
		registry.BucketDeleteAction:        b.runDeleteBucket,
	} {
		if err := r.Handle(registry.BucketQueue, action, h); err != nil {
			return err
		}
	}
	return b.registerShip(r)
}

// runBucket runs a queue-bucket action on space's bucket for the request
// waiting on it.
func (b *Backend) runBucket(ctx context.Context, space did.DID, action string, args cborArgs) error {
	raw, err := encodeBucketTask(args)
	if err != nil {
		return err
	}
	_, err = b.buckets.Run(ctx, b.bucketSpec(space, action, raw))
	return err
}

// bucketOn returns bucket name's state when it still lives in the space
// subject, the bucket task's subject. A bucket deleted and recreated under the
// same name lives in another space, and is not the bucket a task queued for
// the first was meant for: that is NoSuchBucket, so the task acts on no
// bucket but its own while it holds its own space's lease.
func (b *Backend) bucketOn(ctx context.Context, name, subject string) (*registry.State, error) {
	st, err := b.reg.Get(ctx, name)
	if errors.Is(err, registry.ErrNotFound) || (err == nil && st.Space.String() != subject) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return st, err
}

// runCreateBucket is queue-bucket.create: it records the bucket, whose space
// Hilt created for the request beforehand, with its empty root.
func (b *Backend) runCreateBucket(ctx context.Context, _ *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
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
	if err := b.reg.Create(ctx, args.Name, space, init); err != nil {
		if errors.Is(err, registry.ErrExists) {
			return nil, s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
		}
		return nil, err
	}
	return nil, nil
}

// runSetVersioning is queue-bucket.set-versioning. Suspending a bucket that
// carries an object-lock configuration is refused, which makes "a lock
// bucket is always versioning-Enabled" an invariant (docs/s3-object-lock.md
// §5): the configuration changes on this subject too, so the check holds
// until the state is written.
func (b *Backend) runSetVersioning(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args BucketSetVersioningTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode set-versioning: %w", err))
	}
	st, err := b.bucketOn(ctx, args.Name, t.Subject)
	if err != nil {
		return nil, err
	}
	v := registry.VersioningState(args.State)
	if v == registry.VersioningSuspended && st.ObjectLockConfig != nil {
		return nil, s3err.GetAPIError(s3err.ErrSuspendedVersioningNotAllowed)
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
func (b *Backend) runSetLockConfig(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args BucketSetLockConfigTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode set-object-lock-config: %w", err))
	}
	st, err := b.bucketOn(ctx, args.Name, t.Subject)
	if err != nil {
		return nil, err
	}
	if st.Versioning != registry.VersioningEnabled {
		return nil, s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotAllowed)
	}
	st2 := registry.LockConfigSet(args.Name, args.Config)
	// Written with the task's commit, as the versioning state is.
	return nil, ex.WriteConditional(ctx, st2.SQL, st2.Args...)
}
