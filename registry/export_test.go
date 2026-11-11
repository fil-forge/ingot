package registry

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/taskrunner"
)

// CommitRoot applies RootCommitWrites as a queue-bucket.commit commits
// them, in one transaction, for tests that drive the registry directly. A
// guard that affects no row rolls everything back: the root's guard with
// ErrNotFound for a missing bucket and ErrConflict for a moved root, a
// reference take-over's with ErrReferenceExpired.
func (r *Postgres) CommitRoot(ctx context.Context, name string, expect, next cid.Cid, c RootCommit) error {
	w, err := r.RootCommitWrites(ctx, name, expect, next, c, nil)
	if err != nil {
		return err
	}
	if (len(w.Spawns) > 0 || len(w.Expiries) > 0) && r.tasks == nil {
		return fmt.Errorf("registry: commit %q queues tasks but the registry has no task runner", name)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for i, st := range w.Guards {
		tag, err := tx.Exec(ctx, st.SQL, st.Args...)
		if err != nil {
			return err
		}
		if tag.RowsAffected() > 0 {
			continue
		}
		if i > 0 {
			return ErrReferenceExpired
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM ingot.buckets WHERE name = $1)`, name).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrNotFound
		}
		return ErrConflict
	}
	for _, st := range w.Statements {
		if _, err := tx.Exec(ctx, st.SQL, st.Args...); err != nil {
			return err
		}
	}
	if len(w.Spawns) > 0 {
		if _, err := r.tasks.SubmitAllTx(ctx, tx, w.Spawns); err != nil {
			return err
		}
	}
	if len(w.Expiries) > 0 {
		if err := r.tasks.CancelTx(ctx, tx, SpaceShardQueue, w.Expiries...); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// CASRoot is CommitRoot with nothing besides the root.
func (r *Postgres) CASRoot(ctx context.Context, name string, expect, next cid.Cid) error {
	return r.CommitRoot(ctx, name, expect, next, RootCommit{})
}

// TakeoverGuard exposes takeoverGuard to the package's tests.
func TakeoverGuard(refs []taskrunner.TaskID) Statement { return takeoverGuard(refs) }

// PutPart applies PartWrites as a queue-multipart-upload.add-part commits
// them, in one transaction, for tests that drive the registry directly. A
// guard that affects no row is ErrNotFound: the upload was no longer open,
// or a reference had gone.
func (r *Postgres) PutPart(ctx context.Context, p MultipartPart, tree *PartTree, release ReleaseTask) ([]multihash.Multihash, error) {
	w, released, err := r.PartWrites(ctx, p, tree, release)
	if err != nil {
		return nil, err
	}
	if r.tasks == nil {
		return nil, fmt.Errorf("registry: put part: the registry has no task runner")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, st := range w.Guards {
		tag, err := tx.Exec(ctx, st.SQL, st.Args...)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() == 0 {
			return nil, ErrNotFound
		}
	}
	for _, st := range w.Statements {
		if _, err := tx.Exec(ctx, st.SQL, st.Args...); err != nil {
			return nil, err
		}
	}
	for _, spec := range w.Spawns {
		if _, err := r.tasks.SubmitTx(ctx, tx, spec); err != nil {
			return nil, err
		}
	}
	if len(w.Expiries) > 0 {
		if err := r.tasks.CancelTx(ctx, tx, SpaceShardQueue, w.Expiries...); err != nil {
			return nil, err
		}
	}
	return released, tx.Commit(ctx)
}
