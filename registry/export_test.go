package registry

import (
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/taskrunner"
)

// OwedTasks queues in tx the tasks a commit's writes owe: their spawns and
// releases, and the cancels of their expiries. The package's tests supply
// it: the queue the releases go on imports this package.
type OwedTasks func(ctx context.Context, tx pgx.Tx, w CommitWrites) error

// owedTasks holds each test registry's OwedTasks.
var owedTasks sync.Map

// WithOwedTasks has r's CommitRoot and PutPart queue the tasks a commit's
// writes owe through queue, and returns r.
func WithOwedTasks(r *Postgres, queue OwedTasks) *Postgres {
	owedTasks.Store(r, queue)
	return r
}

// queueOwed queues w's tasks in tx through r's OwedTasks.
func (r *Postgres) queueOwed(ctx context.Context, tx pgx.Tx, w CommitWrites) error {
	if len(w.Spawns) == 0 && len(w.Releases) == 0 && len(w.Expiries) == 0 {
		return nil
	}
	queue, ok := owedTasks.Load(r)
	if !ok {
		return fmt.Errorf("registry: the writes queue tasks but the registry has no task runner")
	}
	return queue.(OwedTasks)(ctx, tx, w)
}

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
	if err := r.queueOwed(ctx, tx, w); err != nil {
		return err
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
// or a reference had gone. It returns the blobs whose releases it queued.
func (r *Postgres) PutPart(ctx context.Context, p MultipartPart, tree *PartTree) ([]multihash.Multihash, error) {
	w, err := r.PartWrites(ctx, p, tree)
	if err != nil {
		return nil, err
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
	if err := r.queueOwed(ctx, tx, w); err != nil {
		return nil, err
	}
	var released []multihash.Multihash
	for _, rel := range w.Releases {
		released = append(released, rel.Digest)
	}
	return released, tx.Commit(ctx)
}
