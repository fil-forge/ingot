package registry

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
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
	for _, st := range w.Statements {
		if _, err := tx.Exec(ctx, st.SQL, st.Args...); err != nil {
			return err
		}
	}
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
	if len(w.Spawns) > 0 {
		if _, err := r.tasks.SubmitAllTx(ctx, tx, w.Spawns); err != nil {
			return err
		}
	}
	if len(w.Expiries) > 0 {
		if err := r.tasks.CancelTx(ctx, tx, ReleaseQueue, w.Expiries...); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// CASRoot is CommitRoot with nothing besides the root.
func (r *Postgres) CASRoot(ctx context.Context, name string, expect, next cid.Cid) error {
	return r.CommitRoot(ctx, name, expect, next, RootCommit{})
}
