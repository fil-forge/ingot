package taskrunner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// FailedTask is a task that failed for good: its handler returned a permanent
// error, or it ran out of attempts. It stays in its queue until it is
// resubmitted or its queue's FailedRetention passes, so the work it stood for
// can be inspected and replayed.
type FailedTask struct {
	// Task is the task as it last ran.
	Task Task
	// Error is the error it last failed with.
	Error string
	// Failed is when it failed for good.
	Failed time.Time
	// Sync is whether it ran inline for a caller. Such a task is listed but
	// cannot be resubmitted (ErrSyncTask).
	Sync bool
}

const listFailed = `SELECT seq, id, subject, action, created, not_before, expires, args, attempts, node,
	coalesce(error, ''), finished, sync FROM %[1]s
	WHERE state = 'failed' AND ($1 = '' OR subject = $1)
	  AND ($2::uuid IS NULL OR seq > (SELECT seq FROM %[1]s WHERE id = $2))
	ORDER BY seq LIMIT $3`

// ListFailed returns a queue's failed tasks, oldest first, at most limit. An
// empty subject lists the whole queue's. after pages through them: the zero
// TaskID starts at the oldest, and the ID of the last task of one page lists
// the next. A task that page listed stays a valid cursor until its queue's
// retention removes it, whether or not it has been resubmitted since.
func (r *Runner) ListFailed(ctx context.Context, queueName, subject string, after TaskID, limit int) ([]FailedTask, error) {
	q, ok := r.byName[queueName]
	if !ok {
		return nil, fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	return listFailedTasks(ctx, r.db, q, subject, after, limit)
}

// ListFailed is Runner.ListFailed, read through the handler's DB.
func (ex *Exec) ListFailed(ctx context.Context, queueName, subject string, after TaskID, limit int) ([]FailedTask, error) {
	q, ok := ex.tr.r.byName[queueName]
	if !ok {
		return nil, fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	return listFailedTasks(ctx, ex.DB(), q, subject, after, limit)
}

func listFailedTasks(ctx context.Context, db Querier, q *queue, subject string, after TaskID, limit int) ([]FailedTask, error) {
	var cursor *TaskID
	if after != uuid.Nil {
		cursor = &after
	}
	rows, err := db.Query(ctx, fmt.Sprintf(listFailed, q.tasks), subject, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("list failed %s tasks: %w", q.name, err)
	}
	defer rows.Close()
	var out []FailedTask
	for rows.Next() {
		f := FailedTask{Task: Task{Queue: q.name}}
		t := &f.Task
		if err := rows.Scan(&t.seq, &t.ID, &t.Subject, &t.Action, &t.Created, &t.NotBefore, &t.Expires,
			&t.Args, &t.Attempt, &t.Node, &f.Error, &f.Failed, &f.Sync); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// resubmitStatements return the writes that replace a failed async task with
// a new pending one on the same subject and action: with args when they are
// not nil, its own otherwise, and none of its delay, expiry or attempts. The
// old row is marked resubmitted, so it is not listed or resubmitted again.
func resubmitStatements(q *queue, old, fresh TaskID, args []byte) []write {
	return []write{
		{
			sql: `INSERT INTO ` + q.tasks + ` (id, subject, action, args, sync, node)
				SELECT $2, subject, action, coalesce($3, args), false, node FROM ` + q.tasks + `
				WHERE id = $1 AND state = 'failed' AND NOT sync`,
			args:        []any{old, fresh, args},
			conditional: true,
		},
		{
			sql:         `UPDATE ` + q.tasks + ` SET state = 'resubmitted' WHERE id = $1 AND state = 'failed'`,
			args:        []any{old},
			conditional: true,
		},
	}
}

// Resubmit replaces a failed task with a new pending one on the same subject
// and action, which runs after the tasks already queued on its subject. It
// takes args when they are not nil, so a replay can carry what the failure
// lacked, such as fresh authority; otherwise it keeps the failed task's. The
// new task has no delay or expiry and starts its attempts afresh. It returns
// the new task's ID, ErrNotFound when the queue holds no failed task with
// that ID, or ErrSyncTask when the task ran sync.
func (r *Runner) Resubmit(ctx context.Context, queueName string, id TaskID, args []byte) (TaskID, error) {
	q, ok := r.byName[queueName]
	if !ok {
		return TaskID{}, fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	fresh, err := uuid.NewV7()
	if err != nil {
		return TaskID{}, err
	}
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return TaskID{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := checkResubmittable(ctx, tx, q, id); err != nil {
		return TaskID{}, err
	}
	for _, w := range resubmitStatements(q, id, fresh, args) {
		tag, err := tx.Exec(ctx, w.sql, w.args...)
		if err != nil {
			return TaskID{}, fmt.Errorf("resubmit %s/%s: %w", queueName, id, err)
		}
		if tag.RowsAffected() == 0 {
			return TaskID{}, ErrNotFound
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return TaskID{}, fmt.Errorf("resubmit %s/%s: %w", queueName, id, err)
	}
	r.pokeQueue(q)
	return fresh, nil
}

// Resubmit adds Runner.Resubmit to the tree's writes, so the replay commits
// with them. A task that is no longer failed when the tree commits fails the
// commit, as a conditional write does.
func (ex *Exec) Resubmit(ctx context.Context, queueName string, id TaskID, args []byte) (TaskID, error) {
	q, ok := ex.tr.r.byName[queueName]
	if !ok {
		return TaskID{}, fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	if err := checkResubmittable(ctx, ex.DB(), q, id); err != nil {
		return TaskID{}, err
	}
	fresh, err := uuid.NewV7()
	if err != nil {
		return TaskID{}, err
	}
	for _, w := range resubmitStatements(q, id, fresh, args) {
		if err := ex.tr.add(ctx, w); err != nil {
			return TaskID{}, err
		}
	}
	ex.tr.spawned[q] = true
	return fresh, nil
}

// checkResubmittable fails with ErrNotFound unless q holds a failed task id,
// and with ErrSyncTask when that task ran sync.
func checkResubmittable(ctx context.Context, db Querier, q *queue, id TaskID) error {
	var sync bool
	err := db.QueryRow(ctx, `SELECT sync FROM `+q.tasks+` WHERE id = $1 AND state = 'failed'`, id).Scan(&sync)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrNotFound
	case err != nil:
		return fmt.Errorf("resubmit %s/%s: %w", q.name, id, err)
	case sync:
		return ErrSyncTask
	}
	return nil
}

// retentionBatch is the most finished tasks one statement deletes, so a large
// backlog is cleared in short statements rather than one long one.
const retentionBatch = 1000

// retentionLoop deletes each queue's finished tasks once they are older than
// its retention, every RetentionInterval.
func (r *Runner) retentionLoop(ctx context.Context) {
	defer r.wg.Done()
	t := time.NewTicker(r.opts.RetentionInterval)
	defer t.Stop()
	for {
		for _, q := range r.order {
			r.prune(ctx, q, []string{"succeeded", "expired", "cancelled", "resubmitted"}, q.retention)
			if q.failedRetention > 0 {
				r.prune(ctx, q, []string{"failed"}, q.failedRetention)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// prune deletes q's tasks in the given states that finished more than age ago.
func (r *Runner) prune(ctx context.Context, q *queue, states []string, age time.Duration) {
	for {
		tag, err := r.db.Exec(ctx, `DELETE FROM `+q.tasks+` WHERE id IN (
			SELECT id FROM `+q.tasks+` WHERE state = ANY($1) AND finished < now() - $2 * interval '1 millisecond'
			LIMIT $3)`, states, age.Milliseconds(), retentionBatch)
		if err != nil {
			if ctx.Err() == nil {
				r.log.Warn("delete finished tasks", zapQueue(q.name), zapErr(err))
			}
			return
		}
		if tag.RowsAffected() < retentionBatch {
			return
		}
	}
}

// countFailed returns each action's failed tasks in q, for the failed gauge.
func countFailed(ctx context.Context, db Querier, q *queue) (map[string]int64, error) {
	rows, err := db.Query(ctx, `SELECT action, count(*) FROM `+q.tasks+` WHERE state = 'failed' GROUP BY action`)
	if err != nil {
		return nil, err
	}
	counts := map[string]int64{}
	for rows.Next() {
		var action string
		var n int64
		if err := rows.Scan(&action, &n); err != nil {
			rows.Close()
			return nil, err
		}
		counts[action] = n
	}
	rows.Close()
	return counts, rows.Err()
}
