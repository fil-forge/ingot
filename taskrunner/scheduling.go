package taskrunner

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

// BatchHandler runs a batch of tasks with the same subject and action, oldest
// first. It returns one result per task, and the batch commits or fails
// together.
type BatchHandler func(ctx context.Context, ex *Exec, tasks []Task) ([][]byte, error)

// RetryPolicy says when an async task whose handler returned an error runs
// again. A sync task is never retried: its error goes to its caller.
type RetryPolicy struct {
	// First is the delay before the second attempt.
	First time.Duration
	// Max caps the delay between attempts.
	Max time.Duration
	// Multiplier scales the delay after each failed attempt. Values below 1
	// count as 1.
	Multiplier float64
	// Attempts is the most times the task runs. Zero means no limit.
	Attempts int
}

// delay is the wait after the given number of failed attempts.
func (p RetryPolicy) delay(failures int) time.Duration {
	m := math.Max(p.Multiplier, 1)
	d := float64(p.First) * math.Pow(m, float64(failures-1))
	if p.Max > 0 && d > float64(p.Max) {
		return p.Max
	}
	return time.Duration(d)
}

// HandleOption configures a registered action.
type HandleOption func(*action)

// WithRetry retries the action's async tasks under p.
func WithRetry(p RetryPolicy) HandleOption {
	return func(a *action) { a.retry = &p }
}

// action is a registered (queue, action).
type action struct {
	handler Handler
	// batch and limit are set for a batching action instead of handler.
	batch BatchHandler
	limit int
	retry *RetryPolicy
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks a handler's error as one a retry cannot get past, so the
// task fails without its remaining attempts.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// ErrCancelled is returned by Wait for a task that was cancelled before it
// started.
var ErrCancelled = errors.New("task cancelled")

// insertSpec returns the statement that adds s's task with the given ID. A
// replacement keeps whichever of it and a pending, unstarted replacement with
// the same subject and action has the later NotBefore, and drops the other.
func insertSpec(q *queue, id TaskID, s Spec, sync bool) (string, []any) {
	args := []any{id, s.Subject, s.Action, s.NotBefore, s.Expires, s.Args, sync}
	if !s.Replace {
		return fmt.Sprintf(insertTask, q.tasks), args
	}
	return `INSERT INTO ` + q.tasks + ` AS t (id, subject, action, not_before, expires, args, sync, replaceable)
	VALUES ($1, $2, $3, $4, $5, $6, $7, true)
	ON CONFLICT (subject, action) WHERE state = 'pending' AND replaceable AND started IS NULL
	DO UPDATE SET id = EXCLUDED.id, seq = DEFAULT, created = EXCLUDED.created,
		not_before = EXCLUDED.not_before, expires = EXCLUDED.expires, args = EXCLUDED.args
	WHERE t.not_before < EXCLUDED.not_before`, args
}

const cancelTask = `UPDATE %s SET state = 'cancelled', finished = now()
	WHERE id = $1 AND state = 'pending' AND started IS NULL`

// Cancel cancels a pending task that has not started. Cancelling a task that
// has started, finished or been cancelled does nothing, so a handler whose
// task can be cancelled must still check its own preconditions.
func (r *Runner) Cancel(ctx context.Context, queueName string, id TaskID) error {
	q, ok := r.byName[queueName]
	if !ok {
		return fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	if _, err := r.db.Exec(ctx, fmt.Sprintf(cancelTask, q.tasks), id); err != nil {
		return fmt.Errorf("cancel %s/%s: %w", queueName, id, err)
	}
	r.finished(id)
	return nil
}

// CancelTx is Cancel inside tx, so the cancel commits or rolls back with the
// caller's own writes.
func (r *Runner) CancelTx(ctx context.Context, tx pgx.Tx, queueName string, id TaskID) error {
	q, ok := r.byName[queueName]
	if !ok {
		return fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(cancelTask, q.tasks), id); err != nil {
		return fmt.Errorf("cancel %s/%s: %w", queueName, id, err)
	}
	return nil
}

// Cancel adds the cancel of a pending task to the tree's writes, so it
// commits with them. See Runner.Cancel.
func (ex *Exec) Cancel(ctx context.Context, queueName string, id TaskID) error {
	q, ok := ex.tr.r.byName[queueName]
	if !ok {
		return fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	if err := ex.tr.add(ctx, write{sql: fmt.Sprintf(cancelTask, q.tasks), args: []any{id}}); err != nil {
		return err
	}
	ex.tr.woken = append(ex.tr.woken, id)
	return nil
}

const claimColumns = `seq, id, action, created, not_before, expires, args, sync, attempts`

func scanTask(row pgx.Row, t *Task) error {
	return row.Scan(&t.seq, &t.ID, &t.Action, &t.Created, &t.NotBefore, &t.Expires, &t.Args, &t.sync, &t.Attempt)
}

// claim marks the subject's next due task started and returns it. When its
// action batches, the due tasks straight after it with the same action join
// it, up to the action's limit. A next task other than target whose Run
// caller is waiting is left unclaimed, for its caller, with a *yieldError.
func (r *Runner) claim(ctx context.Context, q *queue, subject string, upTo int64, target TaskID) ([]Task, error) {
	var next TaskID
	var waited bool
	if err := r.db.QueryRow(ctx, `SELECT id, coalesce(waiting_until > now(), false) FROM `+q.tasks+`
		WHERE subject = $1 AND state = 'pending' AND seq <= $2
		AND (not_before IS NULL OR not_before <= now())
		ORDER BY seq LIMIT 1`, subject, upTo).Scan(&next, &waited); err != nil {
		return nil, err
	}
	if waited && next != target {
		return nil, &yieldError{next}
	}
	first := Task{Queue: q.name, Subject: subject}
	err := scanTask(r.db.QueryRow(ctx, `UPDATE `+q.tasks+` SET started = coalesce(started, now())
		WHERE id = $1 AND state = 'pending'
		RETURNING `+claimColumns, next), &first)
	if err != nil {
		return nil, err
	}
	tasks := []Task{first}
	a, ok := r.handler(actionKey{q.name, first.Action})
	if !ok || a.batch == nil || first.sync || a.limit < 2 {
		return tasks, nil
	}

	rows, err := r.db.Query(ctx, `SELECT id, action, sync FROM `+q.tasks+`
		WHERE subject = $1 AND state = 'pending' AND seq > $2 AND seq <= $3
		AND (not_before IS NULL OR not_before <= now())
		ORDER BY seq LIMIT $4`, subject, first.seq, upTo, a.limit-1)
	if err != nil {
		return nil, err
	}
	var ids []TaskID
	for rows.Next() {
		var id TaskID
		var act string
		var sync bool
		if err := rows.Scan(&id, &act, &sync); err != nil {
			rows.Close()
			return nil, err
		}
		if act != first.Action || sync {
			break
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return tasks, nil
	}
	rows, err = r.db.Query(ctx, `UPDATE `+q.tasks+` SET started = coalesce(started, now())
		WHERE id = ANY($1) AND state = 'pending' RETURNING `+claimColumns, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		t := Task{Queue: q.name, Subject: subject}
		if err := scanTask(rows, &t); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(tasks, func(a, b Task) int { return cmp.Compare(a.seq, b.seq) })
	return tasks, nil
}

// failOrRetry records the outcome of tasks whose handler returned herr: each
// async one runs again under the action's retry policy while attempts remain
// and herr is not permanent, and the rest fail.
func (r *Runner) failOrRetry(ctx context.Context, h *hold, a *action, tasks []Task, herr error) error {
	for _, t := range tasks {
		p := a.retry
		if t.sync || p == nil || IsPermanent(herr) || (p.Attempts > 0 && t.Attempt+1 >= p.Attempts) {
			r.log.Warn("task failed", zapTask(t), zapErr(herr))
			if err := r.finish(ctx, h, t, "failed", herr.Error()); err != nil {
				return err
			}
			continue
		}
		r.log.Info("task will be retried", zapTask(t), zapInt("attempt", t.Attempt+1), zapErr(herr))
		if err := r.scheduleRetry(ctx, h, t, p.delay(t.Attempt+1), herr); err != nil {
			return err
		}
	}
	return nil
}

// scheduleRetry delays a failed task for its next attempt, holding its lease
// row locked while it does, as finish does.
func (r *Runner) scheduleRetry(ctx context.Context, h *hold, t Task, delay time.Duration, herr error) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("schedule retry of %s/%s: %w", t.Queue, t.ID, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := lockLease(ctx, tx, h); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE `+h.q.tasks+`
		SET attempts = attempts + 1, error = $2, started = NULL, replaceable = false,
		    not_before = now() + $3 * interval '1 millisecond'
		WHERE id = $1 AND state = 'pending'`, t.ID, herr.Error(), delay.Milliseconds())
	if err != nil {
		return fmt.Errorf("schedule retry of %s/%s: %w", t.Queue, t.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return errNotPending
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("schedule retry of %s/%s: %w", t.Queue, t.ID, err)
	}
	return nil
}
