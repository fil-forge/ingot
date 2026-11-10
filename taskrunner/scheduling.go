package taskrunner

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// BatchHandler runs a batch of tasks with the same subject and action, oldest
// first, and returns one Outcome per task. The batch's writes commit together.
// A task whose Outcome carries an error is retried or failed on its own, under
// its own attempts, in the same commit as its batchmates' success; returning an
// error instead fails or retries the whole batch, writes and all. Expiry is
// decided per task, before the batch runs: a task already past its Expires is
// recorded as expired and left out, so the handler only sees tasks that may
// still start.
type BatchHandler func(ctx context.Context, ex *Exec, tasks []Task) ([]Outcome, error)

// Outcome is one batched task's outcome.
type Outcome struct {
	// Result is the task's result when Err is nil.
	Result []byte
	// Err, when set, fails or retries this task alone, as a handler error
	// would. It does not undo the batch's writes.
	Err error
	// Args, when set with Err, replaces the task's arguments, for its next
	// attempt or for whoever lists it once it has failed: something the
	// attempt learned that the next one needs, such as renewed authority.
	Args []byte
}

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
	// HoldsSubject keeps the due tasks behind a task waiting to retry
	// waiting too, so the subject's order holds across a failure: an add
	// that fails and retries still runs before a remove submitted after it.
	// Without it, the tasks behind run during the backoff. Holding the
	// subject also holds up its sync tasks, so leave it off for an action
	// sharing a subject with sync tasks that should not wait on its retries.
	HoldsSubject bool
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
// replacement keeps whichever of it and a pending replacement with the same
// subject and action that has not yet run has the later NotBefore, and drops
// the other. One that has run, even if only to fail and wait to retry, is
// left in place and the new one is added beside it: its attempt may have
// acted, so it keeps its place and finishes what it started.
func insertSpec(q *queue, id TaskID, s Spec, sync bool) (string, []any) {
	args := []any{id, s.Subject, s.Action, s.NotBefore, s.Expires, s.Args, sync, s.Node}
	if !s.Replace {
		return fmt.Sprintf(insertTask, q.tasks), args
	}
	return `INSERT INTO ` + q.tasks + ` AS t (id, subject, action, not_before, expires, args, sync, node, replaceable)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true)
	ON CONFLICT (subject, action) WHERE state = 'pending' AND replaceable AND started IS NULL
	DO UPDATE SET id = EXCLUDED.id, seq = DEFAULT, created = EXCLUDED.created,
		not_before = EXCLUDED.not_before, expires = EXCLUDED.expires, args = EXCLUDED.args,
		node = EXCLUDED.node
	WHERE t.not_before < EXCLUDED.not_before`, args
}

const cancelTasks = `UPDATE %s SET state = 'cancelled', finished = now()
	WHERE id = ANY($1) AND state = 'pending' AND started IS NULL`

// Cancel cancels pending tasks that have not started, in one statement however
// many there are. Cancelling a task that has started, finished or been
// cancelled does nothing, so a handler whose task can be cancelled must still
// check its own preconditions. A task waiting to retry counts as not started:
// a handler returns a retryable error only before its first effect outside
// Postgres, so its failed attempt left nothing a cancel would strand.
func (r *Runner) Cancel(ctx context.Context, queueName string, ids ...TaskID) error {
	q, ok := r.byName[queueName]
	if !ok {
		return fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	if len(ids) == 0 {
		return nil
	}
	if _, err := r.db.Exec(ctx, fmt.Sprintf(cancelTasks, q.tasks), ids); err != nil {
		return fmt.Errorf("cancel %s: %w", queueName, err)
	}
	r.finished(ids...)
	return nil
}

// CancelTx is Cancel inside tx, so the cancel commits or rolls back with the
// caller's own writes.
func (r *Runner) CancelTx(ctx context.Context, tx pgx.Tx, queueName string, ids ...TaskID) error {
	q, ok := r.byName[queueName]
	if !ok {
		return fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	if len(ids) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(cancelTasks, q.tasks), ids); err != nil {
		return fmt.Errorf("cancel %s: %w", queueName, err)
	}
	return nil
}

// Cancel adds the cancel of pending tasks to the tree's writes, so it commits
// with them. See Runner.Cancel.
func (ex *Exec) Cancel(ctx context.Context, queueName string, ids ...TaskID) error {
	q, ok := ex.tr.r.byName[queueName]
	if !ok {
		return fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	if len(ids) == 0 {
		return nil
	}
	if err := ex.tr.add(ctx, write{sql: fmt.Sprintf(cancelTasks, q.tasks), args: []any{ids}}); err != nil {
		return err
	}
	ex.tr.woken = append(ex.tr.woken, ids...)
	return nil
}

// Pending returns the pending tasks of the action, running or not, on each of
// subjects that has any, oldest first. A subject with none is left out. It
// reads and changes nothing else: it is for a caller that must leave alone
// work the queue still has to do, or wait for it.
func (r *Runner) Pending(ctx context.Context, queueName string, subjects []string, action string) (map[string][]TaskID, error) {
	q, ok := r.byName[queueName]
	if !ok {
		return nil, fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	rows, err := r.db.Query(ctx, `SELECT subject, id FROM `+q.tasks+`
		WHERE subject = ANY($1) AND action = $2 AND state = 'pending'
		ORDER BY seq`, subjects, action)
	if err != nil {
		return nil, fmt.Errorf("pending %s.%s: %w", queueName, action, err)
	}
	out, err := collectBySubject(rows)
	if err != nil {
		return nil, fmt.Errorf("pending %s.%s: %w", queueName, action, err)
	}
	return out, nil
}

// expedite makes the pending tasks of an action on the subjects matching $1
// no longer replacements, and the unstarted ones due now, whether they waited
// for a NotBefore, a retry's backoff or nothing, and returns them all, oldest
// first. A started one keeps its timing but not its replaceability: a retry
// clears its start, and a replacement must not take its ID then. $1 is a LIKE pattern: the subject
// itself with its wildcards escaped, or a prefix of it followed by %, which
// the pending index serves since subjects sort byte by byte. It reads the
// locked rows, which are the current ones, so a replacement that committed
// after the statement began is returned by its own ID.
const expedite = `WITH locked AS (
		SELECT id, subject, seq, started FROM %[1]s
		WHERE subject LIKE $1 AND action = $2 AND state = 'pending'
		FOR UPDATE),
	due AS (
		UPDATE %[1]s t SET replaceable = false,
			not_before = CASE WHEN locked.started IS NULL THEN NULL ELSE t.not_before END,
			retry_at = CASE WHEN locked.started IS NULL THEN NULL ELSE t.retry_at END
		FROM locked WHERE t.id = locked.id)
	SELECT subject, id FROM locked ORDER BY seq`

// Expedite makes the subject's pending tasks of the action due now, whatever
// their NotBefore or retry backoff, and returns all of the action's pending
// tasks on it, by subject and oldest first, for the caller to Wait on or to
// wait behind with a sync task on each subject. A subject ending in * stands for every subject
// that starts with what comes before the *, so a caller can bring forward a
// group of subjects at once, such as a bucket delete running its space's
// pending releases. No task it returns is a replacement any more, due
// already or not, so a later submission cannot push it back or take its ID.
// A task waiting to retry is brought
// forward too, whether or not it holds its subject. One placed on another
// instance still runs only there.
func (r *Runner) Expedite(ctx context.Context, queueName, subject, action string) (map[string][]TaskID, error) {
	q, ok := r.byName[queueName]
	if !ok {
		return nil, fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	rows, err := r.db.Query(ctx, fmt.Sprintf(expedite, q.tasks), subjectPattern(subject), action)
	if err != nil {
		return nil, fmt.Errorf("expedite %s %s.%s: %w", subject, queueName, action, err)
	}
	out, err := collectBySubject(rows)
	if err != nil {
		return nil, fmt.Errorf("expedite %s %s.%s: %w", subject, queueName, action, err)
	}
	r.pokeQueue(q)
	return out, nil
}

// ExpediteTasks makes the given pending tasks of the queue due now, as
// Expedite does those of an action on a subject: whatever their NotBefore or
// retry backoff, and no longer replacements, due already or not. It brings forward exactly these
// tasks, so a caller cleaning up after itself leaves others' tasks of the same
// subject and action at their own times. A task that has started, finished or
// gone is left as it is.
func (r *Runner) ExpediteTasks(ctx context.Context, queueName string, ids ...TaskID) error {
	q, ok := r.byName[queueName]
	if !ok {
		return fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	if len(ids) == 0 {
		return nil
	}
	if _, err := r.db.Exec(ctx, `UPDATE `+q.tasks+` SET not_before = NULL, retry_at = NULL, replaceable = false
		WHERE id = ANY($1) AND state = 'pending' AND started IS NULL`, ids); err != nil {
		return fmt.Errorf("expedite %d %s tasks: %w", len(ids), queueName, err)
	}
	r.pokeQueue(q)
	return nil
}

// collectBySubject reads rows of (subject, id) into IDs by subject, in row
// order, and closes them.
func collectBySubject(rows pgx.Rows) (map[string][]TaskID, error) {
	defer rows.Close()
	out := map[string][]TaskID{}
	for rows.Next() {
		var subject string
		var id TaskID
		if err := rows.Scan(&subject, &id); err != nil {
			return nil, err
		}
		out[subject] = append(out[subject], id)
	}
	return out, rows.Err()
}

// subjectPattern returns the LIKE pattern matching subject, or, when it ends in
// *, every subject starting with what precedes the *.
func subjectPattern(subject string) string {
	prefix, isPrefix := strings.CutSuffix(subject, "*")
	p := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)
	if isPrefix {
		p += "%"
	}
	return p
}

const claimColumns = `seq, id, action, created, not_before, expires, args, sync, attempts, node`

func scanTask(row pgx.Row, t *Task) error {
	return row.Scan(&t.seq, &t.ID, &t.Action, &t.Created, &t.NotBefore, &t.Expires, &t.Args, &t.sync, &t.Attempt, &t.Node)
}

// claim marks the subject's next due task started and returns it. When its
// action batches, the due tasks straight after it with the same action join
// it, up to the action's limit. A next task other
// than target whose Run caller is waiting is left unclaimed, for its caller,
// with a *yieldError.
func (r *Runner) claim(ctx context.Context, q *queue, subject string, upTo int64, target TaskID) ([]Task, error) {
	var next TaskID
	var waited, backingOff bool
	var node *string
	if err := r.db.QueryRow(ctx, `SELECT id, coalesce(waiting_until > now(), false),
		coalesce(retry_at > now(), false), node FROM `+q.tasks+`
		WHERE subject = $1 AND state = 'pending' AND seq <= $2
		AND (not_before IS NULL OR not_before <= now())
		ORDER BY seq LIMIT 1`, subject, upTo).Scan(&next, &waited, &backingOff, &node); err != nil {
		return nil, err
	}
	if backingOff || !r.mayRun(node) {
		// The oldest due task waits to retry, or belongs to another
		// instance, and holds the subject either way.
		return nil, pgx.ErrNoRows
	}
	if waited && next != target {
		return nil, &yieldError{next}
	}
	if target != (TaskID{}) && next != target {
		return nil, &aheadError{next}
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
	limit := 0
	if ok && a.batch != nil {
		limit = a.limit
	}
	if limit < 2 || first.sync {
		return tasks, nil
	}

	rows, err := r.db.Query(ctx, `SELECT id, action,
		sync OR coalesce(retry_at > now(), false) OR node IS DISTINCT FROM $5 FROM `+q.tasks+`
		WHERE subject = $1 AND state = 'pending' AND seq > $2 AND seq <= $3
		AND (not_before IS NULL OR not_before <= now())
		ORDER BY seq LIMIT $4`, subject, first.seq, upTo, limit-1, first.Node)
	if err != nil {
		return nil, err
	}
	var ids []TaskID
	for rows.Next() {
		var id TaskID
		var act string
		// stop is set for a sync task, which never batches, for one waiting
		// to retry, which holds up the rest, and for one placed differently
		// from the batch.
		var stop bool
		if err := rows.Scan(&id, &act, &stop); err != nil {
			rows.Close()
			return nil, err
		}
		if act != first.Action || stop {
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
func (r *Runner) failOrRetry(ctx context.Context, h *hold, a *action, tasks []Task, herr error, start time.Time) error {
	// A batch's outcomes commit together, under its lease, like its success.
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("record failure of %s.%s: %w", tasks[0].Queue, tasks[0].Action, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := lockLease(ctx, tx, h); err != nil {
		return err
	}
	errs := make([]error, len(tasks))
	for i := range errs {
		errs[i] = herr
	}
	var rec failures
	if err := rec.record(ctx, tx, h, a, tasks, errs, nil); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("record failure of %s.%s: %w", tasks[0].Queue, tasks[0].Action, err)
	}
	rec.report(ctx, r, start)
	return nil
}

// failures collects the tasks a commit or failure record failed or retried,
// to log and count once it has committed.
type failures struct {
	failed, retried []Task
	errs            map[TaskID]error
}

// record writes, in tx, each task's failure: a retry when its action's policy
// allows another attempt, otherwise its final failure. errs holds each task's
// error, in the order of tasks, and args, when not nil, each task's
// replacement arguments, nil to keep its own.
func (f *failures) record(ctx context.Context, tx pgx.Tx, h *hold, a *action, tasks []Task, errs []error, args [][]byte) error {
	if f.errs == nil {
		f.errs = map[TaskID]error{}
	}
	for i, t := range tasks {
		herr := errs[i]
		f.errs[t.ID] = herr
		p := a.retry
		var sql string
		// $3 is the replacement arguments, null to keep the task's own.
		var newArgs []byte
		if args != nil {
			newArgs = args[i]
		}
		params := []any{t.ID, herr.Error(), newArgs}
		if t.sync || p == nil || IsPermanent(herr) || (p.Attempts > 0 && t.Attempt+1 >= p.Attempts) {
			sql = `UPDATE ` + h.q.tasks + ` SET state = 'failed', error = $2, finished = now(),
				args = coalesce($3, args)
				WHERE id = $1 AND state = 'pending'`
			f.failed = append(f.failed, t)
		} else {
			// A retry that holds its subject waits in retry_at, which keeps
			// the tasks behind it waiting too; otherwise it waits in
			// not_before like any delayed task. A task that has run is no
			// longer replaceable (see insertSpec).
			col := "not_before"
			if p.HoldsSubject {
				col = "retry_at"
			}
			sql = `UPDATE ` + h.q.tasks + ` SET attempts = attempts + 1, error = $2, started = NULL,
				replaceable = false, args = coalesce($3, args),
				` + col + ` = now() + $4 * interval '1 millisecond'
				WHERE id = $1 AND state = 'pending'`
			params = append(params, p.delay(t.Attempt+1).Milliseconds())
			f.retried = append(f.retried, t)
		}
		tag, err := tx.Exec(ctx, sql, params...)
		if err != nil {
			return fmt.Errorf("record failure of %s/%s: %w", t.Queue, t.ID, err)
		}
		if tag.RowsAffected() == 0 {
			return errNotPending
		}
	}
	return nil
}

// report logs and counts the recorded failures, and wakes the failed tasks'
// waiters. Call it once the record has committed.
func (f *failures) report(ctx context.Context, r *Runner, start time.Time) {
	ids := make([]TaskID, 0, len(f.failed))
	for _, t := range f.failed {
		r.log.Warn("task failed", zapTask(t), zapErr(f.errs[t.ID]))
		ids = append(ids, t.ID)
	}
	for _, t := range f.retried {
		r.log.Info("task will be retried", zapTask(t), zapInt("attempt", t.Attempt+1), zapErr(f.errs[t.ID]))
	}
	r.finished(ids...)
	r.metrics.observeTasks(ctx, f.failed, outcomeFailed, start)
	r.metrics.observeTasks(ctx, f.retried, outcomeRetried, start)
}
