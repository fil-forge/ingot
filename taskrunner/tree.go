package taskrunner

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// write is one statement in a tree's write list.
type write struct {
	sql  string
	args []any
	// conditional fails the tree's commit when the statement affects no rows.
	conditional bool
}

// tree is one run of a root task and its sync children.
type tree struct {
	r *Runner
	// roots are the tasks the tree completes: one, or a batch.
	roots []Task
	age   treeAge
	// holds are the leases the tree's sync children took, released after the
	// commit. The root's lease belongs to whoever is draining its subject.
	holds    map[subjectKey]*hold
	rootHold *hold
	writes   []write
	// spawned are the queues that received async tasks, poked after the
	// commit.
	spawned map[*queue]bool
	// woken are the IDs of the sync children and cancelled tasks, whose
	// waiters are woken after the commit.
	woken []TaskID
	// childRuns are the sync children whose handlers ran, reported to the
	// metrics once the tree has an outcome.
	childRuns []childRun
	// tx is the open transaction under applyWritesImmediately; cur is the
	// savepoint of the sync child now running, or tx itself.
	tx  pgx.Tx
	cur pgx.Tx
	// failed is the first error a sync child returned. The tree fails with it
	// even if a parent handler carries on.
	failed error
	// deferred is the first error a write hit under applyWritesImmediately.
	// It fails the commit, as the same write would with writes held, rather
	// than reaching the handler.
	deferred error
}

// Exec is a handler's view of its tree: it records the handler's writes and
// spawns, and runs its sync children.
type Exec struct {
	tr   *tree
	q    *queue
	task Task
	// path is the (queue, action) of this task and every ancestor.
	path []actionKey
}

// DB returns what the handler reads through. Rows the tree has written are
// not visible through it until the tree commits.
func (ex *Exec) DB() Querier {
	if ex.tr.cur != nil {
		return ex.tr.cur
	}
	return ex.tr.r.db
}

// Write adds a statement to the tree's write list.
func (ex *Exec) Write(ctx context.Context, sql string, args ...any) error {
	return ex.tr.add(ctx, write{sql: sql, args: args})
}

// WriteConditional adds a statement that must affect at least one row. When
// it affects none, the commit fails, and the tree runs again from scratch.
func (ex *Exec) WriteConditional(ctx context.Context, sql string, args ...any) error {
	return ex.tr.add(ctx, write{sql: sql, args: args, conditional: true})
}

func (tr *tree) add(ctx context.Context, w write) error {
	if tr.cur == nil {
		tr.writes = append(tr.writes, w)
		return nil
	}
	if tr.deferred != nil {
		return nil
	}
	// Each write runs in its own savepoint, so one that fails leaves the
	// transaction usable for the handler's later reads.
	sp, err := tr.cur.Begin(ctx)
	if err != nil {
		return err
	}
	tag, err := sp.Exec(ctx, w.sql, w.args...)
	if err == nil && w.conditional && tag.RowsAffected() == 0 {
		err = ErrConflict
	}
	if err != nil {
		_ = sp.Rollback(ctx)
		tr.deferred = err
		return nil
	}
	return sp.Commit(ctx)
}

// Submit adds an async task, committed with the tree. It runs once the tree
// has committed, it is due and its subject is free.
func (ex *Exec) Submit(ctx context.Context, s Spec) (TaskID, error) {
	q, err := ex.tr.r.checkSpec(s)
	if err != nil {
		return TaskID{}, err
	}
	if slices.Contains(ex.path, actionKey{s.Queue, s.Action}) {
		return TaskID{}, fmt.Errorf("%s.%s: %w", s.Queue, s.Action, ErrRecursion)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return TaskID{}, err
	}
	sql, args := insertSpec(q, id, s, false)
	if err := ex.tr.add(ctx, write{sql: sql, args: args}); err != nil {
		return TaskID{}, err
	}
	ex.tr.spawned[q] = true
	return id, nil
}

// SubmitAll adds async tasks, committed with the tree, in one statement for
// each queue they are on however many there are, and returns their IDs in
// the order of specs. Of two replacements in specs with the same subject and
// action, the one with the later NotBefore is kept, as it would be had they
// been submitted one by one, and both are given its ID.
func (ex *Exec) SubmitAll(ctx context.Context, specs []Spec) ([]TaskID, error) {
	for _, s := range specs {
		if slices.Contains(ex.path, actionKey{s.Queue, s.Action}) {
			return nil, fmt.Errorf("%s.%s: %w", s.Queue, s.Action, ErrRecursion)
		}
	}
	return ex.tr.r.submitAll(specs, func(q *queue, sql string, args []any) error {
		if err := ex.tr.add(ctx, write{sql: sql, args: args}); err != nil {
			return err
		}
		ex.tr.spawned[q] = true
		return nil
	})
}

// Run runs a sync child inline, once its subject's lease is taken, and
// returns its result. The child's writes join the tree's, and its subject
// stays leased until the tree commits. A child's error fails the whole tree.
func (ex *Exec) Run(ctx context.Context, s Spec) ([]byte, error) {
	res, err := ex.run(ctx, s)
	if err != nil && ex.tr.failed == nil {
		ex.tr.failed = err
	}
	return res, err
}

func (ex *Exec) run(ctx context.Context, s Spec) ([]byte, error) {
	tr := ex.tr
	q, err := tr.r.checkSync(s)
	if err != nil {
		return nil, err
	}
	if q.idx <= ex.q.idx {
		return nil, fmt.Errorf("%s.%s from %s.%s: %w", s.Queue, s.Action, ex.q.name, ex.task.Action, ErrQueueOrder)
	}
	k := actionKey{s.Queue, s.Action}
	if slices.Contains(ex.path, k) {
		return nil, fmt.Errorf("%s.%s: %w", s.Queue, s.Action, ErrRecursion)
	}
	if s.Expires != nil && !s.Expires.After(time.Now()) {
		return nil, ErrExpired
	}
	if err := tr.lease(ctx, q, s.Subject); err != nil {
		return nil, err
	}
	// The wait for the lease may have outlasted the child's Expires.
	if s.Expires != nil && !s.Expires.After(time.Now()) {
		return nil, ErrExpired
	}
	// Nor may a child start once renewal has found the root's lease taken
	// over: the tree cannot commit, and the child's effects would only be
	// repeated when another consumer runs it. A loss renewal has not seen
	// yet is caught by the commit.
	if tr.rootHold.lost.Load() {
		return nil, errLeaseLost
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	child := Task{ID: id, Queue: s.Queue, Subject: s.Subject, Action: s.Action, Created: time.Now(), Expires: s.Expires, Args: s.Args, Node: s.Node}
	cex := &Exec{tr: tr, q: q, task: child, path: append(slices.Clone(ex.path), k)}

	// A failed child's writes are dropped, whatever its parent does next.
	mark := len(tr.writes)
	parent := tr.cur
	if parent != nil {
		sp, err := parent.Begin(ctx)
		if err != nil {
			return nil, err
		}
		tr.cur = sp
	}
	a, _ := tr.r.handler(k)
	start := time.Now()
	res, err := callHandler(ctx, a.handler, cex, child)
	defer func() { tr.childRuns = append(tr.childRuns, childRun{child, time.Since(start), err}) }()
	if err == nil {
		err = tr.add(ctx, write{
			sql: `INSERT INTO ` + q.tasks + ` (id, subject, action, expires, args, sync, node, state, result, finished)
				VALUES ($1, $2, $3, $4, $5, true, $6, 'succeeded', $7, now())`,
			args: []any{id, s.Subject, s.Action, s.Expires, s.Args, s.Node, res},
		})
	}
	if parent != nil {
		if err == nil {
			err = tr.cur.Commit(ctx)
		} else {
			_ = tr.cur.Rollback(ctx)
		}
		tr.cur = parent
	} else if err != nil {
		tr.writes = tr.writes[:mark]
	}
	if err != nil {
		return nil, err
	}
	tr.woken = append(tr.woken, id)
	return res, nil
}

// lease takes a sync child's subject for the rest of the tree. It waits for a
// younger holder and gives way to an older one (see dieError).
func (tr *tree) lease(ctx context.Context, q *queue, subject string) error {
	key := subjectKey{q.name, subject}
	if h, ok := tr.holds[key]; ok {
		if h.lost.Load() {
			return &childLostError{key}
		}
		return nil
	}
	start := time.Now()
	wait := newBackoff(tr.r.opts.PollInterval)
	for {
		h, holder, err := tr.r.tryLease(ctx, q, subject, tr.age)
		if err != nil {
			return err
		}
		if h != nil {
			tr.holds[key] = h
			tr.r.metrics.observeLeaseWait(ctx, q, start)
			return nil
		}
		if holder != nil && holder.olderThan(tr.age) {
			return &dieError{key}
		}
		tr.r.awaitFree(ctx, key, wait.next())
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (tr *tree) releaseChildren(ctx context.Context) {
	for k, h := range tr.holds {
		tr.r.release(ctx, h)
		delete(tr.holds, k)
	}
}

// childRun is one sync child's run, and the error it ended with.
type childRun struct {
	task Task
	d    time.Duration
	err  error
}

// observeChildren reports the tree's sync children once it has an outcome:
// all of them succeeded when it committed, and the one that failed it when it
// failed. A tree that restarts reports none of its run.
func (tr *tree) observeChildren(ctx context.Context, committed bool) {
	for _, c := range tr.childRuns {
		switch {
		case committed:
			tr.r.metrics.observeChild(ctx, c.task, outcomeSucceeded, c.d)
		case c.err != nil:
			tr.r.metrics.observeChild(ctx, c.task, outcomeFailed, c.d)
		}
	}
}

// childLostError restarts a tree whose earlier child's lease was taken over
// before a later child on the same subject could start. The tree runs again
// from scratch with fresh leases.
type childLostError struct{ key subjectKey }

func (e *childLostError) Error() string {
	return fmt.Sprintf("lease on %s/%s lost before a child could run under it", e.key.queue, e.key.subject)
}

// callHandler runs h, turning a panic into an error.
func callHandler(ctx context.Context, h Handler, ex *Exec, t Task) (res []byte, err error) {
	defer recoverHandler(t, &err)
	return h(ctx, ex, t)
}

// callBatch runs a batch handler, turning a panic into an error.
func callBatch(ctx context.Context, h BatchHandler, ex *Exec, tasks []Task) (res []Outcome, err error) {
	defer recoverHandler(tasks[0], &err)
	res, err = h(ctx, ex, tasks)
	if err == nil && len(res) != len(tasks) {
		err = fmt.Errorf("batch handler %s.%s returned %d outcomes for %d tasks", tasks[0].Queue, tasks[0].Action, len(res), len(tasks))
	}
	return res, err
}

func recoverHandler(t Task, err *error) {
	if p := recover(); p != nil {
		*err = fmt.Errorf("handler %s.%s panicked: %v", t.Queue, t.Action, p)
	}
}

// commit runs the tree's write list, its children's rows and the roots'
// outcomes in one transaction, after checking the tree still holds every
// lease it took. A root whose outcome carries an error is retried or failed
// there; it returns those, to report once the commit has landed.
func (tr *tree) commit(ctx context.Context, a *action, outcomes []Outcome) (*failures, error) {
	tx := tr.tx
	if tx == nil {
		var err error
		if tx, err = tr.r.db.Begin(ctx); err != nil {
			return nil, err
		}
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if tr.deferred != nil {
		return nil, tr.deferred
	}
	lockRoot := lockLease
	if tr.rootHold.releaseOnCommit {
		lockRoot = deleteLease
	}
	if err := lockRoot(ctx, tx, tr.rootHold, tr.r.opts.LeaseTTL); err != nil {
		return nil, err
	}
	for _, h := range tr.holds {
		if err := lockLease(ctx, tx, h, tr.r.opts.LeaseTTL); errors.Is(err, errLeaseLost) {
			// A child's subject can be leased afresh when the tree runs again.
			return nil, fmt.Errorf("lease on %s/%s lost before commit", h.q.name, h.subject)
		} else if err != nil {
			return nil, err
		}
	}
	for _, w := range tr.writes {
		tag, err := tx.Exec(ctx, w.sql, w.args...)
		if err != nil {
			return nil, err
		}
		if w.conditional && tag.RowsAffected() == 0 {
			return nil, ErrConflict
		}
	}
	var failedTasks []Task
	var failedErrs []error
	var failedArgs [][]byte
	for i, t := range tr.roots {
		if err := outcomes[i].Err; err != nil {
			failedTasks = append(failedTasks, t)
			failedErrs = append(failedErrs, err)
			failedArgs = append(failedArgs, outcomes[i].Args)
			continue
		}
		tag, err := tx.Exec(ctx, `UPDATE `+tr.rootHold.q.tasks+` SET state = 'succeeded', result = $2, finished = now()
			WHERE id = $1 AND state = 'pending'`, t.ID, outcomes[i].Result)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() == 0 {
			return nil, fmt.Errorf("task %s/%s is no longer pending", t.Queue, t.ID)
		}
	}
	rec := &failures{}
	if len(failedTasks) > 0 {
		if err := rec.record(ctx, tx, tr.rootHold, a, failedTasks, failedErrs, failedArgs); err != nil {
			return nil, err
		}
	}
	root := tr.rootHold
	if !root.releaseOnCommit {
		return rec, tx.Commit(ctx)
	}
	// The commit deletes the lease row: stop renewing the hold first, as
	// release does, so the renewer never finds the row gone and takes the
	// lease for lost. A failed commit leaves the row, and the hold renewed.
	tr.r.forget(root)
	if err := tx.Commit(ctx); err != nil {
		tr.r.mu.Lock()
		tr.r.holds[root.token] = root
		tr.r.mu.Unlock()
		return nil, err
	}
	return rec, nil
}

// lockLease locks h's lease row for the rest of tx, and extends it by a
// lease TTL from now, or returns errLeaseLost when another consumer owns it
// now or it has expired, which leaves it free for one to take. Renewal skips
// a row a commit holds locked (renew), so the extension is what keeps a long
// commit's lease from lapsing under it.
func lockLease(ctx context.Context, tx pgx.Tx, h *hold, ttl time.Duration) error {
	var one int
	err := tx.QueryRow(ctx, `UPDATE `+h.q.leases+` SET expires = now() + $3 * interval '1 millisecond'
		WHERE subject = $1 AND owner = $2 AND expires >= now() RETURNING 1`,
		h.subject, h.token, ttl.Milliseconds()).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		h.lost.Store(true)
		return errLeaseLost
	}
	return err
}

// deleteLease is lockLease for a hold that ends with tx: it deletes h's
// lease row, which stays locked until tx ends, so the release commits with
// the task. It returns errLeaseLost as lockLease does.
func deleteLease(ctx context.Context, tx pgx.Tx, h *hold, _ time.Duration) error {
	var one int
	err := tx.QueryRow(ctx, `DELETE FROM `+h.q.leases+`
		WHERE subject = $1 AND owner = $2 AND expires >= now() RETURNING 1`,
		h.subject, h.token).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		h.lost.Store(true)
		return errLeaseLost
	}
	return err
}

// runRoot runs due tasks under h, their subject's lease: one task, or a batch
// of one action's tasks. It returns the result and error of the first task
// once it has an outcome; stop is set instead when it has none yet: the tree
// gave way to an older one (*dieError), ctx ended, or the commit kept failing
// (errNotCommitted).
func (r *Runner) runRoot(ctx context.Context, h *hold, tasks []Task) (res []byte, err error, stop error) {
	first := tasks[0]
	age := treeAge{first.Created, first.ID}
	if h.claimed && h.age.id == age.id && h.age.created.Equal(age.created) {
		// The statement that took the lease claimed this task and recorded
		// its age: the lease is this hold's, and says so.
		h.claimed = false
	} else if err := r.setAge(ctx, h, age); err != nil {
		return nil, nil, err
	}
	live := tasks
	start := time.Now()
	a, ok := r.handler(actionKey{first.Queue, first.Action})
	if !ok {
		// Only a registered batching action forms a batch, so this is one
		// task; it is recorded the same way as any failure all the same.
		msg := fmt.Sprintf("no handler for %s.%s", first.Queue, first.Action)
		if err := r.failOrRetry(ctx, h, &action{}, live, errors.New(msg), start); err != nil {
			return nil, nil, err
		}
		return nil, &HandlerError{Queue: first.Queue, Task: first.ID, Message: msg}, nil
	}

	for attempt := 1; ; attempt++ {
		if h.lost.Load() {
			return nil, nil, errLeaseLost
		}
		// Each attempt starts the tree afresh, so each checks its tasks may
		// still start.
		var next, expired []Task
		for _, t := range live {
			if t.Expires != nil && !t.Expires.After(time.Now()) {
				expired = append(expired, t)
				continue
			}
			next = append(next, t)
		}
		if err := r.finish(ctx, h, expired, "expired", ""); err != nil {
			return nil, nil, err
		}
		r.metrics.observeTasks(ctx, expired, outcomeExpired, start)
		if live = next; len(live) == 0 {
			return nil, ErrExpired, nil
		}
		tr := &tree{r: r, roots: live, age: age, holds: map[subjectKey]*hold{}, rootHold: h, spawned: map[*queue]bool{}}
		if r.opts.applyWritesImmediately {
			tx, err := r.db.Begin(ctx)
			if err != nil {
				return nil, nil, err
			}
			tr.tx, tr.cur = tx, tx
		}
		ex := &Exec{tr: tr, q: h.q, task: live[0], path: []actionKey{{first.Queue, first.Action}}}
		var outcomes []Outcome
		var herr error
		if a.batch != nil {
			outcomes, herr = callBatch(ctx, a.batch, ex, live)
		} else {
			var res []byte
			res, herr = callHandler(ctx, a.handler, ex, live[0])
			outcomes = []Outcome{{Result: res}}
		}
		// A failed child fails the tree even if its parent carried on, and
		// giving way to an older tree ends the run however the handler
		// reported it.
		var die *dieError
		var lost *childLostError
		if tr.failed != nil && (herr == nil || errors.As(tr.failed, &die) || errors.As(tr.failed, &lost) || errors.Is(tr.failed, errLeaseLost)) {
			herr = tr.failed
		}
		if herr != nil {
			if tr.tx != nil {
				_ = tr.tx.Rollback(context.WithoutCancel(ctx))
			}
			tr.releaseChildren(ctx)
			if errors.As(herr, &die) {
				r.log.Debug("task gave way to an older one", zapTask(first), zapErr(die))
				return nil, nil, die
			}
			if errors.Is(herr, errLeaseLost) {
				r.log.Warn("task's lease was taken over before a child could start", zapTask(first))
				return nil, nil, errLeaseLost
			}
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			if errors.As(herr, &lost) {
				r.log.Warn("task's child lost its lease", zapTask(first), zapInt("attempt", attempt), zapErr(lost))
				if attempt >= r.opts.CommitAttempts {
					return nil, nil, errNotCommitted
				}
				continue
			}
			if err := r.failOrRetry(ctx, h, a, live, herr, start); err != nil {
				return nil, nil, err
			}
			tr.observeChildren(ctx, false)
			return nil, herr, nil
		}
		rec, cerr := tr.commit(ctx, a, outcomes)
		tr.releaseChildren(ctx)
		if cerr == nil {
			if h.releaseOnCommit {
				r.releasedInCommit(h)
			}
			woken := tr.woken
			var succeeded []Task
			for i, t := range live {
				if outcomes[i].Err == nil {
					woken = append(woken, t.ID)
					succeeded = append(succeeded, t)
				}
			}
			r.finished(woken...)
			for q := range tr.spawned {
				r.pokeQueue(q)
			}
			r.metrics.observeTasks(ctx, succeeded, outcomeSucceeded, start)
			rec.report(ctx, r, start)
			tr.observeChildren(ctx, true)
			return outcomes[0].Result, outcomes[0].Err, nil
		}
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		if errors.Is(cerr, errLeaseLost) {
			r.log.Warn("task's lease was taken over before its commit", zapTask(first))
			return nil, nil, errLeaseLost
		}
		r.log.Warn("task commit failed", zapTask(first), zapInt("attempt", attempt), zapErr(cerr))
		if attempt >= r.opts.CommitAttempts {
			return nil, nil, errNotCommitted
		}
	}
}

// finish records one outcome for tasks without running them, in one
// transaction that holds their lease row locked. It returns errLeaseLost when
// another consumer owns the subject now, and errNotPending when any of the
// tasks already has an outcome; either way it records nothing.
func (r *Runner) finish(ctx context.Context, h *hold, tasks []Task, state, msg string) error {
	if len(tasks) == 0 {
		return nil
	}
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("record %s.%s as %s: %w", tasks[0].Queue, tasks[0].Action, state, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := lockLease(ctx, tx, h, r.opts.LeaseTTL); err != nil {
		return err
	}
	ids := make([]TaskID, len(tasks))
	for i, t := range tasks {
		ids[i] = t.ID
	}
	tag, err := tx.Exec(ctx, `UPDATE `+h.q.tasks+` SET state = $2, error = NULLIF($3, ''), finished = now()
		WHERE id = ANY($1) AND state = 'pending'`, ids, state, msg)
	if err != nil {
		return fmt.Errorf("record %s.%s as %s: %w", tasks[0].Queue, tasks[0].Action, state, err)
	}
	if tag.RowsAffected() != int64(len(tasks)) {
		return errNotPending
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("record %s.%s as %s: %w", tasks[0].Queue, tasks[0].Action, state, err)
	}
	r.finished(ids...)
	return nil
}
