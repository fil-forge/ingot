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
	r    *Runner
	root Task
	age  treeAge
	// holds are the leases the tree's sync children took, released after the
	// commit. The root's lease belongs to whoever is draining its subject.
	holds    map[subjectKey]*hold
	rootHold *hold
	writes   []write
	// spawned are the queues that received async tasks, poked after the
	// commit.
	spawned map[*queue]bool
	// children are the IDs of the sync children, whose waiters are woken
	// after the commit.
	children []TaskID
	// tx is the open transaction under ApplyWritesImmediately; cur is the
	// savepoint of the sync child now running, or tx itself.
	tx  pgx.Tx
	cur pgx.Tx
	// failed is the first error a sync child returned. The tree fails with it
	// even if a parent handler carries on.
	failed error
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
	tag, err := tr.cur.Exec(ctx, w.sql, w.args...)
	if err != nil {
		return err
	}
	if w.conditional && tag.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}

// Spawn adds an async task, committed with the tree. It runs once the tree
// has committed, it is due and its subject is free.
func (ex *Exec) Spawn(ctx context.Context, s Spec) (TaskID, error) {
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
	if err := ex.tr.add(ctx, write{
		sql:  fmt.Sprintf(insertTask, q.tasks),
		args: []any{id, s.Subject, s.Action, s.NotBefore, s.Expires, s.Args, false},
	}); err != nil {
		return TaskID{}, err
	}
	ex.tr.spawned[q] = true
	return id, nil
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
	q, err := tr.r.checkSpec(s)
	if err != nil {
		return nil, err
	}
	if s.NotBefore != nil {
		return nil, fmt.Errorf("task runner: %s.%s: a sync task cannot be delayed", s.Queue, s.Action)
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

	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	child := Task{ID: id, Queue: s.Queue, Subject: s.Subject, Action: s.Action, Created: time.Now(), Expires: s.Expires, Args: s.Args}
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
	h, _ := tr.r.handler(k)
	res, err := callHandler(ctx, h, cex, child)
	if err == nil {
		err = tr.add(ctx, write{
			sql: `INSERT INTO ` + q.tasks + ` (id, subject, action, expires, args, sync, state, result, finished)
				VALUES ($1, $2, $3, $4, $5, true, 'succeeded', $6, now())`,
			args: []any{id, s.Subject, s.Action, s.Expires, s.Args, res},
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
	tr.children = append(tr.children, id)
	return res, nil
}

// lease takes a sync child's subject for the rest of the tree. It waits for a
// younger holder and gives way to an older one (see dieError).
func (tr *tree) lease(ctx context.Context, q *queue, subject string) error {
	key := subjectKey{q.name, subject}
	if _, ok := tr.holds[key]; ok {
		return nil
	}
	wait := newBackoff(tr.r.opts.PollInterval)
	for {
		h, holder, err := tr.r.tryLease(ctx, q, subject, tr.age)
		if err != nil {
			return err
		}
		if h != nil {
			tr.holds[key] = h
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

// callHandler runs h, turning a panic into an error.
func callHandler(ctx context.Context, h Handler, ex *Exec, t Task) (res []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("handler %s.%s panicked: %v", t.Queue, t.Action, p)
		}
	}()
	return h(ctx, ex, t)
}

// commit runs the tree's write list, its children's rows and the root's
// completion in one transaction, after checking the tree still holds every
// lease it took.
func (tr *tree) commit(ctx context.Context, result []byte) error {
	tx := tr.tx
	if tx == nil {
		var err error
		if tx, err = tr.r.db.Begin(ctx); err != nil {
			return err
		}
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	fence := append([]*hold{tr.rootHold}, mapValues(tr.holds)...)
	for _, h := range fence {
		var one int
		err := tx.QueryRow(ctx, `SELECT 1 FROM `+h.q.leases+` WHERE subject = $1 AND owner = $2 FOR UPDATE`,
			h.subject, h.token).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("lease on %s/%s lost before commit", h.q.name, h.subject)
		}
		if err != nil {
			return err
		}
	}
	for _, w := range tr.writes {
		tag, err := tx.Exec(ctx, w.sql, w.args...)
		if err != nil {
			return err
		}
		if w.conditional && tag.RowsAffected() == 0 {
			return ErrConflict
		}
	}
	tag, err := tx.Exec(ctx, `UPDATE `+tr.rootHold.q.tasks+` SET state = 'succeeded', result = $2, finished = now()
		WHERE id = $1 AND state = 'pending'`, tr.root.ID, result)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("task %s/%s is no longer pending", tr.root.Queue, tr.root.ID)
	}
	return tx.Commit(ctx)
}

func mapValues[K comparable, V any](m map[K]V) []V {
	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

// runRoot runs a due task under h, its subject's lease. It returns the task's
// result and error once the task has an outcome; stop is set instead when it
// has none yet: the tree gave way to an older one (*dieError), ctx ended, or
// the commit kept failing (errNotCommitted).
func (r *Runner) runRoot(ctx context.Context, h *hold, t Task) (res []byte, err error, stop error) {
	age := treeAge{t.Created, t.ID}
	if err := r.setAge(ctx, h, age); err != nil {
		return nil, nil, err
	}
	if t.Expires != nil && !t.Expires.After(time.Now()) {
		if err := r.finish(ctx, h, t, "expired", ""); err != nil {
			return nil, nil, err
		}
		return nil, ErrExpired, nil
	}
	hd, ok := r.handler(actionKey{t.Queue, t.Action})
	if !ok {
		msg := fmt.Sprintf("no handler for %s.%s", t.Queue, t.Action)
		if err := r.finish(ctx, h, t, "failed", msg); err != nil {
			return nil, nil, err
		}
		return nil, &HandlerError{Queue: t.Queue, Task: t.ID, Message: msg}, nil
	}

	for attempt := 1; ; attempt++ {
		tr := &tree{r: r, root: t, age: age, holds: map[subjectKey]*hold{}, rootHold: h, spawned: map[*queue]bool{}}
		if r.opts.ApplyWritesImmediately {
			tx, err := r.db.Begin(ctx)
			if err != nil {
				return nil, nil, err
			}
			tr.tx, tr.cur = tx, tx
		}
		ex := &Exec{tr: tr, q: h.q, task: t, path: []actionKey{{t.Queue, t.Action}}}
		res, herr := callHandler(ctx, hd, ex, t)
		// A failed child fails the tree even if its parent carried on, and
		// giving way to an older tree ends the run however the handler
		// reported it.
		var die *dieError
		if tr.failed != nil && (herr == nil || errors.As(tr.failed, &die)) {
			herr = tr.failed
		}
		if herr != nil {
			if tr.tx != nil {
				_ = tr.tx.Rollback(context.WithoutCancel(ctx))
			}
			tr.releaseChildren(ctx)
			if errors.As(herr, &die) {
				r.log.Debug("task gave way to an older one", zapTask(t), zapErr(die))
				return nil, nil, die
			}
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			r.log.Warn("task failed", zapTask(t), zapErr(herr))
			if err := r.finish(ctx, h, t, "failed", herr.Error()); err != nil {
				return nil, nil, err
			}
			return nil, herr, nil
		}
		cerr := tr.commit(ctx, res)
		tr.releaseChildren(ctx)
		if cerr == nil {
			r.finished(append(tr.children, t.ID)...)
			for q := range tr.spawned {
				r.pokeQueue(q)
			}
			return res, nil, nil
		}
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		r.log.Warn("task commit failed", zapTask(t), zapInt("attempt", attempt), zapErr(cerr))
		if attempt >= r.opts.CommitAttempts {
			return nil, nil, errNotCommitted
		}
	}
}

// finish records a task's outcome without running its tree, if this process
// still holds its subject.
func (r *Runner) finish(ctx context.Context, h *hold, t Task, state, msg string) error {
	_, err := r.db.Exec(ctx, `UPDATE `+h.q.tasks+` SET state = $2, error = NULLIF($3, ''), finished = now()
		WHERE id = $1 AND state = 'pending'
		AND EXISTS (SELECT 1 FROM `+h.q.leases+` WHERE subject = $4 AND owner = $5)`,
		t.ID, state, msg, h.subject, h.token)
	if err != nil {
		return fmt.Errorf("record task %s/%s as %s: %w", t.Queue, t.ID, state, err)
	}
	r.finished(t.ID)
	return nil
}
