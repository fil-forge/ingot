package taskrunner

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

// TaskID identifies a task within its queue.
type TaskID = uuid.UUID

// Task is a task as its handler sees it.
type Task struct {
	// ID identifies the task within its queue.
	ID TaskID
	// Queue is the queue the task belongs to.
	Queue string
	// Subject is what the task is serialized on: at most one task per
	// (queue, subject) runs at a time.
	Subject string
	// Action selects the handler that runs the task.
	Action string
	// Created orders a subject's due tasks, oldest first.
	Created time.Time
	// NotBefore, when set, is the earliest time the task may run.
	NotBefore *time.Time
	// Expires, when set, is the latest time the task may start. A task found
	// after it is recorded as expired without running.
	Expires *time.Time
	// Args are the action's arguments, which the runner never interprets.
	Args []byte
}

// Spec describes a task to submit or spawn.
type Spec struct {
	// Queue is the queue to add the task to.
	Queue string
	// Subject is what the task is serialized on.
	Subject string
	// Action selects the handler, which must be registered with Handle.
	Action string
	// Args are the action's arguments.
	Args []byte
	// NotBefore, when set, delays the task. Only async tasks may be delayed.
	NotBefore *time.Time
	// Expires, when set, is the latest time the task may start.
	Expires *time.Time
}

// Handler runs one task. ex records the task's writes and spawns, and runs
// its sync children. The returned bytes are the task's result, stored with it
// and returned to whoever runs or waits on it.
type Handler func(ctx context.Context, ex *Exec, task Task) ([]byte, error)

// DB is the database the runner keeps its queues in. *pgxpool.Pool satisfies
// it.
type DB interface {
	Querier
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Querier is what a handler reads through.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// QueueOptions configures one queue.
type QueueOptions struct {
	// Name names the queue: lowercase letters, digits and hyphens, starting
	// with a letter.
	Name string
	// Workers is the most async tasks of the queue this process runs at once.
	// Zero runs none, leaving the queue's async tasks to other processes.
	Workers int
}

// Options configures a Runner.
type Options struct {
	// Schema is the Postgres schema the queue tables are created in.
	Schema string
	// Queues lists the queues in order. A task may run sync children only in
	// queues later in the list.
	Queues []QueueOptions
	// LeaseTTL is how long a subject's lease lasts without renewal. A crashed
	// process's tasks are resumed once it passes. Defaults to 30 seconds.
	LeaseTTL time.Duration
	// PollInterval is how often each queue is checked for due tasks, and the
	// longest a waiter goes between checks of a task in another process.
	// Defaults to 1 second.
	PollInterval time.Duration
	// DrainLimit is the most tasks a worker runs on one subject before
	// releasing its lease. Defaults to 100.
	DrainLimit int
	// CommitAttempts is how many times a tree is run when its final commit
	// fails, before the task is left for resumption. Defaults to 3.
	CommitAttempts int
	// ApplyWritesImmediately runs each write as a handler makes it, in a
	// transaction held open for the whole tree, and routes the handlers'
	// reads through it. It exists for taskrunnertest.CheckOwnWrites and must
	// not be set in production.
	ApplyWritesImmediately bool
	// Logger receives task failures and background errors. Defaults to a
	// no-op logger.
	Logger *zap.Logger
}

var (
	// ErrExpired is returned for a task that passed its Expires before it ran.
	ErrExpired = errors.New("task expired before it ran")
	// ErrQueueOrder is returned when a task runs a sync child in its own queue
	// or an earlier one.
	ErrQueueOrder = errors.New("a sync child must be in a later queue than its parent")
	// ErrRecursion is returned when a task spawns, directly or through its
	// children, a task with its own queue and action.
	ErrRecursion = errors.New("a task may not spawn its own action")
	// ErrConflict is returned when a conditional write affects no rows.
	ErrConflict = errors.New("a conditional write affected no rows")
	// ErrNotCommitted is returned by Run when the task's writes could not be
	// committed. The task stays submitted and is resumed later.
	ErrNotCommitted = errors.New("task did not commit and will be resumed")
	// ErrNotFound is returned by Wait for a task its queue does not hold.
	ErrNotFound = errors.New("task not found")
	// ErrNotStarted is returned by Run before Start or after Stop.
	ErrNotStarted = errors.New("task runner is not running")
)

// HandlerError is the outcome of a task whose handler returned an error, as
// read back from its row.
type HandlerError struct {
	// Queue is the task's queue.
	Queue string
	// Task is the task.s ID.
	Task TaskID
	// Message is the handler's error text.
	Message string
}

func (e *HandlerError) Error() string {
	return fmt.Sprintf("task %s/%s failed: %s", e.Queue, e.Task, e.Message)
}

var queueName = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// Runner runs the tasks of a fixed set of queues.
type Runner struct {
	db     DB
	opts   Options
	log    *zap.Logger
	order  []*queue
	byName map[string]*queue

	handlersMu sync.RWMutex
	handlers   map[actionKey]Handler

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	holds   map[uuid.UUID]*hold
	waiters map[TaskID]map[chan struct{}]struct{}
	freed   map[subjectKey]chan struct{}

	wg sync.WaitGroup
}

type queue struct {
	name    string
	idx     int
	table   string
	tasks   string
	leases  string
	workers int
	poke    chan struct{}
	// inflight is the set of subjects this process's workers have taken or
	// are about to take, guarded by Runner.mu.
	inflight map[string]bool
}

type actionKey struct{ queue, action string }

type subjectKey struct{ queue, subject string }

// New returns a runner over db. Register handlers with Handle, create the
// tables with Migrate, then call Start.
func New(db DB, opts Options) (*Runner, error) {
	if opts.Schema == "" {
		return nil, errors.New("task runner: no schema")
	}
	if len(opts.Queues) == 0 {
		return nil, errors.New("task runner: no queues")
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = 30 * time.Second
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = time.Second
	}
	if opts.DrainLimit <= 0 {
		opts.DrainLimit = 100
	}
	if opts.CommitAttempts <= 0 {
		opts.CommitAttempts = 3
	}
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
	r := &Runner{
		db:       db,
		opts:     opts,
		log:      opts.Logger,
		byName:   map[string]*queue{},
		handlers: map[actionKey]Handler{},
		holds:    map[uuid.UUID]*hold{},
		waiters:  map[TaskID]map[chan struct{}]struct{}{},
		freed:    map[subjectKey]chan struct{}{},
	}
	for i, qo := range opts.Queues {
		if !queueName.MatchString(qo.Name) {
			return nil, fmt.Errorf("task runner: invalid queue name %q", qo.Name)
		}
		if _, dup := r.byName[qo.Name]; dup {
			return nil, fmt.Errorf("task runner: queue %q listed twice", qo.Name)
		}
		table := strings.ReplaceAll(qo.Name, "-", "_")
		q := &queue{
			name:     qo.Name,
			idx:      i,
			table:    table,
			tasks:    pgx.Identifier{opts.Schema, table + "_tasks"}.Sanitize(),
			leases:   pgx.Identifier{opts.Schema, table + "_leases"}.Sanitize(),
			workers:  qo.Workers,
			poke:     make(chan struct{}, 1),
			inflight: map[string]bool{},
		}
		r.order = append(r.order, q)
		r.byName[q.name] = q
	}
	return r, nil
}

// Handle registers the handler for an action of a queue.
func (r *Runner) Handle(queueName, action string, h Handler) error {
	if _, ok := r.byName[queueName]; !ok {
		return fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	if action == "" {
		return errors.New("task runner: empty action")
	}
	r.handlersMu.Lock()
	defer r.handlersMu.Unlock()
	k := actionKey{queueName, action}
	if _, dup := r.handlers[k]; dup {
		return fmt.Errorf("task runner: %s.%s registered twice", queueName, action)
	}
	r.handlers[k] = h
	return nil
}

func (r *Runner) handler(k actionKey) (Handler, bool) {
	r.handlersMu.RLock()
	defer r.handlersMu.RUnlock()
	h, ok := r.handlers[k]
	return h, ok
}

// Start starts the lease renewer and each queue's poller and workers. Tasks
// already in the queues, including ones a crashed process left behind, are
// picked up once due and free.
func (r *Runner) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running {
		return errors.New("task runner: already started")
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	r.cancel = cancel
	r.running = true
	r.wg.Add(1)
	go r.renewLoop(ctx)
	for _, q := range r.order {
		if q.workers > 0 {
			r.wg.Add(1)
			go r.pollLoop(ctx, q)
		}
	}
	return nil
}

// Stop stops the workers and waits for them. A task interrupted by Stop stays
// pending and is resumed by the next process to take its subject.
func (r *Runner) Stop(ctx context.Context) error {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return nil
	}
	r.running = false
	r.cancel()
	r.mu.Unlock()

	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runner) isRunning() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.running
}

func (r *Runner) checkSpec(s Spec) (*queue, error) {
	q, ok := r.byName[s.Queue]
	if !ok {
		return nil, fmt.Errorf("task runner: unknown queue %q", s.Queue)
	}
	if s.Subject == "" {
		return nil, fmt.Errorf("task runner: %s.%s: empty subject", s.Queue, s.Action)
	}
	if _, ok := r.handler(actionKey{s.Queue, s.Action}); !ok {
		return nil, fmt.Errorf("task runner: no handler for %s.%s", s.Queue, s.Action)
	}
	return q, nil
}

const insertTask = `INSERT INTO %s (id, subject, action, not_before, expires, args, sync)
	VALUES ($1, $2, $3, $4, $5, $6, $7)`

// Submit adds an async task and commits it straight away. A worker runs it
// once it is due and its subject is free.
func (r *Runner) Submit(ctx context.Context, s Spec) (TaskID, error) {
	id, err := r.submit(ctx, r.db, s)
	if err == nil {
		r.pokeQueue(r.byName[s.Queue])
	}
	return id, err
}

// SubmitTx adds an async task inside tx, so it commits or rolls back with the
// caller's own writes. Workers find it at their next poll after the commit.
func (r *Runner) SubmitTx(ctx context.Context, tx pgx.Tx, s Spec) (TaskID, error) {
	return r.submit(ctx, tx, s)
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func (r *Runner) submit(ctx context.Context, db execer, s Spec) (TaskID, error) {
	q, err := r.checkSpec(s)
	if err != nil {
		return TaskID{}, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return TaskID{}, err
	}
	if _, err := db.Exec(ctx, fmt.Sprintf(insertTask, q.tasks),
		id, s.Subject, s.Action, s.NotBefore, s.Expires, s.Args, false); err != nil {
		return TaskID{}, fmt.Errorf("submit %s.%s: %w", s.Queue, s.Action, err)
	}
	return id, nil
}

// Run submits a sync task and runs it in the caller's goroutine once its
// subject's lease is free, first running any older due tasks on the subject.
// It returns the task's result, or the handler's error. If ctx ends first the
// task stays submitted and a worker runs it later.
func (r *Runner) Run(ctx context.Context, s Spec) ([]byte, error) {
	if !r.isRunning() {
		return nil, ErrNotStarted
	}
	if s.NotBefore != nil {
		return nil, fmt.Errorf("task runner: %s.%s: a sync task cannot be delayed", s.Queue, s.Action)
	}
	q, err := r.checkSpec(s)
	if err != nil {
		return nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	var seq int64
	var created time.Time
	if err := r.db.QueryRow(ctx, fmt.Sprintf(insertTask+` RETURNING seq, created`, q.tasks),
		id, s.Subject, s.Action, nil, s.Expires, s.Args, true).Scan(&seq, &created); err != nil {
		return nil, fmt.Errorf("submit %s.%s: %w", s.Queue, s.Action, err)
	}
	age := treeAge{created, id}
	key := subjectKey{q.name, s.Subject}

	wait := newBackoff(r.opts.PollInterval)
	for {
		h, _, err := r.tryLease(ctx, q, s.Subject, age)
		if err != nil {
			return nil, err
		}
		if h != nil {
			d := r.drain(ctx, q, h, seq, id)
			r.endHold(ctx, h, d.stop)
			if d.ran {
				return d.res, d.err
			}
			var die *dieError
			switch {
			case d.stop == nil:
				// Another holder ran the task first, or the drain stopped
				// short of it at DrainLimit.
			case errors.As(d.stop, &die):
				r.awaitFree(ctx, die.key, r.opts.PollInterval)
			case errors.Is(d.stop, errNotCommitted):
				return nil, ErrNotCommitted
			default:
				return nil, d.stop
			}
		}
		if res, err, done := r.outcome(ctx, q, id); done {
			return res, err
		}
		if err := r.waitTask(ctx, id, key, wait.next()); err != nil {
			return nil, err
		}
	}
}

// Wait waits for a task to finish and returns its result. A failed task
// returns a *HandlerError, an expired one ErrExpired.
func (r *Runner) Wait(ctx context.Context, queueName string, id TaskID) ([]byte, error) {
	q, ok := r.byName[queueName]
	if !ok {
		return nil, fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	wait := newBackoff(r.opts.PollInterval)
	for {
		ch := r.addWaiter(id)
		res, err, done := r.outcome(ctx, q, id)
		if done {
			r.removeWaiter(id, ch)
			return res, err
		}
		t := time.NewTimer(wait.next())
		select {
		case <-ch:
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			r.removeWaiter(id, ch)
			return nil, ctx.Err()
		}
		t.Stop()
		r.removeWaiter(id, ch)
	}
}

// outcome reads a task's state. done is false while it is pending, or when
// the read itself failed with a cancelled ctx.
func (r *Runner) outcome(ctx context.Context, q *queue, id TaskID) (res []byte, err error, done bool) {
	var state string
	var msg *string
	qerr := r.db.QueryRow(ctx, `SELECT state, result, error FROM `+q.tasks+` WHERE id = $1`, id).Scan(&state, &res, &msg)
	switch {
	case errors.Is(qerr, pgx.ErrNoRows):
		return nil, ErrNotFound, true
	case qerr != nil:
		if ctx.Err() != nil {
			return nil, ctx.Err(), true
		}
		return nil, fmt.Errorf("read task %s/%s: %w", q.name, id, qerr), true
	}
	switch state {
	case "succeeded":
		return res, nil, true
	case "failed":
		m := ""
		if msg != nil {
			m = *msg
		}
		return nil, &HandlerError{Queue: q.name, Task: id, Message: m}, true
	case "expired":
		return nil, ErrExpired, true
	}
	return nil, nil, false
}

func (r *Runner) addWaiter(id TaskID) chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan struct{})
	if r.waiters[id] == nil {
		r.waiters[id] = map[chan struct{}]struct{}{}
	}
	r.waiters[id][ch] = struct{}{}
	return ch
}

func (r *Runner) removeWaiter(id TaskID, ch chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if set := r.waiters[id]; set != nil {
		delete(set, ch)
		if len(set) == 0 {
			delete(r.waiters, id)
		}
	}
}

// finished wakes this process's waiters on the given tasks.
func (r *Runner) finished(ids ...TaskID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range ids {
		for ch := range r.waiters[id] {
			close(ch)
		}
		delete(r.waiters, id)
	}
}

// waitTask waits until the task finishes in this process, the subject's lease
// is released in this process, d passes, or ctx ends.
func (r *Runner) waitTask(ctx context.Context, id TaskID, key subjectKey, d time.Duration) error {
	ch := r.addWaiter(id)
	defer r.removeWaiter(id, ch)
	free := r.freedChan(key)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
	case <-free:
	case <-t.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (r *Runner) pokeQueue(q *queue) {
	select {
	case q.poke <- struct{}{}:
	default:
	}
}

// backoff yields waits that double from a few milliseconds up to max.
type backoff struct{ cur, max time.Duration }

func newBackoff(max time.Duration) *backoff {
	return &backoff{cur: 5 * time.Millisecond, max: max}
}

func (b *backoff) next() time.Duration {
	d := b.cur
	b.cur *= 2
	if b.cur > b.max {
		b.cur = b.max
	}
	return d
}
