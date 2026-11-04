package taskrunner

import (
	"cmp"
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
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
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
	// Created is when the task was queued. A tree is as old as its first
	// task, and of two trees contending for the same subjects the older wins.
	Created time.Time
	// NotBefore, when set, is the earliest time the task may run.
	NotBefore *time.Time
	// Expires, when set, is the latest time the task may start. A task found
	// after it is recorded as expired without running.
	Expires *time.Time
	// Args are the action's arguments, which the runner never interprets.
	Args []byte
	// Attempt is how many earlier runs of the task failed and were retried.
	Attempt int
	// Node, when set, is the only instance that may run the task.
	Node *string

	seq  int64
	sync bool
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
	// Node, when set, places the task on one instance, the runner whose
	// Options.Node matches. Other instances never run it, and while it is its
	// subject's oldest due task they run none of the tasks behind it either.
	// Until its NotBefore passes it holds up nothing, as any delayed task. A
	// task that touches one instance's local state is placed there.
	Node *string
	// Replace submits the task as a replacement, which needs a NotBefore: if a
	// replacement with the same subject and action is pending and has not
	// started, whichever has the later NotBefore is kept and the other is
	// dropped.
	Replace bool
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
	// Name names the queue: up to 40 lowercase letters, digits and hyphens,
	// starting with a letter.
	Name string
	// Workers is the most async tasks of the queue this process runs at once.
	// Zero runs none, leaving the queue's async tasks to other processes.
	Workers int
	// Retention is how long a task that succeeded, expired, was cancelled or
	// was resubmitted is kept after it finished. Defaults to 24 hours, and
	// must not be negative.
	Retention time.Duration
	// FailedRetention is how long a failed task is kept after it failed, for
	// inspection and replay. Zero keeps it until it is resubmitted, and it
	// must not be negative.
	FailedRetention time.Duration
}

// Options configures a Runner.
type Options struct {
	// Schema is the Postgres schema the queue tables are created in.
	Schema string
	// Queues lists the queues in order. A task may run sync children only in
	// queues later in the list.
	Queues []QueueOptions
	// LeaseTTL is how long a subject's lease lasts without renewal. A crashed
	// process's tasks are resumed once it passes. Defaults to 30 seconds, and
	// must be at least a millisecond.
	LeaseTTL time.Duration
	// PollInterval is how often each queue is checked for due tasks, and the
	// longest a waiter goes between checks of a task in another process.
	// Defaults to 1 second.
	PollInterval time.Duration
	// RetentionInterval is how often finished tasks past their queue's
	// retention are deleted. Defaults to 1 minute.
	RetentionInterval time.Duration
	// DrainLimit is the most handler runs a worker makes on one subject
	// before releasing its lease. A batch is one run, whatever its size.
	// Defaults to 100.
	DrainLimit int
	// CommitAttempts is how many times a tree is run when its final commit
	// fails, before the task is left for resumption. Defaults to 3.
	CommitAttempts int
	// ApplyWritesImmediately runs each write as a handler makes it, in a
	// transaction held open for the whole tree, and routes the handlers'
	// reads through it. It exists for taskrunnertest.CheckOwnWrites and must
	// not be set in production.
	ApplyWritesImmediately bool
	// Node names this instance for placement: the runner runs tasks placed
	// on it and tasks placed nowhere. Empty runs only tasks placed nowhere.
	Node string
	// Logger receives task failures and background errors. Defaults to a
	// no-op logger.
	Logger *zap.Logger
	// MeterProvider receives the runner's metrics. Defaults to the global
	// OpenTelemetry provider, which is a no-op until one is installed.
	MeterProvider metric.MeterProvider
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
	// ErrNotFound is returned by Wait for a task its queue does not hold.
	ErrNotFound = errors.New("task not found")
	// ErrSyncTask is returned by Resubmit for a task that ran sync: its
	// caller has had its failure, and a handler written for a caller may
	// depend on what only the caller holds.
	ErrSyncTask = errors.New("a sync task cannot be resubmitted")
	// ErrOtherNode is returned when a sync task is placed on another
	// instance, which is the only one that may run it.
	ErrOtherNode = errors.New("task is placed on another instance")
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

// queueName bounds names so that every identifier derived from one, such as
// "<name>_tasks_pending", fits Postgres's 63-byte limit.
var queueName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,39}$`)

// Runner runs the tasks of a fixed set of queues.
type Runner struct {
	db      DB
	opts    Options
	log     *zap.Logger
	metrics *metrics
	order   []*queue
	byName  map[string]*queue

	handlersMu sync.RWMutex
	handlers   map[actionKey]*action

	mu      sync.Mutex
	running bool
	// ctx is cancelled by Stop, ending workers and inline runs alike.
	ctx    context.Context
	cancel context.CancelFunc
	holds  map[uuid.UUID]*hold
	// waiting maps each sync task whose caller is waiting in Run to its queue,
	// so the renewer keeps its claim live.
	waiting map[TaskID]*queue
	waiters map[TaskID]map[chan struct{}]struct{}
	freed   map[subjectKey]*freeSub

	// wg counts pollers and workers, inline the Run calls in progress.
	wg     sync.WaitGroup
	inline sync.WaitGroup
	// renewDone closes when the renewer has stopped.
	renewDone chan struct{}
	stopRenew context.CancelFunc
	// stopped closes when the shutdown Stop began has finished. It is nil
	// before the first Stop.
	stopped chan struct{}
}

type queue struct {
	name    string
	idx     int
	table   string
	tasks   string
	leases  string
	workers int
	// retention and failedRetention are QueueOptions' namesakes.
	retention       time.Duration
	failedRetention time.Duration
	poke            chan struct{}
	// inflight is the set of subjects this process's workers have taken or
	// are about to take, guarded by Runner.mu.
	inflight map[string]bool
	// saturated is set when the last poll found a due subject for every free
	// worker, so more may be waiting, guarded by Runner.mu.
	saturated bool
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
	if opts.LeaseTTL == 0 {
		opts.LeaseTTL = 30 * time.Second
	}
	// Leases are stored to the millisecond.
	if opts.LeaseTTL < time.Millisecond {
		return nil, fmt.Errorf("task runner: lease TTL %v is under a millisecond", opts.LeaseTTL)
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = time.Second
	}
	if opts.RetentionInterval <= 0 {
		opts.RetentionInterval = time.Minute
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
		handlers: map[actionKey]*action{},
		holds:    map[uuid.UUID]*hold{},
		waiters:  map[TaskID]map[chan struct{}]struct{}{},
		waiting:  map[TaskID]*queue{},
		freed:    map[subjectKey]*freeSub{},
	}
	// Metrics never stop the runner: instruments the provider refuses leave
	// it reporting nothing.
	m, err := newMetrics(opts.MeterProvider)
	if err != nil {
		r.log.Warn("task runner metrics not created; reporting none", zapErr(err))
		if m, err = newMetrics(noop.NewMeterProvider()); err != nil {
			return nil, fmt.Errorf("task runner: no-op metrics: %w", err)
		}
	}
	r.metrics = m
	for i, qo := range opts.Queues {
		if !queueName.MatchString(qo.Name) {
			return nil, fmt.Errorf("task runner: invalid queue name %q", qo.Name)
		}
		if _, dup := r.byName[qo.Name]; dup {
			return nil, fmt.Errorf("task runner: queue %q listed twice", qo.Name)
		}
		// A negative age would put the prune's cutoff in the future.
		if qo.Retention < 0 || qo.FailedRetention < 0 {
			return nil, fmt.Errorf("task runner: queue %q has a negative retention", qo.Name)
		}
		table := strings.ReplaceAll(qo.Name, "-", "_")
		q := &queue{
			name:            qo.Name,
			idx:             i,
			table:           table,
			tasks:           pgx.Identifier{opts.Schema, table + "_tasks"}.Sanitize(),
			leases:          pgx.Identifier{opts.Schema, table + "_leases"}.Sanitize(),
			workers:         qo.Workers,
			retention:       cmp.Or(qo.Retention, 24*time.Hour),
			failedRetention: qo.FailedRetention,
			poke:            make(chan struct{}, 1),
			inflight:        map[string]bool{},
		}
		r.order = append(r.order, q)
		r.byName[q.name] = q
	}
	return r, nil
}

// Handle registers the handler for an action of a queue.
func (r *Runner) Handle(queueName, name string, h Handler, opts ...HandleOption) error {
	return r.register(queueName, name, &action{handler: h}, opts)
}

// HandleBatch registers a batching handler for an action of a queue. One run
// takes the subject's oldest due task and the due tasks straight after it
// with the same action, up to limit, stopping at the first with a different
// action so the subject's order holds. A task already past its Expires is
// recorded as expired and left out of the batch. Only async tasks batch: the
// action cannot be run sync.
func (r *Runner) HandleBatch(queueName, name string, limit int, h BatchHandler, opts ...HandleOption) error {
	if limit < 1 {
		return fmt.Errorf("task runner: %s.%s: batch limit %d", queueName, name, limit)
	}
	return r.register(queueName, name, &action{batch: h, limit: limit}, opts)
}

func (r *Runner) register(queueName, name string, a *action, opts []HandleOption) error {
	if _, ok := r.byName[queueName]; !ok {
		return fmt.Errorf("task runner: unknown queue %q", queueName)
	}
	if name == "" {
		return errors.New("task runner: empty action")
	}
	for _, o := range opts {
		o(a)
	}
	r.handlersMu.Lock()
	defer r.handlersMu.Unlock()
	k := actionKey{queueName, name}
	if _, dup := r.handlers[k]; dup {
		return fmt.Errorf("task runner: %s.%s registered twice", queueName, name)
	}
	r.handlers[k] = a
	return nil
}

func (r *Runner) handler(k actionKey) (*action, bool) {
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
	if r.stopped != nil {
		select {
		case <-r.stopped:
		default:
			return errors.New("task runner: still stopping")
		}
	}
	base := context.WithoutCancel(ctx)
	r.ctx, r.cancel = context.WithCancel(base)
	r.running = true
	// The renewer outlives the workers and inline runs: Stop ends it only
	// once they have all returned, so no lease lapses under a running task.
	renewCtx, stopRenew := context.WithCancel(base)
	r.renewDone = make(chan struct{})
	go r.renewLoop(renewCtx)
	for _, q := range r.order {
		if q.workers > 0 {
			r.wg.Add(1)
			go r.pollLoop(r.ctx, q)
		}
	}
	r.wg.Add(1)
	go r.retentionLoop(r.ctx)
	r.stopRenew = stopRenew
	// Metrics never stop the runner: a failure to register leaves the
	// gauges unreported.
	if err := r.startCollecting(); err != nil {
		r.log.Warn("task queue gauges not registered", zapErr(err))
	}
	return nil
}

// Stop cancels the workers and the Run calls in progress, and waits for them.
// A Stop that returns early because ctx ended leaves the shutdown running: a
// later Stop waits for it, and Start fails until it has finished.
// A task interrupted by Stop stays pending and is resumed by the next process
// to take its subject.
func (r *Runner) Stop(ctx context.Context) error {
	r.mu.Lock()
	done := r.stopped
	// The gauge callback takes r.mu, and unregistering waits for a
	// collection in progress, so the callback is unregistered after r.mu is
	// released.
	var collecting metric.Registration
	if r.running {
		r.running = false
		r.cancel()
		collecting, r.metrics.collecting = r.metrics.collecting, nil
		done = make(chan struct{})
		r.stopped = done
		stopRenew, renewDone := r.stopRenew, r.renewDone
		go func() {
			r.wg.Wait()
			r.inline.Wait()
			stopRenew()
			<-renewDone
			close(done)
		}()
	}
	r.mu.Unlock()
	if collecting != nil {
		_ = collecting.Unregister()
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// enterInline registers a Run call, returning a ctx that Stop also cancels,
// and the func that ends the registration. It fails once Stop has begun.
func (r *Runner) enterInline(ctx context.Context) (context.Context, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.running {
		return nil, nil, ErrNotStarted
	}
	r.inline.Add(1)
	ctx, cancel := context.WithCancel(ctx)
	unhook := context.AfterFunc(r.ctx, cancel)
	return ctx, func() {
		unhook()
		cancel()
		r.inline.Done()
	}, nil
}

// mayRun reports whether this instance may run a task placed on node.
func (r *Runner) mayRun(node *string) bool {
	return node == nil || *node == r.opts.Node
}

func (r *Runner) checkSpec(s Spec) (*queue, error) {
	q, ok := r.byName[s.Queue]
	if !ok {
		return nil, fmt.Errorf("task runner: unknown queue %q", s.Queue)
	}
	if s.Subject == "" {
		return nil, fmt.Errorf("task runner: %s.%s: empty subject", s.Queue, s.Action)
	}
	// A trailing * marks a prefix in Expedite, so a subject with one could
	// never be matched exactly.
	if strings.HasSuffix(s.Subject, "*") {
		return nil, fmt.Errorf("task runner: %s.%s: subject %q ends in *", s.Queue, s.Action, s.Subject)
	}
	if _, ok := r.handler(actionKey{s.Queue, s.Action}); !ok {
		return nil, fmt.Errorf("task runner: no handler for %s.%s", s.Queue, s.Action)
	}
	if s.Replace && s.NotBefore == nil {
		return nil, fmt.Errorf("task runner: %s.%s: a replacement needs a NotBefore", s.Queue, s.Action)
	}
	// An empty node would match every runner left unnamed, and none once
	// they are all named.
	if s.Node != nil && *s.Node == "" {
		return nil, fmt.Errorf("task runner: %s.%s: empty node name", s.Queue, s.Action)
	}
	return q, nil
}

// checkSync checks s can run as a sync task.
func (r *Runner) checkSync(s Spec) (*queue, error) {
	q, err := r.checkSpec(s)
	if err != nil {
		return nil, err
	}
	if s.NotBefore != nil {
		return nil, fmt.Errorf("task runner: %s.%s: a sync task cannot be delayed", s.Queue, s.Action)
	}
	if a, _ := r.handler(actionKey{s.Queue, s.Action}); a.batch != nil {
		return nil, fmt.Errorf("task runner: %s.%s batches, so it cannot run sync", s.Queue, s.Action)
	}
	if !r.mayRun(s.Node) {
		return nil, fmt.Errorf("task runner: %s.%s on node %q: %w", s.Queue, s.Action, *s.Node, ErrOtherNode)
	}
	return q, nil
}

const insertTask = `INSERT INTO %s (id, subject, action, not_before, expires, args, sync, node)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

// Node returns the name this runner places its instance's tasks under
// (Options.Node), or empty if it has none.
func (r *Runner) Node() string {
	return r.opts.Node
}

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
	sql, args := insertSpec(q, id, s, false)
	if _, err := db.Exec(ctx, sql, args...); err != nil {
		return TaskID{}, fmt.Errorf("submit %s.%s: %w", s.Queue, s.Action, err)
	}
	return id, nil
}

// Run submits a sync task and runs it in the caller's goroutine once its
// subject's lease is free, first running any older due tasks on the subject.
//
// It returns once the task has an outcome: its result, the handler's error,
// or ErrExpired. A commit that fails is retried, holding the lease, until it
// succeeds or the task's Expires passes, so ErrExpired means nothing was
// written. When s.Expires is nil it defaults to ctx's deadline, if ctx has
// one.
//
// If ctx ends first, Run returns ctx's error and the task stays submitted,
// for a worker to run later unless its Expires passes first. Give a sync task
// an Expires, or a ctx with a deadline, whenever a caller that has given up
// must not see the task take effect later.
func (r *Runner) Run(ctx context.Context, s Spec) ([]byte, error) {
	ctx, leave, err := r.enterInline(ctx)
	if err != nil {
		return nil, err
	}
	defer leave()
	if s.Expires == nil {
		if d, ok := ctx.Deadline(); ok {
			s.Expires = &d
		}
	}
	q, err := r.checkSync(s)
	if err != nil {
		return nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	var seq int64
	var created time.Time
	// The row says its caller is waiting, so workers leave it to the caller
	// until the claim lapses.
	r.mu.Lock()
	r.waiting[id] = q
	r.mu.Unlock()
	defer r.stopWaiting(q, id)
	if err := r.db.QueryRow(ctx, `INSERT INTO `+q.tasks+` (id, subject, action, expires, args, sync, node, waiting_until)
		VALUES ($1, $2, $3, $4, $5, true, $6, now() + $7 * interval '1 millisecond') RETURNING seq, created`,
		id, s.Subject, s.Action, s.Expires, s.Args, s.Node, r.opts.LeaseTTL.Milliseconds()).Scan(&seq, &created); err != nil {
		return nil, fmt.Errorf("submit %s.%s: %w", s.Queue, s.Action, err)
	}
	age := treeAge{created, id}
	key := subjectKey{q.name, s.Subject}

	wait := newBackoff(r.opts.PollInterval)
	start := time.Now()
	for {
		h, _, err := r.tryLease(ctx, q, s.Subject, age)
		if err != nil {
			return nil, err
		}
		if h != nil {
			r.metrics.observeLeaseWait(ctx, q, start)
			d := r.drainSync(ctx, q, h, seq, id)
			r.endHold(ctx, h, d.stop)
			start = time.Now()
			if d.ran {
				return d.res, d.err
			}
			var die *dieError
			var yield *yieldError
			switch {
			case d.stop == nil, errors.Is(d.stop, errLeaseLost), errors.Is(d.stop, errNotPending):
				// Another holder ran the task first or took over the lease,
				// or the drain stopped short of it at DrainLimit.
			case errors.As(d.stop, &die):
				r.awaitFree(ctx, die.key, r.opts.PollInterval)
			case errors.As(d.stop, &yield):
				// An older sync task's caller is waiting for the lease.
				if err := r.waitTask(ctx, yield.id, key, r.opts.PollInterval); err != nil {
					return nil, err
				}
			case ctx.Err() != nil:
				return nil, ctx.Err()
			default:
				return nil, d.stop
			}
		}
		if res, err, done := r.outcome(ctx, q, id); done {
			return res, err
		}
		// Past its Expires the task can no longer start: whoever takes the
		// subject next records it as expired, so the caller can stop waiting.
		d := wait.next()
		if s.Expires != nil {
			left := time.Until(*s.Expires)
			if left <= 0 {
				return nil, ErrExpired
			}
			d = min(d, left)
		}
		if err := r.waitTask(ctx, id, key, d); err != nil {
			return nil, err
		}
	}
}

// drainSync is drain for a sync caller: when a commit keeps failing, it keeps
// the lease and tries again after a backoff, so the caller gets an outcome
// instead of a task left to be resumed. It stops trying when ctx ends; the
// task's Expires ends it with ErrExpired.
func (r *Runner) drainSync(ctx context.Context, q *queue, h *hold, upTo int64, target TaskID) drained {
	wait := newBackoff(r.opts.PollInterval)
	for {
		d := r.drain(ctx, q, h, upTo, target)
		if !errors.Is(d.stop, errNotCommitted) {
			return d
		}
		t := time.NewTimer(wait.next())
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return drained{stop: errNotCommitted}
		}
	}
}

// Wait waits for a task to finish and returns its result. A failed task
// returns a *HandlerError, as does one since resubmitted under a new ID; an
// expired one returns ErrExpired and a cancelled one ErrCancelled. A task
// dropped by a replacement, or deleted past its queue's retention, is not
// found.
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
	case "failed", "resubmitted":
		m := ""
		if msg != nil {
			m = *msg
		}
		return nil, &HandlerError{Queue: q.name, Task: id, Message: m}, true
	case "expired":
		return nil, ErrExpired, true
	case "cancelled":
		return nil, ErrCancelled, true
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

// stopWaiting ends a Run caller's claim on its task, so that if the task is
// still pending, workers may take it at once.
func (r *Runner) stopWaiting(q *queue, id TaskID) {
	r.mu.Lock()
	delete(r.waiting, id)
	ctx := context.WithoutCancel(r.ctx)
	r.mu.Unlock()
	if _, err := r.db.Exec(ctx, `UPDATE `+q.tasks+` SET waiting_until = NULL
		WHERE id = $1 AND state = 'pending'`, id); err != nil {
		r.log.Warn("clear a sync task's waiting caller; it lapses on its own", zapQueue(q.name), zapErr(err))
	}
}

// waitTask waits until the task finishes in this process, the subject's lease
// is released in this process, d passes, or ctx ends.
func (r *Runner) waitTask(ctx context.Context, id TaskID, key subjectKey, d time.Duration) error {
	ch := r.addWaiter(id)
	defer r.removeWaiter(id, ch)
	free, unsubscribe := r.subscribeFree(key)
	defer unsubscribe()
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
