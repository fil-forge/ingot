package taskrunner_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
)

// countingDB counts the statements the runner sends outside transactions, and
// among them its attempts to take a lease.
type countingDB struct {
	*pgxpool.Pool
	statements atomic.Int64
	leaseTries atomic.Int64
	// callerStatements counts the statements other than background ones.
	callerStatements atomic.Int64
	// stopWaitingDelay holds back the statement that clears a sync task's
	// waiting caller, widening the window in which others can see it.
	stopWaitingDelay atomic.Int64
	// afterOutcome, when set, is called once, after the next read of a sync
	// task's outcome and before the runner sees the result.
	afterOutcome atomic.Pointer[func()]
}

// pausedRow is a row whose Scan calls a hook once it has read the row.
type pausedRow struct {
	pgx.Row
	after func()
}

func (p pausedRow) Scan(dest ...any) error {
	err := p.Row.Scan(dest...)
	p.after()
	return err
}

func (c *countingDB) count(sql string) {
	c.statements.Add(1)
	if strings.Contains(sql, "ON CONFLICT (subject) DO UPDATE") {
		c.leaseTries.Add(1)
	}
	if !background(sql) {
		c.callerStatements.Add(1)
	}
}

// background reports whether sql is one of the runner's own periodic
// statements, which run whatever its callers do: the workers' poll, lease
// and claim renewal, and retention.
func background(sql string) bool {
	for _, s := range []string{"SELECT d.subject", "FOR UPDATE SKIP LOCKED", "SET waiting_until = now() +", "WHERE id IN ("} {
		if strings.Contains(sql, s) {
			return true
		}
	}
	return false
}

func (c *countingDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	c.count(sql)
	return c.Pool.Query(ctx, sql, args...)
}

func (c *countingDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	c.count(sql)
	row := c.Pool.QueryRow(ctx, sql, args...)
	if strings.Contains(sql, "SELECT state, result, error FROM") {
		if after := c.afterOutcome.Swap(nil); after != nil {
			return pausedRow{Row: row, after: *after}
		}
	}
	return row
}

func (c *countingDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	c.count(sql)
	if strings.Contains(sql, "SET waiting_until = NULL") {
		time.Sleep(time.Duration(c.stopWaitingDelay.Load()))
	}
	return c.Pool.Exec(ctx, sql, args...)
}

// newCountingRunner starts a runner over a counting DB, with one queue whose
// workers poll every pollInterval.
func newCountingRunner(t *testing.T, pollInterval time.Duration, h taskrunner.Handler) (*taskrunner.Runner, *countingDB) {
	t.Helper()
	return newCountingRunnerWithWorkers(t, pollInterval, 4, h)
}

// newCountingRunnerWithWorkers is newCountingRunner with the queue's worker
// count given.
func newCountingRunnerWithWorkers(t *testing.T, pollInterval time.Duration, workers int, h taskrunner.Handler) (*taskrunner.Runner, *countingDB) {
	t.Helper()
	env := taskrunnertest.NewEnv(t)
	db := &countingDB{Pool: env.DB}
	r, err := taskrunner.New(db, taskrunner.Options{
		Schema:       env.Schema,
		Queues:       []taskrunner.QueueOptions{{Name: qTop, Workers: workers}},
		LeaseTTL:     5 * time.Second,
		PollInterval: pollInterval,
		Logger:       zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Handle(qTop, "step", h); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := r.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })
	return r, db
}

// Run callers queued on one subject take its lease in turn: each release wakes
// the next caller only, so the lease attempts per task stay flat however many
// callers wait. Waking every caller on each release costs attempts that grow
// with the number waiting.
func TestRunCallersOnOneSubjectTakeTheLeaseInTurn(t *testing.T) {
	const callers = 32
	r, db := newCountingRunner(t, 20*time.Millisecond, func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		time.Sleep(2 * time.Millisecond)
		return nil, nil
	})
	ctx := waitCtx(t)
	before := db.leaseTries.Load()

	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := range callers {
		wg.Go(func() {
			if _, err := r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Args: []byte(strconv.Itoa(i))}); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	tries := db.leaseTries.Load() - before
	t.Logf("%d lease attempts for %d sync tasks on one subject", tries, callers)
	if tries > 3*callers {
		t.Errorf("%d lease attempts for %d sync tasks; want at most %d", tries, callers, 3*callers)
	}
}

// A Run caller that leaves its subject's line, once its task has run or
// because it gave up, wakes the caller behind it at once rather than when that
// caller's poll interval comes round.
func TestRunCallerLeavingTheLineWakesTheNext(t *testing.T) {
	hold := make(chan struct{})
	started := make(chan struct{}, 1)
	r, _ := newCountingRunner(t, time.Minute, func(_ context.Context, _ *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if string(task.Args) == "first" {
			started <- struct{}{}
			<-hold
		}
		return nil, nil
	})
	ctx := waitCtx(t)
	run := func(ctx context.Context, arg string) chan error {
		done := make(chan error, 1)
		go func() {
			_, err := r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Args: []byte(arg)})
			done <- err
		}()
		return done
	}

	first := run(ctx, "first")
	<-started
	quitCtx, quit := context.WithCancel(ctx)
	quitter := run(quitCtx, "quitter")
	time.Sleep(50 * time.Millisecond) // queued behind first
	last := run(ctx, "last")
	time.Sleep(50 * time.Millisecond) // queued behind quitter

	quit()
	if err := <-quitter; err == nil {
		t.Fatal("cancelled caller returned no error")
	}
	close(hold)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-last:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the last caller was not woken when the callers ahead of it left the line")
	}
}

// The caller at the front of the line backs off while a task ahead of its own
// holds the subject: releasing the lease it took to find that out does not
// wake it again at once.
func TestRunCallerBacksOffBehindATaskAhead(t *testing.T) {
	r, db := newCountingRunner(t, time.Second, func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		return nil, nil
	})
	ctx := waitCtx(t)
	elsewhere := "elsewhere"
	if _, err := r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Node: &elsewhere}); err != nil {
		t.Fatal(err)
	}
	before := db.leaseTries.Load()
	expires := time.Now().Add(500 * time.Millisecond)
	_, err := r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Expires: &expires})
	if !errors.Is(err, taskrunner.ErrExpired) {
		t.Fatalf("Run behind another instance's task: %v, want ErrExpired", err)
	}
	tries := db.leaseTries.Load() - before
	t.Logf("%d lease attempts in 500ms behind a task ahead", tries)
	if tries > 20 {
		t.Errorf("%d lease attempts in 500ms behind a task ahead; want at most 20", tries)
	}
}

// A caller behind a task that is a worker's to run takes the subject's lease
// once to find that out, then waits to be woken or for the poll interval: it
// does not take and release the lease again and again while it waits.
func TestRunCallerBehindAWorkersTaskDoesNotRetakeTheLease(t *testing.T) {
	// No workers here: the task ahead is another instance's to run, and
	// stays due for the whole of the caller's wait.
	r, db := newCountingRunnerWithWorkers(t, time.Second, 0, func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		return nil, nil
	})
	ctx := waitCtx(t)
	if _, err := r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step"}); err != nil {
		t.Fatal(err)
	}
	before := db.leaseTries.Load()
	expires := time.Now().Add(500 * time.Millisecond)
	_, err := r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Expires: &expires})
	if !errors.Is(err, taskrunner.ErrExpired) {
		t.Fatalf("Run behind a task no worker here runs: %v, want ErrExpired", err)
	}
	tries := db.leaseTries.Load() - before
	t.Logf("%d lease attempts in 500ms behind a worker's task", tries)
	if tries > 2 {
		t.Errorf("%d lease attempts in 500ms behind a worker's task; want at most 2", tries)
	}
}

// A caller behind a task that found every worker busy runs as soon as a
// worker frees up and runs the task ahead, long before the poll interval: the
// worker wakes the caller when it releases the subject.
func TestRunCallerBehindAWorkersTaskRunsOnceAWorkerFrees(t *testing.T) {
	busy := make(chan struct{})
	r, _ := newCountingRunnerWithWorkers(t, time.Minute, 1, func(_ context.Context, _ *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if string(task.Args) == "busy" {
			close(busy)
			time.Sleep(200 * time.Millisecond)
		}
		return nil, nil
	})
	ctx := waitCtx(t)
	// The queue's one worker is busy on another subject when the task ahead
	// is submitted and when the caller finds it.
	if _, err := r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "other", Action: "step", Args: []byte("busy")}); err != nil {
		t.Fatal(err)
	}
	<-busy
	if _, err := r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step"}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step"}); err != nil {
		t.Fatalf("Run behind a worker's task: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("Run took %v behind a task a worker ran within a second; it waited for the poll interval", took)
	}
}

// A caller behind a task that is cancelled before any worker takes it runs at
// once: no subject is released to wake it, so the cancel does.
func TestRunCallerBehindACancelledTaskRunsAtOnce(t *testing.T) {
	r, db := newCountingRunnerWithWorkers(t, time.Minute, 0, func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		return nil, nil
	})
	ctx := waitCtx(t)
	ahead, err := r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step"})
	if err != nil {
		t.Fatal(err)
	}
	before := db.leaseTries.Load()
	done := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step"})
		done <- err
	}()
	// The caller has taken the lease, found the task ahead and settled into
	// its wait.
	for db.leaseTries.Load() == before {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if err := r.Cancel(ctx, qTop, ahead); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run behind a cancelled task: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run still waits 5s after the task ahead of it was cancelled")
	}
}

// A caller whose task is cancelled while it waits behind another caller
// returns ErrCancelled at once, without waiting for its turn.
func TestCancelledRunReturnsWhileOthersHoldTheSubject(t *testing.T) {
	hold := make(chan struct{})
	started := make(chan struct{}, 1)
	r, _ := newCountingRunner(t, time.Minute, func(_ context.Context, _ *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if string(task.Args) == "first" {
			started <- struct{}{}
			<-hold
		}
		return nil, nil
	})
	ctx := waitCtx(t)
	defer close(hold)
	go func() {
		_, _ = r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Args: []byte("first")})
	}()
	<-started
	second := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Args: []byte("second")})
		second <- err
	}()

	var ids []taskrunner.TaskID
	for len(ids) < 2 {
		pending, err := r.Pending(ctx, qTop, []string{"s"}, "step")
		if err != nil {
			t.Fatal(err)
		}
		ids = pending["s"]
		time.Sleep(10 * time.Millisecond)
	}
	if err := r.Cancel(ctx, qTop, ids[1]); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-second:
		if !errors.Is(err, taskrunner.ErrCancelled) {
			t.Fatalf("cancelled Run returned %v, want ErrCancelled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled Run still waiting behind the caller holding the subject")
	}
}

// A caller that gives up while its task is still pending clears its claim on
// the task before it wakes the caller behind it. Woken first, that caller
// would find the older task still claimed, leave it to its absent caller, and
// wait out its poll interval.
func TestRunCallerGivingUpClearsItsClaimBeforeWakingTheNext(t *testing.T) {
	hold := make(chan struct{})
	started := make(chan struct{}, 1)
	r, db := newCountingRunner(t, time.Minute, func(_ context.Context, _ *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if string(task.Args) == "worker" {
			started <- struct{}{}
			<-hold
		}
		return nil, nil
	})
	db.stopWaitingDelay.Store(int64(300 * time.Millisecond))
	ctx := waitCtx(t)
	if _, err := r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Args: []byte("worker")}); err != nil {
		t.Fatal(err)
	}
	<-started

	quitCtx, quit := context.WithCancel(ctx)
	go func() {
		_, _ = r.Run(quitCtx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Args: []byte("quitter")})
	}()
	time.Sleep(50 * time.Millisecond) // queued behind the worker's task
	last := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Args: []byte("last")})
		last <- err
	}()
	time.Sleep(50 * time.Millisecond) // queued behind quitter

	quit()
	time.Sleep(20 * time.Millisecond)
	close(hold) // the worker releases the subject while quitter's claim stands
	select {
	case err := <-last:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the caller behind one that gave up stalled behind its abandoned claim")
	}
}

// A cancel that commits after a waiting caller has read its task's outcome,
// and before the caller sleeps, still wakes it: it hears of the finish from
// before it reads, so none is missed in between.
func TestCancelBetweenAnOutcomeReadAndTheWaitWakesTheCaller(t *testing.T) {
	hold := make(chan struct{})
	started := make(chan struct{}, 1)
	r, db := newCountingRunner(t, 2*time.Second, func(_ context.Context, _ *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if string(task.Args) == "first" {
			started <- struct{}{}
			<-hold
		}
		return nil, nil
	})
	ctx := waitCtx(t)
	defer close(hold)
	go func() {
		_, _ = r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Args: []byte("first")})
	}()
	<-started

	read, proceed := make(chan struct{}), make(chan struct{})
	after := func() {
		close(read)
		<-proceed
	}
	db.afterOutcome.Store(&after)
	second := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Args: []byte("second")})
		second <- err
	}()
	select {
	case <-read:
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting caller never read its outcome")
	}
	pending, err := r.Pending(ctx, qTop, []string{"s"}, "step")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending["s"]) != 2 {
		t.Fatalf("pending tasks %v, want first and second", pending["s"])
	}
	if err := r.Cancel(ctx, qTop, pending["s"][1]); err != nil {
		t.Fatal(err)
	}
	close(proceed)
	select {
	case err := <-second:
		if !errors.Is(err, taskrunner.ErrCancelled) {
			t.Fatalf("cancelled Run returned %v, want ErrCancelled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("a cancel between the outcome read and the wait went unheard until the poll interval")
	}
}

// A Run caller on a free subject sends three statements outside its task's
// own transaction: the task's insert, one that takes the lease and claims the
// task, and the clearing of its waiting claim. The task's transaction records
// the outcome and releases the lease. Every statement that runs on its own is
// a commit, so each one adds to the time the subject is held.
func TestRunOnAFreeSubjectSendsThreeStatements(t *testing.T) {
	r, db := newCountingRunner(t, time.Minute, func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		return nil, nil
	})
	ctx := waitCtx(t)
	const runs = 10
	before := db.callerStatements.Load()
	for i := range runs {
		if _, err := r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Args: []byte(strconv.Itoa(i))}); err != nil {
			t.Fatal(err)
		}
	}
	per := float64(db.callerStatements.Load()-before) / runs
	t.Logf("%.1f statements per Run on a free subject", per)
	if per > 3 {
		t.Errorf("%.1f statements per Run on a free subject; want at most 3", per)
	}
}
