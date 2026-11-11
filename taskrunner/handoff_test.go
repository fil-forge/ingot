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
}

func (c *countingDB) count(sql string) {
	c.statements.Add(1)
	if strings.Contains(sql, "ON CONFLICT (subject) DO UPDATE") {
		c.leaseTries.Add(1)
	}
}

func (c *countingDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	c.count(sql)
	return c.Pool.Query(ctx, sql, args...)
}

func (c *countingDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	c.count(sql)
	return c.Pool.QueryRow(ctx, sql, args...)
}

func (c *countingDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	c.count(sql)
	return c.Pool.Exec(ctx, sql, args...)
}

// newCountingRunner starts a runner over a counting DB, with one queue whose
// workers poll every pollInterval.
func newCountingRunner(t *testing.T, pollInterval time.Duration, h taskrunner.Handler) (*taskrunner.Runner, *countingDB) {
	t.Helper()
	env := taskrunnertest.NewEnv(t)
	db := &countingDB{Pool: env.DB}
	r, err := taskrunner.New(db, taskrunner.Options{
		Schema:       env.Schema,
		Queues:       []taskrunner.QueueOptions{{Name: qTop, Workers: 4}},
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
