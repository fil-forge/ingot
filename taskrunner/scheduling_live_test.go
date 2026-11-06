package taskrunner_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/fil-forge/ingot/taskrunner"
)

func (f *fixture) migrate(t *testing.T) {
	t.Helper()
	if err := f.r.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) notBefore(t *testing.T, queue string, id taskrunner.TaskID) time.Time {
	t.Helper()
	var nb time.Time
	tbl := f.env.Table(queueTable(queue) + "_tasks")
	if err := f.env.DB.QueryRow(context.Background(), `SELECT not_before FROM `+tbl+` WHERE id = $1`, id).Scan(&nb); err != nil {
		t.Fatalf("read not_before of %s: %v", id, err)
	}
	return nb
}

func at(d time.Duration) *time.Time {
	t := time.Now().Add(d)
	return &t
}

func TestReplacementKeepsTheLaterNotBefore_Live(t *testing.T) {
	f := newFixture(t, 0)
	f.handle(t, qTop, "evict", f.put())
	f.migrate(t)
	ctx := waitCtx(t)
	submit := func(d time.Duration, replace bool) taskrunner.TaskID {
		t.Helper()
		id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "blob", Action: "evict", NotBefore: at(d), Replace: replace})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	first := submit(time.Hour, true)
	later := submit(2*time.Hour, true)
	earlier := submit(30*time.Minute, true)
	plain := submit(time.Minute, false)

	for _, id := range []taskrunner.TaskID{first, earlier} {
		if _, err := f.r.Wait(ctx, qTop, id); !errors.Is(err, taskrunner.ErrNotFound) {
			t.Errorf("replaced task %s: Wait error %v, want ErrNotFound", id, err)
		}
	}
	if s := f.state(t, qTop, later); s != "pending" {
		t.Errorf("kept task is %s, want pending", s)
	}
	if nb := f.notBefore(t, qTop, later); time.Until(nb) < 90*time.Minute {
		t.Errorf("kept task's NotBefore is %v away, want about 2h", time.Until(nb))
	}
	if s := f.state(t, qTop, plain); s != "pending" {
		t.Errorf("a task submitted without Replace was dropped: %s", s)
	}
}

func TestReplacementLeavesAStartedTaskAlone_Live(t *testing.T) {
	f := newFixture(t, 2)
	started := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	f.handle(t, qTop, "evict", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if runs.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil, nil
	})
	f.start(t)
	ctx := waitCtx(t)

	running, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "blob", Action: "evict", NotBefore: at(-time.Second), Replace: true})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	next, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "blob", Action: "evict", NotBefore: at(50 * time.Millisecond), Replace: true})
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	for _, id := range []taskrunner.TaskID{running, next} {
		if _, err := f.r.Wait(ctx, qTop, id); err != nil {
			t.Errorf("Wait %s: %v", id, err)
		}
	}
	if n := runs.Load(); n != 2 {
		t.Errorf("ran %d times, want 2", n)
	}
}

func TestCancel_Live(t *testing.T) {
	f := newFixture(t, 2)
	f.handle(t, qBottom, "expire", f.put())
	f.handle(t, qTop, "claim", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		id, err := uuid.Parse(string(task.Args))
		if err != nil {
			return nil, err
		}
		if err := ex.Cancel(ctx, qBottom, id); err != nil {
			return nil, err
		}
		if task.Subject == "fail" {
			return nil, errors.New("claim failed")
		}
		return nil, nil
	})
	f.start(t)
	ctx := waitCtx(t)
	delayed := func() taskrunner.TaskID {
		t.Helper()
		id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qBottom, Subject: "ref", Action: "expire", Args: []byte("1"), NotBefore: at(time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	// A cancel in a failed tree is dropped with the tree's other writes.
	kept := delayed()
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "fail", Action: "claim", Args: []byte(kept.String())}); err == nil {
		t.Fatal("claim succeeded")
	}
	if s := f.state(t, qBottom, kept); s != "pending" {
		t.Errorf("task cancelled by a failed tree: %s", s)
	}

	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "ok", Action: "claim", Args: []byte(kept.String())}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Wait(ctx, qBottom, kept); !errors.Is(err, taskrunner.ErrCancelled) {
		t.Errorf("Wait error %v, want ErrCancelled", err)
	}

	rolledBack := delayed()
	tx, err := f.env.DB.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.r.CancelTx(ctx, tx, qBottom, rolledBack); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback(ctx)
	if s := f.state(t, qBottom, rolledBack); s != "pending" {
		t.Errorf("rolled-back cancel took effect: %s", s)
	}

	// A cancel after the task has run does nothing.
	done, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qBottom, Subject: "ref", Action: "expire", Args: []byte("2")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Wait(ctx, qBottom, done); err != nil {
		t.Fatal(err)
	}
	if err := f.r.Cancel(ctx, qBottom, done); err != nil {
		t.Fatal(err)
	}
	if s := f.state(t, qBottom, done); s != "succeeded" {
		t.Errorf("cancel changed a finished task to %s", s)
	}
}

func TestBatchStopsAtTheLimitAndAtAnotherAction_Live(t *testing.T) {
	f := newFixture(t, 1)
	var mu sync.Mutex
	var runs [][]string
	record := func(tasks []taskrunner.Task) {
		mu.Lock()
		defer mu.Unlock()
		var args []string
		for _, task := range tasks {
			args = append(args, string(task.Args))
		}
		runs = append(runs, args)
	}
	if err := f.r.HandleBatch(qTop, "register", 3, func(ctx context.Context, ex *taskrunner.Exec, tasks []taskrunner.Task) ([][]byte, error) {
		record(tasks)
		return make([][]byte, len(tasks)), nil
	}); err != nil {
		t.Fatal(err)
	}
	f.handle(t, qTop, "retract", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		record([]taskrunner.Task{task})
		return nil, nil
	})
	f.migrate(t)
	ctx := waitCtx(t)

	var last taskrunner.TaskID
	for _, a := range []string{"r1", "r2", "r3", "r4", "r5", "x", "r6", "r7"} {
		action := "register"
		if a == "x" {
			action = "retract"
		}
		id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "space", Action: action, Args: []byte(a)})
		if err != nil {
			t.Fatal(err)
		}
		last = id
	}
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "space", Action: "register"}); err == nil {
		t.Error("a batching action ran sync")
	}
	f.start(t)
	if _, err := f.r.Wait(ctx, qTop, last); err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"r1", "r2", "r3"}, {"r4", "r5"}, {"x"}, {"r6", "r7"}}
	mu.Lock()
	defer mu.Unlock()
	if !slices.EqualFunc(runs, want, slices.Equal) {
		t.Errorf("batches %v, want %v", runs, want)
	}
}

func TestRetries_Live(t *testing.T) {
	f := newFixture(t, 2)
	policy := taskrunner.WithRetry(taskrunner.RetryPolicy{First: 10 * time.Millisecond, Max: 40 * time.Millisecond, Multiplier: 2, Attempts: 3})
	var mu sync.Mutex
	attempts := map[string][]int{}
	if err := f.r.Handle(qTop, "flaky", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		mu.Lock()
		attempts[task.Subject] = append(attempts[task.Subject], task.Attempt)
		mu.Unlock()
		switch {
		case task.Subject == "permanent":
			return nil, taskrunner.Permanent(errors.New("proof expired"))
		case task.Subject == "recovers" && task.Attempt == 2:
			return []byte("ok"), nil
		}
		return nil, errors.New("unavailable")
	}, policy); err != nil {
		t.Fatal(err)
	}
	f.start(t)
	ctx := waitCtx(t)

	results := map[string]error{}
	for _, s := range []string{"recovers", "never", "permanent"} {
		id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: s, Action: "flaky"})
		if err != nil {
			t.Fatal(err)
		}
		_, results[s] = f.r.Wait(ctx, qTop, id)
	}
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "sync", Action: "flaky"}); err == nil {
		t.Error("sync task succeeded")
	}

	if results["recovers"] != nil {
		t.Errorf("recovers: %v", results["recovers"])
	}
	var failed *taskrunner.HandlerError
	if !errors.As(results["never"], &failed) || failed.Message != "unavailable" {
		t.Errorf("never: %v, want a HandlerError", results["never"])
	}
	if !errors.As(results["permanent"], &failed) {
		t.Errorf("permanent: %v, want a HandlerError", results["permanent"])
	}
	mu.Lock()
	defer mu.Unlock()
	for s, want := range map[string][]int{"recovers": {0, 1, 2}, "never": {0, 1, 2}, "permanent": {0}, "sync": {0}} {
		if !slices.Equal(attempts[s], want) {
			t.Errorf("%s ran with attempts %v, want %v", s, attempts[s], want)
		}
	}
}

func TestBatchFailureIsRecordedTogether_Live(t *testing.T) {
	f := newFixture(t, 1)
	if err := f.r.HandleBatch(qTop, "register", 3, func(ctx context.Context, ex *taskrunner.Exec, tasks []taskrunner.Task) ([][]byte, error) {
		return nil, taskrunner.Permanent(errors.New("rejected"))
	}); err != nil {
		t.Fatal(err)
	}
	f.migrate(t)
	ctx := waitCtx(t)
	// Recording the second task's failure errors, as a crash part way would
	// stop it.
	tasks := f.env.Table("queue_top_tasks")
	refuse := f.env.Table("refuse_b")
	if _, err := f.env.DB.Exec(ctx, `
CREATE FUNCTION `+refuse+`() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
	RAISE EXCEPTION 'refused';
END $$;
CREATE TRIGGER refuse_b BEFORE UPDATE OF state ON `+tasks+` FOR EACH ROW
	WHEN (NEW.state = 'failed' AND OLD.args = 'b') EXECUTE FUNCTION `+refuse+`();`); err != nil {
		t.Fatal(err)
	}
	var ids []taskrunner.TaskID
	for _, a := range []string{"a", "b", "c"} {
		id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "space", Action: "register", Args: []byte(a)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	f.start(t)
	time.Sleep(500 * time.Millisecond)
	for i, id := range ids {
		if s := f.state(t, qTop, id); s != "pending" {
			t.Errorf("task %d is %s; a batch's failure must be recorded for all its tasks or none", i, s)
		}
	}
}

func TestRetryThatHoldsTheSubjectKeepsItsOrder_Live(t *testing.T) {
	for _, holds := range []bool{true, false} {
		t.Run(map[bool]string{true: "holds", false: "does not hold"}[holds], func(t *testing.T) {
			f := newFixture(t, 2)
			var mu sync.Mutex
			var order []string
			var failedOnce atomic.Bool
			policy := taskrunner.WithRetry(taskrunner.RetryPolicy{First: 200 * time.Millisecond, Attempts: 3, HoldsSubject: holds})
			for _, action := range []string{"add", "remove"} {
				if err := f.r.Handle(qTop, action, func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
					if task.Action == "add" && failedOnce.CompareAndSwap(false, true) {
						return nil, errors.New("unavailable")
					}
					mu.Lock()
					order = append(order, task.Action)
					mu.Unlock()
					return nil, nil
				}, policy); err != nil {
					t.Fatal(err)
				}
			}
			f.migrate(t)
			ctx := waitCtx(t)
			add, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "space", Action: "add"})
			if err != nil {
				t.Fatal(err)
			}
			remove, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "space", Action: "remove"})
			if err != nil {
				t.Fatal(err)
			}
			f.start(t)
			for _, id := range []taskrunner.TaskID{add, remove} {
				if _, err := f.r.Wait(ctx, qTop, id); err != nil {
					t.Fatal(err)
				}
			}
			want := []string{"add", "remove"}
			if !holds {
				want = []string{"remove", "add"}
			}
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(order, want) {
				t.Errorf("ran %v, want %v", order, want)
			}
		})
	}
}

func TestATaskWaitingToRetryCanBeCancelled_Live(t *testing.T) {
	f := newFixture(t, 1)
	var runs atomic.Int32
	if err := f.r.Handle(qTop, "expire", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		runs.Add(1)
		return nil, errors.New("unavailable")
	}, taskrunner.WithRetry(taskrunner.RetryPolicy{First: time.Hour})); err != nil {
		t.Fatal(err)
	}
	f.start(t)
	ctx := waitCtx(t)
	id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "ref", Action: "expire"})
	if err != nil {
		t.Fatal(err)
	}
	for runs.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if err := f.r.Cancel(ctx, qTop, id); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Wait(ctx, qTop, id); !errors.Is(err, taskrunner.ErrCancelled) {
		t.Errorf("Wait error %v, want ErrCancelled", err)
	}
}

// TestAReplacementTakesTheRetrysPlace_Live: a replacement waiting to retry
// is replaced like any that has not started, so its retry cannot run ahead of
// the replacement's time, and the replacement starts its attempts afresh.
func TestAReplacementTakesTheRetrysPlace_Live(t *testing.T) {
	f := newFixture(t, 1)
	type run struct {
		id      taskrunner.TaskID
		attempt int
	}
	var mu sync.Mutex
	var runs []run
	if err := f.r.Handle(qTop, "evict", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		runs = append(runs, run{task.ID, task.Attempt})
		if len(runs) == 1 {
			return nil, errors.New("unavailable")
		}
		return nil, nil
	}, taskrunner.WithRetry(taskrunner.RetryPolicy{First: 200 * time.Millisecond})); err != nil {
		t.Fatal(err)
	}
	f.start(t)
	ctx := waitCtx(t)
	failing, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "blob", Action: "evict", NotBefore: at(-time.Second), Replace: true})
	if err != nil {
		t.Fatal(err)
	}
	for {
		mu.Lock()
		n := len(runs)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	next, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "blob", Action: "evict", NotBefore: at(time.Second), Replace: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Wait(ctx, qTop, next); err != nil {
		t.Fatalf("Wait replacement: %v", err)
	}
	if _, err := f.r.Wait(ctx, qTop, failing); !errors.Is(err, taskrunner.ErrNotFound) {
		t.Errorf("replaced retry: Wait error %v, want ErrNotFound", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []run{{failing, 0}, {next, 0}}; !slices.Equal(runs, want) {
		t.Errorf("runs = %v, want the failed attempt then the replacement's first", runs)
	}
}

func TestBatchExpiryIsRecordedTogether_Live(t *testing.T) {
	f := newFixture(t, 1)
	if err := f.r.HandleBatch(qTop, "register", 3, func(ctx context.Context, ex *taskrunner.Exec, tasks []taskrunner.Task) ([][]byte, error) {
		return make([][]byte, len(tasks)), nil
	}); err != nil {
		t.Fatal(err)
	}
	f.migrate(t)
	ctx := waitCtx(t)
	// Recording the second task's expiry errors, as a crash part way would
	// stop it.
	tasks := f.env.Table("queue_top_tasks")
	refuse := f.env.Table("refuse_b")
	if _, err := f.env.DB.Exec(ctx, `
CREATE FUNCTION `+refuse+`() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
	RAISE EXCEPTION 'refused';
END $$;
CREATE TRIGGER refuse_b BEFORE UPDATE OF state ON `+tasks+` FOR EACH ROW
	WHEN (NEW.state = 'expired' AND OLD.args = 'b') EXECUTE FUNCTION `+refuse+`();`); err != nil {
		t.Fatal(err)
	}
	var ids []taskrunner.TaskID
	for _, a := range []string{"a", "b"} {
		id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "space", Action: "register", Args: []byte(a), Expires: at(-time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	f.start(t)
	time.Sleep(500 * time.Millisecond)
	for i, id := range ids {
		if s := f.state(t, qTop, id); s != "pending" {
			t.Errorf("task %d is %s; a batch's expiries must be recorded for all its tasks or none", i, s)
		}
	}
}

func TestBatchLeavesOutAnExpiredTask_Live(t *testing.T) {
	f := newFixture(t, 1)
	var mu sync.Mutex
	var seen [][]string
	if err := f.r.HandleBatch(qTop, "register", 5, func(ctx context.Context, ex *taskrunner.Exec, tasks []taskrunner.Task) ([][]byte, error) {
		var args []string
		for _, task := range tasks {
			args = append(args, string(task.Args))
		}
		mu.Lock()
		seen = append(seen, args)
		mu.Unlock()
		return make([][]byte, len(tasks)), nil
	}); err != nil {
		t.Fatal(err)
	}
	f.migrate(t)
	ctx := waitCtx(t)
	expired, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "space", Action: "register", Args: []byte("expired"), Expires: at(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	live, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "space", Action: "register", Args: []byte("live")})
	if err != nil {
		t.Fatal(err)
	}
	f.start(t)
	if _, err := f.r.Wait(ctx, qTop, live); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Wait(ctx, qTop, expired); !errors.Is(err, taskrunner.ErrExpired) {
		t.Errorf("expired task: Wait error %v, want ErrExpired", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.EqualFunc(seen, [][]string{{"live"}}, slices.Equal) {
		t.Errorf("handler saw %v, want only the live task", seen)
	}
}

// TestDrainLimitCountsRuns_Live: DrainLimit counts handler runs, a batch as
// one, so a batch goes out whole at its own limit, and a subject gives up its
// lease after DrainLimit runs, letting another subject's older task go next.
func TestDrainLimitCountsRuns_Live(t *testing.T) {
	f := newFixture(t, 1, func(o *taskrunner.Options) { o.DrainLimit = 2 })
	var mu sync.Mutex
	var runs []string
	record := func(task taskrunner.Task, n int) {
		mu.Lock()
		runs = append(runs, fmt.Sprintf("%s:%s:%d", task.Subject, task.Action, n))
		mu.Unlock()
	}
	if err := f.r.HandleBatch(qTop, "register", 5, func(ctx context.Context, ex *taskrunner.Exec, tasks []taskrunner.Task) ([][]byte, error) {
		record(tasks[0], len(tasks))
		return make([][]byte, len(tasks)), nil
	}); err != nil {
		t.Fatal(err)
	}
	f.handle(t, qTop, "other", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		record(task, 1)
		return nil, nil
	})
	f.migrate(t)
	ctx := waitCtx(t)
	// Subject a: a batch of five, another action, then one more of the
	// first, which a third run would take. Subject b's task is queued before
	// that last one.
	var last taskrunner.TaskID
	for _, s := range []struct{ subject, action string }{
		{"a", "register"}, {"a", "register"}, {"a", "register"}, {"a", "register"}, {"a", "register"},
		{"a", "other"}, {"b", "other"}, {"a", "register"},
	} {
		id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: s.subject, Action: s.action})
		if err != nil {
			t.Fatal(err)
		}
		last = id
	}
	f.start(t)
	if _, err := f.r.Wait(ctx, qTop, last); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"a:register:5", "a:other:1", "b:other:1", "a:register:1"}; !slices.Equal(runs, want) {
		t.Errorf("runs %v, want %v: the whole batch, then a gives way after two runs", runs, want)
	}
}
