package taskrunner_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
)

// stealLease hands the subject's lease to another consumer until d from now,
// as a takeover after a missed renewal would.
func (f *fixture) stealLease(t *testing.T, queue, subject string, d time.Duration) {
	t.Helper()
	tag, err := f.env.DB.Exec(context.Background(), `UPDATE `+f.env.Table(queueTable(queue)+"_leases")+`
		SET owner = $2, expires = now() + $3 * interval '1 millisecond' WHERE subject = $1`,
		subject, uuid.New(), d.Milliseconds())
	if err != nil || tag.RowsAffected() != 1 {
		t.Errorf("steal lease: %v, %d rows", err, tag.RowsAffected())
	}
}

func TestLostLeaseStopsTheTaskUntilItIsRetaken_Live(t *testing.T) {
	f := newFixture(t, 0)
	var mu sync.Mutex
	var runs []time.Time
	f.handle(t, qTop, "put", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		mu.Lock()
		runs = append(runs, time.Now())
		first := len(runs) == 1
		mu.Unlock()
		if first {
			f.stealLease(t, qTop, task.Subject, 500*time.Millisecond)
		}
		return nil, ex.Write(ctx, `INSERT INTO `+f.env.Table("items")+` (k, v) VALUES ('a', 1)
			ON CONFLICT (k) DO UPDATE SET v = `+f.env.Table("items")+`.v + 1`)
	})
	f.start(t)

	if _, err := f.r.Run(waitCtx(t), taskrunner.Spec{Queue: qTop, Subject: "s", Action: "put"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(runs) != 2 {
		t.Fatalf("handler ran %d times, want 2: once under the lost lease, once after retaking it", len(runs))
	}
	if gap := runs[1].Sub(runs[0]); gap < 400*time.Millisecond {
		t.Errorf("handler ran again %v after losing its lease, before the other consumer's lease expired", gap)
	}
	if v, _ := f.item(t, "a"); v != 1 {
		t.Errorf("item a = %d, want 1: only the run under a held lease commits", v)
	}
}

func TestFailureUnderALostLeaseRecordsNothing_Live(t *testing.T) {
	f := newFixture(t, 0)
	var runs atomic.Int32
	f.handle(t, qTop, "fail", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if runs.Add(1) == 1 {
			f.stealLease(t, qTop, task.Subject, 300*time.Millisecond)
			return nil, errors.New("failed under a lost lease")
		}
		return []byte("ok"), nil
	})
	f.start(t)

	res, err := f.r.Run(waitCtx(t), taskrunner.Spec{Queue: qTop, Subject: "s", Action: "fail"})
	if err != nil || string(res) != "ok" {
		t.Fatalf("Run = %q, %v; want the result of the run under a held lease", res, err)
	}
}

func TestStopWaitsForInlineRuns_Live(t *testing.T) {
	f := newFixture(t, 0)
	started := make(chan struct{})
	var handlerDone atomic.Bool
	f.handle(t, qTop, "block", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		close(started)
		<-ctx.Done()
		time.Sleep(100 * time.Millisecond)
		handlerDone.Store(true)
		return nil, ctx.Err()
	})
	f.start(t)

	runErr := make(chan error, 1)
	go func() {
		_, err := f.r.Run(context.Background(), taskrunner.Spec{Queue: qTop, Subject: "s", Action: "block"})
		runErr <- err
	}()
	<-started
	if err := f.r.Stop(waitCtx(t)); err != nil {
		t.Fatal(err)
	}
	if !handlerDone.Load() {
		t.Error("Stop returned while an inline handler was still running")
	}
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Errorf("Run error %v, want context.Canceled", err)
	}
	if _, err := f.r.Run(context.Background(), taskrunner.Spec{Queue: qTop, Subject: "s", Action: "block"}); !errors.Is(err, taskrunner.ErrNotStarted) {
		t.Errorf("Run after Stop: %v, want ErrNotStarted", err)
	}
}

type callerKey struct{}

func TestWorkersLeaveSyncTasksToTheirCaller_Live(t *testing.T) {
	f := newFixture(t, 4)
	var workerRan atomic.Int32
	f.handle(t, qTop, "slow", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		time.Sleep(50 * time.Millisecond)
		return nil, nil
	})
	f.handle(t, qTop, "sync", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if ctx.Value(callerKey{}) == nil {
			workerRan.Add(1)
		}
		return nil, nil
	})
	f.start(t)
	ctx := context.WithValue(waitCtx(t), callerKey{}, true)

	for range 10 {
		// A worker is draining the subject when the caller arrives, so the
		// caller's task is next in the worker's way.
		if _, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "slow"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
		if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "sync"}); err != nil {
			t.Fatal(err)
		}
	}
	if n := workerRan.Load(); n > 0 {
		t.Errorf("a worker ran %d sync tasks whose caller was waiting", n)
	}
}

func TestChildExpiringWhileWaitingForItsLeaseDoesNotRun_Live(t *testing.T) {
	f := newFixture(t, 0)
	var childRan atomic.Bool
	f.handle(t, qBottom, "child", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		childRan.Store(true)
		return nil, nil
	})
	f.handle(t, qTop, "parent", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		expires := time.Now().Add(200 * time.Millisecond)
		_, err := ex.Run(ctx, taskrunner.Spec{Queue: qBottom, Subject: "c", Action: "child", Expires: &expires})
		return nil, err
	})
	f.start(t)
	ctx := waitCtx(t)
	// A younger tree holds the child's subject for longer than the child may
	// wait, so the parent waits rather than giving way.
	if _, err := f.env.DB.Exec(ctx, `INSERT INTO `+f.env.Table("queue_bottom_leases")+` (subject, owner, expires, tree_created, tree_id)
		VALUES ('c', $1, now() + interval '500 milliseconds', now() + interval '1 hour', $2)`, uuid.New(), uuid.New()); err != nil {
		t.Fatal(err)
	}

	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "p", Action: "parent"}); !errors.Is(err, taskrunner.ErrExpired) {
		t.Errorf("Run error %v, want ErrExpired", err)
	}
	if childRan.Load() {
		t.Error("a child ran after its Expires had passed")
	}
}

func TestQueueNamesFitPostgresIdentifiers_Live(t *testing.T) {
	env := taskrunnertest.NewEnv(t)
	long := "q" + strings.Repeat("x", 39)
	r, err := taskrunner.New(env.DB, taskrunner.Options{Schema: env.Schema, Queues: []taskrunner.QueueOptions{{Name: long}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Migrate(context.Background()); err != nil {
		t.Errorf("migrate a %d-character queue: %v", len(long), err)
	}
	if _, err := taskrunner.New(env.DB, taskrunner.Options{Schema: env.Schema, Queues: []taskrunner.QueueOptions{{Name: long + "x"}}}); err == nil {
		t.Errorf("a %d-character queue name was accepted", len(long)+1)
	}
}

func TestCheckOwnWritesTreatsAConditionalMissAlike_Live(t *testing.T) {
	taskrunnertest.CheckOwnWrites(t, taskrunnertest.Case{
		Queues: []taskrunner.QueueOptions{{Name: "queue-claim"}},
		Setup: func(ctx context.Context, env taskrunnertest.Env, r *taskrunner.Runner) error {
			refs := env.Table("refs")
			if _, err := env.DB.Exec(ctx, `CREATE TABLE `+refs+` (k text PRIMARY KEY, owned boolean NOT NULL)`); err != nil {
				return err
			}
			return r.Handle("queue-claim", "claim", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
				return nil, ex.WriteConditional(ctx, `UPDATE `+refs+` SET owned = true WHERE k = 'missing'`)
			})
		},
		Spec:    taskrunner.Spec{Queue: "queue-claim", Subject: "c", Action: "claim"},
		Expires: 300 * time.Millisecond,
		State: func(ctx context.Context, env taskrunnertest.Env) (any, error) {
			var n int
			err := env.DB.QueryRow(ctx, `SELECT count(*) FROM `+env.Table("refs")+` WHERE owned`).Scan(&n)
			return n, err
		},
	})
}

func TestLeaseTakenBetweenDrainedTasksStopsTheDrain_Live(t *testing.T) {
	f := newFixture(t, 1)
	var secondAt atomic.Int64
	f.handle(t, qTop, "first", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		return nil, nil
	})
	f.handle(t, qTop, "second", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		secondAt.CompareAndSwap(0, time.Now().UnixNano())
		return nil, nil
	})
	ctx := waitCtx(t)
	if err := f.r.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// Another consumer takes the subject over as the first task commits, so
	// the takeover lands between the two tasks of one drain.
	leases, tasks := f.env.Table("queue_top_leases"), f.env.Table("queue_top_tasks")
	steal := f.env.Table("steal_lease")
	if _, err := f.env.DB.Exec(ctx, `
CREATE FUNCTION `+steal+`() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
	UPDATE `+leases+` SET owner = gen_random_uuid(), expires = now() + interval '500 milliseconds'
		WHERE subject = NEW.subject;
	RETURN NEW;
END $$;
CREATE TRIGGER steal AFTER UPDATE OF state ON `+tasks+` FOR EACH ROW
	WHEN (NEW.action = 'first' AND NEW.state = 'succeeded') EXECUTE FUNCTION `+steal+`();`); err != nil {
		t.Fatal(err)
	}
	first, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "second"})
	if err != nil {
		t.Fatal(err)
	}
	f.start(t)
	if _, err := f.r.Wait(ctx, qTop, second); err != nil {
		t.Fatal(err)
	}
	var stolenAt time.Time
	if err := f.env.DB.QueryRow(ctx, `SELECT finished FROM `+tasks+` WHERE id = $1`, first).Scan(&stolenAt); err != nil {
		t.Fatal(err)
	}
	if gap := time.Unix(0, secondAt.Load()).Sub(stolenAt); gap < 400*time.Millisecond {
		t.Errorf("the next task started %v after its subject's lease was taken over", gap)
	}
}

func TestChildReusingALostLeaseRestartsTheTree_Live(t *testing.T) {
	f := newFixture(t, 0)
	var stole atomic.Bool
	var secondRuns atomic.Int32
	f.handle(t, qBottom, "first", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if stole.CompareAndSwap(false, true) {
			f.stealLease(t, qBottom, task.Subject, 300*time.Millisecond)
		}
		return nil, nil
	})
	f.handle(t, qBottom, "second", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		secondRuns.Add(1)
		return nil, nil
	})
	f.handle(t, qTop, "parent", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if _, err := ex.Run(ctx, taskrunner.Spec{Queue: qBottom, Subject: "c", Action: "first"}); err != nil {
			return nil, err
		}
		// Long enough for renewal to find the child's lease taken over.
		time.Sleep(500 * time.Millisecond)
		_, err := ex.Run(ctx, taskrunner.Spec{Queue: qBottom, Subject: "c", Action: "second"})
		return nil, err
	})
	f.start(t)

	if _, err := f.r.Run(waitCtx(t), taskrunner.Spec{Queue: qTop, Subject: "p", Action: "parent"}); err != nil {
		t.Fatal(err)
	}
	if n := secondRuns.Load(); n != 1 {
		t.Errorf("the second child ran %d times, want once: never under the lost lease", n)
	}
}

func TestStopAfterATimedOutStopWaitsForTheShutdown_Live(t *testing.T) {
	f := newFixture(t, 0)
	started := make(chan struct{})
	var handlerDone atomic.Bool
	f.handle(t, qTop, "block", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		close(started)
		<-ctx.Done()
		time.Sleep(200 * time.Millisecond)
		handlerDone.Store(true)
		return nil, ctx.Err()
	})
	f.start(t)
	go func() {
		_, _ = f.r.Run(context.Background(), taskrunner.Spec{Queue: qTop, Subject: "s", Action: "block"})
	}()
	<-started

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.r.Stop(expired); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop with an ended ctx: %v, want context.Canceled", err)
	}
	if err := f.r.Start(context.Background()); err == nil {
		t.Error("Start succeeded while the previous shutdown was still running")
	}
	if err := f.r.Stop(waitCtx(t)); err != nil {
		t.Fatal(err)
	}
	if !handlerDone.Load() {
		t.Error("a second Stop returned before the first one's shutdown finished")
	}
	if err := f.r.Start(context.Background()); err != nil {
		t.Errorf("Start after the shutdown finished: %v", err)
	}
}
