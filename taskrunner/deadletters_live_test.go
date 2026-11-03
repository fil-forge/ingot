package taskrunner_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/fil-forge/ingot/taskrunner"
)

func TestBatchedTasksFailOnTheirOwn_Live(t *testing.T) {
	f := newFixture(t, 1)
	var mu sync.Mutex
	runs := map[string]int{}
	if err := f.r.HandleBatch(qTop, "register", 5, func(ctx context.Context, ex *taskrunner.Exec, tasks []taskrunner.Task) ([]taskrunner.Outcome, error) {
		out := make([]taskrunner.Outcome, len(tasks))
		for i, task := range tasks {
			k := string(task.Args)
			mu.Lock()
			runs[k]++
			mu.Unlock()
			if err := ex.Write(ctx, `INSERT INTO `+f.env.Table("items")+` (k, v) VALUES ($1, 1)
				ON CONFLICT (k) DO UPDATE SET v = `+f.env.Table("items")+`.v + 1`, k); err != nil {
				return nil, err
			}
			if k == "refused" {
				out[i].Err = errors.New("refused")
			}
		}
		return out, nil
	}, taskrunner.WithRetry(taskrunner.RetryPolicy{First: 10 * time.Millisecond, Attempts: 2})); err != nil {
		t.Fatal(err)
	}
	f.migrate(t)
	ctx := waitCtx(t)
	ids := map[string]taskrunner.TaskID{}
	for _, k := range []string{"a", "refused", "c"} {
		id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "space", Action: "register", Args: []byte(k)})
		if err != nil {
			t.Fatal(err)
		}
		ids[k] = id
	}
	f.start(t)

	for _, k := range []string{"a", "c"} {
		if _, err := f.r.Wait(ctx, qTop, ids[k]); err != nil {
			t.Errorf("%s: %v", k, err)
		}
	}
	var failed *taskrunner.HandlerError
	if _, err := f.r.Wait(ctx, qTop, ids["refused"]); !errors.As(err, &failed) || failed.Message != "refused" {
		t.Errorf("refused: %v, want a HandlerError", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if runs["a"] != 1 || runs["c"] != 1 || runs["refused"] != 2 {
		t.Errorf("runs %v: the batchmates once each, the refused task once per attempt", runs)
	}
	// The batch's writes committed with its successes, the failed task's
	// included.
	if v, _ := f.item(t, "a"); v != 1 {
		t.Errorf("item a = %d, want 1", v)
	}
	if v, _ := f.item(t, "refused"); v != 2 {
		t.Errorf("item refused = %d, want 2", v)
	}
}

func TestFailedTasksCanBeListedAndResubmitted_Live(t *testing.T) {
	f := newFixture(t, 1)
	f.handle(t, qTop, "put", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if string(task.Args) == "stale" {
			return nil, taskrunner.Permanent(errors.New("authority spent"))
		}
		return nil, ex.Write(ctx, `INSERT INTO `+f.env.Table("items")+` (k, v) VALUES ($1, 1)`, task.Subject)
	})
	f.start(t)
	ctx := waitCtx(t)

	id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "space", Action: "put", Args: []byte("stale")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Wait(ctx, qTop, id); err == nil {
		t.Fatal("task succeeded")
	}
	failed, err := f.r.ListFailed(ctx, qTop, "space", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 1 || failed[0].Task.ID != id || failed[0].Error != "authority spent" || string(failed[0].Task.Args) != "stale" {
		t.Fatalf("ListFailed = %+v, want the failed task with its error", failed)
	}
	if other, _ := f.r.ListFailed(ctx, qTop, "elsewhere", 10); len(other) != 0 {
		t.Errorf("ListFailed for another subject = %+v", other)
	}

	fresh, err := f.r.Resubmit(ctx, qTop, id, []byte("renewed"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Wait(ctx, qTop, fresh); err != nil {
		t.Fatalf("resubmitted task: %v", err)
	}
	if v, _ := f.item(t, "space"); v != 1 {
		t.Errorf("item space = %d, want 1 from the resubmitted task", v)
	}
	if left, _ := f.r.ListFailed(ctx, qTop, "", 10); len(left) != 0 {
		t.Errorf("a resubmitted task is still listed: %+v", left)
	}
	if _, err := f.r.Resubmit(ctx, qTop, id, nil); !errors.Is(err, taskrunner.ErrNotFound) {
		t.Errorf("second Resubmit: %v, want ErrNotFound", err)
	}
	var fe *taskrunner.HandlerError
	if _, err := f.r.Wait(ctx, qTop, id); !errors.As(err, &fe) {
		t.Errorf("Wait on the resubmitted ID: %v, want its HandlerError", err)
	}
}

func TestAHandlerCanResubmitFailedTasks_Live(t *testing.T) {
	f := newFixture(t, 1)
	f.handle(t, qBottom, "remove", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if string(task.Args) == "stale" {
			return nil, taskrunner.Permanent(errors.New("authority spent"))
		}
		return nil, ex.Write(ctx, `INSERT INTO `+f.env.Table("items")+` (k, v) VALUES ($1, 1)`, task.Subject)
	})
	// The last chance to send a space's removes: resubmit each failed one with
	// the authority this request carries.
	f.handle(t, qTop, "drain", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		failed, err := ex.ListFailed(ctx, qBottom, task.Subject, 100)
		if err != nil {
			return nil, err
		}
		for _, ft := range failed {
			if _, err := ex.Resubmit(ctx, qBottom, ft.Task.ID, task.Args); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	f.start(t)
	ctx := waitCtx(t)

	id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qBottom, Subject: "space", Action: "remove", Args: []byte("stale")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Wait(ctx, qBottom, id); err == nil {
		t.Fatal("remove succeeded")
	}
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "space", Action: "drain", Args: []byte("fresh")}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if v, _ := f.item(t, "space"); v == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the resubmitted remove never ran")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestFinishedTasksAreDeletedPastTheirRetention_Live(t *testing.T) {
	f := newFixture(t, 1, func(o *taskrunner.Options) {
		o.RetentionInterval = 50 * time.Millisecond
		o.Queues[0].Retention = 100 * time.Millisecond
	})
	f.handle(t, qTop, "put", f.put())
	f.handle(t, qTop, "fail", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		return nil, errors.New("boom")
	})
	f.start(t)
	ctx := waitCtx(t)

	done, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "a", Action: "put", Args: []byte("1")})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "b", Action: "fail"})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []taskrunner.TaskID{done, failed} {
		_, _ = f.r.Wait(ctx, qTop, id)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := f.r.Wait(ctx, qTop, done); errors.Is(err, taskrunner.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a succeeded task outlived its retention")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if left, _ := f.r.ListFailed(ctx, qTop, "", 10); len(left) != 1 {
		t.Errorf("failed tasks = %d, want 1: failed tasks stay until resubmitted", len(left))
	}
}

func TestFailedGauge_Live(t *testing.T) {
	f, reader := metricsFixture(t, 1)
	f.handle(t, qTop, "fail", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		return nil, errors.New("boom")
	})
	f.start(t)
	ctx := waitCtx(t)
	id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "a", Action: "fail"})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.r.Wait(ctx, qTop, id)

	got := collect(t, reader)
	aa := []attribute.KeyValue{attribute.String("queue", qTop), attribute.String("action", "fail")}
	if v, ok := gaugeValue[int64](t, got["taskrunner.queue.failed"], aa...); !ok || v != 1 {
		t.Errorf("failed = %d (reported %v), want 1", v, ok)
	}
}

func TestQueueTablesAreTunedForChurn_Live(t *testing.T) {
	f := newFixture(t, 0)
	f.migrate(t)
	for _, table := range []string{"queue_top_tasks", "queue_top_leases"} {
		var opts []string
		if err := f.env.DB.QueryRow(context.Background(), `SELECT coalesce(reloptions, '{}') FROM pg_class
			WHERE oid = $1::regclass`, f.env.Table(table)).Scan(&opts); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"fillfactor=80", "autovacuum_vacuum_scale_factor=0.01"} {
			if !slices.Contains(opts, want) {
				t.Errorf("%s storage options %v lack %s", table, opts, want)
			}
		}
	}
}

// A handler removed or renamed in a deploy leaves its tasks failed, and they
// still show in the gauge though no handler for them is registered.
func TestFailedGaugeCountsUnregisteredActions_Live(t *testing.T) {
	f, reader := metricsFixture(t, 0)
	f.handle(t, qTop, "put", f.put())
	f.start(t)
	if _, err := f.env.DB.Exec(context.Background(), `INSERT INTO `+f.env.Table("queue_top_tasks")+`
		(id, subject, action, sync, state, error, finished) VALUES (gen_random_uuid(), 's', 'renamed', false, 'failed', 'no handler', now())`); err != nil {
		t.Fatal(err)
	}

	got := collect(t, reader)
	if v, ok := gaugeValue[int64](t, got["taskrunner.queue.failed"], attribute.String("queue", qTop), attribute.String("action", "renamed")); !ok || v != 1 {
		t.Errorf("failed for an unregistered action = %d (reported %v), want 1", v, ok)
	}
	if v, ok := gaugeValue[int64](t, got["taskrunner.queue.failed"], attribute.String("queue", qTop), attribute.String("action", "put")); !ok || v != 0 {
		t.Errorf("failed for a registered action with none = %d (reported %v), want 0", v, ok)
	}
}
