package taskrunner_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/taskrunner"
)

func node(n string) *string { return &n }

// peer returns a second runner over f's schema, as another instance named
// nodeName with its own workers.
func (f *fixture) peer(t *testing.T, nodeName string, workers int) *taskrunner.Runner {
	t.Helper()
	r, err := taskrunner.New(f.env.DB, taskrunner.Options{
		Schema: f.env.Schema,
		Queues: []taskrunner.QueueOptions{
			{Name: qTop, Workers: workers},
			{Name: qMiddle, Workers: workers},
			{Name: qBottom, Workers: workers},
		},
		LeaseTTL:     time.Second,
		PollInterval: 20 * time.Millisecond,
		Node:         nodeName,
		Logger:       zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestPlacedTasksRunOnlyOnTheirNode(t *testing.T) {
	f := newFixture(t, 2, func(o *taskrunner.Options) { o.Node = "a" })
	b := f.peer(t, "b", 2)
	var mu sync.Mutex
	ranOn := map[string]string{}
	record := func(name string) taskrunner.Handler {
		return func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
			mu.Lock()
			ranOn[string(task.Args)] = name
			mu.Unlock()
			return nil, nil
		}
	}
	f.handle(t, qTop, "cache", record("a"))
	if err := b.Handle(qTop, "cache", record("b")); err != nil {
		t.Fatal(err)
	}
	f.migrate(t)
	ctx := waitCtx(t)

	// Placed on b, then a task placed nowhere behind it on the same subject.
	placed, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "blob", Action: "cache", Args: []byte("placed"), Node: node("b")})
	if err != nil {
		t.Fatal(err)
	}
	behind, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "blob", Action: "cache", Args: []byte("behind")})
	if err != nil {
		t.Fatal(err)
	}
	f.start(t)
	time.Sleep(300 * time.Millisecond)
	for _, id := range []taskrunner.TaskID{placed, behind} {
		if s := f.state(t, qTop, id); s != "pending" {
			t.Errorf("a ran a task placed on b, or overtook it: task is %s", s)
		}
	}

	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	for _, id := range []taskrunner.TaskID{placed, behind} {
		if _, err := f.r.Wait(ctx, qTop, id); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if ranOn["placed"] != "b" {
		t.Errorf("the placed task ran on %q, want b", ranOn["placed"])
	}
}

func TestSyncTasksPlacedElsewhereAreRefused(t *testing.T) {
	f := newFixture(t, 0, func(o *taskrunner.Options) { o.Node = "a" })
	noop := func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) { return nil, nil }
	f.handle(t, qBottom, "noop", noop)
	f.handle(t, qTop, "noop", noop)
	f.handle(t, qTop, "parent", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		_, err := ex.Run(ctx, taskrunner.Spec{Queue: qBottom, Subject: "c", Action: "noop", Node: node(string(task.Args))})
		return nil, err
	})
	f.start(t)
	ctx := waitCtx(t)

	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "noop", Node: node("b")}); !errors.Is(err, taskrunner.ErrOtherNode) {
		t.Errorf("Run placed on b: %v, want ErrOtherNode", err)
	}
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "parent", Args: []byte("b")}); !errors.Is(err, taskrunner.ErrOtherNode) {
		t.Errorf("child placed on b: %v, want ErrOtherNode", err)
	}
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "parent", Args: []byte("a")}); err != nil {
		t.Errorf("child placed on this node: %v", err)
	}
}

// metricsFixture is a fixture whose runner reports to a manual reader.
func metricsFixture(t *testing.T, workers int) (*fixture, *sdkmetric.ManualReader) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return newFixture(t, workers, func(o *taskrunner.Options) { o.MeterProvider = mp }), reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Aggregation {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]metricdata.Aggregation{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m.Data
		}
	}
	return out
}

func gaugeValue[N int64 | float64](t *testing.T, data metricdata.Aggregation, attrs ...attribute.KeyValue) (N, bool) {
	t.Helper()
	g, ok := data.(metricdata.Gauge[N])
	if !ok {
		return 0, false
	}
	want := attribute.NewSet(attrs...)
	for _, dp := range g.DataPoints {
		if dp.Attributes.Equals(&want) {
			return dp.Value, true
		}
	}
	return 0, false
}

func TestQueueGauges(t *testing.T) {
	f, reader := metricsFixture(t, 0)
	f.handle(t, qTop, "put", f.put())
	f.start(t)
	ctx := waitCtx(t)
	if _, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "a", Action: "put", Args: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "b", Action: "put", Args: []byte("1"), NotBefore: at(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	got := collect(t, reader)
	aa := []attribute.KeyValue{attribute.String("queue", qTop), attribute.String("action", "put")}
	if v, ok := gaugeValue[int64](t, got["taskrunner.queue.ready"], aa...); !ok || v != 1 {
		t.Errorf("ready = %d (reported %v), want 1", v, ok)
	}
	if v, ok := gaugeValue[int64](t, got["taskrunner.queue.delayed"], aa...); !ok || v != 1 {
		t.Errorf("delayed = %d (reported %v), want 1", v, ok)
	}
	if v, ok := gaugeValue[float64](t, got["taskrunner.queue.oldest_ready_age"], aa...); !ok || v <= 0 {
		t.Errorf("oldest_ready_age = %v (reported %v), want above 0", v, ok)
	}
	qa := attribute.String("queue", qTop)
	if v, ok := gaugeValue[int64](t, got["taskrunner.workers.limit"], qa); !ok || v != 0 {
		t.Errorf("workers.limit = %d (reported %v), want 0", v, ok)
	}
	if _, ok := gaugeValue[int64](t, got["taskrunner.workers.busy"], qa); !ok {
		t.Error("workers.busy not reported")
	}
}

func TestTaskOutcomesAndTakeovers(t *testing.T) {
	f, reader := metricsFixture(t, 1)
	f.handle(t, qTop, "put", f.put())
	f.handle(t, qTop, "fail", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		return nil, errors.New("boom")
	})
	ctx := waitCtx(t)
	f.migrate(t)
	// A crashed consumer's lease, which the worker must take over.
	crashed, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "crashed", Action: "put", Args: []byte("1")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.env.DB.Exec(ctx, `INSERT INTO `+f.env.Table("queue_top_leases")+` (subject, owner, expires, tree_created, tree_id)
		VALUES ('crashed', $1, now() + interval '200 milliseconds', now(), $2)`, uuid.New(), uuid.New()); err != nil {
		t.Fatal(err)
	}
	f.start(t)
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "put", Args: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "fail"}); err == nil {
		t.Fatal("fail succeeded")
	}
	if _, err := f.r.Wait(ctx, qTop, crashed); err != nil {
		t.Fatal(err)
	}

	got := collect(t, reader)
	h, ok := got["taskrunner.task.duration"].(metricdata.Histogram[float64])
	if !ok {
		t.Fatal("task.duration not reported")
	}
	counts := map[string]uint64{}
	for _, dp := range h.DataPoints {
		outcome, _ := dp.Attributes.Value("outcome")
		action, _ := dp.Attributes.Value("action")
		counts[action.AsString()+"/"+outcome.AsString()] += dp.Count
	}
	if counts["put/succeeded"] != 2 || counts["fail/failed"] != 1 {
		t.Errorf("outcomes %v, want put/succeeded 2 and fail/failed 1", counts)
	}
	if _, ok := got["taskrunner.lease.wait"].(metricdata.Histogram[float64]); !ok {
		t.Error("lease.wait not reported")
	}
	takeovers, ok := got["taskrunner.lease.takeovers"].(metricdata.Sum[int64])
	if !ok || len(takeovers.DataPoints) != 1 || takeovers.DataPoints[0].Value != 1 {
		t.Errorf("lease.takeovers = %+v, want 1", takeovers.DataPoints)
	}
}

func TestQueueGaugesReadZeroOnceDrained(t *testing.T) {
	f, reader := metricsFixture(t, 1)
	f.handle(t, qTop, "put", f.put())
	f.start(t)
	ctx := waitCtx(t)
	id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "a", Action: "put", Args: []byte("1")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Wait(ctx, qTop, id); err != nil {
		t.Fatal(err)
	}

	got := collect(t, reader)
	aa := []attribute.KeyValue{attribute.String("queue", qTop), attribute.String("action", "put")}
	for _, name := range []string{"taskrunner.queue.ready", "taskrunner.queue.delayed"} {
		if v, ok := gaugeValue[int64](t, got[name], aa...); !ok || v != 0 {
			t.Errorf("%s = %d (reported %v), want 0 once drained", name, v, ok)
		}
	}
	if v, ok := gaugeValue[float64](t, got["taskrunner.queue.oldest_ready_age"], aa...); !ok || v != 0 {
		t.Errorf("oldest_ready_age = %v (reported %v), want 0 once drained", v, ok)
	}
}

func TestSyncChildOutcomesAreRecorded(t *testing.T) {
	f, reader := metricsFixture(t, 0)
	f.handle(t, qBottom, "child", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if string(task.Args) == "fail" {
			return nil, errors.New("child failed")
		}
		return nil, nil
	})
	f.handle(t, qTop, "parent", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		_, err := ex.Run(ctx, taskrunner.Spec{Queue: qBottom, Subject: "c", Action: "child", Args: task.Args})
		return nil, err
	})
	f.start(t)
	ctx := waitCtx(t)
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "p", Action: "parent", Args: []byte("ok")}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "p", Action: "parent", Args: []byte("fail")}); err == nil {
		t.Fatal("parent succeeded")
	}

	h, ok := collect(t, reader)["taskrunner.task.duration"].(metricdata.Histogram[float64])
	if !ok {
		t.Fatal("task.duration not reported")
	}
	counts := map[string]uint64{}
	for _, dp := range h.DataPoints {
		queue, _ := dp.Attributes.Value("queue")
		outcome, _ := dp.Attributes.Value("outcome")
		if queue.AsString() == qBottom {
			counts[outcome.AsString()] += dp.Count
		}
	}
	if counts["succeeded"] != 1 || counts["failed"] != 1 {
		t.Errorf("child outcomes %v, want succeeded 1 and failed 1", counts)
	}
}

func TestTaskWaitingOnAHoldingRetryCountsAsDelayed(t *testing.T) {
	f, reader := metricsFixture(t, 1)
	var runs atomic.Int32
	if err := f.r.Handle(qTop, "add", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		runs.Add(1)
		return nil, errors.New("unavailable")
	}, taskrunner.WithRetry(taskrunner.RetryPolicy{First: time.Hour, HoldsSubject: true})); err != nil {
		t.Fatal(err)
	}
	f.start(t)
	ctx := waitCtx(t)
	if _, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "space", Action: "add"}); err != nil {
		t.Fatal(err)
	}
	for runs.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	got := collect(t, reader)
	aa := []attribute.KeyValue{attribute.String("queue", qTop), attribute.String("action", "add")}
	if v, _ := gaugeValue[int64](t, got["taskrunner.queue.ready"], aa...); v != 0 {
		t.Errorf("ready = %d, want 0: the task waits on its retry", v)
	}
	if v, _ := gaugeValue[int64](t, got["taskrunner.queue.delayed"], aa...); v != 1 {
		t.Errorf("delayed = %d, want 1", v)
	}
	if v, _ := gaugeValue[float64](t, got["taskrunner.queue.oldest_ready_age"], aa...); v != 0 {
		t.Errorf("oldest_ready_age = %v, want 0", v)
	}
}

func TestEmptyNodeNamesAreRefused(t *testing.T) {
	f := newFixture(t, 0)
	f.handle(t, qTop, "put", f.put())
	f.start(t)
	ctx := waitCtx(t)
	spec := taskrunner.Spec{Queue: qTop, Subject: "a", Action: "put", Args: []byte("1"), Node: node("")}
	if _, err := f.r.Submit(ctx, spec); err == nil {
		t.Error("Submit accepted an empty node name")
	}
	if _, err := f.r.Run(ctx, spec); err == nil {
		t.Error("Run accepted an empty node name")
	}
}

func TestMetricsReachTheGlobalProviderByDefault(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = mp.Shutdown(context.Background())
	})
	f := newFixture(t, 0)
	f.handle(t, qTop, "put", f.put())
	f.start(t)
	if _, err := f.r.Run(waitCtx(t), taskrunner.Spec{Queue: qTop, Subject: "a", Action: "put", Args: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if _, ok := collect(t, reader)["taskrunner.task.duration"]; !ok {
		t.Error("a runner given no meter provider reported nothing to the global one")
	}
}
