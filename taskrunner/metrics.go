package taskrunner

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// meterName is the instrumentation scope of the runner's metrics.
const meterName = "github.com/fil-forge/ingot/taskrunner"

// statsTimeout bounds the query each collection of the queue gauges makes,
// so a slow database can't hold up the rest of the export.
const statsTimeout = 5 * time.Second

// Task outcomes, as the duration histogram labels them.
const (
	outcomeSucceeded = "succeeded"
	outcomeFailed    = "failed"
	outcomeRetried   = "retried"
	outcomeExpired   = "expired"
)

// metrics holds the runner's instruments. Every instrument is labelled by
// queue, and by action where it has one, so the runner reports nothing about
// what its handlers do.
type metrics struct {
	meter     metric.Meter
	duration  metric.Float64Histogram
	leaseWait metric.Float64Histogram
	takeovers metric.Int64Counter

	ready      metric.Int64ObservableGauge
	delayed    metric.Int64ObservableGauge
	oldest     metric.Float64ObservableGauge
	busy       metric.Int64ObservableGauge
	workers    metric.Int64ObservableGauge
	collecting metric.Registration
}

func newMetrics(mp metric.MeterProvider) (*metrics, error) {
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	meter := mp.Meter(meterName)
	m := &metrics{meter: meter}
	var err error
	if m.duration, err = meter.Float64Histogram("taskrunner.task.duration", metric.WithUnit("s"),
		metric.WithDescription("Time from a task starting to its outcome, labelled by outcome: succeeded, failed, retried or expired")); err != nil {
		return nil, err
	}
	if m.leaseWait, err = meter.Float64Histogram("taskrunner.lease.wait", metric.WithUnit("s"),
		metric.WithDescription("Time a sync task or sync child waited for its subject's lease")); err != nil {
		return nil, err
	}
	if m.takeovers, err = meter.Int64Counter("taskrunner.lease.takeovers", metric.WithUnit("{lease}"),
		metric.WithDescription("Leases taken over after they expired, which means a consumer stopped renewing: it crashed, stalled or lost its database connection")); err != nil {
		return nil, err
	}
	if m.ready, err = meter.Int64ObservableGauge("taskrunner.queue.ready", metric.WithUnit("{task}"),
		metric.WithDescription("Pending tasks that are due to run")); err != nil {
		return nil, err
	}
	if m.delayed, err = meter.Int64ObservableGauge("taskrunner.queue.delayed", metric.WithUnit("{task}"),
		metric.WithDescription("Pending tasks waiting on their NotBefore")); err != nil {
		return nil, err
	}
	if m.oldest, err = meter.Float64ObservableGauge("taskrunner.queue.oldest_ready_age", metric.WithUnit("s"),
		metric.WithDescription("How long the oldest due task has been due; 0 when none is. Growth means the queue's workers are not keeping up, or a subject is stuck")); err != nil {
		return nil, err
	}
	if m.busy, err = meter.Int64ObservableGauge("taskrunner.workers.busy", metric.WithUnit("{worker}"),
		metric.WithDescription("Workers running a queue's tasks in this process")); err != nil {
		return nil, err
	}
	if m.workers, err = meter.Int64ObservableGauge("taskrunner.workers.limit", metric.WithUnit("{worker}"),
		metric.WithDescription("The most workers this process runs for a queue")); err != nil {
		return nil, err
	}
	return m, nil
}

// observeTasks records the outcome and duration of tasks that started at
// start.
func (m *metrics) observeTasks(ctx context.Context, tasks []Task, outcome string, start time.Time) {
	d := time.Since(start).Seconds()
	for _, t := range tasks {
		m.duration.Record(ctx, d, metric.WithAttributes(
			attribute.String("queue", t.Queue), attribute.String("action", t.Action), attribute.String("outcome", outcome)))
	}
}

func (m *metrics) observeLeaseWait(ctx context.Context, q *queue, start time.Time) {
	m.leaseWait.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attribute.String("queue", q.name)))
}

func (m *metrics) observeTakeover(ctx context.Context, q *queue) {
	m.takeovers.Add(ctx, 1, metric.WithAttributes(attribute.String("queue", q.name)))
}

// startCollecting registers the gauges' callback, which reads each queue's
// pending tasks and this process's worker counts.
func (r *Runner) startCollecting() error {
	m := r.metrics
	collecting, err := m.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, statsTimeout)
		defer cancel()
		for _, q := range r.order {
			r.mu.Lock()
			busy := len(q.inflight)
			r.mu.Unlock()
			qa := metric.WithAttributes(attribute.String("queue", q.name))
			o.ObserveInt64(m.busy, int64(busy), qa)
			o.ObserveInt64(m.workers, int64(q.workers), qa)

			rows, err := r.db.Query(ctx, `SELECT action,
				count(*) FILTER (WHERE not_before IS NULL OR not_before <= now()),
				count(*) FILTER (WHERE not_before > now()),
				coalesce(extract(epoch FROM now() - min(greatest(created, coalesce(not_before, created)))
					FILTER (WHERE not_before IS NULL OR not_before <= now())), 0)::float8
				FROM `+q.tasks+` WHERE state = 'pending' GROUP BY action`)
			if err != nil {
				return err
			}
			for rows.Next() {
				var action string
				var ready, delayed int64
				var oldest float64
				if err := rows.Scan(&action, &ready, &delayed, &oldest); err != nil {
					rows.Close()
					return err
				}
				aa := metric.WithAttributes(attribute.String("queue", q.name), attribute.String("action", action))
				o.ObserveInt64(m.ready, ready, aa)
				o.ObserveInt64(m.delayed, delayed, aa)
				o.ObserveFloat64(m.oldest, oldest, aa)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
		}
		return nil
	}, m.ready, m.delayed, m.oldest, m.busy, m.workers)
	if err != nil {
		return err
	}
	m.collecting = collecting
	return nil
}
