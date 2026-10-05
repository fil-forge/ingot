package s3frontend

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

// meterName scopes the backend's instruments.
const meterName = "github.com/fil-forge/ingot/s3frontend"

// Why a spool file was removed: the reason attribute on the eviction
// counters.
const (
	spoolRemovedReleased     = "released"      // its object was deleted or its upload abandoned
	spoolRemovedParked       = "parked"        // a multipart part parked on its provider
	spoolRemovedBudget       = "budget"        // the budget pass evicted it
	spoolRemovedBudgetForced = "budget_forced" // the forced pass evicted it inside a retention window
	spoolRemovedOrphan       = "orphan"        // the orphan pass found no intent for it
)

// spoolMetrics counts the files removed from the spool, by reason. The
// usage and budget gauges are observed by a callback (registerSpoolGauges).
type spoolMetrics struct {
	evicted   metric.Int64Counter // bytes
	evictions metric.Int64Counter // files
}

// newSpoolMetrics creates the spool instruments from mp and registers the
// gauges that read b. A failure to create an instrument is logged and leaves
// that instrument a no-op: metrics never stop the backend.
func newSpoolMetrics(mp metric.MeterProvider, b *Backend, logger *zap.Logger) (spoolMetrics, metric.Registration) {
	meter := mp.Meter(meterName)
	m := spoolMetrics{evicted: noop.Int64Counter{}, evictions: noop.Int64Counter{}}
	if c, err := meter.Int64Counter("ingot.spool.evicted", metric.WithUnit("By"),
		metric.WithDescription("Bytes removed from the local spool, by reason")); err == nil {
		m.evicted = c
	} else {
		logger.Warn("spool metric not created", zap.String("metric", "ingot.spool.evicted"), zap.Error(err))
	}
	if c, err := meter.Int64Counter("ingot.spool.evictions", metric.WithUnit("{file}"),
		metric.WithDescription("Files removed from the local spool, by reason")); err == nil {
		m.evictions = c
	} else {
		logger.Warn("spool metric not created", zap.String("metric", "ingot.spool.evictions"), zap.Error(err))
	}
	reg, err := registerSpoolGauges(meter, b)
	if err != nil {
		logger.Warn("spool gauges not registered", zap.Error(err))
	}
	return m, reg
}

// registerSpoolGauges reports the spool's usage and its budget.
func registerSpoolGauges(meter metric.Meter, b *Backend) (metric.Registration, error) {
	usage, err := meter.Int64ObservableGauge("ingot.spool.usage", metric.WithUnit("By"),
		metric.WithDescription("Bytes held by the local spool's files, writes in progress included"))
	if err != nil {
		return nil, err
	}
	budget, err := meter.Int64ObservableGauge("ingot.spool.budget", metric.WithUnit("By"),
		metric.WithDescription("The local spool's byte budget (spool_max_bytes); 0 means none"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		if b.spool == nil {
			return nil
		}
		o.ObserveInt64(usage, b.spool.Usage())
		o.ObserveInt64(budget, b.spoolMaxBytes)
		return nil
	}, usage, budget)
}

// removed records files removed from the spool for one reason. Nothing is
// recorded for a removal that freed nothing.
func (m spoolMetrics) removed(ctx context.Context, reason string, files, bytes int64) {
	if files == 0 {
		return
	}
	attrs := metric.WithAttributes(attribute.String("reason", reason))
	m.evictions.Add(ctx, files, attrs)
	m.evicted.Add(ctx, bytes, attrs)
}

// removedFile records one file removed for reason, if the removal freed
// anything: Spool.Remove frees nothing for a file already gone.
func (m spoolMetrics) removedFile(ctx context.Context, reason string, freed int64) {
	if freed > 0 {
		m.removed(ctx, reason, 1, freed)
	}
}

// CloseMetrics stops reporting the spool gauges. The backend still counts
// removals; with no reader those cost nothing. Server.Stop calls it, so a
// final export the host makes after Stop carries the counters but not the
// gauges.
func (b *Backend) CloseMetrics() error {
	if b.spoolGauges == nil {
		return nil
	}
	err := b.spoolGauges.Unregister()
	b.spoolGauges = nil
	return err
}
