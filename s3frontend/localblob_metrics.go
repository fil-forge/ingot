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

// Why a local blob file was removed: the reason attribute on the removal
// counters.
const (
	removedReleased = "released" // its object was deleted or its upload abandoned
	removedParked   = "parked"   // a multipart part parked on its provider
	removedBudget   = "budget"   // the budget pass evicted it
	removedOrphan   = "orphan"   // the orphan pass found no intent for it
)

// localBlobMetrics counts the files removed from local blob storage (the
// spool and the cache), by reason. The usage and budget gauges are observed
// by a callback (registerLocalBlobGauges).
type localBlobMetrics struct {
	removedBytes metric.Int64Counter
	removals     metric.Int64Counter // files
}

// newLocalBlobMetrics creates the local blob instruments from mp and
// registers the gauges that read b. A failure to create an instrument is
// logged and leaves that instrument a no-op: metrics never stop the backend.
func newLocalBlobMetrics(mp metric.MeterProvider, b *Backend, logger *zap.Logger) (localBlobMetrics, metric.Registration) {
	meter := mp.Meter(meterName)
	m := localBlobMetrics{removedBytes: noop.Int64Counter{}, removals: noop.Int64Counter{}}
	if c, err := meter.Int64Counter("ingot.local_blobs.removed_bytes", metric.WithUnit("By"),
		metric.WithDescription("Bytes removed from local blob storage, by reason")); err == nil {
		m.removedBytes = c
	} else {
		logger.Warn("local blob metric not created", zap.String("metric", "ingot.local_blobs.removed_bytes"), zap.Error(err))
	}
	if c, err := meter.Int64Counter("ingot.local_blobs.removals", metric.WithUnit("{file}"),
		metric.WithDescription("Files removed from local blob storage, by reason")); err == nil {
		m.removals = c
	} else {
		logger.Warn("local blob metric not created", zap.String("metric", "ingot.local_blobs.removals"), zap.Error(err))
	}
	reg, err := registerLocalBlobGauges(meter, b)
	if err != nil {
		logger.Warn("local blob gauges not registered", zap.Error(err))
	}
	return m, reg
}

var (
	inSpool = metric.WithAttributes(attribute.String("dir", "spool"))
	inCache = metric.WithAttributes(attribute.String("dir", "cache"))
)

// registerLocalBlobGauges reports the bytes each directory holds, the budget
// they share, and the bytes of stalled uploads once a sweep has summed them.
// The spool's figure includes writes in progress, and is what eviction cannot
// touch.
func registerLocalBlobGauges(meter metric.Meter, b *Backend) (metric.Registration, error) {
	usage, err := meter.Int64ObservableGauge("ingot.local_blobs.usage", metric.WithUnit("By"),
		metric.WithDescription("Bytes held by local blob storage, by dir: spool (writes in progress and bodies awaiting upload) or cache"))
	if err != nil {
		return nil, err
	}
	budget, err := meter.Int64ObservableGauge("ingot.local_blobs.budget", metric.WithUnit("By"),
		metric.WithDescription("The byte budget for local blob storage (local_blob_max_bytes); 0 means none"))
	if err != nil {
		return nil, err
	}
	stalled, err := meter.Int64ObservableGauge("ingot.local_blobs.stalled_bytes", metric.WithUnit("By"),
		metric.WithDescription("Bytes of bodies whose upload has stalled: spooled or uploading for over an hour, which nothing reclaims yet"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		if b.spool == nil || b.cache == nil {
			return nil
		}
		o.ObserveInt64(usage, b.spool.Usage(), inSpool)
		o.ObserveInt64(usage, b.cache.Usage(), inCache)
		o.ObserveInt64(budget, b.localBlobMaxBytes)
		if b.stalledKnown.Load() {
			o.ObserveInt64(stalled, b.stalledBytes.Load())
		}
		return nil
	}, usage, budget, stalled)
}

// removed records files removed for one reason. Nothing is recorded for a
// removal that freed nothing.
func (m localBlobMetrics) removed(ctx context.Context, reason string, files, bytes int64) {
	if files == 0 {
		return
	}
	attrs := metric.WithAttributes(attribute.String("reason", reason))
	m.removals.Add(ctx, files, attrs)
	m.removedBytes.Add(ctx, bytes, attrs)
}

// removedFile records one file removed for reason, if the removal freed
// anything: a removal frees nothing for a file already gone.
func (m localBlobMetrics) removedFile(ctx context.Context, reason string, freed int64) {
	if freed > 0 {
		m.removed(ctx, reason, 1, freed)
	}
}

// CloseMetrics stops reporting the local blob gauges. The backend still
// counts removals; with no reader those cost nothing. Server.Stop calls it,
// so a final export the host makes after Stop carries the counters but not
// the gauges.
func (b *Backend) CloseMetrics() error {
	if b.localBlobGauges == nil {
		return nil
	}
	err := b.localBlobGauges.Unregister()
	b.localBlobGauges = nil
	return err
}
