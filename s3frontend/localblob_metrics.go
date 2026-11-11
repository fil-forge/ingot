package s3frontend

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/queue/shardqueue"
)

// meterName scopes the backend's instruments.
const meterName = "github.com/fil-forge/ingot/s3frontend"

// Why a local blob file was removed: the reason attribute on the removal
// counters.
const (
	removedReleased = "released" // its object was deleted
	removedBudget   = "budget"   // the budget pass evicted it
	removedExpired  = "expired"  // its time in the cache ran out
	removedOrphan   = "orphan"   // the orphan pass found it left by an unfinished write
)

// localBlobMetrics counts the files removed from local blob storage (the
// cache), by reason. The usage and budget gauges are observed
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
	inCache = metric.WithAttributes(attribute.String("dir", "cache"))
	// The intent state a stalled upload's bytes are in.
	stalledUploading = metric.WithAttributes(attribute.String("state", "uploading"))
)

// registerLocalBlobGauges reports the bytes the cache holds, writes in
// progress included, its budget, and the bytes of stalled uploads once a
// sweep has summed them.
func registerLocalBlobGauges(meter metric.Meter, b *Backend) (metric.Registration, error) {
	usage, err := meter.Int64ObservableGauge("ingot.local_blobs.usage", metric.WithUnit("By"),
		metric.WithDescription("Bytes held by local blob storage, by dir: cache (cached bodies and writes in progress)"))
	if err != nil {
		return nil, err
	}
	budget, err := meter.Int64ObservableGauge("ingot.local_blobs.budget", metric.WithUnit("By"),
		metric.WithDescription("The byte budget for local blob storage (local_blob_max_bytes); 0 means none"))
	if err != nil {
		return nil, err
	}
	stalled, err := meter.Int64ObservableGauge("ingot.local_blobs.stalled_bytes", metric.WithUnit("By"),
		metric.WithDescription("Bytes of bodies whose upload has stalled, by state: uploading (may have reached its provider) for over an hour; a provider expires an allocation never concluded within a day, and one accepted before a crash stays until reconciled"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		if b.cache == nil {
			return nil
		}
		o.ObserveInt64(usage, b.cache.Usage(), inCache)
		o.ObserveInt64(budget, b.localBlobMaxBytes)
		if b.stalledKnown.Load() {
			o.ObserveInt64(stalled, b.stalledUploading.Load(), stalledUploading)
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

// cacheRemoved records a copy an eviction removed.
func (b *Backend) cacheRemoved(ctx context.Context, why shardqueue.Removal, freed int64) {
	reason := removedExpired
	if why == shardqueue.Released {
		reason = removedReleased
	}
	b.localBlobMetrics.removedFile(ctx, reason, freed)
}

// removedFile records one file removed for reason, if the removal freed
// anything: a removal frees nothing for a file already gone.
func (m localBlobMetrics) removedFile(ctx context.Context, reason string, freed int64) {
	if freed > 0 {
		m.removed(ctx, reason, 1, freed)
	}
}

// CloseMetrics stops reporting the local blob, multipart, catalog journal
// and catalog usage gauges. The backend still counts; with no reader those
// cost nothing. Server.Stop calls it, so a final export the host makes after
// Stop carries the counters but not the gauges.
func (b *Backend) CloseMetrics() error {
	var errs []error
	if b.localBlobGauges != nil {
		errs = append(errs, b.localBlobGauges.Unregister())
		b.localBlobGauges = nil
	}
	if b.multipartGauges != nil {
		errs = append(errs, b.multipartGauges.Unregister())
		b.multipartGauges = nil
	}
	if b.journalGauges != nil {
		errs = append(errs, b.journalGauges.Unregister())
		b.journalGauges = nil
	}
	if b.catalogGauges != nil {
		errs = append(errs, b.catalogGauges.Unregister())
		b.catalogGauges = nil
	}
	return errors.Join(errs...)
}
