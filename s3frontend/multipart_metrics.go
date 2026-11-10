package s3frontend

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

// The outcome of a blob's conclude: the outcome attribute on the pass counter.
var (
	passAccepted = metric.WithAttributes(attribute.String("outcome", "accepted"))
	passFailed   = metric.WithAttributes(attribute.String("outcome", "failed"))
)

// multipartMetrics counts the multipart blobs' concludes and the Completes
// answered OperationAborted because the parts' accepts did not finish in time. The age of the
// oldest queued part is a gauge observed by a callback
// (registerMultipartGauges).
type multipartMetrics struct {
	acceptPasses     metric.Int64Counter
	completeDeferred metric.Int64Counter
}

// newMultipartMetrics creates the multipart instruments from mp and
// registers the gauge that reads b. A failure to create an instrument is
// logged and leaves that instrument a no-op: metrics never stop the backend.
func newMultipartMetrics(mp metric.MeterProvider, b *Backend, logger *zap.Logger) (multipartMetrics, metric.Registration) {
	meter := mp.Meter(meterName)
	m := multipartMetrics{acceptPasses: noop.Int64Counter{}, completeDeferred: noop.Int64Counter{}}
	if c, err := meter.Int64Counter("ingot.multipart.accept_passes", metric.WithUnit("{pass}"),
		metric.WithDescription("Multipart part blobs concluded, by outcome: accepted, or failed (the blob's accept retries; a timed-out conclude included)")); err == nil {
		m.acceptPasses = c
	} else {
		logger.Warn("multipart metrics: create accept_passes counter", zap.Error(err))
	}
	if c, err := meter.Int64Counter("ingot.multipart.complete_deferred", metric.WithUnit("{request}"),
		metric.WithDescription("CompleteMultipartUpload requests answered OperationAborted because the parts' accepts did not finish in time; botocore clients treat it as final")); err == nil {
		m.completeDeferred = c
	} else {
		logger.Warn("multipart metrics: create complete_deferred counter", zap.Error(err))
	}
	reg, err := registerMultipartGauges(meter, b)
	if err != nil {
		logger.Warn("multipart metrics: register gauges", zap.Error(err))
		return m, nil
	}
	return m, reg
}

// parkedAgeTimeout bounds the gauge's query of the oldest parked part.
const parkedAgeTimeout = 5 * time.Second

// registerMultipartGauges reports how long the oldest queued part has
// waited, read from the part rows when the gauge is collected.
func registerMultipartGauges(meter metric.Meter, b *Backend) (metric.Registration, error) {
	age, err := meter.Int64ObservableGauge("ingot.multipart.parked_age", metric.WithUnit("s"),
		metric.WithDescription("Seconds the oldest parked part of an open multipart upload has waited for acceptance; 0 when none waits. Parts are accepted within seconds, so growth means the acceptance queue is stuck"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if b.multipart == nil {
			return nil
		}
		ctx, cancel := context.WithTimeout(ctx, parkedAgeTimeout)
		defer cancel()
		oldest, found, err := b.multipart.OldestParkedPart(ctx)
		if err != nil {
			// Reported as missing rather than as no wait.
			b.logger.Debug("multipart metrics: oldest parked part", zap.Error(err))
			return nil
		}
		var secs int64
		if found {
			secs = int64(max(time.Since(oldest), 0) / time.Second)
		}
		o.ObserveInt64(age, secs)
		return nil
	}, age)
}

// pass records the outcome of a blob's conclude.
func (m multipartMetrics) pass(ctx context.Context, err error) {
	if err != nil {
		m.acceptPasses.Add(ctx, 1, passFailed)
		return
	}
	m.acceptPasses.Add(ctx, 1, passAccepted)
}
