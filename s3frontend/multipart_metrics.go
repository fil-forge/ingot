package s3frontend

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

// The outcome of an acceptor pass: the outcome attribute on the pass counter.
var (
	passAccepted = metric.WithAttributes(attribute.String("outcome", "accepted"))
	passFailed   = metric.WithAttributes(attribute.String("outcome", "failed"))
)

// multipartMetrics counts the acceptors' passes and the Completes answered
// OperationAborted because a part they name is still queued. The age of the
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
		metric.WithDescription("Multipart acceptor passes that concluded parts, by outcome: accepted, or failed (retried after a backoff; a timed-out pass included)")); err == nil {
		m.acceptPasses = c
	} else {
		logger.Warn("multipart metrics: create accept_passes counter", zap.Error(err))
	}
	if c, err := meter.Int64Counter("ingot.multipart.complete_deferred", metric.WithUnit("{request}"),
		metric.WithDescription("CompleteMultipartUpload requests answered OperationAborted because a part they name was still queued for acceptance; botocore clients treat it as final")); err == nil {
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

// registerMultipartGauges reports how long the oldest queued part has
// waited, once the accept sweeper has measured it.
func registerMultipartGauges(meter metric.Meter, b *Backend) (metric.Registration, error) {
	age, err := meter.Int64ObservableGauge("ingot.multipart.parked_age", metric.WithUnit("s"),
		metric.WithDescription("Seconds the oldest parked part of an open or completing multipart upload has waited for acceptance; 0 when none waits. Parts are accepted within seconds, so growth means the acceptance queue is stuck"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		if b.parkedAgeKnown.Load() {
			o.ObserveInt64(age, b.parkedAgeSeconds.Load())
		}
		return nil
	}, age)
}

// pass records the outcome of an acceptor pass that concluded parts.
func (m multipartMetrics) pass(ctx context.Context, err error) {
	if err != nil {
		m.acceptPasses.Add(ctx, 1, passFailed)
		return
	}
	m.acceptPasses.Add(ctx, 1, passAccepted)
}
