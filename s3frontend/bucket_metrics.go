package s3frontend

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// bucketTotalsTimeout bounds the query each collection of the bucket gauges
// makes, so a slow database can't hold up the rest of the export.
const bucketTotalsTimeout = 5 * time.Second

// newBucketGauges registers the bucket and object total gauges with mp. A
// failure is logged and leaves them unregistered: metrics never stop the
// backend.
func newBucketGauges(mp metric.MeterProvider, b *Backend, logger *zap.Logger) metric.Registration {
	reg, err := registerBucketGauges(mp.Meter(meterName), b)
	if err != nil {
		logger.Warn("bucket gauges not registered", zap.Error(err))
		return nil
	}
	return reg
}

// registerBucketGauges reports how many buckets the gateway holds and how
// many objects are in them (registry.BucketTotals). Each collection sums the
// totals the buckets' rows already keep, one row per bucket, so it runs the
// query itself rather than reading a snapshot. The figures are the
// database's: every instance sharing it reports the same ones.
func registerBucketGauges(meter metric.Meter, b *Backend) (metric.Registration, error) {
	buckets, err := meter.Int64ObservableGauge("ingot.buckets.count", metric.WithUnit("{bucket}"),
		metric.WithDescription("Buckets in the database"))
	if err != nil {
		return nil, err
	}
	objects, err := meter.Int64ObservableGauge("ingot.objects.count", metric.WithUnit("{object}"),
		metric.WithDescription("Objects in those buckets, as the usage report counts them: every version of every key, delete markers included, and every part of a multipart upload still in progress"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if b.reg == nil {
			return nil
		}
		ctx, cancel := context.WithTimeout(ctx, bucketTotalsTimeout)
		defer cancel()
		t, err := b.reg.BucketTotals(ctx)
		if err != nil {
			return err
		}
		o.ObserveInt64(buckets, t.Buckets)
		o.ObserveInt64(objects, t.Objects)
		return nil
	}, buckets, objects)
}
