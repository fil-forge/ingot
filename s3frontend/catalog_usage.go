package s3frontend

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// registerCatalogUsageGauges reports each bucket's catalog storage on Forge
// from the latest snapshot RefreshCatalogUsage took. Each series is labeled by
// the bucket's space DID rather than its name, which is customer data; a
// bucket recreated under the same name has a new space, so a new series.
func registerCatalogUsageGauges(meter metric.Meter, b *Backend) (metric.Registration, error) {
	forge, err := meter.Int64ObservableGauge("ingot.catalog.forge_bytes", metric.WithUnit("By"),
		metric.WithDescription("Bytes of the bucket's catalog segments shipped to Forge and not yet released (their CARs, not their index blobs), by space"))
	if err != nil {
		return nil, err
	}
	dead, err := meter.Int64ObservableGauge("ingot.catalog.dead_on_arrival_bytes", metric.WithUnit("By"),
		metric.WithDescription("Bytes of those segments' blocks that were already dead when their segment shipped, over the segments measured, by space"))
	if err != nil {
		return nil, err
	}
	unmeasured, err := meter.Int64ObservableGauge("ingot.catalog.unmeasured_bytes", metric.WithUnit("By"),
		metric.WithDescription("Bytes of shipped catalog segments whose dead-on-arrival bytes were not measured, by space"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		snapshot := b.catalogSnapshot.Load()
		if snapshot == nil {
			return nil
		}
		for _, u := range *snapshot {
			space := metric.WithAttributes(attribute.String("space", u.Space.String()))
			o.ObserveInt64(forge, u.Bytes, space)
			o.ObserveInt64(dead, u.DeadOnArrivalBytes, space)
			o.ObserveInt64(unmeasured, u.UnmeasuredBytes, space)
		}
		return nil
	}, forge, dead, unmeasured)
}

// ReportsCatalogUsage reports whether the backend has a catalog usage store
// to report from (Deps.CatalogUsage).
func (b *Backend) ReportsCatalogUsage() bool { return b.catalogUsage != nil }

// RefreshCatalogUsage replaces the snapshot the catalog gauges report with
// the registry's current figures, returning how many buckets it covers.
func (b *Backend) RefreshCatalogUsage(ctx context.Context) (int, error) {
	if b.catalogUsage == nil {
		return 0, nil
	}
	usage, err := b.catalogUsage.CatalogUsage(ctx)
	if err != nil {
		return 0, err
	}
	b.catalogSnapshot.Store(&usage)
	return len(usage), nil
}
