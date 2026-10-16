package s3frontend

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/fil-forge/ingot/registry"
)

// The state of a bucket's last catalog walk, for the coverage gauge: current
// if it reached the bucket's present root, stale if the root has moved since,
// never if no walk has completed.
var (
	walkCurrent = metric.WithAttributes(attribute.String("state", "current"))
	walkStale   = metric.WithAttributes(attribute.String("state", "stale"))
	walkNever   = metric.WithAttributes(attribute.String("state", "never"))
)

// registerCatalogUsageGauges reports each bucket's catalog storage on Forge
// from the latest snapshot RefreshCatalogUsage took. Each series is labeled by
// the bucket's space DID rather than its name, which is customer data; a
// bucket recreated under the same name has a new space, so a new series
// (though see registry.CatalogUsage on rows a failed cleanup left behind).
// The live and dead figures come from the bucket's last completed catalog
// walk (see walkedDead for which moment the dead ones are of); the coverage
// gauge sums the shipped bytes of every bucket by the state of its walk, so
// a reader can see how much of the catalog the walked figures speak for.
func registerCatalogUsageGauges(meter metric.Meter, b *Backend) (metric.Registration, error) {
	var errs []error
	gauge := func(name, unit, desc string) metric.Int64ObservableGauge {
		g, err := meter.Int64ObservableGauge(name, metric.WithUnit(unit), metric.WithDescription(desc))
		errs = append(errs, err)
		return g
	}
	forge := gauge("ingot.catalog.forge_bytes", "By",
		"Bytes of the bucket's catalog segments shipped to Forge and not yet released (their CARs, not their index blobs), by space")
	deadOnArrival := gauge("ingot.catalog.dead_on_arrival_bytes", "By",
		"Bytes of those segments' blocks that were already dead when their segment shipped, over the segments measured, by space")
	unmeasured := gauge("ingot.catalog.unmeasured_bytes", "By",
		"Bytes of shipped catalog segments whose dead-on-arrival bytes were not measured, by space")
	live := gauge("ingot.catalog.live_bytes", "By",
		"Bytes of the catalog blocks reachable from the root the bucket's last catalog walk reached, by space")
	dead := gauge("ingot.catalog.dead_bytes", "By",
		"Shipped catalog bytes the bucket's root doesn't reach: now while the walked root is still the bucket's, else as of the walk's start, by space")
	deadAfterShip := gauge("ingot.catalog.dead_after_ship_bytes", "By",
		"The part of dead_bytes not already dead on arrival: blocks that died after their segment shipped (and the dead blocks of unmeasured segments), by space")
	walkedAt := gauge("ingot.catalog.walked_at", "s",
		"When the bucket's last completed catalog walk started, in Unix seconds, by space")
	coverage := gauge("ingot.catalog.walk_coverage_bytes", "By",
		"Shipped catalog bytes of all buckets, by the state of each bucket's last catalog walk: current, stale (its root has moved since) or never")
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		snapshot := b.catalogSnapshot.Load()
		if snapshot == nil {
			return nil
		}
		var current, stale, never int64
		for _, u := range *snapshot {
			space := metric.WithAttributes(attribute.String("space", u.Space.String()))
			o.ObserveInt64(forge, u.Bytes, space)
			o.ObserveInt64(deadOnArrival, u.DeadOnArrivalBytes, space)
			o.ObserveInt64(unmeasured, u.UnmeasuredBytes, space)
			switch {
			case u.Walk == nil:
				never += u.Bytes
				continue
			case u.Walk.Root.Equals(u.Root):
				current += u.Bytes
			default:
				stale += u.Bytes
			}
			d, onArrival := walkedDead(u)
			o.ObserveInt64(live, u.Walk.LiveBytes, space)
			o.ObserveInt64(dead, d, space)
			o.ObserveInt64(deadAfterShip, max(0, d-onArrival), space)
			o.ObserveInt64(walkedAt, u.Walk.StartedAt, space)
		}
		o.ObserveInt64(coverage, current, walkCurrent)
		o.ObserveInt64(coverage, stale, walkStale)
		o.ObserveInt64(coverage, never, walkNever)
		return nil
	}, forge, deadOnArrival, unmeasured, live, dead, deadAfterShip, walkedAt, coverage)
}

// walkedDead is the shipped catalog bytes u's root doesn't reach, and how
// much of them was dead on arrival, for a bucket with a completed walk. Live
// bytes depend only on the root, so while the walked root is still the
// bucket's, they are set against its usage now, which counts every segment
// shipped since the walk; once the root has moved, against its usage when the
// walk started. A block reached only in a segment not yet shipped counts as
// live without counting as shipped, so the difference can come out below
// zero; it reads as none.
func walkedDead(u registry.CatalogUsage) (dead, onArrival int64) {
	forge, onArrival := u.Walk.ForgeBytes, u.Walk.DeadOnArrivalBytes
	if u.Walk.Root.Equals(u.Root) {
		forge, onArrival = u.Bytes, u.DeadOnArrivalBytes
	}
	return max(0, forge-u.Walk.LiveBytes), onArrival
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
