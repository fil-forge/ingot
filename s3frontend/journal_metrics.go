package s3frontend

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// journalStatsTimeout bounds the query each collection of the journal gauges
// makes, so a slow database can't hold up the rest of the export.
const journalStatsTimeout = 5 * time.Second

// newJournalGauges registers the catalog journal gauges with mp. A failure is
// logged and leaves them unregistered: metrics never stop the backend.
func newJournalGauges(mp metric.MeterProvider, b *Backend, logger *zap.Logger) metric.Registration {
	reg, err := registerJournalGauges(mp.Meter(meterName), b)
	if err != nil {
		logger.Warn("catalog journal gauges not registered", zap.Error(err))
		return nil
	}
	return reg
}

// registerJournalGauges reports how much the catalog journal holds: the
// catalog blocks commits journaled in Postgres whose log segments haven't
// shipped yet (registry.RootCommit.Journal). Segments ship within seconds of
// sealing, so a journal that keeps growing, or whose oldest block keeps
// aging, means shipping has stalled and the unshipped catalog is piling up
// in Postgres.
func registerJournalGauges(meter metric.Meter, b *Backend) (metric.Registration, error) {
	blocks, err := meter.Int64ObservableGauge("ingot.catalog_journal.blocks", metric.WithUnit("{block}"),
		metric.WithDescription("Catalog blocks journaled in Postgres whose log segment has not shipped to Forge yet"))
	if err != nil {
		return nil, err
	}
	bytes, err := meter.Int64ObservableGauge("ingot.catalog_journal.bytes", metric.WithUnit("By"),
		metric.WithDescription("Bytes of the catalog blocks journaled in Postgres whose log segment has not shipped to Forge yet"))
	if err != nil {
		return nil, err
	}
	age, err := meter.Int64ObservableGauge("ingot.catalog_journal.oldest_age", metric.WithUnit("s"),
		metric.WithDescription("Seconds since the oldest journaled catalog block was written; 0 when the journal is empty. Segments ship within seconds of sealing, so growth means shipping has stalled"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if b.reg == nil {
			return nil
		}
		ctx, cancel := context.WithTimeout(ctx, journalStatsTimeout)
		defer cancel()
		st, err := b.reg.CatalogJournalStats(ctx)
		if err != nil {
			return err
		}
		o.ObserveInt64(blocks, st.Blocks)
		o.ObserveInt64(bytes, st.Bytes)
		var oldest int64
		if !st.Oldest.IsZero() {
			oldest = int64(time.Since(st.Oldest).Seconds())
		}
		o.ObserveInt64(age, oldest)
		return nil
	}, blocks, bytes, age)
}
