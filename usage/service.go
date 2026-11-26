package usage

import (
	"context"
	"errors"
	"fmt"
	"time"

	hiltclient "github.com/fil-forge/hilt/pkg/client"
	usagecmds "github.com/fil-forge/libforge/commands/s3/usage"
	"github.com/fil-forge/ucantone/did"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/registry"
)

const (
	// DefaultReportBatch is the most counts one report carries. A backlog
	// larger than this goes out as several reports.
	DefaultReportBatch = 1000
	// DefaultRetention is how long a count is kept once the auth service
	// holds it. Until then a report can be repeated if the service loses
	// its copy.
	DefaultRetention = 7 * 24 * time.Hour
	// meterName scopes the usage instruments.
	meterName = "github.com/fil-forge/ingot/usage"
	// backlogTimeout bounds the query each collection of the backlog gauges
	// makes, so a slow database can't hold up the rest of the export.
	backlogTimeout = 5 * time.Second
)

// Reporter sends usage counts to the auth service and returns its warnings
// about them, such as the spaces whose counts it did not store.
// [hiltclient.Client] implements it.
type Reporter interface {
	ReportUsage(ctx context.Context, report *usagecmds.ReportArguments, opts ...hiltclient.MethodOption) (*usagecmds.ReportOK, error)
}

// Config configures a [Service].
type Config struct {
	// Counter is what the responses count into. Required.
	Counter *Counter
	// Store holds the counts between reports: the egress the responses
	// count, and the object counts the bucket commits write. Required.
	Store registry.UsageStore
	// Reporter sends the counts to the auth service. Required.
	Reporter Reporter
	// Logger is optional.
	Logger *zap.Logger
	// MeterProvider is where the usage instruments are created; nil creates
	// none.
	MeterProvider metric.MeterProvider
	// ReportBatch is the most counts one report carries; zero takes
	// [DefaultReportBatch].
	ReportBatch int
	// Retention is how long an acknowledged count is kept; zero takes
	// [DefaultRetention].
	Retention time.Duration
}

// Service moves counted egress to Postgres, and from there to the auth
// service with the object counts the bucket commits write to Postgres
// themselves. A host calls [Service.Flush] every few seconds and
// [Service.Report] every few minutes.
type Service struct {
	counter   *Counter
	store     registry.UsageStore
	reporter  Reporter
	logger    *zap.Logger
	batch     int
	retention time.Duration

	warnings metric.Int64Counter
	gauges   metric.Registration
}

// New creates the service and registers its instruments.
func New(cfg Config) (*Service, error) {
	switch {
	case cfg.Counter == nil:
		return nil, errors.New("usage: counter is required")
	case cfg.Store == nil:
		return nil, errors.New("usage: store is required")
	case cfg.Reporter == nil:
		return nil, errors.New("usage: reporter is required")
	}
	s := &Service{
		counter:   cfg.Counter,
		store:     cfg.Store,
		reporter:  cfg.Reporter,
		logger:    cfg.Logger,
		batch:     cfg.ReportBatch,
		retention: cfg.Retention,
		warnings:  noop.Int64Counter{},
	}
	if s.logger == nil {
		s.logger = zap.NewNop()
	}
	if s.batch <= 0 {
		s.batch = DefaultReportBatch
	}
	if s.retention <= 0 {
		s.retention = DefaultRetention
	}
	if cfg.MeterProvider != nil {
		// A failure is logged and leaves the instruments unregistered:
		// metrics never stop the service.
		if err := s.registerMetrics(cfg.MeterProvider.Meter(meterName)); err != nil {
			s.logger.Warn("usage metrics not registered", zap.Error(err))
		}
	}
	return s, nil
}

// Flush adds what the responses have counted since the last flush to the
// store. If the store cannot take it, the counts stay in memory and go with
// the next flush.
//
// A write the database committed can still look like a failure here, when its
// reply is lost, and its counts are then added a second time. That overstates
// egress by at most one flush interval of this instance's traffic, which is
// accepted: preventing it takes a record of what each instance has added.
func (s *Service) Flush(ctx context.Context) error {
	counts := s.counter.take()
	if len(counts) == 0 {
		return nil
	}
	if err := s.store.AddEgress(ctx, counts); err != nil {
		s.counter.restore(counts)
		return fmt.Errorf("usage: flush: %w", err)
	}
	return nil
}

// ReportStats is what one [Service.Report] did.
type ReportStats struct {
	// Reports is how many reports the auth service accepted.
	Reports int
	// Counts is how many counts they carried.
	Counts int
	// Warnings is how many warnings the service answered them with.
	Warnings int
	// Pruned is how many acknowledged counts were deleted for age.
	Pruned int64
}

// Report sends the auth service every count that has changed since the
// service last acknowledged it, the egress and then the object counts, each
// oldest hour first, in as many reports as it takes. It then deletes the
// counts acknowledged longer ago than the retention. The hour in progress is
// among them: a count is a total so far and the service keeps the largest it
// is sent, so the hour is sent again each time it has grown.
//
// Instances sharing a database may report the same counts at once. The
// service ends up holding the largest total whichever arrives last.
//
// A report that fails ends the pass, and the next pass starts from the same
// counts. An accepted report settles every count in it: one the service
// warned about, because it did not store it, is acknowledged like the rest and
// not sent again. Each warning is logged and counted by its name.
func (s *Service) Report(ctx context.Context) (ReportStats, error) {
	var stats ReportStats
	if err := s.reportEgress(ctx, &stats); err != nil {
		return stats, fmt.Errorf("usage: report: %w", err)
	}
	if err := s.reportObjects(ctx, &stats); err != nil {
		return stats, fmt.Errorf("usage: report: %w", err)
	}
	before := time.Now().Add(-s.retention)
	pruned, err := s.store.PruneEgress(ctx, before)
	if err != nil {
		return stats, fmt.Errorf("usage: report: %w", err)
	}
	stats.Pruned = pruned
	if pruned, err = s.store.PruneObjects(ctx, before); err != nil {
		return stats, fmt.Errorf("usage: report: %w", err)
	}
	stats.Pruned += pruned
	return stats, nil
}

// reportEgress sends the egress that was waiting when it began. A count that
// grows while the pass runs is due again as soon as it is acknowledged, and
// is left to the next pass, so this one always ends.
func (s *Service) reportEgress(ctx context.Context, stats *ReportStats) error {
	backlog, err := s.store.EgressBacklog(ctx)
	if err != nil {
		return err
	}
	for remaining := backlog.Counts; remaining > 0; remaining -= int64(s.batch) {
		due, err := s.store.DueEgress(ctx, s.batch)
		if err != nil {
			return err
		}
		if len(due) == 0 {
			break
		}
		ok, err := s.reporter.ReportUsage(ctx, &usagecmds.ReportArguments{Egress: groupBySpace(due)})
		if err != nil {
			return err
		}
		if err := s.store.AcknowledgeEgress(ctx, due); err != nil {
			return err
		}
		stats.Reports++
		stats.Counts += len(due)
		stats.Warnings += s.noteWarnings(ctx, ok.Warnings, func(space did.DID) []zap.Field {
			var (
				counts int
				bytes  int64
			)
			for _, c := range due {
				if c.Space == space {
					counts++
					bytes += c.Bytes
				}
			}
			return []zap.Field{zap.Int("egress_counts", counts), zap.Int64("bytes", bytes)}
		})
	}
	return nil
}

// reportObjects sends the object counts that were waiting when it began, as
// reportEgress sends the egress.
func (s *Service) reportObjects(ctx context.Context, stats *ReportStats) error {
	backlog, err := s.store.ObjectBacklog(ctx)
	if err != nil {
		return err
	}
	for remaining := backlog.Counts; remaining > 0; remaining -= int64(s.batch) {
		due, err := s.store.DueObjects(ctx, s.batch)
		if err != nil {
			return err
		}
		if len(due) == 0 {
			break
		}
		ok, err := s.reporter.ReportUsage(ctx, &usagecmds.ReportArguments{Objects: groupObjectsBySpace(due)})
		if err != nil {
			return err
		}
		if err := s.store.AcknowledgeObjects(ctx, due); err != nil {
			return err
		}
		stats.Reports++
		stats.Counts += len(due)
		stats.Warnings += s.noteWarnings(ctx, ok.Warnings, func(space did.DID) []zap.Field {
			counts := 0
			for _, c := range due {
				if c.Space == space {
					counts++
				}
			}
			return []zap.Field{zap.Int("object_counts", counts)}
		})
	}
	return nil
}

// noteWarnings logs and counts the warnings the service answered a report
// with, each with what about tells of how much of the report was about its
// space, and returns how many there were. A name this version does not know
// is logged and counted like the rest.
func (s *Service) noteWarnings(ctx context.Context, warnings []usagecmds.Warning, about func(space did.DID) []zap.Field) int {
	for _, w := range warnings {
		s.warnings.Add(ctx, 1, metric.WithAttributes(attribute.String("name", w.Name)))
		fields := append([]zap.Field{
			zap.String("warning", w.Name), zap.String("message", w.Message), zap.Stringer("space", w.Space),
		}, about(w.Space)...)
		s.logger.Warn("the auth service warned about usage counts", fields...)
	}
	return len(warnings)
}

// CloseMetrics unregisters the service's gauges.
func (s *Service) CloseMetrics() error {
	if s.gauges == nil {
		return nil
	}
	return s.gauges.Unregister()
}

func (s *Service) registerMetrics(meter metric.Meter) error {
	warnings, err := meter.Int64Counter("ingot.usage.report_warnings", metric.WithUnit("{warning}"),
		metric.WithDescription("Warnings the auth service answered usage reports with, by name: UnknownBucket and BucketRegionMismatch each mean a bucket's counts were not stored"))
	if err != nil {
		return err
	}
	unreported, err := meter.Int64ObservableGauge("ingot.usage.egress_unreported_counts", metric.WithUnit("{count}"),
		metric.WithDescription("Egress counts that have changed since the auth service last acknowledged them"))
	if err != nil {
		return err
	}
	age, err := meter.Int64ObservableGauge("ingot.usage.egress_oldest_unreported_age", metric.WithUnit("s"),
		metric.WithDescription("Seconds the longest-waiting egress count has been due for reporting; 0 when none is. Counts are reported every few minutes, so growth means reports are failing"))
	if err != nil {
		return err
	}
	objectsUnreported, err := meter.Int64ObservableGauge("ingot.usage.object_unreported_counts", metric.WithUnit("{count}"),
		metric.WithDescription("Object counts that have changed since the auth service last acknowledged them"))
	if err != nil {
		return err
	}
	objectsAge, err := meter.Int64ObservableGauge("ingot.usage.object_oldest_unreported_age", metric.WithUnit("s"),
		metric.WithDescription("Seconds the longest-waiting object count has been due for reporting; 0 when none is. Counts are reported every few minutes, so growth means reports are failing"))
	if err != nil {
		return err
	}
	seconds := func(since time.Time) int64 {
		if since.IsZero() {
			return 0
		}
		return int64(time.Since(since).Seconds())
	}
	reg, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, backlogTimeout)
		defer cancel()
		b, err := s.store.EgressBacklog(ctx)
		if err != nil {
			return err
		}
		o.ObserveInt64(unreported, b.Counts)
		o.ObserveInt64(age, seconds(b.Oldest))
		ob, err := s.store.ObjectBacklog(ctx)
		if err != nil {
			return err
		}
		o.ObserveInt64(objectsUnreported, ob.Counts)
		o.ObserveInt64(objectsAge, seconds(ob.Oldest))
		return nil
	}, unreported, age, objectsUnreported, objectsAge)
	if err != nil {
		return err
	}
	s.warnings, s.gauges = warnings, reg
	return nil
}

// groupBySpace turns counts ordered by hour into the report's shape: one entry
// for each space, holding its hours in order.
func groupBySpace(counts []registry.EgressCount) []usagecmds.SpaceEgress {
	index := map[did.DID]int{}
	var out []usagecmds.SpaceEgress
	for _, c := range counts {
		i, ok := index[c.Space]
		if !ok {
			i = len(out)
			index[c.Space] = i
			out = append(out, usagecmds.SpaceEgress{Space: c.Space})
		}
		out[i].Hours = append(out[i].Hours, usagecmds.HourEgress{Hour: c.Hour.Unix(), Bytes: uint64(c.Bytes)})
	}
	return out
}

// groupObjectsBySpace turns object counts ordered by hour into the report's
// shape: one entry for each space, holding its hours in order.
func groupObjectsBySpace(counts []registry.ObjectCount) []usagecmds.SpaceObjects {
	index := map[did.DID]int{}
	var out []usagecmds.SpaceObjects
	for _, c := range counts {
		i, ok := index[c.Space]
		if !ok {
			i = len(out)
			index[c.Space] = i
			out = append(out, usagecmds.SpaceObjects{Space: c.Space})
		}
		out[i].Hours = append(out[i].Hours, usagecmds.HourObjects{
			Hour: c.Hour.Unix(), Added: uint64(c.Added), Removed: uint64(c.Removed),
		})
	}
	return out
}
