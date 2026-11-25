package usage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	hiltclient "github.com/fil-forge/hilt/pkg/client"
	usagecmds "github.com/fil-forge/libforge/commands/s3/usage"
	"github.com/fil-forge/ucantone/did"
	"github.com/google/uuid"
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
	ReportUsage(ctx context.Context, egress []usagecmds.SpaceEgress, opts ...hiltclient.MethodOption) (*usagecmds.ReportOK, error)
}

// Config configures a [Service].
type Config struct {
	// Counter is what the responses count into. Required.
	Counter *Counter
	// Store holds the counts between reports. Required.
	Store registry.EgressUsageStore
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

// Service moves counted usage to Postgres and from there to the auth service.
// A host calls [Service.Flush] every few seconds and [Service.Report] every
// few minutes.
type Service struct {
	counter   *Counter
	store     registry.EgressUsageStore
	reporter  Reporter
	logger    *zap.Logger
	batch     int
	retention time.Duration

	// flushMu serialises flushes and guards the fields below it.
	flushMu sync.Mutex
	// instance identifies this process to the store.
	instance string
	// seq is the number of the last batch taken from the counter.
	seq int64
	// pending is a batch the store did not confirm, kept apart from the
	// counter so it goes again under the same number.
	pending []registry.EgressCount
	// pendingSince is when pending was taken from the counter.
	pendingSince time.Time
	// now is the clock, which tests move.
	now func() time.Time

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
		instance:  uuid.NewString(),
		now:       time.Now,
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
// store.
//
// A failed add may still have taken effect, so its batch is kept as it was
// and the next flush sends it again under the same number, which the store
// adds at most once. What is counted in the meantime follows as a new batch.
//
// The store forgets an instance's number once nothing has been added under it
// for the retention, and a batch sent again after that would count twice. So
// a batch still unconfirmed after half the retention is given up: it is
// logged and its bytes go uncounted, where sending it could count them twice.
func (s *Service) Flush(ctx context.Context) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	for {
		if s.pending != nil && s.now().Sub(s.pendingSince) > s.retention/2 {
			var bytes int64
			for _, c := range s.pending {
				bytes += c.Bytes
			}
			s.logger.Error("giving up on egress counts the database never confirmed",
				zap.Int("counts", len(s.pending)), zap.Int64("bytes", bytes),
				zap.Time("since", s.pendingSince))
			s.pending = nil
		}
		retry := s.pending != nil
		if !retry {
			counts := s.counter.take()
			if len(counts) == 0 {
				return nil
			}
			s.seq++
			s.pending, s.pendingSince = counts, s.now()
		}
		flush := registry.EgressFlush{Instance: s.instance, Seq: s.seq}
		if err := s.store.AddEgress(ctx, flush, s.pending); err != nil {
			return fmt.Errorf("usage: flush: %w", err)
		}
		s.pending = nil
		if !retry {
			return nil
		}
	}
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
// service last acknowledged it, oldest hour first, in as many reports as it
// takes, then deletes the counts acknowledged longer ago than the retention.
// The hour in progress is among them: a count is an hour's total so far and
// the service replaces what it holds, so the hour is sent again each time it
// has grown.
//
// A report that fails ends the pass, and the next pass starts from the same
// counts. An accepted report settles every count in it: one the service
// warned about, because it did not store it, is acknowledged like the rest and
// not sent again. Each warning is logged and counted by its name.
func (s *Service) Report(ctx context.Context) (ReportStats, error) {
	var stats ReportStats
	// A count that grows while the pass runs is due again as soon as it is
	// acknowledged. The pass sends what was waiting when it began, and leaves
	// what has become due since to the next one, so it always ends.
	backlog, err := s.store.EgressBacklog(ctx)
	if err != nil {
		return stats, fmt.Errorf("usage: report: %w", err)
	}
	for remaining := backlog.Counts; remaining > 0; remaining -= int64(s.batch) {
		due, err := s.store.DueEgress(ctx, s.batch)
		if err != nil {
			return stats, fmt.Errorf("usage: report: %w", err)
		}
		if len(due) == 0 {
			break
		}
		ok, err := s.reporter.ReportUsage(ctx, groupBySpace(due))
		if err != nil {
			return stats, fmt.Errorf("usage: report: %w", err)
		}
		if err := s.store.AcknowledgeEgress(ctx, due); err != nil {
			return stats, fmt.Errorf("usage: report: %w", err)
		}
		stats.Reports++
		stats.Counts += len(due)
		stats.Warnings += s.noteWarnings(ctx, due, ok.Warnings)
	}
	pruned, err := s.store.PruneEgress(ctx, s.now().Add(-s.retention))
	if err != nil {
		return stats, fmt.Errorf("usage: report: %w", err)
	}
	stats.Pruned = pruned
	return stats, nil
}

// noteWarnings logs and counts the warnings the service answered a report
// with, each with how much of the report was about its space, and returns how
// many there were. A name this version does not know is logged and counted
// like the rest.
func (s *Service) noteWarnings(ctx context.Context, reported []registry.EgressCount, warnings []usagecmds.Warning) int {
	for _, w := range warnings {
		var (
			counts int
			bytes  int64
		)
		for _, c := range reported {
			if c.Space == w.Space {
				counts++
				bytes += c.Bytes
			}
		}
		s.warnings.Add(ctx, 1, metric.WithAttributes(attribute.String("name", w.Name)))
		s.logger.Warn("the auth service warned about usage counts",
			zap.String("warning", w.Name), zap.String("message", w.Message),
			zap.Stringer("space", w.Space), zap.Int("counts", counts), zap.Int64("bytes", bytes))
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
	reg, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, backlogTimeout)
		defer cancel()
		b, err := s.store.EgressBacklog(ctx)
		if err != nil {
			return err
		}
		o.ObserveInt64(unreported, b.Counts)
		var oldest int64
		if !b.Oldest.IsZero() {
			oldest = int64(time.Since(b.Oldest).Seconds())
		}
		o.ObserveInt64(age, oldest)
		return nil
	}, unreported, age)
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
