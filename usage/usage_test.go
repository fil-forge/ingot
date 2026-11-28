package usage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	hiltclient "github.com/fil-forge/hilt/pkg/client"
	usagecmds "github.com/fil-forge/libforge/commands/s3/usage"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/registry"
)

var hour0 = time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)

// clock is a time a test moves by hand.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

func newTestCounter(c *clock) *Counter {
	counter := NewCounter()
	counter.now = c.Now
	return counter
}

func sorted(counts []registry.EgressCount) []registry.EgressCount {
	sort.Slice(counts, func(i, j int) bool {
		if !counts[i].Hour.Equal(counts[j].Hour) {
			return counts[i].Hour.Before(counts[j].Hour)
		}
		return counts[i].Space.String() < counts[j].Space.String()
	})
	return counts
}

func wantCounts(t *testing.T, got []registry.EgressCount, want ...registry.EgressCount) {
	t.Helper()
	got, want = sorted(got), sorted(want)
	require.Len(t, got, len(want), "counts")
	for i := range got {
		require.True(t, got[i].Space == want[i].Space && got[i].Hour.Equal(want[i].Hour) && got[i].Bytes == want[i].Bytes,
			"counts:\n got %v\nwant %v", got, want)
	}
}

// body is a response body of n bytes.
func body(n int) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(make([]byte, n)))
}

func TestCounterCountsWhatAResponseReads(t *testing.T) {
	c := newTestCounter(&clock{now: hour0.Add(5 * time.Minute)})
	a, b := testutil.RandomDID(t), testutil.RandomDID(t)

	_, err := io.Copy(io.Discard, c.Egress(a, body(1000)))
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, c.Egress(a, body(24)))
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, c.Egress(b, body(7)))
	require.NoError(t, err)

	wantCounts(t, c.take(),
		registry.EgressCount{Space: a, Hour: hour0, Bytes: 1024},
		registry.EgressCount{Space: b, Hour: hour0, Bytes: 7},
	)
	// What was taken is forgotten.
	wantCounts(t, c.take())
}

// A client that goes away part way is counted for what it read, and the
// count is there without the body being closed.
func TestCounterCountsAnAbandonedResponse(t *testing.T) {
	c := newTestCounter(&clock{now: hour0})
	space := testutil.RandomDID(t)

	rc := c.Egress(space, body(1000))
	_, err := io.ReadFull(rc, make([]byte, 300))
	require.NoError(t, err)
	wantCounts(t, c.take(), registry.EgressCount{Space: space, Hour: hour0, Bytes: 300})
	require.NoError(t, rc.Close())
	wantCounts(t, c.take())
}

// A response that runs across an hour boundary adds to each hour the bytes
// read in it.
func TestCounterSplitsAResponseAcrossHours(t *testing.T) {
	clk := &clock{now: hour0.Add(59 * time.Minute)}
	c := newTestCounter(clk)
	space := testutil.RandomDID(t)

	rc := c.Egress(space, body(1000))
	_, err := io.ReadFull(rc, make([]byte, 800))
	require.NoError(t, err)
	clk.set(hour0.Add(61 * time.Minute))
	_, err = io.Copy(io.Discard, rc)
	require.NoError(t, err)

	wantCounts(t, c.take(),
		registry.EgressCount{Space: space, Hour: hour0, Bytes: 800},
		registry.EgressCount{Space: space, Hour: hour0.Add(time.Hour), Bytes: 200},
	)
}

func TestNilCounterCountsNothing(t *testing.T) {
	var c *Counter
	rc := body(10)
	require.True(t, c.Egress(testutil.RandomDID(t), rc) == rc, "a nil counter should hand the body back as it is")
}

func TestCounterIsSafeForConcurrentResponses(t *testing.T) {
	c := newTestCounter(&clock{now: hour0})
	space := testutil.RandomDID(t)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = io.Copy(io.Discard, c.Egress(space, body(4096)))
		}()
	}
	wg.Wait()
	wantCounts(t, c.take(), registry.EgressCount{Space: space, Hour: hour0, Bytes: 50 * 4096})
}

// fakeReporter stands in for the auth service: it records each report and
// answers as told.
type fakeReporter struct {
	mu      sync.Mutex
	reports [][]usagecmds.SpaceEgress
	// objects is the object counts of each report that carried some.
	objects [][]usagecmds.SpaceObjects
	// warn is the warning to answer with for a space, by name.
	warn map[did.DID]string
	err  error
	// onReport, when set, runs inside each report, before it answers.
	onReport func()
}

func (f *fakeReporter) ReportUsage(_ context.Context, report *usagecmds.ReportArguments, _ ...hiltclient.MethodOption) (*usagecmds.ReportOK, error) {
	if f.onReport != nil {
		f.onReport()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	warnings := []usagecmds.Warning{}
	warn := func(space did.DID) {
		if name, ok := f.warn[space]; ok {
			warnings = append(warnings, usagecmds.Warning{Name: name, Message: "left out", Space: space})
		}
	}
	if len(report.Egress) > 0 {
		f.reports = append(f.reports, report.Egress)
		for _, e := range report.Egress {
			warn(e.Space)
		}
	}
	if len(report.Objects) > 0 {
		f.objects = append(f.objects, report.Objects)
		for _, o := range report.Objects {
			warn(o.Space)
		}
	}
	return &usagecmds.ReportOK{Warnings: warnings}, nil
}

// held is what the service would hold after the reports: the last total sent
// for each space and hour.
func (f *fakeReporter) held() map[did.DID]map[int64]uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[did.DID]map[int64]uint64{}
	for _, report := range f.reports {
		for _, e := range report {
			if out[e.Space] == nil {
				out[e.Space] = map[int64]uint64{}
			}
			for _, h := range e.Hours {
				out[e.Space][h.Hour] = h.Bytes
			}
		}
	}
	return out
}

type fixture struct {
	svc      *Service
	counter  *Counter
	clock    *clock
	store    *registry.Postgres
	pool     *pgxpool.Pool
	reporter *fakeReporter
}

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	f := &fixture{
		clock:    &clock{now: hour0.Add(time.Minute)},
		pool:     pgtest.New(t),
		reporter: &fakeReporter{warn: map[did.DID]string{}},
	}
	f.store = registry.NewPostgres(f.pool)
	f.counter = newTestCounter(f.clock)
	cfg.Counter, cfg.Store, cfg.Reporter = f.counter, f.store, f.reporter
	svc, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.CloseMetrics() })
	f.svc = svc
	return f
}

func (f *fixture) send(t *testing.T, space did.DID, n int) {
	t.Helper()
	_, err := io.Copy(io.Discard, f.counter.Egress(space, body(n)))
	require.NoError(t, err)
}

// bucket creates a bucket and returns its space.
func (f *fixture) bucket(t *testing.T, name string) did.DID {
	t.Helper()
	space := testutil.RandomDID(t)
	require.NoError(t, f.store.Create(context.Background(), name, space, registry.CreateState{Tenant: testutil.RandomDID(t)}), "create bucket")
	return space
}

// change counts the objects a commit to the bucket adds and takes away, as
// the commit does.
func (f *fixture) change(t *testing.T, name string, added, removed int64) {
	t.Helper()
	for _, st := range registry.ObjectCountWrites(name, added, removed) {
		_, err := f.pool.Exec(context.Background(), st.SQL, st.Args...)
		require.NoError(t, err, "object count write")
	}
}

func (f *fixture) flush(t *testing.T) {
	t.Helper()
	require.NoError(t, f.svc.Flush(context.Background()), "Flush")
}

func (f *fixture) report(t *testing.T) ReportStats {
	t.Helper()
	stats, err := f.svc.Report(context.Background())
	require.NoError(t, err, "Report")
	return stats
}

func TestReportSendsWhatWasCountedGroupedBySpace(t *testing.T) {
	f := newFixture(t, Config{})
	a, b := testutil.RandomDID(t), testutil.RandomDID(t)

	f.send(t, a, 100)
	f.send(t, b, 7)
	f.clock.set(hour0.Add(61 * time.Minute))
	f.send(t, a, 50)
	f.flush(t)

	stats := f.report(t)
	require.Equal(t, 1, stats.Reports, "stats: %+v", stats)
	require.Equal(t, 3, stats.Counts, "stats: %+v", stats)
	require.Zero(t, stats.Warnings, "stats: %+v", stats)
	require.Len(t, f.reporter.reports, 1, "reports sent")
	// One entry for each space, its hours in order.
	for _, e := range f.reporter.reports[0] {
		switch e.Space {
		case a:
			want := []usagecmds.HourEgress{{Hour: hour0.Unix(), Bytes: 100}, {Hour: hour0.Add(time.Hour).Unix(), Bytes: 50}}
			require.Equal(t, want, e.Hours, "space a hours")
		case b:
			require.Equal(t, []usagecmds.HourEgress{{Hour: hour0.Unix(), Bytes: 7}}, e.Hours, "space b hours")
		default:
			require.FailNow(t, "report names an unknown space", "space %s", e.Space)
		}
	}
	require.Len(t, f.reporter.reports[0], 2, "report entries, want one for each of 2 spaces")

	// Nothing has changed, so the next pass sends nothing.
	require.Zero(t, f.report(t).Reports, "a pass with nothing due sent reports")
}

// The hour in progress is reported as it stands, and again with its new total
// each time it has grown.
func TestReportSendsAnHourAgainAsItGrows(t *testing.T) {
	f := newFixture(t, Config{})
	space := testutil.RandomDID(t)

	f.send(t, space, 100)
	f.flush(t)
	f.report(t)
	f.send(t, space, 25)
	f.flush(t)
	f.report(t)

	got := f.reporter.held()[space][hour0.Unix()]
	require.Equal(t, uint64(125), got, "the service holds %d for the hour", got)
	require.Len(t, f.reporter.reports, 2, "reports sent")
}

// A backlog larger than one report goes out as several, oldest hour first,
// and all of it arrives.
func TestReportWorksThroughABacklog(t *testing.T) {
	f := newFixture(t, Config{ReportBatch: 2})
	space := testutil.RandomDID(t)
	for i := range 5 {
		f.clock.set(hour0.Add(time.Duration(i) * time.Hour))
		f.send(t, space, 10+i)
	}
	f.flush(t)

	stats := f.report(t)
	require.Equal(t, 3, stats.Reports, "stats: %+v, want 3 reports carrying 5 counts", stats)
	require.Equal(t, 5, stats.Counts, "stats: %+v, want 3 reports carrying 5 counts", stats)
	require.Equal(t, hour0.Unix(), f.reporter.reports[0][0].Hours[0].Hour, "the first report should start at the oldest hour")
	held := f.reporter.held()[space]
	for i := range 5 {
		require.Equal(t, uint64(10+i), held[hour0.Add(time.Duration(i)*time.Hour).Unix()], "hour %d: the service holds", i)
	}
}

// A report that fails loses nothing: the same counts go with the next pass.
func TestReportRetriesAfterAFailure(t *testing.T) {
	f := newFixture(t, Config{})
	space := testutil.RandomDID(t)
	f.send(t, space, 100)
	f.flush(t)

	f.reporter.err = errors.New("auth service unreachable")
	_, err := f.svc.Report(context.Background())
	require.Error(t, err, "Report succeeded though the service was unreachable")
	f.send(t, space, 5)
	f.flush(t)

	f.reporter.err = nil
	f.report(t)
	got := f.reporter.held()[space][hour0.Unix()]
	require.Equal(t, uint64(105), got, "the service holds %d for the hour", got)
}

// A count the service warned about is not sent again, and the counts beside
// it are stored. Each warning is counted under its name, one this version has
// never heard of included.
func TestReportDoesNotResendACountItWasWarnedAbout(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	f := newFixture(t, Config{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))})
	known, unknown, elsewhere, odd := testutil.RandomDID(t), testutil.RandomDID(t), testutil.RandomDID(t), testutil.RandomDID(t)
	f.reporter.warn[unknown] = usagecmds.UnknownBucketWarningName
	f.reporter.warn[elsewhere] = usagecmds.BucketRegionMismatchWarningName
	f.reporter.warn[odd] = "SomethingNewer"

	for _, space := range []did.DID{known, unknown, elsewhere, odd} {
		f.send(t, space, 100)
	}
	f.flush(t)

	stats := f.report(t)
	require.Equal(t, 4, stats.Counts, "stats: %+v, want 4 counts and 3 warnings", stats)
	require.Equal(t, 3, stats.Warnings, "stats: %+v, want 4 counts and 3 warnings", stats)
	stats = f.report(t)
	require.Zero(t, stats.Reports, "a count that was warned about was sent again: %+v", stats)
	got := warningCounts(t, reader)
	want := map[string]int64{
		usagecmds.UnknownBucketWarningName:        1,
		usagecmds.BucketRegionMismatchWarningName: 1,
		"SomethingNewer":                          1,
	}
	require.Equal(t, want, got, "ingot.usage.report_warnings by name")
}

// A count that grows during a pass does not keep the pass going: it is sent
// with what was waiting, and what it gained goes with the next pass.
func TestReportEndsThoughCountsKeepGrowing(t *testing.T) {
	f := newFixture(t, Config{ReportBatch: 1})
	space := testutil.RandomDID(t)
	f.send(t, space, 100)
	f.flush(t)

	reports := 0
	f.reporter.onReport = func() {
		reports++
		require.LessOrEqual(t, reports, 50, "the pass did not end")
		f.send(t, space, 1)
		f.flush(t)
	}
	stats := f.report(t)
	require.Equal(t, 1, stats.Reports, "the pass sent %d reports, want the 1 that was waiting when it began", stats.Reports)

	f.reporter.onReport = nil
	f.report(t)
	got := f.reporter.held()[space][hour0.Unix()]
	require.Equal(t, uint64(101), got, "the service holds %d for the hour", got)
}

// A flush the database refuses keeps its counts for the next one.
func TestFlushKeepsCountsItCouldNotStore(t *testing.T) {
	f := newFixture(t, Config{})
	space := testutil.RandomDID(t)
	f.send(t, space, 100)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, f.svc.Flush(ctx), "Flush succeeded on a cancelled context")
	f.send(t, space, 5)
	f.flush(t)

	due, err := f.store.DueEgress(context.Background(), 10)
	require.NoError(t, err)
	wantCounts(t, due, registry.EgressCount{Space: space, Hour: hour0, Bytes: 105})
}

func TestBacklogGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	f := newFixture(t, Config{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))})
	a, b := testutil.RandomDID(t), testutil.RandomDID(t)

	got := metricValue(t, reader, "ingot.usage.egress_unreported_counts")
	require.Zero(t, got, "unreported counts with nothing sent")
	require.Zero(t, metricValue(t, reader, "ingot.usage.egress_oldest_unreported_age"), "oldest unreported age with nothing sent")

	f.send(t, a, 1)
	f.send(t, b, 1)
	f.flush(t)
	require.Equal(t, int64(2), metricValue(t, reader, "ingot.usage.egress_unreported_counts"), "unreported counts")

	f.report(t)
	require.Zero(t, metricValue(t, reader, "ingot.usage.egress_unreported_counts"), "unreported counts after a report")
}

func TestNewRequiresItsParts(t *testing.T) {
	store := registry.NewPostgres(pgtest.New(t))
	for name, cfg := range map[string]Config{
		"no counter":  {Store: store, Reporter: &fakeReporter{}},
		"no store":    {Counter: NewCounter(), Reporter: &fakeReporter{}},
		"no reporter": {Counter: NewCounter(), Store: store},
	} {
		_, err := New(cfg)
		require.Error(t, err, "New accepted a config with %s", name)
	}
}

// metricValue collects the reader and returns the named instrument's one
// data point.
func metricValue(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm), "collect")
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Gauge[int64]:
				return onePoint(t, name, data.DataPoints)
			case metricdata.Sum[int64]:
				return onePoint(t, name, data.DataPoints)
			}
		}
	}
	// A counter nothing has been added to has no data yet.
	return 0
}

func onePoint(t *testing.T, name string, points []metricdata.DataPoint[int64]) int64 {
	t.Helper()
	require.Len(t, points, 1, "%s data points", name)
	require.Equal(t, *attribute.EmptySet(), points[0].Attributes, "%s carries attributes", name)
	return points[0].Value
}

// warningCounts collects the reader and returns the warnings counter's values
// by warning name.
func warningCounts(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm), "collect")
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "ingot.usage.report_warnings" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "ingot.usage.report_warnings is a %T", m.Data)
			for _, dp := range sum.DataPoints {
				name, _ := dp.Attributes.Value("name")
				out[name.AsString()] = dp.Value
			}
		}
	}
	return out
}

// The object counts the commits wrote go out after the egress, grouped by
// space, each with the bucket's totals, and are sent again only once they
// have changed.
func TestReportSendsObjectCounts(t *testing.T) {
	f := newFixture(t, Config{})
	a, b := f.bucket(t, "bucket-a"), f.bucket(t, "bucket-b")
	f.change(t, "bucket-a", 3, 0)
	f.change(t, "bucket-a", 1, 2)
	f.change(t, "bucket-b", 1, 0)
	f.send(t, a, 10)
	f.flush(t)

	stats := f.report(t)
	require.Equal(t, 2, stats.Reports, "stats %+v, want 2 reports carrying 3 counts", stats)
	require.Equal(t, 3, stats.Counts, "stats %+v, want 2 reports carrying 3 counts", stats)
	require.Len(t, f.reporter.reports, 1, "sent %d egress and %d object reports", len(f.reporter.reports), len(f.reporter.objects))
	require.Len(t, f.reporter.objects, 1, "sent %d egress and %d object reports", len(f.reporter.reports), len(f.reporter.objects))
	got := map[did.DID][2]uint64{}
	for _, o := range f.reporter.objects[0] {
		require.Len(t, o.Hours, 1, "space %s reported %d hours", o.Space, len(o.Hours))
		require.Zero(t, o.Hours[0].Hour%3600, "reported hour %d is not a whole hour", o.Hours[0].Hour)
		got[o.Space] = [2]uint64{o.Hours[0].Added, o.Hours[0].Removed}
	}
	want := map[did.DID][2]uint64{a: {4, 2}, b: {1, 0}}
	require.Equal(t, want, got, "reported object counts")

	// Nothing has changed, so nothing is sent.
	require.Zero(t, f.report(t).Reports, "a pass with nothing due sent reports")
	f.change(t, "bucket-b", 0, 1)
	f.report(t)
	require.Len(t, f.reporter.objects, 2, "object reports after a change")
	require.Len(t, f.reporter.objects[1], 1, "after a change, sent %+v", f.reporter.objects)
	require.Equal(t, b, f.reporter.objects[1][0].Space, "after a change, sent %+v", f.reporter.objects)
	require.Equal(t, uint64(1), f.reporter.objects[1][0].Hours[0].Removed, "after a change, sent %+v", f.reporter.objects)
}

// A report that fails leaves its object counts due, and a count the service
// warned about is settled like the rest.
func TestReportRetriesObjectCountsAndSettlesWarnedOnes(t *testing.T) {
	f := newFixture(t, Config{})
	space := f.bucket(t, "bucket-a")
	f.change(t, "bucket-a", 2, 0)

	f.reporter.err = errors.New("unreachable")
	_, err := f.svc.Report(context.Background())
	require.Error(t, err, "Report succeeded though the service was unreachable")
	f.reporter.err = nil
	f.reporter.warn[space] = usagecmds.UnknownBucketWarningName
	stats := f.report(t)
	require.Equal(t, 1, stats.Reports, "stats %+v, want one report with one warning", stats)
	require.Equal(t, 1, stats.Warnings, "stats %+v, want one report with one warning", stats)
	stats = f.report(t)
	require.Zero(t, stats.Reports, "a warned-about count was sent again: %+v", stats)
}

func TestObjectBacklogGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	f := newFixture(t, Config{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))})
	f.bucket(t, "bucket-a")
	got := metricValue(t, reader, "ingot.usage.object_unreported_counts")
	require.Zero(t, got, "unreported object counts")
	f.change(t, "bucket-a", 1, 0)
	require.Equal(t, int64(1), metricValue(t, reader, "ingot.usage.object_unreported_counts"), "unreported object counts")
	f.report(t)
	require.Zero(t, metricValue(t, reader, "ingot.usage.object_unreported_counts"), "unreported object counts after a report")
	require.Zero(t, metricValue(t, reader, "ingot.usage.object_oldest_unreported_age"), "oldest unreported object count age")
}
