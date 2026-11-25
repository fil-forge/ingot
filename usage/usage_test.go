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
	if len(got) != len(want) {
		t.Fatalf("counts:\n got %v\nwant %v", got, want)
	}
	for i := range got {
		if got[i].Space != want[i].Space || !got[i].Hour.Equal(want[i].Hour) || got[i].Bytes != want[i].Bytes {
			t.Fatalf("counts:\n got %v\nwant %v", got, want)
		}
	}
}

// body is a response body of n bytes.
func body(n int) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(make([]byte, n)))
}

func TestCounterCountsWhatAResponseReads(t *testing.T) {
	c := newTestCounter(&clock{now: hour0.Add(5 * time.Minute)})
	a, b := testutil.RandomDID(t), testutil.RandomDID(t)

	if _, err := io.Copy(io.Discard, c.Egress(a, body(1000))); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, c.Egress(a, body(24))); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, c.Egress(b, body(7))); err != nil {
		t.Fatal(err)
	}

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
	if _, err := io.ReadFull(rc, make([]byte, 300)); err != nil {
		t.Fatal(err)
	}
	wantCounts(t, c.take(), registry.EgressCount{Space: space, Hour: hour0, Bytes: 300})
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	wantCounts(t, c.take())
}

// A response that runs across an hour boundary adds to each hour the bytes
// read in it.
func TestCounterSplitsAResponseAcrossHours(t *testing.T) {
	clk := &clock{now: hour0.Add(59 * time.Minute)}
	c := newTestCounter(clk)
	space := testutil.RandomDID(t)

	rc := c.Egress(space, body(1000))
	if _, err := io.ReadFull(rc, make([]byte, 800)); err != nil {
		t.Fatal(err)
	}
	clk.set(hour0.Add(61 * time.Minute))
	if _, err := io.Copy(io.Discard, rc); err != nil {
		t.Fatal(err)
	}

	wantCounts(t, c.take(),
		registry.EgressCount{Space: space, Hour: hour0, Bytes: 800},
		registry.EgressCount{Space: space, Hour: hour0.Add(time.Hour), Bytes: 200},
	)
}

func TestNilCounterCountsNothing(t *testing.T) {
	var c *Counter
	rc := body(10)
	if got := c.Egress(testutil.RandomDID(t), rc); got != rc {
		t.Fatal("a nil counter should hand the body back as it is")
	}
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
	skip    map[did.DID]bool
	err     error
	// onReport, when set, runs inside each report, before it answers.
	onReport func()
}

func (f *fakeReporter) ReportUsage(_ context.Context, egress []usagecmds.SpaceEgress, _ ...hiltclient.MethodOption) (*usagecmds.ReportOK, error) {
	if f.onReport != nil {
		f.onReport()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.reports = append(f.reports, egress)
	skipped := []did.DID{}
	for _, e := range egress {
		if f.skip[e.Space] {
			skipped = append(skipped, e.Space)
		}
	}
	return &usagecmds.ReportOK{Skipped: skipped}, nil
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
	reporter *fakeReporter
}

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	f := &fixture{
		clock:    &clock{now: hour0.Add(time.Minute)},
		store:    registry.NewPostgres(pgtest.New(t)),
		reporter: &fakeReporter{skip: map[did.DID]bool{}},
	}
	f.counter = newTestCounter(f.clock)
	cfg.Counter, cfg.Store, cfg.Reporter = f.counter, f.store, f.reporter
	svc, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.CloseMetrics() })
	f.svc = svc
	return f
}

func (f *fixture) send(t *testing.T, space did.DID, n int) {
	t.Helper()
	if _, err := io.Copy(io.Discard, f.counter.Egress(space, body(n))); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) flush(t *testing.T) {
	t.Helper()
	if err := f.svc.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func (f *fixture) report(t *testing.T) ReportStats {
	t.Helper()
	stats, err := f.svc.Report(context.Background())
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
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
	if stats.Reports != 1 || stats.Counts != 3 || stats.Skipped != 0 {
		t.Fatalf("stats: %+v", stats)
	}
	if len(f.reporter.reports) != 1 {
		t.Fatalf("reports sent: %d, want 1", len(f.reporter.reports))
	}
	// One entry for each space, its hours in order.
	for _, e := range f.reporter.reports[0] {
		switch e.Space {
		case a:
			want := []usagecmds.HourEgress{{Hour: hour0.Unix(), Bytes: 100}, {Hour: hour0.Add(time.Hour).Unix(), Bytes: 50}}
			if len(e.Hours) != 2 || e.Hours[0] != want[0] || e.Hours[1] != want[1] {
				t.Fatalf("space a hours: %v, want %v", e.Hours, want)
			}
		case b:
			if len(e.Hours) != 1 || e.Hours[0] != (usagecmds.HourEgress{Hour: hour0.Unix(), Bytes: 7}) {
				t.Fatalf("space b hours: %v", e.Hours)
			}
		default:
			t.Fatalf("report names an unknown space %s", e.Space)
		}
	}
	if len(f.reporter.reports[0]) != 2 {
		t.Fatalf("report has %d entries, want one for each of 2 spaces", len(f.reporter.reports[0]))
	}

	// Nothing has changed, so the next pass sends nothing.
	if stats := f.report(t); stats.Reports != 0 {
		t.Fatalf("a pass with nothing due sent %d reports", stats.Reports)
	}
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

	if got := f.reporter.held()[space][hour0.Unix()]; got != 125 {
		t.Fatalf("the service holds %d for the hour, want 125", got)
	}
	if len(f.reporter.reports) != 2 {
		t.Fatalf("reports sent: %d, want 2", len(f.reporter.reports))
	}
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
	if stats.Reports != 3 || stats.Counts != 5 {
		t.Fatalf("stats: %+v, want 3 reports carrying 5 counts", stats)
	}
	if first := f.reporter.reports[0][0].Hours[0].Hour; first != hour0.Unix() {
		t.Fatalf("the first report starts at %d, want the oldest hour %d", first, hour0.Unix())
	}
	held := f.reporter.held()[space]
	for i := range 5 {
		if got := held[hour0.Add(time.Duration(i)*time.Hour).Unix()]; got != uint64(10+i) {
			t.Fatalf("hour %d: the service holds %d, want %d", i, got, 10+i)
		}
	}
}

// A report that fails loses nothing: the same counts go with the next pass.
func TestReportRetriesAfterAFailure(t *testing.T) {
	f := newFixture(t, Config{})
	space := testutil.RandomDID(t)
	f.send(t, space, 100)
	f.flush(t)

	f.reporter.err = errors.New("auth service unreachable")
	if _, err := f.svc.Report(context.Background()); err == nil {
		t.Fatal("Report succeeded though the service was unreachable")
	}
	f.send(t, space, 5)
	f.flush(t)

	f.reporter.err = nil
	f.report(t)
	if got := f.reporter.held()[space][hour0.Unix()]; got != 105 {
		t.Fatalf("the service holds %d for the hour, want 105", got)
	}
}

// A count the service skipped is not sent again, and the counts beside it
// are stored.
func TestReportDoesNotResendASkippedCount(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	f := newFixture(t, Config{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))})
	known, unknown := testutil.RandomDID(t), testutil.RandomDID(t)
	f.reporter.skip[unknown] = true

	f.send(t, known, 100)
	f.send(t, unknown, 9)
	f.flush(t)

	stats := f.report(t)
	if stats.Counts != 2 || stats.Skipped != 1 {
		t.Fatalf("stats: %+v, want 2 counts of which 1 skipped", stats)
	}
	if stats := f.report(t); stats.Reports != 0 {
		t.Fatalf("the skipped count was sent again: %+v", stats)
	}
	if got := metricValue(t, reader, "ingot.usage.egress_skipped_counts"); got != 1 {
		t.Fatalf("ingot.usage.egress_skipped_counts = %d, want 1", got)
	}
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
		if reports > 50 {
			t.Fatal("the pass did not end")
		}
		f.send(t, space, 1)
		f.flush(t)
	}
	stats := f.report(t)
	if stats.Reports != 1 {
		t.Fatalf("the pass sent %d reports, want the 1 that was waiting when it began", stats.Reports)
	}

	f.reporter.onReport = nil
	f.report(t)
	if got := f.reporter.held()[space][hour0.Unix()]; got != 101 {
		t.Fatalf("the service holds %d for the hour, want 101", got)
	}
}

// A flush the database refuses keeps its counts for the next one.
func TestFlushKeepsCountsItCouldNotStore(t *testing.T) {
	f := newFixture(t, Config{})
	space := testutil.RandomDID(t)
	f.send(t, space, 100)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.svc.Flush(ctx); err == nil {
		t.Fatal("Flush succeeded on a cancelled context")
	}
	f.send(t, space, 5)
	f.flush(t)

	due, err := f.store.DueEgress(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	wantCounts(t, due, registry.EgressCount{Space: space, Hour: hour0, Bytes: 105})
}

func TestBacklogGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	f := newFixture(t, Config{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))})
	a, b := testutil.RandomDID(t), testutil.RandomDID(t)

	if got := metricValue(t, reader, "ingot.usage.egress_unreported_counts"); got != 0 {
		t.Fatalf("unreported counts with nothing sent: %d", got)
	}
	if got := metricValue(t, reader, "ingot.usage.egress_oldest_unreported_age"); got != 0 {
		t.Fatalf("oldest unreported age with nothing sent: %d", got)
	}

	f.send(t, a, 1)
	f.send(t, b, 1)
	f.flush(t)
	if got := metricValue(t, reader, "ingot.usage.egress_unreported_counts"); got != 2 {
		t.Fatalf("unreported counts: %d, want 2", got)
	}

	f.report(t)
	if got := metricValue(t, reader, "ingot.usage.egress_unreported_counts"); got != 0 {
		t.Fatalf("unreported counts after a report: %d, want 0", got)
	}
}

func TestNewRequiresItsParts(t *testing.T) {
	store := registry.NewPostgres(pgtest.New(t))
	for name, cfg := range map[string]Config{
		"no counter":  {Store: store, Reporter: &fakeReporter{}},
		"no store":    {Counter: NewCounter(), Reporter: &fakeReporter{}},
		"no reporter": {Counter: NewCounter(), Store: store},
	} {
		if _, err := New(cfg); err == nil {
			t.Fatalf("New accepted a config with %s", name)
		}
	}
}

// metricValue collects the reader and returns the named instrument's one
// data point.
func metricValue(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
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
	if len(points) != 1 {
		t.Fatalf("%s has %d data points, want 1", name, len(points))
	}
	if points[0].Attributes != *attribute.EmptySet() {
		t.Fatalf("%s carries attributes %v, want none", name, points[0].Attributes)
	}
	return points[0].Value
}
