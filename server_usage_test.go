package ingot

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	hiltclient "github.com/fil-forge/hilt/pkg/client"
	usagecmds "github.com/fil-forge/libforge/commands/s3/usage"
	"github.com/fil-forge/libforge/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// recordingReporter stands in for the auth service, keeping what it is sent.
type recordingReporter struct {
	mu      sync.Mutex
	reports [][]usagecmds.SpaceEgress
	objects [][]usagecmds.SpaceObjects
}

func (r *recordingReporter) ReportUsage(_ context.Context, report *usagecmds.ReportArguments, _ ...hiltclient.MethodOption) (*usagecmds.ReportOK, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(report.Egress) > 0 {
		r.reports = append(r.reports, report.Egress)
	}
	if len(report.Objects) > 0 {
		r.objects = append(r.objects, report.Objects)
	}
	return &usagecmds.ReportOK{}, nil
}

func (r *recordingReporter) sent() [][]usagecmds.SpaceEgress {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]usagecmds.SpaceEgress(nil), r.reports...)
}

// TestServerReportsUsage: with a usage store and a reporter, the server
// reports the egress counts that are due at its report interval, registers
// the usage gauges while it runs, and unregisters them on Stop.
func TestServerReportsUsage(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	reporter := &recordingReporter{}
	var store *registry.Postgres
	s := newTestServer(t, config.ServerConfig{UsageReportInterval: 20 * time.Millisecond}, func(d *ServerDeps) {
		store = d.Registry.(*registry.Postgres)
		d.Usage = store
		d.UsageReporter = reporter
	})
	names := metricNames(t, reader)
	require.True(t, names["ingot.usage.egress_unreported_counts"], "usage gauges registered by New: %v", names)
	require.True(t, names["ingot.usage.egress_oldest_unreported_age"], "usage gauges registered by New: %v", names)
	require.True(t, names["ingot.usage.object_unreported_counts"], "usage gauges registered by New: %v", names)
	require.True(t, names["ingot.usage.object_oldest_unreported_age"], "usage gauges registered by New: %v", names)

	ctx := t.Context()
	space := testutil.RandomDID(t)
	hour := time.Now().UTC().Truncate(time.Hour)
	require.NoError(t, store.AddEgress(ctx, []registry.EgressCount{{Space: space, Hour: hour, Bytes: 4096}}))

	require.NoError(t, s.Start(ctx))
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = s.Stop(context.Background())
		}
	})
	require.Eventually(t, func() bool { return len(reporter.sent()) > 0 },
		5*time.Second, 10*time.Millisecond, "the usage sweeper never reported the count")
	require.Equal(t, []usagecmds.SpaceEgress{{
		Space: space,
		Hours: []usagecmds.HourEgress{{Hour: hour.Unix(), Bytes: 4096}},
	}}, reporter.sent()[0])

	// The count is acknowledged, so later passes have nothing to send.
	require.Eventually(t, func() bool {
		due, err := store.DueEgress(ctx, 10)
		return err == nil && len(due) == 0
	}, 5*time.Second, 10*time.Millisecond, "the reported count was never acknowledged")
	// A pass that ran out of time before it acknowledged repeats its report,
	// so the count taken here is of the reports up to the acknowledgement.
	reported := len(reporter.sent())
	time.Sleep(100 * time.Millisecond)
	require.Len(t, reporter.sent(), reported, "a count that had been acknowledged was reported again")

	stopped = true
	require.NoError(t, s.Stop(context.Background()))
	require.Empty(t, metricNames(t, reader), "metrics reported after Stop")
}

// stuckReporter stands in for an auth service that does not answer: each
// report waits until the test releases it or its context ends.
type stuckReporter struct {
	entered chan struct{}
	release chan struct{}
}

func (r *stuckReporter) ReportUsage(ctx context.Context, _ *usagecmds.ReportArguments, _ ...hiltclient.MethodOption) (*usagecmds.ReportOK, error) {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	select {
	case <-r.release:
		return &usagecmds.ReportOK{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TestServerFlushesUsageWhileAReportIsStuck: a report the auth service does
// not answer holds up no flush, so what is counted meanwhile still reaches
// Postgres within the flush interval.
func TestServerFlushesUsageWhileAReportIsStuck(t *testing.T) {
	saved := usageFlushInterval
	usageFlushInterval = 10 * time.Millisecond
	t.Cleanup(func() { usageFlushInterval = saved })

	reporter := &stuckReporter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	var store *registry.Postgres
	s := newTestServer(t, config.ServerConfig{UsageReportInterval: 20 * time.Millisecond}, func(d *ServerDeps) {
		store = d.Registry.(*registry.Postgres)
		d.Usage = store
		d.UsageReporter = reporter
	})

	ctx := t.Context()
	waiting, counted := testutil.RandomDID(t), testutil.RandomDID(t)
	hour := time.Now().UTC().Truncate(time.Hour)
	require.NoError(t, store.AddEgress(ctx, []registry.EgressCount{{Space: waiting, Hour: hour, Bytes: 1}}))

	require.NoError(t, s.Start(ctx))
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	select {
	case <-reporter.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the usage sweeper never reported the count")
	}

	// The report is stuck. Bytes a response sends now are still flushed.
	body := s.usageCounter.Egress(counted, io.NopCloser(bytes.NewReader(make([]byte, 4096))))
	_, err := io.Copy(io.Discard, body)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		due, err := store.DueEgress(ctx, 10)
		if err != nil {
			return false
		}
		for _, c := range due {
			if c.Space == counted && c.Bytes == 4096 {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "the count was not flushed while the report was stuck")
	close(reporter.release)
}

// TestServerUnregistersUsageGaugesWhenStartFails: a Start that fails leaves
// no usage gauge behind, since nothing will call Stop for it.
func TestServerUnregistersUsageGaugesWhenStartFails(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	var tasks *taskrunner.Runner
	s := newTestServer(t, config.ServerConfig{}, func(d *ServerDeps) {
		d.Usage = d.Registry.(*registry.Postgres)
		d.UsageReporter = &recordingReporter{}
		tasks = d.Tasks
	})
	require.True(t, metricNames(t, reader)["ingot.usage.egress_unreported_counts"], "usage gauges registered by New")

	// A task runner that is already running refuses to start again, which
	// fails the server's Start after New has registered the gauges.
	require.NoError(t, tasks.Start(t.Context()))
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	require.Error(t, s.Start(t.Context()))
	for name := range metricNames(t, reader) {
		require.NotContains(t, name, "ingot.usage.")
	}
}

// TestServerCountsNoUsageWithoutAReporter: usage reporting needs both a store
// and a reporter; with either missing the server registers no usage gauges
// and runs no usage sweeper.
func TestServerCountsNoUsageWithoutAReporter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	s := newTestServer(t, config.ServerConfig{}, func(d *ServerDeps) {
		d.Usage = d.Registry.(*registry.Postgres)
	})
	require.Nil(t, s.usage)
	for name := range metricNames(t, reader) {
		require.NotContains(t, name, "ingot.usage.")
	}
	require.NoError(t, s.Stop(context.Background()))
}
