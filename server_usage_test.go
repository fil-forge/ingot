package ingot

import (
	"context"
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
)

// recordingReporter stands in for the auth service, keeping what it is sent.
type recordingReporter struct {
	mu      sync.Mutex
	reports [][]usagecmds.SpaceEgress
}

func (r *recordingReporter) ReportUsage(_ context.Context, egress []usagecmds.SpaceEgress, _ ...hiltclient.MethodOption) (*usagecmds.ReportOK, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, egress)
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
		d.EgressUsage = store
		d.UsageReporter = reporter
	})
	names := metricNames(t, reader)
	require.True(t, names["ingot.usage.egress_unreported_counts"], "usage gauges registered by New: %v", names)
	require.True(t, names["ingot.usage.egress_oldest_unreported_age"], "usage gauges registered by New: %v", names)

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
	time.Sleep(100 * time.Millisecond)
	require.Len(t, reporter.sent(), 1, "a count that had not changed was reported again")

	stopped = true
	require.NoError(t, s.Stop(context.Background()))
	require.Empty(t, metricNames(t, reader), "metrics reported after Stop")
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
		d.EgressUsage = d.Registry.(*registry.Postgres)
	})
	require.Nil(t, s.usage)
	for name := range metricNames(t, reader) {
		require.NotContains(t, name, "ingot.usage.")
	}
	require.NoError(t, s.Stop(context.Background()))
}
