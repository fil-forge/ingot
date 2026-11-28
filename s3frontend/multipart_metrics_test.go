package s3frontend

import (
	"context"
	"testing"
	"time"

	"github.com/fil-forge/versitygw/s3err"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collectMultipartMetrics reads the multipart instruments: the parked-age
// gauge by name, and the counters by name and, for the pass counter,
// outcome ("ingot.multipart.accept_passes/failed").
func collectMultipartMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm), "collect")
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					got[m.Name] = dp.Value
				}
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					name := m.Name
					if outcome, ok := dp.Attributes.Value(attribute.Key("outcome")); ok {
						name += "/" + outcome.AsString()
					}
					got[name] = dp.Value
				}
			}
		}
	}
	return got
}

// TestMultipartMetrics_AcceptPasses: a blob's conclude that fails and the
// retry's that accepts are counted by outcome; an accept that found the blob
// located counts nothing.
func TestMultipartMetrics_AcceptPasses(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	cc := &countingConcluder{fails: 1}
	b, _ := newDeferredBackend(t, cc, meteredBackend(reader))
	key := "metrics-passes"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)

	for range 3 {
		require.NoError(t, runAccepts(t, b, uploadID), "accept")
	}
	got := collectMultipartMetrics(t, reader)
	require.Equal(t, int64(1), got["ingot.multipart.accept_passes/failed"], "accept passes = %v, want one failed and one accepted", got)
	require.Equal(t, int64(1), got["ingot.multipart.accept_passes/accepted"], "accept passes = %v, want one failed and one accepted", got)
}

// TestMultipartMetrics_CompleteDeferred: a Complete answered OperationAborted
// because the upload's accept did not start in time is counted.
func TestMultipartMetrics_CompleteDeferred(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	g := newGatedConcluder()
	b, _ := newDeferredBackend(t, g, meteredBackend(reader))
	storeFor(b).limits.AcceptWait = 20 * time.Millisecond
	key := "metrics-deferred"
	uploadID := mpCreate(t, b, key, "", "")
	parts := uploadDistinctParts(t, b, key, uploadID, 1)
	background := acceptInBackground(t, b, uploadID)
	t.Cleanup(func() {
		close(g.open)
		<-background
	})
	<-g.entered

	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	var apiErr s3err.APIError
	require.ErrorAs(t, err, &apiErr, "Complete")
	require.Equal(t, "OperationAborted", apiErr.Code, "Complete")
	got := collectMultipartMetrics(t, reader)
	require.Equal(t, int64(1), got["ingot.multipart.complete_deferred"], "complete_deferred (all: %v)", got)
}

// TestMultipartMetrics_ParkedAge: the gauge reports how long the oldest
// parked part has waited, read when it is collected, and 0 once the part is
// accepted.
func TestMultipartMetrics_ParkedAge(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, mem := newDeferredBackend(t, &countingConcluder{}, meteredBackend(reader))
	key := "metrics-parked-age"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)

	// The part as recorded an hour ago, still parked.
	mem.AgePart(uploadID, 1, time.Hour)
	age := collectMultipartMetrics(t, reader)["ingot.multipart.parked_age"]
	require.GreaterOrEqual(t, age, int64(3590), "parked_age")
	require.LessOrEqual(t, age, int64(3700), "parked_age")

	require.NoError(t, runAccepts(t, b, uploadID), "accept")
	age, ok := collectMultipartMetrics(t, reader)["ingot.multipart.parked_age"]
	require.True(t, ok, "parked_age after acceptance not reported")
	require.Zero(t, age, "parked_age after acceptance")
}
