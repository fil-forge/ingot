package s3frontend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fil-forge/versitygw/s3err"
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
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
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
		if err := runAccepts(t, b, uploadID); err != nil {
			t.Fatalf("accept: %v", err)
		}
	}
	got := collectMultipartMetrics(t, reader)
	if got["ingot.multipart.accept_passes/failed"] != 1 || got["ingot.multipart.accept_passes/accepted"] != 1 {
		t.Fatalf("accept passes = %v, want one failed and one accepted", got)
	}
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
	if !errors.As(err, &apiErr) || apiErr.Code != "OperationAborted" {
		t.Fatalf("Complete: err = %v, want OperationAborted", err)
	}
	if got := collectMultipartMetrics(t, reader); got["ingot.multipart.complete_deferred"] != 1 {
		t.Fatalf("complete_deferred = %d, want 1 (all: %v)", got["ingot.multipart.complete_deferred"], got)
	}
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
	if age := collectMultipartMetrics(t, reader)["ingot.multipart.parked_age"]; age < 3590 || age > 3700 {
		t.Fatalf("parked_age = %d, want about 3600", age)
	}

	if err := runAccepts(t, b, uploadID); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if age, ok := collectMultipartMetrics(t, reader)["ingot.multipart.parked_age"]; !ok || age != 0 {
		t.Fatalf("parked_age after acceptance = %d (reported %v), want 0", age, ok)
	}
}
