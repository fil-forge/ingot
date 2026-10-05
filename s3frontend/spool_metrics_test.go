package s3frontend

import (
	"context"
	"testing"

	"github.com/fil-forge/versitygw/backend"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fil-forge/ingot/inmem"
)

// collectSpoolMetrics reads every spool instrument: gauges by name, and the
// removal counters by name and reason ("ingot.spool.evictions/budget").
func collectSpoolMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
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
					reason, _ := dp.Attributes.Value(attribute.Key("reason"))
					got[m.Name+"/"+reason.AsString()] = dp.Value
				}
			}
		}
	}
	return got
}

func meteredBackend(reader *sdkmetric.ManualReader) func(*Deps) {
	return func(d *Deps) { d.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)) }
}

func TestSpoolMetrics_BudgetPass(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newSweepBackend(t, meteredBackend(reader))
	digests := putSweepObjects(t, b, 4)
	size := blobSize(t, b, digests[0])
	budgetToEvict(b, 2, size)

	sweepSpool(t, b)

	got := collectSpoolMetrics(t, reader)
	want := map[string]int64{
		"ingot.spool.usage":            b.spool.Usage(),
		"ingot.spool.budget":           b.spoolMaxBytes,
		"ingot.spool.evictions/budget": 2,
		"ingot.spool.evicted/budget":   2 * size,
	}
	for name, v := range want {
		if got[name] != v {
			t.Fatalf("%s = %d, want %d (all: %v)", name, got[name], v, got)
		}
	}

	if err := b.CloseMetrics(); err != nil {
		t.Fatalf("CloseMetrics: %v", err)
	}
	if _, ok := collectSpoolMetrics(t, reader)["ingot.spool.usage"]; ok {
		t.Fatal("usage gauge still reported after CloseMetrics")
	}
}

func TestSpoolMetrics_ParkedPart(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, mem := newDeferredBackend(t, &parkingUploader{}, meteredBackend(reader))
	key := "parked"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	parts := int64(len(hygienePartDigests(t, mem, uploadID, 1)))

	got := collectSpoolMetrics(t, reader)
	if got["ingot.spool.evictions/parked"] != parts || got["ingot.spool.evicted/parked"] <= 0 {
		t.Fatalf("parked removals = %d files, %d bytes; want %d files (all: %v)",
			got["ingot.spool.evictions/parked"], got["ingot.spool.evicted/parked"], parts, got)
	}
	if got["ingot.spool.usage"] != 0 {
		t.Fatalf("usage after the part parked = %d, want 0", got["ingot.spool.usage"])
	}
}

func TestSpoolMetrics_Release(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newDeferredBackend(t, inmem.NopUploader{}, meteredBackend(reader))
	key := "deleted"
	putObj(t, b, key, testBody(1<<10))
	size := blobSize(t, b, blobDigestOf(t, b, key, ""))
	deleteObj(t, b, key)

	if n, err := b.SweepPendingReleases(context.Background()); err != nil || n != 1 {
		t.Fatalf("release sweep executed %d releases (err=%v), want 1", n, err)
	}

	got := collectSpoolMetrics(t, reader)
	if got["ingot.spool.evictions/released"] != 1 || got["ingot.spool.evicted/released"] != size {
		t.Fatalf("released removals = %d files, %d bytes; want 1 file, %d bytes (all: %v)",
			got["ingot.spool.evictions/released"], got["ingot.spool.evicted/released"], size, got)
	}
}
