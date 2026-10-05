package blockstore

import (
	"context"
	"testing"

	"github.com/fil-forge/ucantone/did"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestLayered_CountBlobReadsByTier(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	digest := mustDigest(t, []byte("blob"))

	hit := NewLayered(fakeBlobTier{data: []byte("from-spool")}, nil, fakeBlobTier{data: []byte("from-base")})
	miss := NewLayered(fakeBlobTier{data: nil}, nil, fakeBlobTier{data: []byte("from-base")})
	for _, l := range []*Layered{hit, miss} {
		if err := l.CountBlobReads(meter); err != nil {
			t.Fatalf("CountBlobReads: %v", err)
		}
	}
	for _, l := range []*Layered{hit, hit, miss} {
		if _, err := readOpenBlob(t, l, digest); err != nil {
			t.Fatalf("OpenBlob: %v", err)
		}
	}
	rc, err := miss.OpenBlobRange(context.Background(), did.Undef, digest, 0, 1)
	if err != nil {
		t.Fatalf("OpenBlobRange: %v", err)
	}
	_ = rc.Close()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "ingot.spool.reads" {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				tier, _ := dp.Attributes.Value(attribute.Key("tier"))
				got[tier.AsString()] = dp.Value
			}
		}
	}
	if want := map[string]int64{"spool": 2, "network": 2}; len(got) != 2 || got["spool"] != want["spool"] || got["network"] != want["network"] {
		t.Fatalf("ingot.spool.reads by tier = %v, want %v", got, want)
	}
}
