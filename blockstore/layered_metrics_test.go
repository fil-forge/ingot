package blockstore

import (
	"context"
	"testing"

	"github.com/fil-forge/ucantone/did"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestLayered_CountBlobReadsByTier(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	digest := mustDigest(t, []byte("blob"))

	hit := NewLayered(fakeBlobTier{data: []byte("from-cache")}, nil, fakeBlobTier{data: []byte("from-base")})
	miss := NewLayered(fakeBlobTier{data: nil}, nil, fakeBlobTier{data: []byte("from-base")})
	for _, l := range []*Layered{hit, miss} {
		require.NoError(t, l.CountBlobReads(meter), "CountBlobReads")
	}
	for _, l := range []*Layered{hit, hit, miss} {
		_, err := readOpenBlob(t, l, digest)
		require.NoError(t, err, "OpenBlob")
	}
	rc, err := miss.OpenBlobRange(context.Background(), did.Undef, digest, 0, 1)
	require.NoError(t, err, "OpenBlobRange")
	_ = rc.Close()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm), "collect")
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "ingot.local_blobs.reads" {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				tier, _ := dp.Attributes.Value(attribute.Key("tier"))
				got[tier.AsString()] = dp.Value
			}
		}
	}
	want := map[string]int64{"local": 2, "network": 2}
	require.Len(t, got, 2, "ingot.local_blobs.reads by tier")
	require.Equal(t, want["local"], got["local"], "ingot.local_blobs.reads by tier")
	require.Equal(t, want["network"], got["network"], "ingot.local_blobs.reads by tier")
}
