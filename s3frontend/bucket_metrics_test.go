package s3frontend

import (
	"context"
	"testing"

	"github.com/fil-forge/libforge/testutil"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/fil-forge/ingot/registry"
)

// TestBucketGaugesReportTheTotals: the gauges report every bucket and the
// objects the buckets hold now, as each collection finds them.
func TestBucketGaugesReportTheTotals(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, mem := newCountingBackend(t, meteredBackend(reader))
	totals := func() [2]int64 {
		got := collectLocalBlobMetrics(t, reader)
		return [2]int64{got["ingot.buckets.count"], got["ingot.objects.count"]}
	}
	require.Equal(t, [2]int64{1, 0}, totals(), "one empty bucket")

	putObjV(t, b, "a", []byte("hello"))
	putObjV(t, b, "b", []byte("world"))
	putObjV(t, b, "b", []byte("again"))
	require.NoError(t, mem.Create(context.Background(), "other", testutil.RandomDID(t), registry.CreateState{}))

	require.Equal(t, [2]int64{2, 2}, totals(), "two buckets, and two keys after an overwrite took a version away")
}
