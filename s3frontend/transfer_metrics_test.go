package s3frontend

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collectTransferMetrics reads the transfer counters by name, failing if one
// carries attributes: they are totals.
func collectTransferMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm), "collect")
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			d, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range d.DataPoints {
				if strings.HasPrefix(m.Name, "ingot.s3.") {
					require.Empty(t, dp.Attributes.ToSlice(), "%s carries attributes", m.Name)
				}
				got[m.Name] = dp.Value
			}
		}
	}
	return got
}

// TestTransferMetrics: PutObject and UploadPart count the bytes they receive;
// GetObject counts the bytes read from its body, once, on Close, whether the
// read was whole, ranged or abandoned part way.
func TestTransferMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newSweepBackend(t, meteredBackend(reader))
	ctx := t.Context()

	bucket, key := sweepBucket, "obj"
	body := bytes.Repeat([]byte{'a'}, 1000)
	_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(body), ContentLength: byteLen(body)})
	require.NoError(t, err, "PutObject")
	got := collectTransferMetrics(t, reader)
	require.Equal(t, int64(1000), got["ingot.s3.object_ingress_bytes"], "after PutObject: ingress")
	require.Zero(t, got["ingot.s3.object_egress_bytes"], "after PutObject: egress")

	var sent int64
	get := func(rng string, read int64) {
		t.Helper()
		out, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key, Range: backend.GetPtrFromString(rng)})
		require.NoError(t, err, "GetObject %q", rng)
		_, err = io.CopyN(io.Discard, out.Body, read)
		require.NoError(t, err, "read %q", rng)
		got := collectTransferMetrics(t, reader)["ingot.s3.object_egress_bytes"]
		require.Equal(t, sent, got, "egress before Close of %q", rng)
		sent += read
		require.NoError(t, out.Body.Close(), "close %q", rng)
		require.NoError(t, out.Body.Close(), "second close %q", rng)
	}

	get("", 1000)
	get("bytes=0-99", 100)
	get("", 10) // the client went away after 10 bytes
	got = collectTransferMetrics(t, reader)
	require.Equal(t, int64(1110), got["ingot.s3.object_egress_bytes"], "after GetObjects: egress")

	uploadID := mpCreate(t, b, "mp", "", "")
	uploadDistinctParts(t, b, "mp", uploadID, 1)
	got = collectTransferMetrics(t, reader)
	require.Equal(t, int64(1000+backend.MinPartSize), got["ingot.s3.object_ingress_bytes"], "after UploadPart: ingress")
}
