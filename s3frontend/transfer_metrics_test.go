package s3frontend

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3response"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collectTransferMetrics reads the transfer counters by name, and the
// per-space egress counter by name and space ("…/<space DID>"), checking each
// of its points carries tenant.
func collectTransferMetrics(t *testing.T, reader *sdkmetric.ManualReader, tenant string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			d, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range d.DataPoints {
				name := m.Name
				if space, ok := dp.Attributes.Value(attribute.Key("space")); ok {
					name += "/" + space.AsString()
					if tn, _ := dp.Attributes.Value(attribute.Key("tenant")); tn.AsString() != tenant {
						t.Fatalf("%s tenant = %q, want %q", name, tn.AsString(), tenant)
					}
				}
				got[name] = dp.Value
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
	b, mem := newSweepBackend(t, meteredBackend(reader))
	ctx := t.Context()
	st, err := mem.Get(ctx, sweepBucket)
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	perSpace := "ingot.s3.bucket_object_egress_bytes/" + st.Space.String()

	bucket, key := sweepBucket, "obj"
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(bytes.Repeat([]byte{'a'}, 1000))}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if got := collectTransferMetrics(t, reader, st.Tenant.String()); got["ingot.s3.object_ingress_bytes"] != 1000 || got["ingot.s3.object_egress_bytes"] != 0 {
		t.Fatalf("after PutObject: %v, want 1000 bytes in and none out", got)
	}

	var sent int64
	get := func(rng string, read int64) {
		t.Helper()
		out, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key, Range: backend.GetPtrFromString(rng)})
		if err != nil {
			t.Fatalf("GetObject %q: %v", rng, err)
		}
		if _, err := io.CopyN(io.Discard, out.Body, read); err != nil {
			t.Fatalf("read %q: %v", rng, err)
		}
		if got := collectTransferMetrics(t, reader, st.Tenant.String())["ingot.s3.object_egress_bytes"]; got != sent {
			t.Fatalf("egress before Close of %q = %d, want %d", rng, got, sent)
		}
		sent += read
		if err := out.Body.Close(); err != nil {
			t.Fatalf("close %q: %v", rng, err)
		}
		if err := out.Body.Close(); err != nil {
			t.Fatalf("second close %q: %v", rng, err)
		}
	}

	get("", 1000)
	get("bytes=0-99", 100)
	get("", 10) // the client went away after 10 bytes
	got := collectTransferMetrics(t, reader, st.Tenant.String())
	if got["ingot.s3.object_egress_bytes"] != 1110 || got[perSpace] != 1110 {
		t.Fatalf("after GetObjects: %v, want 1110 bytes out in all and for %s", got, perSpace)
	}

	uploadID := mpCreate(t, b, "mp", "", "")
	uploadDistinctParts(t, b, "mp", uploadID, 1)
	if got := collectTransferMetrics(t, reader, st.Tenant.String()); got["ingot.s3.object_ingress_bytes"] != 1000+backend.MinPartSize {
		t.Fatalf("after UploadPart: ingress = %d, want %d", got["ingot.s3.object_ingress_bytes"], 1000+backend.MinPartSize)
	}
}
