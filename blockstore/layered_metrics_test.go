package blockstore

import (
	"context"
	"testing"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
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
			if m.Name != "ingot.local_blobs.reads" {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				tier, _ := dp.Attributes.Value(attribute.Key("tier"))
				got[tier.AsString()] = dp.Value
			}
		}
	}
	if want := map[string]int64{"local": 2, "network": 2}; len(got) != 2 || got["local"] != want["local"] || got["network"] != want["network"] {
		t.Fatalf("ingot.local_blobs.reads by tier = %v, want %v", got, want)
	}
}

// TestLayered_OnReadSeesEveryRead: the OnRead hook sees the space of block,
// typed and blob reads, before the read is served and whether or not it is.
func TestLayered_OnReadSeesEveryRead(t *testing.T) {
	space, err := did.Parse("did:web:onread.example")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	digest := mustDigest(t, []byte("blob"))
	l := NewLayered(fakeBlobTier{data: []byte("local")}, nil, fakeBlobTier{})
	var seen []did.DID
	l.OnRead(func(_ context.Context, s did.DID) { seen = append(seen, s) })

	ctx := context.Background()
	if _, err := l.GetBlock(ctx, space, cid.NewCidV1(cid.DagCBOR, mustDigest(t, []byte("missing")))); err == nil {
		t.Fatal("GetBlock of a missing block succeeded")
	}
	var out any
	_ = l.Get(ctx, space, cid.NewCidV1(cid.DagCBOR, mustDigest(t, []byte("missing too"))), &out)
	rc, err := l.OpenBlob(ctx, space, digest)
	if err != nil {
		t.Fatalf("OpenBlob: %v", err)
	}
	_ = rc.Close()
	rc, err = l.OpenBlobRange(ctx, space, digest, 0, 1)
	if err != nil {
		t.Fatalf("OpenBlobRange: %v", err)
	}
	_ = rc.Close()

	if len(seen) != 4 {
		t.Fatalf("OnRead saw %d reads, want 4", len(seen))
	}
	for _, s := range seen {
		if s != space {
			t.Fatalf("OnRead saw space %s, want %s", s, space)
		}
	}
}
