package ingot

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"testing"

	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/regionkey"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/tenantkey"
)

// TestServerReportsLocalBlobGauges: New registers the local blob gauges on
// the global meter provider, and Stop unregisters them.
func TestServerReportsLocalBlobGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	s := newInmemServer(t, config.ServerConfig{LocalBlobMaxBytes: 1 << 20})

	require.Equal(t, map[string]bool{
		"ingot.local_blobs.usage":  true,
		"ingot.local_blobs.budget": true,
	}, metricNames(t, reader), "metrics reported after New")

	require.NoError(t, s.Stop(context.Background()))
	require.Empty(t, metricNames(t, reader), "metrics reported after Stop")
}

// TestServerReportsCatalogUsageGauges: with the catalog stores, the catalog
// gauges report each bucket's shipped catalog segments under its space once
// refreshed, the live and dead bytes once a walk has completed, and the
// coverage by walk state; Stop unregisters them.
func TestServerReportsCatalogUsageGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	ctx := t.Context()
	mem := inmem.NewMemStore()
	space, err := did.Parse("did:web:catalog-usage.example")
	require.NoError(t, err)
	require.NoError(t, mem.Create(ctx, "bk", space, registry.CreateState{}))
	// Two shipped segments, one measured and one not, and one that shipped
	// without registering anything, which is not on Forge.
	index := multihash.Multihash("index-digest")
	for _, seg := range []struct {
		size, dead int64
		index      multihash.Multihash
	}{{size: 1000, dead: 300, index: index}, {size: 500, dead: -1, index: index}, {size: 59, dead: 0}} {
		seq, err := mem.NextSegmentSeq(ctx)
		require.NoError(t, err)
		require.NoError(t, mem.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, "bk"))
		require.NoError(t, mem.MarkSegmentSealed(ctx, blockstore.PlaneCatalog, seq, 1, seg.size, nil, nil))
		require.NoError(t, mem.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, 2, seg.index, seg.dead, nil))
	}

	s := newInmemServer(t, config.ServerConfig{}, func(d *ServerDeps) {
		d.CatalogUsage = mem
		d.CatalogWalks = mem
	})
	require.Empty(t, gaugeValues(t, reader, "ingot.catalog.forge_bytes"), "reported before the first refresh")

	n, err := s.backend.RefreshCatalogUsage(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	bySpace := map[string]int64{space.String(): 1500}
	require.Equal(t, bySpace, gaugeValues(t, reader, "ingot.catalog.forge_bytes"))
	bySpace[space.String()] = 300
	require.Equal(t, bySpace, gaugeValues(t, reader, "ingot.catalog.dead_on_arrival_bytes"))
	bySpace[space.String()] = 500
	require.Equal(t, bySpace, gaugeValues(t, reader, "ingot.catalog.unmeasured_bytes"))
	require.Empty(t, gaugeValues(t, reader, "ingot.catalog.live_bytes"), "live bytes reported before a walk")
	require.Equal(t, map[string]int64{"current": 0, "stale": 0, "never": 1500}, coverageValues(t, reader))

	// The bucket has no root, so its walk needs no authority and finds
	// nothing live: every shipped byte is dead.
	due, err := s.walker.Due(ctx)
	require.NoError(t, err)
	require.Len(t, due, 1)
	walked, err := s.walker.Walk(ctx, due[0])
	require.NoError(t, err)
	require.True(t, walked.Completed)
	_, err = s.backend.RefreshCatalogUsage(ctx)
	require.NoError(t, err)
	bySpace[space.String()] = 0
	require.Equal(t, bySpace, gaugeValues(t, reader, "ingot.catalog.live_bytes"))
	bySpace[space.String()] = 1500
	require.Equal(t, bySpace, gaugeValues(t, reader, "ingot.catalog.dead_bytes"))
	bySpace[space.String()] = 1200
	require.Equal(t, bySpace, gaugeValues(t, reader, "ingot.catalog.dead_after_ship_bytes"))
	require.Contains(t, gaugeValues(t, reader, "ingot.catalog.walked_at"), space.String())
	require.Equal(t, map[string]int64{"current": 1500, "stale": 0, "never": 0}, coverageValues(t, reader))

	// A segment shipping after the walk, the root unchanged, counts at once:
	// the walked root's live bytes still hold.
	seq, err := mem.NextSegmentSeq(ctx)
	require.NoError(t, err)
	require.NoError(t, mem.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, "bk"))
	require.NoError(t, mem.MarkSegmentSealed(ctx, blockstore.PlaneCatalog, seq, 1, 400, nil, nil))
	require.NoError(t, mem.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, 2, index, 100, nil))
	_, err = s.backend.RefreshCatalogUsage(ctx)
	require.NoError(t, err)
	bySpace[space.String()] = 1900
	require.Equal(t, bySpace, gaugeValues(t, reader, "ingot.catalog.dead_bytes"))
	bySpace[space.String()] = 1500
	require.Equal(t, bySpace, gaugeValues(t, reader, "ingot.catalog.dead_after_ship_bytes"))

	require.NoError(t, s.Stop(context.Background()))
	require.Empty(t, metricNames(t, reader), "metrics reported after Stop")
}

// gaugeValues returns the int64 gauge name's current values by their space
// attribute.
func gaugeValues(t *testing.T, reader *sdkmetric.ManualReader, name string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "%s is a %T", name, m.Data)
			for _, dp := range g.DataPoints {
				v, _ := dp.Attributes.Value("space")
				out[v.AsString()] = dp.Value
			}
		}
	}
	return out
}

// coverageValues returns ingot.catalog.walk_coverage_bytes by state.
func coverageValues(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "ingot.catalog.walk_coverage_bytes" {
				continue
			}
			for _, dp := range m.Data.(metricdata.Gauge[int64]).DataPoints {
				v, _ := dp.Attributes.Value("state")
				out[v.AsString()] = dp.Value
			}
		}
	}
	return out
}

// metricNames returns the names of the metrics reader collects now.
func metricNames(t *testing.T, reader *sdkmetric.ManualReader) map[string]bool {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	names := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
		}
	}
	return names
}

// newInmemServer builds a Server over the in-memory fakes. cfg's address,
// data directory, region and connection limits are filled in; each of opts
// may adjust the dependencies before New.
func newInmemServer(t *testing.T, cfg config.ServerConfig, opts ...func(*ServerDeps)) *Server {
	t.Helper()
	id, err := identity.New("", "did:web:ingot.test")
	require.NoError(t, err)
	svc := iam.New(neverAuthorizer{}, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())
	t.Cleanup(func() { require.NoError(t, svc.Shutdown()) })
	kek := make([]byte, regionkey.KEKLen)
	_, _ = rand.Read(kek)
	regionKeys, err := regionkey.NewInProcessProvider("v1", kek)
	require.NoError(t, err)
	tenantPriv, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	mem := inmem.NewMemStore()
	cfg.Addr = freeAddr(t)
	cfg.DataDir = t.TempDir()
	cfg.Region = "us-east-1"
	cfg.MaxConnections = 16
	cfg.MaxRequests = 16
	deps := ServerDeps{
		BaseBlockReader: inmem.NopBaseReader{},
		Uploader:        inmem.NopUploader{},
		BodyUploader:    inmem.NopUploader{},
		Deferred:        inmem.NopUploader{},
		Remover:         inmem.NopUploader{},
		Registrar:       inmem.NopUploader{},
		Registry:        mem,
		UploadRegs:      mem,
		Intents:         mem,
		Locations:       mem,
		Inclusions:      mem,
		BlobRefs:        mem,
		GC:              mem,
		Multipart:       mem,
		Parks:           mem,
		PendingReleases: mem,
		EncParams:       mem,
		RegionKeys:      regionKeys,
		TenantKeys:      tenantkey.NewStatic(tenantPriv.PublicKey()),
		Meta:            mem,
		Identity:        id,
		IAM:             svc,
	}
	for _, opt := range opts {
		opt(&deps)
	}
	s, err := New(t.Context(), cfg, deps)
	require.NoError(t, err)
	return s
}
