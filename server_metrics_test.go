package ingot

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"testing"
	"time"

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

// TestServerReportsLocalBlobGauges: New registers the local blob and catalog
// journal gauges on the global meter provider, and Stop unregisters them.
func TestServerReportsLocalBlobGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	s := newInmemServer(t, config.ServerConfig{LocalBlobMaxBytes: 1 << 20})

	require.Equal(t, map[string]bool{
		"ingot.local_blobs.usage":          true,
		"ingot.local_blobs.budget":         true,
		"ingot.catalog_journal.blocks":     true,
		"ingot.catalog_journal.bytes":      true,
		"ingot.catalog_journal.oldest_age": true,
	}, metricNames(t, reader), "metrics reported after New")

	require.NoError(t, s.Stop(context.Background()))
	require.Empty(t, metricNames(t, reader), "metrics reported after Stop")
}

// TestServerReportsCatalogUsageGauges: with a catalog usage store, the
// catalog usage gauges report each bucket's shipped catalog segments under
// its space, retired ones included, once the sweep Start runs has refreshed
// them; a later refresh replaces the figures, and Stop unregisters them.
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
	index := multihash.Multihash("index-digest")
	// segment records a shipped segment of bk, and retires it if asked.
	segment := func(size, dead int64, index multihash.Multihash, retire bool) {
		t.Helper()
		seq, err := mem.NextSegmentSeq(ctx)
		require.NoError(t, err)
		require.NoError(t, mem.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, "bk"))
		require.NoError(t, mem.MarkSegmentSealed(ctx, blockstore.PlaneCatalog, seq, 1, size, nil, nil))
		require.NoError(t, mem.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, 2, index, dead, nil))
		if retire {
			require.NoError(t, mem.RetireSegment(ctx, blockstore.PlaneCatalog, seq, 3))
		}
	}
	segment(1000, 300, index, false)
	segment(500, -1, index, false) // unmeasured
	segment(2000, 900, index, true)
	segment(59, 0, nil, false) // shipped without registering anything: not on Forge

	s := newInmemServer(t, config.ServerConfig{}, func(d *ServerDeps) { d.CatalogUsage = mem })
	require.Empty(t, gaugeValues(t, reader, "ingot.catalog.forge_bytes"), "reported before the first refresh")

	require.NoError(t, s.Start(ctx))
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = s.Stop(context.Background())
		}
	})
	key := space.String()
	require.Eventually(t, func() bool {
		return gaugeValues(t, reader, "ingot.catalog.forge_bytes")[key] == 3500
	}, 5*time.Second, 10*time.Millisecond, "the startup sweep never reported the bucket's shipped bytes")
	require.Equal(t, map[string]int64{key: 1200}, gaugeValues(t, reader, "ingot.catalog.dead_on_arrival_bytes"))
	require.Equal(t, map[string]int64{key: 500}, gaugeValues(t, reader, "ingot.catalog.unmeasured_bytes"))

	// A segment shipped since counts at the next refresh.
	segment(400, 100, index, false)
	n, err := s.backend.RefreshCatalogUsage(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, map[string]int64{key: 3900}, gaugeValues(t, reader, "ingot.catalog.forge_bytes"))
	require.Equal(t, map[string]int64{key: 1300}, gaugeValues(t, reader, "ingot.catalog.dead_on_arrival_bytes"))

	stopped = true
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
