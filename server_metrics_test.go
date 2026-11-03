package ingot

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"testing"

	"github.com/fil-forge/libforge/identity"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/regionkey"
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
// data directory, region and connection limits are filled in.
func newInmemServer(t *testing.T, cfg config.ServerConfig) *Server {
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
	s, err := New(t.Context(), cfg, ServerDeps{
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
	})
	require.NoError(t, err)
	return s
}
