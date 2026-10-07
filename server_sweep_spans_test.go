package ingot

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/fil-forge/ingot/config"
)

// TestSweepersRecordRootSpans: each background sweep starts a trace of its
// own, and the cancellation that stops it at shutdown does not mark it
// failed.
func TestSweepersRecordRootSpans(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	// A 2ms session TTL makes the multipart sweeper tick every millisecond.
	s := newInmemServer(t, config.ServerConfig{MultipartSessionTTL: 2 * time.Millisecond})
	require.NoError(t, s.Start(t.Context()))
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.Stop(ctx))

	var sweeps int
	for _, sp := range rec.Ended() {
		if sp.Name() != "sweep.multipart_sessions" {
			continue
		}
		sweeps++
		require.False(t, sp.Parent().IsValid(), "a sweep span is a trace's root")
		require.NotEqual(t, codes.Error, sp.Status().Code, "sweep span marked failed: %s", sp.Status().Description)
	}
	require.NotZero(t, sweeps, "no sweep.multipart_sessions spans")
}
