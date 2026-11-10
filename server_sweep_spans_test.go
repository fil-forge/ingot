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

	shortSweepInterval(t)
	s := newTestServer(t, config.ServerConfig{})
	require.NoError(t, s.Start(t.Context()))
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.Stop(ctx))

	var sweeps int
	for _, sp := range rec.Ended() {
		if sp.Name() != "sweep.local_blobs" {
			continue
		}
		sweeps++
		require.False(t, sp.Parent().IsValid(), "a sweep span is a trace's root")
		require.NotEqual(t, codes.Error, sp.Status().Code, "sweep span marked failed: %s", sp.Status().Description)
	}
	require.NotZero(t, sweeps, "no sweep.local_blobs spans")
}

// shortSweepInterval makes the local blob sweeper tick every millisecond
// for the rest of the test.
func shortSweepInterval(t *testing.T) {
	t.Helper()
	saved := localBlobSweepInterval
	localBlobSweepInterval = time.Millisecond
	t.Cleanup(func() { localBlobSweepInterval = saved })
}

// TestEndSweepSpan: a sweep's error marks its span failed while the server
// runs, and not once Stop has cancelled the sweeps.
func TestEndSweepSpan(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stopping bool
		want     codes.Code
	}{
		{"running", false, codes.Error},
		{"stopping", true, codes.Unset},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			s := &Server{}
			s.sweepCtx, s.sweepCancel = context.WithCancel(context.Background())
			defer s.sweepCancel()
			if tc.stopping {
				s.sweepCancel()
			}
			_, span := tp.Tracer("test").Start(context.Background(), "sweep.test")
			s.endSweepSpan(span, context.Canceled)
			require.Len(t, rec.Ended(), 1)
			require.Equal(t, tc.want, rec.Ended()[0].Status().Code)
		})
	}
}
