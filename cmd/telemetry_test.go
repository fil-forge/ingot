package cmd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// sampledNames starts an orphan client span, then a request span with a
// client child, and returns the names of the spans sampler kept.
func sampledNames(t *testing.T, sampler sdktrace.Sampler) []string {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler), sdktrace.WithSpanProcessor(rec))
	tracer := tp.Tracer("test")
	ctx := context.Background()

	_, orphan := tracer.Start(ctx, "orphan query", trace.WithSpanKind(trace.SpanKindClient))
	orphan.End()

	reqCtx, req := tracer.Start(ctx, "PutObject", trace.WithSpanKind(trace.SpanKindServer))
	_, query := tracer.Start(reqCtx, "query", trace.WithSpanKind(trace.SpanKindClient))
	query.End()
	req.End()

	var names []string
	for _, s := range rec.Ended() {
		names = append(names, s.Name())
	}
	return names
}

func TestSamplerFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name, sampler, arg string
		want               []string
	}{
		{name: "default traces every request", want: []string{"query", "PutObject"}},
		{name: "ratio 1", arg: "1", want: []string{"query", "PutObject"}},
		{name: "ratio 0", arg: "0", want: nil},
		{name: "named ratio sampler", sampler: "parentbased_traceidratio", arg: "1", want: []string{"query", "PutObject"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_SAMPLER", tc.sampler)
			t.Setenv("OTEL_TRACES_SAMPLER_ARG", tc.arg)
			sampler, err := samplerFromEnv()
			if err != nil {
				t.Fatal(err)
			}
			if got := sampledNames(t, sampler); !slices.Equal(got, tc.want) {
				t.Fatalf("expected %v sampled, got %v", tc.want, got)
			}
		})
	}
}

func TestSamplerFromEnvRejectsBadRatio(t *testing.T) {
	for _, arg := range []string{"1.5", "-0.1", "one"} {
		t.Setenv("OTEL_TRACES_SAMPLER_ARG", arg)
		if _, err := samplerFromEnv(); err == nil {
			t.Fatalf("expected an error for OTEL_TRACES_SAMPLER_ARG=%q", arg)
		}
	}
}

// Any other named sampler is the SDK's to build from the environment.
func TestSamplerFromEnvDefersOtherSamplers(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
	sampler, err := samplerFromEnv()
	if err != nil || sampler != nil {
		t.Fatalf("expected no sampler, got %v, %v", sampler, err)
	}
}

func TestSetupTracingExportsToConfiguredEndpoint(t *testing.T) {
	got := make(chan string, 1)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case got <- r.URL.Path:
		default:
		}
	}))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	prev := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	shutdown, err := setupTracing(context.Background(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	_, span := otel.Tracer("test").Start(context.Background(), "PutObject", trace.WithSpanKind(trace.SpanKindServer))
	span.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case path := <-got:
		if path != "/v1/traces" {
			t.Fatalf("expected an export to /v1/traces, got %s", path)
		}
	default:
		t.Fatal("expected shutdown to flush the span to the collector")
	}
}

func TestSetupTracingOffWithoutEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	prev := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	if _, err := setupTracing(context.Background(), zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	if otel.GetTracerProvider() != prev {
		t.Fatal("expected no tracer provider installed without an endpoint")
	}
}
