package cmd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// sampledNames starts an orphan client span, then a request span with a
// client child, and returns the names of the spans kept. The provider is
// built as setupTracing builds it: a nil sampler leaves the SDK's own, which
// NewTracerProvider reads from OTEL_TRACES_SAMPLER.
func sampledNames(t *testing.T, sampler sdktrace.Sampler) []string {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	opts := []sdktrace.TracerProviderOption{sdktrace.WithSpanProcessor(rec)}
	if sampler != nil {
		opts = append(opts, sdktrace.WithSampler(sampler))
	}
	tp := sdktrace.NewTracerProvider(opts...)
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
		// Other samplers are the SDK's, and keep background client spans.
		{name: "always_off", sampler: "always_off", want: nil},
		{name: "always_on", sampler: "always_on", want: []string{"orphan query", "query", "PutObject"}},
		{name: "traceidratio 0", sampler: "traceidratio", arg: "0", want: nil},
		{name: "traceidratio 1", sampler: "traceidratio", arg: "1", want: []string{"orphan query", "query", "PutObject"}},
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

func TestSetupTracingExportsToConfiguredEndpoint(t *testing.T) {
	got := make(chan string, 1)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case got <- r.URL.Path:
		default:
		}
	}))
	defer collector.Close()
	isolateOTelEnv(t)
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

// namespaceOf installs tracing against a collector that accepts everything,
// starts a span, and returns its resource's service.namespace.
func namespaceOf(t *testing.T) string {
	t.Helper()
	collector := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	prev := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	shutdown, err := setupTracing(context.Background(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	_, span := otel.Tracer("test").Start(context.Background(), "PutObject")
	ro, ok := span.(sdktrace.ReadOnlySpan)
	if !ok {
		t.Fatal("expected an SDK span")
	}
	ns, _ := ro.Resource().Set().Value(semconv.ServiceNamespaceKey)
	span.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	return ns.AsString()
}

func TestSetupTracingReportsForgeNamespace(t *testing.T) {
	isolateOTelEnv(t)
	if got := namespaceOf(t); got != "forge" {
		t.Fatalf("expected service.namespace forge, got %q", got)
	}
}

func TestSetupTracingNamespaceFromEnvWins(t *testing.T) {
	isolateOTelEnv(t)
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.namespace=elsewhere")
	if got := namespaceOf(t); got != "elsewhere" {
		t.Fatalf("expected OTEL_RESOURCE_ATTRIBUTES to set service.namespace, got %q", got)
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

// isolateOTelEnv unsets, for the test's duration, the OTEL_* variables that
// would send the export elsewhere or sample the span away, so the shell or CI
// running the tests cannot change the outcome.
func isolateOTelEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_TRACES_SAMPLER",
		"OTEL_TRACES_SAMPLER_ARG",
		"OTEL_SDK_DISABLED",
		"OTEL_RESOURCE_ATTRIBUTES",
		"OTEL_SERVICE_NAME",
	} {
		t.Setenv(k, "") // restores the original value when the test ends
		os.Unsetenv(k)
	}
}

func TestCollectorHostOmitsCredentials(t *testing.T) {
	for endpoint, want := range map[string]string{
		"http://otel-collector:4318":                      "http://otel-collector:4318",
		"https://user:secret@collector.example.com/v1":    "https://collector.example.com",
		"https://collector.example.com:4318?token=hunter": "https://collector.example.com:4318",
		"not a url": "unparsed endpoint",
	} {
		if got := collectorHost(endpoint); got != want {
			t.Errorf("collectorHost(%q) = %q, want %q", endpoint, got, want)
		}
	}
}

func TestSetupMetricsOffWithoutEndpoint(t *testing.T) {
	isolateOTelEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	prev := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	shutdown, err := setupMetrics(context.Background(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if otel.GetMeterProvider() != prev {
		t.Fatal("expected no meter provider installed without an endpoint")
	}
}

// With a collector configured, shutdown exports what the meters observed.
func TestSetupMetricsExportsOnShutdown(t *testing.T) {
	got := make(chan string, 4)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case got <- r.URL.Path:
		default:
		}
	}))
	defer collector.Close()
	isolateOTelEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	prev := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	shutdown, err := setupMetrics(context.Background(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	counter, err := otel.Meter("test").Int64Counter("test.requests")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(context.Background(), 1)
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case path := <-got:
		if path != "/v1/metrics" {
			t.Fatalf("expected an export to /v1/metrics, got %s", path)
		}
	default:
		t.Fatal("expected shutdown to flush the metric to the collector")
	}
}
