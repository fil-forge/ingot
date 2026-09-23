package cmd

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/internal/build"
)

// setupTracing installs the global tracer provider and propagators, exporting
// spans over OTLP/HTTP. Tracing is off unless OTEL_EXPORTER_OTLP_ENDPOINT or
// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT names a collector. The other standard
// OTEL_* environment variables apply as usual: OTEL_EXPORTER_OTLP_HEADERS
// authenticates to the collector, OTEL_SERVICE_NAME and
// OTEL_RESOURCE_ATTRIBUTES label the spans, and OTEL_TRACES_SAMPLER_ARG sets
// the fraction of requests traced (see samplerFromEnv). The returned function
// flushes buffered spans and stops the exporter.
func setupTracing(ctx context.Context, logger *zap.Logger) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	if endpoint == "" {
		logger.Info("tracing off: no OTLP endpoint configured")
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("ingot"),
			semconv.ServiceVersion(build.Version),
		),
		resource.WithHost(),
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, fmt.Errorf("creating trace resource: %w", err)
	}

	tpOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	}
	sampler, err := samplerFromEnv()
	if err != nil {
		return nil, err
	}
	if sampler != nil {
		tpOpts = append(tpOpts, sdktrace.WithSampler(sampler))
	}
	tp := sdktrace.NewTracerProvider(tpOpts...)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Warn("opentelemetry", zap.Error(err))
	}))
	logger.Info("tracing enabled", zap.String("endpoint", endpoint))
	return tp.Shutdown, nil
}

// samplerFromEnv returns ingot's sampler: a caller's sampling decision is
// followed, and a trace starting at ingot is sampled by requestRoots at the
// ratio in OTEL_TRACES_SAMPLER_ARG (default 1, every request). It applies when
// OTEL_TRACES_SAMPLER is unset or parentbased_traceidratio. Any other
// OTEL_TRACES_SAMPLER value returns nil: sdktrace.NewTracerProvider builds
// that sampler from the environment itself when no WithSampler is given.
func samplerFromEnv() (sdktrace.Sampler, error) {
	switch os.Getenv("OTEL_TRACES_SAMPLER") {
	case "", "parentbased_traceidratio":
	default:
		return nil, nil
	}
	ratio := 1.0
	if arg := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG")); arg != "" {
		r, err := strconv.ParseFloat(arg, 64)
		if err != nil || r < 0 || r > 1 {
			return nil, fmt.Errorf("OTEL_TRACES_SAMPLER_ARG %q: want a ratio from 0 to 1", arg)
		}
		ratio = r
	}
	return sdktrace.ParentBased(requestRoots{ratio: sdktrace.TraceIDRatioBased(ratio)}), nil
}

// requestRoots samples traces that start at ingot by ratio, except one whose
// root is a client span: a Postgres query or outbound call made outside any
// request, such as a background poll, which would otherwise arrive as a
// stream of single-span traces.
type requestRoots struct {
	ratio sdktrace.Sampler
}

func (s requestRoots) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if p.Kind == trace.SpanKindClient {
		return sdktrace.SamplingResult{
			Decision:   sdktrace.Drop,
			Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
		}
	}
	return s.ratio.ShouldSample(p)
}

func (s requestRoots) Description() string { return "RequestRoots{" + s.ratio.Description() + "}" }
