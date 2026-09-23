// Package tracing is ingot's OpenTelemetry instrumentation: the per-request
// server span and the instrumented HTTP client every outbound call goes
// through. Spans go to the global tracer provider, so the host decides where
// they are exported: the daemon installs one (cmd/telemetry.go); an embedding
// host installs its own or leaves OpenTelemetry's no-op default.
package tracing

import (
	"context"
	"net/http"

	"github.com/gofiber/fiber/v3"
	"github.com/valyala/fasthttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

// Name is the instrumentation scope of ingot's own spans.
const Name = "github.com/fil-forge/ingot"

// Tracer returns ingot's tracer from the global provider.
func Tracer() trace.Tracer {
	return otel.Tracer(Name)
}

// NewHTTPClient returns an HTTP client that records each request as a client
// span and propagates the trace context to the callee.
func NewHTTPClient() *http.Client {
	return &http.Client{Transport: NewTransport()}
}

// NewTransport is the round tripper behind NewHTTPClient, for clients that
// take a transport rather than a client.
func NewTransport() http.RoundTripper {
	return otelhttp.NewTransport(http.DefaultTransport)
}

// Middleware starts the server span for each request, continuing a trace
// context the caller sent. The span is named for the HTTP method until the
// audit logger renames it for the S3 action (see SpanFromRequest).
//
// versitygw hands the backend the bare *fasthttp.RequestCtx as its
// context.Context, whose Value reads the request's user values, so a span
// placed on a derived context never reaches the backend. Middleware instead
// stores the span as a user value under the trace API's own key, which makes
// trace.SpanFromContext find it on the request context itself: every backend
// call, and the Postgres and HTTP spans beneath it, joins the request's trace.
func Middleware() fiber.Handler {
	tracer := Tracer()
	return func(c fiber.Ctx) error {
		rc := c.RequestCtx()
		parent := otel.GetTextMapPropagator().Extract(context.Background(), headerCarrier{&rc.Request.Header})
		method := c.Method()
		_, span := tracer.Start(parent, "S3 "+method,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(semconv.HTTPRequestMethodKey.String(method)),
		)
		defer span.End()
		if spanKey != nil {
			rc.SetUserValue(spanKey, span)
		}

		err := c.Next()
		status := rc.Response.StatusCode()
		span.SetAttributes(semconv.HTTPResponseStatusCode(status))
		if status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
		return err
	}
}

// SpanFromRequest returns the request's server span, or a no-op span when
// Middleware did not run.
func SpanFromRequest(c fiber.Ctx) trace.Span {
	return trace.SpanFromContext(c.RequestCtx())
}

// spanKey is the key the trace API stores the current span under. The API
// keeps it unexported, so it is captured by watching which key
// trace.SpanFromContext asks a context for.
var spanKey = func() any {
	var spy keySpy
	trace.SpanFromContext(&spy)
	return spy.key
}()

type keySpy struct {
	context.Context
	key any
}

func (s *keySpy) Value(key any) any {
	s.key = key
	return nil
}

// headerCarrier reads trace context from fasthttp request headers.
type headerCarrier struct{ h *fasthttp.RequestHeader }

func (c headerCarrier) Get(key string) string { return string(c.h.Peek(key)) }

func (c headerCarrier) Set(key, value string) { c.h.Set(key, value) }

func (c headerCarrier) Keys() []string {
	var keys []string
	for k := range c.h.All() {
		keys = append(keys, string(k))
	}
	return keys
}
