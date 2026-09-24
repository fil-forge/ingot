package tracing

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func installRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return rec
}

// The backend receives the bare *fasthttp.RequestCtx as its context; a span
// started from it must be a child of the request's server span.
func TestMiddlewareParentsSpansStartedFromRequestCtx(t *testing.T) {
	rec := installRecorder(t)
	app := fiber.New()
	app.Use(Middleware())
	app.Get("/", func(c fiber.Ctx) error {
		var ctx context.Context = c.RequestCtx()
		_, child := Tracer().Start(ctx, "child")
		child.End()
		SpanFromRequest(c).SetName("GetObject")
		return c.SendStatus(http.StatusOK)
	})

	res, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	spans := rec.Ended()
	require.Len(t, spans, 2)
	child, server := spans[0], spans[1]
	require.Equal(t, "child", child.Name())
	require.Equal(t, "GetObject", server.Name())
	require.Equal(t, trace.SpanKindServer, server.SpanKind())
	require.Equal(t, server.SpanContext().SpanID(), child.Parent().SpanID())
	require.Equal(t, server.SpanContext().TraceID(), child.SpanContext().TraceID())
}

func TestMiddlewareContinuesIncomingTrace(t *testing.T) {
	rec := installRecorder(t)
	app := fiber.New()
	app.Use(Middleware())
	app.Get("/", func(c fiber.Ctx) error { return c.SendStatus(http.StatusInternalServerError) })

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	_, err := app.Test(req)
	require.NoError(t, err)

	spans := rec.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", spans[0].SpanContext().TraceID().String())
	require.Equal(t, "00f067aa0ba902b7", spans[0].Parent().SpanID().String())
	require.Equal(t, "Error", spans[0].Status().Code.String())
}

// A streamed body is read after the handler returns; the server span must
// stay open for it and carry the reads made while streaming.
func TestMiddlewareSpanCoversStreamedBody(t *testing.T) {
	rec := installRecorder(t)
	app := fiber.New()
	app.Use(Middleware())
	app.Get("/", func(c fiber.Ctx) error {
		var ctx context.Context = c.RequestCtx()
		c.RequestCtx().SetBodyStream(&lazyBody{ctx: ctx, data: []byte("hello")}, 5)
		return nil
	})

	res, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	require.NoError(t, err)
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, "hello", string(body))

	require.Eventually(t, func() bool { return len(rec.Ended()) == 2 }, time.Second, 10*time.Millisecond)
	read, server := rec.Ended()[0], rec.Ended()[1]
	require.Equal(t, "blob.read", read.Name())
	require.Equal(t, server.SpanContext().SpanID(), read.Parent().SpanID())
	require.False(t, server.EndTime().Before(read.EndTime()))
	require.Contains(t, server.Attributes(), attribute.Int64("ingot.reads.blobs.network", 1))
}

// lazyBody reads its blob on first Read, as the object body opener does.
type lazyBody struct {
	ctx    context.Context
	data   []byte
	opened bool
}

func (b *lazyBody) Read(p []byte) (int, error) {
	if !b.opened {
		b.opened = true
		_, span := Start(b.ctx, "blob.read")
		CountRead(b.ctx, BlobNetwork)
		span.End()
	}
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}
