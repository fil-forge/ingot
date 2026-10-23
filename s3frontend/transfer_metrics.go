package s3frontend

import (
	"context"
	"io"
	"sync"

	"github.com/fil-forge/ingot/registry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

// transferMetrics counts object body bytes: those GetObject sends to clients
// (egress) and those PutObject and UploadPart receive from them (ingress).
// Copies stay inside Ingot and count as neither.
type transferMetrics struct {
	egress      metric.Int64Counter
	egressSpace metric.Int64Counter // by space and tenant
	ingress     metric.Int64Counter
}

// newTransferMetrics creates the transfer instruments from mp. A failure to
// create an instrument is logged and leaves that instrument a no-op: metrics
// never stop the backend.
func newTransferMetrics(mp metric.MeterProvider, logger *zap.Logger) transferMetrics {
	meter := mp.Meter(meterName)
	m := transferMetrics{egress: noop.Int64Counter{}, egressSpace: noop.Int64Counter{}, ingress: noop.Int64Counter{}}
	if c, err := meter.Int64Counter("ingot.s3.object_egress_bytes", metric.WithUnit("By"),
		metric.WithDescription("Object body bytes GetObject sent to clients")); err == nil {
		m.egress = c
	} else {
		logger.Warn("transfer metric not created", zap.String("metric", "ingot.s3.object_egress_bytes"), zap.Error(err))
	}
	if c, err := meter.Int64Counter("ingot.s3.bucket_object_egress_bytes", metric.WithUnit("By"),
		metric.WithDescription("Object body bytes GetObject sent to clients, by the bucket's space and tenant DIDs")); err == nil {
		m.egressSpace = c
	} else {
		logger.Warn("transfer metric not created", zap.String("metric", "ingot.s3.bucket_object_egress_bytes"), zap.Error(err))
	}
	if c, err := meter.Int64Counter("ingot.s3.object_ingress_bytes", metric.WithUnit("By"),
		metric.WithDescription("Object body bytes PutObject and UploadPart received from clients, whether or not the write succeeded")); err == nil {
		m.ingress = c
	} else {
		logger.Warn("transfer metric not created", zap.String("metric", "ingot.s3.object_ingress_bytes"), zap.Error(err))
	}
	return m
}

// countEgress wraps a GetObject body so the bytes read from it, which is what
// the response streams to the client, are recorded against st's space and
// tenant when the body is closed. A client that disconnects part way counts
// only what was read.
func (m transferMetrics) countEgress(ctx context.Context, st *registry.State, body io.ReadCloser) io.ReadCloser {
	return &egressBody{ReadCloser: body, ctx: ctx, m: m, attrs: metric.WithAttributes(
		attribute.String("space", st.Space.String()),
		attribute.String("tenant", st.Tenant.String()),
	)}
}

// countIngress wraps a request body so its bytes can be recorded once ingest
// is done; the caller defers the returned record.
func (m transferMetrics) countIngress(ctx context.Context, body io.Reader) (io.Reader, func()) {
	cr := &countingReader{r: body}
	return cr, func() {
		if cr.n > 0 {
			m.ingress.Add(ctx, cr.n)
		}
	}
}

// egressBody counts the bytes read through it and records them once, on the
// first Close.
type egressBody struct {
	io.ReadCloser
	ctx   context.Context
	m     transferMetrics
	attrs metric.AddOption
	n     int64
	once  sync.Once
}

func (e *egressBody) Read(p []byte) (int, error) {
	n, err := e.ReadCloser.Read(p)
	e.n += int64(n)
	return n, err
}

func (e *egressBody) Close() error {
	e.once.Do(func() {
		if e.n > 0 {
			e.m.egress.Add(e.ctx, e.n)
			e.m.egressSpace.Add(e.ctx, e.n, e.attrs)
		}
	})
	return e.ReadCloser.Close()
}
