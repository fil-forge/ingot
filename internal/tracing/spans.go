package tracing

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/fil-forge/versitygw/s3err"
	"github.com/valyala/fasthttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Start starts a span on ingot's tracer as a child of the span in ctx, or as
// the root of a new trace when ctx carries none (background work).
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, trace.WithAttributes(attrs...))
}

// StartClient is Start for a call to another service made without the
// instrumented HTTP client.
func StartClient(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
}

// End records err on span and ends it. An [s3err.S3Error] (NoSuchKey and the
// rest of the S3 error table) is an outcome the response carries, so it does
// not mark the span as failed.
func End(span trace.Span, err error) {
	var s3e s3err.S3Error
	if err != nil && !errors.As(err, &s3e) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// ReadSource is the tier a read was served from.
type ReadSource int

const (
	// BlockSpool is a block read from the local blob spool.
	BlockSpool ReadSource = iota
	// BlockLog is a catalog block read from the local log.
	BlockLog
	// BlockCache is a block read from the in-memory block cache.
	BlockCache
	// BlockNetwork is a block fetched from Forge.
	BlockNetwork
	// BlobSpool is a body blob streamed from the local spool.
	BlobSpool
	// BlobNetwork is a body blob streamed from Forge.
	BlobNetwork
	numReadSources
)

var readSourceAttrs = [numReadSources]string{
	BlockSpool:   "ingot.reads.blocks.spool",
	BlockLog:     "ingot.reads.blocks.log",
	BlockCache:   "ingot.reads.blocks.cache",
	BlockNetwork: "ingot.reads.blocks.network",
	BlobSpool:    "ingot.reads.blobs.spool",
	BlobNetwork:  "ingot.reads.blobs.network",
}

// readCounts tallies one request's reads by source. Reads within a request
// can run concurrently, so the counters are atomic.
type readCounts [numReadSources]atomic.Int64

type readCountsKey struct{}

// CountRead records that a read in ctx's request was served from src. The
// counts land on the request's server span as ingot.reads.* attributes. A
// context outside any request (background work) counts nothing.
func CountRead(ctx context.Context, src ReadSource) {
	if rc, ok := ctx.Value(readCountsKey{}).(*readCounts); ok {
		rc[src].Add(1)
	}
}

func attachReadCounts(rc *fasthttp.RequestCtx) *readCounts {
	counts := new(readCounts)
	rc.SetUserValue(readCountsKey{}, counts)
	return counts
}

func (c *readCounts) attributes() []attribute.KeyValue {
	var attrs []attribute.KeyValue
	for src := range numReadSources {
		if n := c[src].Load(); n > 0 {
			attrs = append(attrs, attribute.Int64(readSourceAttrs[src], n))
		}
	}
	return attrs
}
