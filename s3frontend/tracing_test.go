package s3frontend

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3response"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// spanTree records the spans one operation produces under a root span.
type spanTree struct {
	t     *testing.T
	root  trace.SpanContext
	spans []sdktrace.ReadOnlySpan
}

// traceOp runs op under a root span with a recording tracer provider and
// returns the spans it produced.
func traceOp(t *testing.T, op func(ctx context.Context)) *spanTree {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	ctx, root := otel.Tracer("test").Start(context.Background(), "request")
	op(ctx)
	root.End()
	return &spanTree{t: t, root: root.SpanContext(), spans: rec.Ended()}
}

// one returns the single span named name.
func (s *spanTree) one(name string) sdktrace.ReadOnlySpan {
	s.t.Helper()
	var found []sdktrace.ReadOnlySpan
	for _, sp := range s.spans {
		if sp.Name() == name {
			found = append(found, sp)
		}
	}
	if len(found) != 1 {
		s.t.Fatalf("expected one %q span, got %d", name, len(found))
	}
	return found[0]
}

// requireChild fails unless child's parent is parent.
func (s *spanTree) requireChild(parent, child sdktrace.ReadOnlySpan) {
	s.t.Helper()
	if child.Parent().SpanID() != parent.SpanContext().SpanID() {
		s.t.Fatalf("expected %q under %q", child.Name(), parent.Name())
	}
}

func requireAttr(t *testing.T, sp sdktrace.ReadOnlySpan, want attribute.KeyValue) {
	t.Helper()
	for _, kv := range sp.Attributes() {
		if kv == want {
			return
		}
	}
	t.Fatalf("expected %q to carry %v, got %v", sp.Name(), want, sp.Attributes())
}

func TestPutObjectSpans(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	data := testBody(4 << 10)
	bucket, key := "bk", "k1"

	tree := traceOp(t, func(ctx context.Context) {
		if _, err := b.PutObject(ctx, s3response.PutObjectInput{
			Bucket: &bucket, Key: &key, Body: bytes.NewReader(data),
		}); err != nil {
			t.Fatalf("PutObject: %v", err)
		}
	})

	spool := tree.one("body.spool")
	if spool.Parent().SpanID() != tree.root.SpanID() {
		t.Fatalf("expected body.spool under the request span")
	}
	requireAttr(t, spool, attribute.Int64("ingot.body.bytes", int64(len(data))))
	requireAttr(t, spool, attribute.Int("ingot.body.blobs", 1))
	if ev := spool.Events(); len(ev) != 1 || ev[0].Name != "body.received" {
		t.Fatalf("expected a body.received event on body.spool, got %v", ev)
	}

	requireAttr(t, tree.one("blob.upload"), attribute.String("ingot.blob.result", "uploaded"))

	tx := tree.one("bucket.tx")
	tree.requireChild(tx, tree.one("bucket.lock"))
	commit := tree.one("bucket.commit")
	tree.requireChild(tx, commit)
	tree.requireChild(commit, tree.one("log.append"))
}

func TestReadSpans(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	for _, k := range []string{"a", "b", "c"} {
		putObj(t, b, k, testBody(1<<10))
	}
	bucket, key := "bk", "b"

	get := traceOp(t, func(ctx context.Context) {
		out, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
		if err != nil {
			t.Fatalf("GetObject: %v", err)
		}
		_, _ = io.Copy(io.Discard, out.Body)
		_ = out.Body.Close()
	})
	get.one("tree.lookup")

	missing := "nope"
	miss := traceOp(t, func(ctx context.Context) {
		if _, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &missing}); err == nil {
			t.Fatal("expected NoSuchKey")
		}
	})
	if code := miss.one("tree.lookup").Status().Code.String(); code != "Unset" {
		t.Fatalf("expected NoSuchKey to leave tree.lookup unmarked, got %s", code)
	}

	list := traceOp(t, func(ctx context.Context) {
		maxKeys := int32(2)
		if _, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, MaxKeys: &maxKeys}); err != nil {
			t.Fatalf("ListObjectsV2: %v", err)
		}
	})
	walk := list.one("tree.list")
	requireAttr(t, walk, attribute.Int("ingot.tree.keys_returned", 2))
	requireAttr(t, walk, attribute.Int("ingot.tree.keys_scanned", 3))
}

func TestMultipartSpans(t *testing.T) {
	b, _, _ := newBatchRecordingBackend(t)
	key := "mp"
	uploadID := mpCreate(t, b, key, "", "")

	var completed []types.CompletedPart
	parts := traceOp(t, func(context.Context) {
		for i := int32(1); i <= 2; i++ {
			n := i
			body := append(testBody(int(backend.MinPartSize)), byte(i))
			out, err := mpUploadPart(t, b, key, uploadID, n, body, nil)
			if err != nil {
				t.Fatalf("UploadPart %d: %v", n, err)
			}
			completed = append(completed, types.CompletedPart{PartNumber: &n, ETag: out.ETag})
		}
	})
	parked := 0
	for _, sp := range parts.spans {
		if sp.Name() == "blob.park" {
			requireAttr(t, sp, attribute.String("ingot.blob.result", "parked"))
			parked++
		}
	}
	if parked != 2 {
		t.Fatalf("expected a blob.park span per part, got %d", parked)
	}

	complete := traceOp(t, func(context.Context) {
		if _, err := mpComplete(t, b, key, uploadID, completed, nil); err != nil {
			t.Fatalf("CompleteMultipartUpload: %v", err)
		}
	})
	conclude := complete.one("blobs.conclude")
	requireAttr(t, conclude, attribute.Int("ingot.blobs.total", 2))
	requireAttr(t, conclude, attribute.Int("ingot.blobs.concluded", 2))
	requireAttr(t, conclude, attribute.Int("ingot.blobs.uploaded", 0))
}
