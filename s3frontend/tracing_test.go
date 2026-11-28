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
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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
	require.Len(s.t, found, 1, "%q spans", name)
	return found[0]
}

// requireChild fails unless child's parent is parent.
func (s *spanTree) requireChild(parent, child sdktrace.ReadOnlySpan) {
	s.t.Helper()
	require.Equal(s.t, parent.SpanContext().SpanID(), child.Parent().SpanID())
}

// requireInTrace fails unless sp belongs to the root span's trace.
func (s *spanTree) requireInTrace(sp sdktrace.ReadOnlySpan) {
	s.t.Helper()
	require.Equal(s.t, s.root.TraceID(), sp.SpanContext().TraceID(), "expected %q in the request's trace", sp.Name())
}

func requireAttr(t *testing.T, sp sdktrace.ReadOnlySpan, want attribute.KeyValue) {
	t.Helper()
	require.Contains(t, sp.Attributes(), want, "%q attributes", sp.Name())
}

func TestPutObjectSpans(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	data := testBody(4 << 10)
	bucket, key := "bk", "k1"

	tree := traceOp(t, func(ctx context.Context) {
		_, err := b.PutObject(ctx, s3response.PutObjectInput{
			Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data),
		})
		require.NoError(t, err, "PutObject")
	})

	write := tree.one("body.write")
	require.Equal(t, tree.root.SpanID(), write.Parent().SpanID(), "expected body.write under the request span")
	requireAttr(t, write, attribute.Int64("ingot.body.bytes", int64(len(data))))
	requireAttr(t, write, attribute.Int("ingot.body.blobs", 1))
	ev := write.Events()
	require.Len(t, ev, 1, "expected a body.received event on body.write")
	require.Equal(t, "body.received", ev[0].Name, "expected a body.received event on body.write")

	requireAttr(t, tree.one("blob.upload"), attribute.String("ingot.blob.result", "streamed"))

	commit := tree.one("bucket.commit")
	appended := tree.one("bucket.append")
	tree.requireChild(commit, appended)
	tree.requireChild(appended, tree.one("log.append"))
}

func TestReadSpans(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	for _, k := range []string{"a", "b", "c"} {
		putObj(t, b, k, testBody(1<<10))
	}
	bucket, key := "bk", "b"

	get := traceOp(t, func(ctx context.Context) {
		out, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
		require.NoError(t, err, "GetObject")
		_, _ = io.Copy(io.Discard, out.Body)
		_ = out.Body.Close()
	})
	get.one("tree.lookup")

	missing := "nope"
	miss := traceOp(t, func(ctx context.Context) {
		_, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &missing})
		require.Error(t, err, "expected NoSuchKey")
	})
	code := miss.one("tree.lookup").Status().Code.String()
	require.Equal(t, "Unset", code, "expected NoSuchKey to leave tree.lookup unmarked")

	list := traceOp(t, func(ctx context.Context) {
		maxKeys := int32(2)
		_, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, MaxKeys: &maxKeys})
		require.NoError(t, err, "ListObjectsV2")
	})
	walk := list.one("tree.list")
	requireAttr(t, walk, attribute.Int("ingot.tree.keys_returned", 2))
	requireAttr(t, walk, attribute.Int("ingot.tree.keys_scanned", 3))

	// A delimiter rolls d/1 and d/2 into one common prefix, which is not a
	// version.
	putObj(t, b, "d/1", testBody(1<<10))
	putObj(t, b, "d/2", testBody(1<<10))
	versions := traceOp(t, func(ctx context.Context) {
		delim := "/"
		_, err := b.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: &bucket, Delimiter: &delim})
		require.NoError(t, err, "ListObjectVersions")
	})
	requireAttr(t, versions.one("tree.list_versions"), attribute.Int("ingot.tree.versions_returned", 3))
}

func TestMultipartSpans(t *testing.T) {
	b, _, _ := newBatchRecordingBackend(t)
	key := "mp"
	uploadID := mpCreate(t, b, key, "", "")

	bucket := "bk"
	var completed []types.CompletedPart
	parts := traceOp(t, func(ctx context.Context) {
		for i := int32(1); i <= 2; i++ {
			n := i
			body := append(testBody(int(backend.MinPartSize)), byte(i))
			out, err := b.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &n, Body: bytes.NewReader(body), ContentLength: byteLen(body),
			})
			require.NoError(t, err, "UploadPart %d", n)
			completed = append(completed, types.CompletedPart{PartNumber: &n, ETag: out.ETag})
		}
	})
	parked := 0
	for _, sp := range parts.spans {
		if sp.Name() == "blob.park" {
			parts.requireInTrace(sp)
			requireAttr(t, sp, attribute.String("ingot.blob.result", "streamed"))
			parked++
		}
	}
	require.Equal(t, 2, parked, "expected a blob.park span per part")

	complete := traceOp(t, func(ctx context.Context) {
		_, _, err := b.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: &bucket, Key: &key, UploadId: &uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
		})
		require.NoError(t, err, "CompleteMultipartUpload")
	})
	// Each part's blob is concluded by its own accept, on the queue's
	// workers rather than under the request.
	concludes := 0
	for _, sp := range complete.spans {
		if sp.Name() == "blobs.conclude" {
			requireAttr(t, sp, attribute.Int("ingot.blobs.total", 1))
			requireAttr(t, sp, attribute.Int("ingot.blobs.concluded", 1))
			concludes++
		}
	}
	require.Equal(t, 2, concludes, "expected a blobs.conclude span per part blob")
}

// TestSweepLocalBlobsSpans: each pass of a local blob sweep records a span
// under the caller's, carrying what it removed and why it stopped.
func TestSweepLocalBlobsSpans(t *testing.T) {
	b, _ := newSweepBackend(t)
	digests := putSweepObjects(t, b, 4)
	size := blobSize(t, b, digests[0])
	budgetToEvict(b, 2, size)

	tree := traceOp(t, func(ctx context.Context) {
		_, err := b.SweepLocalBlobs(ctx)
		require.NoError(t, err, "SweepLocalBlobs")
	})

	budget := tree.one("local_blobs.budget_pass")
	tree.requireInTrace(budget)
	requireAttr(t, budget, attribute.Int64("ingot.sweep.files", 2))
	requireAttr(t, budget, attribute.Int64("ingot.sweep.bytes", 2*size))
	requireAttr(t, budget, attribute.String("ingot.sweep.stop", "watermark"))
	tree.requireInTrace(tree.one("local_blobs.orphan_pass"))
}

// TestSweepLocalBlobsSpansOnCancel: a sweep whose context is cancelled (the
// daemon stopping) does not mark its pass spans failed.
func TestSweepLocalBlobsSpansOnCancel(t *testing.T) {
	b, _ := newSweepBackend(t)
	digests := putSweepObjects(t, b, 4)
	budgetToEvict(b, 2, blobSize(t, b, digests[0]))

	tree := traceOp(t, func(ctx context.Context) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		_, _ = b.SweepLocalBlobs(ctx)
	})

	for _, sp := range tree.spans {
		require.NotEqual(t, codes.Error, sp.Status().Code, "span %q marked failed on cancellation: %s", sp.Name(), sp.Status().Description)
	}
	tree.one("local_blobs.orphan_pass")
}
