package s3frontend

import (
	"io"

	"go.opentelemetry.io/otel/trace"
)

// receivedReader adds a body.received event to span when the request body
// reaches EOF. Bodies stream in as they are read, so the event marks where the
// client finished sending; the span time after it is ingot's own.
type receivedReader struct {
	r    io.Reader
	span trace.Span
	done bool
}

func (r *receivedReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err == io.EOF && !r.done {
		r.done = true
		r.span.AddEvent("body.received")
	}
	return n, err
}
