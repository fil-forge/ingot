package fasthttputil

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// TestRequestFromHTTPContext_RepeatedHeader locks the SigV4 fidelity rule for
// GetObjectAttributes: a header sent on multiple lines (one
// X-Amz-Object-Attributes line per attribute) must be comma-joined in wire
// order, not collapsed to its last value. Truncating it makes the re-derived
// canonical request diverge from what the client signed (SignatureDoesNotMatch).
func TestRequestFromHTTPContext_RepeatedHeader(t *testing.T) {
	var rc fasthttp.RequestCtx
	rc.Request.Header.SetMethod("GET")
	rc.Request.Header.SetRequestURI("/bucket/obj?attributes")
	for _, attr := range []string{"ETag", "Checksum", "ObjectParts", "StorageClass", "ObjectSize"} {
		rc.Request.Header.Add("X-Amz-Object-Attributes", attr)
	}

	got := RequestFromHTTPContext(&rc)

	const want = "ETag,Checksum,ObjectParts,StorageClass,ObjectSize"
	v := got.Headers["X-Amz-Object-Attributes"]
	assert.Equal(t, want, v, "X-Amz-Object-Attributes")
}

// TestRequestFromHTTPContext_SingleHeader confirms the common single-valued
// case is unchanged.
func TestRequestFromHTTPContext_SingleHeader(t *testing.T) {
	var rc fasthttp.RequestCtx
	rc.Request.Header.SetMethod("PUT")
	rc.Request.Header.SetRequestURI("/bucket/obj")
	rc.Request.Header.Set("Content-Encoding", "gzip")

	got := RequestFromHTTPContext(&rc)

	v := got.Headers["Content-Encoding"]
	assert.Equal(t, "gzip", v, "Content-Encoding")
}

// TestLimitedBody covers the capture of a body hilt decides on: a streamed
// body within the limit is returned and left for the handler, one over it is
// refused after reading one byte past the limit, and an in-memory body is
// bounded the same way.
func TestLimitedBody(t *testing.T) {
	t.Run("streamed within the limit", func(t *testing.T) {
		var rc fasthttp.RequestCtx
		rc.Request.Header.SetMethod("POST")
		rc.Request.SetBodyStream(strings.NewReader("<Delete/>"), -1)
		got, err := LimitedBody(&rc, 16)
		require.NoError(t, err)
		assert.Equal(t, "<Delete/>", string(got), "body")
		assert.False(t, rc.Request.IsBodyStream(), "handler body is still a stream")
		assert.Equal(t, "<Delete/>", string(rc.Request.Body()), "handler body")
		got[0] = 'x'
		assert.Equal(t, "<Delete/>", string(rc.Request.Body()), "the returned body must be the caller's own copy")
	})
	t.Run("streamed over the limit", func(t *testing.T) {
		var rc fasthttp.RequestCtx
		rc.Request.Header.SetMethod("POST")
		src := &countingReader{Reader: strings.NewReader(strings.Repeat("x", 100))}
		rc.Request.SetBodyStream(src, -1)
		_, err := LimitedBody(&rc, 16)
		require.ErrorIs(t, err, ErrBodyTooLarge, "err")
		assert.LessOrEqual(t, src.n, 17, "read %d bytes of a refused body", src.n)
	})
	t.Run("in memory", func(t *testing.T) {
		var rc fasthttp.RequestCtx
		rc.Request.SetBody([]byte("<Delete/>"))
		got, err := LimitedBody(&rc, 16)
		require.NoError(t, err)
		require.Equal(t, "<Delete/>", string(got))
		rc.Request.SetBody([]byte(strings.Repeat("x", 17)))
		_, err = LimitedBody(&rc, 16)
		require.ErrorIs(t, err, ErrBodyTooLarge)
	})
}

type countingReader struct {
	io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.Reader.Read(p)
	c.n += n
	return n, err
}
