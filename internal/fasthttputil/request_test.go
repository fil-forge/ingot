package fasthttputil

import (
	"errors"
	"io"
	"strings"
	"testing"

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
	if v := got.Headers["X-Amz-Object-Attributes"]; v != want {
		t.Errorf("X-Amz-Object-Attributes = %q, want %q", v, want)
	}
}

// TestRequestFromHTTPContext_SingleHeader confirms the common single-valued
// case is unchanged.
func TestRequestFromHTTPContext_SingleHeader(t *testing.T) {
	var rc fasthttp.RequestCtx
	rc.Request.Header.SetMethod("PUT")
	rc.Request.Header.SetRequestURI("/bucket/obj")
	rc.Request.Header.Set("Content-Encoding", "gzip")

	got := RequestFromHTTPContext(&rc)

	if v := got.Headers["Content-Encoding"]; v != "gzip" {
		t.Errorf("Content-Encoding = %q, want %q", v, "gzip")
	}
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
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "<Delete/>" {
			t.Errorf("body = %q", got)
		}
		if rc.Request.IsBodyStream() || string(rc.Request.Body()) != "<Delete/>" {
			t.Errorf("handler body = %q, stream=%v", rc.Request.Body(), rc.Request.IsBodyStream())
		}
		got[0] = 'x'
		if string(rc.Request.Body()) != "<Delete/>" {
			t.Error("the returned body must be the caller's own copy")
		}
	})
	t.Run("streamed over the limit", func(t *testing.T) {
		var rc fasthttp.RequestCtx
		rc.Request.Header.SetMethod("POST")
		src := &countingReader{Reader: strings.NewReader(strings.Repeat("x", 100))}
		rc.Request.SetBodyStream(src, -1)
		_, err := LimitedBody(&rc, 16)
		if !errors.Is(err, ErrBodyTooLarge) {
			t.Fatalf("err = %v, want ErrBodyTooLarge", err)
		}
		if src.n > 17 {
			t.Errorf("read %d bytes of a refused body, want at most 17", src.n)
		}
	})
	t.Run("in memory", func(t *testing.T) {
		var rc fasthttp.RequestCtx
		rc.Request.SetBody([]byte("<Delete/>"))
		got, err := LimitedBody(&rc, 16)
		if err != nil || string(got) != "<Delete/>" {
			t.Fatalf("got %q, %v", got, err)
		}
		rc.Request.SetBody([]byte(strings.Repeat("x", 17)))
		if _, err := LimitedBody(&rc, 16); !errors.Is(err, ErrBodyTooLarge) {
			t.Fatalf("err = %v, want ErrBodyTooLarge", err)
		}
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
