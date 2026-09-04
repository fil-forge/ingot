package fasthttputil

import (
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
