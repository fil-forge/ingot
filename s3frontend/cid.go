package s3frontend

import (
	"context"
	"encoding/base64"

	"github.com/fil-forge/versitygw/s3response"
	"github.com/valyala/fasthttp"

	"github.com/fil-forge/ingot/blake3tree"
	msbucket "github.com/fil-forge/ingot/bucket"
)

// cidHeader is the response header that carries an object's content
// identifier on GetObject and HeadObject: a CIDv1 with the raw codec over
// the body's BLAKE3 multihash (see bucket.Body.CID). A whole-object reader
// verifies the body against it with any BLAKE3 implementation. It is an
// Ingot extension to the S3 API; S3 SDKs surface it through their raw
// response metadata.
const cidHeader = "x-cid"

// setCIDHeader writes the body's CID to the response when the body records a
// digest and ctx is the request. versitygw hands the backend the bare
// *fasthttp.RequestCtx as its context, and the controller adds its own
// headers to the same response afterwards, so a header set here reaches the
// client. A plain context (a direct caller, a test) gets no header.
func setCIDHeader(ctx context.Context, body msbucket.Body) {
	c, ok := body.CID()
	if !ok {
		return
	}
	rc, ok := ctx.(*fasthttp.RequestCtx)
	if !ok {
		return
	}
	rc.Response.Header.Set(cidHeader, c.String())
}

// blake3Attribute is the Blake3 object attribute GetObjectAttributes returns
// when asked for it: the body's CID, the group exponent (the Bao block size)
// and the Bao outboard built from the stored leaves, in base64. A client
// loads the three into a Bao library and verifies any block-aligned range
// of the body. Nil for a body written before the digest was recorded, which
// omits the element.
func blake3Attribute(body msbucket.Body) *s3response.Blake3Tree {
	c, ok := body.CID()
	if !ok {
		return nil
	}
	leaves, err := body.TreeLeafCVs()
	if err != nil {
		return nil
	}
	return &s3response.Blake3Tree{
		CID:      c.String(),
		Group:    body.TreeGroup,
		Outboard: base64.StdEncoding.EncodeToString(blake3tree.Outboard(leaves, body.Size)),
	}
}
