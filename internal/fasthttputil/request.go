package fasthttputil

import (
	"context"

	"github.com/fil-forge/libforge/commands/s3"
	"github.com/valyala/fasthttp"
)

// RequestFromHTTPContext captures an in-flight fasthttp request as the
// [s3.Request] Hilt's /s3/* commands take. Two fidelity rules matter, because
// Hilt re-derives the SigV4 canonical request from what we send:
//
//   - The URL is the raw request-line URI (path + query) exactly as the
//     client signed it — read from the header, which versitygw's DecodeURL
//     middleware does not touch when it path-unescapes the parsed URI.
//   - Every header is copied out (including Host), both because Hilt needs
//     the signed set and because fasthttp pools request contexts: the
//     returned value must stay valid after the request completes.
func RequestFromHTTPContext(rc *fasthttp.RequestCtx) s3.Request {
	headers := make(map[string]string, rc.Request.Header.Len())
	for k, v := range rc.Request.Header.All() {
		headers[string(k)] = string(v)
	}
	return s3.Request{
		Method:  string(rc.Method()),
		URL:     string(rc.Request.Header.RequestURI()),
		Headers: headers,
	}
}

// RequestFromContext captures an in-flight request from a context.Context
// (which must be a *fasthttp.RequestCtx) as the [s3.Request] Hilt's /s3/*
// commands take.
func RequestFromContext(ctx context.Context) (s3.Request, bool) {
	rc, ok := ctx.(*fasthttp.RequestCtx)
	if !ok {
		return s3.Request{}, false
	}
	return RequestFromHTTPContext(rc), true
}
