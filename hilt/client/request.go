package client

import (
	s3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/valyala/fasthttp"
)

// RequestFromHTTP captures an in-flight fasthttp request as the [s3.Request]
// Hilt's /s3/* commands take. Two fidelity rules matter, because Hilt
// re-derives the SigV4 canonical request from what we send:
//
//   - The URL is the raw request-line URI (path + query) exactly as the
//     client signed it — read from the header, which versitygw's DecodeURL
//     middleware does not touch when it path-unescapes the parsed URI.
//   - Every header is copied out (including Host), both because Hilt needs
//     the signed set and because fasthttp pools request contexts: the
//     returned value must stay valid after the request completes.
func RequestFromHTTP(rc *fasthttp.RequestCtx) s3.Request {
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
