// Package usage counts what the gateway does that only it can know and
// reports it to the auth service, which serves it to tenants as their usage.
//
// Today that is egress: the bytes of object data GetObject sends to clients,
// by bucket space and by hour. A [Counter] takes the bytes as responses send
// them; a [Service] moves the counts to Postgres every few seconds and reports
// the ones that have changed to the auth service every few minutes.
package usage

import (
	"io"
	"sync"
	"time"

	"github.com/fil-forge/ucantone/did"

	"github.com/fil-forge/ingot/registry"
)

// Counter accumulates egress in memory, by space and hour, until a [Service]
// takes it. A nil Counter counts nothing, so a host that reports no usage
// passes none.
type Counter struct {
	mu     sync.Mutex
	counts map[countKey]int64
	now    func() time.Time
}

type countKey struct {
	space did.DID
	hour  int64
}

// NewCounter returns an empty counter.
func NewCounter() *Counter {
	return &Counter{counts: map[countKey]int64{}, now: time.Now}
}

// Egress wraps a GetObject response body so the bytes read from it, which is
// what the response sends to the client, count as the space's egress. Each
// read counts toward the hour it happens in, so a response that runs across an
// hour boundary adds to both hours, and one the client abandons counts only
// what was read: at most a buffer more than reached the client.
func (c *Counter) Egress(space did.DID, body io.ReadCloser) io.ReadCloser {
	if c == nil {
		return body
	}
	return &egressBody{ReadCloser: body, counter: c, space: space}
}

// add counts n bytes sent from space now.
func (c *Counter) add(space did.DID, n int64) {
	if n <= 0 {
		return
	}
	k := countKey{space: space, hour: c.now().UTC().Truncate(time.Hour).Unix()}
	c.mu.Lock()
	c.counts[k] += n
	c.mu.Unlock()
}

// take returns what has been counted since the last take, and forgets it.
func (c *Counter) take() []registry.EgressCount {
	c.mu.Lock()
	counts := c.counts
	c.counts = make(map[countKey]int64, len(counts))
	c.mu.Unlock()

	out := make([]registry.EgressCount, 0, len(counts))
	for k, n := range counts {
		out = append(out, registry.EgressCount{Space: k.space, Hour: time.Unix(k.hour, 0).UTC(), Bytes: n})
	}
	return out
}

// egressBody counts the bytes read through it as they are read.
type egressBody struct {
	io.ReadCloser
	counter *Counter
	space   did.DID
}

func (e *egressBody) Read(p []byte) (int, error) {
	n, err := e.ReadCloser.Read(p)
	e.counter.add(e.space, int64(n))
	return n, err
}
