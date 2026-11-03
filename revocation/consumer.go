// Package revocation subscribes to the revocation service's (Swarf's) SSE
// revocation firehose and applies incoming UCAN revocations to ingot's local
// authorization caches, so that an access key Hilt has deleted (whose
// delegations Hilt revokes) stops being authorized from cache — its next
// request falls through to Hilt, which refuses.
//
// The consumer maintains a persistent resume cursor (registry's
// revocation_cursor row): the recorded_at of the last processed record, on
// the service's own timeline. On first ever connect — no cursor stored — it
// subscribes from "now": every cache a revocation could clear is process
// memory, so nothing revoked before the process first subscribed can be
// cached. The gateway keeps serving while the service is unreachable
// (reconnect with capped backoff); in the worst case cache entries age out
// on their own TTLs, exactly as they would with no consumer at all.
package revocation

import (
	"context"
	"errors"
	"iter"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/fil-forge/swarf/pkg/api"
	swarfclient "github.com/fil-forge/swarf/pkg/client"
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/registry"
)

// Source is the slice of the swarf client the consumer reads
// (swarf/pkg/client.Client satisfies it): the firehose stream from a `since`
// cursor. A zero since streams every stored record.
type Source interface {
	Stream(ctx context.Context, from time.Time) iter.Seq2[api.FirehoseRevocation, error]
}

// Invalidator applies one revocation to local authorization state
// (iam.Revoker satisfies it), returning the access keys whose caches it
// cleared. Must be idempotent: reconnects may re-deliver records.
type Invalidator interface {
	Revoke(revoked cid.Cid) []did.DID
}

// Compile-time assertion: the real swarf client satisfies Source.
var _ Source = (*swarfclient.Client)(nil)

// Reconnect backoff bounds: the exponential backoff's initial interval and
// its cap (growth and jitter use the backoff package's defaults).
const (
	defaultMinBackoff = time.Second
	defaultMaxBackoff = time.Minute
)

// Consumer runs the subscribe → invalidate → persist-cursor loop.
type Consumer struct {
	src    Source
	cursor registry.RevocationCursorStore
	inv    Invalidator
	logger *zap.Logger

	minBackoff time.Duration
	maxBackoff time.Duration
	now        func() time.Time // clock seam for tests
}

// Option configures a Consumer.
type Option func(*Consumer)

// WithLogger sets the consumer logger (default: no-op).
func WithLogger(logger *zap.Logger) Option {
	return func(c *Consumer) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithBackoff overrides the reconnect backoff's initial interval and cap
// (defaults 1s..1m).
func WithBackoff(min, max time.Duration) Option {
	return func(c *Consumer) {
		c.minBackoff = min
		c.maxBackoff = max
	}
}

// WithClock overrides the wall clock (test seam for the since-defaults-to-now
// behavior).
func WithClock(now func() time.Time) Option {
	return func(c *Consumer) {
		if now != nil {
			c.now = now
		}
	}
}

// NewConsumer returns a Consumer reading revocations from src, clearing
// caches through inv, and persisting its resume point in cursor.
func NewConsumer(src Source, cursor registry.RevocationCursorStore, inv Invalidator, opts ...Option) *Consumer {
	c := &Consumer{
		src:        src,
		cursor:     cursor,
		inv:        inv,
		logger:     zap.NewNop(),
		minBackoff: defaultMinBackoff,
		maxBackoff: defaultMaxBackoff,
		now:        time.Now,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run streams revocations until ctx is canceled, reconnecting with capped
// exponential backoff and persisting the resume cursor as records arrive. It
// only ever returns on ctx cancellation: the firehose is availability-
// tolerant — records missed while disconnected are re-delivered from the
// cursor on reconnect, and duplicates are harmless (Invalidator is
// idempotent).
func (c *Consumer) Run(ctx context.Context) {
	since := c.loadFrom(ctx)
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = c.minBackoff
	bo.MaxInterval = c.maxBackoff
	for {
		for rec, err := range c.src.Stream(ctx, since) {
			if err != nil {
				c.logger.Warn("revocation: stream error", zap.Error(err))
				break
			}
			// A live stream means the endpoint is healthy — reset backoff.
			bo.Reset()
			keys := c.inv.Revoke(rec.Revoke)
			c.logger.Info("revocation: record processed",
				zap.Stringer("revoke", rec.Revoke),
				zap.Stringer("cause", rec.Cause),
				zap.Int("keys_invalidated", len(keys)))
			since = rec.RecordedAt.Time()
			// Persist per record: revocations are human-scale events (key
			// deletions), so a row upsert each is negligible. If the shared
			// firehose ever becomes high-volume, debounce here — safe, since
			// reprocessing a window after a crash is idempotent. A failed
			// write only costs re-delivery from the previous durable cursor.
			if err := c.cursor.PutRevocationCursor(ctx, registry.RevocationCursor{
				RecordedAt: since,
				Revoke:     rec.Revoke,
			}); err != nil && ctx.Err() == nil {
				c.logger.Warn("revocation: persist cursor", zap.Error(err))
			}
		}
		if ctx.Err() != nil {
			return
		}
		// The stream ended (server close yields no error) or errored:
		// wait, then reconnect from the last processed record.
		if !sleepCtx(ctx, bo.NextBackOff()) {
			return
		}
	}
}

// loadFrom resolves the initial firehose cursor: the stored resume point,
// or "now" when none exists. Any read failure (not just ErrNotFound)
// degrades to "now" with a warning rather than blocking startup — the cost
// is missing revocations recorded while disconnected, bounded by the cache
// TTLs the firehose exists to undercut.
func (c *Consumer) loadFrom(ctx context.Context) time.Time {
	cur, err := c.cursor.GetRevocationCursor(ctx)
	if err == nil {
		return cur.RecordedAt
	}
	if !errors.Is(err, registry.ErrNotFound) {
		c.logger.Warn("revocation: load cursor, subscribing from now", zap.Error(err))
	}
	return c.now()
}

// sleepCtx sleeps for d, reporting false if ctx was canceled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
