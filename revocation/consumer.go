// Package revocation subscribes to the revocation service's (Swarf's) SSE
// firehose and applies its records to ingot's local authorization caches, so
// that a cached access key stops being authorized locally the moment Hilt
// changes what it may do — its next request falls through to Hilt, which
// re-authorizes it or refuses.
//
// The firehose carries two kinds of record. A revocation withdraws one
// delegation, which is how Hilt retires a deleted access key. A principal
// invalidation names a (tenant, principal) pair whose access has changed,
// published before Hilt commits the change. Both clear the caches of every
// access key they affect, and both advance the resume cursor.
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
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/registry"
)

// Source is the firehose the consumer reads: the event stream from a `since`
// cursor. A zero since streams every stored record. NewSwarfSource adapts
// the swarf client to it.
type Source interface {
	Stream(ctx context.Context, from time.Time) iter.Seq2[Event, error]
}

// Invalidator applies one firehose event to local authorization state
// (iam.Revoker satisfies it), returning the access keys whose caches it
// cleared. Both methods must be idempotent: reconnects may re-deliver
// records.
type Invalidator interface {
	// Revoke clears every access key holding the revoked delegation.
	Revoke(revoked cid.Cid) []did.DID
	// InvalidatePrincipal clears every access key bound to the pair.
	InvalidatePrincipal(tenant did.DID, principal string) []did.DID
}

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
		for ev, err := range c.src.Stream(ctx, since) {
			if err != nil {
				c.logger.Warn("revocation: stream error", zap.Error(err))
				break
			}
			// A live stream means the endpoint is healthy — reset backoff.
			bo.Reset()
			c.apply(ev)
			since = ev.RecordedAt
			if !ev.Cause.Defined() {
				// Every firehose record names the invocation that caused
				// it; a record without one is a source bug, and advancing
				// the durable cursor past it would bury that.
				c.logger.Warn("revocation: record without a cause, cursor not advanced",
					zap.Time("recorded_at", ev.RecordedAt))
				continue
			}
			// Persist per record: these are human-scale events (key
			// deletions, access changes), so a row upsert each is
			// negligible. If the shared firehose ever becomes high-volume,
			// debounce here — safe, since reprocessing a window after a
			// crash is idempotent. A failed write only costs re-delivery
			// from the previous durable cursor.
			if err := c.cursor.PutRevocationCursor(ctx, registry.RevocationCursor{
				RecordedAt: since,
				Revoke:     ev.Revoke,
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

// apply dispatches one event to the invalidator by kind and logs what it
// cleared.
func (c *Consumer) apply(ev Event) {
	if ev.IsPrincipal() {
		keys := c.inv.InvalidatePrincipal(ev.Tenant, ev.Principal)
		c.logger.Info("revocation: principal invalidation processed",
			zap.Stringer("tenant", ev.Tenant),
			zap.String("principal", ev.Principal),
			zap.Stringer("cause", ev.Cause),
			zap.Int("keys_invalidated", len(keys)))
		return
	}
	keys := c.inv.Revoke(ev.Revoke)
	c.logger.Info("revocation: record processed",
		zap.Stringer("revoke", ev.Revoke),
		zap.Stringer("cause", ev.Cause),
		zap.Int("keys_invalidated", len(keys)))
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
