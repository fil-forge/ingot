// Package revocation subscribes to the revocation service's (Swarf's) SSE
// revocation firehose and applies incoming UCAN revocations to ingot's local
// authorization caches, so that an access key Hilt has deleted (whose
// delegations Hilt revokes) stops being authorized from cache — its next
// request falls through to Hilt, which refuses.
//
// The consumer maintains a persistent resume cursor (registry's
// revocation_cursor row): the service's opaque cursor for the last processed
// record, which a reconnect or restart resumes after, missing nothing. A
// background saver writes it at most once per save interval and once more on
// stop, so applying a record never waits on the database; records applied
// since the last save are re-delivered after a crash, which is harmless.
//
// With no cursor stored, the consumer reads the head of the firehose from the
// service, as the service's database sees it rather than on ingot's clock,
// and saves it before it streams: every cache a revocation could clear is
// process memory, so nothing revoked before the process first subscribed can
// be cached, and a restart before the first record resumes from the saved
// head rather than from a later one, which would skip the revocations
// recorded in between. While the service doesn't answer, the consumer
// retries; a revocation recorded before its first answer is not delivered,
// which matters only to a cache filled in that window.
//
// Moving the service's database to another server can void its cursors
// (Swarf's README, "Moving the database"). The service refuses those it can
// tell, which the consumer logs as an error on every retry. To start the
// consumer over, stop ingot, delete the ingot.revocation_cursor row, and
// start it again: it starts at the firehose's head.
//
// The gateway keeps serving while the service is unreachable (reconnect with
// capped backoff); in the worst case cache entries age out on their own TTLs,
// exactly as they would with no consumer at all.
package revocation

import (
	"context"
	"errors"
	"iter"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v5"
	swarfclient "github.com/fil-forge/swarf/pkg/client"
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/registry"
)

// Source is the slice of the swarf client the consumer reads
// (swarf/pkg/client.Client satisfies it): the firehose's head, and the
// firehose after a cursor, each record with its own cursor.
type Source interface {
	Head(ctx context.Context) (swarfclient.Cursor, error)
	StreamAfter(ctx context.Context, after swarfclient.Cursor) iter.Seq2[swarfclient.Event, error]
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

// defaultSaveInterval bounds how much a crash re-delivers: at most this much
// of the firehose, plus records applied since the last save.
const defaultSaveInterval = time.Second

// finalSaveTimeout bounds the save made after ctx is canceled, which can no
// longer use ctx itself.
const finalSaveTimeout = 5 * time.Second

// Consumer runs the subscribe → invalidate → persist-cursor loop.
type Consumer struct {
	src    Source
	cursor registry.RevocationCursorStore
	inv    Invalidator
	logger *zap.Logger

	minBackoff   time.Duration
	maxBackoff   time.Duration
	saveInterval time.Duration
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

// WithSaveInterval overrides how often the resume cursor is saved (default 1s).
func WithSaveInterval(d time.Duration) Option {
	return func(c *Consumer) {
		c.saveInterval = d
	}
}

// NewConsumer returns a Consumer reading revocations from src, clearing
// caches through inv, and persisting its resume point in cursor.
func NewConsumer(src Source, cursor registry.RevocationCursorStore, inv Invalidator, opts ...Option) *Consumer {
	c := &Consumer{
		src:          src,
		cursor:       cursor,
		inv:          inv,
		logger:       zap.NewNop(),
		minBackoff:   defaultMinBackoff,
		maxBackoff:   defaultMaxBackoff,
		saveInterval: defaultSaveInterval,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run streams revocations until ctx is canceled, reconnecting with capped
// exponential backoff, while a background saver persists the resume cursor
// (see saveCursor). It only ever returns on ctx cancellation, after the
// saver's final save once streaming has begun: the firehose is
// availability-tolerant — records missed while disconnected are delivered on
// reconnect after the last processed record, and duplicates are harmless
// (Invalidator is idempotent).
func (c *Consumer) Run(ctx context.Context) {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = c.minBackoff
	bo.MaxInterval = c.maxBackoff

	position, ok := c.startPosition(ctx, bo)
	if !ok {
		return
	}
	var latest atomic.Pointer[registry.RevocationCursor]
	saved := make(chan struct{})
	go c.saveCursor(ctx, &latest, saved)
	defer func() { <-saved }()

	bo.Reset()
	for {
		for event, err := range c.src.StreamAfter(ctx, swarfclient.Cursor(position.Token)) {
			if errors.Is(err, swarfclient.ErrCursorRefused) {
				c.logger.Error("revocation: cursor refused",
					zap.String("cursor", position.Token),
					zap.String("fix", "with ingot stopped, delete the ingot.revocation_cursor row; ingot then starts at the firehose's head"),
					zap.Error(err))
				break
			}
			if err != nil {
				c.logger.Warn("revocation: stream error", zap.Error(err))
				break
			}
			// The stream can hold records it read before ctx was canceled.
			if ctx.Err() != nil {
				break
			}
			// A live stream means the endpoint is healthy — reset backoff.
			bo.Reset()
			rec := event.Revocation
			keys := c.inv.Revoke(rec.Revoke)
			c.logger.Info("revocation: record processed",
				zap.Stringer("revoke", rec.Revoke),
				zap.Stringer("cause", rec.Cause),
				zap.String("cursor", string(event.Cursor)),
				zap.Int("keys_invalidated", len(keys)))
			cur := registry.RevocationCursor{
				Token:      string(event.Cursor),
				RecordedAt: rec.RecordedAt.Time(),
				Revoke:     rec.Revoke,
			}
			position = cur
			// Published only after Revoke returns, so the saver never saves
			// past a record that has not been applied.
			latest.Store(&cur)
		}
		if ctx.Err() != nil {
			return
		}
		// The stream ended: wait, then reconnect after the last processed
		// record.
		if !sleepCtx(ctx, bo.NextBackOff()) {
			return
		}
	}
}

// startPosition resolves where the firehose starts: after the stored cursor,
// or, with none stored, at the firehose's head, which it saves first (see the
// package doc). A cursor that cannot be read is not overwritten: the consumer
// starts at the head without saving it, rather than replacing a position it
// could not see. It reads the head with backoff until the service answers,
// and reports false only when ctx is canceled first.
func (c *Consumer) startPosition(ctx context.Context, bo *backoff.ExponentialBackOff) (registry.RevocationCursor, bool) {
	stored, err := c.cursor.GetRevocationCursor(ctx)
	if err == nil {
		return *stored, true
	}
	save := errors.Is(err, registry.ErrNotFound)
	if !save {
		c.logger.Warn("revocation: load cursor, starting at the firehose head without saving it", zap.Error(err))
	}
	for {
		head, err := c.src.Head(ctx)
		if err == nil {
			position := registry.RevocationCursor{Token: string(head)}
			if save {
				if err := c.cursor.PutRevocationCursor(ctx, position); err != nil {
					c.logger.Warn("revocation: persist cursor", zap.Error(err))
				}
			}
			return position, true
		}
		if ctx.Err() != nil {
			return registry.RevocationCursor{}, false
		}
		c.logger.Warn("revocation: read firehose head", zap.Error(err))
		if !sleepCtx(ctx, bo.NextBackOff()) {
			return registry.RevocationCursor{}, false
		}
	}
}

// saveCursor saves the newest position in latest every save interval,
// skipping intervals in which it has not moved, and once more when ctx is
// canceled, then closes done. Saving off the record loop matters on a busy
// firehose: each save is a durable commit, and on a node whose database sits
// on slow disks one commit per record capped the loop at about 55 records a
// second. A failed save is retried at the next interval; until one succeeds,
// a restart re-delivers from the previous saved cursor.
func (c *Consumer) saveCursor(ctx context.Context, latest *atomic.Pointer[registry.RevocationCursor], done chan<- struct{}) {
	defer close(done)
	var last *registry.RevocationCursor
	save := func(ctx context.Context) {
		cur := latest.Load()
		if cur == nil || cur == last {
			return
		}
		if err := c.cursor.PutRevocationCursor(ctx, *cur); err != nil {
			c.logger.Warn("revocation: persist cursor", zap.Error(err))
			return
		}
		last = cur
	}

	ticker := time.NewTicker(c.saveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			save(ctx)
		case <-ctx.Done():
			final, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalSaveTimeout)
			save(final)
			cancel()
			return
		}
	}
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
