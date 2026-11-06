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
// which matters only to a cache filled in that window. A stored cursor the
// consumer can't read is retried as well, never replaced by the head.
//
// Restoring the service's database from a backup older than the consumer's
// cursor (Swarf's README, "Moving the database"), or restarting a service
// that keeps its records in memory, leaves the cursor ahead of the firehose's
// head, and the service refuses it. The consumer logs an error, reads the
// head, saves it and streams after it. Revocations recorded since the restore
// and before that head are never delivered, and nothing says which keys they
// named, so a key one of them named keeps working from cache until its cached
// authorization expires. Retrying the refused cursor instead would skip more:
// the service accepts it once new records pass it, skipping every record in
// between.
//
// If new records pass the cursor before the consumer reconnects, the service
// accepts it and nothing reports the gap. So after restoring the service's
// database from a backup, or restarting a service that keeps its records in
// memory, restart ingot once the service is serving again, and after a refused
// cursor too. The restart empties every cache, and any record the consumer
// then skips was recorded before it, so none can matter.
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

	var latest atomic.Pointer[registry.RevocationCursor]
	position, ok := c.startPosition(ctx, bo, &latest)
	if !ok {
		return
	}
	saved := make(chan struct{})
	go c.saveCursor(ctx, &latest, saved)
	defer func() { <-saved }()

	bo.Reset()
	for {
		refused := false
		for event, err := range c.src.StreamAfter(ctx, swarfclient.Cursor(position.Token)) {
			if errors.Is(err, swarfclient.ErrCursorRefused) {
				c.logger.Error("revocation: cursor refused",
					zap.String("cursor", position.Token),
					zap.String("action", "starting over at the firehose head; restart ingot to drop authorization cached for revocations recorded since the service's database was restored"),
					zap.Error(err))
				refused = true
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
		if refused {
			head, ok := c.readHead(ctx, bo)
			if !ok {
				return
			}
			restart := registry.RevocationCursor{Token: string(head)}
			position = restart
			// The saver persists it, so a restart doesn't return to the
			// refused cursor.
			latest.Store(&restart)
		}
		// The stream ended: wait, then reconnect after the last processed
		// record, or after the head when the service refused the cursor.
		if !sleepCtx(ctx, bo.NextBackOff()) {
			return
		}
	}
}

// startPosition resolves where the firehose starts: after the stored cursor,
// or, with none stored, at the firehose's head, which it saves first (see the
// package doc). It reads the cursor, and then the head, with backoff until
// each answers; a cursor it can't read is never replaced by the head. A head
// it fails to save goes to latest, for the saver to retry. It reports false
// only when ctx is canceled first.
func (c *Consumer) startPosition(ctx context.Context, bo *backoff.ExponentialBackOff, latest *atomic.Pointer[registry.RevocationCursor]) (registry.RevocationCursor, bool) {
	for {
		stored, err := c.cursor.GetRevocationCursor(ctx)
		if err == nil {
			return *stored, true
		}
		if errors.Is(err, registry.ErrNotFound) {
			break
		}
		if ctx.Err() != nil {
			return registry.RevocationCursor{}, false
		}
		// No revocation is applied until it can be read, so this is an error.
		c.logger.Error("revocation: load cursor", zap.Error(err))
		if !sleepCtx(ctx, bo.NextBackOff()) {
			return registry.RevocationCursor{}, false
		}
	}
	head, ok := c.readHead(ctx, bo)
	if !ok {
		return registry.RevocationCursor{}, false
	}
	position := registry.RevocationCursor{Token: string(head)}
	if err := c.cursor.PutRevocationCursor(ctx, position); err != nil {
		c.logger.Warn("revocation: persist cursor", zap.Error(err))
		saveLater := position
		latest.Store(&saveLater)
	}
	return position, true
}

// readHead reads the firehose's head with backoff until the service answers,
// reporting false only when ctx is canceled first.
func (c *Consumer) readHead(ctx context.Context, bo *backoff.ExponentialBackOff) (swarfclient.Cursor, bool) {
	for {
		head, err := c.src.Head(ctx)
		if err == nil {
			return head, true
		}
		if ctx.Err() != nil {
			return "", false
		}
		c.logger.Warn("revocation: read firehose head", zap.Error(err))
		if !sleepCtx(ctx, bo.NextBackOff()) {
			return "", false
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
