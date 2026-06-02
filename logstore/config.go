package logstore

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
)

// Config wires a Store to its dependencies. Defaults are applied by
// (*Config).defaults() before Open returns.
type Config struct {
	// Dir is the on-disk directory for segment files. Created if
	// missing.
	Dir string

	// Meta is the persistence backing for segment metadata. Required.
	Meta Meta

	// SealBytes is the open-segment size threshold at which the
	// segment is sealed and queued for flush. 0 → 64 MiB.
	SealBytes int64

	// SealAge is the maximum time a segment may remain open before it
	// is sealed even if SealBytes has not been reached. 0 → 5s.
	SealAge time.Duration

	// Retain is the number of most-recent flushed segments to keep
	// on disk as a local read tier. Older flushed segments are
	// unlinked. 0 → 6.
	Retain int

	// Flush is the callback invoked by the background flusher for
	// each sealed segment. It is responsible for shipping the
	// segment's blocks to Forge and updating the persistence layer
	// (typically Meta.MarkSegmentFlushed). Returning a non-nil
	// error keeps the segment in StateSealed and triggers retry on
	// the next flush tick.
	Flush FlushFunc

	// Logger is optional; defaults to zap.NewNop().
	Logger *zap.Logger
}

// FlushFunc is the contract for shipping a sealed segment to Forge.
type FlushFunc func(ctx context.Context, seg *Segment) error

func (c *Config) validate() error {
	if c.Dir == "" {
		return errors.New("logstore: Dir is required")
	}
	if c.Meta == nil {
		return errors.New("logstore: Meta is required")
	}
	if c.Flush == nil {
		return errors.New("logstore: Flush is required")
	}
	return nil
}

func (c *Config) defaults() {
	if c.SealBytes <= 0 {
		c.SealBytes = 64 << 20
	}
	if c.SealAge <= 0 {
		c.SealAge = 5 * time.Second
	}
	if c.Retain <= 0 {
		c.Retain = 6
	}
	if c.Logger == nil {
		c.Logger = zap.NewNop()
	}
}
