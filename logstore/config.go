package logstore

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
)

// Config wires a Store to its dependencies. Defaults are applied by
// (*Config).defaults() before Open returns.
type Config struct {
	// Dir is the on-disk directory for segment files. Created if
	// missing.
	Dir string

	// Meta is the persistence backing for segment metadata. Required.
	Meta Meta

	// SealBytes is the open-segment size threshold (data CAR + catalog
	// CAR, combined) at which the segment is sealed and queued for
	// flush. 0 → 64 MiB. Sealing is shared across planes — both CARs
	// seal together as one segment.
	SealBytes int64

	// SealAge is the maximum time a segment may remain open before it
	// is sealed even if SealBytes has not been reached. 0 → 5s.
	SealAge time.Duration

	// Data / Catalog configure the two independent upload pipelines. A
	// plane with Ship=false never ships to Forge and is retained on
	// local disk forever; a plane with Ship=true ships each sealed CAR
	// via its Flush callback and retires shipped CARs beyond Retain.
	Data    PlaneConfig
	Catalog PlaneConfig

	// Logger is optional; defaults to zap.NewNop().
	Logger *zap.Logger
}

// PlaneConfig tunes one plane's (data or catalog) upload pipeline.
type PlaneConfig struct {
	// Ship enables this plane's upload pipeline. When false the plane's
	// CAR is never shipped and is retained on local disk indefinitely —
	// it is the only durable copy and the sole source for local reads of
	// that plane.
	Ship bool

	// Flush ships one sealed segment's CAR of THIS plane to Forge. It is
	// bound to the plane it serves (the host builds one closure per
	// plane). Required when Ship is true; ignored otherwise. Returning a
	// non-nil error keeps the plane unshipped and triggers retry.
	Flush FlushFunc

	// Retain is the number of most-recent SHIPPED CARs of this plane to
	// keep on local disk as a read tier. Older shipped CARs are unlinked.
	// 0 → 6. Ignored when Ship is false.
	Retain int
}

// FlushFunc is the contract for shipping one plane's CAR of a sealed
// segment to Forge.
type FlushFunc func(ctx context.Context, seg *Segment) error

// plane returns the PlaneConfig for p.
func (c *Config) plane(p blockstore.Plane) PlaneConfig {
	if p == blockstore.PlaneData {
		return c.Data
	}
	return c.Catalog
}

func (c *Config) validate() error {
	if c.Dir == "" {
		return errors.New("logstore: Dir is required")
	}
	if c.Meta == nil {
		return errors.New("logstore: Meta is required")
	}
	for _, p := range blockstore.Planes {
		pc := c.plane(p)
		if pc.Ship && pc.Flush == nil {
			return errors.New("logstore: " + p.String() + " plane: Flush is required when Ship is true")
		}
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
	if c.Data.Retain <= 0 {
		c.Data.Retain = 6
	}
	if c.Catalog.Retain <= 0 {
		c.Catalog.Retain = 6
	}
	if c.Logger == nil {
		c.Logger = zap.NewNop()
	}
}
