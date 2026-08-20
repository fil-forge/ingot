package logstore

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
)

// Config wires a Store (the catalog log) to its dependencies. Defaults are
// applied by (*Config).defaults() before Open returns.
type Config struct {
	// Dir is the on-disk root for segment files. The catalog plane lives under
	// <Dir>/catalog. Created if missing.
	Dir string

	// Bucket is the bucket this store belongs to. The log is segregated per
	// bucket (each bucket's segments ship to its own Forge space); segment
	// rows are stamped with it, and recovery lists only this bucket's rows.
	Bucket string

	// Meta is the persistence backing for segment metadata. Required.
	Meta Meta

	// Catalog configures the catalog pipeline (seal trigger, ship gate,
	// retention).
	Catalog PlaneConfig

	// Logger is optional; defaults to zap.NewNop().
	Logger *zap.Logger
}

// PlaneConfig tunes one plane's (data or catalog) independent pipeline:
// its seal trigger, whether/how it ships, and how much it retains.
type PlaneConfig struct {
	// SealBytes is the open-segment CAR size threshold at which this
	// plane's segment seals and is queued for flush. 0 → 64 MiB.
	SealBytes int64

	// SealAge is the maximum time this plane's segment may stay open
	// before it seals even under SealBytes. 0 → 5s.
	SealAge time.Duration

	// Ship enables this plane's upload pipeline. When false the plane's
	// CARs are never shipped and are retained on local disk indefinitely —
	// the only durable copy and the sole source for local reads of that
	// plane.
	Ship bool

	// Flush ships one sealed segment's CAR of this plane to Forge.
	// Required when Ship is true; ignored otherwise. A non-nil error
	// keeps the segment unshipped and triggers retry.
	Flush FlushFunc

	// Retain is the number of most-recent SHIPPED CARs of this plane to
	// keep on local disk as a read tier. Older shipped CARs are unlinked.
	// 0 → 6. Ignored when Ship is false.
	Retain int
}

// FlushFunc is the contract for shipping one sealed segment's CAR to
// Forge. The segment is single-plane, so no plane argument is needed. On
// success it returns the digest of the shipped sharded-dag-index blob (the
// ship registers the CAR and its index in the bucket's space; DeleteBucket
// releases both), or nil when nothing registered (header-only segment, or a
// non-publishing uploader).
type FlushFunc func(ctx context.Context, seg *Segment) ([]byte, error)

func (c *Config) validate() error {
	if c.Dir == "" {
		return errors.New("logstore: Dir is required")
	}
	if c.Meta == nil {
		return errors.New("logstore: Meta is required")
	}
	if c.Catalog.Ship && c.Catalog.Flush == nil {
		return errors.New("logstore: catalog plane: Flush is required when Ship is true")
	}
	return nil
}

func (c *Config) defaults() {
	c.Catalog = c.Catalog.withDefaults()
	if c.Logger == nil {
		c.Logger = zap.NewNop()
	}
}

// withDefaults returns pc with zero-valued seal/retain knobs filled in.
func (pc PlaneConfig) withDefaults() PlaneConfig {
	if pc.SealBytes <= 0 {
		pc.SealBytes = 64 << 20
	}
	if pc.SealAge <= 0 {
		pc.SealAge = 5 * time.Second
	}
	if pc.Retain <= 0 {
		pc.Retain = 6
	}
	return pc
}
