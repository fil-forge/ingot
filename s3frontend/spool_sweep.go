package s3frontend

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/registry"
)

const (
	// spoolLowWatermarkPercent is how far below spool_max_bytes a budget pass
	// evicts, so the next sweep does not start over budget again at once.
	// The headroom must exceed ingest rate × the sweep interval.
	spoolLowWatermarkPercent = 90
	// spoolSweepBatch is how many rows a pass reads per query.
	spoolSweepBatch = 512
	// spoolBudgetPassTimeLimit caps the budget pass so the forced pass that
	// may follow always gets the rest of the sweep's time.
	spoolBudgetPassTimeLimit = 30 * time.Second
	// spoolOrphanPassInterval spaces the orphan pass, which scans the whole
	// spool directory and so stays off the per-sweep path.
	spoolOrphanPassInterval = time.Hour
	// DefaultSpoolOrphanAge is the age at which a .tmp-* file or a blob file
	// with no intent row is deleted, when Deps.SpoolOrphanAge is zero.
	DefaultSpoolOrphanAge = 24 * time.Hour
)

// SpoolSweepStats counts what one SweepSpool run removed, per pass.
type SpoolSweepStats struct {
	BudgetFiles, BudgetBytes int64
	ForcedFiles, ForcedBytes int64
	OrphanFiles, OrphanBytes int64
}

// Removed reports whether the run removed anything.
func (s SpoolSweepStats) Removed() bool {
	return s.BudgetFiles+s.ForcedFiles+s.OrphanFiles > 0
}

// SweepSpool bounds the local spool. With a budget set (Deps.SpoolMaxBytes)
// and usage over it, the budget pass evicts blobs the provider already holds
// (registry.IntentStore.ListEvictable), oldest state change first, down to
// 90% of the budget. It stops at the first blob younger than
// SpoolMinResidency, since every later one is newer, and skips a blob read
// from the spool within SpoolReadRetention. If usage is still over budget,
// the forced pass repeats it without either window: evicting a located blob
// costs read latency, and a full disk fails every write.
//
// Eviction removes only the file. The intent keeps its row and state, marked
// evicted: a release recognises a committed blob by its published state, and
// Complete reads part sizes from intents. The file goes first, so a crash in
// between leaves a row describing a missing file, which the next pass finds
// missing and marks. Readers tolerate the unlink: an open file survives it,
// and a spool miss falls through to the network tier.
//
// On the first run and then hourly, the orphan pass deletes .tmp-* files and
// blob files with no intent row once they are older than SpoolOrphanAge, and
// resets the spool's usage count from the directory scan.
//
// Called periodically by the daemon's spool sweeper, and directly by tests.
func (b *Backend) SweepSpool(ctx context.Context) (SpoolSweepStats, error) {
	b.spoolSweepMu.Lock()
	defer b.spoolSweepMu.Unlock()

	var stats SpoolSweepStats
	if b.spoolMaxBytes > 0 && b.spool.Usage() > b.spoolMaxBytes {
		deadline := time.Now().Add(spoolBudgetPassTimeLimit)
		files, bytes, err := b.evictToBudget(ctx, deadline, true)
		stats.BudgetFiles, stats.BudgetBytes = files, bytes
		if err != nil {
			return stats, fmt.Errorf("s3frontend: spool budget pass: %w", err)
		}
	}
	if b.spoolMaxBytes > 0 && b.spool.Usage() > b.spoolMaxBytes {
		b.logger.Warn("spool is over budget after the budget pass; evicting blobs inside the retention windows",
			zap.Int64("usage", b.spool.Usage()),
			zap.Int64("budget", b.spoolMaxBytes))
		deadline, ok := ctx.Deadline()
		if !ok {
			deadline = time.Now().Add(spoolBudgetPassTimeLimit)
		}
		files, bytes, err := b.evictToBudget(ctx, deadline, false)
		stats.ForcedFiles, stats.ForcedBytes = files, bytes
		if err != nil {
			return stats, fmt.Errorf("s3frontend: spool forced pass: %w", err)
		}
		if b.spool.Usage() > b.spoolMaxBytes {
			b.logger.Warn("spool is still over budget with nothing left to evict: the remaining files are bodies being written or uploaded, blobs with no recorded location, or orphans younger than spool_orphan_age",
				zap.Int64("usage", b.spool.Usage()),
				zap.Int64("budget", b.spoolMaxBytes))
		}
	}
	if b.lastOrphanPass.IsZero() || time.Since(b.lastOrphanPass) >= spoolOrphanPassInterval {
		files, bytes, err := b.removeSpoolOrphans(ctx)
		stats.OrphanFiles, stats.OrphanBytes = files, bytes
		if err != nil {
			return stats, fmt.Errorf("s3frontend: spool orphan pass: %w", err)
		}
		b.lastOrphanPass = time.Now()
	}
	return stats, nil
}

// SpoolUsage returns the byte count of the spool's blob files.
func (b *Backend) SpoolUsage() int64 {
	return b.spool.Usage()
}

// evictToBudget pages through the evictable intents, removing each file and
// marking its intent evicted, until usage is at the low watermark, the rows
// run out, or the deadline passes. honorRetention applies the residency stop
// and the read-recency skip.
func (b *Backend) evictToBudget(ctx context.Context, deadline time.Time, honorRetention bool) (files, bytes int64, err error) {
	target := b.spoolMaxBytes / 100 * spoolLowWatermarkPercent
	now := time.Now()
	var cursor registry.EvictCursor
	for b.spool.Usage() > target && time.Now().Before(deadline) {
		page, err := b.intents.ListEvictable(ctx, cursor, spoolSweepBatch)
		if err != nil {
			return files, bytes, err
		}
		if len(page) == 0 {
			return files, bytes, nil
		}
		for _, in := range page {
			if b.spool.Usage() <= target {
				return files, bytes, nil
			}
			cursor = registry.EvictCursor{UpdatedAt: in.UpdatedAt, Digest: in.Digest}
			if honorRetention {
				if now.Sub(in.UpdatedAt) < b.spoolMinResidency {
					return files, bytes, nil
				}
				if at, ok := b.spool.LastRead(in.Digest); ok && now.Sub(at) < b.spoolReadRetention {
					continue
				}
			}
			freed, err := b.spool.Remove(in.Digest)
			if err != nil {
				return files, bytes, err
			}
			if err := b.intents.MarkEvicted(ctx, in.Digest); err != nil && !errors.Is(err, registry.ErrNotFound) {
				return files, bytes, fmt.Errorf("mark %s evicted: %w", hex.EncodeToString(in.Digest), err)
			}
			files++
			bytes += freed
		}
	}
	return files, bytes, nil
}

// removeSpoolOrphans deletes the files no intent-driven cleanup can find, once
// they are older than the orphan age: .tmp-* files from a write that never
// finished, and blob files with no intent row (a split that failed before
// its intents were recorded). The age must exceed the longest time one
// request body takes to stream, because a request records its intents only
// after its whole body is spooled. Then it resets the usage count from the
// scan.
func (b *Backend) removeSpoolOrphans(ctx context.Context) (files, bytes int64, err error) {
	cutoff := time.Now().Add(-b.spoolOrphanAge)
	var oldTemps []blockstore.SpoolEntry
	var oldBlobs []blockstore.SpoolEntry
	total, err := b.spool.Scan(func(e blockstore.SpoolEntry) {
		if !e.ModTime.Before(cutoff) {
			return
		}
		if e.Digest == nil {
			oldTemps = append(oldTemps, e)
		} else {
			oldBlobs = append(oldBlobs, e)
		}
	})
	if err != nil {
		return 0, 0, err
	}
	for _, e := range oldTemps {
		if err := b.spool.RemoveTemp(e.Name); err != nil {
			return files, bytes, err
		}
		files++
		bytes += e.Size
	}
	var blobBytesRemoved int64
	for start := 0; start < len(oldBlobs); start += spoolSweepBatch {
		batch := oldBlobs[start:min(start+spoolSweepBatch, len(oldBlobs))]
		digests := make([]multihash.Multihash, len(batch))
		for i, e := range batch {
			digests[i] = e.Digest
		}
		missing, err := b.intents.MissingIntents(ctx, digests)
		if err != nil {
			return files, bytes, err
		}
		for _, d := range missing {
			freed, err := b.spool.Remove(d)
			if err != nil {
				return files, bytes, err
			}
			if freed > 0 {
				files++
				bytes += freed
				blobBytesRemoved += freed
			}
		}
	}
	b.spool.ResetUsage(total - blobBytesRemoved)
	return files, bytes, nil
}
