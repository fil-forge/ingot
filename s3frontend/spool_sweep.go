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
	// spoolSweepBatch is how many rows a pass reads per query, unless a test
	// sets Backend.spoolSweepBatch.
	spoolSweepBatch = 512
	// spoolBudgetPassTimeLimit caps the budget pass so the orphan pass that
	// may follow still runs within the sweep's time.
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
	OrphanFiles, OrphanBytes int64
}

// Removed reports whether the run removed anything.
func (s SpoolSweepStats) Removed() bool {
	return s.BudgetFiles+s.OrphanFiles > 0
}

// SweepSpool bounds the local spool. With a budget set (Deps.SpoolMaxBytes)
// and usage over it, the budget pass evicts blobs the provider already holds
// (registry.IntentStore.ListEvictable), oldest state change first, down to
// 90% of the budget, within a time limit. A file it cannot remove is logged
// and skipped. If the pass runs out of evictable blobs with usage still over
// budget, the rest is files no rule lets it remove (bodies being written or
// uploaded, blobs with no recorded location, young orphans), and it logs a
// warning.
//
// Eviction removes only the file. The intent keeps its row and state, marked
// evicted: a release recognises a committed blob by its published state, and
// Complete reads part sizes from intents. The file goes first, so a crash in
// between leaves a row describing a missing file, which the next pass finds
// missing and marks. Readers tolerate the unlink: an open file survives it,
// and a spool miss falls through to the network tier.
//
// On the first run and then hourly, with or without a budget, the orphan
// pass deletes .tmp-* files and blob files with no intent row once they are
// older than SpoolOrphanAge, and corrects the spool's usage count to the
// directory scan. It runs even when the budget pass fails.
//
// Called periodically by the daemon's spool sweeper, and directly by tests.
func (b *Backend) SweepSpool(ctx context.Context) (SpoolSweepStats, error) {
	b.spoolSweepMu.Lock()
	defer b.spoolSweepMu.Unlock()

	// One reading of the clock dates the whole sweep: the orphan cutoff and
	// the time the orphan pass is recorded as run. Only the budget pass's
	// time limit reads it again, as it goes.
	now := time.Now()
	var stats SpoolSweepStats
	var errs []error
	if b.spoolMaxBytes > 0 && b.spool.Usage() > b.spoolMaxBytes {
		deadline := now.Add(spoolBudgetPassTimeLimit)
		pass, err := b.evictToBudget(ctx, deadline)
		stats.BudgetFiles, stats.BudgetBytes = pass.files, pass.bytes
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("s3frontend: spool budget pass: %w", err))
		case b.spool.Usage() <= b.spoolMaxBytes:
		case pass.exhausted:
			b.logger.Warn("spool is still over budget with nothing left to evict: the remaining files are bodies being written or uploaded, blobs with no recorded location, files that could not be removed, or orphans younger than spool_orphan_age",
				zap.Int64("usage", b.spool.Usage()),
				zap.Int64("budget", b.spoolMaxBytes))
		default:
			b.logger.Info("spool budget pass reached its time limit while still over budget; the next sweep continues",
				zap.Int64("usage", b.spool.Usage()),
				zap.Int64("budget", b.spoolMaxBytes))
		}
	}
	if b.lastOrphanPass.IsZero() || now.Sub(b.lastOrphanPass) >= spoolOrphanPassInterval {
		files, bytes, err := b.removeSpoolOrphans(ctx, now)
		stats.OrphanFiles, stats.OrphanBytes = files, bytes
		if err != nil {
			errs = append(errs, fmt.Errorf("s3frontend: spool orphan pass: %w", err))
		} else {
			b.lastOrphanPass = now
		}
	}
	return stats, errors.Join(errs...)
}

// SpoolUsage returns the byte count of the spool's files, writes in progress
// included.
func (b *Backend) SpoolUsage() int64 {
	return b.spool.Usage()
}

// budgetPass is what one evictToBudget run did: the files it removed and
// their bytes, and whether it stopped because no evictable rows were left
// (rather than at the low watermark or its deadline).
type budgetPass struct {
	files, bytes int64
	exhausted    bool
}

// evictToBudget pages through the evictable intents, oldest state change
// first, removing each file and marking its intent evicted, until usage is at
// the low watermark, the rows run out, or the deadline passes. A file it
// cannot remove is logged and skipped, so one bad file does not stop every
// later pass at the same row; its intent stays unmarked. A row whose file was
// already gone is marked but not counted.
func (b *Backend) evictToBudget(ctx context.Context, deadline time.Time) (pass budgetPass, err error) {
	target := b.spoolMaxBytes * spoolLowWatermarkPercent / 100
	batch := b.spoolSweepBatch
	if batch <= 0 {
		batch = spoolSweepBatch
	}
	var cursor registry.EvictCursor
	for b.spool.Usage() > target && time.Now().Before(deadline) {
		page, err := b.intents.ListEvictable(ctx, cursor, batch)
		if err != nil {
			return pass, err
		}
		if len(page) == 0 {
			pass.exhausted = true
			return pass, nil
		}
		for _, in := range page {
			if b.spool.Usage() <= target {
				return pass, nil
			}
			cursor = registry.EvictCursor{UpdatedAt: in.UpdatedAt, Digest: in.Digest}
			freed, err := b.spool.Remove(in.Digest)
			if err != nil {
				b.logger.Warn("spool budget pass: cannot remove a file; skipping it",
					zap.String("digest", hex.EncodeToString(in.Digest)),
					zap.Error(err))
				continue
			}
			if err := b.intents.MarkEvicted(ctx, in.Digest); err != nil && !errors.Is(err, registry.ErrNotFound) {
				return pass, fmt.Errorf("mark %s evicted: %w", hex.EncodeToString(in.Digest), err)
			}
			if freed > 0 {
				pass.files++
				pass.bytes += freed
			}
		}
	}
	return pass, nil
}

// removeSpoolOrphans deletes the files no intent-driven cleanup can find, once
// they are older than the orphan age: .tmp-* files from a write that never
// finished, and blob files with no intent row (a split that failed before
// its intents were recorded). The age must exceed the longest time one
// request body takes to stream, because a request records its intents only
// after its whole body is spooled. It corrects the usage count to the scan
// first; each removal then takes its own bytes off. Ages are measured from
// now, the sweep's time.
func (b *Backend) removeSpoolOrphans(ctx context.Context, now time.Time) (files, bytes int64, err error) {
	cutoff := now.Add(-b.spoolOrphanAge)
	var oldTemps []blockstore.SpoolEntry
	var oldBlobs []blockstore.SpoolEntry
	before := b.spool.Usage()
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
	b.spool.CorrectUsage(before, total)
	for _, e := range oldTemps {
		freed, err := b.spool.RemoveTemp(e.Name)
		if err != nil {
			return files, bytes, err
		}
		if freed > 0 {
			files++
			bytes += freed
		}
	}
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
			}
		}
	}
	return files, bytes, nil
}
