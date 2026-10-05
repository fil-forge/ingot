package s3frontend

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/registry"
)

const (
	// lowWatermarkPercent is how far below local_blob_max_bytes a budget pass
	// evicts, so the next sweep does not start over budget again at once.
	// The headroom must exceed ingest rate × the sweep interval.
	lowWatermarkPercent = 90
	// localBlobSweepBatch is how many rows a pass reads per query, unless a test
	// sets Backend.localBlobSweepBatch.
	localBlobSweepBatch = 512
	// budgetPassTimeLimit and forcedPassTimeLimit cap the budget and forced
	// passes so the passes that may follow each still run within the sweep's
	// time.
	budgetPassTimeLimit = 30 * time.Second
	forcedPassTimeLimit = 30 * time.Second
	// orphanPassTimeLimit caps the orphan pass separately, so a long
	// budget pass cannot starve it.
	orphanPassTimeLimit = 2 * time.Minute
	// orphanPassInterval spaces the orphan pass, which scans the whole
	// spool and cache directories and so stays off the per-sweep path.
	orphanPassInterval = time.Hour
	// DefaultLocalBlobOrphanAge is the age at which a .tmp-* file or a blob file
	// with no intent row is deleted, when Deps.LocalBlobOrphanAge is zero.
	DefaultLocalBlobOrphanAge = 24 * time.Hour
)

// LocalBlobSweepStats counts what one SweepLocalBlobs run removed, per pass.
type LocalBlobSweepStats struct {
	BudgetFiles, BudgetBytes int64
	ForcedFiles, ForcedBytes int64
	OrphanFiles, OrphanBytes int64
}

// Removed reports whether the run removed anything.
func (s LocalBlobSweepStats) Removed() bool {
	return s.BudgetFiles+s.ForcedFiles+s.OrphanFiles > 0
}

// SweepLocalBlobs bounds the local blob directories. With a budget set
// (Deps.LocalBlobMaxBytes) and their usage over it, the budget pass evicts blobs
// the provider already holds (registry.IntentStore.ListEvictable), oldest
// state change first, down to 90% of the budget, within a time limit. Eviction
// removes a blob's local copy, which is in the cache, or still in the spool
// if the process stopped between recording that the provider holds it and
// moving it. The pass stops at the first blob younger than
// CacheMinResidency, since every later one is newer, and skips a blob read
// from the cache within CacheReadRetention. If usage is still over budget,
// the forced pass repeats it without either window: evicting a held blob
// costs read latency, and a full disk fails every write. Either pass logs and
// skips a file it cannot remove. If the forced pass runs out of evictable
// blobs with usage still over budget, the rest is files no rule lets it
// remove (bodies being written or uploaded, bodies whose upload failed, blobs
// with no recorded location, young orphans), and it logs a warning once
// until usage is back under budget. A failed upload's intent
// stays spooled or uploading and nothing reclaims its file yet, so those
// bytes count against the budget until an operator removes them.
//
// Eviction removes only the file. The intent keeps its row and state, marked
// evicted: a session's release recognises a committed part blob by its
// published state, and Complete reads part sizes from intents. The file goes
// first, so a crash in between leaves a row describing a missing file, which
// the next pass finds missing and marks. Readers tolerate the unlink: an open file survives it,
// and a local miss falls through to the network tier.
//
// On the first run and then hourly, with or without a budget, the orphan
// pass deletes .tmp-* files and blob files with no intent row once they are
// older than LocalBlobOrphanAge, in both directories. It runs even when the
// budget pass fails, with a time limit of its own, and counts as run whether
// or not it finishes, so a slow or failing pass waits for the next hour
// rather than starting over every sweep.
//
// Called periodically by the daemon's local blob sweeper, and directly by tests.
func (b *Backend) SweepLocalBlobs(ctx context.Context) (LocalBlobSweepStats, error) {
	b.localBlobSweepMu.Lock()
	defer b.localBlobSweepMu.Unlock()

	// One reading of the clock dates the whole sweep: the orphan cutoff and
	// the time the orphan pass is recorded as run. Only the eviction passes'
	// time limits read it again, as they go.
	now := time.Now()
	var stats LocalBlobSweepStats
	var errs []error
	if b.localBlobMaxBytes > 0 && b.localUsage() > b.localBlobMaxBytes {
		budgetCtx, cancel := context.WithTimeout(ctx, budgetPassTimeLimit)
		pass, err := b.evictToBudget(budgetCtx, now.Add(budgetPassTimeLimit), true)
		cancel()
		stats.BudgetFiles, stats.BudgetBytes = pass.files, pass.bytes
		b.localBlobMetrics.removed(ctx, removedBudget, pass.files, pass.bytes)
		// A query cut off by the pass's own time limit is the time limit,
		// not a failure.
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			err = nil
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("s3frontend: local blob budget pass: %w", err))
		}
	}
	if len(errs) == 0 && b.localBlobMaxBytes > 0 && b.localUsage() > b.localBlobMaxBytes {
		forcedCtx, cancel := context.WithTimeout(ctx, forcedPassTimeLimit)
		pass, err := b.evictToBudget(forcedCtx, time.Now().Add(forcedPassTimeLimit), false)
		cancel()
		stats.ForcedFiles, stats.ForcedBytes = pass.files, pass.bytes
		b.localBlobMetrics.removed(ctx, removedBudgetForced, pass.files, pass.bytes)
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			err = nil
		}
		if pass.files > 0 {
			b.logger.Warn("local blob storage was over budget after the budget pass; evicted blobs inside the retention windows (cache_min_residency, cache_read_retention)",
				zap.Int64("files", pass.files),
				zap.Int64("bytes", pass.bytes),
				zap.Int64("budget", b.localBlobMaxBytes))
		}
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("s3frontend: local blob forced pass: %w", err))
		case b.localUsage() <= b.localBlobMaxBytes:
		case pass.exhausted:
			if !b.overBudgetWarned {
				b.overBudgetWarned = true
				b.logger.Warn("local blob storage is over budget with nothing left to evict; this is logged once until usage is back under budget. The remaining files are bodies being written or uploaded, bodies whose upload failed (their intents stay spooled or uploading; nothing reclaims them yet), blobs with no recorded location, files that could not be removed, or orphans younger than local_blob_orphan_age",
					zap.Int64("usage", b.localUsage()),
					zap.Int64("spool", b.spool.Usage()),
					zap.Int64("budget", b.localBlobMaxBytes))
			}
		default:
			if !b.timeLimitLogged {
				b.timeLimitLogged = true
				b.logger.Info("local blob forced pass reached its time limit while still over budget; the next sweep continues. This is logged once until usage is back under budget",
					zap.Int64("usage", b.localUsage()),
					zap.Int64("budget", b.localBlobMaxBytes))
			}
		}
	}
	if b.localUsage() <= b.localBlobMaxBytes {
		b.overBudgetWarned = false
		b.timeLimitLogged = false
	}
	if b.lastOrphanPass.IsZero() || now.Sub(b.lastOrphanPass) >= orphanPassInterval {
		orphanCtx, cancel := context.WithTimeout(ctx, orphanPassTimeLimit)
		files, bytes, err := b.removeSpoolOrphans(orphanCtx, now)
		cancel()
		stats.OrphanFiles, stats.OrphanBytes = files, bytes
		b.localBlobMetrics.removed(ctx, removedOrphan, files, bytes)
		b.lastOrphanPass = now
		switch {
		case errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
			b.logger.Info("local blob orphan pass reached its time limit; the next pass, in an hour, starts again",
				zap.Int64("files", files),
				zap.Int64("bytes", bytes))
		case err != nil:
			errs = append(errs, fmt.Errorf("s3frontend: local blob orphan pass: %w", err))
		}
	}
	return stats, errors.Join(errs...)
}

// LocalBlobUsage returns the bytes the local blob directories hold, writes in
// progress included.
func (b *Backend) LocalBlobUsage() int64 {
	return b.localUsage()
}

// budgetPass is what one evictToBudget run did: the files it removed and
// their bytes, and whether it stopped because no evictable rows were left
// (rather than at the low watermark, the residency stop, or its deadline).
type budgetPass struct {
	files, bytes int64
	exhausted    bool
}

// removeFailures tallies the files one pass could not remove, so the pass
// skips them and logs once rather than once per file.
type removeFailures struct {
	count int
	first string
	err   error
}

func (f *removeFailures) add(name string, err error) {
	if f.count == 0 {
		f.first, f.err = name, err
	}
	f.count++
}

// log warns once if any removal failed.
func (f *removeFailures) log(logger *zap.Logger, pass string) {
	if f.count == 0 {
		return
	}
	logger.Warn("local blob "+pass+" pass: could not remove some files; skipped them",
		zap.Int("files", f.count),
		zap.String("first", f.first),
		zap.Error(f.err))
}

// evictToBudget pages through the evictable intents, oldest state change
// first, removing each file and marking its intent evicted, until usage is at
// the low watermark, the rows run out, or the deadline passes. honorRetention
// applies the residency stop and the read-recency skip. A file it cannot
// remove is skipped, so one bad file does not stop every later pass at the
// same row; its intent stays unmarked, and the pass logs the failures once. A
// row whose file was already gone is marked but not counted.
func (b *Backend) evictToBudget(ctx context.Context, deadline time.Time, honorRetention bool) (pass budgetPass, err error) {
	var failed removeFailures
	if honorRetention {
		defer failed.log(b.logger, "budget")
	} else {
		defer failed.log(b.logger, "forced")
	}
	target := b.localBlobMaxBytes * lowWatermarkPercent / 100
	batch := b.localBlobSweepBatch
	if batch <= 0 {
		batch = localBlobSweepBatch
	}
	now := time.Now()
	var cursor registry.EvictCursor
	for b.localUsage() > target && time.Now().Before(deadline) {
		page, err := b.intents.ListEvictable(ctx, cursor, batch)
		if err != nil {
			return pass, err
		}
		if len(page) == 0 {
			pass.exhausted = true
			return pass, nil
		}
		for _, in := range page {
			if b.localUsage() <= target {
				return pass, nil
			}
			cursor = registry.EvictCursor{UpdatedAt: in.UpdatedAt, Digest: in.Digest}
			if honorRetention {
				if now.Sub(in.UpdatedAt) < b.cacheMinResidency {
					return pass, nil
				}
				if at, ok := b.cache.LastRead(in.Digest); ok && now.Sub(at) < b.cacheReadRetention {
					continue
				}
			}
			freed, err := b.removeLocal(in.Digest)
			if freed > 0 {
				pass.files++
				pass.bytes += freed
			}
			if err != nil {
				failed.add(hex.EncodeToString(in.Digest), err)
				continue
			}
			if err := b.intents.MarkEvicted(ctx, in.Digest); err != nil && !errors.Is(err, registry.ErrNotFound) {
				return pass, fmt.Errorf("mark %s evicted: %w", hex.EncodeToString(in.Digest), err)
			}
		}
	}
	return pass, nil
}

// removeSpoolOrphans deletes the files no intent-driven cleanup can find, once
// they are older than the orphan age: .tmp-* files from a write that never
// finished, and blob files with no intent row (a split that failed before
// its intents were recorded), in the spool and the cache. The age must exceed
// the longest time one request body takes to stream, because a request
// records its intents only after its whole body is spooled. The scans take no
// lock, so writes and moves go on meanwhile; a file found in the spool may have
// moved to the cache by the time it is checked, which removeLocal handles. It
// checks the spool's blobs first, since they are few and a stray one may be the
// cause of an over-budget spool, then the cache's in random order, so a pass
// that runs out of time still covers a different part of a large cache each
// hour. A file it cannot remove is skipped and the failures are logged once,
// so one bad file does not stop the pass; only a failed scan or query does.
// Ages are measured from now, the sweep's time.
func (b *Backend) removeSpoolOrphans(ctx context.Context, now time.Time) (files, bytes int64, err error) {
	var failed removeFailures
	defer failed.log(b.logger, "orphan")
	cutoff := now.Add(-b.localBlobOrphanAge)
	// Each directory's old temp files, and the digests of its old blobs.
	type found struct {
		temps []string
		blobs []multihash.Multihash
	}
	collect := func(into *found) func(blockstore.BlobFile) {
		return func(f blockstore.BlobFile) {
			if !f.ModTime.Before(cutoff) {
				return
			}
			if f.Digest == nil {
				into.temps = append(into.temps, f.Name)
			} else {
				into.blobs = append(into.blobs, f.Digest)
			}
		}
	}
	var inSpool, inCache found
	if err := b.spool.Scan(ctx, collect(&inSpool)); err != nil {
		return 0, 0, err
	}
	if err := b.cache.Scan(ctx, collect(&inCache)); err != nil {
		return 0, 0, err
	}
	for _, t := range []struct {
		remove func(string) (int64, error)
		names  []string
	}{{b.spool.RemoveTemp, inSpool.temps}, {b.cache.RemoveTemp, inCache.temps}} {
		for _, name := range t.names {
			freed, err := t.remove(name)
			if err != nil {
				failed.add(name, err)
				continue
			}
			if freed > 0 {
				files++
				bytes += freed
			}
		}
	}
	rand.Shuffle(len(inCache.blobs), func(i, j int) {
		inCache.blobs[i], inCache.blobs[j] = inCache.blobs[j], inCache.blobs[i]
	})
	oldBlobs := append(inSpool.blobs, inCache.blobs...)
	batch := b.localBlobSweepBatch
	if batch <= 0 {
		batch = localBlobSweepBatch
	}
	for start := 0; start < len(oldBlobs); start += batch {
		missing, err := b.intents.MissingIntents(ctx, oldBlobs[start:min(start+batch, len(oldBlobs))])
		if err != nil {
			return files, bytes, err
		}
		for _, d := range missing {
			freed, err := b.removeLocal(d)
			if freed > 0 {
				files++
				bytes += freed
			}
			if err != nil {
				failed.add(hex.EncodeToString(d), err)
			}
		}
	}
	return files, bytes, nil
}
