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
	// lowWatermarkPercent is how far below local_blob_max_bytes a budget pass
	// evicts, so the next sweep does not start over budget again at once.
	// The headroom must exceed ingest rate × the sweep interval.
	lowWatermarkPercent = 90
	// localBlobSweepBatch is how many rows a pass reads per query, unless a test
	// sets Backend.localBlobSweepBatch.
	localBlobSweepBatch = 512
	// budgetPassTimeLimit caps the budget pass so the orphan pass that
	// may follow still runs within the sweep's time.
	budgetPassTimeLimit = 30 * time.Second
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
	OrphanFiles, OrphanBytes int64
}

// Removed reports whether the run removed anything.
func (s LocalBlobSweepStats) Removed() bool {
	return s.BudgetFiles+s.OrphanFiles > 0
}

// SweepLocalBlobs bounds the local blob directories. With a budget set
// (Deps.LocalBlobMaxBytes) and their usage over it, the budget pass evicts blobs
// the provider already holds (registry.IntentStore.ListEvictable), oldest
// state change first, down to 90% of the budget, within a time limit. Eviction
// removes a blob's local copy, which is in the cache, or still in the spool
// if the process stopped between recording that the provider holds it and
// moving it. A file it cannot remove is logged and skipped. If the pass runs
// out of evictable blobs with usage still over budget, the rest is files no
// rule lets it remove (bodies being written or uploaded, bodies whose upload
// failed, blobs with no recorded location, young orphans), and it logs a
// warning once until usage is back under budget. A failed upload's intent
// stays spooled or uploading and nothing reclaims its file yet, so those
// bytes count against the budget until an operator removes them. The counts
// see such a removal at the next restart, or at the next orphan pass that
// runs while the directory is quiet.
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
	// the time the orphan pass is recorded as run. Only the budget pass's
	// time limit reads it again, as it goes.
	now := time.Now()
	var stats LocalBlobSweepStats
	var errs []error
	if b.localBlobMaxBytes > 0 && b.localUsage() > b.localBlobMaxBytes {
		budgetCtx, cancel := context.WithTimeout(ctx, budgetPassTimeLimit)
		pass, err := b.evictToBudget(budgetCtx, now.Add(budgetPassTimeLimit))
		cancel()
		stats.BudgetFiles, stats.BudgetBytes = pass.files, pass.bytes
		b.localBlobMetrics.removed(ctx, removedBudget, pass.files, pass.bytes)
		// A query cut off by the pass's own time limit is the time limit,
		// not a failure.
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			err = nil
		}
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("s3frontend: local blob budget pass: %w", err))
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
				b.logger.Info("local blob budget pass reached its time limit while still over budget; the next sweep continues. This is logged once until usage is back under budget",
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
		files, bytes, err := b.removeOrphans(orphanCtx, now)
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
// (rather than at the low watermark or its deadline).
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
// the low watermark, the rows run out, or the deadline passes. A file it
// cannot remove is skipped, so one bad file does not stop every later pass at
// the same row; its intent stays unmarked, and the pass logs the failures
// once. A row whose file was already gone is marked but not counted.
func (b *Backend) evictToBudget(ctx context.Context, deadline time.Time) (pass budgetPass, err error) {
	var failed removeFailures
	defer failed.log(b.logger, "budget")
	target := b.localBlobMaxBytes * lowWatermarkPercent / 100
	batch := b.localBlobSweepBatch
	if batch <= 0 {
		batch = localBlobSweepBatch
	}
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

// removeOrphans deletes the files no intent-driven cleanup can find, once
// they are older than the orphan age: .tmp-* files from a write that never
// finished, and blob files with no intent row (a split that failed before
// its intents were recorded), in the spool and the cache. The age must exceed
// the longest time one request body takes to stream, because a request
// records its intents only after its whole body is spooled. The scans take no
// lock, so writes and moves go on meanwhile; a file found in the spool may have
// moved to the cache by the time it is checked, which removeLocal handles. A
// scan that nothing in this process overlapped also corrects its directory's
// byte count, which only a file added or removed outside ingot can throw off,
// and logs the correction. It finishes the spool, which is small and where
// orphans arise, before it scans the cache, so a slow cache cannot hold up
// the spool's cleanup. It checks each directory's old blobs a batch at a time
// as the scan reaches them, so its memory stays small however large the
// cache; a cache orphan is rare, since a file enters the cache only after its
// intent exists and every path that deletes an intent removes the file first.
// A file it cannot remove is skipped and the failures are logged once, so one
// bad file does not stop the pass; only a failed scan or query does. Ages are
// measured from now, the sweep's time.
func (b *Backend) removeOrphans(ctx context.Context, now time.Time) (files, bytes int64, err error) {
	var failed removeFailures
	defer failed.log(b.logger, "orphan")
	cutoff := now.Add(-b.localBlobOrphanAge)
	batch := b.localBlobSweepBatch
	if batch <= 0 {
		batch = localBlobSweepBatch
	}
	count := func(freed int64) {
		if freed > 0 {
			files++
			bytes += freed
		}
	}
	// checkBlobs removes the local copy of each digest that has no intent.
	checkBlobs := func(ctx context.Context, digests []multihash.Multihash) error {
		missing, err := b.intents.MissingIntents(ctx, digests)
		if err != nil {
			return err
		}
		for _, d := range missing {
			freed, err := b.removeLocal(d)
			count(freed)
			if err != nil {
				failed.add(hex.EncodeToString(d), err)
			}
		}
		return nil
	}
	for _, dir := range []struct {
		name       string
		scan       func(context.Context, func(blockstore.BlobFile)) (int64, error)
		removeTemp func(string) (int64, error)
	}{
		{"spool", b.spool.ScanAndCorrect, b.spool.RemoveTemp},
		{"cache", b.cache.ScanAndCorrect, b.cache.RemoveTemp},
	} {
		// Each batch of old blobs is checked as the scan reaches it, so a
		// large directory is never held in memory; a failed query stops the
		// scan through scanCtx.
		scanCtx, cancel := context.WithCancel(ctx)
		var pending []multihash.Multihash
		var queryErr error
		drift, err := dir.scan(scanCtx, func(f blockstore.BlobFile) {
			if queryErr != nil || !f.ModTime.Before(cutoff) {
				return
			}
			if f.Digest == nil {
				freed, err := dir.removeTemp(f.Name)
				count(freed)
				if err != nil {
					failed.add(f.Name, err)
				}
				return
			}
			pending = append(pending, f.Digest)
			if len(pending) == batch {
				queryErr = checkBlobs(scanCtx, pending)
				pending = pending[:0]
				if queryErr != nil {
					cancel()
				}
			}
		})
		if queryErr == nil && err == nil && len(pending) > 0 {
			queryErr = checkBlobs(ctx, pending)
		}
		cancel()
		if queryErr != nil {
			return files, bytes, queryErr
		}
		if err != nil {
			return files, bytes, err
		}
		if drift != 0 {
			b.logger.Warn("local blob "+dir.name+" byte count corrected: files were added or removed outside ingot",
				zap.Int64("counted_minus_on_disk", drift))
		}
	}
	return files, bytes, nil
}
