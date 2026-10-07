package s3frontend

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/tracing"
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
	// forcedWarnInterval spaces the forced pass's warnings.
	forcedWarnInterval = time.Hour
	// orphanPassTimeLimit caps the orphan pass separately, so a long
	// budget pass cannot starve it.
	orphanPassTimeLimit = 2 * time.Minute
	// orphanPassInterval spaces the orphan pass, which scans the whole
	// spool and cache directories and so stays off the per-sweep path.
	orphanPassInterval = time.Hour
	// stalledUploadAge is how long a body may wait for upload, since its
	// intent last changed state, before the stalled_bytes gauge counts it:
	// well past the longest a live upload takes.
	stalledUploadAge = time.Hour
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

// Add adds o's counts to s, to total several runs.
func (s *LocalBlobSweepStats) Add(o LocalBlobSweepStats) {
	s.BudgetFiles += o.BudgetFiles
	s.BudgetBytes += o.BudgetBytes
	s.ForcedFiles += o.ForcedFiles
	s.ForcedBytes += o.ForcedBytes
	s.OrphanFiles += o.OrphanFiles
	s.OrphanBytes += o.OrphanBytes
}

// SpanAttributes returns the counts as span attributes.
func (s LocalBlobSweepStats) SpanAttributes() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int64("ingot.local_blobs.budget_files", s.BudgetFiles),
		attribute.Int64("ingot.local_blobs.budget_bytes", s.BudgetBytes),
		attribute.Int64("ingot.local_blobs.forced_files", s.ForcedFiles),
		attribute.Int64("ingot.local_blobs.forced_bytes", s.ForcedBytes),
		attribute.Int64("ingot.local_blobs.orphan_files", s.OrphanFiles),
		attribute.Int64("ingot.local_blobs.orphan_bytes", s.OrphanBytes),
	}
}

// LogFields returns the counts as log fields.
func (s LocalBlobSweepStats) LogFields() []zap.Field {
	return []zap.Field{
		zap.Int64("budget_files", s.BudgetFiles),
		zap.Int64("budget_bytes", s.BudgetBytes),
		zap.Int64("forced_files", s.ForcedFiles),
		zap.Int64("forced_bytes", s.ForcedBytes),
		zap.Int64("orphan_files", s.OrphanFiles),
		zap.Int64("orphan_bytes", s.OrphanBytes),
	}
}

// SweepLocalBlobs bounds the local blob directories. With a budget set
// (Deps.LocalBlobMaxBytes) and their usage over it, the budget pass evicts
// blobs the provider already holds (registry.IntentStore.ListEvictable), oldest
// state change first, down to 90% of the budget, within a time limit. Eviction
// removes a blob's local copy, which is in the cache, or still in the spool if
// the process stopped between recording that the provider holds it and moving
// it. The pass stops at the first blob younger than CacheMinResidency, since
// every later one is newer, and skips a blob read from the cache within
// CacheReadRetention. If it stopped at the residency window, or ran out of
// blobs having passed over recently read ones, with usage still over budget,
// the forced pass gives up the windows in stages (see forcedPass); a budget
// pass that only ran out of time leaves the rest to the next sweep. Every pass
// logs and skips a file it cannot remove. If the last pass to run (the forced
// pass, or the budget pass when there was nothing for the forced pass to try)
// runs out of evictable blobs with usage still over budget, the rest is files
// no rule lets it remove (bodies being written or uploaded, bodies whose upload
// failed, blobs with no recorded location, young orphans), and it logs a
// warning once until usage falls back to the low watermark. A failed upload's
// intent stays spooled or uploading and nothing reclaims its file yet, so those
// bytes count against the budget until an operator removes them. The counts see
// such a removal at the next restart, or at the next orphan pass that runs
// while the directory is quiet.
//
// Eviction removes only the file. The intent keeps its row and state, marked
// evicted: a session's release recognises a committed part blob by its
// published state, and Complete reads part sizes from intents. The file goes
// first, so a crash in between leaves a row describing a missing file, which
// the next pass finds missing and marks. Readers tolerate the unlink: an open
// file survives it, and a local miss falls through to the network tier.
//
// On the first run and then hourly, with or without a budget, the orphan pass
// deletes .tmp-* files in both directories, and spool blob files with no intent
// row, once they are older than LocalBlobOrphanAge. It runs even when the
// budget pass fails, with a time limit of its own, and counts as run whether or
// not it finishes, so a slow or failing pass waits for the next hour rather
// than starting over every sweep.
//
// Every run also sums the bodies whose upload has stalled: intents still
// spooled or uploading an hour after their last state change, which no live
// request holds that long. The stalled_bytes gauge reports the sum.
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
		budgetCtx, span := tracing.Start(budgetCtx, "local_blobs.budget_pass")
		pass, err := b.evictToBudget(budgetCtx, now.Add(budgetPassTimeLimit), honorBothWindows)
		cancel()
		stats.BudgetFiles, stats.BudgetBytes = pass.files, pass.bytes
		b.localBlobMetrics.removed(ctx, removedBudget, pass.files, pass.bytes)
		// A query cut off by the pass's own time limit is the time limit,
		// not a failure.
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			err, pass.stop = nil, stopDeadline
		}
		span.SetAttributes(pass.spanAttributes()...)
		tracing.End(span, err)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("s3frontend: local blob budget pass: %w", err))
		case b.localUsage() <= b.localBlobMaxBytes:
		case pass.stop == stopDeadline:
			// Candidates outside the windows may remain; the next sweep
			// continues rather than evicting inside them.
			b.logTimeLimit("budget")
		case pass.stop == stopResidency, pass.stop == stopExhausted && pass.skipped > 0:
			if err := b.forcedPass(ctx, pass.stop, &stats); err != nil {
				errs = append(errs, err)
			}
		case pass.stop == stopExhausted:
			// Having passed over no recently read rows, the forced pass's
			// last stage would repeat the budget pass exactly.
			b.warnNothingLeft()
		}
	}
	// The forced pass leaves usage just under the budget, so the latches
	// reset at the low watermark, or they would reset nearly every sweep.
	if b.localUsage() <= b.localBlobMaxBytes*lowWatermarkPercent/100 {
		b.overBudgetWarned = false
		b.timeLimitLogged = false
	}
	if b.lastOrphanPass.IsZero() || now.Sub(b.lastOrphanPass) >= orphanPassInterval {
		orphanCtx, cancel := context.WithTimeout(ctx, orphanPassTimeLimit)
		orphanCtx, span := tracing.Start(orphanCtx, "local_blobs.orphan_pass")
		files, bytes, err := b.removeOrphans(orphanCtx, now)
		cancel()
		span.SetAttributes(
			attribute.Int64("ingot.local_blobs.files", files),
			attribute.Int64("ingot.local_blobs.bytes", bytes))
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			// The pass's own time limit, logged below, not a failure.
			tracing.End(span, nil)
		} else {
			tracing.End(span, err)
		}
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
	// The stalled_bytes gauge reads this sum, so the metric callback never
	// queries the registry.
	if n, err := b.intents.StalledBytes(ctx, now.Add(-stalledUploadAge)); err != nil {
		errs = append(errs, fmt.Errorf("s3frontend: sum stalled uploads: %w", err))
	} else {
		b.stalledBytes.Store(n)
		b.stalledKnown.Store(true)
	}
	return stats, errors.Join(errs...)
}

// LocalBlobUsage returns the bytes the local blob directories hold, writes in
// progress included.
func (b *Backend) LocalBlobUsage() int64 {
	return b.localUsage()
}

// budgetPass is what one evictToBudget run did: the files it removed, their
// bytes, the rows it passed over as recently read, and why it stopped.
type budgetPass struct {
	files, bytes int64
	skipped      int64
	stop         passStop
}

// spanAttributes returns what the pass did as span attributes.
func (p budgetPass) spanAttributes() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int64("ingot.local_blobs.files", p.files),
		attribute.Int64("ingot.local_blobs.bytes", p.bytes),
		attribute.Int64("ingot.local_blobs.skipped", p.skipped),
		attribute.String("ingot.local_blobs.stop", p.stop.String()),
	}
}

// passStop is why an eviction pass stopped.
type passStop int

const (
	stopWatermark passStop = iota // usage reached the pass's target (the low watermark, or the budget for a forced stage)
	stopResidency                 // the next row is inside cache_min_residency
	stopExhausted                 // no evictable rows were left
	stopDeadline                  // the pass reached its time limit
)

func (s passStop) String() string {
	switch s {
	case stopWatermark:
		return "watermark"
	case stopResidency:
		return "residency"
	case stopExhausted:
		return "exhausted"
	case stopDeadline:
		return "deadline"
	}
	return "unknown"
}

// retention is which windows an eviction pass honours.
type retention int

const (
	honorBothWindows retention = iota // the budget pass
	honorReadWindow                   // the forced pass's first stage
	honorNoWindow                     // the forced pass's last stage
)

// forcedPass evicts inside the retention windows, once the budget pass has
// stopped at the residency window or run out of candidates having passed over
// recently read ones (budgetStop) with usage still over budget: a full disk
// fails every write, which costs more than reading a blob from the network. It
// gives up the windows in stages: first cache_min_residency, still passing over
// blobs read within cache_read_retention, so young unread blobs go before old
// hot ones; then, only if that is not enough, both. After a budget pass that
// ran out of candidates, the first stage would see the same rows, so it starts
// at the second; with no read window, the two stages are the same, so it runs
// only the first. It evicts only down to the budget, not to the low watermark,
// to give up as little of the windows as it can. It warns at most once an hour
// that it evicted inside the windows; the budget_forced removals count each
// time.
func (b *Backend) forcedPass(ctx context.Context, budgetStop passStop, stats *LocalBlobSweepStats) (err error) {
	forcedCtx, cancel := context.WithTimeout(ctx, forcedPassTimeLimit)
	defer cancel()
	forcedCtx, span := tracing.Start(forcedCtx, "local_blobs.forced_pass")
	defer func() {
		span.SetAttributes(
			attribute.Int64("ingot.local_blobs.files", stats.ForcedFiles),
			attribute.Int64("ingot.local_blobs.bytes", stats.ForcedBytes))
		tracing.End(span, err)
	}()
	deadline := time.Now().Add(forcedPassTimeLimit)
	var last budgetPass
	stages := []retention{honorReadWindow, honorNoWindow}
	switch {
	case budgetStop == stopExhausted:
		stages = stages[1:]
	case b.cacheReadRetention == 0:
		// With no read window, the last stage would see the same rows as
		// the first.
		stages = stages[:1]
	}
	for _, r := range stages {
		last, err = b.evictToBudget(forcedCtx, deadline, r)
		stats.ForcedFiles += last.files
		stats.ForcedBytes += last.bytes
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			err, last.stop = nil, stopDeadline
		}
		if err != nil || last.stop != stopExhausted {
			break
		}
	}
	b.localBlobMetrics.removed(ctx, removedBudgetForced, stats.ForcedFiles, stats.ForcedBytes)
	if stats.ForcedFiles > 0 && time.Since(b.lastForcedWarn) >= forcedWarnInterval {
		b.lastForcedWarn = time.Now()
		b.logger.Warn("local blob storage was over budget after the budget pass; evicted blobs inside the retention windows (cache_min_residency, cache_read_retention). This is logged at most once an hour; the budget_forced removals count each time",
			zap.Int64("files", stats.ForcedFiles),
			zap.Int64("bytes", stats.ForcedBytes),
			zap.Int64("budget", b.localBlobMaxBytes))
	}
	switch {
	case err != nil:
		return fmt.Errorf("s3frontend: local blob forced pass: %w", err)
	case b.localUsage() <= b.localBlobMaxBytes:
	case last.stop == stopExhausted:
		b.warnNothingLeft()
	case last.stop == stopDeadline:
		b.logTimeLimit("forced")
	}
	return nil
}

// warnNothingLeft warns, once per over-budget episode, that usage is over
// budget with nothing left to evict.
func (b *Backend) warnNothingLeft() {
	if b.overBudgetWarned {
		return
	}
	b.overBudgetWarned = true
	b.logger.Warn("local blob storage is over budget with nothing left to evict; this is logged once until usage falls back to the low watermark. The remaining files are bodies being written or uploaded, bodies whose upload failed (their intents stay spooled or uploading; nothing reclaims them yet), blobs with no recorded location, files that could not be removed, or orphans younger than local_blob_orphan_age",
		zap.Int64("usage", b.localUsage()),
		zap.Int64("spool", b.spool.Usage()),
		zap.Int64("budget", b.localBlobMaxBytes))
}

// logTimeLimit notes, once per over-budget episode, that an eviction pass
// ran out of time with usage still over budget.
func (b *Backend) logTimeLimit(pass string) {
	if b.timeLimitLogged {
		return
	}
	b.timeLimitLogged = true
	b.logger.Info("local blob "+pass+" pass reached its time limit while still over budget; the next sweep continues. This is logged once until usage falls back to the low watermark",
		zap.Int64("usage", b.localUsage()),
		zap.Int64("budget", b.localBlobMaxBytes))
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
// first, removing each file and marking its intent evicted, until usage
// reaches its target (the low watermark for the budget pass, the budget for a
// forced stage), the rows run out, or the deadline passes. r says which
// windows apply: the residency stop (honorBothWindows only) and the
// read-recency skip (all but honorNoWindow). A zero window is off. A file it
// cannot remove is skipped, so one bad file does not stop every later pass at
// the same row; its intent stays unmarked, and the pass logs the failures
// once. A row whose file was already gone is marked but not counted. Skipped
// recently read rows stay at the head of the order, so each pass pages past
// them again; the LRU's cap bounds how many there are.
func (b *Backend) evictToBudget(ctx context.Context, deadline time.Time, r retention) (pass budgetPass, err error) {
	var failed removeFailures
	if r == honorBothWindows {
		defer failed.log(b.logger, "budget")
	} else {
		defer failed.log(b.logger, "forced")
	}
	// The budget pass evicts to the low watermark, for headroom; a forced
	// stage only to the budget, giving up as little of the windows as it can.
	target := b.localBlobMaxBytes * lowWatermarkPercent / 100
	if r != honorBothWindows {
		target = b.localBlobMaxBytes
	}
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
			pass.stop = stopExhausted
			return pass, nil
		}
		for _, in := range page {
			if b.localUsage() <= target {
				return pass, nil
			}
			cursor = registry.EvictCursor{UpdatedAt: in.UpdatedAt, Digest: in.Digest}
			// UpdatedAt is the registry's clock (Postgres's now()), so
			// skew between the hosts shifts the residency window.
			if r == honorBothWindows && b.cacheMinResidency > 0 && now.Sub(in.UpdatedAt) < b.cacheMinResidency {
				pass.stop = stopResidency
				return pass, nil
			}
			if r != honorNoWindow && b.cacheReadRetention > 0 {
				if at, ok := b.cache.LastRead(in.Digest); ok && now.Sub(at) < b.cacheReadRetention {
					pass.skipped++
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
	if b.localUsage() > target {
		pass.stop = stopDeadline
	}
	return pass, nil
}

// RemoveReleasedPublished deletes the local copy and then the intent of every
// committed blob nothing names any more: one whose release finished while
// releases still kept both (registry.IntentStore.ListReleasedPublished). No
// release leaves such a blob now, so only a node upgraded from that version
// has any; the daemon still runs this at every startup, where on a clean node
// it scans the published intents and finds nothing. It pages through the
// candidates by digest, a batch at a time, with no time limit of its own. The
// file goes first, so the intent stays as the marker of a copy still to
// remove: a copy it cannot remove keeps its intent, and so does one whose
// removal a stop interrupted, and the next startup tries again. (Nothing else
// would find a cached copy without an intent.) The intent is then deleted only
// if it still qualifies (registry.IntentStore.DeleteReleasedPublished). A
// digest named again between the listing and the file's removal would lose its
// local copy; every write encrypts under a fresh key and so has a digest of its
// own, so that should not happen. The failures are logged once, naming the
// first, and only a failed query ends the pass. It runs beside the sweeper:
// the budget pass evicts only blobs with a location, which these have not,
// and a spool file the orphan pass reaches first is counted by whichever
// removal finds it.
func (b *Backend) RemoveReleasedPublished(ctx context.Context) (files, bytes int64, err error) {
	var failed removeFailures
	defer failed.log(b.logger, "released")
	defer func() { b.localBlobMetrics.removed(ctx, removedReleased, files, bytes) }()
	batch := b.localBlobSweepBatch
	if batch <= 0 {
		batch = localBlobSweepBatch
	}
	var after multihash.Multihash
	for {
		page, err := b.intents.ListReleasedPublished(ctx, after, batch)
		if err != nil {
			return files, bytes, err
		}
		for _, in := range page {
			after = in.Digest
			freed, err := b.removeLocal(in.Digest)
			if freed > 0 {
				files++
				bytes += freed
			}
			if err != nil {
				failed.add(hex.EncodeToString(in.Digest), err)
				continue
			}
			if _, err := b.intents.DeleteReleasedPublished(ctx, in.Digest); err != nil {
				return files, bytes, fmt.Errorf("delete released intent %s: %w", hex.EncodeToString(in.Digest), err)
			}
		}
		if len(page) < batch {
			return files, bytes, nil
		}
	}
}

// removeOrphans deletes the files no intent-driven cleanup can find, once they
// are older than the orphan age: .tmp-* files from a write that never finished,
// in the spool and the cache, and spool blob files with no intent row (a split
// that failed before its intents were recorded). The age must exceed the
// longest time one request body takes to stream, because a request records its
// intents only after its whole body is spooled. The scans take no lock, so
// writes and moves go on meanwhile; a file found in the spool may have moved to
// the cache by the time it is checked, which removeLocal handles. A scan that
// nothing in this process overlapped also corrects its directory's byte count,
// which only a file added or removed outside ingot can throw off, and logs the
// correction. It finishes the spool, which is small and where orphans arise,
// before it scans the cache, so a slow cache cannot hold up the spool's
// cleanup. It checks the spool's old blobs a batch at a time as the scan
// reaches them, so its memory stays small. It does not check the cache's blobs
// against intents: a file enters the cache only after its intent exists, and
// every path that deletes an intent removes the file first, so a cache orphan
// can only come from outside ingot, and checking every cached file each hour
// would cost a query per file of a large cache. The cache scan removes old temp
// files and corrects the count. A file it cannot remove is skipped and the
// failures are logged once, so one bad file does not stop the pass; only a
// failed scan or query does. Ages are measured from now, the sweep's time.
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
		checkBlobs bool
	}{
		{"spool", b.spool.ScanAndCorrect, b.spool.RemoveTemp, true},
		{"cache", b.cache.ScanAndCorrect, b.cache.RemoveTemp, false},
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
			if !dir.checkBlobs {
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
