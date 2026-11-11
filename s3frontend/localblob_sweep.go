package s3frontend

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/tracing"
)

const (
	// lowWatermarkPercent is how far below local_blob_max_bytes a budget pass
	// evicts, so the next sweep does not start over budget again at once.
	// The headroom must exceed ingest rate × the sweep interval.
	lowWatermarkPercent = 90
	// orphanPassTimeLimit caps the orphan pass separately, so a long
	// budget pass cannot starve it.
	orphanPassTimeLimit = 2 * time.Minute
	// orphanPassInterval spaces the orphan pass, which scans the whole cache
	// directory and so stays off the per-sweep path.
	orphanPassInterval = time.Hour
	// stalledUploadAge is how long a body may wait for upload, since its
	// intent last changed state, before the stalled_bytes gauge counts it:
	// well past the longest a live upload takes.
	stalledUploadAge = time.Hour
	// DefaultLocalBlobOrphanAge is the age at which a .tmp-* file is deleted,
	// when Deps.LocalBlobOrphanAge is zero.
	DefaultLocalBlobOrphanAge = 24 * time.Hour
)

// budgetPassTimeLimit caps the budget pass's removals, counted from the end of
// its scan, so the orphan pass that may follow it still runs within the
// sweep's time. The scan is not counted: one cut short would leave the pass
// nothing to remove, and the next would scan again. Package-level so tests can
// shrink it.
var budgetPassTimeLimit = 30 * time.Second

// LocalBlobSweepStats counts what one SweepLocalBlobs run removed, per pass.
type LocalBlobSweepStats struct {
	BudgetFiles, BudgetBytes int64
	OrphanFiles, OrphanBytes int64
}

// Removed reports whether the run removed anything.
func (s LocalBlobSweepStats) Removed() bool {
	return s.BudgetFiles+s.OrphanFiles > 0
}

// Add adds o's counts to s, to total several runs.
func (s *LocalBlobSweepStats) Add(o LocalBlobSweepStats) {
	s.BudgetFiles += o.BudgetFiles
	s.BudgetBytes += o.BudgetBytes
	s.OrphanFiles += o.OrphanFiles
	s.OrphanBytes += o.OrphanBytes
}

// SpanAttributes returns the counts as span attributes.
func (s LocalBlobSweepStats) SpanAttributes() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int64("ingot.sweep.budget_files", s.BudgetFiles),
		attribute.Int64("ingot.sweep.budget_bytes", s.BudgetBytes),
		attribute.Int64("ingot.sweep.orphan_files", s.OrphanFiles),
		attribute.Int64("ingot.sweep.orphan_bytes", s.OrphanBytes),
	}
}

// LogFields returns the counts as log fields.
func (s LocalBlobSweepStats) LogFields() []zap.Field {
	return []zap.Field{
		zap.Int64("budget_files", s.BudgetFiles),
		zap.Int64("budget_bytes", s.BudgetBytes),
		zap.Int64("orphan_files", s.OrphanFiles),
		zap.Int64("orphan_bytes", s.OrphanBytes),
	}
}

// SweepLocalBlobs bounds the cache. With a budget set (Deps.LocalBlobMaxBytes)
// and its usage over it, the budget pass evicts cached blobs, oldest first by
// when their time in the cache was last renewed (the file's modification
// time), down to 90% of the budget, within a time limit. A pass that runs out
// of time leaves the rest to the next sweep. It logs and skips a file it
// cannot remove. If it runs out of cached blobs with usage still over budget,
// the rest is writes in progress and young orphans, which no rule lets it
// remove, and it logs a warning once until usage falls back to the low
// watermark. The counts see a file removed outside ingot at the next restart,
// or at the next orphan pass that runs while the directory is quiet.
//
// A cached blob's own eviction, when its time in the cache runs out, is a
// queue-shard.evict task (see queue/shardqueue); the budget pass bounds the disk
// besides, since a burst of writes can fill it first. It evicts through the
// same action, run as a sync task, so a removal takes its turn with the
// digest's other tasks; the eviction due later finds the file gone. Readers
// tolerate the unlink: an open file survives it, and a local miss falls
// through to the network tier.
//
// On the first run and then hourly, with or without a budget, the orphan pass
// deletes .tmp-* files once they are older than LocalBlobOrphanAge. It runs
// even when the budget pass fails, with a time limit of its own, and counts as run whether or
// not it finishes, so a slow or failing pass waits for the next hour rather
// than starting over every sweep.
//
// Every run also sums the bodies whose upload has stalled: intents still
// uploading an hour after their last state change, which no live request
// holds that long. The stalled_bytes gauge reports the sum.
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
	if b.localBlobMaxBytes > 0 && b.cache.Usage() > b.localBlobMaxBytes {
		budgetCtx, span := tracing.Start(ctx, "local_blobs.budget_pass")
		pass, err := b.evictToBudget(budgetCtx, budgetPassTimeLimit)
		stats.BudgetFiles, stats.BudgetBytes = pass.files, pass.bytes
		b.localBlobMetrics.removed(ctx, removedBudget, pass.files, pass.bytes)
		// A removal cut off by the pass's own time limit is the time limit,
		// not a failure.
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			err, pass.stop = nil, stopDeadline
		}
		span.SetAttributes(pass.spanAttributes()...)
		endPassSpan(ctx, span, err)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("s3frontend: local blob budget pass: %w", err))
		case b.cache.Usage() <= b.localBlobMaxBytes:
		case pass.stop == stopDeadline:
			b.logTimeLimit()
		case pass.stop == stopExhausted:
			b.warnNothingLeft()
		}
	}
	// The latches reset at the low watermark, where the budget pass stops, so
	// usage hovering at the budget does not reset them every sweep.
	if b.cache.Usage() <= b.localBlobMaxBytes*lowWatermarkPercent/100 {
		b.overBudgetWarned = false
		b.timeLimitLogged = false
	}
	if b.lastOrphanPass.IsZero() || now.Sub(b.lastOrphanPass) >= orphanPassInterval {
		orphanCtx, cancel := context.WithTimeout(ctx, orphanPassTimeLimit)
		orphanCtx, span := tracing.Start(orphanCtx, "local_blobs.orphan_pass")
		files, bytes, err := b.removeOrphans(orphanCtx, now)
		cancel()
		span.SetAttributes(
			attribute.Int64("ingot.sweep.files", files),
			attribute.Int64("ingot.sweep.bytes", bytes))
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			// The pass's own time limit, logged below, not a failure.
			endPassSpan(ctx, span, nil)
		} else {
			endPassSpan(ctx, span, err)
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
	if s, err := b.intents.StalledBytes(ctx, now.Add(-stalledUploadAge)); err != nil {
		errs = append(errs, fmt.Errorf("s3frontend: sum stalled uploads: %w", err))
	} else {
		b.stalledUploading.Store(s)
		b.stalledKnown.Store(true)
	}
	return stats, errors.Join(errs...)
}

// LocalBlobUsage returns the bytes the cache holds, writes in progress
// included.
func (b *Backend) LocalBlobUsage() int64 {
	return b.cache.Usage()
}

// budgetPass is what one evictToBudget run did: the files it removed, their
// bytes, and why it stopped.
type budgetPass struct {
	files, bytes int64
	stop         passStop
}

// endPassSpan ends a pass's span. Once the sweep's own ctx is done (the
// daemon is stopping), the pass's error is the cancellation's, so it does not
// mark the span failed.
func endPassSpan(ctx context.Context, span trace.Span, err error) {
	if ctx.Err() != nil {
		err = nil
	}
	tracing.End(span, err)
}

// spanAttributes returns what the pass did as span attributes.
func (p budgetPass) spanAttributes() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int64("ingot.sweep.files", p.files),
		attribute.Int64("ingot.sweep.bytes", p.bytes),
		attribute.String("ingot.sweep.stop", p.stop.String()),
	}
}

// passStop is why an eviction pass stopped.
type passStop int

const (
	stopWatermark passStop = iota // usage reached the low watermark
	stopExhausted                 // no cached blobs were left
	stopDeadline                  // the pass reached its time limit
)

func (s passStop) String() string {
	switch s {
	case stopWatermark:
		return "watermark"
	case stopExhausted:
		return "exhausted"
	case stopDeadline:
		return "deadline"
	}
	return "unknown"
}

// warnNothingLeft warns, once per over-budget episode, that usage is over
// budget with nothing left to evict.
func (b *Backend) warnNothingLeft() {
	if b.overBudgetWarned {
		return
	}
	b.overBudgetWarned = true
	b.logger.Warn("local blob storage is over budget with nothing left to evict; this is logged once until usage falls back to the low watermark. The remaining bytes are bodies being written, files that could not be removed, or orphans younger than local_blob_orphan_age",
		zap.Int64("usage", b.cache.Usage()),
		zap.Int64("in_flight", b.cache.InFlight()),
		zap.Int64("budget", b.localBlobMaxBytes))
}

// logTimeLimit notes, once per over-budget episode, that the budget pass ran
// out of time with usage still over budget.
func (b *Backend) logTimeLimit() {
	if b.timeLimitLogged {
		return
	}
	b.timeLimitLogged = true
	b.logger.Info("local blob budget pass reached its time limit while still over budget; the next sweep continues. This is logged once until usage falls back to the low watermark",
		zap.Int64("usage", b.cache.Usage()),
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

// evictToBudget evicts cached blobs (shardqueue.Queue.Evict), oldest modification time
// first, until usage reaches the low watermark, the cache runs out, or limit
// has passed since it began removing. It reads the whole cache directory
// first, to order it. A file it cannot remove is skipped, and the pass logs
// the failures once; a file already gone is not counted. Once ctx is done it
// stops with ctx's error.
func (b *Backend) evictToBudget(ctx context.Context, limit time.Duration) (pass budgetPass, err error) {
	var failed removeFailures
	defer failed.log(b.logger, "budget")
	target := b.localBlobMaxBytes * lowWatermarkPercent / 100
	var files []blockstore.BlobFile
	if err := b.cache.Scan(ctx, func(f blockstore.BlobFile) {
		if f.Digest != nil {
			files = append(files, f)
		}
	}); err != nil {
		return pass, err
	}
	slices.SortFunc(files, func(x, y blockstore.BlobFile) int { return x.ModTime.Compare(y.ModTime) })
	deadline := time.Now().Add(limit)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for _, f := range files {
		if b.cache.Usage() <= target {
			return pass, nil
		}
		if !time.Now().Before(deadline) {
			pass.stop = stopDeadline
			return pass, nil
		}
		freed, err := b.shards.Evict(ctx, f.Digest)
		if freed > 0 {
			pass.files++
			pass.bytes += freed
		}
		if err != nil {
			if ctx.Err() != nil {
				return pass, ctx.Err()
			}
			failed.add(hex.EncodeToString(f.Digest), err)
		}
	}
	if b.cache.Usage() > target {
		pass.stop = stopExhausted
	}
	return pass, nil
}

// removeOrphans deletes the .tmp-* files from writes that never finished,
// once they are older than the orphan age, which must exceed the longest time
// one request body takes to stream. It does not check the cache's blobs
// against intents: a file enters the cache only after its intent is accepted,
// so a cache orphan can only come from outside ingot, and checking every
// cached file each hour would cost a query per file of a large cache. A scan
// that nothing in this process overlapped also corrects the cache's byte
// count, which only a file added or removed outside ingot can throw off, and
// logs the correction. A file it cannot remove is skipped and the failures are
// logged once, so one bad file does not stop the pass; only a failed scan
// does. Ages are measured from now, the sweep's time.
func (b *Backend) removeOrphans(ctx context.Context, now time.Time) (files, bytes int64, err error) {
	var failed removeFailures
	defer failed.log(b.logger, "orphan")
	cutoff := now.Add(-b.localBlobOrphanAge)
	drift, err := b.cache.ScanAndCorrect(ctx, func(f blockstore.BlobFile) {
		if f.Digest != nil || !f.ModTime.Before(cutoff) {
			return
		}
		freed, err := b.cache.RemoveTemp(f.Name)
		if freed > 0 {
			files++
			bytes += freed
		}
		if err != nil {
			failed.add(f.Name, err)
		}
	})
	if err != nil {
		return files, bytes, err
	}
	if drift != 0 {
		b.logger.Warn("local blob cache byte count corrected: files were added or removed outside ingot",
			zap.Int64("counted_minus_on_disk", drift))
	}
	return files, bytes, nil
}
