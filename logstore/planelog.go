package logstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/tracing"
)

// PlaneLog is one plane's independent LSM pipeline: one open segment
// accepting appends, plus N sealed segments (some shipped, some pending)
// that serve reads. It seals on its own threshold and retains on its own
// window, and ships its sealed segments when its owner calls Ship: it tells
// the owner through OnSealed when there is one to ship. A plane configured
// never to ship retains every sealed CAR on local disk forever.
//
// Concurrency:
//   - catMu (RWMutex) guards open + sealed slice.
//   - appMu (Mutex) serializes appenders so the open-segment append fd
//     has a single writer.
type PlaneLog struct {
	plane  blockstore.Plane
	bucket string // the bucket this log belongs to; stamps segment rows
	dir    string
	pc     PlaneConfig
	meta   Meta
	logger *zap.Logger

	catMu  sync.RWMutex
	open   *Segment
	sealed []*Segment // newest-first; includes shipped-and-retained

	appMu sync.Mutex
	// shipMu serializes Ship calls, so one segment is never shipped twice
	// at once.
	shipMu sync.Mutex

	closing chan struct{}
	wg      sync.WaitGroup

	openedAt time.Time

	// sealReq is a coalesced "seal the open segment now" signal.
	sealReq chan struct{}
}

// openPlaneLog initializes one plane's pipeline: scans its subdir,
// reconciles with Meta, force-seals any previously-open segment, tells the
// owner of any sealed segment left unshipped, and starts the seal ticker.
func openPlaneLog(ctx context.Context, plane blockstore.Plane, bucket, dir string, pc PlaneConfig, meta Meta, logger *zap.Logger) (*PlaneLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("logstore: mkdir %s: %w", dir, err)
	}
	pl := &PlaneLog{
		plane:   plane,
		bucket:  bucket,
		dir:     dir,
		pc:      pc,
		meta:    meta,
		logger:  logger,
		closing: make(chan struct{}),
		sealReq: make(chan struct{}, 1),
	}
	if err := pl.recover(ctx); err != nil {
		return nil, err
	}

	for _, seg := range pl.sealed {
		if !seg.IsShipped() && !seg.IsRetired() {
			pl.notifySealed()
			break
		}
	}

	pl.wg.Add(1)
	go pl.sealTickerLoop()

	return pl, nil
}

// Append persists one batch's blocks to the open segment and returns that
// segment's seq. opRoots are the bucket-root advances the batch records. A
// call with no blocks AND no op-roots is a no-op — it does not even create a
// segment, and returns seq 0. The CAR is fsynced before Append returns.
func (pl *PlaneLog) Append(ctx context.Context, blocks []block.Block, opRoots ...blockstore.OpRoot) (seq uint64, err error) {
	if len(blocks) == 0 && len(opRoots) == 0 {
		return 0, nil
	}
	// The span covers the wait for the append lock and the segment write and
	// fsync every acked write waits on.
	var size int
	for _, b := range blocks {
		size += len(b.RawData())
	}
	ctx, span := tracing.Start(ctx, "log.append",
		attribute.Int("ingot.log.blocks", len(blocks)),
		attribute.Int("ingot.log.bytes", size),
	)
	defer func() { tracing.End(span, err) }()

	pl.appMu.Lock()
	defer pl.appMu.Unlock()

	open, err := pl.ensureOpenLockedAppMu(ctx)
	if err != nil {
		return 0, err
	}
	if err := open.append(blocks, opRoots); err != nil {
		return 0, err
	}
	if open.Size() >= pl.pc.SealBytes {
		pl.requestSeal()
	}
	return open.Seq(), nil
}

// Get returns the block from this plane's local segments (open first,
// then sealed newest-first) or blockstore.ErrNotFound.
func (pl *PlaneLog) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	pl.catMu.RLock()
	open := pl.open
	sealed := make([]*Segment, len(pl.sealed))
	copy(sealed, pl.sealed)
	pl.catMu.RUnlock()

	if open != nil {
		if blk, err := open.get(ctx, c); err == nil {
			return blk, nil
		} else if !errors.Is(err, blockstore.ErrNotFound) {
			return nil, err
		}
	}
	for _, seg := range sealed {
		blk, err := seg.get(ctx, c)
		if err == nil {
			return blk, nil
		}
		if !errors.Is(err, blockstore.ErrNotFound) {
			return nil, err
		}
	}
	return nil, blockstore.ErrNotFound
}

// Close seals the open segment and stops the seal ticker. A segment left
// unshipped ships when the log is next opened and its owner calls Ship.
// Safe to call once.
func (pl *PlaneLog) Close(ctx context.Context) error {
	pl.catMu.Lock()
	select {
	case <-pl.closing:
		pl.catMu.Unlock()
		return nil
	default:
		close(pl.closing)
	}
	pl.catMu.Unlock()

	// Force-seal the open segment so anything still buffered is durable
	// (the next open reports it to ship).
	pl.appMu.Lock()
	pl.catMu.Lock()
	open := pl.open
	pl.open = nil
	pl.catMu.Unlock()
	if open != nil {
		if err := open.seal(ctx, pl.meta); err != nil {
			pl.logger.Error("logstore: seal at close", zap.Stringer("plane", pl.plane), zap.Error(err))
		} else {
			pl.catMu.Lock()
			pl.sealed = append([]*Segment{open}, pl.sealed...)
			pl.catMu.Unlock()
		}
	}
	pl.appMu.Unlock()

	pl.wg.Wait()
	return nil
}

// requestSeal coalesces seal triggers.
func (pl *PlaneLog) requestSeal() {
	select {
	case pl.sealReq <- struct{}{}:
	default:
	}
}

// notifySealed tells the owner there is a sealed segment to ship. No-op
// for a non-shipping plane.
func (pl *PlaneLog) notifySealed() {
	if pl.pc.Ship && pl.pc.OnSealed != nil {
		pl.pc.OnSealed()
	}
}

// ensureOpenLockedAppMu returns the current open segment, creating a
// fresh one if none exists. Caller must hold appMu.
func (pl *PlaneLog) ensureOpenLockedAppMu(ctx context.Context) (*Segment, error) {
	pl.catMu.RLock()
	open := pl.open
	pl.catMu.RUnlock()
	if open != nil {
		return open, nil
	}

	seq, err := pl.meta.NextSegmentSeq(ctx)
	if err != nil {
		return nil, err
	}
	seg, err := createOpenSegment(ctx, pl.dir, seq, pl.plane, pl.bucket, pl.meta, pl.logger)
	if err != nil {
		return nil, err
	}

	pl.catMu.Lock()
	if pl.open == nil {
		pl.open = seg
		pl.openedAt = time.Now()
		pl.catMu.Unlock()
		return seg, nil
	}
	pl.catMu.Unlock()
	if err := seg.retireOpen(); err != nil {
		pl.logger.Warn("logstore: retire raced new segment", zap.Error(err))
	}
	if err := pl.meta.DeleteSegment(ctx, pl.plane, seq); err != nil {
		pl.logger.Warn("logstore: delete raced new segment row", zap.Error(err))
	}
	pl.catMu.RLock()
	open = pl.open
	pl.catMu.RUnlock()
	return open, nil
}

// sealOpenIfDue seals the current open segment if one exists and is due
// (over SealAge or SealBytes), or force is set, then reports it to ship. A
// segment that froze but whose seal didn't record is logged and reported too,
// and returns nil: it has moved on, and its ship or the next start records it.
func (pl *PlaneLog) sealOpenIfDue(ctx context.Context, force bool) error {
	pl.appMu.Lock()
	defer pl.appMu.Unlock()

	pl.catMu.RLock()
	open := pl.open
	openedAt := pl.openedAt
	pl.catMu.RUnlock()
	if open == nil {
		return nil
	}
	if !force {
		if open.Size() < pl.pc.SealBytes && time.Since(openedAt) < pl.pc.SealAge {
			return nil
		}
	}

	sealCtx, span := tracing.Start(ctx, "log.seal",
		attribute.String("ingot.log.plane", pl.plane.String()),
		attribute.Int64("ingot.log.segment_bytes", open.Size()),
	)
	err := open.seal(sealCtx, pl.meta)
	tracing.End(span, err)
	if err != nil {
		if open.State() == StateOpen {
			return err // still open: the next tick tries again
		}
		// A segment that froze but whose seal wasn't recorded moves on too,
		// so writes go to a new segment. Its ship records the seal first.
		pl.logger.Warn("logstore: seal not recorded; its ship or the next start records it",
			zap.Stringer("plane", pl.plane), zap.Uint64("seq", open.Seq()), zap.Error(err))
	}

	pl.catMu.Lock()
	if pl.open == open {
		pl.open = nil
		pl.sealed = append([]*Segment{open}, pl.sealed...)
	}
	pl.catMu.Unlock()

	pl.notifySealed()
	return nil
}

// Ship ships the plane's sealed segments that are still unshipped, oldest
// first, and stops at the first that fails, returning its error: whatever
// failed it (the upload service, the space's ship authority, the stamp in
// Meta) most likely fails the rest, and the caller's retry starts again from
// it. Segments sealed while it runs are shipped by the next call.
func (pl *PlaneLog) Ship(ctx context.Context) error {
	if !pl.pc.Ship {
		return nil
	}
	pl.shipMu.Lock()
	defer pl.shipMu.Unlock()
	pl.catMu.RLock()
	var pending []*Segment
	for i := len(pl.sealed) - 1; i >= 0; i-- { // pl.sealed is newest first
		if seg := pl.sealed[i]; !seg.IsShipped() && !seg.IsRetired() {
			pending = append(pending, seg)
		}
	}
	pl.catMu.RUnlock()
	for _, seg := range pending {
		if err := pl.shipOne(ctx, seg); err != nil {
			return err
		}
	}
	return nil
}

// shipOne ships one sealed segment. On success it stamps the ship state in
// Meta, with the segment's dead-on-arrival bytes (the stamp also advances
// forge_root_cid for the catalog plane and drops the segment's journaled
// blocks), then in memory, and runs the retention sweep. The segment counts
// as shipped only once its stamp lands: retention retires shipped segments,
// and a retired segment whose stamp never landed would leave its journal
// rows behind for good. A failed stamp leaves the segment for the next Ship,
// which stamps it again without shipping it again; a failed Flush leaves it
// to be shipped again, whose blob/add dedups on the provider.
func (pl *PlaneLog) shipOne(ctx context.Context, seg *Segment) (err error) {
	if seg.IsShipped() || seg.IsRetired() {
		return nil
	}
	ctx, span := tracing.Start(ctx, "log.ship",
		attribute.String("ingot.log.plane", pl.plane.String()),
		attribute.Int64("ingot.log.segment_bytes", seg.Size()),
	)
	defer func() { tracing.End(span, err) }()

	// What the segment holds that is already dead is measured from its local
	// files before it ships, and recorded with the stamp; a failure only
	// leaves it unmeasured.
	deadBytes := seg.deadOnArrival()
	span.SetAttributes(attribute.Int64("ingot.log.dead_bytes", deadBytes))

	if !seg.flushed {
		// Record a seal that failed to record before shipping: a ship would
		// stamp a row still open, with no sha256 for a release to find. A
		// no-op once recorded.
		if err := seg.seal(ctx, pl.meta); err != nil {
			return fmt.Errorf("logstore: record %s segment %d seal: %w", pl.plane, seg.Seq(), err)
		}
		indexDigest, err := pl.pc.Flush(ctx, seg)
		if err != nil {
			return fmt.Errorf("logstore: ship %s segment %d: %w", pl.plane, seg.Seq(), err)
		}
		seg.flushed, seg.flushedIndex, seg.flushedAt = true, indexDigest, time.Now().Unix()
	}
	shippedAt := seg.flushedAt
	if err := pl.meta.MarkSegmentShipped(ctx, pl.plane, seg.Seq(), shippedAt, seg.flushedIndex, deadBytes, seg.OpRoots()); err != nil {
		// The segment is on Forge; only its stamp in Meta failed.
		return fmt.Errorf("logstore: mark %s segment %d shipped: %w", pl.plane, seg.Seq(), err)
	}
	seg.markShipped(shippedAt)
	pl.runRetention(ctx)
	return nil
}

// runRetention retires shipped segments beyond the Retain window: unlinks
// their files, drops them off the read tier, and stamps their rows retired.
// The rows stay as the record of what each segment registered in the
// bucket's space. A non-shipping plane is never retired.
func (pl *PlaneLog) runRetention(ctx context.Context) {
	if !pl.pc.Ship {
		return
	}

	pl.catMu.RLock()
	sealed := make([]*Segment, len(pl.sealed))
	copy(sealed, pl.sealed)
	pl.catMu.RUnlock()

	var toRetire []*Segment
	shippedSeen := 0
	for _, seg := range sealed {
		if !seg.IsShipped() || seg.IsRetired() {
			continue
		}
		shippedSeen++
		if shippedSeen <= pl.pc.Retain {
			continue
		}
		toRetire = append(toRetire, seg)
	}

	for _, seg := range toRetire {
		if err := seg.retire(); err != nil {
			pl.logger.Warn("logstore: retire",
				zap.Stringer("plane", pl.plane), zap.Uint64("seq", seg.Seq()), zap.Error(err))
		}
	}

	pl.catMu.Lock()
	var keep, remove []*Segment
	for _, seg := range pl.sealed {
		if seg.IsRetired() {
			remove = append(remove, seg)
		} else {
			keep = append(keep, seg)
		}
	}
	pl.sealed = keep
	pl.catMu.Unlock()

	now := time.Now().Unix()
	for _, seg := range remove {
		if err := pl.meta.RetireSegment(ctx, pl.plane, seg.Seq(), now); err != nil {
			pl.logger.Warn("logstore: retire segment row",
				zap.Stringer("plane", pl.plane), zap.Uint64("seq", seg.Seq()), zap.Error(err))
		}
	}
}

// sealTickerLoop wakes periodically and seals the open segment when due,
// and services explicit seal requests.
func (pl *PlaneLog) sealTickerLoop() {
	defer pl.wg.Done()
	interval := pl.pc.SealAge / 4
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-pl.closing:
			return
		case <-t.C:
			if err := pl.sealOpenIfDue(context.Background(), false); err != nil {
				pl.logger.Warn("logstore: tick seal", zap.Stringer("plane", pl.plane), zap.Error(err))
			}
		case <-pl.sealReq:
			if err := pl.sealOpenIfDue(context.Background(), false); err != nil {
				pl.logger.Warn("logstore: req seal", zap.Stringer("plane", pl.plane), zap.Error(err))
			}
		}
	}
}
