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
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
)

// PlaneLog is one plane's independent LSM pipeline: one open segment
// accepting appends, plus N sealed segments (some shipped, some pending)
// that serve reads. It seals on its own threshold, ships through its own
// transport, and retains on its own window — there is no coordination
// with the other plane. A plane configured never to ship retains every
// sealed CAR on local disk forever.
//
// Concurrency:
//   - catMu (RWMutex) guards open + sealed slice.
//   - appMu (Mutex) serializes appenders so the open-segment append fd
//     has a single writer.
type PlaneLog struct {
	plane  blockstore.Plane
	dir    string
	pc     PlaneConfig
	meta   Meta
	logger *zap.Logger

	catMu  sync.RWMutex
	open   *Segment
	sealed []*Segment // newest-first; includes shipped-and-retained

	appMu sync.Mutex

	// flushQ delivers sealed segments to the flush worker. nil (and no
	// worker) when the plane is configured not to ship.
	flushQ  chan *Segment
	closing chan struct{}
	wg      sync.WaitGroup

	openedAt time.Time

	// sealReq is a coalesced "seal the open segment now" signal.
	sealReq chan struct{}
}

// openPlaneLog initializes one plane's pipeline: scans its subdir,
// reconciles with Meta, re-enqueues unshipped sealed segments, force-seals
// any previously-open segment, and starts the flush worker (when shipping)
// + seal ticker.
func openPlaneLog(ctx context.Context, plane blockstore.Plane, dir string, pc PlaneConfig, meta Meta, logger *zap.Logger) (*PlaneLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("logstore: mkdir %s: %w", dir, err)
	}
	pl := &PlaneLog{
		plane:   plane,
		dir:     dir,
		pc:      pc,
		meta:    meta,
		logger:  logger,
		closing: make(chan struct{}),
		sealReq: make(chan struct{}, 1),
	}
	if pc.Ship {
		pl.flushQ = make(chan *Segment, 64)
	}

	if err := pl.recover(ctx); err != nil {
		return nil, err
	}

	// Force-seal a recovered open segment so a fresh open is always
	// brand-new on each process startup.
	if pl.open != nil {
		if err := pl.open.seal(ctx, meta); err != nil {
			return nil, fmt.Errorf("logstore: force-seal recovered open %s segment: %w", plane, err)
		}
		pl.sealed = append([]*Segment{pl.open}, pl.sealed...)
		pl.enqueueFlush(pl.open)
		pl.open = nil
	}

	if pc.Ship {
		pl.wg.Add(1)
		go pl.flushLoop()
	}
	pl.wg.Add(1)
	go pl.sealTickerLoop()

	return pl, nil
}

// Append persists one batch's blocks to the open segment. opRoots is
// non-empty only for the catalog plane (the bucket-root advances it
// records). A call with no blocks AND no op-roots is a no-op — it does
// not even create a segment (an MST-only S3 op writes no data blocks, so
// the data plane stays dormant). The CAR is fsynced before Append
// returns.
func (pl *PlaneLog) Append(ctx context.Context, blocks []block.Block, opRoots ...blockstore.OpRoot) error {
	if len(blocks) == 0 && len(opRoots) == 0 {
		return nil
	}

	pl.appMu.Lock()
	defer pl.appMu.Unlock()

	open, err := pl.ensureOpenLockedAppMu(ctx)
	if err != nil {
		return err
	}
	if err := open.append(blocks, opRoots); err != nil {
		return err
	}
	if open.Size() >= pl.pc.SealBytes {
		pl.requestSeal()
	}
	return nil
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

// Close seals the open segment, drains the flush queue, and stops
// background goroutines. Safe to call once.
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
	// (recovery re-enqueues it for ship next start).
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
			pl.enqueueFlush(open)
		}
	}
	pl.appMu.Unlock()

	if pl.flushQ != nil {
		close(pl.flushQ)
	}
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

// enqueueFlush hands a sealed segment to the flush worker. No-op for a
// non-shipping plane. Non-blocking: a full queue is logged and retried on
// the next restart.
func (pl *PlaneLog) enqueueFlush(seg *Segment) {
	if pl.flushQ == nil {
		return
	}
	select {
	case pl.flushQ <- seg:
	default:
		pl.logger.Warn("logstore: flush queue full; segment will retry on restart",
			zap.Stringer("plane", pl.plane), zap.Uint64("seq", seg.Seq()))
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
	seg, err := createOpenSegment(ctx, pl.dir, seq, pl.plane, pl.meta, pl.logger)
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
// (over SealAge or SealBytes), or force is set, then enqueues it.
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

	if err := open.seal(ctx, pl.meta); err != nil {
		return err
	}

	pl.catMu.Lock()
	if pl.open == open {
		pl.open = nil
		pl.sealed = append([]*Segment{open}, pl.sealed...)
	}
	pl.catMu.Unlock()

	pl.enqueueFlush(open)
	return nil
}

// flushLoop drains the queue, shipping each sealed segment. Exits on close.
func (pl *PlaneLog) flushLoop() {
	defer pl.wg.Done()
	for {
		select {
		case <-pl.closing:
			return
		case seg, ok := <-pl.flushQ:
			if !ok {
				return
			}
			pl.flushOne(seg)
		}
	}
}

// flushOne ships one sealed segment, retrying with backoff. On success it
// stamps the ship state (in memory + in Meta — which advances
// forge_root_cid for the catalog plane) and runs the retention sweep.
func (pl *PlaneLog) flushOne(seg *Segment) {
	ctx := context.Background()
	const maxAttempts = 5
	backoff := time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := pl.pc.Flush(ctx, seg)
		if err == nil {
			now := time.Now().Unix()
			seg.markShipped(now)
			if merr := pl.meta.MarkSegmentShipped(ctx, pl.plane, seg.Seq(), now, seg.OpRoots()); merr != nil {
				pl.logger.Error("logstore: mark shipped",
					zap.Stringer("plane", pl.plane), zap.Uint64("seq", seg.Seq()), zap.Error(merr))
			}
			pl.runRetention(ctx)
			return
		}
		pl.logger.Warn("logstore: ship attempt failed",
			zap.Stringer("plane", pl.plane), zap.Uint64("seq", seg.Seq()),
			zap.Int("attempt", attempt), zap.Error(err))
		select {
		case <-pl.closing:
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
	pl.logger.Error("logstore: ship exhausted retries; segment remains unshipped",
		zap.Stringer("plane", pl.plane), zap.Uint64("seq", seg.Seq()))
}

// runRetention retires shipped segments beyond the Retain window and drops
// them off the read tier + DB. A non-shipping plane is never retired.
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

	for _, seg := range remove {
		if err := pl.meta.DeleteSegment(ctx, pl.plane, seg.Seq()); err != nil {
			pl.logger.Warn("logstore: delete segment row",
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
