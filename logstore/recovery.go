package logstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
)

// recover scans this plane's subdirectory and the Meta backend, rebuilds
// the sealed segment list, picks the in-flight open segment (if any), and
// re-enqueues anything still pending ship. Discovery keys on the CAR
// file. Reconciliation per seq:
//   - row sealed  → load from .idx; re-enqueue when shipping & not shipped.
//   - row open or no row, .idx present → the segment finished sealing but
//     Meta was restored from behind the disk: load from .idx, bring the row
//     up to it (MarkSegmentSealed, or RehydrateSegment when missing),
//     re-enqueue (a re-ship of what already shipped is a no-op).
//   - row open or no row, no (readable) .idx → open at the crash: rebuild
//     from the CAR, seeding the row if missing, then force-seal. Every such
//     segment is force-sealed, so each start opens a brand-new one.
//   - no unretired row, but a retired one → files left by an interrupted
//     retire; unlink them, keep the row.
//   - row sealed, no CAR → retire the row: a crash between retention's
//     unlink and its retired stamp, or a lost file. The row stays the
//     record of what the segment may have registered in the space.
//   - row open, no CAR → delete the row.
//   - sidecar (.idx/.ops) with no CAR → stray; unlink.
//
// Before the fileless rows go, rebuildFromJournal writes the blocks the
// catalog journal still holds for any segment whose CAR is gone into new
// segments, which re-enqueue with the rest.
func (pl *PlaneLog) recover(ctx context.Context) error {
	rows, err := pl.meta.ListSegments(ctx, pl.plane, pl.bucket)
	if err != nil {
		return fmt.Errorf("logstore: list %s segments for %q: %w", pl.plane, pl.bucket, err)
	}
	dbBySeq := make(map[uint64]SegmentMeta, len(rows))
	for _, r := range rows {
		dbBySeq[r.Seq] = r
	}

	entries, err := os.ReadDir(pl.dir)
	if err != nil {
		return fmt.Errorf("logstore: readdir %s: %w", pl.dir, err)
	}
	carPresent := map[uint64]struct{}{}
	sidecarSeqs := map[uint64]struct{}{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "seg-") {
			continue
		}
		var (
			set    map[uint64]struct{}
			suffix string
		)
		switch {
		case strings.HasSuffix(name, ".car"):
			set, suffix = carPresent, ".car"
		case strings.HasSuffix(name, ".idx"):
			set, suffix = sidecarSeqs, ".idx"
		case strings.HasSuffix(name, ".ops"):
			set, suffix = sidecarSeqs, ".ops"
		default:
			continue
		}
		stem := strings.TrimSuffix(strings.TrimPrefix(name, "seg-"), suffix)
		seq, perr := strconv.ParseUint(stem, 10, 64)
		if perr != nil {
			pl.logger.Warn("logstore: skip non-segment file", zap.String("name", name))
			continue
		}
		set[seq] = struct{}{}
	}

	// A CAR whose seq has a retired row is left over from a retire whose
	// unlink never reached the disk; the row is the record DeleteBucket
	// needs, so the files go and the row stays. Only read when some CAR has
	// no unretired row.
	var retired map[uint64]bool
	isRetired := func(seq uint64) (bool, error) {
		if retired == nil {
			rows, err := pl.meta.ListRetiredSegments(ctx, pl.plane, pl.bucket)
			if err != nil {
				return false, fmt.Errorf("logstore: list retired %s segments for %q: %w", pl.plane, pl.bucket, err)
			}
			retired = make(map[uint64]bool, len(rows))
			for _, r := range rows {
				retired[r.Seq] = true
			}
		}
		return retired[seq], nil
	}

	var (
		sealedRecovered []*Segment
		opens           []*Segment
	)

	for seq := range carPresent {
		row, hasRow := dbBySeq[seq]
		if !hasRow {
			gone, err := isRetired(seq)
			if err != nil {
				return err
			}
			if gone {
				pl.logger.Warn("logstore: removing the files of a retired segment",
					zap.Stringer("plane", pl.plane), zap.Uint64("seq", seq))
				removeSegmentFiles(pl.dir, seq, pl.plane)
				delete(carPresent, seq)
				continue
			}
		}
		if hasRow && row.State == StateSealed {
			seg, err := loadSealedFromIdx(pl.dir, seq, pl.plane, row.ShippedAt, pl.logger)
			if err != nil {
				return fmt.Errorf("logstore: load sealed %s seg %d: %w", pl.plane, seq, err)
			}
			sealedRecovered = append(sealedRecovered, seg)
			continue
		}
		seg, err := pl.sealedFromIdx(ctx, seq, hasRow)
		if err != nil {
			return err
		}
		if seg != nil {
			sealedRecovered = append(sealedRecovered, seg)
			continue
		}
		// Open at the crash, with or without its row: rebuild it from the
		// CAR, seeding the row if missing.
		if seg, err = rebuildOpenFromDisk(pl.dir, seq, pl.plane, pl.logger); err != nil {
			return fmt.Errorf("logstore: rebuild open %s seg %d: %w", pl.plane, seq, err)
		}
		if !hasRow {
			if err := pl.meta.InsertSegmentOpen(ctx, pl.plane, seq, pl.bucket); err != nil {
				return fmt.Errorf("logstore: insert orphan %s row %d: %w", pl.plane, seq, err)
			}
		}
		opens = append(opens, seg)
	}

	// Force-seal every recovered open segment, so each process starts with a
	// brand-new open one. There can be more than one: Meta restored from
	// behind the disk, or a rebuild from the journal that crashed partway,
	// leaves an open segment besides the one open at the crash.
	for _, seg := range opens {
		if err := seg.seal(ctx, pl.meta); err != nil {
			return fmt.Errorf("logstore: force-seal recovered open %s seg %d: %w", pl.plane, seg.Seq(), err)
		}
		sealedRecovered = append(sealedRecovered, seg)
	}

	// Sidecars with no CAR are useless strays (a torn createOpenSegment or
	// a half-deleted retire). Unlink them.
	for seq := range sidecarSeqs {
		if _, ok := carPresent[seq]; ok {
			continue
		}
		pl.logger.Warn("logstore: removing stray sidecar files",
			zap.Stringer("plane", pl.plane), zap.Uint64("seq", seq))
		removeSegmentFiles(pl.dir, seq, pl.plane)
	}

	rebuilt, err := pl.rebuildFromJournal(ctx, carPresent)
	if err != nil {
		return err
	}
	sealedRecovered = append(sealedRecovered, rebuilt...)

	// DB rows without an on-disk CAR. A sealed row converges to retired, so
	// DeleteBucket still releases whatever its ship registered; an open row
	// never shipped, so it is deleted.
	now := time.Now().Unix()
	for seq, row := range dbBySeq {
		if _, ok := carPresent[seq]; ok {
			continue
		}
		if row.State == StateSealed {
			pl.logger.Warn("logstore: sealed segment row without on-disk file; retiring row",
				zap.Stringer("plane", pl.plane), zap.Uint64("seq", seq))
			if err := pl.meta.RetireSegment(ctx, pl.plane, seq, now); err != nil {
				return fmt.Errorf("logstore: retire orphan %s row %d: %w", pl.plane, seq, err)
			}
			continue
		}
		pl.logger.Error("logstore: DB segment row without on-disk file; deleting row",
			zap.Stringer("plane", pl.plane), zap.Uint64("seq", seq))
		if err := pl.meta.DeleteSegment(ctx, pl.plane, seq); err != nil {
			return fmt.Errorf("logstore: delete orphan %s row %d: %w", pl.plane, seq, err)
		}
	}

	// Sealed segments newest-first.
	sort.Slice(sealedRecovered, func(i, j int) bool {
		return sealedRecovered[i].Seq() > sealedRecovered[j].Seq()
	})
	pl.sealed = sealedRecovered

	// Re-enqueue sealed segments not yet shipped (shipping plane only).
	if pl.pc.Ship {
		for _, seg := range pl.sealed {
			if seg.IsShipped() || seg.IsRetired() {
				continue
			}
			pl.enqueueFlush(seg)
		}
	}

	return nil
}

// sealedFromIdx recovers a segment that finished sealing (seal writes its
// .idx by rename) while Meta has its row open or has no row: Meta was
// restored from a point behind the disk. It brings the row up to the
// sidecar and returns the sealed segment, or nil when there is no .idx or
// it won't load (a rename that never reached the disk), in which case the
// caller rebuilds the segment from its CAR.
func (pl *PlaneLog) sealedFromIdx(ctx context.Context, seq uint64, hasRow bool) (*Segment, error) {
	if _, err := os.Stat(filepath.Join(pl.dir, idxName(seq))); err != nil {
		return nil, nil
	}
	seg, err := loadSealedFromIdx(pl.dir, seq, pl.plane, 0, pl.logger)
	if err != nil {
		pl.logger.Warn("logstore: unreadable .idx; rebuilding the segment from its CAR",
			zap.Stringer("plane", pl.plane), zap.Uint64("seq", seq), zap.Error(err))
		return nil, nil
	}
	if hasRow {
		err = pl.meta.MarkSegmentSealed(ctx, pl.plane, seq, seg.SealedAt(), seg.Size(), seg.SHA256(), seg.OpRoots())
	} else {
		err = pl.meta.RehydrateSegment(ctx, SegmentMeta{
			Seq: seq, Plane: pl.plane, Bucket: pl.bucket, State: StateSealed,
			SealedAt: seg.SealedAt(), Size: seg.Size(), SHA256: seg.SHA256(), OpRoots: seg.OpRoots(),
		})
	}
	if err != nil {
		_ = seg.fdRO.Close()
		return nil, fmt.Errorf("logstore: restore %s row %d from its .idx: %w", pl.plane, seq, err)
	}
	return seg, nil
}

// journalPage bounds how many journaled blocks rebuildFromJournal holds in
// memory at once.
const journalPage = 256

// frameBound is an upper bound on the bytes block takes in a segment CAR: its
// CID and data behind a varint length.
func frameBound(b block.Block) int64 {
	return int64(binary.MaxVarintLen64 + len(b.Cid().Bytes()) + len(b.RawData()))
}

// rebuildFromJournal rebuilds the catalog blocks of this bucket's lost
// segments from the journal and returns the segments it wrote them into,
// sealed. A segment is lost when the journal still files blocks under it
// but its CAR is not on disk: the data volume was lost or replaced, or the
// file deleted. The journal is then the only copy of those blocks until they
// ship. They go into new segments split at SealBytes, page by page, and each
// stretch of a page's journal rows moves onto its new segment once fsynced
// there, so a crash partway leaves the rest filed under the lost seq for the
// next recovery.
// The new segments carry no op-roots: the bucket's forge root then waits for
// its next shipped write.
func (pl *PlaneLog) rebuildFromJournal(ctx context.Context, onDisk map[uint64]struct{}) ([]*Segment, error) {
	if pl.plane != blockstore.PlaneCatalog {
		return nil, nil
	}
	seqs, err := pl.meta.JournalSegments(ctx, pl.bucket)
	if err != nil {
		return nil, fmt.Errorf("logstore: list journaled segments for %q: %w", pl.bucket, err)
	}
	var lost []uint64
	for _, seq := range seqs {
		if _, ok := onDisk[seq]; !ok {
			lost = append(lost, seq)
		}
	}
	if len(lost) == 0 {
		return nil, nil
	}

	var (
		built []*Segment
		cur   *Segment
	)
	seal := func() error {
		if err := cur.seal(ctx, pl.meta); err != nil {
			return fmt.Errorf("logstore: seal rebuilt %s seg %d: %w", pl.plane, cur.Seq(), err)
		}
		built = append(built, cur)
		cur = nil
		return nil
	}
	for _, from := range lost {
		after := cid.Undef
		for {
			blocks, err := pl.meta.ReadJournal(ctx, from, after, journalPage)
			if err != nil {
				return nil, fmt.Errorf("logstore: read journal of lost seg %d: %w", from, err)
			}
			if len(blocks) == 0 {
				break
			}
			full := len(blocks) == journalPage
			after = blocks[len(blocks)-1].Cid()
			for len(blocks) > 0 {
				if cur == nil {
					seq, err := pl.meta.NextSegmentSeq(ctx)
					if err != nil {
						return nil, fmt.Errorf("logstore: next segment seq: %w", err)
					}
					if cur, err = createOpenSegment(ctx, pl.dir, seq, pl.plane, pl.bucket, pl.meta, pl.logger); err != nil {
						return nil, fmt.Errorf("logstore: create segment to rebuild seg %d: %w", from, err)
					}
				}
				// As many of the page's blocks as fit under SealBytes, at
				// least one; the rest go to the next segment.
				n, size := 1, cur.Size()+frameBound(blocks[0])
				for n < len(blocks) && size+frameBound(blocks[n]) <= pl.pc.SealBytes {
					size += frameBound(blocks[n])
					n++
				}
				chunk := blocks[:n]
				blocks = blocks[n:]
				if err := cur.append(chunk, nil); err != nil {
					return nil, fmt.Errorf("logstore: rebuild seg %d into seg %d: %w", from, cur.Seq(), err)
				}
				if err := pl.meta.MoveJournal(ctx, pl.bucket, from, cur.Seq(), chunk[0].Cid(), chunk[n-1].Cid()); err != nil {
					return nil, fmt.Errorf("logstore: move journal of seg %d to seg %d: %w", from, cur.Seq(), err)
				}
				if len(blocks) > 0 || cur.Size() >= pl.pc.SealBytes {
					if err := seal(); err != nil {
						return nil, err
					}
				}
			}
			if !full {
				break
			}
		}
	}
	if cur != nil {
		if err := seal(); err != nil {
			return nil, err
		}
	}
	rebuiltSeqs := make([]uint64, len(built))
	for i, seg := range built {
		rebuiltSeqs[i] = seg.Seq()
	}
	pl.logger.Warn("logstore: rebuilt lost segments from the catalog journal",
		zap.Stringer("plane", pl.plane), zap.Uint64s("lost", lost), zap.Uint64s("rebuilt", rebuiltSeqs))
	return built, nil
}

// removeSegmentFiles unlinks every possible file of a segment seq for the
// given plane.
func removeSegmentFiles(dir string, seq uint64, plane blockstore.Plane) {
	names := []string{carName(seq), idxName(seq)}
	if plane == blockstore.PlaneCatalog {
		names = append(names, opsName(seq))
	}
	for _, name := range names {
		_ = os.Remove(filepath.Join(dir, name))
	}
}
