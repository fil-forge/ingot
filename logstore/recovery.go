package logstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

// recover scans the on-disk directory and the Meta backend, builds
// the sealed segment list, picks an in-flight open segment (if any),
// and re-enqueues anything still pending flush.
//
// Reconciliation rules (file × DB):
//   - File present, DB row absent: rehydrate DB from sidecar; if no
//     sidecar (segment was open at crash), rebuild from CAR + ops.
//   - File present, DB state=open: rebuild as open. Will be
//     force-sealed by Open.
//   - File present, DB state=sealed: load from .idx as sealed;
//     re-enqueue for flush.
//   - File present, DB state=flushed: load from .idx as flushed;
//     keep on the read tier subject to retention.
//   - File absent, DB row present: log error, delete row to converge.
func (s *Store) recover(ctx context.Context) error {
	rows, err := s.cfg.Meta.ListUnflushedSegments(ctx)
	if err != nil {
		return fmt.Errorf("logstore: list unflushed: %w", err)
	}

	// Index DB rows by seq.
	dbBySeq := make(map[uint64]SegmentMeta, len(rows))
	for _, r := range rows {
		dbBySeq[r.Seq] = r
	}

	// Discover on-disk segments by walking the dir.
	entries, err := os.ReadDir(s.cfg.Dir)
	if err != nil {
		return fmt.Errorf("logstore: readdir: %w", err)
	}
	carSeqs := map[uint64]struct{}{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "seg-") || !strings.HasSuffix(name, ".car") {
			continue
		}
		stem := strings.TrimSuffix(strings.TrimPrefix(name, "seg-"), ".car")
		seq, err := strconv.ParseUint(stem, 10, 64)
		if err != nil {
			s.logger.Warn("logstore: skip non-segment file", zap.String("name", name))
			continue
		}
		carSeqs[seq] = struct{}{}
	}

	// Track the highest seen seq so the next allocated seq is strictly
	// greater than anything we've already used.
	var maxSeq uint64

	// Discover flushed segments still on disk by reading their .idx
	// files (no DB rows since flushed segments are pruned from the
	// unflushed list — but we still want them in the read tier).
	flushedOnDisk := map[uint64]bool{}
	for seq := range carSeqs {
		if _, ok := dbBySeq[seq]; ok {
			continue
		}
		// Either flushed (still on disk for retention) or orphan from
		// a missing DB row. We probe .idx; if present, this looks like
		// a sealed/flushed segment with no DB row → rehydrate DB and
		// load as flushed (safe default — the flusher will not run on
		// a flushed segment).
		idxPath := filepath.Join(s.cfg.Dir, idxName(seq))
		if _, err := os.Stat(idxPath); err == nil {
			flushedOnDisk[seq] = true
		}
	}

	type loaded struct {
		seg *Segment
	}
	var (
		sealedRecovered  []loaded
		flushedRecovered []loaded
		recoveredOpen    *Segment
	)

	for seq := range carSeqs {
		if seq > maxSeq {
			maxSeq = seq
		}
		row, hasRow := dbBySeq[seq]

		switch {
		case hasRow && row.State == StateOpen:
			seg, err := rebuildOpenFromDisk(s.cfg.Dir, seq, s.logger)
			if err != nil {
				return fmt.Errorf("logstore: rebuild open seg %d: %w", seq, err)
			}
			if recoveredOpen != nil {
				return fmt.Errorf("logstore: more than one open segment on disk (seqs %d and %d)",
					recoveredOpen.seq, seq)
			}
			recoveredOpen = seg

		case hasRow && row.State == StateSealed:
			seg, err := loadSealedFromIdx(s.cfg.Dir, seq, s.logger)
			if err != nil {
				return fmt.Errorf("logstore: load sealed seg %d: %w", seq, err)
			}
			sealedRecovered = append(sealedRecovered, loaded{seg: seg})

		case flushedOnDisk[seq]:
			seg, err := loadFlushedFromIdx(s.cfg.Dir, seq, 0, s.logger)
			if err != nil {
				return fmt.Errorf("logstore: load flushed seg %d: %w", seq, err)
			}
			flushedRecovered = append(flushedRecovered, loaded{seg: seg})

		default:
			// File on disk but no DB row and no idx — treat as a
			// previously-open segment that crashed before sealing. Rebuild
			// as open and let the force-seal path in Open() finalize it.
			seg, err := rebuildOpenFromDisk(s.cfg.Dir, seq, s.logger)
			if err != nil {
				return fmt.Errorf("logstore: rebuild orphan seg %d: %w", seq, err)
			}
			// Seed the DB row in 'open' so the seal transition's
			// "from open" UPDATE matches.
			if err := s.cfg.Meta.InsertSegmentOpen(ctx, seq); err != nil {
				return fmt.Errorf("logstore: insert orphan row %d: %w", seq, err)
			}
			if recoveredOpen != nil {
				return fmt.Errorf("logstore: orphan + open conflict (seqs %d and %d)",
					recoveredOpen.seq, seq)
			}
			recoveredOpen = seg
		}
	}

	// DB rows without a corresponding .car file → log + clean up.
	for seq, row := range dbBySeq {
		if _, ok := carSeqs[seq]; ok {
			continue
		}
		s.logger.Error("logstore: DB segment row without on-disk file; deleting row",
			zap.Uint64("seq", seq), zap.String("state", row.State.String()))
		if err := s.cfg.Meta.DeleteSegment(ctx, seq); err != nil {
			return fmt.Errorf("logstore: delete orphan row %d: %w", seq, err)
		}
	}

	// Sort recovered sealed/flushed segments newest-first by seq.
	sort.Slice(sealedRecovered, func(i, j int) bool {
		return sealedRecovered[i].seg.Seq() > sealedRecovered[j].seg.Seq()
	})
	sort.Slice(flushedRecovered, func(i, j int) bool {
		return flushedRecovered[i].seg.Seq() > flushedRecovered[j].seg.Seq()
	})

	// Combine into the sealed slice (newest-first overall).
	all := make([]*Segment, 0, len(sealedRecovered)+len(flushedRecovered))
	for _, l := range sealedRecovered {
		all = append(all, l.seg)
	}
	for _, l := range flushedRecovered {
		all = append(all, l.seg)
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].Seq() > all[j].Seq() })
	s.sealed = all

	// Re-enqueue sealed segments (not flushed) for the flusher.
	for _, seg := range s.sealed {
		if seg.State() == StateSealed {
			select {
			case s.flushQ <- seg:
			default:
				s.logger.Warn("logstore: flush queue full at recovery; will retry on tick",
					zap.Uint64("seq", seg.Seq()))
			}
		}
	}

	s.open = recoveredOpen
	if recoveredOpen != nil && recoveredOpen.Seq() > maxSeq {
		maxSeq = recoveredOpen.Seq()
	}
	s.nextSeq = maxSeq + 1

	return nil
}
