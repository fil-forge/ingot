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

	"github.com/fil-forge/ingot/blockstore"
)

// recover scans the on-disk directory and the Meta backend, rebuilds
// the sealed segment list, picks the in-flight open segment (if any),
// and re-enqueues — per plane — anything still pending ship.
//
// A segment is present on disk if EITHER its data CAR or its catalog
// CAR survives (the two planes retire independently). Reconciliation:
//   - row state=open  → rebuild as open; force-sealed by Open.
//   - row state=sealed → load from .idx sidecars (a plane whose sidecar
//     is gone is treated as already-retired); re-enqueue each shipping
//     plane that has not yet shipped.
//   - no row, both CARs present → orphan open (rebuild + insert row).
//   - no row, partial files → stray; unlink.
//   - row present, no CAR files → delete row.
func (s *Store) recover(ctx context.Context) error {
	rows, err := s.cfg.Meta.ListSegments(ctx)
	if err != nil {
		return fmt.Errorf("logstore: list segments: %w", err)
	}
	dbBySeq := make(map[uint64]SegmentMeta, len(rows))
	for _, r := range rows {
		dbBySeq[r.Seq] = r
	}

	entries, err := os.ReadDir(s.cfg.Dir)
	if err != nil {
		return fmt.Errorf("logstore: readdir: %w", err)
	}
	dataPresent := map[uint64]struct{}{}
	catPresent := map[uint64]struct{}{}
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
		case strings.HasSuffix(name, ".data.car"):
			set, suffix = dataPresent, ".data.car"
		case strings.HasSuffix(name, ".cat.car"):
			set, suffix = catPresent, ".cat.car"
		default:
			continue
		}
		stem := strings.TrimSuffix(strings.TrimPrefix(name, "seg-"), suffix)
		seq, perr := strconv.ParseUint(stem, 10, 64)
		if perr != nil {
			s.logger.Warn("logstore: skip non-segment file", zap.String("name", name))
			continue
		}
		set[seq] = struct{}{}
	}

	// Union of seqs present on disk (either plane).
	allSeqs := map[uint64]struct{}{}
	for seq := range dataPresent {
		allSeqs[seq] = struct{}{}
	}
	for seq := range catPresent {
		allSeqs[seq] = struct{}{}
	}

	var (
		sealedRecovered []*Segment
		recoveredOpen   *Segment
		maxSeq          uint64
	)

	for seq := range allSeqs {
		if seq > maxSeq {
			maxSeq = seq
		}
		row, hasRow := dbBySeq[seq]
		_, hasData := dataPresent[seq]
		_, hasCat := catPresent[seq]

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
			seg, err := loadSealedFromIdx(s.cfg.Dir, seq, row.SealedAt, row.DataShippedAt, row.CatShippedAt, s.logger)
			if err != nil {
				return fmt.Errorf("logstore: load sealed seg %d: %w", seq, err)
			}
			sealedRecovered = append(sealedRecovered, seg)

		case !hasRow && hasData && hasCat:
			// Orphan: open at crash before its row was inserted. Rebuild
			// and seed the row so the force-seal UPDATE matches.
			seg, err := rebuildOpenFromDisk(s.cfg.Dir, seq, s.logger)
			if err != nil {
				return fmt.Errorf("logstore: rebuild orphan seg %d: %w", seq, err)
			}
			if err := s.cfg.Meta.InsertSegmentOpen(ctx, seq); err != nil {
				return fmt.Errorf("logstore: insert orphan row %d: %w", seq, err)
			}
			if recoveredOpen != nil {
				return fmt.Errorf("logstore: orphan + open conflict (seqs %d and %d)",
					recoveredOpen.seq, seq)
			}
			recoveredOpen = seg

		default:
			// No row and only a partial set of files: a torn
			// createOpenSegment. Nothing was acked; unlink the strays.
			s.logger.Warn("logstore: removing stray partial segment files", zap.Uint64("seq", seq))
			removeSegmentFiles(s.cfg.Dir, seq)
		}
	}

	// DB rows without any on-disk CAR → converge by deleting the row.
	for seq := range dbBySeq {
		if _, ok := allSeqs[seq]; ok {
			continue
		}
		s.logger.Error("logstore: DB segment row without on-disk file; deleting row",
			zap.Uint64("seq", seq))
		if err := s.cfg.Meta.DeleteSegment(ctx, seq); err != nil {
			return fmt.Errorf("logstore: delete orphan row %d: %w", seq, err)
		}
	}

	// Sealed segments newest-first.
	sort.Slice(sealedRecovered, func(i, j int) bool {
		return sealedRecovered[i].Seq() > sealedRecovered[j].Seq()
	})
	s.sealed = sealedRecovered

	// Re-enqueue, per shipping plane, any sealed segment that has not
	// yet shipped that plane.
	for _, seg := range s.sealed {
		for _, p := range blockstore.Planes {
			q, ok := s.flushQ[p]
			if !ok {
				continue
			}
			if seg.IsShipped(p) || seg.IsRetired(p) {
				continue
			}
			select {
			case q <- seg:
			default:
				s.logger.Warn("logstore: flush queue full at recovery; will retry on restart",
					zap.Stringer("plane", p), zap.Uint64("seq", seg.Seq()))
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

// removeSegmentFiles unlinks every possible file of a segment seq.
func removeSegmentFiles(dir string, seq uint64) {
	for _, name := range []string{
		dataCARName(seq), dataIdxName(seq),
		catCARName(seq), catIdxName(seq),
		opsName(seq),
	} {
		_ = os.Remove(filepath.Join(dir, name))
	}
}
