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

// recover scans this plane's subdirectory and the Meta backend, rebuilds
// the sealed segment list, picks the in-flight open segment (if any), and
// re-enqueues anything still pending ship. Discovery keys on the CAR
// file. Reconciliation per seq:
//   - row open    → rebuild as open; force-sealed by openPlaneLog.
//   - row sealed  → load from .idx; re-enqueue when shipping & not shipped.
//   - no row, CAR present → orphan (crashed before the row, or a row-lost
//     sealed segment): rebuild as open + insert row; force-sealed by
//     openPlaneLog.
//   - row present, no CAR → delete the row.
//   - sidecar (.idx/.ops) with no CAR → stray; unlink.
func (pl *PlaneLog) recover(ctx context.Context) error {
	rows, err := pl.meta.ListSegments(ctx, pl.plane)
	if err != nil {
		return fmt.Errorf("logstore: list %s segments: %w", pl.plane, err)
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

	var (
		sealedRecovered []*Segment
		recoveredOpen   *Segment
	)

	for seq := range carPresent {
		row, hasRow := dbBySeq[seq]
		switch {
		case hasRow && row.State == StateOpen:
			seg, err := rebuildOpenFromDisk(pl.dir, seq, pl.plane, pl.logger)
			if err != nil {
				return fmt.Errorf("logstore: rebuild open %s seg %d: %w", pl.plane, seq, err)
			}
			if recoveredOpen != nil {
				return fmt.Errorf("logstore: more than one open %s segment on disk (seqs %d and %d)",
					pl.plane, recoveredOpen.seq, seq)
			}
			recoveredOpen = seg

		case hasRow && row.State == StateSealed:
			seg, err := loadSealedFromIdx(pl.dir, seq, pl.plane, row.ShippedAt, pl.logger)
			if err != nil {
				return fmt.Errorf("logstore: load sealed %s seg %d: %w", pl.plane, seq, err)
			}
			sealedRecovered = append(sealedRecovered, seg)

		default:
			// No row but a CAR is present: open at crash before its row was
			// inserted, or a sealed segment whose row was lost. Rebuild as
			// open and seed the row; openPlaneLog force-seals it.
			seg, err := rebuildOpenFromDisk(pl.dir, seq, pl.plane, pl.logger)
			if err != nil {
				return fmt.Errorf("logstore: rebuild orphan %s seg %d: %w", pl.plane, seq, err)
			}
			if err := pl.meta.InsertSegmentOpen(ctx, pl.plane, seq); err != nil {
				return fmt.Errorf("logstore: insert orphan %s row %d: %w", pl.plane, seq, err)
			}
			if recoveredOpen != nil {
				return fmt.Errorf("logstore: orphan + open conflict (%s seqs %d and %d)",
					pl.plane, recoveredOpen.seq, seq)
			}
			recoveredOpen = seg
		}
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

	// DB rows without an on-disk CAR → converge by deleting the row.
	for seq := range dbBySeq {
		if _, ok := carPresent[seq]; ok {
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

	pl.open = recoveredOpen
	return nil
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
