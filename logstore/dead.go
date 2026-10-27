package logstore

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/cars"
)

// deadOnArrival returns the segment's dead-on-arrival bytes (see deadBytes),
// or -1 when they could not be measured, which leaves the ship unmeasured
// rather than failing it. A sealed segment's blocks never change, so it is
// measured once, at its first ship attempt: the retry sweep's later attempts
// at a segment that keeps failing to ship reuse the figure instead of reading
// the segment again every ShipRetry.
func (s *Segment) deadOnArrival() int64 {
	s.deadOnce.Do(func() {
		dead, err := s.deadBytes()
		if err != nil {
			s.logger.Warn("logstore: measure dead bytes",
				zap.Stringer("plane", s.plane), zap.Uint64("seq", s.seq), zap.Error(err))
			dead = -1
		}
		s.dead = dead
	})
	return s.dead
}

// deadBytes measures how much of a sealed segment is dead on arrival: the
// bytes of the frames holding blocks that the segment's last op-root no
// longer reaches. Those blocks were superseded before the segment shipped,
// mostly MST interior nodes that later writes in the same segment rewrote.
// The CAR header is not counted either way.
//
// The walk follows links only through this segment's own blocks. A block
// reached only through an older segment's block counts as dead although it
// is live; an older copy of it is then dead instead, so the bucket's dead
// bytes in total stay right and only the split between dead on arrival and
// dead later shifts. The last op-root can also be one the bucket never
// adopted (its write's root CAS failed after the append); the split is then
// off by about that write's rewritten path.
func (s *Segment) deadBytes() (int64, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	if s.state != StateSealed || s.retired || s.fdRO == nil {
		return 0, errors.New("logstore: dead bytes of a segment that is not sealed and on disk")
	}

	var total int64
	for c, loc := range s.index {
		total += cars.FrameSize(c, loc.Length)
	}
	if len(s.opRoots) == 0 {
		return total, nil
	}

	var live int64
	reached := make(map[cid.Cid]struct{}, len(s.index))
	pending := []cid.Cid{s.opRoots[len(s.opRoots)-1].Root}
	for len(pending) > 0 {
		c := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if _, ok := reached[c]; ok {
			continue
		}
		loc, ok := s.index[c]
		if !ok {
			continue
		}
		reached[c] = struct{}{}
		live += cars.FrameSize(c, loc.Length)
		if c.Prefix().Codec != cid.DagCBOR {
			continue
		}

		buf := make([]byte, loc.Length)
		if _, err := s.fdRO.ReadAt(buf, int64(loc.Offset)); err != nil {
			return 0, fmt.Errorf("logstore: read seg %d offset %d: %w", s.seq, loc.Offset, err)
		}
		if err := cbg.ScanForLinks(bytes.NewReader(buf), func(link cid.Cid) {
			if _, ok := s.index[link]; ok {
				pending = append(pending, link)
			}
		}); err != nil {
			return 0, fmt.Errorf("logstore: scan %s in seg %d: %w", c, s.seq, err)
		}
	}
	return total - live, nil
}
