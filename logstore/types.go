package logstore

import (
	"context"

	"github.com/fil-forge/ingot/blockstore"
)

// State describes the lifecycle stage of a segment as observed at the
// catalog/Postgres level. The on-disk MANIFEST may briefly lag the
// in-memory state but recovery reconciles the two.
type State int

const (
	// StateOpen means the segment is the current append target. Exactly
	// one segment is in this state at a time.
	StateOpen State = iota
	// StateSealed means the segment is closed for writes and waiting to
	// be (or being) shipped to Forge.
	StateSealed
	// StateFlushed means the segment has been successfully shipped to
	// Forge and the per-bucket forge_root advances were applied. The
	// segment may still be on disk, kept around as a read tier.
	StateFlushed
)

// String renders State for logs.
func (s State) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateSealed:
		return "sealed"
	case StateFlushed:
		return "flushed"
	default:
		return "unknown"
	}
}

// ParseState is the inverse of State.String. Unknown strings yield
// StateOpen and ok=false, matching what we want at the SQL boundary.
func ParseState(s string) (State, bool) {
	switch s {
	case "open":
		return StateOpen, true
	case "sealed":
		return StateSealed, true
	case "flushed":
		return StateFlushed, true
	default:
		return StateOpen, false
	}
}

// SegmentMeta is the persistence-layer view of a segment. Used by
// recovery to enumerate segments that need attention.
type SegmentMeta struct {
	Seq       uint64
	State     State
	SealedAt  int64
	FlushedAt int64
	SizeBytes int64
	SHA256    []byte
	OpRoots   []blockstore.OpRoot
}

// Meta is the persistence backing for the segment lifecycle. The
// production implementation is *registry.Postgres; tests use an
// in-memory fake. Logstore never touches SQL directly.
type Meta interface {
	// NextSegmentSeq returns a fresh monotonic segment id.
	NextSegmentSeq(ctx context.Context) (uint64, error)

	// InsertSegmentOpen records that segment seq has just been opened.
	// Idempotent: if the row already exists in any state it is left
	// alone.
	InsertSegmentOpen(ctx context.Context, seq uint64) error

	// MarkSegmentSealed transitions a segment from open to sealed in
	// one transaction: updates ingot.segments and inserts the
	// per-segment op-root rows. opRoots are applied in slice order
	// (each gets seq_within = i).
	MarkSegmentSealed(ctx context.Context, seq uint64, sealedAt int64, sizeBytes int64, sha256 []byte, opRoots []blockstore.OpRoot) error

	// MarkSegmentFlushed transitions a segment from sealed to flushed
	// AND advances forge_root_cid in ingot.buckets for every op-root
	// recorded against this segment, all in one transaction. opRoots
	// is the in-order list from MarkSegmentSealed; the registry uses
	// it directly so callers can treat the sidecar as the source of
	// truth.
	MarkSegmentFlushed(ctx context.Context, seq uint64, flushedAt int64, opRoots []blockstore.OpRoot) error

	// DeleteSegment removes a segment row (cascades to op-root rows).
	// Used by retention after the on-disk file is unlinked.
	DeleteSegment(ctx context.Context, seq uint64) error

	// ListUnflushedSegments returns every segment whose state is open
	// or sealed, ordered by seq ascending. Recovery uses this to
	// re-enqueue work for the flusher and to verify on-disk vs DB
	// state.
	ListUnflushedSegments(ctx context.Context) ([]SegmentMeta, error)

	// RehydrateSegment writes a segment row + its op-root rows from a
	// sidecar `.idx` when the DB row is missing or torn. Idempotent
	// on (seq) — replaces any existing rows for that segment.
	RehydrateSegment(ctx context.Context, m SegmentMeta) error
}
