package logstore

import (
	"context"

	"github.com/fil-forge/ingot/blockstore"
)

// State describes the lifecycle stage of a segment. A segment is open
// (the single current append target) or sealed (closed for writes).
// There is no "flushed" state: shipping is tracked per-plane via the
// DataShippedAt / CatShippedAt timestamps, because the data plane and
// the catalog plane ship to Forge through independent pipelines and
// either may be configured never to ship at all.
type State int

const (
	// StateOpen means the segment is the current append target. Exactly
	// one segment is in this state at a time.
	StateOpen State = iota
	// StateSealed means the segment is closed for writes. Its two CARs
	// may ship and retire independently; a sealed segment stays sealed
	// until both planes have retired off local disk.
	StateSealed
)

// String renders State for logs.
func (s State) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateSealed:
		return "sealed"
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
	default:
		return StateOpen, false
	}
}

// SegmentMeta is the persistence-layer view of a segment. Used by
// recovery to enumerate segments that need attention. The data and
// catalog planes carry independent size/sha/shipped state because they
// ship through separate pipelines.
type SegmentMeta struct {
	Seq      uint64
	State    State
	SealedAt int64

	// Per-plane CAR size + seal-time sha256.
	DataSize   int64
	DataSHA256 []byte
	CatSize    int64
	CatSHA256  []byte

	// Per-plane ship high-water: unix seconds when that plane's CAR was
	// shipped to Forge, or 0 if not (yet) shipped. A plane configured
	// never to ship stays 0 forever.
	DataShippedAt int64
	CatShippedAt  int64

	OpRoots []blockstore.OpRoot
}

// ShippedAt returns the ship timestamp for the given plane (0 = not
// shipped).
func (m SegmentMeta) ShippedAt(p blockstore.Plane) int64 {
	if p == blockstore.PlaneData {
		return m.DataShippedAt
	}
	return m.CatShippedAt
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

	// MarkSegmentSealed transitions a segment from open to sealed in one
	// transaction: records both planes' size+sha and inserts the
	// per-segment op-root rows. opRoots are applied in slice order (each
	// gets seq_within = i).
	MarkSegmentSealed(ctx context.Context, seq uint64, sealedAt int64,
		dataSize int64, dataSHA []byte, catSize int64, catSHA []byte,
		opRoots []blockstore.OpRoot) error

	// MarkSegmentShipped records that the given plane's CAR finished
	// shipping to Forge, stamping <plane>_shipped_at. For
	// blockstore.PlaneCatalog it ALSO advances forge_root_cid in
	// ingot.buckets for every op-root recorded against this segment
	// (catalog roots are the MST roots durable on Forge), all in one
	// transaction; for PlaneData opRoots is ignored.
	MarkSegmentShipped(ctx context.Context, seq uint64, plane blockstore.Plane, shippedAt int64, opRoots []blockstore.OpRoot) error

	// DeleteSegment removes a segment row (cascades to op-root rows).
	// Used by retention after both planes' on-disk files are unlinked.
	DeleteSegment(ctx context.Context, seq uint64) error

	// ListSegments returns every segment row (open + sealed) ordered by
	// seq ascending, with per-plane shipped state and op-roots hydrated.
	// Recovery uses this to rebuild the read tier and to re-enqueue, per
	// plane, any sealed segment whose plane has not yet shipped.
	ListSegments(ctx context.Context) ([]SegmentMeta, error)

	// RehydrateSegment writes a segment row + its op-root rows from the
	// on-disk `.idx` sidecars when the DB row is missing or torn.
	// Idempotent on (seq) — replaces any existing rows for that segment.
	RehydrateSegment(ctx context.Context, m SegmentMeta) error
}
