package logstore

import (
	"context"

	"github.com/fil-forge/ingot/blockstore"
)

// State describes the lifecycle stage of a segment. A segment is open
// (its plane's current append target) or sealed (closed for writes).
// There is no "flushed" state: shipping is tracked via the ShippedAt
// timestamp, and a plane may be configured never to ship at all (its
// segments stay sealed-and-local forever).
type State int

const (
	// StateOpen means the segment is its plane's current append target.
	// Exactly one segment per plane is in this state at a time.
	StateOpen State = iota
	// StateSealed means the segment is closed for writes. It may ship and
	// retire; a sealed segment stays sealed until it retires off disk.
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

// SegmentMeta is the persistence-layer view of a single-plane segment.
// Recovery uses it to enumerate a bucket's segments. Each segment belongs
// to exactly one plane and — since the log is segregated per bucket — to
// exactly one bucket, whose space its CAR ships to.
type SegmentMeta struct {
	Seq    uint64
	Plane  blockstore.Plane
	Bucket string
	State  State

	SealedAt int64

	// Size + seal-time sha256 of this segment's CAR.
	Size   int64
	SHA256 []byte

	// ShippedAt is the unix-seconds high-water mark: when this segment's
	// CAR shipped to Forge, or 0 if not (yet) shipped. A non-shipping
	// plane stays 0 forever.
	ShippedAt int64

	// OpRoots are the per-batch (bucket, root) records. Populated only
	// for catalog-plane segments — op-roots are MST roots.
	OpRoots []blockstore.OpRoot
}

// Meta is the persistence backing for the per-plane segment lifecycle.
// Every method is plane-scoped except NextSegmentSeq, which draws from a
// single globally-unique allocator shared by both planes. The production
// implementation is *registry.Postgres; tests use an in-memory fake.
// Logstore never touches SQL directly.
type Meta interface {
	// NextSegmentSeq returns a fresh, globally-unique, monotonic segment
	// id. Both planes draw from the same allocator; an id belongs to
	// whichever plane records it via InsertSegmentOpen.
	NextSegmentSeq(ctx context.Context) (uint64, error)

	// InsertSegmentOpen records that segment seq has just been opened for
	// plane by bucket's log. Idempotent: an existing row in any state is
	// left alone.
	InsertSegmentOpen(ctx context.Context, plane blockstore.Plane, seq uint64, bucket string) error

	// MarkSegmentSealed transitions a segment from open to sealed in one
	// transaction: records the CAR size+sha and inserts the per-segment
	// op-root rows. opRoots are applied in slice order (each gets
	// seq_within = i).
	MarkSegmentSealed(ctx context.Context, plane blockstore.Plane, seq uint64, sealedAt int64,
		size int64, sha []byte, opRoots []blockstore.OpRoot) error

	// MarkSegmentShipped records that this segment's CAR finished shipping
	// to Forge, stamping shipped_at, and advances forge_root_cid in
	// ingot.buckets for every op-root recorded against this segment (catalog
	// roots are the MST roots durable on Forge), all in one transaction.
	MarkSegmentShipped(ctx context.Context, plane blockstore.Plane, seq uint64, shippedAt int64, opRoots []blockstore.OpRoot) error

	// DeleteSegment removes a segment row (cascades to op-root rows).
	// Used by retention after the on-disk files are unlinked.
	DeleteSegment(ctx context.Context, plane blockstore.Plane, seq uint64) error

	// ListSegments returns every segment row for plane belonging to bucket
	// (open + sealed) ordered by seq ascending, with op-roots hydrated.
	// Recovery uses it to rebuild the read tier and re-enqueue unshipped
	// segments.
	ListSegments(ctx context.Context, plane blockstore.Plane, bucket string) ([]SegmentMeta, error)

	// ListSegmentBuckets returns the distinct buckets that have at least
	// one segment row for plane. The log manager's startup sweep uses it
	// to re-open every bucket log with history, so unshipped segments
	// re-enqueue without waiting for the bucket's next write.
	ListSegmentBuckets(ctx context.Context, plane blockstore.Plane) ([]string, error)

	// RehydrateSegment writes a segment row + its op-root rows from the
	// on-disk `.idx` sidecar when the DB row is missing or torn.
	// Idempotent on (seq) — replaces any existing row for that segment.
	RehydrateSegment(ctx context.Context, m SegmentMeta) error
}
