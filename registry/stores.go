package registry

import (
	"context"
	"time"

	"github.com/fil-forge/ucantone/did"
)

// This file defines the relational surface the upload/storage/delete
// architecture relies on (docs/architecture.md §5–§7, Appendix C):
// the reverse reference index (blob_refs), the local-store index
// (upload_intents), the local blob-location table (blob_locations), the
// multipart session/part tables, and the gc_candidates log. The stores
// are split into focused interfaces so a caller can depend on only what
// it needs; *Postgres and *inmem.MemStore satisfy all of them.
//
// Versioning is not implemented this iteration. NullVersionID is the
// sentinel every blob_refs / manifest version id carries until versioning
// lands (it is S3's "null" version id for an unversioned object).
const NullVersionID = "null"

// upload_intents.state values (the local-store lifecycle, §5).
const (
	IntentSpooled   = "spooled"
	IntentParked    = "parked"
	IntentAccepted  = "accepted"
	IntentPublished = "published"
)

// multipart_sessions.state values (the single-winner latch, §7.3).
const (
	SessionOpen       = "open"
	SessionCompleting = "completing"
	SessionAborting   = "aborting"
	// SessionCompleted: the object committed; the session and its parts are
	// retained so a duplicate CompleteMultipartUpload with identical parts is
	// idempotent (S3 semantics). Reaped by the abandoned-session sweeper.
	SessionCompleted = "completed"
)

// multipart_parts.state values (§7.2).
const (
	PartParked   = "parked"
	PartAccepted = "accepted"
)

// BlobClaim is one row of ingot.blob_refs: a single object version's
// reference to a blob. The space-claim on (Space, Digest) is released when
// no BlobClaim rows remain for it. Distinct from bucket.BlobRef (the
// manifest's blob descriptor); this is the reverse index over those.
type BlobClaim struct {
	Digest    []byte
	Bucket    string
	ObjectKey string
	VersionID string
	Space     did.DID
}

// UploadIntent is one row of ingot.upload_intents: a blob Ingot holds on
// disk, with its lifecycle state. Keyed globally by Digest.
type UploadIntent struct {
	Digest    []byte
	LocalPath string
	Size      int64
	State     string
	Bucket    string
}

// BlobLocation is one row of ingot.blob_locations: where a blob can be
// retrieved from, captured at accept. Keyed by (Space, Digest).
type BlobLocation struct {
	Space    did.DID
	Digest   []byte
	Provider string
	URL      string
	Size     int64
}

// MultipartSession is one row of ingot.multipart_sessions. The HTTP metadata
// headers (ContentEncoding..Expires) are captured at CreateMultipartUpload so
// Complete can write them into the manifest exactly like a single-shot PUT.
type MultipartSession struct {
	UploadID                string
	Bucket                  string
	ObjectKey               string
	State                   string
	ContentType             string
	ContentEncoding         string
	ContentDisposition      string
	ContentLanguage         string
	CacheControl            string
	Expires                 string
	WebsiteRedirectLocation string
	// ChecksumAlgorithm/ChecksumType are the x-amz-checksum-* declarations from
	// CreateMultipartUpload, echoed by ListMultipartUploads. Per-part checksum
	// computation is separate (FIL-620).
	ChecksumAlgorithm string
	ChecksumType      string
	Metadata          map[string]string
	CreatedAt         time.Time
}

// MultipartPart is one row of ingot.multipart_parts. BlobDigests is the
// ordered list of blobs the part split into (one entry unless the part
// exceeded max_blob_size).
type MultipartPart struct {
	UploadID    string
	PartNumber  int
	ETagMD5     []byte
	Size        int64
	BlobDigests [][]byte
	State       string
	CreatedAt   time.Time
}

// BlobRefStore is the reverse reference index (§5, §6). A commit adds a
// claim per body digest; a delete/overwrite removes it; CountClaims gates
// remove(digest) (physical reclamation when the count reaches zero).
type BlobRefStore interface {
	AddBlobClaim(ctx context.Context, claim BlobClaim) error
	DeleteBlobClaim(ctx context.Context, digest []byte, bucket, objectKey, versionID string) error
	// CountClaims returns how many object versions in space still reference
	// digest. Zero means the space's claim may be released.
	CountClaims(ctx context.Context, space did.DID, digest []byte) (int, error)
}

// IntentStore is the local-store index (§5): the on-disk blobs Ingot holds
// and their lifecycle state. Drives read-after-write, cache lookup, and
// crash recovery.
type IntentStore interface {
	PutIntent(ctx context.Context, in UploadIntent) error
	SetIntentState(ctx context.Context, digest []byte, state string) error
	GetIntent(ctx context.Context, digest []byte) (*UploadIntent, error)
	ListIntentsByState(ctx context.Context, state string) ([]UploadIntent, error)
	DeleteIntent(ctx context.Context, digest []byte) error
}

// LocationStore is the local blob-location table (§8, appliance topology):
// the (space, digest) → provider/URL mapping captured at accept, resolved
// on read in place of the indexing-service.
type LocationStore interface {
	PutLocation(ctx context.Context, loc BlobLocation) error
	GetLocation(ctx context.Context, space did.DID, digest []byte) (*BlobLocation, error)
	DeleteLocation(ctx context.Context, space did.DID, digest []byte) error
}

// BlobPark is one row of ingot.blob_parks: the persistable state of a blob
// that is durable on its provider but not yet accepted (multipart's deferred
// conclude, §7.2). AddTask/AcceptTask are the /blob/add and
// /blob/accept task CIDs; PutInvocation is the sealed /http/put invocation
// whose metadata carries the derived signer keys needed to conclude —
// sensitive, deleted at conclude/abort. Keyed globally by Digest (like
// upload_intents: content-addressed dedup shares parks across sessions).
type BlobPark struct {
	Digest        []byte
	AddTask       []byte // cid bytes
	AcceptTask    []byte // cid bytes
	PutInvocation []byte
	Size          int64
	CreatedAt     time.Time
}

// ParkStore persists deferred-accept park state between UploadPart and
// Complete/Abort (§7.2).
type ParkStore interface {
	PutPark(ctx context.Context, p BlobPark) error
	// GetPark returns ErrNotFound when digest has no park row.
	GetPark(ctx context.Context, digest []byte) (*BlobPark, error)
	DeletePark(ctx context.Context, digest []byte) error
}

// MultipartStore tracks in-flight multipart uploads (§7.2). LatchSession is
// the single-winner transition (§7.3): exactly one of Complete/Abort moves
// the session off 'open'.
type MultipartStore interface {
	CreateSession(ctx context.Context, s MultipartSession) error
	GetSession(ctx context.Context, uploadID string) (*MultipartSession, error)
	// LatchSession atomically moves uploadID from->to, returning true iff this
	// caller performed the transition (the session was still in `from`).
	LatchSession(ctx context.Context, uploadID, from, to string) (bool, error)
	DeleteSession(ctx context.Context, uploadID string) error
	PutPart(ctx context.Context, p MultipartPart) error
	ListParts(ctx context.Context, uploadID string) ([]MultipartPart, error)
	// ListSessions returns bucket's sessions ordered by (object_key, created_at,
	// upload_id) — the S3 ListMultipartUploads presentation order. Filtering
	// (prefix/markers/max) happens in the handler; in-flight session counts are
	// small.
	ListSessions(ctx context.Context, bucket string) ([]MultipartSession, error)
	// ListStaleSessions returns sessions in `state` created before cutoff, for
	// the abandoned-upload sweeper.
	ListStaleSessions(ctx context.Context, state string, cutoff time.Time) ([]MultipartSession, error)
	// CountPartRefs returns how many parts OUTSIDE excludeUploadID reference
	// digest — the shared-blob guard for abort/supersede spool cleanup
	// (content-addressed part blobs may be deduped across sessions).
	CountPartRefs(ctx context.Context, digest []byte, excludeUploadID string) (int, error)
}

// GCStore records superseded MST node CIDs (§4). Write-only this iteration.
type GCStore interface {
	AddGCCandidate(ctx context.Context, cidBytes []byte, bucket string) error
}
