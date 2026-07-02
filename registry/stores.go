package registry

import (
	"context"
	"errors"
	"fmt"
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
	Space     string
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

// BlobLocation is one row of ingot.blob_locations: the full per-blob record
// keyed by (Space, Digest) — where the blob can be retrieved from (captured at
// accept) and, when the blob is an encrypted FEE envelope, the wrap material
// the read path needs to decrypt it.
//
// The FEE fields are populated only for encrypted blobs; they are the cached
// inputs a (range) GET's aesstream decryptor needs, so a read unwraps the CEK
// and goes straight to a body-range fetch with no COSE envelope-header
// round-trip. They are all-or-nothing: an unencrypted blob leaves them zero
// (stored as SQL NULL), and PutLocation rejects a partial set via ValidateFEE
// so no row is ever persisted that the decrypt path cannot use. A fresh CEK per
// encryption event makes every ciphertext digest unique to one encryption, so
// this wrap material is a 1:1 fact about the row. Raw CEK bytes are never
// stored — only the region-KEK-wrapped CEK and the identifiers needed to unwrap
// it. See docs/architecture.md §8 and FIL-480.
type BlobLocation struct {
	Space    string
	Digest   []byte
	Provider string
	URL      string
	Size     int64

	// RegionWrappedCEK is the content-encryption key wrapped under the region
	// KEK (A256KW). Nil for an unencrypted blob; its presence marks the blob as
	// an encrypted FEE envelope. Never the raw CEK.
	RegionWrappedCEK []byte
	// RegionKeyVersion identifies which version of the region KEK wrapped the
	// CEK, so a rotation can re-wrap in place. Opaque, so it is agnostic to the
	// region-key cardinality decision (FIL-572). Empty for an unencrypted blob.
	RegionKeyVersion string
	// TenantRecipientKID identifies the Hilt wrap key used for insurance-recovery
	// unwrap. Opaque, so it is agnostic to the tenant-vs-bucket granularity
	// decision (FIL-574). Empty for an unencrypted blob.
	TenantRecipientKID string
	// BaseNonce is the COSE iv: the STREAM nonce seed for this blob's ciphertext.
	// Nil for an unencrypted blob.
	BaseNonce []byte
	// ChunkSize is the FEE chunk size written into the COSE protected header,
	// cached so the read path need not fetch and decode the envelope header.
	// Zero for an unencrypted blob.
	ChunkSize int64
	// ProtectedHeader is the raw COSE protected header bytes, cached to
	// reconstruct the Enc_structure (AAD) without an envelope round-trip. Nil for
	// an unencrypted blob.
	ProtectedHeader []byte
}

// ErrPartialFEE is returned by PutLocation when a BlobLocation carries some but
// not all of the FEE wrap material — a row the decrypt path could not use.
var ErrPartialFEE = errors.New("registry: partial FEE wrap material")

// ValidateFEE enforces the all-or-nothing FEE invariant that BlobLocation
// documents: either every wrap field is present (non-empty byte slices, non-empty
// identifiers, and ChunkSize > 0) or none is. A partial set — e.g. a wrapped CEK
// with no nonce, or an empty (non-nil) byte slice standing in for real material —
// would persist a row a later GET cannot decrypt, so PutLocation rejects it.
func (loc BlobLocation) ValidateFEE() error {
	present := 0
	for _, ok := range []bool{
		len(loc.RegionWrappedCEK) > 0,
		loc.RegionKeyVersion != "",
		loc.TenantRecipientKID != "",
		len(loc.BaseNonce) > 0,
		loc.ChunkSize > 0,
		len(loc.ProtectedHeader) > 0,
	} {
		if ok {
			present++
		}
	}
	if present != 0 && present != 6 {
		return fmt.Errorf("%w: %d of 6 fields set", ErrPartialFEE, present)
	}
	return nil
}

// MultipartSession is one row of ingot.multipart_sessions.
type MultipartSession struct {
	UploadID    string
	Bucket      string
	ObjectKey   string
	State       string
	ContentType string
	Metadata    map[string]string
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
}

// BlobRefStore is the reverse reference index (§5, §6). A commit adds a
// claim per body digest; a delete/overwrite removes it; CountClaims gates
// remove(digest) (physical reclamation when the count reaches zero).
type BlobRefStore interface {
	AddBlobClaim(ctx context.Context, claim BlobClaim) error
	DeleteBlobClaim(ctx context.Context, digest []byte, bucket, objectKey, versionID string) error
	// CountClaims returns how many object versions in space still reference
	// digest. Zero means the space's claim may be released.
	CountClaims(ctx context.Context, space string, digest []byte) (int, error)
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

// LocationStore is the local blob-location table (§8, appliance topology): the
// per-blob record keyed by (space, digest), captured at accept and resolved on
// read in place of the indexing-service. Besides the provider/URL/size that
// locate the bytes, a row also carries the FEE wrap material for an encrypted
// blob (see BlobLocation) — PutLocation writes it, GetLocation reads it back.
// Per-blob crypto-shred is nulling that material (write a row with the FEE
// fields zero) or DeleteLocation; a rotation re-wrap is a PutLocation with a new
// RegionWrappedCEK/RegionKeyVersion.
type LocationStore interface {
	PutLocation(ctx context.Context, loc BlobLocation) error
	GetLocation(ctx context.Context, space string, digest []byte) (*BlobLocation, error)
	DeleteLocation(ctx context.Context, space string, digest []byte) error
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
}

// GCStore records superseded MST node CIDs (§4). Write-only this iteration.
type GCStore interface {
	AddGCCandidate(ctx context.Context, cidBytes []byte, bucket string) error
}
