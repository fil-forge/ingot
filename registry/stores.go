package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fil-forge/ucantone/did"
)

// This file defines the relational surface the upload/storage/delete
// architecture relies on (docs/architecture.md §5–§7, Appendix C):
// the reverse reference index (blob_refs), the local-store index
// (upload_intents), the local blob-location table (blob_locations), the
// per-blob FEE encryption parameters (blob_encryption_params), the
// multipart session/part tables, and the gc_candidates log. The stores
// are split into focused interfaces so a caller can depend on only what
// it needs; *Postgres and *inmem.MemStore satisfy all of them.
//
// NullVersionID is S3's version id for a null version — the version written
// while a bucket is unversioned or Suspended (docs/s3-versioning.md §3). It
// names those versions' blob_refs rows and answers `?versionId=null`.
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

// BlobEncryptionParams is one row of ingot.blob_encryption_params: the FEE
// (FilOne encryption envelope) parameters a read needs to decrypt an encrypted
// blob, keyed by (Space, Digest) like the blob's location.
//
// An encrypted body blob is an independent COSE/STREAM ciphertext envelope.
// These are the cached inputs a (range) GET's decryptor needs, so a read
// unwraps the CEK and goes straight to a body-range fetch with no COSE
// envelope-header round-trip. Existence of the row is what marks a blob as
// encrypted: every field is required, and an unencrypted blob simply has no row
// (see EncryptionParamsStore). A fresh CEK per encryption event makes every
// ciphertext digest unique to one encryption, so these parameters are a 1:1
// fact about the blob. Raw CEK bytes are never stored — only the
// region-KEK-wrapped CEK and the identifiers needed to unwrap it. See
// docs/architecture.md §8 and FIL-480.
type BlobEncryptionParams struct {
	Space  did.DID
	Digest []byte

	// RegionWrappedCEK is the content-encryption key wrapped under the region
	// KEK (A256KW). Never the raw CEK.
	RegionWrappedCEK []byte
	// RegionKeyVersion identifies which version of the region KEK wrapped the
	// CEK, so a rotation can re-wrap in place. Opaque, so it is agnostic to the
	// region-key cardinality decision (FIL-572).
	RegionKeyVersion string
	// TenantRecipientKID identifies the Hilt wrap key used for insurance-recovery
	// unwrap. Opaque, so it is agnostic to the tenant-vs-bucket granularity
	// decision (FIL-574).
	TenantRecipientKID string
	// HeaderLen is the encoded length of the blob's COSE envelope, and so the
	// offset at which its ciphertext begins. Without it a read could not locate
	// byte 0 of the ciphertext without decoding the header.
	HeaderLen int64
	// BaseNonce is the COSE iv: the STREAM nonce seed for this blob's ciphertext.
	BaseNonce []byte
	// ChunkSize is the FEE chunk size written into the COSE protected header,
	// cached so the read path need not fetch and decode the envelope header.
	ChunkSize int64
	// AAD is the envelope's COSE Enc_structure — the additional authenticated
	// data bound into every chunk's GCM tag. Stored whole rather than as the
	// protected header because the Enc_structure's context string differs
	// between a COSE_Encrypt and a COSE_Encrypt0, which a bare row cannot
	// record. The protected header remains recoverable from it as element 1.
	AAD []byte
}

// ErrInvalidEncryptionParams is returned by PutEncryptionParams when a
// BlobEncryptionParams is missing a field — a row the decrypt path could not
// use.
var ErrInvalidEncryptionParams = errors.New("registry: invalid blob encryption params")

// Validate enforces that every encryption parameter is present: non-empty byte
// slices and identifiers, and positive HeaderLen/ChunkSize. A partial set — e.g.
// a wrapped CEK with no nonce, or an empty (non-nil) byte slice standing in for
// real material — would persist a row a later GET cannot decrypt, so
// PutEncryptionParams rejects it before touching the store.
func (p BlobEncryptionParams) Validate() error {
	var missing []string
	for _, f := range []struct {
		name    string
		present bool
	}{
		{"space", p.Space.String() != ""},
		{"digest", len(p.Digest) > 0},
		{"region_wrapped_cek", len(p.RegionWrappedCEK) > 0},
		{"region_key_version", p.RegionKeyVersion != ""},
		{"tenant_recipient_kid", p.TenantRecipientKID != ""},
		{"header_len", p.HeaderLen > 0},
		{"base_nonce", len(p.BaseNonce) > 0},
		{"chunk_size", p.ChunkSize > 0},
		{"aad", len(p.AAD) > 0},
	} {
		if !f.present {
			missing = append(missing, f.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: missing %s", ErrInvalidEncryptionParams, strings.Join(missing, ", "))
	}
	return nil
}

// ValidateRewrap enforces the two parameters a re-wrap replaces, so a rotation
// cannot blank out the material the decrypt path needs. Every
// EncryptionParamsStore implementation calls it from RewrapEncryptionParams.
func ValidateRewrap(wrappedCEK []byte, keyVersion string) error {
	var missing []string
	if len(wrappedCEK) == 0 {
		missing = append(missing, "region_wrapped_cek")
	}
	if keyVersion == "" {
		missing = append(missing, "region_key_version")
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: missing %s", ErrInvalidEncryptionParams, strings.Join(missing, ", "))
	}
	return nil
}

// BlobInclusion is one row of ingot.shard_inclusions: block Digest lives at
// the inclusive byte range [RangeStart, RangeEnd] inside the shard CAR whose
// own location is the (Space, ShardDigest) row of blob_locations. It is the
// local mirror of the sharded-dag-index published at ship time, and the second
// half of the indexing-service contract the appliance read tier mimics
// (locations + inclusions).
type BlobInclusion struct {
	Space       did.DID
	Digest      []byte // inner block multihash
	ShardDigest []byte // enclosing shard CAR multihash
	RangeStart  int64
	RangeEnd    int64 // inclusive
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
	// CreateMultipartUpload. They select the per-part checksum algorithm at
	// UploadPart and the final checksum derivation (COMPOSITE vs FULL_OBJECT)
	// at Complete, and are echoed by ListParts and ListMultipartUploads.
	ChecksumAlgorithm string
	ChecksumType      string
	// LockMode / LockRetainUntil / LockLegalHold carry CreateMultipartUpload's
	// x-amz-object-lock-* headers to Complete, which stamps them onto the
	// version it commits (docs/s3-object-lock.md §7). LockMode is the
	// retention mode ("GOVERNANCE"/"COMPLIANCE"); LockLegalHold is the
	// legal-hold status ("ON"/"OFF"); empty / nil when the header was absent.
	LockMode        string
	LockRetainUntil *time.Time
	LockLegalHold   string
	// Tagging carries CreateMultipartUpload's raw x-amz-tagging header
	// (validated at create) to Complete, which stamps the parsed set
	// (docs/s3-object-tagging.md §4). Empty when the header was absent.
	Tagging   string
	Metadata  map[string]string
	CreatedAt time.Time
}

// MultipartPart is one row of ingot.multipart_parts. BlobDigests is the
// ordered list of blobs the part split into (one entry unless the part
// exceeded max_blob_size).
type MultipartPart struct {
	UploadID   string
	PartNumber int
	ETagMD5    []byte
	Size       int64
	// Checksum is the part's base64 checksum. When the session declares a
	// checksum algorithm it is that algorithm's value (client-validated or
	// server-computed); when the session declares none it is the internal
	// full-object CRC64NVME used to derive the default final checksum at
	// Complete, and is never echoed by ListParts.
	Checksum    string
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

// EncryptionParamsStore is the per-blob FEE encryption-parameter table
// (blob_encryption_params): what a read needs to decrypt an encrypted blob
// without fetching its envelope header. A blob has a row only when it is
// encrypted, so GetEncryptionParams returning ErrNotFound is the answer "this
// blob is stored as plaintext".
//
// It is deliberately separate from LocationStore, with no foreign key between
// the tables: a location is reconstructible from the indexer or the accept
// receipt, a wrapped CEK is not. Put is an upsert, so re-encrypting a blob
// replaces its whole parameter set; a region-key rotation instead calls
// RewrapEncryptionParams, which touches only the key material. Delete is the
// per-blob crypto-shred and is idempotent — and because nothing cascades,
// DeleteLocation does NOT shred: a caller removing a blob must call both.
type EncryptionParamsStore interface {
	PutEncryptionParams(ctx context.Context, params BlobEncryptionParams) error
	GetEncryptionParams(ctx context.Context, space did.DID, digest []byte) (*BlobEncryptionParams, error)
	DeleteEncryptionParams(ctx context.Context, space did.DID, digest []byte) error
	// RewrapEncryptionParams replaces the wrapped CEK and its key version for an
	// already-encrypted blob, leaving every other parameter untouched — the write
	// a region-key rotation performs. The ciphertext is unchanged, so the nonce,
	// chunk size, AAD and header length must survive the rotation, and a rotation
	// that rewrote them would corrupt the row. Returns ErrNotFound when the blob
	// has no row (nothing to re-wrap) and ErrInvalidEncryptionParams when either
	// argument is empty.
	RewrapEncryptionParams(ctx context.Context, space did.DID, digest, wrappedCEK []byte, keyVersion string) error
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

// InclusionStore is the local shard-inclusion table (§8): block digest →
// (shard digest, byte range) for every block of a shipped catalog segment,
// written by the flush path before the segment is marked shipped. Resolved on
// read (after the local segment is retired) in place of the indexing-service's
// index claims.
type InclusionStore interface {
	// PutInclusions records a batch of inclusions (one shipped shard's worth).
	// Re-ships of the same blocks are idempotent.
	PutInclusions(ctx context.Context, incs []BlobInclusion) error
	GetInclusion(ctx context.Context, space did.DID, digest []byte) (*BlobInclusion, error)
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
