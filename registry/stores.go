package registry

import (
	"context"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
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

// upload_intents.state values (the local-store lifecycle, §5): spooled on
// ingest (uploading from the start for a blob streamed as it spools),
// uploading once its upload has started, parked once durable on a provider
// with the accept deferred, accepted once the provider has accepted it (at
// once for a blob already stored for the space), and published once a
// bucket commit has claimed it. Published is written with the blob's first
// reference claim, in the same transaction, and never changes: it is the
// durable record that the blob was committed. Its release removes the local
// copy (spool or cache) and the intent, as it does for a blob that was never
// committed.
const (
	IntentSpooled = "spooled"
	// IntentUploading is set before a blob's first network call and stands
	// until a row records the outcome (a park or a location): the blob may
	// be on the network. A blob still 'spooled' never left this node, which
	// a release relies on to skip a network remove it could not authorize
	// from the background.
	IntentUploading = "uploading"
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
	Digest    multihash.Multihash
	Bucket    string
	ObjectKey string
	VersionID string
	Space     did.DID
}

// UploadIntent is one row of ingot.upload_intents: a blob Ingot holds on
// disk, with its lifecycle state for one space. Keyed by (Space, Digest): a
// blob stored as received has the digest of its bytes, so the same bytes
// written to two spaces are one spool file and two intents, each tracking
// its own space's upload. The file is shared, so it may go only when no
// intent for the digest still needs it (see ListEvictable, CountOtherIntents).
type UploadIntent struct {
	Space  did.DID
	Digest multihash.Multihash
	// LocalPath is where the blob was written: its spool path. Once the
	// provider holds the blob its copy may move to the cache, so nothing
	// reads a blob through this path.
	LocalPath string
	Size      int64
	State     string
	Bucket    string
	// UpdatedAt is when State last changed (for a committed blob, its commit
	// time). Filled on every read; ignored by PutIntent.
	UpdatedAt time.Time
}

// StalledSizes is StalledBytes's sum for each state a stalled upload can be
// left in: Spooled, a body that never left this node; Uploading, one whose
// upload started and may have reached its provider.
type StalledSizes struct {
	Spooled, Uploading int64
}

// EvictCursor is a position in ListEvictable's (updated_at, digest, space)
// order. The zero value starts at the beginning.
type EvictCursor struct {
	UpdatedAt time.Time
	Digest    multihash.Multihash
	Space     did.DID
}

// BlobLocation is one row of ingot.blob_locations: where a blob can be
// retrieved from, captured at accept. Keyed by (Space, Digest).
type BlobLocation struct {
	Space    did.DID
	Digest   multihash.Multihash
	Provider string
	URL      string
	Size     int64
}

// BlobEncryptionParams is one row of ingot.blob_encryption_params: the FEE
// (Filecoin Encryption Envelope) parameters a read needs to decrypt an
// encrypted blob, keyed by (Space, Digest) like the blob's location.
//
// An encrypted body blob is an independent COSE/STREAM ciphertext envelope.
// These are the cached inputs a (range) GET's decryptor needs, so a read
// unwraps the CEK and goes straight to a body-range fetch with no COSE
// envelope-header round-trip. Existence of the row is what marks a blob as
// encrypted: every field is required, and an unencrypted blob simply has no row
// (see EncryptionParamsStore). A fresh CEK per encryption event makes every
// ciphertext digest unique to one encryption, so these parameters are a 1:1
// fact about the blob.
//
// Only what the region-KEK read path needs is stored. The COSE recipients,
// including the tenant wrap key the insurance-recovery unwrap uses, stay in the
// envelope header: that path is rare and out-of-band, and it reads the header
// anyway. Raw CEK bytes are never stored — only the region-KEK-wrapped CEK and
// the identifiers needed to unwrap it.
type BlobEncryptionParams struct {
	Space  did.DID
	Digest multihash.Multihash

	// RegionWrappedCEK is the content-encryption key wrapped by the region key
	// provider (e.g. secrets-manager transit ciphertext). Never the raw CEK:
	// unwrapping it requires the region KEK, which never leaves the provider.
	RegionWrappedCEK []byte
	// RegionKeyVersion identifies which version of the region KEK wrapped the
	// CEK, so a rotation can re-wrap in place. Opaque, so it is agnostic to the
	// region-key cardinality decision (FIL-572).
	RegionKeyVersion string
	// HeaderLen is the encoded length of the blob's COSE envelope, and so the
	// offset at which its ciphertext begins. Without it a read could not locate
	// byte 0 of the ciphertext without decoding the header.
	HeaderLen int64
	// AAD is the envelope's COSE Enc_structure — the additional authenticated
	// data bound into every chunk's GCM tag. Stored whole rather than as the
	// protected header because the Enc_structure's context string differs
	// between a COSE_Encrypt and a COSE_Encrypt0, which a bare row cannot
	// record. The protected header is its element 1, and carries the FEE base
	// nonce, chunk size and plaintext length, so the read path reads them from
	// here (see fee.BodyDescriptor) rather than from the envelope header.
	AAD []byte
}

// BlobInclusion is one row of ingot.shard_inclusions: block Digest lives at
// the inclusive byte range [RangeStart, RangeEnd] inside the shard CAR whose
// own location is the (Space, ShardDigest) row of blob_locations. It is the
// local mirror of the sharded-dag-index published at ship time, and the second
// half of the indexing-service contract the appliance read tier mimics
// (locations + inclusions).
type BlobInclusion struct {
	Space       did.DID
	Digest      multihash.Multihash // inner block
	ShardDigest multihash.Multihash // enclosing shard CAR
	RangeStart  int64
	RangeEnd    int64 // inclusive
}

// MultipartSession is one row of ingot.multipart_sessions. The HTTP metadata
// headers (ContentEncoding..Expires) are captured at CreateMultipartUpload so
// Complete can write them into the manifest exactly like a single-shot PUT.
type MultipartSession struct {
	UploadID  string
	Bucket    string
	ObjectKey string
	// Space is the bucket's Forge space, recorded at create and required: it
	// is the subject of every release of the session's blobs, and a teardown
	// never resolves it from the bucket name, which a later bucket of the
	// same name would answer with another space.
	Space                   did.DID
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
	Tagging  string
	Metadata map[string]string
	// Plaintext is whether the session's object is stored as received, with
	// no encryption: decided at CreateMultipartUpload from the bucket's
	// encryption and the request's x-amz-server-side-encryption header,
	// applied to every part, and stamped onto the manifest at Complete.
	Plaintext bool
	// CommittedETag / CommittedVersionID are the result the winning Complete
	// returned, recorded by CompleteSession with the move to 'completed'
	// (empty before that). A duplicate or latch-losing Complete replays them
	// rather than resolving the live key, which a later overwrite may have
	// replaced. CommittedVersionID is empty for unversioned buckets.
	CommittedETag      string
	CommittedVersionID string
	CreatedAt          time.Time
	// StateChangedAt is when State last changed (creation, for a session
	// still open). The sweeper measures staleness from it, so a Complete
	// that latches an old session to 'completing' holds the row for a TTL
	// of its own.
	StateChangedAt time.Time
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
	BlobDigests []multihash.Multihash
	State       string
	CreatedAt   time.Time
}

// PartTree is the BLAKE3 tree material UploadPart records for one part: the
// part hashed at the object offset it was assumed to sit at, as blake3tree
// produces it (a blake3tree.Range, serialized). It lives in its own row
// (multipart_part_trees), written by PutPart beside the part, read back by
// ListPartTrees for Complete alone, and dropped by DeletePartTrees once the
// object is committed. Complete checks the assumed offset against the true
// one and, when they agree, merges this into the object's tree without
// reading the part again; a part without a tree (recorded before the tree
// existed, or a copy that read no bytes) is re-hashed.
type PartTree struct {
	// Offset is the object byte offset the part was hashed at: the offset
	// UploadPart assumed from the parts recorded so far, which Complete
	// checks against the part's true offset.
	Offset int64
	// ChunkLog is the block size of Blocks as a base-2 exponent of BLAKE3
	// chunks: that of a body ending where the part ends, the finest block
	// the object can have when Offset proves true.
	ChunkLog uint8
	// Nodes holds the aligned subtrees of the part's whole chunks, encoded
	// with blake3tree.EncodeSubtrees.
	Nodes []byte
	// Blocks holds the part's blocks at ChunkLog, encoded with
	// blake3tree.EncodeSubtrees.
	Blocks []byte
	// Head is the part's bytes before its first chunk boundary, under 1 KiB
	// and empty when the part's offset is a 1 KiB multiple. Complete hashes
	// the chunk straddling a part boundary from the previous part's Tail and
	// this part's Head.
	Head []byte
	// Tail is the part's bytes after its last chunk boundary, under 1 KiB
	// and empty when the part ends on one. For the last part it is the
	// object's final chunk, which Complete hashes.
	Tail []byte
	// Root is the part's own BLAKE3 hash, set only when Offset is 0: a
	// single-part completion takes its digest from here.
	Root []byte
}

// BlobRefStore is the reverse reference index (§5, §6). A commit adds a
// claim per body digest; a delete/overwrite removes it; CountClaims gates
// remove(digest) (physical reclamation when the count reaches zero).
type BlobRefStore interface {
	// AddBlobClaim records an object version's claim on a digest and, in the
	// same transaction, marks the space's upload intent for the digest
	// published (see IntentPublished). Idempotent. Under the (space, digest)
	// claim-key lock it refuses with ErrReleaseInProgress while a started
	// release holds the digest (PendingReleaseStore.StartRelease), or when
	// the space has no location for the digest: a claim landing then would
	// name a blob a release is removing, or has removed since the write
	// settled it. Every committed body blob is located by its commit, so a
	// claim with no location is one such write's.
	AddBlobClaim(ctx context.Context, claim BlobClaim) error
	// AddBlobClaims is AddBlobClaim for every claim of one generation, all
	// in one transaction, each under its claim-key lock, taken in digest
	// order: a digest refused for a started release records nothing for
	// the others, so a commit that fails leaves no claim behind that no
	// generation will ever drop.
	AddBlobClaims(ctx context.Context, claims []BlobClaim) error
	// PinBlobClaims is AddBlobClaim for digests borrowed from another object
	// of the same space (a same-space copy pins the source's body). Each
	// claim is recorded only while the space still holds a claim on its
	// digest, all in one transaction, and the transaction excludes
	// DropClaimEnqueueRelease on the same (space, digest), so a pin and the
	// drop of the digest's last claim commit in one order or the other: the
	// drop counts the pin, or the pin finds no claim. Reports whether every
	// claim is recorded; false records nothing and means some digest's last
	// claim is gone and the blob is being released. Idempotent.
	PinBlobClaims(ctx context.Context, claims []BlobClaim) (bool, error)
	DeleteBlobClaim(ctx context.Context, digest multihash.Multihash, bucket, objectKey, versionID string) error
	// CountClaims returns how many object versions in space still reference
	// digest. Zero means the space's claim may be released.
	CountClaims(ctx context.Context, space did.DID, digest multihash.Multihash) (int, error)
	// DropClaimEnqueueRelease deletes one claim row and, when it was the
	// space's last claim on digest, records a release intent due at
	// notBefore — atomically, so no crash window separates "last claim
	// gone" from "release recorded". Reports whether an intent was
	// enqueued.
	DropClaimEnqueueRelease(ctx context.Context, digest multihash.Multihash, bucket, objectKey, versionID string, space did.DID, notBefore time.Time) (bool, error)
}

// PendingRelease is one deferred blob release: executed by the release
// sweeper once not_before passes and the digest still has zero claims.
type PendingRelease struct {
	Space     did.DID
	Digest    multihash.Multihash
	NotBefore time.Time
	// StartedAt is when a release took the record (StartRelease): the blob
	// is being removed from its provider, or was and a later step failed.
	// Zero until then.
	StartedAt time.Time
}

// ReleaseStart is StartRelease's answer: whether the release may go ahead,
// and if not, why.
type ReleaseStart int

const (
	// ReleaseStarted: the record is marked started and the blob is this
	// release's to remove.
	ReleaseStarted ReleaseStart = iota
	// ReleaseCancelled: no record; a write reusing the blob cancelled it.
	ReleaseCancelled
	// ReleaseClaimed: an object version claims the blob again; the record
	// is stale.
	ReleaseClaimed
	// ReleaseReferenced: a part of one of the space's in-flight multipart
	// sessions names the blob; the release waits.
	ReleaseReferenced
)

// ReleaseCancel is CancelRelease's answer.
type ReleaseCancel int

const (
	// ReleaseNone: no record; the blob is not being released.
	ReleaseNone ReleaseCancel = iota
	// ReleaseCancelledByWrite: the record was pending and is now deleted;
	// the blob is the caller's to reuse.
	ReleaseCancelledByWrite
	// ReleaseInProgress: the record is started; the blob is going, and the
	// caller waits for the record to disappear and uploads afresh.
	ReleaseInProgress
)

// PendingReleaseStore is the deferred-release queue (blob_release_intents):
// the durable record between "last claim dropped" and "release executed"
// (crypto-shred + location delete + network remove). Enqueue upserts,
// keeping the later not_before.
type PendingReleaseStore interface {
	// EnqueueRelease records that (space, digest) is to be released once
	// notBefore has passed. Upsert: an existing record keeps the later of the
	// two not_before values.
	EnqueueRelease(ctx context.Context, space did.DID, digest multihash.Multihash, notBefore time.Time) error
	// EnqueueReleases is EnqueueRelease for many digests in one statement,
	// for a session teardown with a blob per part. Repeated digests are
	// recorded once. Returns the records as they stand afterwards: a digest
	// that already had a record keeps its later not_before, so the caller
	// can tell which releases are due now.
	EnqueueReleases(ctx context.Context, space did.DID, digests []multihash.Multihash, notBefore time.Time) ([]PendingRelease, error)
	// ListDueReleases returns intents with not_before <= now, oldest first,
	// at most limit. Started records are listed too: a release that failed
	// after starting is retried.
	ListDueReleases(ctx context.Context, now time.Time, limit int) ([]PendingRelease, error)
	// StartRelease decides, under the (space, digest) claim-key lock, whether
	// the record's release may go ahead, and marks it started if so. The
	// checks run in this order on purpose: a Complete adds its claims while
	// its session is still in flight and only then marks it completed, so a
	// release that read the claims first could see none, lose the race to
	// the commit, and then find no live session either; reading the live
	// part references first closes that window. A record already started
	// (a retry) starts again without the checks: the release has owned the
	// blob since it started, no claim can land meanwhile (AddBlobClaim), and
	// a part recorded since belongs to a write waiting for it to finish. A
	// record found claimed is stale and is deleted here, under the lock: a
	// caller deleting it afterwards by key could take a replacement that a
	// later drop of that claim enqueued in between.
	// The lock excludes CancelRelease, so a write reusing the blob either
	// cancels the record before this, and this finds none, or finds it
	// started and waits.
	StartRelease(ctx context.Context, space did.DID, digest multihash.Multihash) (ReleaseStart, error)
	// CancelRelease is a write's side of StartRelease: under the same lock,
	// it deletes a record not yet started, so the blob is the write's to
	// reuse, or reports one that is started, which the write waits out, or
	// none.
	CancelRelease(ctx context.Context, space did.DID, digest multihash.Multihash) (ReleaseCancel, error)
	// ListReleasesBySpace returns every intent for space regardless of
	// not_before — DeleteBucket executes a space's releases immediately (the
	// bucket is empty and its deletion is explicit, so no reader grace is
	// owed) before asking hilt to delete the space.
	ListReleasesBySpace(ctx context.Context, space did.DID) ([]PendingRelease, error)
	DeleteRelease(ctx context.Context, space did.DID, digest multihash.Multihash) error
	// DeleteIntentAndRelease removes the space's upload intent for the blob
	// and this release's record in one transaction — the last step of
	// releasing a blob — and returns how many intents for the digest other
	// spaces still hold, counted under a lock on the digest, so of two
	// spaces releasing the same digest at once exactly one counts none and
	// removes the shared file. The two deletes must go together: the intent
	// is the only evidence of how far the blob ever got, and a record that
	// outlived it would leave the retry to read a blob with no rows and no
	// intent, owing a network remove that a never-uploaded blob has no
	// authority for and that would fail on every attempt. Both deletes are
	// idempotent.
	DeleteIntentAndRelease(ctx context.Context, space did.DID, digest multihash.Multihash) (int, error)
}

// IntentStore is the local-store index (§5): the on-disk blobs Ingot holds
// and their lifecycle state. Drives read-after-write, cache lookup, spool
// eviction, and crash recovery.
type IntentStore interface {
	// PutIntent upserts the (Space, Digest) intent. A digest spooled again is
	// on disk again, so PutIntent also clears the evicted mark, on every
	// intent for the digest: the file is one.
	PutIntent(ctx context.Context, in UploadIntent) error
	// SetIntentState moves the space's intent for digest to state. UpdatedAt
	// records the last state change, which orders eviction, so setting the
	// state an intent already has (a retry) leaves it alone.
	SetIntentState(ctx context.Context, space did.DID, digest multihash.Multihash, state string) error
	GetIntent(ctx context.Context, space did.DID, digest multihash.Multihash) (*UploadIntent, error)
	ListIntentsByState(ctx context.Context, state string) ([]UploadIntent, error)
	DeleteIntent(ctx context.Context, space did.DID, digest multihash.Multihash) error
	// CountOtherIntents returns how many spaces other than space hold an
	// intent for digest. Zero means the local file is this space's alone to
	// remove; otherwise another space's upload or read may still use it,
	// and its removal is left to that space's release or to eviction.
	CountOtherIntents(ctx context.Context, space did.DID, digest multihash.Multihash) (int, error)
	// ListEvictable returns intents whose spool file the provider already
	// holds a copy of and that are not marked evicted: 'accepted' or
	// 'published' with a blob_locations row for the intent's space and
	// digest, or 'parked' with a blob_parks row for them. The state alone is
	// not enough: a release drops the location before the intent, and a
	// failure between the two leaves an accepted intent with none. The file
	// is shared by every space's intent for the digest, so a digest with an
	// intent still 'spooled' or 'uploading' in any space is not listed: that
	// upload reads the file. Ordered by (UpdatedAt, Digest, Space), starting
	// after the cursor, at most limit rows.
	ListEvictable(ctx context.Context, after EvictCursor, limit int) ([]UploadIntent, error)
	// MarkEvicted records that the digest's spool file is gone, on every
	// intent for the digest whose last state change is not after notAfter,
	// leaving State and UpdatedAt alone. An intent changed since belongs to
	// a write that spooled the bytes again after the caller listed or read
	// the intent it is acting on: that write's file is on disk, and its mark
	// is left clear. Returns ErrNotFound when no intent is left (a release
	// got there first).
	MarkEvicted(ctx context.Context, digest multihash.Multihash, notAfter time.Time) error
	// MissingIntents returns the digests among digests that have no intent
	// row in any space, for the sweeper's orphan-file pass.
	MissingIntents(ctx context.Context, digests []multihash.Multihash) ([]multihash.Multihash, error)
	// ListReleasedPublished returns up to limit published intents that
	// nothing names any more: no location, claim, pending release or part of
	// an open or completing multipart session has the digest, in any space,
	// and no other space holds an intent for it. Those are committed blobs
	// whose release finished while releases still kept the local copy and
	// the intent; the startup released pass deletes both. Each digest listed
	// has that one intent. Ordered by digest, starting after the digest
	// after (nil starts at the beginning), so a caller pages through rather
	// than re-scanning.
	ListReleasedPublished(ctx context.Context, after multihash.Multihash, limit int) ([]UploadIntent, error)
	// DeleteReleasedPublished deletes the space's intent for digest if it
	// still meets ListReleasedPublished's conditions, and reports whether it
	// did. The check and the delete are one statement, so a digest named
	// again since it was listed keeps its intent.
	DeleteReleasedPublished(ctx context.Context, space did.DID, digest multihash.Multihash) (bool, error)
	// StalledBytes sums the sizes of 'spooled' and 'uploading' intents whose
	// state last changed before before, by state: bodies whose upload has
	// outlasted any live request, which nothing reclaims.
	StalledBytes(ctx context.Context, before time.Time) (StalledSizes, error)
}

// LocationStore is the local blob-location table (§8, appliance topology):
// the (space, digest) → provider/URL mapping captured at accept, resolved
// on read in place of the indexing-service.
type LocationStore interface {
	PutLocation(ctx context.Context, loc BlobLocation) error
	GetLocation(ctx context.Context, space did.DID, digest multihash.Multihash) (*BlobLocation, error)
	// GetLocations returns the space's locations for digests in one query,
	// keyed by string(digest). A digest with no location is absent from the
	// map; that is not an error.
	GetLocations(ctx context.Context, space did.DID, digests []multihash.Multihash) (map[string]BlobLocation, error)
	DeleteLocation(ctx context.Context, space did.DID, digest multihash.Multihash) error
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
// per-blob crypto-shred (without the wrapped CEK the region has no path to the
// plaintext) and is idempotent — and because nothing cascades, DeleteLocation
// does NOT shred: a caller removing a blob must call both.
type EncryptionParamsStore interface {
	PutEncryptionParams(ctx context.Context, params BlobEncryptionParams) error
	GetEncryptionParams(ctx context.Context, space did.DID, digest multihash.Multihash) (*BlobEncryptionParams, error)
	// GetEncryptionParamsBatch returns the space's rows for digests in one
	// query, keyed by string(digest). A digest with no row is absent from the
	// map; that is not an error.
	GetEncryptionParamsBatch(ctx context.Context, space did.DID, digests []multihash.Multihash) (map[string]BlobEncryptionParams, error)
	DeleteEncryptionParams(ctx context.Context, space did.DID, digest multihash.Multihash) error
	// RewrapEncryptionParams replaces the wrapped CEK and its key version for an
	// already-encrypted blob, leaving every other parameter untouched — the write
	// a region-key rotation performs. The ciphertext is unchanged, so the nonce,
	// chunk size, AAD and header length must survive the rotation, and a rotation
	// that rewrote them would corrupt the row. Returns ErrNotFound when the blob
	// has no row (nothing to re-wrap).
	RewrapEncryptionParams(ctx context.Context, space did.DID, digest multihash.Multihash, wrappedCEK []byte, keyVersion string) error
}

// BlobPark is one row of ingot.blob_parks: the persistable state of a blob
// that is durable on its provider but not yet accepted (multipart's deferred
// conclude, §7.2). AddTask/AcceptTask are the /blob/add and
// /blob/accept task CIDs; PutInvocation is the sealed /http/put invocation
// whose metadata carries the derived signer keys needed to conclude —
// sensitive, deleted at conclude/abort. Keyed by (Space, Digest), like
// upload_intents: the allocation it records is the space's, so another
// space parking the same bytes allocates and parks its own.
type BlobPark struct {
	Space         did.DID
	Digest        multihash.Multihash
	AddTask       []byte // cid bytes
	AcceptTask    []byte // cid bytes
	PutInvocation []byte
	Size          int64
	CreatedAt     time.Time
}

// ParkStore persists deferred-accept park state between UploadPart and
// Complete/Abort (§7.2).
type ParkStore interface {
	// PutPark records a park and reports whether it did: a park already
	// recorded for the (space, digest), by a write in another instance that
	// parked the same bytes first, is left as it is, and the caller gives up
	// its own allocation instead.
	PutPark(ctx context.Context, p BlobPark) (bool, error)
	// GetPark returns ErrNotFound when the space has no park row for digest.
	GetPark(ctx context.Context, space did.DID, digest multihash.Multihash) (*BlobPark, error)
	DeletePark(ctx context.Context, space did.DID, digest multihash.Multihash) error
	// HasParkFor reports whether a park row records the /blob/add task
	// addTask (cid bytes).
	HasParkFor(ctx context.Context, addTask []byte) (bool, error)
}

// BlobStream is one row of ingot.blob_streams: a body blob allocated by
// digest code whose bytes are, or were, being sent to its provider before its
// digest was known. AddTask (cid bytes) is the only name the provider knows the
// upload by until it is parked or accepted.
type BlobStream struct {
	AddTask   []byte
	Space     did.DID
	Bucket    string
	Size      int64
	CreatedAt time.Time
	// TouchedAt is when the request that owns the row last renewed its
	// lease on it; a row whose lease has lapsed belongs to a dead request.
	TouchedAt time.Time
}

// StreamStore tracks uploads allocated by digest code from allocation until
// their park or acceptance is recorded, so one a request abandons can still be
// aborted on its provider.
type StreamStore interface {
	PutStream(ctx context.Context, s BlobStream) error
	// DeleteStream is idempotent.
	DeleteStream(ctx context.Context, addTask []byte) error
	// TouchStreams renews the lease on the rows of addTasks. Rows already
	// deleted are skipped.
	TouchStreams(ctx context.Context, addTasks [][]byte) error
	// ListStaleStreams returns up to limit rows last touched before
	// olderThan, least recently touched first.
	ListStaleStreams(ctx context.Context, olderThan time.Time, limit int) ([]BlobStream, error)
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
	GetInclusion(ctx context.Context, space did.DID, digest multihash.Multihash) (*BlobInclusion, error)
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
	// CompleteSession is the completing→completed latch that also records the
	// winner's result (etag; versionID, empty for unversioned buckets) in the
	// same statement, so a completed row always carries what to replay.
	// Returns true iff this caller performed the transition.
	CompleteSession(ctx context.Context, uploadID, etag, versionID string) (bool, error)
	DeleteSession(ctx context.Context, uploadID string) error
	// PutPart records a part of an OPEN session and its tree (nil for none),
	// superseding a prior row for the part number and its tree row, and
	// returns ErrNotFound when the session is missing or no longer open: the
	// write and the state check are one statement that holds the session
	// row, so a part cannot land after a Complete or a teardown has taken
	// the session, and a teardown's part listing sees every part that did
	// land.
	PutPart(ctx context.Context, p MultipartPart, tree *PartTree) error
	// ListParts returns uploadID's parts in part-number order. The trees
	// are separate (ListPartTrees): a part's tree is up to hundreds of KiB
	// and only Complete merges them, while UploadPart, ListParts and the
	// teardown list the parts of every upload, large ones once per part.
	ListParts(ctx context.Context, uploadID string) ([]MultipartPart, error)
	// ListPartTrees returns uploadID's recorded part trees by part number,
	// for Complete, which already holds the parts from ListParts. A part
	// without a tree has no entry.
	ListPartTrees(ctx context.Context, uploadID string) (map[int]*PartTree, error)
	// DeletePartTrees drops uploadID's tree rows. Complete calls it once the
	// object is committed: the manifest then holds the object's tree and
	// nothing reads the parts' again, while the part rows themselves stay
	// for the idempotent re-Complete. A missing upload is not an error.
	DeletePartTrees(ctx context.Context, uploadID string) error
	// ListSessions returns bucket's sessions ordered by (object_key, created_at,
	// upload_id) — the S3 ListMultipartUploads presentation order. Filtering
	// (prefix/markers/max) happens in the handler; in-flight session counts are
	// small.
	ListSessions(ctx context.Context, bucket string) ([]MultipartSession, error)
	// ListStaleSessions returns sessions in `state` whose state last changed
	// before cutoff, for the abandoned-upload sweeper. A session that has
	// never left 'open' counts from its creation.
	ListStaleSessions(ctx context.Context, state string, cutoff time.Time) ([]MultipartSession, error)
	// CountPartRefs returns how many parts OUTSIDE excludeUploadID reference
	// digest — the shared-blob guard for abort/supersede spool cleanup
	// (content-addressed part blobs may be deduped across sessions).
	CountPartRefs(ctx context.Context, digest multihash.Multihash, excludeUploadID string) (int, error)
	// CountLivePartRefs returns how many parts of space's in-flight sessions
	// (open or completing) reference digest: the guard a deferred release
	// checks before taking a blob a session still means to claim. Parts of
	// completed and aborting sessions do not count — a completed session's
	// winners hold claims and its orphans have releases of their own, and an
	// aborting session is recording releases for its parts. Another space's
	// sessions do not count either: a release is one space's, and the same
	// bytes in another space are that space's own blob.
	CountLivePartRefs(ctx context.Context, space did.DID, digest multihash.Multihash) (int, error)
}

// GCStore records superseded MST node CIDs (§4). Write-only this iteration.
type GCStore interface {
	AddGCCandidate(ctx context.Context, cidBytes []byte, bucket string) error
}

// CatalogUsage is one bucket's catalog storage on Forge: the CARs of its
// shipped catalog segments, which stay registered in its space until
// DeleteBucket releases them. The segments' index blobs are not counted.
// Segment rows are matched to the bucket by name: rows a failed DeleteBucket
// cleanup left behind count toward a later bucket of the same name.
type CatalogUsage struct {
	Bucket string
	Space  did.DID
	// Bytes is the total size of the bucket's shipped catalog CARs.
	Bytes int64
	// DeadOnArrivalBytes is how much of Bytes was already dead when its
	// segment shipped, summed over the segments that were measured.
	DeadOnArrivalBytes int64
	// UnmeasuredBytes is the part of Bytes in segments shipped without that
	// measurement.
	UnmeasuredBytes int64
}

// CatalogUsageStore reports how much catalog storage each bucket holds on
// Forge, from the segment rows.
type CatalogUsageStore interface {
	// CatalogUsage returns one entry per bucket with shipped catalog
	// segments, ordered by bucket name.
	CatalogUsage(ctx context.Context) ([]CatalogUsage, error)
}

// RevocationCursor is the single row of ingot.revocation_cursor: the resume
// point in the revocation service's firehose. RecordedAt is the recorded_at
// of the last processed revocation record — the service's own timeline, so a
// reconnect's `since` cursor is immune to clock skew between ingot and the
// service. Revoke is the CID of the delegation that record revoked, kept for
// observability.
type RevocationCursor struct {
	RecordedAt time.Time
	Revoke     cid.Cid
}

// RevocationCursorStore persists the firehose cursor across disconnects and
// restarts. With no cursor stored (GetRevocationCursor → ErrNotFound) the
// consumer connects from "now": nothing revoked before the process first
// subscribed can be sitting in its in-memory caches.
type RevocationCursorStore interface {
	// GetRevocationCursor returns ErrNotFound when no cursor has been stored.
	GetRevocationCursor(ctx context.Context) (*RevocationCursor, error)
	// PutRevocationCursor upserts the cursor (a single logical row).
	PutRevocationCursor(ctx context.Context, cur RevocationCursor) error
}
