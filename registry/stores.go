package registry

import (
	"context"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/taskrunner"
)

// This file defines the relational surface the upload/storage/delete
// architecture relies on (docs/architecture.md §5–§7, Appendix C):
// the reverse reference index (blob_refs), the local-store index
// (upload_intents), the local blob-location table (blob_locations), the
// per-blob FEE encryption parameters (blob_encryption_params), the
// multipart session/part tables, and the gc_candidates log. The stores
// are split into focused interfaces so a caller can depend on only what
// it needs; *Postgres satisfies all of them.
//
// NullVersionID is S3's version id for a null version — the version written
// while a bucket is unversioned or Suspended (docs/s3-versioning.md §3). It
// names those versions' blob_refs rows and answers `?versionId=null`.
const NullVersionID = "null"

// upload_intents.state values (the upload lifecycle, §5): uploading on
// ingest, the blob having streamed to its provider, parked once durable on a
// provider with the accept deferred, accepted once the provider has accepted
// it (at once for a blob already stored for the space), and published once a
// bucket commit has claimed it. Published is written with the blob's first
// reference claim, in the same transaction, and never changes: it is the
// durable record that the blob was committed. Its release removes the intent,
// as it does for a blob that was never committed.
const (
	// IntentUploading stands until a row records the upload's outcome (a
	// park or a location): the blob may be on the network.
	IntentUploading = "uploading"
	IntentParked    = "parked"
	IntentAccepted  = "accepted"
	IntentPublished = "published"
)

// multipart_sessions.state values. Every change to an upload runs on its
// subject on MultipartQueue, so an upload is open until a Complete commits it
// or an Abort deletes it.
const (
	SessionOpen = "open"
	// SessionCompleted: the object committed; the session and its parts are
	// retained so a duplicate CompleteMultipartUpload with identical parts is
	// idempotent (S3 semantics), until its queue-multipart-upload.reap.
	SessionCompleted = "completed"
)

// multipart_parts.state values (§7.2). The part rows are the upload's
// acceptance queue: the queue-space-shard.accept of each of their blobs,
// queued by queue-multipart-upload.accept, concludes 'parked' parts while the
// upload is open, and Complete requires every part it names
// to be 'accepted'.
const (
	// PartParked: every blob has a park (or a location), ready to conclude.
	// A part is recorded parked: its blobs were parked before its row.
	PartParked = "parked"
	// PartAccepted: every blob is accepted on Forge.
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
	// Ref is the write's pending reference the claim takes over: the ID of
	// the queue-space-shard.add that took it. Not stored; zero for a claim
	// that takes none over (see RootCommit.Claims).
	Ref taskrunner.TaskID
}

// UploadIntent is one row of ingot.upload_intents: a blob Ingot wrote, with
// its lifecycle state for one space. Keyed by (Space, Digest): a blob stored
// as received has the digest of its bytes, so the same bytes written to two
// spaces are one cached copy and two intents, each tracking its own space's
// upload. The cached copy is shared, so it may go only when no intent for the
// digest still needs it (see CountOtherIntents).
type UploadIntent struct {
	Space  did.DID
	Digest multihash.Multihash
	Size   int64
	State  string
	Bucket string
	// UpdatedAt is when State last changed (for a committed blob, its commit
	// time). Filled on every read; ignored by PutIntent.
	UpdatedAt time.Time
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
	// CommittedETag / CommittedVersionID are the result the Complete
	// returned, recorded with its commit by the move to 'completed'
	// (empty before that). A repeated Complete replays them
	// rather than resolving the live key, which a later overwrite may have
	// replaced. CommittedVersionID is empty for unversioned buckets.
	CommittedETag      string
	CommittedVersionID string
	CreatedAt          time.Time
	// StateChangedAt is when State last changed: the creation, or the
	// Complete.
	StateChangedAt time.Time
}

// MultipartPart is one row of ingot.multipart_parts.
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
	Checksum string
	// BlobDigests are the blobs the part split into, in order (one unless
	// the part exceeded max_blob_size). Each is the digest of the blob as
	// stored: its FEE envelope for an encrypted session, the part's own
	// bytes for a session stored as received.
	BlobDigests []multihash.Multihash
	// SpanLengths, parallel to BlobDigests, is the length of the span each
	// blob covers in the object: how many of the part's bytes it carries,
	// before encryption. Complete lays the object out by these. For an
	// encrypted session the stored blob, the bytes its digest names, is
	// larger by the envelope. Nil on a row recorded before the lengths
	// were kept; the reader then derives them per blob.
	SpanLengths []int64
	// State is one of the Part* values; PutPart records PartParked.
	State     string
	CreatedAt time.Time
	// Refs are the write's pending references to the part's blobs, which
	// PutPart takes over: the IDs of the queue-space-shard.add tasks that
	// took them. Not stored.
	Refs []taskrunner.TaskID
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

// BlobRefStore reads the reverse reference index (§5, §6). The index is
// written by commits alone: a version's claims land and a discarded
// version's claims drop with the root's advance
// (Registry.RootCommitWrites, RootCommit). CountReferences gates a blob's release:
// physical reclamation when the count reaches zero.
type BlobRefStore interface {
	// CountReferences returns how many references space holds to digest:
	// the object versions that claim it, the writes' pending references
	// other than except's (the add task ID that took one; zero for none),
	// and the parts of its open multipart sessions. Parts of completed
	// sessions do not count: the listed parts' blobs hold claims and the
	// unlisted ones have releases of their own. Zero means the blob may be
	// released.
	CountReferences(ctx context.Context, space did.DID, digest multihash.Multihash, except taskrunner.TaskID) (int, error)
	// PendingRefByExpiry returns the add task ID of the pending reference
	// whose queue-space-shard.expire is expire, or ErrNotFound once nothing
	// holds it: a claim or a part row took it over, or it was dropped.
	PendingRefByExpiry(ctx context.Context, expire taskrunner.TaskID) (taskrunner.TaskID, error)
	// ExpiriesOf returns the queue-space-shard.expire task of each of refs
	// that is still a pending reference, in no particular order.
	ExpiriesOf(ctx context.Context, refs []taskrunner.TaskID) ([]taskrunner.TaskID, error)
	// QueueReleases queues the release of each of digests in space, due at
	// notBefore and carrying release's authority, in one transaction. A
	// digest repeated is released once, and a release already pending keeps
	// the later time.
	QueueReleases(ctx context.Context, space did.DID, digests []multihash.Multihash, notBefore time.Time, release ReleaseTask) error
}

// IntentStore is the upload index (§5): the blobs Ingot wrote and their
// lifecycle state. Drives releases, cache eviction, and crash recovery.
type IntentStore interface {
	// PutIntent upserts the (Space, Digest) intent.
	PutIntent(ctx context.Context, in UploadIntent) error
	// SetIntentState moves the space's intent for digest to state. UpdatedAt
	// records the last state change, which dates a stalled upload, so setting
	// the state an intent already has (a retry) leaves it alone.
	SetIntentState(ctx context.Context, space did.DID, digest multihash.Multihash, state string) error
	GetIntent(ctx context.Context, space did.DID, digest multihash.Multihash) (*UploadIntent, error)
	ListIntentsByState(ctx context.Context, state string) ([]UploadIntent, error)
	DeleteIntent(ctx context.Context, space did.DID, digest multihash.Multihash) error
	// CountOtherIntents returns how many spaces other than space hold an
	// intent for digest. Zero means the cached copy is this space's alone to
	// remove; otherwise another space's reads may still use it, and its
	// removal is left to that space's release.
	CountOtherIntents(ctx context.Context, space did.DID, digest multihash.Multihash) (int, error)
	// StalledBytes sums the sizes of 'uploading' intents whose state last
	// changed before before: bodies whose upload has outlasted any live
	// request, which nothing reclaims.
	StalledBytes(ctx context.Context, before time.Time) (int64, error)
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

// MultipartStore tracks in-flight multipart uploads (§7.2). The writes of an
// upload's changes are the queue-multipart-upload tasks' (multipartqueue.go);
// the store reads them.
type MultipartStore interface {
	// CreateSession records an open upload outside any task, for tests and
	// tools that set one up directly.
	CreateSession(ctx context.Context, s MultipartSession) error
	GetSession(ctx context.Context, uploadID string) (*MultipartSession, error)
	DeleteSession(ctx context.Context, uploadID string) error
	// PartWrites returns the writes that record a part (Postgres.PartWrites).
	PartWrites(ctx context.Context, p MultipartPart, tree *PartTree, release ReleaseTask) (CommitWrites, []multihash.Multihash, error)
	// ListParkedParts returns up to limit of uploadID's 'parked' parts
	// numbered above after, in part-number order: the accept's queue.
	ListParkedParts(ctx context.Context, uploadID string, after, limit int) ([]MultipartPart, error)
	// OldestParkedPart returns when the oldest 'parked' part of an open
	// session was recorded, and false when there is none: how
	// long the acceptance queue's oldest entry has waited.
	OldestParkedPart(ctx context.Context) (time.Time, bool, error)
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
	// ListSessions returns the sessions of bucket name in space, ordered by
	// (object_key, created_at, upload_id) — the S3 ListMultipartUploads
	// presentation order. A name outlives the bucket that bore it, and a
	// session of a predecessor bucket lives in that bucket's space, so it is
	// never listed for the bucket that has the name now. Filtering
	// (prefix/markers/max) happens in the handler; in-flight session counts
	// are small.
	ListSessions(ctx context.Context, bucket string, space did.DID) ([]MultipartSession, error)
	// CountPartRefs returns how many parts OUTSIDE excludeUploadID reference
	// digest — the shared-blob guard for abort/supersede cleanup
	// (content-addressed part blobs may be deduped across sessions).
	CountPartRefs(ctx context.Context, digest multihash.Multihash, excludeUploadID string) (int, error)
}

// GCStore records superseded MST node CIDs (§4). Write-only this iteration.
type GCStore interface {
	AddGCCandidate(ctx context.Context, cidBytes []byte, bucket string) error
}

// CatalogUsage is one bucket's catalog storage on Forge: the CARs of its
// shipped catalog segments, retired ones included, which stay registered in
// its space until DeleteBucket releases them. The segments' index blobs are
// not counted. Segment rows are matched to the bucket by name: rows a failed
// DeleteBucket cleanup left behind count toward a later bucket of the same
// name. A DeleteBucket that fails partway drops the rows of the retired
// segments it released, but not those of the unretired ones, which keep
// counting until the bucket is deleted. One that fails after releasing the
// shipped segments (hilt refusing, say) has released those blobs too.
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
// point in the revocation service's firehose. Token is the service's opaque
// cursor, which the consumer resumes after: the last processed revocation
// record's, or the firehose's head, which the consumer saves before its first
// record and when the service refuses its cursor.
type RevocationCursor struct {
	Token string
}

// RevocationCursorStore persists the firehose cursor across disconnects and
// restarts. With no cursor stored (GetRevocationCursor → ErrNotFound) the
// consumer saves the head of the firehose as the service sees it and starts
// there: nothing revoked before the process first subscribed can be sitting
// in its in-memory caches.
type RevocationCursorStore interface {
	// GetRevocationCursor returns ErrNotFound when no cursor has been stored.
	GetRevocationCursor(ctx context.Context) (*RevocationCursor, error)
	// PutRevocationCursor upserts the cursor (a single logical row).
	PutRevocationCursor(ctx context.Context, cur RevocationCursor) error
}
