// Package registry tracks the set of buckets and the current MST root CID
// for each. The interface is small enough that swapping SQLite for postgres
// or DynamoDB later is just a new implementation.
package registry

import (
	"context"
	"errors"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/taskrunner"
)

// VersioningState is a bucket's S3 versioning configuration. Buckets start
// Unversioned; once configured they only ever move between Enabled and
// Suspended (S3 has no way back to unversioned).
type VersioningState string

const (
	VersioningUnversioned VersioningState = "unversioned"
	VersioningEnabled     VersioningState = "enabled"
	VersioningSuspended   VersioningState = "suspended"
)

// Configured reports whether the bucket carries a versioning configuration
// (Enabled or Suspended) — the states in which version ids appear in S3
// responses (docs/s3-versioning.md §4.3).
func (s VersioningState) Configured() bool {
	return s == VersioningEnabled || s == VersioningSuspended
}

// UnknownTenant is the Tenant of a bucket row that predates the tenant column
// (migration 00017 backfills it). A syntactically valid DID, so it reads back
// like any other row, and never a did:plc, so no hilt tenant can equal it. It
// records that the owner is unknown, so it matches no tenant: two rows carrying
// it may belong to different tenants, and the copy paths refuse to treat them
// as one.
var UnknownTenant = did.MustParse("did:web:unknown-tenant.invalid")

// BucketEncryption is a bucket's default server-side encryption: the
// SSEAlgorithm of its ServerSideEncryptionConfiguration, as PutBucketEncryption
// stored it.
type BucketEncryption string

const (
	// BucketEncryptionUnset is a bucket that was never configured. Its objects
	// are encrypted exactly as under BucketEncryptionAES256; the two differ
	// only in what GetBucketEncryption answers (not found vs the document).
	BucketEncryptionUnset BucketEncryption = ""
	// BucketEncryptionAES256 is S3's SSE-S3: every body blob is stored as a
	// FEE envelope under the tenant and region wraps.
	BucketEncryptionAES256 BucketEncryption = "AES256"
	// BucketEncryptionNone is the non-standard value under which body blobs
	// are stored as received, for tenants that encrypt before upload. A
	// deployment opts into accepting it (config encryption.allow_none).
	BucketEncryptionNone BucketEncryption = "none"
)

// State is the metadata stored per bucket.
type State struct {
	Name  string
	Space did.DID // Forge space DID the bucket's data lives in
	// Tenant is the owning tenant's DID (hilt's did:plc for the caller that
	// created the bucket). The copy paths compare it across buckets to refuse a
	// source hilt never authorized; UnknownTenant on rows that predate it.
	Tenant     did.DID
	Root       cid.Cid         // current MST root; cid.Undef for empty bucket
	ForgeRoot  cid.Cid         // last MST root whose DAG has been shipped to Forge
	Versioning VersioningState // S3 versioning configuration
	// ObjectLockConfig is the bucket's object-lock configuration: the
	// controller's auth.BucketLockConfig JSON, stored verbatim. Nil when the
	// bucket has never been configured (docs/s3-object-lock.md §4.2).
	ObjectLockConfig []byte
	// BucketTagging is the bucket's tag set as a JSON object. Nil when the
	// bucket carries no tag set — never set, or deleted since
	// (docs/s3-object-tagging.md §9).
	BucketTagging []byte
	// CORSConfig is the bucket's own CORS configuration: the controller's
	// validated <CORSConfiguration> XML, stored verbatim. Nil when the
	// bucket has none of its own and answers the gateway-wide document
	// (docs/s3-cors.md).
	CORSConfig []byte
	// Encryption is the bucket's default encryption as PutBucketEncryption
	// stored it; BucketEncryptionUnset when never configured, or deleted
	// since.
	Encryption BucketEncryption
	// Closing is set once a DeleteBucket has closed the bucket: no commit
	// or new multipart upload is admitted (queue-bucket.close).
	Closing   bool
	CreatedAt time.Time // set by the implementation at create time
}

// CreateState is the initial bucket state Create installs, so a bucket
// created with x-amz-bucket-object-lock-enabled is versioned and locked
// atomically (docs/s3-object-lock.md §5).
type CreateState struct {
	Tenant           did.DID         // owning tenant; required by the Postgres registry
	Versioning       VersioningState // "" = unversioned
	ObjectLockConfig []byte          // nil = no lock
}

// ListOptions selects a page of buckets.
type ListOptions struct {
	// Prefix restricts the page to buckets whose name has this prefix.
	Prefix string
	// ContinuationToken resumes a listing strictly after this name;
	// empty starts from the beginning.
	ContinuationToken string
	// Max caps the page size; <= 0 means no cap.
	Max int
}

// Page is one page of bucket state, in lexicographic name order.
type Page struct {
	Buckets []*State
	// ContinuationToken resumes the listing where this page ended;
	// empty when the listing is complete.
	ContinuationToken string
}

// Registry tracks bucket state. All methods are safe for concurrent use.
type Registry interface {
	// Create inserts a new bucket with the given initial state, stamping its
	// creation time. Returns ErrExists if name is taken.
	Create(ctx context.Context, name string, space did.DID, init CreateState) error

	// Get returns the state of a bucket, or ErrNotFound.
	Get(ctx context.Context, name string) (*State, error)

	// Delete removes bucket name while it lives in space, so a delete never
	// removes another bucket recreated under the name. Returns ErrNotFound
	// if absent.
	Delete(ctx context.Context, name string, space did.DID) error

	// RootCommitWrites returns the writes that advance the bucket root
	// from expect to next with the rows the commit owes: its upload
	// registrations, the new generation's blob claims, the discarded
	// generations' claim drops, the journaled catalog blocks (RootCommit)
	// and the catalog blocks it made garbage. A queue-bucket.commit writes
	// them with its outcome, so none can describe a version the catalog
	// never published, and no published version lacks them. A claim whose
	// pending reference has gone fails it with ErrReferenceExpired.
	RootCommitWrites(ctx context.Context, name string, expect, next cid.Cid, c RootCommit, gc []cid.Cid) (CommitWrites, error)

	// EmptyRoot swaps the bucket root from expect to the empty tree, and in
	// the same transaction clears forge_root_cid and drops the bucket's
	// journaled catalog blocks: nothing the empty tree reaches needs them.
	// A root other than expect is ErrConflict. DeleteBucket's first write,
	// made before it releases anything, so a delete that fails after it
	// leaves a bucket that lists empty.
	EmptyRoot(ctx context.Context, name string, expect cid.Cid) error

	// PendingRefsHeld reports whether every one of refs is still a pending
	// reference, for a commit to refuse a change whose reference has gone
	// before it writes anything.
	PendingRefsHeld(ctx context.Context, refs []taskrunner.TaskID) (bool, error)

	// CatalogJournalStats reports how much the catalog journal holds: the
	// blocks commits journaled whose segments have not shipped yet.
	CatalogJournalStats(ctx context.Context) (CatalogJournalStats, error)

	// SetForgeRoot records that the DAG reachable from root has been
	// successfully shipped to Forge. Used as the high-water mark by
	// the recovery loop: anything reachable from Root but not from
	// ForgeRoot needs to be re-submitted on startup.
	SetForgeRoot(ctx context.Context, name string, root cid.Cid) error

	// SetClosing marks the bucket closing, or clears the mark
	// (State.Closing). Returns ErrNotFound if absent.
	SetClosing(ctx context.Context, name string, closing bool) error

	// AllocVersionSeq atomically advances and returns the bucket's version
	// ordinal (the first call returns 1; 0 is reserved to mean "none").
	// Gaps from failed commits are harmless — the ordinal only orders
	// versions of a key relative to each other.
	AllocVersionSeq(ctx context.Context, name string) (uint64, error)
}

// Common errors.
var (
	ErrNotFound = errors.New("registry: bucket not found")
	// ErrReferenceExpired refuses a claim or a part whose write's pending
	// reference to a blob has gone, dropped at its deadline: the blob may be
	// released, so the write starts again.
	ErrReferenceExpired = errors.New("registry: the write's reference to the blob has expired")
	ErrNotEmpty         = errors.New("registry: bucket not empty")
	ErrExists           = errors.New("registry: bucket already exists")
	ErrConflict         = errors.New("registry: root cas conflict")
)

// RootCommit is what a root's advance carries besides the root
// (Registry.RootCommitWrites):
// the rows that must exist exactly when the version does.
type RootCommit struct {
	// Registrations are the queue-space-upload tasks the commit queues in
	// its transaction: the versions it publishes and retracts from the
	// space's content-entry list.
	Registrations []taskrunner.Spec
	// Claims are the new generation's claims, one per body digest. A claim
	// with a Ref takes over the write's pending reference to the blob, and
	// cancels the reference's expiry, in the same transaction; a reference
	// that has expired refuses the commit with ErrReferenceExpired. A claim
	// without one is a Complete's, whose blobs the session's parts reference
	// until it is torn down. Recording a claim marks the space's upload
	// intent for the digest published (IntentPublished).
	Claims []BlobClaim
	// Drops are the discarded generations' claims. Each is deleted and, when
	// it was the space's last claim on the digest, the blob's release is
	// queued in the same transaction, so no crash window separates "last
	// claim gone" from "release queued".
	Drops []ClaimDrop
	// Release is the authority the releases the Drops queue remove their
	// blobs with, captured from the request.
	Release ReleaseTask
	// ReleaseGrace is how long after the commit a release the Drops queue
	// is due. It is measured when the swap commits, after the catalog fsync
	// and any lock wait that precede it, so a reader that resolved the
	// discarded version just before the swap has the whole grace to open
	// its blobs.
	ReleaseGrace time.Duration
	// Journal is the commit's catalog blocks and the log segment they went
	// into. They are kept in the catalog journal until that segment ships,
	// so a copy of the database (a standby, a restore) holds every block
	// its roots need that exists nowhere but this host's disk. A commit
	// whose segment has already shipped journals nothing: its blocks are on
	// Forge.
	Journal blockstore.Appended
}

// Empty reports whether the commit carries nothing besides the root.
func (c RootCommit) Empty() bool {
	return len(c.Registrations) == 0 && len(c.Claims) == 0 && len(c.Drops) == 0 &&
		len(c.Journal.Blocks) == 0
}

// CatalogJournalStats is the size of the catalog journal (RootCommit.Journal).
type CatalogJournalStats struct {
	// Blocks is the number of journaled blocks.
	Blocks int64
	// Bytes is their total size.
	Bytes int64
	// Oldest is when the oldest of them was journaled; zero when the
	// journal is empty.
	Oldest time.Time
}

// ClaimDrop is one claim a commit removes: a discarded version's row on a
// digest.
type ClaimDrop struct {
	Digest    multihash.Multihash
	Bucket    string
	ObjectKey string
	VersionID string
	Space     did.DID
}
