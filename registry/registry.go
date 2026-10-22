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
	CreatedAt  time.Time // set by the implementation at create time
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

	// Delete removes a bucket. Returns ErrNotFound if absent.
	Delete(ctx context.Context, name string) error

	// CASRoot atomically advances the bucket root from expect to next.
	// Returns ErrConflict if the current root does not equal expect.
	CASRoot(ctx context.Context, name string, expect, next cid.Cid) error

	// CommitRoot is CASRoot plus the rows the commit owes, all in one
	// transaction: the upload registrations, the new generation's blob
	// claims and the discarded generations' claim drops (RootCommit). They
	// belong with the swap: a registration or claim without its commit
	// would describe a version the catalog never published, and a commit
	// without them would leave the version uncounted and its blobs
	// unclaimed, with nothing left to say so. A refused claim or pin
	// (ErrReleaseInProgress, ErrClaimGone) fails the commit, and the root
	// does not advance. Passing an empty RootCommit is exactly CASRoot.
	CommitRoot(ctx context.Context, name string, expect, next cid.Cid, c RootCommit) error

	// SetForgeRoot records that the DAG reachable from root has been
	// successfully shipped to Forge. Used as the high-water mark by
	// the recovery loop: anything reachable from Root but not from
	// ForgeRoot needs to be re-submitted on startup.
	SetForgeRoot(ctx context.Context, name string, root cid.Cid) error

	// SetVersioning updates the bucket's versioning state. Only Enabled and
	// Suspended are settable. Returns ErrNotFound if the bucket is absent.
	SetVersioning(ctx context.Context, name string, v VersioningState) error

	// SetObjectLockConfig stores the bucket's object-lock configuration
	// document verbatim. Returns ErrNotFound if the bucket is absent.
	SetObjectLockConfig(ctx context.Context, name string, cfg []byte) error

	// SetBucketTagging stores the bucket's tag set, replacing any previous
	// one; nil clears it. Returns ErrNotFound if the bucket is absent.
	SetBucketTagging(ctx context.Context, name string, tags []byte) error

	// SetCORSConfig stores the bucket's own CORS configuration document,
	// replacing any previous one; nil clears it. Returns ErrNotFound if the
	// bucket is absent.
	SetCORSConfig(ctx context.Context, name string, cfg []byte) error

	// SetEncryption stores the bucket's default encryption;
	// BucketEncryptionUnset clears it. Returns ErrNotFound if the bucket is
	// absent.
	SetEncryption(ctx context.Context, name string, enc BucketEncryption) error

	// AllocVersionSeq atomically advances and returns the bucket's version
	// ordinal (the first call returns 1; 0 is reserved to mean "none").
	// Gaps from failed commits are harmless — the ordinal only orders
	// versions of a key relative to each other.
	AllocVersionSeq(ctx context.Context, name string) (uint64, error)
}

// Common errors.
var (
	ErrNotFound = errors.New("registry: bucket not found")
	// ErrReleaseInProgress refuses a blob claim while a started release is
	// removing the blob from its provider (PendingReleaseStore.StartRelease):
	// the write retries once the release has run and uploads afresh.
	ErrReleaseInProgress = errors.New("registry: a release of the blob is in progress")
	// ErrClaimGone refuses a pinned claim (RootCommit.Pins) when the space
	// holds no claim on the digest to pin to: the source object's last claim
	// is gone and the blob is being released, so the copy has nothing to
	// pin.
	ErrClaimGone = errors.New("registry: no claim on the blob to pin to")
	ErrNotEmpty  = errors.New("registry: bucket not empty")
	ErrExists    = errors.New("registry: bucket already exists")
	ErrConflict  = errors.New("registry: root cas conflict")
)

// RootCommit is what a root swap carries besides the root (Registry.CommitRoot):
// the rows that must exist exactly when the version does.
type RootCommit struct {
	// Registrations are the upload-registration outbox rows the commit
	// owes: the versions it publishes and retracts from the space's
	// content-entry list.
	Registrations []UploadRegistration
	// Claims are the new generation's claims, one per body digest. Each is
	// recorded under the (space, digest) claim-key lock and refused with
	// ErrReleaseInProgress while a started release holds the digest, or
	// when the space has no location for it: a claim landing then would
	// name a blob a release is removing, or has removed since the write
	// settled it. Every committed body blob is located by its commit, so a
	// claim with no location is one such write's. Recording a claim marks
	// the space's upload intent for the digest published (IntentPublished).
	Claims []BlobClaim
	// Pins are claims borrowed from another object of the same space (a
	// same-space copy pins the source's body). Each is recorded only while
	// the space still holds a claim on its digest, under the claim-key
	// lock, which excludes the drop of that last claim: the drop counts the
	// pin, or the pin finds no claim and the commit fails with ErrClaimGone.
	Pins []BlobClaim
	// Drops are the discarded generations' claims. Each is deleted and, when
	// it was the space's last claim on the digest, the blob's release is
	// recorded in the same transaction, so no crash window separates "last
	// claim gone" from "release recorded".
	Drops []ClaimDrop
	// ReleaseGrace is how long after the commit a release the Drops record
	// is due. It is measured when the swap commits, after the catalog fsync
	// and any lock wait that precede it, so a reader that resolved the
	// discarded version just before the swap has the whole grace to open
	// its blobs.
	ReleaseGrace time.Duration
}

// Empty reports whether the commit carries nothing besides the root.
func (c RootCommit) Empty() bool {
	return len(c.Registrations) == 0 && len(c.Claims) == 0 && len(c.Pins) == 0 && len(c.Drops) == 0
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
