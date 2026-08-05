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

// State is the metadata stored per bucket.
type State struct {
	Name       string
	Space      did.DID         // Forge space DID the bucket's data lives in
	Root       cid.Cid         // current MST root; cid.Undef for empty bucket
	ForgeRoot  cid.Cid         // last MST root whose DAG has been shipped to Forge
	Versioning VersioningState // S3 versioning configuration
	CreatedAt  time.Time       // set by the implementation at create time
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
	// Create inserts a new bucket, stamping its creation time. Returns
	// ErrExists if name is taken.
	Create(ctx context.Context, name string, space did.DID) error

	// Get returns the state of a bucket, or ErrNotFound.
	Get(ctx context.Context, name string) (*State, error)

	// Delete removes a bucket. Returns ErrNotFound if absent.
	Delete(ctx context.Context, name string) error

	// CASRoot atomically advances the bucket root from expect to next.
	// Returns ErrConflict if the current root does not equal expect.
	CASRoot(ctx context.Context, name string, expect, next cid.Cid) error

	// SetForgeRoot records that the DAG reachable from root has been
	// successfully shipped to Forge. Used as the high-water mark by
	// the recovery loop: anything reachable from Root but not from
	// ForgeRoot needs to be re-submitted on startup.
	SetForgeRoot(ctx context.Context, name string, root cid.Cid) error

	// SetVersioning updates the bucket's versioning state. Only Enabled and
	// Suspended are settable. Returns ErrNotFound if the bucket is absent.
	SetVersioning(ctx context.Context, name string, v VersioningState) error

	// AllocVersionSeq atomically advances and returns the bucket's version
	// ordinal (the first call returns 1; 0 is reserved to mean "none").
	// Gaps from failed commits are harmless — the ordinal only orders
	// versions of a key relative to each other.
	AllocVersionSeq(ctx context.Context, name string) (uint64, error)
}

// Common errors.
var (
	ErrNotFound = errors.New("registry: bucket not found")
	ErrNotEmpty = errors.New("registry: bucket not empty")
	ErrExists   = errors.New("registry: bucket already exists")
	ErrConflict = errors.New("registry: root cas conflict")
)
