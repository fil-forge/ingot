// Package registry tracks the set of buckets and the current MST root CID
// for each. The interface is small enough that swapping SQLite for postgres
// or DynamoDB later is just a new implementation.
package registry

import (
	"context"
	"errors"

	"github.com/ipfs/go-cid"
)

// State is the metadata stored per bucket.
type State struct {
	Name      string
	Root      cid.Cid // current MST root; cid.Undef for empty bucket
	ForgeRoot cid.Cid // last MST root whose DAG has been shipped to Forge
	CreatedAt int64   // unix seconds
}

// Registry tracks bucket state. All methods are safe for concurrent use.
type Registry interface {
	// Create inserts a new bucket. Returns ErrExists if name is taken.
	Create(ctx context.Context, name string, createdAt int64) error

	// Get returns the state of a bucket, or ErrNotFound.
	Get(ctx context.Context, name string) (*State, error)

	// List returns every bucket in lexicographic name order.
	List(ctx context.Context) ([]*State, error)

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
}

// Common errors.
var (
	ErrNotFound = errors.New("registry: bucket not found")
	ErrExists   = errors.New("registry: bucket already exists")
	ErrConflict = errors.New("registry: root cas conflict")
)
