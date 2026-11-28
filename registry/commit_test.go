package registry_test

import (
	"time"

	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/taskrunner/taskrunnertest"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
)

// The reference index is written by commits alone (registry.RootCommit), so
// a test records a claim the way a write does: by advancing a bucket's root
// with the claim carried along.

var commitSeq atomic.Int64

// commit advances bucket's root by one commit carrying c, creating the
// bucket for space the first time, and returns what the commit reported.
func commit(t *testing.T, st registry.Registry, bucket string, space did.DID, c registry.RootCommit) error {
	t.Helper()
	ctx := context.Background()
	s, err := st.Get(ctx, bucket)
	if errors.Is(err, registry.ErrNotFound) {
		require.NoError(t, st.Create(ctx, bucket, space, registry.CreateState{Tenant: testutil.RandomDID(t)}), "Create %q", bucket)
		s, err = st.Get(ctx, bucket)
	}
	require.NoError(t, err, "Get %q", bucket)
	return st.(*registry.Postgres).CommitRoot(ctx, bucket, s.Root, liveCid(t, fmt.Sprintf("commit-%d", commitSeq.Add(1))), c)
}

// claim commits one claim of a new generation.
func claim(t *testing.T, st registry.Registry, c registry.BlobClaim) error {
	t.Helper()
	return commit(t, st, c.Bucket, c.Space, registry.RootCommit{Claims: []registry.BlobClaim{c}})
}

func mustClaim(t *testing.T, st registry.Registry, c registry.BlobClaim) {
	t.Helper()
	require.NoError(t, claim(t, st, c), "claim %x in %s/%s", c.Digest, c.Bucket, c.ObjectKey)
}

// drop commits the removal of one claim.
func drop(t *testing.T, st registry.Registry, d registry.ClaimDrop) {
	t.Helper()
	require.NoError(t, commit(t, st, d.Bucket, d.Space, registry.RootCommit{Drops: []registry.ClaimDrop{d}}), "drop %x in %s/%s", d.Digest, d.Bucket, d.ObjectKey)
}

// queuedRelease is a pending release, as a test reads it.
type queuedRelease struct {
	Digest    multihash.Multihash
	NotBefore time.Time
}

// mustListReleases returns the space's pending releases, oldest first.
func mustListReleases(t *testing.T, pool *pgxpool.Pool, space did.DID) []queuedRelease {
	t.Helper()
	tasks, err := taskrunnertest.Tasks(context.Background(), pool, "ingot", spaceshardqueue.Name)
	require.NoError(t, err, "list releases")
	var out []queuedRelease
	for _, q := range tasks {
		if q.Action != spaceshardqueue.ReleaseAction || q.State != "pending" {
			continue
		}
		sp, d, err := spaceshardqueue.ParseSubject(q.Subject)
		require.NoError(t, err)
		if sp != space {
			continue
		}
		at := q.Created
		if q.NotBefore != nil {
			at = *q.NotBefore
		}
		out = append(out, queuedRelease{Digest: d, NotBefore: at})
	}
	return out
}

// releaseRecorded reports whether space has a release queued for digest.
func releaseRecorded(t *testing.T, pool *pgxpool.Pool, space did.DID, digest []byte) bool {
	t.Helper()
	for _, pr := range mustListReleases(t, pool, space) {
		if string(pr.Digest) == string(digest) {
			return true
		}
	}
	return false
}
