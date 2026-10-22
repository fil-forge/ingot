package registry_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/fil-forge/ingot/registry"
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
		if err := st.Create(ctx, bucket, space, registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
			t.Fatalf("Create %q: %v", bucket, err)
		}
		s, err = st.Get(ctx, bucket)
	}
	if err != nil {
		t.Fatalf("Get %q: %v", bucket, err)
	}
	return st.CommitRoot(ctx, bucket, s.Root, liveCid(t, fmt.Sprintf("commit-%d", commitSeq.Add(1))), c)
}

// claim commits one claim of a new generation.
func claim(t *testing.T, st registry.Registry, c registry.BlobClaim) error {
	t.Helper()
	return commit(t, st, c.Bucket, c.Space, registry.RootCommit{Claims: []registry.BlobClaim{c}})
}

func mustClaim(t *testing.T, st registry.Registry, c registry.BlobClaim) {
	t.Helper()
	if err := claim(t, st, c); err != nil {
		t.Fatalf("claim %x in %s/%s: %v", c.Digest, c.Bucket, c.ObjectKey, err)
	}
}

// pin commits claims borrowed from another object of the space.
func pin(t *testing.T, st registry.Registry, claims []registry.BlobClaim) error {
	t.Helper()
	return commit(t, st, claims[0].Bucket, claims[0].Space, registry.RootCommit{Pins: claims})
}

// drop commits the removal of one claim.
func drop(t *testing.T, st registry.Registry, d registry.ClaimDrop) {
	t.Helper()
	if err := commit(t, st, d.Bucket, d.Space, registry.RootCommit{Drops: []registry.ClaimDrop{d}}); err != nil {
		t.Fatalf("drop %x in %s/%s: %v", d.Digest, d.Bucket, d.ObjectKey, err)
	}
}

// releaseRecorded reports whether space has a release record for digest.
func releaseRecorded(t *testing.T, st registry.PendingReleaseStore, space did.DID, digest []byte) bool {
	t.Helper()
	for _, pr := range mustListReleases(t, st, space) {
		if string(pr.Digest) == string(digest) {
			return true
		}
	}
	return false
}
