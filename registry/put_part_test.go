package registry_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
)

// TestPutPartReplaced runs PutPart's release of the row it replaces
// against Postgres.
func TestPutPartReplaced(t *testing.T) {
	pool := pgtest.New(t)
	runPutPartReplacedSuite(t, pool, liveRegistry(pool, liveUploadTasks(t, pool)))
}

func runPutPartReplacedSuite(t *testing.T, pool *pgxpool.Pool, st *registry.Postgres) {
	ctx := t.Context()
	digest := func(b byte) multihash.Multihash {
		return multihash.Multihash([]byte{0x12, 0x01, b})
	}
	part := func(uploadID string, n int, digests ...multihash.Multihash) registry.MultipartPart {
		return registry.MultipartPart{UploadID: uploadID, PartNumber: n, ETagMD5: make([]byte, 16), Size: 7, BlobDigests: digests, State: registry.PartParked}
	}
	open := func(uploadID string) did.DID {
		t.Helper()
		space := testutil.RandomDID(t)
		require.NoError(t, st.CreateSession(ctx, registry.MultipartSession{UploadID: uploadID, Bucket: "bk", ObjectKey: uploadID, Space: space}), "CreateSession")
		return space
	}
	key := func(d multihash.Multihash) string { return string(d) }
	recorded := func(space did.DID) map[string]queuedRelease {
		t.Helper()
		out := map[string]queuedRelease{}
		for _, pr := range mustListReleases(t, pool, space) {
			out[key(pr.Digest)] = pr
		}
		return out
	}

	t.Run("a new part number releases nothing", func(t *testing.T) {
		space := open("pp-new")
		released, err := st.PutPart(ctx, part("pp-new", 1, digest(1)), nil)
		require.NoError(t, err, "PutPart = %v, %v", released, err)
		require.Nil(t, released, "PutPart = %v, %v", released, err)
		n := len(recorded(space))
		require.Zero(t, n, "%d releases recorded for a new part", n)
	})

	t.Run("a supersede records a release for each blob the part drops, with the write", func(t *testing.T) {
		space := open("pp-again")
		_, err := st.PutPart(ctx, part("pp-again", 1, digest(1), digest(2), digest(2)), nil)
		require.NoError(t, err, "PutPart")
		// Digest 2 stays named; digest 1 is dropped and released, once.
		released, err := st.PutPart(ctx, part("pp-again", 1, digest(3), digest(2)), nil)
		require.NoError(t, err, "PutPart")
		require.Len(t, released, 1, "PutPart: want one release, of the dropped digest")
		require.Equal(t, key(digest(1)), key(released[0]), "PutPart: want one release, of the dropped digest")
		rec := recorded(space)
		require.Len(t, rec, 1, "releases recorded = %v, want the dropped digest's, due now", rec)
		require.False(t, rec[key(digest(1))].NotBefore.IsZero(), "releases recorded = %v, want the dropped digest's, due now", rec)
		// Another part number of the upload replaces no row.
		released, err = st.PutPart(ctx, part("pp-again", 2, digest(4)), nil)
		require.NoError(t, err, "PutPart of part 2")
		require.Nil(t, released, "PutPart of part 2")
	})
}
