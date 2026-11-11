package registry_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"

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
		if err := st.CreateSession(ctx, registry.MultipartSession{UploadID: uploadID, Bucket: "bk", ObjectKey: uploadID, Space: space}); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
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
		if err != nil || released != nil {
			t.Fatalf("PutPart = %v, %v; want no releases", released, err)
		}
		if n := len(recorded(space)); n != 0 {
			t.Fatalf("%d releases recorded for a new part", n)
		}
	})

	t.Run("a supersede records a release for each blob the part drops, with the write", func(t *testing.T) {
		space := open("pp-again")
		if _, err := st.PutPart(ctx, part("pp-again", 1, digest(1), digest(2), digest(2)), nil); err != nil {
			t.Fatalf("PutPart: %v", err)
		}
		// Digest 2 stays named; digest 1 is dropped and released, once.
		released, err := st.PutPart(ctx, part("pp-again", 1, digest(3), digest(2)), nil)
		if err != nil || len(released) != 1 || key(released[0]) != key(digest(1)) {
			t.Fatalf("PutPart = %v, %v; want one release, of the dropped digest", released, err)
		}
		rec := recorded(space)
		if len(rec) != 1 || rec[key(digest(1))].NotBefore.IsZero() {
			t.Fatalf("releases recorded = %v, want the dropped digest's, due now", rec)
		}
		// Another part number of the upload replaces no row.
		released, err = st.PutPart(ctx, part("pp-again", 2, digest(4)), nil)
		if err != nil || released != nil {
			t.Fatalf("PutPart of part 2 = %v, %v; want no releases", released, err)
		}
	})
}
