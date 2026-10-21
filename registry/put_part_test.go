package registry_test

import (
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/migrations"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
)

// putPartStore is what the PutPart suite needs of a store.
type putPartStore interface {
	registry.MultipartStore
	registry.PendingReleaseStore
}

// TestPutPartReplacedLive runs PutPart's release of the row it replaces
// against the in-memory store and, when INGOT_TEST_DSN is set, against
// Postgres (the Live suffix puts it in CI's live-Postgres job).
func TestPutPartReplacedLive(t *testing.T) {
	t.Run("inmem", func(t *testing.T) {
		runPutPartReplacedSuite(t, inmem.NewMemStore())
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv("INGOT_TEST_DSN")
		if dsn == "" {
			t.Skip("set INGOT_TEST_DSN to run PutPart against Postgres")
		}
		ctx := t.Context()
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(pool.Close)
		if err := migrations.Up(ctx, pool, zaptest.NewLogger(t)); err != nil {
			t.Fatalf("migrations.Up: %v", err)
		}
		if _, err := pool.Exec(ctx, `TRUNCATE ingot.multipart_sessions, ingot.multipart_parts, ingot.blob_release_intents CASCADE`); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		runPutPartReplacedSuite(t, registry.NewPostgres(pool))
	})
}

func runPutPartReplacedSuite(t *testing.T, st putPartStore) {
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
	recorded := func(space did.DID) map[string]registry.PendingRelease {
		t.Helper()
		out := map[string]registry.PendingRelease{}
		for _, pr := range mustListReleases(t, st, space) {
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
		if err != nil || len(released) != 1 || key(released[0].Digest) != key(digest(1)) || released[0].Space != space {
			t.Fatalf("PutPart = %v, %v; want one release of the dropped digest in the session's space", released, err)
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

	t.Run("concurrent writes of one part number release every row they replace", func(t *testing.T) {
		// Every write of the part number either stands at the end or is
		// released by the write that replaced it: no row is replaced
		// unseen, however the writes interleave.
		space := open("pp-race")
		const writers = 8
		var wg sync.WaitGroup
		reports := make([][]registry.PendingRelease, writers)
		errs := make([]error, writers)
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				reports[i], errs[i] = st.PutPart(ctx, part("pp-race", 1, digest(byte(0x10+i))), nil)
			}(i)
		}
		wg.Wait()
		seen := map[string]int{}
		fresh := 0
		for i := 0; i < writers; i++ {
			if errs[i] != nil {
				t.Fatalf("writer %d: %v", i, errs[i])
			}
			if reports[i] == nil {
				fresh++
				continue
			}
			for _, pr := range reports[i] {
				seen[key(pr.Digest)]++
			}
		}
		if fresh != 1 {
			t.Fatalf("%d writers found no row to replace, want exactly the first", fresh)
		}
		parts, err := st.ListParts(ctx, "pp-race")
		if err != nil || len(parts) != 1 || len(parts[0].BlobDigests) != 1 {
			t.Fatalf("ListParts = %+v, %v", parts, err)
		}
		standing := key(parts[0].BlobDigests[0])
		seen[standing]++
		rec := recorded(space)
		for i := 0; i < writers; i++ {
			d := key(digest(byte(0x10 + i)))
			if n := seen[d]; n != 1 {
				t.Fatalf("writer %d's digest accounted for %d times, want once (released by one write, or standing)", i, n)
			}
			if _, ok := rec[d]; ok == (d == standing) {
				t.Fatalf("writer %d's digest recorded for release = %v, want %v", i, ok, d != standing)
			}
		}
	})
}
