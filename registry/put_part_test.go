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
)

// TestPutPartReplacedLive runs PutPart's report of the row it replaced
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
		if _, err := pool.Exec(ctx, `TRUNCATE ingot.multipart_sessions, ingot.multipart_parts CASCADE`); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		runPutPartReplacedSuite(t, registry.NewPostgres(pool))
	})
}

func runPutPartReplacedSuite(t *testing.T, st registry.MultipartStore) {
	ctx := t.Context()
	space := testutil.RandomDID(t)
	digest := func(b byte) multihash.Multihash {
		return multihash.Multihash([]byte{0x12, 0x01, b})
	}
	part := func(uploadID string, n int, digests ...multihash.Multihash) registry.MultipartPart {
		return registry.MultipartPart{UploadID: uploadID, PartNumber: n, ETagMD5: make([]byte, 16), Size: 7, BlobDigests: digests, State: registry.PartParked}
	}
	open := func(uploadID string) {
		t.Helper()
		if err := st.CreateSession(ctx, registry.MultipartSession{UploadID: uploadID, Bucket: "bk", ObjectKey: uploadID, Space: space}); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
	}
	key := func(d multihash.Multihash) string { return string(d) }

	t.Run("a new part number replaces nothing", func(t *testing.T) {
		open("pp-new")
		replaced, err := st.PutPart(ctx, part("pp-new", 1, digest(1)), nil)
		if err != nil || replaced != nil {
			t.Fatalf("PutPart = %x, %v; want no replaced digests", replaced, err)
		}
	})

	t.Run("a supersede reports the replaced row's digests", func(t *testing.T) {
		open("pp-again")
		if _, err := st.PutPart(ctx, part("pp-again", 1, digest(1), digest(2)), nil); err != nil {
			t.Fatalf("PutPart: %v", err)
		}
		replaced, err := st.PutPart(ctx, part("pp-again", 1, digest(3)), nil)
		if err != nil || len(replaced) != 2 || key(replaced[0]) != key(digest(1)) || key(replaced[1]) != key(digest(2)) {
			t.Fatalf("PutPart = %x, %v; want the replaced row's digests", replaced, err)
		}
		// Another part number of the upload is not the replaced row.
		replaced, err = st.PutPart(ctx, part("pp-again", 2, digest(4)), nil)
		if err != nil || replaced != nil {
			t.Fatalf("PutPart of part 2 = %x, %v; want no replaced digests", replaced, err)
		}
	})

	t.Run("concurrent writes of one part number each learn what they replaced", func(t *testing.T) {
		// Every write of the part number either stands at the end or is
		// reported replaced by exactly one other write: no row is replaced
		// unseen, however the writes interleave.
		open("pp-race")
		const writers = 8
		var wg sync.WaitGroup
		reports := make([][]multihash.Multihash, writers)
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
			for _, d := range reports[i] {
				seen[key(d)]++
			}
		}
		if fresh != 1 {
			t.Fatalf("%d writers found no row to replace, want exactly the first", fresh)
		}
		parts, err := st.ListParts(ctx, "pp-race")
		if err != nil || len(parts) != 1 || len(parts[0].BlobDigests) != 1 {
			t.Fatalf("ListParts = %+v, %v", parts, err)
		}
		seen[key(parts[0].BlobDigests[0])]++
		for i := 0; i < writers; i++ {
			if n := seen[key(digest(byte(0x10+i)))]; n != 1 {
				t.Fatalf("writer %d's digest accounted for %d times, want once (replaced by one write, or standing)", i, n)
			}
		}
	})
}
