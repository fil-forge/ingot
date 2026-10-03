package registry_test

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/migrations"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
)

// evictStore is what the spool-eviction queries need of a store.
type evictStore interface {
	registry.IntentStore
	registry.LocationStore
	registry.ParkStore
}

// TestEvictionQueries runs the eviction queries against the in-memory store
// and, when INGOT_TEST_DSN is set, against Postgres, so the fake the
// s3frontend tests use is held to the SQL.
func TestEvictionQueries(t *testing.T) {
	t.Run("inmem", func(t *testing.T) {
		runEvictionSuite(t, func(t *testing.T) evictStore { return inmem.NewMemStore() })
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv("INGOT_TEST_DSN")
		if dsn == "" {
			t.Skip("set INGOT_TEST_DSN to run the eviction queries against Postgres")
		}
		ctx := context.Background()
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(pool.Close)
		if err := migrations.Up(ctx, pool, zaptest.NewLogger(t)); err != nil {
			t.Fatalf("migrations.Up: %v", err)
		}
		runEvictionSuite(t, func(t *testing.T) evictStore {
			if _, err := pool.Exec(ctx,
				`TRUNCATE ingot.upload_intents, ingot.blob_locations, ingot.blob_parks CASCADE`); err != nil {
				t.Fatalf("truncate: %v", err)
			}
			return registry.NewPostgres(pool)
		})
	})
}

func evictDigest(t *testing.T, s string) multihash.Multihash {
	t.Helper()
	d, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return d
}

func runEvictionSuite(t *testing.T, fresh func(t *testing.T) evictStore) {
	ctx := context.Background()

	// seed records an intent in state, optionally with a location or a park.
	seed := func(t *testing.T, st evictStore, d multihash.Multihash, state string, located, parked bool) {
		t.Helper()
		if err := st.PutIntent(ctx, registry.UploadIntent{Digest: d, LocalPath: "/spool/x", Size: 7, State: registry.IntentSpooled}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if state != registry.IntentSpooled {
			if err := st.SetIntentState(ctx, d, state); err != nil {
				t.Fatalf("SetIntentState: %v", err)
			}
		}
		if located {
			if err := st.PutLocation(ctx, registry.BlobLocation{Space: testutil.RandomDID(t), Digest: d, Provider: "did:key:p", URL: "http://p/blob", Size: 7}); err != nil {
				t.Fatalf("PutLocation: %v", err)
			}
		}
		if parked {
			if err := st.PutPark(ctx, registry.BlobPark{Digest: d, AddTask: []byte{1}, AcceptTask: []byte{2}, PutInvocation: []byte{3}, Size: 7}); err != nil {
				t.Fatalf("PutPark: %v", err)
			}
		}
	}
	listed := func(t *testing.T, st evictStore) []string {
		t.Helper()
		rows, err := st.ListEvictable(ctx, registry.EvictCursor{}, 100)
		if err != nil {
			t.Fatalf("ListEvictable: %v", err)
		}
		var out []string
		for _, in := range rows {
			out = append(out, string(in.Digest))
		}
		return out
	}

	predicateCases := []struct {
		name            string
		state           string
		located, parked bool
		evicted         bool
		want            bool
	}{
		{name: "published with a location", state: registry.IntentPublished, located: true, want: true},
		{name: "accepted with a location", state: registry.IntentAccepted, located: true, want: true},
		{name: "parked with a park row", state: registry.IntentParked, parked: true, want: true},
		{name: "published without a location", state: registry.IntentPublished, want: false},
		{name: "accepted without a location", state: registry.IntentAccepted, want: false},
		{name: "parked without a park row", state: registry.IntentParked, want: false},
		{name: "spooled with a location", state: registry.IntentSpooled, located: true, want: false},
		{name: "uploading with a location", state: registry.IntentUploading, located: true, want: false},
		{name: "published with a location, already evicted", state: registry.IntentPublished, located: true, evicted: true, want: false},
	}
	for _, tc := range predicateCases {
		t.Run("ListEvictable: "+tc.name, func(t *testing.T) {
			st := fresh(t)
			d := evictDigest(t, tc.name)
			seed(t, st, d, tc.state, tc.located, tc.parked)
			if tc.evicted {
				if err := st.MarkEvicted(ctx, d); err != nil {
					t.Fatalf("MarkEvicted: %v", err)
				}
			}
			if got := len(listed(t, st)) == 1; got != tc.want {
				t.Fatalf("listed = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("PutIntent clears the evicted mark", func(t *testing.T) {
		st := fresh(t)
		d := evictDigest(t, "respooled")
		seed(t, st, d, registry.IntentAccepted, true, false)
		if err := st.MarkEvicted(ctx, d); err != nil {
			t.Fatalf("MarkEvicted: %v", err)
		}
		if err := st.PutIntent(ctx, registry.UploadIntent{Digest: d, LocalPath: "/spool/x", Size: 7, State: registry.IntentAccepted}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if got := listed(t, st); len(got) != 1 {
			t.Fatalf("listed %d rows, want 1", len(got))
		}
	})

	t.Run("MarkEvicted keeps state and updated_at", func(t *testing.T) {
		st := fresh(t)
		d := evictDigest(t, "kept")
		seed(t, st, d, registry.IntentPublished, true, false)
		before, err := st.GetIntent(ctx, d)
		if err != nil {
			t.Fatalf("GetIntent: %v", err)
		}
		if err := st.MarkEvicted(ctx, d); err != nil {
			t.Fatalf("MarkEvicted: %v", err)
		}
		after, err := st.GetIntent(ctx, d)
		if err != nil {
			t.Fatalf("GetIntent: %v", err)
		}
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("intent after MarkEvicted = %+v, want %+v", after, before)
		}
	})

	t.Run("MarkEvicted of a missing intent is ErrNotFound", func(t *testing.T) {
		st := fresh(t)
		if err := st.MarkEvicted(ctx, evictDigest(t, "gone")); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("MarkEvicted err = %v, want ErrNotFound", err)
		}
	})

	t.Run("ListEvictable pages oldest first from a cursor", func(t *testing.T) {
		st := fresh(t)
		var want []string
		for _, name := range []string{"first", "second", "third"} {
			d := evictDigest(t, name)
			seed(t, st, d, registry.IntentPublished, true, false)
			want = append(want, string(d))
			// Distinct state-change times, so the order is by age, not by
			// the digest tiebreak.
			time.Sleep(2 * time.Millisecond)
		}
		var got []string
		var cursor registry.EvictCursor
		for {
			page, err := st.ListEvictable(ctx, cursor, 2)
			if err != nil {
				t.Fatalf("ListEvictable: %v", err)
			}
			if len(page) == 0 {
				break
			}
			for _, in := range page {
				got = append(got, string(in.Digest))
				cursor = registry.EvictCursor{UpdatedAt: in.UpdatedAt, Digest: in.Digest}
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("paged digests in the wrong order or with gaps: got %x, want %x", got, want)
		}
	})

	t.Run("MissingIntents returns only unknown digests", func(t *testing.T) {
		st := fresh(t)
		known := evictDigest(t, "known")
		unknown := evictDigest(t, "unknown")
		seed(t, st, known, registry.IntentSpooled, false, false)
		got, err := st.MissingIntents(ctx, []multihash.Multihash{known, unknown})
		if err != nil {
			t.Fatalf("MissingIntents: %v", err)
		}
		if !reflect.DeepEqual(got, []multihash.Multihash{unknown}) {
			t.Fatalf("MissingIntents = %x, want [%x]", got, unknown)
		}
	})
}
