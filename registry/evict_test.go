package registry_test

import (
	"errors"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/migrations"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
)

// evictStore is what the spool-eviction queries need of a store.
type evictStore interface {
	registry.IntentStore
	registry.LocationStore
	registry.ParkStore
}

// TestEvictionQueriesLive runs the eviction queries against the in-memory
// store and, when INGOT_TEST_DSN is set, against Postgres, so the fake the
// s3frontend tests use is held to the SQL. The Live suffix puts it in CI's
// live-Postgres job.
func TestEvictionQueriesLive(t *testing.T) {
	t.Run("inmem", func(t *testing.T) {
		runEvictionSuite(t, func(t *testing.T) evictStore { return inmem.NewMemStore() })
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv("INGOT_TEST_DSN")
		if dsn == "" {
			t.Skip("set INGOT_TEST_DSN to run the eviction queries against Postgres")
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
		runEvictionSuite(t, func(t *testing.T) evictStore {
			if _, err := pool.Exec(ctx,
				`TRUNCATE ingot.upload_intents, ingot.blob_locations, ingot.blob_parks CASCADE`); err != nil {
				t.Fatalf("truncate: %v", err)
			}
			return registry.NewPostgres(pool)
		})
		// Rows one transaction updates share its now(), so the cursor's
		// digest tie-break has to carry a page boundary between them.
		t.Run("ListEvictable pages through equal updated_at", func(t *testing.T) {
			if _, err := pool.Exec(ctx,
				`TRUNCATE ingot.upload_intents, ingot.blob_locations, ingot.blob_parks CASCADE`); err != nil {
				t.Fatalf("truncate: %v", err)
			}
			st := registry.NewPostgres(pool)
			space := testutil.RandomDID(t)
			var want []string
			for _, name := range []string{"tie-a", "tie-b", "tie-c"} {
				d := evictDigest(t, name)
				if err := st.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: d, LocalPath: "/spool/x", Size: 7, State: registry.IntentAccepted}); err != nil {
					t.Fatalf("PutIntent: %v", err)
				}
				if err := st.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: d, Provider: "did:key:p", URL: "http://p/blob", Size: 7}); err != nil {
					t.Fatalf("PutLocation: %v", err)
				}
				want = append(want, string(d))
			}
			// One digest held by a second space too: the cursor's space
			// tie-break carries a page boundary between the two rows.
			shared := evictDigest(t, "tie-b")
			other := testutil.RandomDID(t)
			if err := st.PutIntent(ctx, registry.UploadIntent{Space: other, Digest: shared, LocalPath: "/spool/x", Size: 7, State: registry.IntentAccepted}); err != nil {
				t.Fatalf("PutIntent: %v", err)
			}
			if err := st.PutLocation(ctx, registry.BlobLocation{Space: other, Digest: shared, Provider: "did:key:p", URL: "http://p/blob", Size: 7}); err != nil {
				t.Fatalf("PutLocation: %v", err)
			}
			want = append(want, string(shared))
			if _, err := pool.Exec(ctx, `UPDATE ingot.upload_intents SET updated_at = now()`); err != nil {
				t.Fatalf("equalize updated_at: %v", err)
			}
			slices.Sort(want)

			var got []string
			var cursor registry.EvictCursor
			for range len(want) + 1 {
				page, err := st.ListEvictable(ctx, cursor, 1)
				if err != nil {
					t.Fatalf("ListEvictable: %v", err)
				}
				if len(page) == 0 {
					break
				}
				got = append(got, string(page[0].Digest))
				cursor = registry.EvictCursor{UpdatedAt: page[0].UpdatedAt, Digest: page[0].Digest, Space: page[0].Space}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("paged digests = %x, want each once in digest order %x", got, want)
			}
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
	ctx := t.Context()
	space := testutil.RandomDID(t)

	// seed records an intent in state, optionally with a location or a park.
	seed := func(t *testing.T, st evictStore, d multihash.Multihash, state string, located, parked bool) {
		t.Helper()
		if err := st.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: d, LocalPath: "/spool/x", Size: 7, State: registry.IntentSpooled}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if state != registry.IntentSpooled {
			if err := st.SetIntentState(ctx, space, d, state); err != nil {
				t.Fatalf("SetIntentState: %v", err)
			}
		}
		if located {
			if err := st.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: d, Provider: "did:key:p", URL: "http://p/blob", Size: 7}); err != nil {
				t.Fatalf("PutLocation: %v", err)
			}
		}
		if parked {
			if err := st.PutPark(ctx, registry.BlobPark{Space: space, Digest: d, AddTask: []byte{1}, AcceptTask: []byte{2}, PutInvocation: []byte{3}, Size: 7}); err != nil {
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
				if err := st.MarkEvicted(ctx, d, time.Now().Add(time.Hour)); err != nil {
					t.Fatalf("MarkEvicted: %v", err)
				}
			}
			if got := len(listed(t, st)) == 1; got != tc.want {
				t.Fatalf("listed = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("SetIntentState to the current state keeps updated_at", func(t *testing.T) {
		st := fresh(t)
		d := evictDigest(t, "retried")
		seed(t, st, d, registry.IntentAccepted, true, false)
		before, err := st.GetIntent(ctx, space, d)
		if err != nil {
			t.Fatalf("GetIntent: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
		if err := st.SetIntentState(ctx, space, d, registry.IntentAccepted); err != nil {
			t.Fatalf("SetIntentState: %v", err)
		}
		repeated, err := st.GetIntent(ctx, space, d)
		if err != nil {
			t.Fatalf("GetIntent: %v", err)
		}
		if err := st.SetIntentState(ctx, space, d, registry.IntentPublished); err != nil {
			t.Fatalf("SetIntentState: %v", err)
		}
		changed, err := st.GetIntent(ctx, space, d)
		if err != nil {
			t.Fatalf("GetIntent: %v", err)
		}
		got := [2]bool{repeated.UpdatedAt.Equal(before.UpdatedAt), changed.UpdatedAt.After(before.UpdatedAt)}
		if got != [2]bool{true, true} {
			t.Fatalf("[repeat kept updated_at, change advanced it] = %v, want [true true]", got)
		}
	})

	t.Run("PutIntent clears the evicted mark", func(t *testing.T) {
		st := fresh(t)
		d := evictDigest(t, "respooled")
		seed(t, st, d, registry.IntentAccepted, true, false)
		if err := st.MarkEvicted(ctx, d, time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("MarkEvicted: %v", err)
		}
		if err := st.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: d, LocalPath: "/spool/x", Size: 7, State: registry.IntentAccepted}); err != nil {
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
		before, err := st.GetIntent(ctx, space, d)
		if err != nil {
			t.Fatalf("GetIntent: %v", err)
		}
		if err := st.MarkEvicted(ctx, d, time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("MarkEvicted: %v", err)
		}
		after, err := st.GetIntent(ctx, space, d)
		if err != nil {
			t.Fatalf("GetIntent: %v", err)
		}
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("intent after MarkEvicted = %+v, want %+v", after, before)
		}
	})

	t.Run("MarkEvicted of a missing intent is ErrNotFound", func(t *testing.T) {
		st := fresh(t)
		if err := st.MarkEvicted(ctx, evictDigest(t, "gone"), time.Now().Add(time.Hour)); !errors.Is(err, registry.ErrNotFound) {
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
				cursor = registry.EvictCursor{UpdatedAt: in.UpdatedAt, Digest: in.Digest, Space: in.Space}
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

	t.Run("StalledBytes sums spooled and uploading intents older than the cutoff", func(t *testing.T) {
		st := fresh(t)
		seed(t, st, evictDigest(t, "spooled"), registry.IntentSpooled, false, false)
		// A different size from seed's, so a sum credited to the wrong
		// state shows.
		uploading := evictDigest(t, "uploading")
		if err := st.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: uploading, LocalPath: "/spool/x", Size: 11, State: registry.IntentSpooled}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if err := st.SetIntentState(ctx, space, uploading, registry.IntentUploading); err != nil {
			t.Fatalf("SetIntentState: %v", err)
		}
		seed(t, st, evictDigest(t, "accepted"), registry.IntentAccepted, true, false)
		seed(t, st, evictDigest(t, "parked"), registry.IntentParked, false, true)
		var got []registry.StalledSizes
		// An hour either side of now, so a database clock off from this
		// one by less still puts every row on the same side.
		for _, before := range []time.Time{time.Now().Add(-time.Hour), time.Now().Add(time.Hour)} {
			s, err := st.StalledBytes(ctx, before)
			if err != nil {
				t.Fatalf("StalledBytes: %v", err)
			}
			got = append(got, s)
		}
		if want := []registry.StalledSizes{{}, {Spooled: 7, Uploading: 11}}; !reflect.DeepEqual(got, want) {
			t.Fatalf("StalledBytes before [an hour ago, an hour from now] = %v, want %v", got, want)
		}
	})

	t.Run("ListEvictable: an upload of the digest in another space keeps it", func(t *testing.T) {
		// The same bytes stored as received in two spaces are one spool
		// file: while either space is still uploading it, no row lists.
		st := fresh(t)
		d := evictDigest(t, "shared-upload")
		seed(t, st, d, registry.IntentAccepted, true, false)
		other := testutil.RandomDID(t)
		if err := st.PutIntent(ctx, registry.UploadIntent{Space: other, Digest: d, LocalPath: "/spool/x", Size: 7, State: registry.IntentSpooled}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		var got []int
		got = append(got, len(listed(t, st)))
		if err := st.SetIntentState(ctx, other, d, registry.IntentUploading); err != nil {
			t.Fatalf("SetIntentState: %v", err)
		}
		got = append(got, len(listed(t, st)))
		if err := st.SetIntentState(ctx, other, d, registry.IntentAccepted); err != nil {
			t.Fatalf("SetIntentState: %v", err)
		}
		// Accepted in the other space, but only located in the first:
		// the first row lists, the other does not.
		got = append(got, len(listed(t, st)))
		if err := st.PutLocation(ctx, registry.BlobLocation{Space: other, Digest: d, Provider: "did:key:p", URL: "http://p/blob", Size: 7}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		got = append(got, len(listed(t, st)))
		if want := []int{0, 0, 1, 2}; !reflect.DeepEqual(got, want) {
			t.Fatalf("rows listed while the other space is [spooled uploading accepted located] = %v, want %v", got, want)
		}
	})

	t.Run("MarkEvicted and PutIntent act on every space's intent", func(t *testing.T) {
		st := fresh(t)
		d := evictDigest(t, "shared-file")
		other := testutil.RandomDID(t)
		for _, sp := range []did.DID{space, other} {
			if err := st.PutIntent(ctx, registry.UploadIntent{Space: sp, Digest: d, LocalPath: "/spool/x", Size: 7, State: registry.IntentAccepted}); err != nil {
				t.Fatalf("PutIntent: %v", err)
			}
			if err := st.PutLocation(ctx, registry.BlobLocation{Space: sp, Digest: d, Provider: "did:key:p", URL: "http://p/blob", Size: 7}); err != nil {
				t.Fatalf("PutLocation: %v", err)
			}
		}
		var got []int
		got = append(got, len(listed(t, st)))
		if err := st.MarkEvicted(ctx, d, time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("MarkEvicted: %v", err)
		}
		got = append(got, len(listed(t, st)))
		// Spooled again by one space: the file is back for both.
		if err := st.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: d, LocalPath: "/spool/x", Size: 7, State: registry.IntentAccepted}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		got = append(got, len(listed(t, st)))
		if want := []int{2, 0, 2}; !reflect.DeepEqual(got, want) {
			t.Fatalf("rows listed [both held, evicted, respooled] = %v, want %v", got, want)
		}
	})

	t.Run("CountOtherIntents counts the other spaces' intents", func(t *testing.T) {
		st := fresh(t)
		d := evictDigest(t, "others")
		other := testutil.RandomDID(t)
		seed(t, st, d, registry.IntentAccepted, true, false)
		count := func() int {
			t.Helper()
			n, err := st.CountOtherIntents(ctx, space, d)
			if err != nil {
				t.Fatalf("CountOtherIntents: %v", err)
			}
			return n
		}
		var got []int
		got = append(got, count())
		if err := st.PutIntent(ctx, registry.UploadIntent{Space: other, Digest: d, LocalPath: "/spool/x", Size: 7, State: registry.IntentSpooled}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		got = append(got, count())
		if err := st.DeleteIntent(ctx, other, d); err != nil {
			t.Fatalf("DeleteIntent: %v", err)
		}
		got = append(got, count())
		if _, err := st.GetIntent(ctx, space, d); err != nil {
			t.Fatalf("the first space's intent after the other's delete: %v", err)
		}
		if want := []int{0, 1, 0}; !reflect.DeepEqual(got, want) {
			t.Fatalf("other intents [alone, shared, other deleted] = %v, want %v", got, want)
		}
	})

	t.Run("parks are one space's", func(t *testing.T) {
		st := fresh(t)
		d := evictDigest(t, "parked-twice")
		other := testutil.RandomDID(t)
		if err := st.PutPark(ctx, registry.BlobPark{Space: space, Digest: d, AddTask: []byte{1}, AcceptTask: []byte{2}, PutInvocation: []byte{3}, Size: 7}); err != nil {
			t.Fatalf("PutPark: %v", err)
		}
		if _, err := st.GetPark(ctx, other, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("GetPark in another space err = %v, want ErrNotFound", err)
		}
		if err := st.PutPark(ctx, registry.BlobPark{Space: other, Digest: d, AddTask: []byte{4}, AcceptTask: []byte{5}, PutInvocation: []byte{6}, Size: 7}); err != nil {
			t.Fatalf("PutPark: %v", err)
		}
		if err := st.DeletePark(ctx, other, d); err != nil {
			t.Fatalf("DeletePark: %v", err)
		}
		park, err := st.GetPark(ctx, space, d)
		if err != nil || park.AddTask[0] != 1 {
			t.Fatalf("the first space's park after the other's delete = %+v, %v", park, err)
		}
	})

	t.Run("MarkEvicted leaves an intent put since the listing", func(t *testing.T) {
		// The mark is as of the intent the caller listed or read: a write
		// that spooled the bytes again since has a newer intent and its
		// file on disk, and keeps its mark clear.
		st := fresh(t)
		d := evictDigest(t, "respooled-since")
		seed(t, st, d, registry.IntentAccepted, true, false)
		listed, err := st.GetIntent(ctx, space, d)
		if err != nil {
			t.Fatalf("GetIntent: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
		other := testutil.RandomDID(t)
		if err := st.PutIntent(ctx, registry.UploadIntent{Space: other, Digest: d, LocalPath: "/spool/x", Size: 7, State: registry.IntentAccepted}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if err := st.PutLocation(ctx, registry.BlobLocation{Space: other, Digest: d, Provider: "did:key:p", URL: "http://p/blob", Size: 7}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		if err := st.MarkEvicted(ctx, d, listed.UpdatedAt); err != nil {
			t.Fatalf("MarkEvicted: %v", err)
		}
		rows, err := st.ListEvictable(ctx, registry.EvictCursor{}, 10)
		if err != nil {
			t.Fatalf("ListEvictable: %v", err)
		}
		if len(rows) != 1 || rows[0].Space != other {
			t.Fatalf("listed after the mark = %+v, want only the newer intent", rows)
		}
		// Every intent newer than the mark: nothing marked, but not ErrNotFound.
		if err := st.MarkEvicted(ctx, d, listed.UpdatedAt); err != nil {
			t.Fatalf("MarkEvicted with nothing older: %v", err)
		}
	})
}
