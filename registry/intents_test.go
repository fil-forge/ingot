package registry_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
)

// intentStore is what the intent queries need of a store.
type intentStore interface {
	registry.IntentStore
	registry.LocationStore
	registry.ParkStore
}

// TestIntentQueries runs the intent queries against Postgres, each case
// on a database of its own.
func TestIntentQueries(t *testing.T) {
	runIntentSuite(t, func(t *testing.T) intentStore { return registry.NewPostgres(pgtest.New(t)) })
}

func intentDigest(t *testing.T, s string) multihash.Multihash {
	t.Helper()
	d, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return d
}

func runIntentSuite(t *testing.T, fresh func(t *testing.T) intentStore) {
	ctx := t.Context()
	space := testutil.RandomDID(t)

	// seed records an intent in state, optionally with a location or a park.
	seed := func(t *testing.T, st intentStore, d multihash.Multihash, state string, located, parked bool) {
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
			if _, err := st.PutPark(ctx, registry.BlobPark{Space: space, Digest: d, AddTask: []byte{1}, AcceptTask: []byte{2}, PutInvocation: []byte{3}, Size: 7}); err != nil {
				t.Fatalf("PutPark: %v", err)
			}
		}
	}
	t.Run("SetIntentState to the current state keeps updated_at", func(t *testing.T) {
		st := fresh(t)
		d := intentDigest(t, "retried")
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

	t.Run("MissingIntents returns only unknown digests", func(t *testing.T) {
		st := fresh(t)
		known := intentDigest(t, "known")
		unknown := intentDigest(t, "unknown")
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
		seed(t, st, intentDigest(t, "spooled"), registry.IntentSpooled, false, false)
		// A different size from seed's, so a sum credited to the wrong
		// state shows.
		uploading := intentDigest(t, "uploading")
		if err := st.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: uploading, LocalPath: "/spool/x", Size: 11, State: registry.IntentSpooled}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if err := st.SetIntentState(ctx, space, uploading, registry.IntentUploading); err != nil {
			t.Fatalf("SetIntentState: %v", err)
		}
		seed(t, st, intentDigest(t, "accepted"), registry.IntentAccepted, true, false)
		seed(t, st, intentDigest(t, "parked"), registry.IntentParked, false, true)
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

	t.Run("CountOtherIntents counts the other spaces' intents", func(t *testing.T) {
		st := fresh(t)
		d := intentDigest(t, "others")
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
		d := intentDigest(t, "parked-twice")
		other := testutil.RandomDID(t)
		if _, err := st.PutPark(ctx, registry.BlobPark{Space: space, Digest: d, AddTask: []byte{1}, AcceptTask: []byte{2}, PutInvocation: []byte{3}, Size: 7}); err != nil {
			t.Fatalf("PutPark: %v", err)
		}
		if _, err := st.GetPark(ctx, other, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("GetPark in another space err = %v, want ErrNotFound", err)
		}
		if _, err := st.PutPark(ctx, registry.BlobPark{Space: other, Digest: d, AddTask: []byte{4}, AcceptTask: []byte{5}, PutInvocation: []byte{6}, Size: 7}); err != nil {
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
}
