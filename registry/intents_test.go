package registry_test

import (
	"testing"
	"time"

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err, "multihash")
	return d
}

func runIntentSuite(t *testing.T, fresh func(t *testing.T) intentStore) {
	ctx := t.Context()
	space := testutil.RandomDID(t)

	// seed records an intent in state, optionally with a location or a park.
	seed := func(t *testing.T, st intentStore, d multihash.Multihash, state string, located, parked bool) {
		t.Helper()
		require.NoError(t, st.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: d, Size: 7, State: registry.IntentUploading}), "PutIntent")
		if state != registry.IntentUploading {
			require.NoError(t, st.SetIntentState(ctx, space, d, state), "SetIntentState")
		}
		if located {
			require.NoError(t, st.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: d, Provider: "did:key:p", URL: "http://p/blob", Size: 7}), "PutLocation")
		}
		if parked {
			_, err := st.PutPark(ctx, registry.BlobPark{Space: space, Digest: d, AddTask: []byte{1}, AcceptTask: []byte{2}, PutInvocation: []byte{3}, Size: 7})
			require.NoError(t, err, "PutPark")
		}
	}
	t.Run("SetIntentState to the current state keeps updated_at", func(t *testing.T) {
		st := fresh(t)
		d := intentDigest(t, "retried")
		seed(t, st, d, registry.IntentAccepted, true, false)
		before, err := st.GetIntent(ctx, space, d)
		require.NoError(t, err, "GetIntent")
		time.Sleep(5 * time.Millisecond)
		require.NoError(t, st.SetIntentState(ctx, space, d, registry.IntentAccepted), "SetIntentState")
		repeated, err := st.GetIntent(ctx, space, d)
		require.NoError(t, err, "GetIntent")
		require.NoError(t, st.SetIntentState(ctx, space, d, registry.IntentPublished), "SetIntentState")
		changed, err := st.GetIntent(ctx, space, d)
		require.NoError(t, err, "GetIntent")
		got := [2]bool{repeated.UpdatedAt.Equal(before.UpdatedAt), changed.UpdatedAt.After(before.UpdatedAt)}
		require.Equal(t, [2]bool{true, true}, got, "[repeat kept updated_at, change advanced it]")
	})

	t.Run("StalledBytes sums uploading intents older than the cutoff", func(t *testing.T) {
		st := fresh(t)
		seed(t, st, intentDigest(t, "uploading"), registry.IntentUploading, false, false)
		seed(t, st, intentDigest(t, "accepted"), registry.IntentAccepted, true, false)
		seed(t, st, intentDigest(t, "parked"), registry.IntentParked, false, true)
		var got []int64
		// An hour either side of now, so a database clock off from this
		// one by less still puts every row on the same side.
		for _, before := range []time.Time{time.Now().Add(-time.Hour), time.Now().Add(time.Hour)} {
			n, err := st.StalledBytes(ctx, before)
			require.NoError(t, err, "StalledBytes")
			got = append(got, n)
		}
		want := []int64{0, 7}
		require.Equal(t, want, got, "StalledBytes before [an hour ago, an hour from now]")
	})

	t.Run("HasIntent reports whether any space holds an intent", func(t *testing.T) {
		st := fresh(t)
		d := intentDigest(t, "others")
		other := testutil.RandomDID(t)
		has := func() bool {
			t.Helper()
			held, err := st.HasIntent(ctx, d)
			require.NoError(t, err, "HasIntent")
			return held
		}
		var got []bool
		got = append(got, has())
		seed(t, st, d, registry.IntentAccepted, true, false)
		got = append(got, has())
		require.NoError(t, st.PutIntent(ctx, registry.UploadIntent{Space: other, Digest: d, Size: 7, State: registry.IntentUploading}), "PutIntent")
		require.NoError(t, st.DeleteIntent(ctx, space, d), "DeleteIntent")
		got = append(got, has())
		require.NoError(t, st.DeleteIntent(ctx, other, d), "DeleteIntent")
		got = append(got, has())
		require.Equal(t, []bool{false, true, true, false}, got, "held [none, one space, the other, neither]")
	})

	t.Run("parks are one space's", func(t *testing.T) {
		st := fresh(t)
		d := intentDigest(t, "parked-twice")
		other := testutil.RandomDID(t)
		_, err := st.PutPark(ctx, registry.BlobPark{Space: space, Digest: d, AddTask: []byte{1}, AcceptTask: []byte{2}, PutInvocation: []byte{3}, Size: 7})
		require.NoError(t, err, "PutPark")
		_, err = st.GetPark(ctx, other, d)
		require.ErrorIs(t, err, registry.ErrNotFound, "GetPark in another space")
		_, err = st.PutPark(ctx, registry.BlobPark{Space: other, Digest: d, AddTask: []byte{4}, AcceptTask: []byte{5}, PutInvocation: []byte{6}, Size: 7})
		require.NoError(t, err, "PutPark")
		require.NoError(t, st.DeletePark(ctx, other, d), "DeletePark")
		park, err := st.GetPark(ctx, space, d)
		require.NoError(t, err, "the first space's park after the other's delete")
		require.Equal(t, byte(1), park.AddTask[0], "the first space's park after the other's delete = %+v", park)
	})
}
