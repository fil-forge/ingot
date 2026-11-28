package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/registry"
)

// failOnceMarkAccepted is an intent store whose next transition to accepted
// fails, after the location for that blob has already been recorded.
type failOnceMarkAccepted struct {
	registry.IntentStore
	armed   bool
	refused multihash.Multihash
}

func (f *failOnceMarkAccepted) SetIntentState(ctx context.Context, space did.DID, digest multihash.Multihash, state string) error {
	if f.armed && state == registry.IntentAccepted {
		f.armed = false
		f.refused = digest
		return errors.New("intents table unavailable")
	}
	return f.IntentStore.SetIntentState(ctx, space, digest, state)
}

// TestUploadBlobRecordsLocationBeforeAccepted: a by-digest upload that the
// provider accepts records the location before marking the intent accepted.
// Whichever of the two writes fails, the intent is left uploading, where it
// counts as a stalled upload, and never accepted without a location, which
// neither eviction nor the stalled count sees.
func TestUploadBlobRecordsLocationBeforeAccepted(t *testing.T) {
	put := func(t *testing.T, b *Backend) {
		t.Helper()
		bucket, key := sweepBucket, "obj"
		body := bytes.Repeat([]byte{'a'}, 1000)
		_, err := b.PutObject(t.Context(), s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(body), ContentLength: byteLen(body)})
		require.Error(t, err, "PutObject succeeded although recording the acceptance failed")
	}
	// stalledUploading checks that the intent is uploading and counted as a
	// stalled upload, and returns it.
	stalledUploading := func(t *testing.T, mem *testStore, digest []byte) *registry.UploadIntent {
		t.Helper()
		in, err := mem.IntentByDigest(t.Context(), digest)
		require.NoError(t, err, "intent = %+v/%v", in, err)
		require.Equal(t, registry.IntentUploading, in.State, "intent = %+v/%v", in, err)
		stalled, err := mem.StalledBytes(t.Context(), time.Now().Add(time.Hour))
		require.NoError(t, err, "stalled = %d/%v", stalled, err)
		require.Equal(t, in.Size, stalled, "stalled = %d/%v", stalled, err)
		return in
	}

	t.Run("location write fails", func(t *testing.T) {
		locs := &failOncePutLocation{armed: true}
		b, mem := newSweepBackend(t, func(d *Deps) {
			locs.LocationStore = d.Locations
			d.Locations = locs
		})
		put(t, b)
		require.NotNil(t, locs.refused, "the location store never refused a write")
		stalledUploading(t, mem, locs.refused)
	})

	t.Run("accepted write fails", func(t *testing.T) {
		intents := &failOnceMarkAccepted{armed: true}
		b, mem := newSweepBackend(t, func(d *Deps) {
			intents.IntentStore = d.Intents
			d.Intents = intents
		})
		put(t, b)
		require.NotNil(t, intents.refused, "the intent store never refused the accepted transition")
		stalledUploading(t, mem, intents.refused)
		// The location landed, so a release of the stalled intent, if one
		// runs, finds it and removes the blob from the provider.
		st, err := mem.Get(t.Context(), sweepBucket)
		require.NoError(t, err, "get bucket")
		loc, err := mem.GetLocation(t.Context(), st.Space, intents.refused)
		require.NoError(t, err, "location for the accepted blob")
		require.NotNil(t, loc, "no location recorded for the accepted blob")
	})
}
