package s3frontend

import (
	"bytes"
	"testing"
	"time"

	"github.com/fil-forge/versitygw/s3response"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/registry"
)

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
		if _, err := b.PutObject(t.Context(), s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(body)}); err == nil {
			t.Fatal("PutObject succeeded although recording the acceptance failed")
		}
	}
	// stalledUploading checks that the intent is uploading and counted as a
	// stalled upload, and returns it.
	stalledUploading := func(t *testing.T, mem *inmem.MemStore, digest []byte) *registry.UploadIntent {
		t.Helper()
		in, err := mem.GetIntent(t.Context(), digest)
		if err != nil || in.State != registry.IntentUploading {
			t.Fatalf("intent = %+v/%v, want uploading", in, err)
		}
		sizes, err := mem.StalledBytes(t.Context(), time.Now().Add(time.Hour))
		if err != nil || sizes.Uploading != in.Size {
			t.Fatalf("stalled = %+v/%v, want %d uploading", sizes, err, in.Size)
		}
		return in
	}

	t.Run("location write fails", func(t *testing.T) {
		locs := &failOncePutLocation{armed: true}
		b, mem := newSweepBackend(t, func(d *Deps) {
			locs.LocationStore = d.Locations
			d.Locations = locs
		})
		put(t, b)
		if locs.refused == nil {
			t.Fatal("the location store never refused a write")
		}
		stalledUploading(t, mem, locs.refused)
	})

	t.Run("accepted write fails", func(t *testing.T) {
		intents := &failOnceMarkAccepted{armed: true}
		b, mem := newSweepBackend(t, func(d *Deps) {
			intents.IntentStore = d.Intents
			d.Intents = intents
		})
		put(t, b)
		if intents.refused == nil {
			t.Fatal("the intent store never refused the accepted transition")
		}
		stalledUploading(t, mem, intents.refused)
		// The location landed, so a release of the stalled intent, if one
		// runs, finds it and removes the blob from the provider.
		st, err := mem.Get(t.Context(), sweepBucket)
		if err != nil {
			t.Fatalf("get bucket: %v", err)
		}
		if loc, err := mem.GetLocation(t.Context(), st.Space, intents.refused); err != nil || loc == nil {
			t.Fatalf("no location recorded for the accepted blob (err=%v)", err)
		}
	})
}
