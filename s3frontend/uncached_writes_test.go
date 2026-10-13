package s3frontend

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/fil-forge/ingot/inmem"
)

func dropAccepted(d *Deps) { d.DropAcceptedBodies = true }

// TestDropAcceptedBodies: with written bodies not cached, a PUT keeps no
// local copy once its blobs are accepted, and their intents are marked
// evicted; by default the copy stays, in the cache.
func TestDropAcceptedBodies(t *testing.T) {
	for name, tc := range map[string]struct {
		mods     []func(*Deps)
		wantFile bool
	}{
		"cached by default": {wantFile: true},
		"not cached":        {mods: []func(*Deps){dropAccepted}},
	} {
		t.Run(name, func(t *testing.T) {
			b, mem := newDeferredBackend(t, inmem.NopUploader{}, tc.mods...)
			putObj(t, b, "k", testBody(1<<10))
			d := blobDigestOf(t, b, "k", "")

			require.Equal(t, tc.wantFile, fileExists(b.cache.Path(d)), "cached copy present")
			require.False(t, fileExists(b.spool.Path(d)), "spool copy present")
			require.Equal(t, !tc.wantFile, mem.IsEvicted(d))
		})
	}
}

// TestDropAcceptedBodiesStreamed: a streamed PUT's blobs leave local disk
// when their conclude records them accepted, and the object reads back from the
// provider.
func TestDropAcceptedBodiesStreamed(t *testing.T) {
	su := newStreamingUploader()
	b, mem := newStreamingBackend(t, su, dropAccepted)
	data := testBody(700 << 10)
	require.NoError(t, putSized(t, b, "k", data, int64(len(data))))

	rv, err := b.resolveVersion(t.Context(), "bk", "k", "")
	require.NoError(t, err)
	require.Greater(t, len(rv.mf.Body.Blobs), 1)
	for _, blob := range rv.mf.Body.Blobs {
		require.False(t, fileExists(localPath(b, blob.Digest)), "blob %x has a local copy", blob.Digest)
		require.True(t, mem.IsEvicted(blob.Digest))
	}
	require.Equal(t, data, getRange(t, b, "k", ""))
}

// TestDropAcceptedBodiesMultipart: with written bodies not cached, a part the
// provider accepts at once (the no-op uploader's parks come back with a
// location) keeps no local copy from UploadPart on, and Complete leaves none.
func TestDropAcceptedBodiesMultipart(t *testing.T) {
	b, mem := newDeferredBackend(t, inmem.NopUploader{}, dropAccepted)
	key := "mp"
	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err)
	digests := hygienePartDigests(t, mem, uploadID, 1)
	require.NotEmpty(t, digests)
	for _, d := range digests {
		require.False(t, fileExists(localPath(b, d)), "part blob %x has a local copy after UploadPart", d)
		require.True(t, mem.IsEvicted(d), "part blob %x not marked evicted", d)
	}
	one := int32(1)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}, nil)
	require.NoError(t, err)
	for _, d := range digests {
		require.False(t, fileExists(localPath(b, d)), "part blob %x has a local copy after Complete", d)
	}
}

// TestDropAcceptedBodiesCountsRemovals: each dropped copy counts under the
// "accepted" reason.
func TestDropAcceptedBodiesCountsRemovals(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newDeferredBackend(t, inmem.NopUploader{}, dropAccepted, meteredBackend(reader))
	data := testBody(1 << 10)
	putObj(t, b, "k", data)

	got := collectLocalBlobMetrics(t, reader)
	require.Equal(t, int64(1), got["ingot.local_blobs.removals/accepted"], "removals: %v", got)
	require.Positive(t, got["ingot.local_blobs.removed_bytes/accepted"], "removed bytes: %v", got)
}
