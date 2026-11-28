package s3frontend

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
)

// countingUploader records how many times each digest is uploaded. It returns
// a non-empty location so the backend records it (what the add's
// already-located short-circuit reads back).
type countingUploader struct {
	testfakes.NopUploader
	mu    sync.Mutex
	calls map[string]int
}

func (u *countingUploader) UploadBlob(_ context.Context, _ did.DID, digest multihash.Multihash, size int64, _ string) (uploader.UploadedBlob, error) {
	u.mu.Lock()
	u.calls[string(digest)]++
	u.mu.Unlock()
	return uploader.UploadedBlob{
		Digest:   digest,
		Size:     size,
		Location: &uploader.BlobLocation{Provider: "did:test:piri", URL: "http://piri/blob", Size: size},
	}, nil
}

// TestUpload_IdenticalContentUploadsDistinctBlobs pins the encryption RFC's
// stated dedup trade-off: every write mints a fresh CEK, so identical
// plaintext produces a different ciphertext digest each time and each PUT
// uploads its own blob exactly once. (The add still short-circuits on an
// already-recorded location — that guard now only fires for digests shared
// by same-space copies, where no re-upload is needed.)
func TestUpload_IdenticalContentUploadsDistinctBlobs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mem := newTestStore(t)
	cache, err := blockstore.NewBlobCache(filepath.Join(dir, "cache"))
	require.NoError(t, err, "cache")
	log, err := logstore.Open(ctx, logstore.Config{
		Dir:     filepath.Join(dir, "segments"),
		Meta:    mem,
		Catalog: logstore.PlaneConfig{Ship: false},
		Logger:  zaptest.NewLogger(t),
	})
	require.NoError(t, err, "logstore")
	t.Cleanup(func() { _ = log.Close(ctx) })

	up := &countingUploader{calls: map[string]int{}}
	deps := Deps{
		Authority:  mem,
		Registry:   mem,
		Intents:    mem,
		Locations:  mem,
		Reads:      blockstore.NewLayered(cache, log, testfakes.NopBaseReader{}),
		Log:        log,
		Cache:      cache,
		Deferred:   up,
		Remover:    &recordingRemover{},
		Claims:     mem,
		Capturer:   testfakes.NopUploader{},
		EncParams:  mem,
		RegionKeys: testRegionKeys(t),
		TenantKeys: testTenantKeys(),
	}
	withStreaming(t, &deps, dir)
	b := mem.newBackend(t, deps)
	require.NoError(t, mem.Create(ctx, "bk", testutil.RandomDID(t), registry.CreateState{}), "create bucket")

	data := []byte("identical bytes across puts and keys")

	putObj(t, b, "k1", data)
	putObj(t, b, "k1", data) // overwrite-in-place with identical content
	putObj(t, b, "k2", data) // different key, same content

	up.mu.Lock()
	defer up.mu.Unlock()
	require.Len(t, up.calls, 3, "uploaded %d distinct digests, want 3 (fresh CEK per write ends content dedup)", len(up.calls))
	for d, n := range up.calls {
		require.Equal(t, 1, n, "UploadBlob called %d times for blob %x", n, []byte(d))
	}
	n := up.calls[string(digestOf(t, data))]
	require.Zero(t, n, "a stored digest equals hash(plaintext); blobs were not encrypted")
}
