package s3frontend

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
)

// countingUploader records how many times each digest is uploaded, so a test can
// assert that an already-located blob is not re-uploaded. It returns a non-empty
// location so the backend records it (the dedup signal GetLocation reads back).
type countingUploader struct {
	inmem.NopUploader
	mu    sync.Mutex
	calls map[string]int
}

func (u *countingUploader) UploadBlob(_ context.Context, _ did.DID, digest multihash.Multihash, size int64, _ string, _ ...uploader.UploadOption) (uploader.UploadedBlob, error) {
	u.mu.Lock()
	u.calls[string(digest)]++
	u.mu.Unlock()
	return uploader.UploadedBlob{
		Digest:   digest,
		Size:     size,
		Location: &uploader.BlobLocation{Provider: "did:test:piri", URL: "http://piri/blob", Size: size},
	}, nil
}

func (u *countingUploader) count(digest []byte) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls[string(digest)]
}

// TestUploadDedup_SkipsAlreadyLocatedBlob is the in-process regression for the
// forge dedup bug surfaced in smelt: re-uploading a blob already durably stored
// for the space (a re-PUT of identical content, an overwrite, or content shared
// with another key) makes the upload service return an accept receipt with no
// fresh location commitment, which the edge client rejects (500). uploadBlobs
// must short-circuit on the recorded location and not re-upload.
func TestUploadDedup_SkipsAlreadyLocatedBlob(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mem := inmem.NewMemStore()
	spool, err := blockstore.NewSpool(filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	log, err := logstore.Open(ctx, logstore.Config{
		Dir:     filepath.Join(dir, "segments"),
		Meta:    mem,
		Catalog: logstore.PlaneConfig{Ship: false},
		Logger:  zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatalf("logstore: %v", err)
	}
	t.Cleanup(func() { _ = log.Close(ctx) })

	up := &countingUploader{calls: map[string]int{}}
	b := New(Deps{
		Authority:  mem,
		Registry:   mem,
		Intents:    mem,
		Locations:  mem,
		BlobRefs:   mem,
		GC:         mem,
		Reads:      blockstore.NewLayered(spool, log, inmem.NopBaseReader{}),
		Log:        log,
		Spool:      spool,
		Uploader:   up,
		Remover:    &recordingRemover{},
		EncParams:  mem,
		RegionKeys: testRegionKeys(t),
	})
	if err := mem.Create(ctx, "bk", testutil.RandomDID(t), registry.CreateState{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	data := []byte("dedup me across puts and keys")
	d := digestOf(t, data)

	putObj(t, b, "k1", data) // first upload
	putObj(t, b, "k1", data) // overwrite-in-place with identical content
	putObj(t, b, "k2", data) // different key, same content

	if n := up.count(d); n != 1 {
		t.Fatalf("UploadBlob called %d times for the shared blob, want 1 (already-located blobs are skipped)", n)
	}
}
