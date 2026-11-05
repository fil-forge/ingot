package s3frontend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/uploader"
)

// streamingAdapter gives a deferred test uploader the streaming seam every
// upload needs: StartBlob allocates, and PutBlob hashes the bytes and parks
// them through the fake's own UploadBlob. The stream's add task stands in for the fake's until then, so the
// conclude and abort that follow reach the fake under the names it gave the
// park, and a fake's records read as they did when it parked directly. It
// also serves the bytes it was sent as the network tier would, since a
// parked blob keeps no local copy.
type streamingAdapter struct {
	deferredTestUploader
	dir string

	mu      sync.Mutex
	next    int
	spaces  map[cid.Cid]did.DID
	uploads map[cid.Cid]uploader.UploadedBlob
	blobs   map[string][]byte
	digests map[cid.Cid]multihash.Multihash
	aborted map[string]int
	accepts map[string]int
}

func newStreamingAdapter(up deferredTestUploader, dir string) *streamingAdapter {
	return &streamingAdapter{deferredTestUploader: up, dir: dir, spaces: map[cid.Cid]did.DID{}, uploads: map[cid.Cid]uploader.UploadedBlob{}, blobs: map[string][]byte{}, digests: map[cid.Cid]multihash.Multihash{}, aborted: map[string]int{}, accepts: map[string]int{}}
}

func (a *streamingAdapter) StartBlob(_ context.Context, space did.DID, size int64) (uploader.StreamedBlob, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.next++
	sum, err := multihash.Sum(fmt.Appendf(nil, "stream %d", a.next), multihash.SHA2_256, -1)
	if err != nil {
		return uploader.StreamedBlob{}, err
	}
	add := cid.NewCidV1(cid.Raw, sum)
	a.spaces[add] = space
	return uploader.StreamedBlob{Size: size, AddTask: add, AcceptTask: add, PutInvocation: []byte("put")}, nil
}

func (a *streamingAdapter) PutBlob(ctx context.Context, sb uploader.StreamedBlob, body io.Reader) error {
	f, err := os.CreateTemp(a.dir, "stream-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), body)
	if err != nil {
		return err
	}
	if n != sb.Size {
		return fmt.Errorf("streamed %d bytes, allocated %d", n, sb.Size)
	}
	digest, err := multihash.Encode(h.Sum(nil), multihash.SHA2_256)
	if err != nil {
		return err
	}
	a.mu.Lock()
	space := a.spaces[sb.AddTask]
	a.mu.Unlock()
	up, err := a.deferredTestUploader.UploadBlob(ctx, space, digest, n, f.Name())
	if err != nil {
		return err
	}
	data, err := os.ReadFile(f.Name())
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.blobs[string(digest)] = data
	a.digests[sb.AddTask] = digest
	// A fake that names a park by its digest gives a second stream of the
	// same bytes the first's name. That stream keeps its own, so giving up
	// its surplus allocation is not taken for an abort of the first's park.
	if up.AddTask.Defined() {
		for add, prev := range a.uploads {
			if prev.AddTask.Equals(up.AddTask) && a.spaces[add] == space {
				return nil
			}
		}
	}
	a.uploads[sb.AddTask] = up
	return nil
}

// GetBlock serves no catalog blocks: those come from the log tier.
func (a *streamingAdapter) GetBlock(context.Context, did.DID, cid.Cid) (block.Block, error) {
	return nil, blockstore.ErrNotFound
}

// OpenBlob serves a blob from the bytes a stream sent.
func (a *streamingAdapter) OpenBlob(_ context.Context, _ did.DID, digest multihash.Multihash) (io.ReadCloser, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if data, ok := a.blobs[string(digest)]; ok {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return nil, blockstore.ErrNotFound
}

// fakes returns the park the fake made for a stream's add task, or the
// blob as given when the stream sent nothing.
func (a *streamingAdapter) fakes(b uploader.UploadedBlob) uploader.UploadedBlob {
	a.mu.Lock()
	defer a.mu.Unlock()
	if up, ok := a.uploads[b.AddTask]; ok {
		if b.Digest != nil {
			up.Digest, up.Size = b.Digest, b.Size
		}
		return up
	}
	return b
}

func (a *streamingAdapter) ConcludeBlobs(ctx context.Context, space did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	named := make([]uploader.UploadedBlob, len(parked))
	for i, p := range parked {
		named[i] = a.fakes(p)
	}
	locs, err := a.deferredTestUploader.ConcludeBlobs(ctx, space, named)
	a.mu.Lock()
	for i, l := range locs {
		if l != nil && i < len(parked) {
			a.accepts[string(parked[i].Digest)]++
		}
	}
	a.mu.Unlock()
	return locs, err
}

func (a *streamingAdapter) AbortBlob(ctx context.Context, space did.DID, add cid.Cid) error {
	park := a.fakes(uploader.UploadedBlob{AddTask: add})
	if err := a.deferredTestUploader.AbortBlob(ctx, space, park.AddTask); err != nil {
		return err
	}
	a.mu.Lock()
	if d, ok := a.digests[add]; ok {
		a.aborted[string(d)]++
	}
	a.mu.Unlock()
	return nil
}

// streamCounts reports, for digest, how many streams b's provider was sent,
// how many of them it accepted, and how many it was asked to abort.
func streamCounts(t testing.TB, b *Backend, digest multihash.Multihash) (sent, accepted, aborted int) {
	t.Helper()
	a, ok := b.deferred.(*streamingAdapter)
	if !ok {
		t.Fatalf("backend has no streaming adapter")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, d := range a.digests {
		if string(d) == string(digest) {
			sent++
		}
	}
	return sent, a.accepts[string(digest)], a.aborted[string(digest)]
}

// abortedOn reports whether b's provider was asked to abort a park of digest,
// through the test's streaming adapter.
func abortedOn(b *Backend, digest multihash.Multihash) bool {
	a, ok := b.deferred.(*streamingAdapter)
	if !ok {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.aborted[string(digest)] > 0
}

// streamingDir is where a test's adapter writes the bytes it hands its fake.
func streamingDir(dir string) string { return filepath.Join(dir, "streams") }

// byteLen is the declared length of a body of data.
func byteLen(data []byte) *int64 {
	n := int64(len(data))
	return &n
}

// withStreaming gives a test backend's deps the streaming seam every part
// upload needs, wrapping its deferred fake (streamingAdapter), unless the
// test supplies its own. dir is the test's temp directory.
func withStreaming(t testing.TB, deps *Deps, dir string) {
	t.Helper()
	if deps.Streaming != nil {
		return
	}
	up, ok := deps.Deferred.(deferredTestUploader)
	if !ok {
		t.Fatalf("test deps: deferred uploader %T cannot stand behind the streaming seam", deps.Deferred)
	}
	if err := os.MkdirAll(streamingDir(dir), 0o755); err != nil {
		t.Fatalf("streams dir: %v", err)
	}
	a := newStreamingAdapter(up, streamingDir(dir))
	deps.Streaming, deps.Deferred = a, a
	deps.Reads = blockstore.NewLayered(deps.Cache, deps.Log, a)
}

// storedBytes is a blob's bytes as stored: its local copy, or what its
// provider was sent when it keeps none, as a parked part does.
func storedBytes(t testing.TB, b *Backend, digest multihash.Multihash) []byte {
	t.Helper()
	if data, err := os.ReadFile(localPath(b, digest)); err == nil {
		return data
	}
	if a, ok := b.deferred.(*streamingAdapter); ok {
		a.mu.Lock()
		data, ok := a.blobs[string(digest)]
		a.mu.Unlock()
		if ok {
			return data
		}
	}
	t.Fatalf("blob %x: no local copy, and its provider holds none", digest)
	return nil
}
