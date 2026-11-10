package s3frontend

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/filecoin-project/go-fee/aesstream"
	"github.com/filecoin-project/go-fee/cose"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// streamingUploader is a provider that takes blobs allocated by digest code.
// It records every allocation, the bytes each PUT received, and what was
// concluded and aborted.
type streamingUploader struct {
	testfakes.NopUploader

	// unsupported refuses every allocation by digest code.
	unsupported bool
	// failPuts makes the PUTs fail after reading this many bytes; negative
	// never fails.
	failPuts int64

	mu        sync.Mutex
	started   []uploader.StreamedBlob
	puts      map[cid.Cid][]byte
	putErrs   map[cid.Cid]error
	concluded []uploader.UploadedBlob
	aborted   []cid.Cid
	// abortErr, when set, is what AbortBlob answers.
	abortErr error
	// abortStalls makes AbortBlob wait for its context to end, as a stalled
	// upload service does.
	abortStalls bool
	// concludeErr, when set, is returned by ConcludeBlobs alongside the
	// locations, as a batch that failed after its accepts ran returns it.
	concludeErr error
}

func newStreamingUploader() *streamingUploader {
	return &streamingUploader{failPuts: -1, puts: map[cid.Cid][]byte{}, putErrs: map[cid.Cid]error{}}
}

func (s *streamingUploader) StartBlob(_ context.Context, _ did.DID, size int64) (uploader.StreamedBlob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unsupported {
		return uploader.StreamedBlob{}, errors.New("the upload service cannot add a blob by digest code")
	}
	n := len(s.started)
	sb := uploader.StreamedBlob{
		Size:          size,
		AddTask:       cid.NewCidV1(cid.Raw, mustSum([]byte{'a', byte(n)})),
		AcceptTask:    cid.NewCidV1(cid.Raw, mustSum([]byte{'c', byte(n)})),
		PutInvocation: []byte{'p', byte(n)},
	}
	s.started = append(s.started, sb)
	return sb, nil
}

func (s *streamingUploader) PutBlob(_ context.Context, sb uploader.StreamedBlob, body io.Reader) error {
	var buf bytes.Buffer
	var err error
	if s.failPuts >= 0 {
		_, _ = io.CopyN(&buf, body, s.failPuts)
		err = errors.New("provider went away")
	} else {
		_, err = io.Copy(&buf, body)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts[sb.AddTask] = buf.Bytes()
	s.putErrs[sb.AddTask] = err
	return err
}

func (s *streamingUploader) ConcludeBlobs(ctx context.Context, space did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	s.mu.Lock()
	s.concluded = append(s.concluded, parked...)
	concludeErr := s.concludeErr
	s.mu.Unlock()
	locations, err := s.NopUploader.ConcludeBlobs(ctx, space, parked)
	if err != nil {
		return locations, err
	}
	return locations, concludeErr
}

func (s *streamingUploader) AbortBlob(ctx context.Context, _ did.DID, cause cid.Cid) error {
	s.mu.Lock()
	s.aborted = append(s.aborted, cause)
	stalls, err := s.abortStalls, s.abortErr
	s.mu.Unlock()
	if stalls {
		<-ctx.Done()
		return ctx.Err()
	}
	return err
}

// GetBlock serves no catalog blocks: those come from the log tier.
func (s *streamingUploader) GetBlock(context.Context, did.DID, cid.Cid) (block.Block, error) {
	return nil, blockstore.ErrNotFound
}

// OpenBlob serves a blob from the bytes a successful PUT delivered, so a test
// can read an object that keeps no local copy, as the network tier would.
func (s *streamingUploader) OpenBlob(_ context.Context, _ did.DID, digest multihash.Multihash) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for add, data := range s.puts {
		if s.putErrs[add] == nil && bytes.Equal(mustSum(data), digest) {
			return io.NopCloser(bytes.NewReader(data)), nil
		}
	}
	return nil, blockstore.ErrNotFound
}

func mustSum(b []byte) multihash.Multihash {
	sum := sha256.Sum256(b)
	d, err := multihash.Encode(sum[:], multihash.SHA2_256)
	if err != nil {
		panic(err)
	}
	return d
}

const streamBlobCeiling = 300 << 10

func newStreamingBackend(t *testing.T, su *streamingUploader, mods ...func(*Deps)) (*Backend, *testStore) {
	t.Helper()
	return newDeferredBackend(t, su, append([]func(*Deps){func(d *Deps) {
		d.Streaming = su
		d.Reads = blockstore.NewLayered(d.Cache, d.Log, su)
		d.MaxBlobSize = streamBlobCeiling
	}}, mods...)...)
}

func putSized(t *testing.T, b *Backend, key string, data []byte, declared int64) error {
	t.Helper()
	bucket := "bk"
	_, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          bytes.NewReader(data),
		ContentLength: &declared,
	})
	return err
}

func cached(t *testing.T, b *Backend, digest multihash.Multihash) []byte {
	t.Helper()
	data, err := os.ReadFile(localPath(b, digest))
	require.NoError(t, err)
	return data
}

func TestStreamedPutObject(t *testing.T) {
	su := newStreamingUploader()
	b, mem := newStreamingBackend(t, su)
	ctx := context.Background()
	data := testBody(700 << 10) // 3 blobs

	require.NoError(t, putSized(t, b, "k", data, int64(len(data))))

	digests := blobDigestsOf(t, b, "k", "")
	require.Len(t, digests, 3)
	require.Len(t, su.started, 3, "every blob is allocated before its digest is known")
	require.Len(t, su.concluded, 3)
	for i, d := range digests {
		sb := su.started[i]
		envelope := cached(t, b, d)
		require.Equal(t, envelope, su.puts[sb.AddTask], "the provider got the envelope the cache holds")
		require.Equal(t, int64(len(envelope)), sb.Size, "the allocation was the envelope's length")

		require.Equal(t, d, su.concluded[i].Digest, "the conclude reports the envelope's digest")
		require.Equal(t, sb.AddTask, su.concluded[i].AddTask)

		in, err := mem.IntentByDigest(ctx, d)
		require.NoError(t, err)
		require.Equal(t, registry.IntentPublished, in.State, "the committed blob's intent is published")
	}
	require.Equal(t, data, getRange(t, b, "k", ""))
}

// A streamed PUT that carries a Content-MD5 takes its ETag from the header
// rather than from an MD5 pass of its own, and streams as any other.
func TestStreamedPutObjectContentMD5(t *testing.T) {
	su := newStreamingUploader()
	b, _ := newStreamingBackend(t, su)
	data := testBody(700 << 10) // 3 blobs
	sum := md5.Sum(data)
	bucket, key, n := "bk", "k", int64(len(data))

	out, err := b.PutObject(ctxWithContentMD5(data), s3response.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          bytes.NewReader(data),
		ContentLength: &n,
	})
	require.NoError(t, err)
	require.Equal(t, `"`+hex.EncodeToString(sum[:])+`"`, out.ETag)
	require.Len(t, su.started, 3, "the blobs streamed")
	require.Equal(t, data, getRange(t, b, "k", ""))
}

func TestStreamedPutObjectEmpty(t *testing.T) {
	su := newStreamingUploader()
	b, _ := newStreamingBackend(t, su)
	require.NoError(t, putSized(t, b, "k", nil, 0))
	require.Empty(t, su.started, "an empty body has no blob to send")
	require.Empty(t, getRange(t, b, "k", ""))
}

// TestStreamedPutObjectFailedPut: a PUT that fails fails the request, its
// allocation is aborted at once, and its local copy is discarded.
func TestStreamedPutObjectFailedPut(t *testing.T) {
	su := newStreamingUploader()
	su.failPuts = 1000
	b, _ := newStreamingBackend(t, su)
	data := testBody(400 << 10) // 2 blobs

	require.Error(t, putSized(t, b, "k", data, int64(len(data))))
	require.Len(t, su.started, 1, "the body stops at the failed blob")
	require.Empty(t, su.concluded)
	require.Equal(t, []cid.Cid{su.started[0].AddTask}, su.aborted, "the failed stream's allocation is aborted")
	requireNoLocalCopies(t, b)
	bucket, key := "bk", "k"
	_, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	require.Error(t, err, "nothing was committed")
}

// TestStreamedPutObjectUnsupportedDigestCode: an upload service that cannot
// add by digest code fails the request, having stored nothing.
func TestStreamedPutObjectUnsupportedDigestCode(t *testing.T) {
	su := newStreamingUploader()
	su.unsupported = true
	b, _ := newStreamingBackend(t, su)
	data := testBody(700 << 10)

	require.Error(t, putSized(t, b, "k", data, int64(len(data))))
	require.Empty(t, su.started)
	requireNoLocalCopies(t, b)
}

// TestStreamedPutObjectUncachedWhenTheCopyFails: a local copy that cannot be
// written costs only the cached copy: the blob still streams and is
// accepted, and reads go to the provider.
func TestStreamedPutObjectUncachedWhenTheCopyFails(t *testing.T) {
	su := newStreamingUploader()
	b, _ := newStreamingBackend(t, su)
	data := testBody(400 << 10) // 2 blobs
	require.NoError(t, os.RemoveAll(b.cache.Path(nil)))

	require.NoError(t, putSized(t, b, "k", data, int64(len(data))))
	require.Len(t, su.concluded, 2, "the blobs were accepted")
	require.Zero(t, b.cache.Usage(), "nothing was cached")
	require.Equal(t, data, getRange(t, b, "k", ""))
}

// TestStreamedPutObjectUncachedBodies: with written bodies not cached, a
// PUT keeps no local copy at all.
func TestStreamedPutObjectUncachedBodies(t *testing.T) {
	su := newStreamingUploader()
	b, _ := newStreamingBackend(t, su, func(d *Deps) { d.DropAcceptedBodies = true })
	data := testBody(400 << 10) // 2 blobs

	require.NoError(t, putSized(t, b, "k", data, int64(len(data))))
	require.Len(t, su.concluded, 2)
	requireNoLocalCopies(t, b)
	require.Equal(t, data, getRange(t, b, "k", ""))
}

// requireNoLocalCopies fails unless the cache holds no files at all, cached
// or in progress, and counts none.
func requireNoLocalCopies(t *testing.T, b *Backend) {
	t.Helper()
	entries, err := os.ReadDir(b.cache.Path(nil))
	require.NoError(t, err)
	require.Empty(t, entries, "the cache holds no files")
	require.Zero(t, b.cache.Usage(), "the cache counts no bytes")
}

// feePlaintextLengthLabel is the FEE protected header that records an
// envelope's plaintext length. go-fee does not export it; the value is fixed by
// the wire format shared with the Synapse SDK.
const feePlaintextLengthLabel = -65789

// TestStreamedPutObjectRecordsPlaintextLength: a sized PUT declares each
// blob's length to the encryptor, so every envelope records its plaintext
// length in the protected header. A reader checks it against the blob size,
// detecting a truncated blob from the header alone.
func TestStreamedPutObjectRecordsPlaintextLength(t *testing.T) {
	su := newStreamingUploader()
	b, _ := newStreamingBackend(t, su)
	data := testBody(700 << 10) // 3 blobs, of 2, 2 and 1 chunks

	require.NoError(t, putSized(t, b, "k", data, int64(len(data))))
	digests := blobDigestsOf(t, b, "k", "")
	require.Len(t, digests, 3)
	for _, d := range digests {
		env, ciphertext, err := cose.Decode(cached(t, b, d))
		require.NoError(t, err)
		declared, ok := env.Headers.Protected.Int(feePlaintextLengthLabel)
		require.True(t, ok, "the envelope records its plaintext length")
		want, err := aesstream.DecryptedSize(int64(len(ciphertext)), aesstream.DefaultChunkSize)
		require.NoError(t, err)
		require.Equal(t, want, declared)
	}
}

func TestStreamedPutObjectWrongLength(t *testing.T) {
	for name, tc := range map[string]struct {
		body     int
		declared int64
		code     string
	}{
		"short": {body: 400 << 10, declared: 500 << 10, code: "IncompleteBody"},
		"long":  {body: 400 << 10, declared: 350 << 10, code: "IncompleteBody"},
	} {
		t.Run(name, func(t *testing.T) {
			su := newStreamingUploader()
			b, _ := newStreamingBackend(t, su)
			err := putSized(t, b, "k", testBody(tc.body), tc.declared)
			var apiErr s3err.APIError
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, tc.code, apiErr.Code)

			last := su.started[len(su.started)-1]
			require.Error(t, su.putErrs[last.AddTask], "the PUT of the failing blob fails too")
			require.Empty(t, su.concluded)
			require.Contains(t, su.aborted, last.AddTask, "the failed stream's allocation is aborted at once")
			require.Greater(t, len(su.started), 1, "the body streamed a blob before the failing one")
			for _, sb := range su.started {
				require.Contains(t, su.aborted, sb.AddTask, "every allocation the failed body streamed is aborted")
			}
		})
	}
}

// TestStreamedPutObjectStalledAbort: a body of the wrong length still gets its
// error when the upload service stalls the abort, which runs past the request
// for at most abandonTimeLimit and is then left to the provider's expiry.
func TestStreamedPutObjectStalledAbort(t *testing.T) {
	defer func(d time.Duration) { abandonTimeLimit = d }(abandonTimeLimit)
	abandonTimeLimit = 50 * time.Millisecond
	su := newStreamingUploader()
	su.abortStalls = true
	b, _ := newStreamingBackend(t, su)

	done := make(chan error, 1)
	go func() { done <- putSized(t, b, "k", testBody(400<<10), 500<<10) }()
	select {
	case err := <-done:
		var apiErr s3err.APIError
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, "IncompleteBody", apiErr.Code)
	case <-time.After(10 * time.Second):
		t.Fatal("the request waited on the stalled abort")
	}
	require.Len(t, su.aborted, 2, "each allocation's abort was tried")
}

func TestStreamedUploadPart(t *testing.T) {
	su := newStreamingUploader()
	b, mem := newStreamingBackend(t, su)
	ctx := context.Background()
	key := "mp"
	part := testBody(int(backend.MinPartSize))

	uploadID := mpCreate(t, b, key, "", "")
	out, err := mpUploadPart(t, b, key, uploadID, 1, part, func(in *s3.UploadPartInput) {
		n := int64(len(part))
		in.ContentLength = &n
	})
	require.NoError(t, err)
	require.NotEmpty(t, su.started)
	require.Empty(t, su.concluded, "a part stays parked until Complete")

	digests := hygienePartDigests(t, mem, uploadID, 1)
	require.Len(t, digests, len(su.started))
	for i, d := range digests {
		park, err := mem.ParkByDigest(ctx, d)
		require.NoError(t, err)
		require.Equal(t, su.started[i].AddTask.Bytes(), park.AddTask)
		require.Equal(t, su.started[i].PutInvocation, park.PutInvocation)
		in, err := mem.IntentByDigest(ctx, d)
		require.NoError(t, err)
		require.Equal(t, registry.IntentParked, in.State)
		_, err = os.Stat(localPath(b, d))
		require.True(t, os.IsNotExist(err), "a part keeps no local copy (stat err=%v)", err)
	}

	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: ptrInt32(1), ETag: out.ETag}}, nil)
	require.NoError(t, err)
	require.Len(t, su.concluded, len(digests))
	for i, d := range digests {
		require.Equal(t, d, su.concluded[i].Digest)
	}
	require.Equal(t, part, getRange(t, b, key, ""))
}

// A conclude that fails after its accept ran still records the acceptance it
// returned, so the accepted blob is accounted for.
func TestStreamedPutObjectConcludeErrorRecordsAcceptance(t *testing.T) {
	su := newStreamingUploader()
	su.concludeErr = errors.New("batch failed after accepting")
	b, mem := newStreamingBackend(t, su)
	ctx := context.Background()
	data := testBody(100 << 10) // one blob

	require.Error(t, putSized(t, b, "k", data, int64(len(data))))
	require.Len(t, su.concluded, 1)
	in, err := mem.IntentByDigest(ctx, su.concluded[0].Digest)
	require.NoError(t, err)
	require.Equal(t, registry.IntentAccepted, in.State, "the returned acceptance is recorded")
}

func ptrInt32(v int32) *int32 { return &v }

// TestStreamedPutObjectRepeatedPlaintextChunk: a body stored as received can
// hold the same bytes twice, so two streamed blobs share a digest. The
// provider needs the bytes once: the first stream is concluded, the second
// is given up at once rather than left for the provider to expire, and the
// object reads back whole.
func TestStreamedPutObjectRepeatedPlaintextChunk(t *testing.T) {
	su := newStreamingUploader()
	b, _ := newStreamingBackend(t, su)
	setNone(t, b)
	chunk := testBody(streamBlobCeiling)
	data := append(append([]byte{}, chunk...), chunk...)

	require.NoError(t, putSized(t, b, "k", data, int64(len(data))))

	digests := blobDigestsOf(t, b, "k", "")
	require.Len(t, digests, 2)
	require.Equal(t, digests[0], digests[1], "the two chunks share a digest")
	require.Len(t, su.started, 2, "each chunk was allocated as it streamed")
	require.Len(t, su.concluded, 1, "the provider was asked to accept the bytes once")
	require.Len(t, su.aborted, 1, "the second allocation was given up")
	require.Equal(t, su.started[1].AddTask, su.aborted[0], "the later allocation is the one given up")
	require.Equal(t, data, getRange(t, b, "k", ""))
}
