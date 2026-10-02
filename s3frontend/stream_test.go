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
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// streamingUploader is a provider that takes blobs allocated by digest code.
// It records every allocation, the bytes each PUT received, and what was
// concluded, uploaded by digest and aborted.
type streamingUploader struct {
	inmem.NopUploader

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
	uploaded  []multihash.Multihash
	aborted   []cid.Cid
	// abortErr, when set, is what AbortBlob answers.
	abortErr error
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
		return uploader.StreamedBlob{}, uploader.ErrUnsupportedDigestCode
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

func (s *streamingUploader) UploadBlob(ctx context.Context, space did.DID, digest multihash.Multihash, size int64, path string, opts ...uploader.UploadOption) (uploader.UploadedBlob, error) {
	s.mu.Lock()
	s.uploaded = append(s.uploaded, digest)
	s.mu.Unlock()
	return s.NopUploader.UploadBlob(ctx, space, digest, size, path, opts...)
}

func (s *streamingUploader) AbortBlob(_ context.Context, _ did.DID, cause cid.Cid) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aborted = append(s.aborted, cause)
	return s.abortErr
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

func newStreamingBackend(t *testing.T, su *streamingUploader) (*Backend, *inmem.MemStore) {
	t.Helper()
	return newDeferredBackend(t, su, func(d *Deps) {
		d.Streaming = su
		d.Streams = d.Parks.(*inmem.MemStore)
		d.MaxBlobSize = streamBlobCeiling
	})
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

func spooled(t *testing.T, b *Backend, digest multihash.Multihash) []byte {
	t.Helper()
	data, err := os.ReadFile(b.spool.Path(digest))
	require.NoError(t, err)
	return data
}

func staleStreams(t *testing.T, mem *inmem.MemStore) []registry.BlobStream {
	t.Helper()
	rows, err := mem.ListStaleStreams(context.Background(), time.Now().Add(time.Hour), 1000)
	require.NoError(t, err)
	return rows
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
	require.Empty(t, su.uploaded, "nothing is uploaded by digest")
	require.Len(t, su.concluded, 3)
	for i, d := range digests {
		sb := su.started[i]
		envelope := spooled(t, b, d)
		require.Equal(t, envelope, su.puts[sb.AddTask], "the provider got the spooled envelope")
		require.Equal(t, int64(len(envelope)), sb.Size, "the allocation was the envelope's length")

		require.Equal(t, d, su.concluded[i].Digest, "the conclude reports the spooled digest")
		require.Equal(t, sb.AddTask, su.concluded[i].AddTask)

		in, err := mem.GetIntent(ctx, d)
		require.NoError(t, err)
		require.Equal(t, registry.IntentPublished, in.State, "the committed blob's intent is published")
	}
	require.Empty(t, staleStreams(t, mem), "no upload is left for the sweeper")
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
	require.Empty(t, su.uploaded)
	require.Empty(t, getRange(t, b, "k", ""))
}

func TestStreamedPutObjectFailedPut(t *testing.T) {
	su := newStreamingUploader()
	su.failPuts = 1000
	b, mem := newStreamingBackend(t, su)
	data := testBody(400 << 10) // 2 blobs

	require.NoError(t, putSized(t, b, "k", data, int64(len(data))))

	digests := blobDigestsOf(t, b, "k", "")
	require.Len(t, su.started, 2)
	require.Equal(t, digests, su.uploaded, "each blob falls back to uploading its spooled copy")
	require.Empty(t, su.concluded)
	require.Equal(t, []cid.Cid{su.started[0].AddTask, su.started[1].AddTask}, su.aborted,
		"the allocations that took the failed PUTs are released")
	require.Empty(t, staleStreams(t, mem))
	require.Equal(t, data, getRange(t, b, "k", ""))
}

func TestStreamedPutObjectUnsupportedDigestCode(t *testing.T) {
	su := newStreamingUploader()
	su.unsupported = true
	b, _ := newStreamingBackend(t, su)
	data := testBody(700 << 10)

	require.NoError(t, putSized(t, b, "k", data, int64(len(data))))
	require.Empty(t, su.started)
	require.Len(t, su.uploaded, 3, "every blob uploads by digest")
	require.Equal(t, data, getRange(t, b, "k", ""))
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
			b, mem := newStreamingBackend(t, su)
			err := putSized(t, b, "k", testBody(tc.body), tc.declared)
			var apiErr s3err.APIError
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, tc.code, apiErr.Code)

			last := su.started[len(su.started)-1]
			require.Error(t, su.putErrs[last.AddTask], "the PUT of the failing blob fails too")
			require.Empty(t, su.concluded)
			require.NotEmpty(t, staleStreams(t, mem), "the sweeper aborts what the request left")
		})
	}
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
		park, err := mem.GetPark(ctx, d)
		require.NoError(t, err)
		require.Equal(t, su.started[i].AddTask.Bytes(), park.AddTask)
		require.Equal(t, su.started[i].PutInvocation, park.PutInvocation)
		in, err := mem.GetIntent(ctx, d)
		require.NoError(t, err)
		require.Equal(t, registry.IntentParked, in.State)
	}
	require.Empty(t, staleStreams(t, mem))

	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: ptrInt32(1), ETag: out.ETag}}, nil)
	require.NoError(t, err)
	require.Len(t, su.concluded, len(digests))
	for i, d := range digests {
		require.Equal(t, d, su.concluded[i].Digest)
	}
	require.Empty(t, su.uploaded)
	require.Equal(t, part, getRange(t, b, key, ""))
}

func TestSweepStaleStreams(t *testing.T) {
	su := newStreamingUploader()
	b, mem := newStreamingBackend(t, su)
	ctx := context.Background()
	space := did.Undef
	row := func(n byte, age time.Duration) registry.BlobStream {
		return registry.BlobStream{
			AddTask:   cid.NewCidV1(cid.Raw, mustSum([]byte{'s', n})).Bytes(),
			Space:     space,
			Bucket:    "bk",
			CreatedAt: time.Now().Add(-age),
		}
	}
	fresh, stale := row(1, time.Minute), row(2, time.Hour)
	for _, r := range []registry.BlobStream{fresh, stale} {
		require.NoError(t, mem.PutStream(ctx, r))
	}

	n, err := b.SweepStaleStreams(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Len(t, su.aborted, 1)
	require.Equal(t, stale.AddTask, su.aborted[0].Bytes(), "only the stale upload is aborted, by its add task")
	require.Len(t, staleStreams(t, mem), 1, "the live upload's row stays")

	t.Run("an accepted upload's row is dropped", func(t *testing.T) {
		su.abortErr = uploader.ErrBlobAccepted
		require.NoError(t, mem.PutStream(ctx, row(3, time.Hour)))
		n, err := b.SweepStaleStreams(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, n)
	})

	t.Run("a failed abort is retried", func(t *testing.T) {
		su.abortErr = errors.New("upload service unreachable")
		require.NoError(t, mem.PutStream(ctx, row(4, time.Hour)))
		n, err := b.SweepStaleStreams(ctx)
		require.NoError(t, err)
		require.Zero(t, n)
		require.Len(t, staleStreams(t, mem), 2)
	})

	t.Run("a row past the provider's expiry is dropped", func(t *testing.T) {
		require.NoError(t, mem.PutStream(ctx, row(5, 25*time.Hour)))
		n, err := b.SweepStaleStreams(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, n)
	})
}

// A stale row whose blob was parked belongs to a multipart session that still
// needs the blob: its row is dropped, and the blob is not aborted.
func TestSweepStaleStreamsKeepsParkedBlob(t *testing.T) {
	su := newStreamingUploader()
	b, mem := newStreamingBackend(t, su)
	ctx := context.Background()
	addTask := cid.NewCidV1(cid.Raw, mustSum([]byte("parked"))).Bytes()
	require.NoError(t, mem.PutStream(ctx, registry.BlobStream{
		AddTask: addTask, Bucket: "bk", CreatedAt: time.Now().Add(-time.Hour),
	}))
	require.NoError(t, mem.PutPark(ctx, registry.BlobPark{
		Digest: mustSum([]byte("parked blob")), AddTask: addTask,
		AcceptTask: addTask, PutInvocation: []byte("put"), Size: 1,
	}))

	n, err := b.SweepStaleStreams(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the row is dropped")
	require.Empty(t, su.aborted, "the parked blob is not aborted")
}

// A request renews the lease on its rows while it runs, so an old row of a
// live request is not stale; once the lease ends, the row goes stale.
func TestStreamLease(t *testing.T) {
	prev := streamLeaseInterval
	streamLeaseInterval = 10 * time.Millisecond
	t.Cleanup(func() { streamLeaseInterval = prev })
	su := newStreamingUploader()
	b, mem := newStreamingBackend(t, su)
	ctx := context.Background()
	addTask := cid.NewCidV1(cid.Raw, mustSum([]byte("leased"))).Bytes()
	require.NoError(t, mem.PutStream(ctx, registry.BlobStream{
		AddTask: addTask, Bucket: "bk", CreatedAt: time.Now().Add(-time.Hour),
	}))

	lease := newStreamLease(mem, zap.NewNop())
	lease.add(addTask)
	require.Eventually(t, func() bool {
		rows, err := mem.ListStaleStreams(ctx, time.Now().Add(-streamStaleAge), 10)
		require.NoError(t, err)
		return len(rows) == 0
	}, time.Second, 5*time.Millisecond, "the lease renews the row")
	n, err := b.SweepStaleStreams(ctx)
	require.NoError(t, err)
	require.Zero(t, n, "a live request's row is not swept")

	lease.end()
	lease.end()
	require.Len(t, staleStreams(t, mem), 1, "the row stays for the sweeper once the lease ends")
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
	in, err := mem.GetIntent(ctx, su.concluded[0].Digest)
	require.NoError(t, err)
	require.Equal(t, registry.IntentAccepted, in.State, "the returned acceptance is recorded")
	require.Empty(t, staleStreams(t, mem), "the accepted blob's row is dropped")
}

func ptrInt32(v int32) *int32 { return &v }
