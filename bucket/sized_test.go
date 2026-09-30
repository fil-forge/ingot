package bucket

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// sizedRecorder is a SizedBlobWriter that reads each blob to its end and
// records the length it was declared with and the bytes it got.
type sizedRecorder struct {
	declared []int64
	blobs    [][]byte
}

func (s *sizedRecorder) WriteSizedBlob(_ context.Context, r io.Reader, n int64) (mh.Multihash, error) {
	s.declared = append(s.declared, n)
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	s.blobs = append(s.blobs, b)
	sum := sha256.Sum256(b)
	return mh.Encode(sum[:], mh.SHA2_256)
}

// failAtEnd yields its data, then fails where it would report EOF, as a
// checksum-validating reader does.
type failAtEnd struct {
	r   io.Reader
	err error
}

func (f *failAtEnd) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if errors.Is(err, io.EOF) {
		return n, f.err
	}
	return n, err
}

func TestSplitSizedBody(t *testing.T) {
	ctx := t.Context()
	const max = 1000

	t.Run("splits at the blob ceiling and matches SplitBody", func(t *testing.T) {
		data := makeData(2500)
		w := &sizedRecorder{}
		body, err := SplitSizedBody(ctx, w, bytes.NewReader(data), int64(len(data)), max)
		require.NoError(t, err)
		require.Equal(t, []int64{1000, 1000, 500}, w.declared)
		require.Equal(t, data, bytes.Join(w.blobs, nil))

		want, err := SplitBody(ctx, hashingDiscardWriter{}, bytes.NewReader(data), max)
		require.NoError(t, err)
		require.Equal(t, want, body)
	})

	t.Run("skips the MD5 as SplitBody does", func(t *testing.T) {
		data := makeData(2500)
		body, err := SplitSizedBody(ctx, &sizedRecorder{}, bytes.NewReader(data), int64(len(data)), max, WithoutMD5())
		require.NoError(t, err)
		require.Nil(t, body.MD5)

		want, err := SplitBody(ctx, hashingDiscardWriter{}, bytes.NewReader(data), max, WithoutMD5())
		require.NoError(t, err)
		require.Equal(t, want, body)
	})

	t.Run("an empty body has the empty MD5", func(t *testing.T) {
		body, err := SplitSizedBody(ctx, &sizedRecorder{}, bytes.NewReader(nil), 0, max)
		require.NoError(t, err)
		empty := md5.Sum(nil)
		require.Equal(t, empty[:], body.MD5)
	})

	t.Run("an exact multiple of the ceiling has no empty blob", func(t *testing.T) {
		data := makeData(2000)
		w := &sizedRecorder{}
		body, err := SplitSizedBody(ctx, w, bytes.NewReader(data), 2000, max)
		require.NoError(t, err)
		require.Equal(t, []int64{1000, 1000}, w.declared)
		require.Len(t, body.Blobs, 2)
	})

	t.Run("an empty body has no blobs", func(t *testing.T) {
		w := &sizedRecorder{}
		body, err := SplitSizedBody(ctx, w, bytes.NewReader(nil), 0, max)
		require.NoError(t, err)
		require.Empty(t, body.Blobs)
		require.Empty(t, w.declared)
	})

	t.Run("a short body fails inside the blob that runs out", func(t *testing.T) {
		w := &sizedRecorder{}
		_, err := SplitSizedBody(ctx, w, bytes.NewReader(makeData(1500)), 2500, max)
		require.ErrorIs(t, err, ErrBodyShort)
		require.Equal(t, []int64{1000, 1000}, w.declared)
	})

	t.Run("a long body fails inside the last blob", func(t *testing.T) {
		w := &sizedRecorder{}
		_, err := SplitSizedBody(ctx, w, bytes.NewReader(makeData(1501)), 1500, max)
		require.ErrorIs(t, err, ErrBodyLong)
		require.Equal(t, []int64{1000, 500}, w.declared)
		require.Len(t, w.blobs, 1, "the last blob's writer sees the failure")
	})

	t.Run("a non-empty body declared empty fails", func(t *testing.T) {
		_, err := SplitSizedBody(ctx, &sizedRecorder{}, bytes.NewReader([]byte{1}), 0, max)
		require.ErrorIs(t, err, ErrBodyLong)
	})

	t.Run("an error at the body's end reaches the last blob's writer", func(t *testing.T) {
		bad := errors.New("checksum mismatch")
		w := &sizedRecorder{}
		_, err := SplitSizedBody(ctx, w, &failAtEnd{r: bytes.NewReader(makeData(1500)), err: bad}, 1500, max)
		require.ErrorIs(t, err, bad)
		require.Len(t, w.blobs, 1, "the last blob's writer sees the failure")
	})

	t.Run("a writer that stops early fails", func(t *testing.T) {
		_, err := SplitSizedBody(ctx, stopEarly{}, bytes.NewReader(makeData(500)), 500, max)
		require.Error(t, err)
	})
}

type stopEarly struct{}

func (stopEarly) WriteSizedBlob(_ context.Context, r io.Reader, _ int64) (mh.Multihash, error) {
	var b [10]byte
	_, err := io.ReadFull(r, b[:])
	return nil, err
}
