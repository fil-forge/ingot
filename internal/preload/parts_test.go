package preload

import (
	"bytes"
	"crypto/sha256"
	"io"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBulkKey(t *testing.T) {
	k := BulkKey("site-a/", "s", "preload-000", 0)
	require.True(t, strings.HasPrefix(k, "site-a/"), "key %q is not the prefix and 64 lowercase hex digits", k)
	require.Len(t, k, len("site-a/")+64, "key %q is not the prefix and 64 lowercase hex digits", k)
	require.Equal(t, k, strings.ToLower(k), "key %q is not the prefix and 64 lowercase hex digits", k)
	require.Equal(t, BulkKey("site-a/", "s", "preload-000", 0), k, "same inputs, different keys")
	seen := map[string]bool{k: true}
	for _, other := range []string{
		BulkKey("site-a/", "s", "preload-000", 1),
		BulkKey("site-a/", "t", "preload-000", 0),
		BulkKey("site-a/", "s", "preload-001", 0),
		// Field boundaries matter: "s"+"preload-000" is not "sp"+"reload-000".
		BulkKey("site-a/", "sp", "reload-000", 0),
	} {
		require.False(t, seen[other], "key %s repeats", other)
		seen[other] = true
	}
	// Keys arrive in no particular order.
	var keys []string
	for n := range uint64(100) {
		keys = append(keys, BulkKey("", "s", "b", n))
	}
	require.False(t, slices.IsSorted(keys), "bulk keys arrive sorted")
}

func TestBucketNames(t *testing.T) {
	got := BucketName("preload-", 7)
	require.Equal(t, "preload-007", got, "BucketName = %q", got)
	for name, ok := range map[string]bool{
		"preload-149": true, "ab": false, "Preload-0": false, "preload_0": false, "-preload": false, "preload-": false,
	} {
		assert.Equal(t, ok, ValidBucketName(name), "ValidBucketName(%q)", name)
	}
}

func TestRealSize(t *testing.T) {
	sizes := map[int64]bool{}
	for j := range uint64(200) {
		s := RealSize("s", "b", j, 128<<20, 240<<20)
		require.GreaterOrEqual(t, s, int64(128<<20), "size %d out of range", s)
		require.LessOrEqual(t, s, int64(240<<20), "size %d out of range", s)
		require.Equal(t, RealSize("s", "b", j, 128<<20, 240<<20), s, "same inputs, different sizes")
		sizes[s] = true
	}
	require.GreaterOrEqual(t, len(sizes), 190, "only %d distinct sizes in 200", len(sizes))
	s := RealSize("s", "b", 0, 5, 5)
	require.Equal(t, int64(5), s, "fixed size %d", s)
}

func TestBody(t *testing.T) {
	var seed [32]byte
	seed[0] = 1
	const size = 3*bodyChunk + 12345
	all, err := io.ReadAll(NewBody(seed, size))
	require.NoError(t, err)
	require.Len(t, all, size, "read %d bytes", len(all))
	again, _ := io.ReadAll(NewBody(seed, size))
	require.Equal(t, again, all, "same seed, different bodies")
	seed[0] = 2
	other, _ := io.ReadAll(NewBody(seed, size))
	require.False(t, bytes.Equal(all[:bodyChunk], other[:bodyChunk]), "different seeds, same body")
	seed[0] = 1
	// Chunks differ from one another.
	require.False(t, bytes.Equal(all[:1024], all[bodyChunk:bodyChunk+1024]), "chunks repeat")

	b := NewBody(seed, size)
	rng := rand.New(rand.NewPCG(1, 2))
	for range 50 {
		off := rng.Int64N(size)
		n := rng.IntN(2*bodyChunk) + 1
		p := make([]byte, n)
		got, err := b.ReadAt(p, off)
		want := min(int64(n), size-off)
		require.Equal(t, want, int64(got), "ReadAt(%d bytes at %d) = %d, %v", n, off, got, err)
		require.Equal(t, int64(n) > size-off, err != nil, "ReadAt(%d bytes at %d) = %d, %v", n, off, got, err)
		require.Equal(t, all[off:off+int64(got)], p[:got], "ReadAt at %d differs from the sequential read", off)
	}

	// ReadAt may run in parallel, as io.ReaderAt allows.
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			off := int64(i) * (size / 8)
			p := make([]byte, 4096)
			_, err := b.ReadAt(p, off)
			assert.NoError(t, err, "parallel ReadAt at %d", off)
			assert.Equal(t, all[off:off+4096], p, "parallel ReadAt at %d differs from the sequential read", off)
		})
	}
	wg.Wait()

	_, err = b.Seek(-10, io.SeekEnd)
	require.NoError(t, err)
	tail, _ := io.ReadAll(b)
	require.Equal(t, all[size-10:], tail, "read after SeekEnd differs")

	sum, err := b.SHA256()
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(all), sum, "SHA256 differs from the bytes' digest")
	pos, _ := b.Seek(0, io.SeekCurrent)
	require.Zero(t, pos, "SHA256 left the offset at %d", pos)
}

func TestHistogram(t *testing.T) {
	var h Histogram
	require.Zero(t, h.Quantile(0.5), "empty histogram reports latencies")
	require.Zero(t, h.Count(), "empty histogram reports latencies")
	// 1ms to 1000ms, one each.
	for i := 1; i <= 1000; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	for _, c := range []struct {
		q    float64
		want time.Duration
	}{{0.5, 500 * time.Millisecond}, {0.99, 990 * time.Millisecond}, {0.001, time.Millisecond}} {
		got := h.Quantile(c.q)
		assert.GreaterOrEqual(t, got, c.want, "Quantile(%v)", c.q)
		assert.LessOrEqual(t, float64(got), float64(c.want)*1.045, "Quantile(%v)", c.q)
	}
	assert.Equal(t, time.Second, h.Quantile(1), "top quantile")
	assert.Equal(t, time.Second, h.Max(), "max")
	m := h.Mean()
	assert.Equal(t, 500500*time.Microsecond, m, "mean %v", m)

	var a, b Histogram
	a.Record(time.Millisecond)
	b.Record(time.Second)
	b.Record(500 * time.Nanosecond)
	a.Merge(&b)
	require.Equal(t, uint64(3), a.Count(), "merged count")
	require.Equal(t, time.Second, a.Max(), "merged max")
	require.Equal(t, time.Microsecond, a.Quantile(0.01), "merged p1")
	var huge Histogram
	huge.Record(10 * time.Hour)
	require.Equal(t, 10*time.Hour, huge.Quantile(0.5), "latency past the last bucket")
}

func TestWatermark(t *testing.T) {
	w := newWatermark(5, 10)
	var got []uint64
	for {
		n, ok := w.take()
		if !ok {
			break
		}
		got = append(got, n)
	}
	require.True(t, slices.Equal(got, []uint64{5, 6, 7, 8, 9}), "took %v", got)
	reached := make(chan error)
	go func() { reached <- w.wait(t.Context(), 8) }()
	for _, n := range []uint64{7, 6, 9} {
		w.complete(n)
	}
	require.Equal(t, uint64(5), w.Done(), "watermark passed the unfinished 5")
	w.complete(5)
	require.Equal(t, uint64(8), w.Done(), "watermark")
	require.NoError(t, <-reached)
	w.complete(8)
	require.Equal(t, uint64(10), w.Done(), "watermark")
}
