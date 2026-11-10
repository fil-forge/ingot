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
)

func TestBulkKey(t *testing.T) {
	k := BulkKey("site-a/", "s", "preload-000", 0)
	if !strings.HasPrefix(k, "site-a/") || len(k) != len("site-a/")+64 || strings.ToLower(k) != k {
		t.Fatalf("key %q is not the prefix and 64 lowercase hex digits", k)
	}
	if k != BulkKey("site-a/", "s", "preload-000", 0) {
		t.Fatal("same inputs, different keys")
	}
	seen := map[string]bool{k: true}
	for _, other := range []string{
		BulkKey("site-a/", "s", "preload-000", 1),
		BulkKey("site-a/", "t", "preload-000", 0),
		BulkKey("site-a/", "s", "preload-001", 0),
		// Field boundaries matter: "s"+"preload-000" is not "sp"+"reload-000".
		BulkKey("site-a/", "sp", "reload-000", 0),
	} {
		if seen[other] {
			t.Fatalf("key %s repeats", other)
		}
		seen[other] = true
	}
	// Keys arrive in no particular order.
	var keys []string
	for n := range uint64(100) {
		keys = append(keys, BulkKey("", "s", "b", n))
	}
	if slices.IsSorted(keys) {
		t.Fatal("bulk keys arrive sorted")
	}
}

func TestBucketNames(t *testing.T) {
	if got := BucketName("preload-", 7); got != "preload-007" {
		t.Fatalf("BucketName = %q", got)
	}
	for name, ok := range map[string]bool{
		"preload-149": true, "ab": false, "Preload-0": false, "preload_0": false, "-preload": false, "preload-": false,
	} {
		if ValidBucketName(name) != ok {
			t.Errorf("ValidBucketName(%q) = %v", name, !ok)
		}
	}
}

func TestRealSize(t *testing.T) {
	sizes := map[int64]bool{}
	for j := range uint64(200) {
		s := RealSize("s", "b", j, 128<<20, 240<<20)
		if s < 128<<20 || s > 240<<20 {
			t.Fatalf("size %d out of range", s)
		}
		if s != RealSize("s", "b", j, 128<<20, 240<<20) {
			t.Fatal("same inputs, different sizes")
		}
		sizes[s] = true
	}
	if len(sizes) < 190 {
		t.Fatalf("only %d distinct sizes in 200", len(sizes))
	}
	if s := RealSize("s", "b", 0, 5, 5); s != 5 {
		t.Fatalf("fixed size %d", s)
	}
}

func TestBody(t *testing.T) {
	var seed [32]byte
	seed[0] = 1
	const size = 3*bodyChunk + 12345
	all, err := io.ReadAll(NewBody(seed, size))
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != size {
		t.Fatalf("read %d bytes, want %d", len(all), size)
	}
	again, _ := io.ReadAll(NewBody(seed, size))
	if !bytes.Equal(all, again) {
		t.Fatal("same seed, different bodies")
	}
	seed[0] = 2
	other, _ := io.ReadAll(NewBody(seed, size))
	if bytes.Equal(all[:bodyChunk], other[:bodyChunk]) {
		t.Fatal("different seeds, same body")
	}
	seed[0] = 1
	// Chunks differ from one another.
	if bytes.Equal(all[:1024], all[bodyChunk:bodyChunk+1024]) {
		t.Fatal("chunks repeat")
	}

	b := NewBody(seed, size)
	rng := rand.New(rand.NewPCG(1, 2))
	for range 50 {
		off := rng.Int64N(size)
		n := rng.IntN(2*bodyChunk) + 1
		p := make([]byte, n)
		got, err := b.ReadAt(p, off)
		want := min(int64(n), size-off)
		if int64(got) != want || (err != nil) != (int64(n) > size-off) {
			t.Fatalf("ReadAt(%d bytes at %d) = %d, %v", n, off, got, err)
		}
		if !bytes.Equal(p[:got], all[off:off+int64(got)]) {
			t.Fatalf("ReadAt at %d differs from the sequential read", off)
		}
	}

	// ReadAt may run in parallel, as io.ReaderAt allows.
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			off := int64(i) * (size / 8)
			p := make([]byte, 4096)
			if _, err := b.ReadAt(p, off); err != nil {
				t.Errorf("parallel ReadAt at %d: %v", off, err)
			}
			if !bytes.Equal(p, all[off:off+4096]) {
				t.Errorf("parallel ReadAt at %d differs from the sequential read", off)
			}
		})
	}
	wg.Wait()

	if _, err := b.Seek(-10, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	tail, _ := io.ReadAll(b)
	if !bytes.Equal(tail, all[size-10:]) {
		t.Fatal("read after SeekEnd differs")
	}

	sum, err := b.SHA256()
	if err != nil {
		t.Fatal(err)
	}
	if sum != sha256.Sum256(all) {
		t.Fatal("SHA256 differs from the bytes' digest")
	}
	if pos, _ := b.Seek(0, io.SeekCurrent); pos != 0 {
		t.Fatalf("SHA256 left the offset at %d", pos)
	}
}

func TestHistogram(t *testing.T) {
	var h Histogram
	if h.Quantile(0.5) != 0 || h.Count() != 0 {
		t.Fatal("empty histogram reports latencies")
	}
	// 1ms to 1000ms, one each.
	for i := 1; i <= 1000; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	for _, c := range []struct {
		q    float64
		want time.Duration
	}{{0.5, 500 * time.Millisecond}, {0.99, 990 * time.Millisecond}, {0.001, time.Millisecond}} {
		got := h.Quantile(c.q)
		if got < c.want || float64(got) > float64(c.want)*1.045 {
			t.Errorf("Quantile(%v) = %v, want %v to 4.5%% above", c.q, got, c.want)
		}
	}
	if h.Quantile(1) != time.Second || h.Max() != time.Second {
		t.Errorf("top quantile %v, max %v", h.Quantile(1), h.Max())
	}
	if m := h.Mean(); m != 500500*time.Microsecond {
		t.Errorf("mean %v", m)
	}

	var a, b Histogram
	a.Record(time.Millisecond)
	b.Record(time.Second)
	b.Record(500 * time.Nanosecond)
	a.Merge(&b)
	if a.Count() != 3 || a.Max() != time.Second || a.Quantile(0.01) != time.Microsecond {
		t.Fatalf("merged: count %d max %v p1 %v", a.Count(), a.Max(), a.Quantile(0.01))
	}
	var huge Histogram
	huge.Record(10 * time.Hour)
	if huge.Quantile(0.5) != 10*time.Hour {
		t.Fatalf("latency past the last bucket reported as %v", huge.Quantile(0.5))
	}
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
	if !slices.Equal(got, []uint64{5, 6, 7, 8, 9}) {
		t.Fatalf("took %v", got)
	}
	reached := make(chan error)
	go func() { reached <- w.wait(t.Context(), 8) }()
	for _, n := range []uint64{7, 6, 9} {
		w.complete(n)
	}
	if w.Done() != 5 {
		t.Fatalf("watermark %d passed the unfinished 5", w.Done())
	}
	w.complete(5)
	if w.Done() != 8 {
		t.Fatalf("watermark %d, want 8", w.Done())
	}
	if err := <-reached; err != nil {
		t.Fatal(err)
	}
	w.complete(8)
	if w.Done() != 10 {
		t.Fatalf("watermark %d, want 10", w.Done())
	}
}
