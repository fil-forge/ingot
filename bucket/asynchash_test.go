package bucket

import (
	"bytes"
	"crypto/md5"
	"hash"
	"io"
	"sync"
	"testing"
)

// TestAsyncHash_MatchesInline pins that an asyncHash produces exactly the
// inline digest whatever the write pattern: empty input, one large write,
// many small writes, and writes larger than the queue can hold at once.
func TestAsyncHash_MatchesInline(t *testing.T) {
	data := makeData(3<<20 + 12345)

	cases := map[string]struct {
		input []byte
		write func(w io.Writer, data []byte)
	}{
		"empty":       {nil, func(io.Writer, []byte) {}},
		"one write":   {data, func(w io.Writer, d []byte) { w.Write(d) }},
		"64K writes":  {data, func(w io.Writer, d []byte) { io.Copy(w, bytes.NewReader(d)) }},
		"tiny writes": {data, func(w io.Writer, d []byte) { io.CopyBuffer(w, bytes.NewReader(d), make([]byte, 7)) }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			want := md5.Sum(tc.input)
			a := newAsyncHash(md5.New())
			tc.write(a, tc.input)
			got := a.Sum()
			if !bytes.Equal(got, want[:]) {
				t.Fatalf("digest mismatch: got %x, want %x", got, want)
			}
			// Sum is idempotent, so deferred cleanup after an explicit Sum is safe.
			if again := a.Sum(); !bytes.Equal(again, got) {
				t.Fatalf("second Sum differs: %x vs %x", again, got)
			}
		})
	}
}

// TestAsyncHash_CallerReusesBuffer pins the copy-on-Write contract: the
// caller may overwrite its buffer as soon as Write returns.
func TestAsyncHash_CallerReusesBuffer(t *testing.T) {
	a := newAsyncHash(md5.New())
	h := md5.New()
	buf := make([]byte, 64<<10)
	for i := 0; i < 256; i++ {
		for j := range buf {
			buf[j] = byte(i + j)
		}
		h.Write(buf)
		a.Write(buf)
	}
	if got, want := a.Sum(), h.Sum(nil); !bytes.Equal(got, want) {
		t.Fatalf("digest mismatch: got %x, want %x", got, want)
	}
}

// gatedHash reports on taken when the asyncHash goroutine first hands it a
// buffer, then holds every Write until open is closed, so nothing can return
// to the free list before the test allows it.
type gatedHash struct {
	hash.Hash
	taken chan struct{} // closed on the first Write, before it blocks
	open  chan struct{}
	once  sync.Once
}

func (g *gatedHash) Write(p []byte) (int, error) {
	g.once.Do(func() { close(g.taken) })
	<-g.open
	return g.Hash.Write(p)
}

// TestAsyncHash_FixedBufferSet pins the allocation bound: a stream allocates
// exactly asyncHashQueue+1 buffers, whatever size came first (the encrypting
// writer's one-byte EOF probe reaches the hashers before any full read) and
// however large the later writes are (a 256 KiB read from the encrypt reader
// is queued as eight chunks). A buffer sized by a small first write would
// have to be replaced later and would show up here as one extra allocation.
//
// The hash is gated while the first asyncHashQueue+1 writes are queued, so
// the producer cannot find a returned buffer on the free list and must
// allocate its whole set; the gate then opens and every later write reuses
// one. That makes the count exact rather than a bound that scheduling could
// undershoot. The producer waits for the worker to take the first buffer
// before queuing the rest: the worker then holds one and the queue the other
// asyncHashQueue, so the set fills without a send ever blocking.
func TestAsyncHash_FixedBufferSet(t *testing.T) {
	one := make([]byte, 1)
	chunk := make([]byte, asyncHashChunk)
	big := make([]byte, 8*asyncHashChunk)
	streamAllocs := func(write func(a *asyncHash, g *gatedHash)) float64 {
		return testing.AllocsPerRun(5, func() {
			g := &gatedHash{Hash: md5.New(), taken: make(chan struct{}), open: make(chan struct{})}
			a := newAsyncHash(g)
			write(a, g)
			a.Sum()
		})
	}
	base := streamAllocs(func(_ *asyncHash, g *gatedHash) { close(g.open) })
	got := streamAllocs(func(a *asyncHash, g *gatedHash) {
		a.Write(one)
		<-g.taken
		for i := 1; i < asyncHashQueue+1; i++ {
			a.Write(chunk)
		}
		close(g.open)
		for i := 0; i < 2*(asyncHashQueue+1); i++ {
			a.Write(chunk)
		}
		a.Write(big)
	})
	if buffers := got - base; buffers != asyncHashQueue+1 {
		t.Fatalf("stream allocated %.1f buffers; want exactly %d", buffers, asyncHashQueue+1)
	}
}
