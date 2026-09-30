package bucket

import (
	"bytes"
	"crypto/md5"
	"io"
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

// TestAsyncHash_FixedBufferSet pins the allocation bound: a stream allocates
// exactly asyncHashQueue+1 buffers, whatever size came first (the encrypting
// writer's one-byte EOF probe reaches the hashers before any full read) and
// however large the later writes are (a 256 KiB read from the encrypt reader
// is queued as eight chunks). A buffer sized by a small first write would
// have to be replaced later and would show up here as one extra allocation.
func TestAsyncHash_FixedBufferSet(t *testing.T) {
	streamAllocs := func(write func(a *asyncHash)) float64 {
		return testing.AllocsPerRun(5, func() {
			a := newAsyncHash(md5.New())
			write(a)
			a.Sum()
		})
	}
	base := streamAllocs(func(*asyncHash) {})
	chunk := make([]byte, asyncHashChunk)
	big := make([]byte, 8*asyncHashChunk)
	got := streamAllocs(func(a *asyncHash) {
		a.Write([]byte{1})
		for i := 0; i < 2*(asyncHashQueue+1); i++ {
			a.Write(chunk)
		}
		a.Write(big)
	})
	if buffers := got - base; buffers != asyncHashQueue+1 {
		t.Fatalf("stream allocated %.1f buffers; want exactly %d", buffers, asyncHashQueue+1)
	}
}
