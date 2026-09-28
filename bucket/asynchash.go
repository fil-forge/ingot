package bucket

import (
	"hash"
	"io"
	"sync"
)

// asyncHashQueue bounds how far the producer may run ahead of the hashing
// goroutine, in chunks. A chunk is whatever one Write delivers (the
// encryptor pulls the body through io.Copy, so 32 KiB), and a stream holds
// at most asyncHashQueue+1 chunk buffers.
const asyncHashQueue = 8

// asyncHash feeds a hash.Hash from its own goroutine so the producer does not
// pay for the hash inline. It exists for MD5, which has no hardware
// acceleration and is by far the slowest pass over a body: with it inline the
// ingest goroutine pays every pass in sequence, and a single stream runs at
// roughly half the MD5 rate. Moving it aside makes the stream pay only the
// slowest pass, which is MD5 itself.
//
// Write copies p into a buffer from a fixed free list and queues it: the
// caller may reuse p immediately, and a stream allocates asyncHashQueue+1
// buffers in total rather than one per chunk. Sum closes the queue, waits for
// the goroutine to drain it and returns the digest; it is idempotent, so it
// doubles as the cleanup a caller defers on error paths. A hash that is also
// an io.Closer (an md5-simd lane) is closed once summed. Write after Sum is a
// programming error and panics.
type asyncHash struct {
	full  chan []byte // chunks waiting to be hashed
	free  chan []byte // hashed buffers ready for reuse
	spare int         // buffers Write may still allocate before it must wait on free
	once  sync.Once
	done  chan struct{}
	sum   []byte
}

func newAsyncHash(h hash.Hash) *asyncHash {
	a := &asyncHash{
		full:  make(chan []byte, asyncHashQueue),
		free:  make(chan []byte, asyncHashQueue+1),
		spare: asyncHashQueue + 1,
		done:  make(chan struct{}),
	}
	go func() {
		defer close(a.done)
		for buf := range a.full {
			h.Write(buf)
			a.free <- buf[:0]
		}
		a.sum = h.Sum(nil)
		if c, ok := h.(io.Closer); ok {
			c.Close()
		}
	}()
	return a
}

// Write implements io.Writer. It never fails: a hash.Hash's Write cannot.
func (a *asyncHash) Write(p []byte) (int, error) {
	var buf []byte
	select {
	case buf = <-a.free:
	default:
		if a.spare == 0 {
			buf = <-a.free
		} else {
			a.spare--
		}
	}
	a.full <- append(buf, p...)
	return len(p), nil
}

// Sum finishes the stream and returns the digest of everything written.
func (a *asyncHash) Sum() []byte {
	a.once.Do(func() { close(a.full) })
	<-a.done
	return a.sum
}
