package preload

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
)

// bodyChunk is the span of a Body generated from one ChaCha8 stream.
const bodyChunk = 1 << 20

// Body is a pseudo-random object body of a fixed size, the same on every run
// for the same seed. Chunk i of it is the ChaCha8 stream seeded with the
// SHA-256 of the seed and i, so any range can be produced without the bytes
// before it, and a pack-sized body can be hashed, uploaded and later checked
// against a ranged read without being held in memory. Its bytes do not
// compress, as a pack's encrypted bytes do not.
//
// A Body is an io.ReadSeeker and an io.ReaderAt. It is not safe for
// concurrent use.
type Body struct {
	seed  [32]byte
	size  int64
	off   int64
	chunk int64 // the chunk buf holds, or -1
	buf   []byte
}

// NewBody returns the body of size bytes generated from seed.
func NewBody(seed [32]byte, size int64) *Body {
	return &Body{seed: seed, size: size, chunk: -1}
}

// Size returns the body's length in bytes.
func (b *Body) Size() int64 { return b.size }

// Read reads the body from its current offset.
func (b *Body) Read(p []byte) (int, error) {
	n, err := b.ReadAt(p, b.off)
	b.off += int64(n)
	return n, err
}

// ReadAt reads len(p) bytes of the body starting at off.
func (b *Body) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("preload: negative offset")
	}
	n := 0
	for n < len(p) && off < b.size {
		c := off / bodyChunk
		b.fill(c)
		start := off - c*bodyChunk
		k := copy(p[n:], b.buf[start:])
		n += k
		off += int64(k)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Seek sets the offset of the next Read.
func (b *Body) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = b.off + offset
	case io.SeekEnd:
		abs = b.size + offset
	default:
		return 0, errors.New("preload: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("preload: negative position")
	}
	b.off = abs
	return abs, nil
}

// SHA256 returns the digest of the whole body and leaves the offset at the
// start.
func (b *Body) SHA256() ([sha256.Size]byte, error) {
	var sum [sha256.Size]byte
	if _, err := b.Seek(0, io.SeekStart); err != nil {
		return sum, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, b); err != nil {
		return sum, err
	}
	h.Sum(sum[:0])
	_, err := b.Seek(0, io.SeekStart)
	return sum, err
}

// fill makes buf hold chunk c, truncated to the body's end.
func (b *Body) fill(c int64) {
	if b.chunk == c {
		return
	}
	n := min(int64(bodyChunk), b.size-c*bodyChunk)
	if cap(b.buf) < bodyChunk {
		b.buf = make([]byte, bodyChunk)
	}
	b.buf = b.buf[:n]
	h := sha256.New()
	h.Write(b.seed[:])
	var idx [8]byte
	binary.BigEndian.PutUint64(idx[:], uint64(c))
	h.Write(idx[:])
	var s [32]byte
	h.Sum(s[:0])
	// ChaCha8.Read never fails.
	_, _ = rand.NewChaCha8(s).Read(b.buf)
	b.chunk = c
}
