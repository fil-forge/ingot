package aesstream_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/fil-forge/ingot/fee/aesstream"
)

// trackingReaderAt serves bytes via ReadAt while recording access and,
// when a window is set, failing any read that falls outside it. A
// RangeReader that only fetches the chunks overlapping its range will
// never read outside [allowLo, allowHi).
type trackingReaderAt struct {
	t       *testing.T
	data    []byte
	allowLo int64 // start of the permitted byte window
	allowHi int64 // end of the permitted byte window; 0 disables the check

	reads     int   // number of ReadAt calls
	bytesRead int64 // total bytes requested
}

func (r *trackingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if r.allowHi > 0 && (off < r.allowLo || off+int64(len(p)) > r.allowHi) {
		// An out-of-window read means the range math fetched a chunk it
		// shouldn't have. Surface it and fail the read so the caller errors
		// rather than silently succeeding on data it wasn't allowed to see.
		r.t.Errorf("ReadAt [%d,%d) outside permitted window [%d,%d)",
			off, off+int64(len(p)), r.allowLo, r.allowHi)
		return 0, fmt.Errorf("out-of-window read at %d", off)
	}
	r.reads++
	r.bytesRead += int64(len(p))
	return bytes.NewReader(r.data).ReadAt(p, off)
}

// chunkWindow returns the byte window of the ciphertext chunks that overlap
// plaintext range [off, off+effLen) for the given chunk and stream size.
func chunkWindow(off, effLen, ciphertextSize int64, chunkSize int) (lo, hi int64) {
	if effLen == 0 {
		return 0, 0
	}
	enc := int64(chunkSize) + aesstream.TagSize
	firstChunk := off / int64(chunkSize)
	lastChunk := (off + effLen - 1) / int64(chunkSize)
	lo = firstChunk * enc
	hi = (lastChunk + 1) * enc
	if hi > ciphertextSize {
		hi = ciphertextSize
	}
	return lo, hi
}

// TestRange_Exhaustive is the core acceptance check: across a spread of
// plaintext sizes, every (offset, length) request returns exactly the
// corresponding plaintext bytes — including ranges that straddle chunk
// boundaries and lengths that run past the end (which clamp). It exercises
// both the one-shot OpenRange and the streaming RangeReader.
func TestRange_Exhaustive(t *testing.T) {
	const cs = aesstream.MinChunkSize // 4 KiB → easy multi-chunk streams
	cfg := baseConfig(cs)

	sizes := []int{0, 1, 100, cs - 1, cs, cs + 1, 2 * cs, 2*cs + 1, 3*cs + 123, 5 * cs}
	for _, n := range sizes {
		pt := pattern(n)
		ct := mustSeal(t, cfg, pt)
		size := int64(len(ct))

		// Offsets and lengths chosen around chunk boundaries.
		offs := []int64{0, 1, cs - 1, cs, cs + 1, 2*cs - 3, int64(n) - 1, int64(n)}
		lens := []int64{0, 1, 5, cs - 1, cs, cs + 1, 2 * cs, int64(n), int64(n) + 10}

		for _, off := range offs {
			if off < 0 || off > int64(n) {
				continue // out-of-range offsets are covered separately
			}
			for _, length := range lens {
				if length < 0 {
					continue
				}
				effLen := length
				if avail := int64(n) - off; effLen > avail {
					effLen = avail
				}
				want := pt[off : off+effLen]

				got, err := aesstream.OpenRange(cfg, bytes.NewReader(ct), size, off, length)
				if err != nil {
					t.Fatalf("n=%d off=%d len=%d: OpenRange: %v", n, off, length, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("n=%d off=%d len=%d: OpenRange mismatch\n got %d bytes\nwant %d bytes",
						n, off, length, len(got), len(want))
				}

				// The streaming reader, drained in tiny reads, must agree.
				rr, err := aesstream.NewRangeReader(bytes.NewReader(ct), size, cfg, off, length)
				if err != nil {
					t.Fatalf("n=%d off=%d len=%d: NewRangeReader: %v", n, off, length, err)
				}
				if rr.Len() != effLen {
					t.Fatalf("n=%d off=%d len=%d: Len()=%d, want %d", n, off, length, rr.Len(), effLen)
				}
				streamed := drainTiny(t, rr)
				if !bytes.Equal(streamed, want) {
					t.Fatalf("n=%d off=%d len=%d: streamed mismatch (%d vs %d bytes)",
						n, off, length, len(streamed), len(want))
				}
			}
		}
	}
}

// drainTiny reads r to EOF one byte at a time, asserting a clean EOF.
func drainTiny(t *testing.T, r io.Reader) []byte {
	t.Helper()
	var out []byte
	buf := make([]byte, 1)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
}

// TestRange_SpansBoundary zooms in on the boundary case from the acceptance
// criteria: a small range centred on a chunk boundary is byte-exact across
// it, and is assembled from exactly the two adjacent chunks.
func TestRange_SpansBoundary(t *testing.T) {
	const cs = aesstream.MinChunkSize
	cfg := baseConfig(cs)
	pt := pattern(3 * cs)
	ct := mustSeal(t, cfg, pt)
	size := int64(len(ct))

	const off, length = cs - 4, 8 // four bytes either side of the boundary
	lo, hi := chunkWindow(off, length, size, cs)
	src := &trackingReaderAt{t: t, data: ct, allowLo: lo, allowHi: hi}

	got, err := aesstream.OpenRange(cfg, src, size, off, length)
	if err != nil {
		t.Fatalf("OpenRange: %v", err)
	}
	if !bytes.Equal(got, pt[off:off+length]) {
		t.Fatalf("boundary range mismatch: got % x, want % x", got, pt[off:off+length])
	}
	if src.reads != 2 {
		t.Fatalf("fetched %d chunks for a 2-chunk-spanning range, want 2", src.reads)
	}
}

// TestRange_SelectiveFetch is the third acceptance criterion: only the
// ciphertext chunks overlapping the range are fetched. It both (a) pins the
// fetch to the computed window via trackingReaderAt and (b) proves the
// final chunk is genuinely skipped by forbidding it and still succeeding.
func TestRange_SelectiveFetch(t *testing.T) {
	const cs = aesstream.MinChunkSize
	cfg := baseConfig(cs)
	pt := pattern(3*cs + 123) // chunks 0,1,2 full + chunk 3 partial (123 B)
	ct := mustSeal(t, cfg, pt)
	size := int64(len(ct))
	enc := int64(cs) + aesstream.TagSize

	t.Run("only overlapping chunks", func(t *testing.T) {
		cases := []struct{ off, length int64 }{
			{0, 10},                // chunk 0
			{cs + 5, 50},           // chunk 1
			{2*cs - 5, 10},         // chunks 1–2
			{0, int64(3*cs + 123)}, // whole stream → all chunks
		}
		for _, c := range cases {
			effLen := c.length
			if avail := int64(len(pt)) - c.off; effLen > avail {
				effLen = avail
			}
			lo, hi := chunkWindow(c.off, effLen, size, cs)
			src := &trackingReaderAt{t: t, data: ct, allowLo: lo, allowHi: hi}
			got, err := aesstream.OpenRange(cfg, src, size, c.off, c.length)
			if err != nil {
				t.Fatalf("off=%d len=%d: OpenRange: %v", c.off, c.length, err)
			}
			if !bytes.Equal(got, pt[c.off:c.off+effLen]) {
				t.Fatalf("off=%d len=%d: mismatch", c.off, c.length)
			}
		}
	})

	t.Run("final chunk skipped when excluded", func(t *testing.T) {
		// Forbid every byte from chunk 3 onward. A range inside chunks 0–2
		// must still decrypt; a range that reaches into chunk 3 must fail
		// because the read it needs is refused — proving chunk 3 was never
		// touched in the success case.
		forbidFrom := 3 * enc
		ok := &trackingReaderAt{t: t, data: ct, allowHi: forbidFrom}
		if _, err := aesstream.OpenRange(cfg, ok, size, 2*cs, cs); err != nil {
			t.Fatalf("range within chunks 0–2 should not touch chunk 3: %v", err)
		}

		// Now reach into chunk 3. Use a reader that quietly refuses the
		// forbidden window (no t.Errorf) so we can assert the error.
		blocked := &errorReaderAt{data: ct, blockFrom: forbidFrom}
		if _, err := aesstream.OpenRange(cfg, blocked, size, 3*cs, 50); err == nil {
			t.Fatal("expected an error reaching into the forbidden final chunk")
		}
	})
}

// errorReaderAt refuses reads at or beyond blockFrom without failing the
// test, so a caller's own error handling can be asserted.
type errorReaderAt struct {
	data      []byte
	blockFrom int64
}

func (r *errorReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.blockFrom || off+int64(len(p)) > r.blockFrom {
		return 0, fmt.Errorf("read of forbidden chunk at %d", off)
	}
	return bytes.NewReader(r.data).ReadAt(p, off)
}

// TestRange_Tampered is the fourth acceptance criterion: a range over a
// tampered chunk errors rather than returning corrupt plaintext — and a
// range that does not overlap the tampered chunk is unaffected, since that
// chunk is never fetched.
func TestRange_Tampered(t *testing.T) {
	const cs = aesstream.MinChunkSize
	cfg := baseConfig(cs)
	pt := pattern(3*cs + 123)
	enc := int64(cs) + aesstream.TagSize

	// Flip one bit inside chunk 1's ciphertext.
	tampered := mustSeal(t, cfg, pt)
	tampered[enc+100] ^= 0x01
	size := int64(len(tampered))

	t.Run("range over tampered chunk errors", func(t *testing.T) {
		_, err := aesstream.OpenRange(cfg, bytes.NewReader(tampered), size, cs+10, 20)
		if !errors.Is(err, aesstream.ErrCorrupted) {
			t.Fatalf("OpenRange over tampered chunk = %v, want ErrCorrupted", err)
		}
	})

	t.Run("range avoiding tampered chunk succeeds", func(t *testing.T) {
		// Read from chunk 0 only; chunk 1 (tampered) is never fetched.
		got, err := aesstream.OpenRange(cfg, bytes.NewReader(tampered), size, 0, 100)
		if err != nil {
			t.Fatalf("OpenRange avoiding tampered chunk: %v", err)
		}
		if !bytes.Equal(got, pt[:100]) {
			t.Fatal("plaintext mismatch on untampered chunk")
		}
	})
}

// TestRange_WrongCiphertextSize shows the declared total length pins the
// final-chunk flag: lie about the size so the true (final) chunk looks like
// a non-final one, and authentication fails rather than silently returning
// data sealed under a different nonce.
func TestRange_WrongCiphertextSize(t *testing.T) {
	const cs = aesstream.MinChunkSize
	cfg := baseConfig(cs)
	ct := mustSeal(t, cfg, pattern(cs)) // exactly one full, final chunk
	enc := int64(cs) + aesstream.TagSize

	// Claim two chunks: chunk 0 is now treated as non-final (flag 0x00),
	// but it was sealed as the final chunk (flag 0x01).
	_, err := aesstream.OpenRange(cfg, bytes.NewReader(ct), 2*enc, 0, cs)
	if !errors.Is(err, aesstream.ErrCorrupted) {
		t.Fatalf("OpenRange with wrong size = %v, want ErrCorrupted", err)
	}
}

// TestRange_OutOfRange covers the offset/length validation: negative
// inputs and an offset past the end are ErrRange; an offset exactly at the
// end is a valid empty read; a length past the end clamps.
func TestRange_OutOfRange(t *testing.T) {
	const cs = aesstream.MinChunkSize
	cfg := baseConfig(cs)
	const n = 2*cs + 50
	ct := mustSeal(t, cfg, pattern(n))
	size := int64(len(ct))

	bad := []struct {
		name        string
		off, length int64
	}{
		{"negative off", -1, 10},
		{"negative length", 0, -1},
		{"off past end", int64(n) + 1, 0},
	}
	for _, c := range bad {
		if _, err := aesstream.OpenRange(cfg, bytes.NewReader(ct), size, c.off, c.length); !errors.Is(err, aesstream.ErrRange) {
			t.Errorf("%s: err = %v, want ErrRange", c.name, err)
		}
	}

	// Offset exactly at the end: a valid zero-length read that fetches no
	// ciphertext.
	src := &trackingReaderAt{t: t, data: ct}
	got, err := aesstream.OpenRange(cfg, src, size, int64(n), 100)
	if err != nil {
		t.Fatalf("OpenRange at end: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("OpenRange at end returned %d bytes, want 0", len(got))
	}
	if src.reads != 0 {
		t.Fatalf("empty read fetched %d chunks, want 0", src.reads)
	}

	// Length past the end clamps to the available bytes.
	got, err = aesstream.OpenRange(cfg, bytes.NewReader(ct), size, int64(n)-10, 1000)
	if err != nil {
		t.Fatalf("OpenRange clamping: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("clamped read returned %d bytes, want 10", len(got))
	}
}

// TestRange_EmptyPlaintext checks the empty-stream corner: the only valid
// offset is 0, which yields no bytes; any positive offset is ErrRange.
func TestRange_EmptyPlaintext(t *testing.T) {
	const cs = aesstream.MinChunkSize
	cfg := baseConfig(cs)
	ct := mustSeal(t, cfg, nil) // one empty final chunk (TagSize bytes)
	size := int64(len(ct))

	got, err := aesstream.OpenRange(cfg, bytes.NewReader(ct), size, 0, 10)
	if err != nil {
		t.Fatalf("OpenRange(empty, off=0): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d bytes from empty plaintext, want 0", len(got))
	}
	if _, err := aesstream.OpenRange(cfg, bytes.NewReader(ct), size, 1, 0); !errors.Is(err, aesstream.ErrRange) {
		t.Fatalf("OpenRange(empty, off=1) = %v, want ErrRange", err)
	}
}

// TestRange_InvalidCiphertextSize rejects declared lengths that cannot be a
// stream this package produced.
func TestRange_InvalidCiphertextSize(t *testing.T) {
	const cs = aesstream.MinChunkSize
	cfg := baseConfig(cs)
	enc := int64(cs) + aesstream.TagSize

	for _, size := range []int64{0, 1, aesstream.TagSize - 1, enc + 5} {
		if _, err := aesstream.NewRangeReader(bytes.NewReader(nil), size, cfg, 0, 1); !errors.Is(err, aesstream.ErrCiphertextSize) {
			t.Errorf("NewRangeReader(size=%d) = %v, want ErrCiphertextSize", size, err)
		}
		if _, err := aesstream.DecryptedSize(size, cs); !errors.Is(err, aesstream.ErrCiphertextSize) {
			t.Errorf("DecryptedSize(%d) = %v, want ErrCiphertextSize", size, err)
		}
	}
}

// TestRange_BadConfig confirms cfg validation runs before any range math.
func TestRange_BadConfig(t *testing.T) {
	cfg := baseConfig(aesstream.MinChunkSize)
	cfg.Key = cfg.Key[:16] // wrong length
	if _, err := aesstream.NewRangeReader(bytes.NewReader(nil), 64, cfg, 0, 1); !errors.Is(err, aesstream.ErrKeySize) {
		t.Fatalf("NewRangeReader with short key = %v, want ErrKeySize", err)
	}
}

// TestDecryptedSize_InvertsEncryptedSize checks the geometry helpers agree:
// DecryptedSize undoes EncryptedSize for every plaintext length.
func TestDecryptedSize_InvertsEncryptedSize(t *testing.T) {
	const cs = aesstream.MinChunkSize
	for _, n := range []int64{0, 1, 15, cs - 1, cs, cs + 1, 2 * cs, 2*cs + 1, 3*cs + 123} {
		ctLen := aesstream.EncryptedSize(n, cs)
		got, err := aesstream.DecryptedSize(ctLen, cs)
		if err != nil {
			t.Fatalf("n=%d: DecryptedSize(%d): %v", n, ctLen, err)
		}
		if got != n {
			t.Errorf("n=%d: DecryptedSize(EncryptedSize)=%d, want %d", n, got, n)
		}
	}
}

// TestRange_DefaultChunkSize exercises the default 256 KiB chunk path,
// including a boundary-spanning range, to confirm the geometry is not
// hard-coded to the test's small chunk size.
func TestRange_DefaultChunkSize(t *testing.T) {
	cfg := baseConfig(0) // default 256 KiB
	const dc = aesstream.DefaultChunkSize
	pt := pattern(2*dc + 1000)
	ct := mustSeal(t, cfg, pt)
	size := int64(len(ct))

	for _, c := range []struct{ off, length int64 }{
		{0, 100},
		{dc - 50, 100}, // spans the first boundary
		{2 * dc, 1000}, // the partial final chunk
		{dc + 12345, 60000},
	} {
		got, err := aesstream.OpenRange(cfg, bytes.NewReader(ct), size, c.off, c.length)
		if err != nil {
			t.Fatalf("off=%d len=%d: %v", c.off, c.length, err)
		}
		want := pt[c.off : c.off+c.length]
		if !bytes.Equal(got, want) {
			t.Fatalf("off=%d len=%d: mismatch", c.off, c.length)
		}
	}
}
