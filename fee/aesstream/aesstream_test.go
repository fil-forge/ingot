package aesstream_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"runtime"
	"testing"

	"github.com/fil-forge/ingot/fee/aesstream"
)

// testKey is an obviously-synthetic, fixed test CEK — never a real key.
var testKey = bytes.Repeat([]byte{0x2a}, aesstream.KeySize)

// baseConfig returns a valid Config with a fixed key, base nonce and AAD
// and the given chunk size (0 → default). Tests clone and tweak it.
func baseConfig(chunkSize int) aesstream.Config {
	return aesstream.Config{
		Key:       append([]byte(nil), testKey...),
		BaseNonce: []byte{1, 2, 3, 4, 5, 6, 7},
		AAD:       []byte("envelope-enc-structure"),
		ChunkSize: chunkSize,
	}
}

// pattern returns n deterministic, position-dependent bytes.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + 1)
	}
	return b
}

func mustSeal(t *testing.T, cfg aesstream.Config, pt []byte) []byte {
	t.Helper()
	ct, err := aesstream.Seal(cfg, pt)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return ct
}

func TestRoundTrip(t *testing.T) {
	// Sizes chosen around chunk boundaries: empty, sub-tag, tag-sized,
	// just under/over one chunk, exact multiples, and odd remainders.
	const cs = aesstream.MinChunkSize // 4 KiB, the smallest legal chunk
	sizes := []int{0, 1, 15, 16, 17, cs - 1, cs, cs + 1, 2 * cs, 2*cs + 1, 3 * cs, 3*cs + 123}

	for _, n := range sizes {
		pt := pattern(n)
		cfg := baseConfig(cs)

		ct := mustSeal(t, cfg, pt)
		if got := int64(len(ct)); got != aesstream.EncryptedSize(int64(n), cs) {
			t.Errorf("n=%d: ciphertext len %d, EncryptedSize says %d", n, got, aesstream.EncryptedSize(int64(n), cs))
		}

		got, err := aesstream.Open(cfg, ct)
		if err != nil {
			t.Fatalf("n=%d: Open: %v", n, err)
		}
		if !bytes.Equal(got, pt) {
			t.Errorf("n=%d: round-trip mismatch (got %d bytes)", n, len(got))
		}
	}
}

func TestRoundTripDefaultChunkSize(t *testing.T) {
	cfg := baseConfig(0) // default 256 KiB
	if cfg.ChunkSize != 0 {
		t.Fatal("expected zero chunk size in config")
	}
	for _, n := range []int{0, 1, aesstream.DefaultChunkSize, aesstream.DefaultChunkSize + 1, 3*aesstream.DefaultChunkSize + 99} {
		pt := pattern(n)
		got, err := aesstream.Open(cfg, mustSeal(t, cfg, pt))
		if err != nil {
			t.Fatalf("n=%d: Open: %v", n, err)
		}
		if !bytes.Equal(got, pt) {
			t.Errorf("n=%d: round-trip mismatch", n)
		}
	}
}

// TestStreamingOddBoundaries writes the plaintext in awkward-sized pieces
// and reads it back in awkward-sized pieces, exercising the buffering on
// both sides independently of chunk alignment.
func TestStreamingOddBoundaries(t *testing.T) {
	const cs = aesstream.MinChunkSize
	pt := pattern(5*cs + 777)
	cfg := baseConfig(cs)

	for _, wstep := range []int{1, 7, 333, cs, cs + 5} {
		for _, rstep := range []int{1, 3, 500, cs - 1, 4 * cs} {
			var ctBuf bytes.Buffer
			w, err := aesstream.NewWriter(&ctBuf, cfg)
			if err != nil {
				t.Fatal(err)
			}
			for off := 0; off < len(pt); off += wstep {
				end := min(off+wstep, len(pt))
				if _, err := w.Write(pt[off:end]); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			r, err := aesstream.NewReader(bytes.NewReader(ctBuf.Bytes()), cfg)
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			rbuf := make([]byte, rstep)
			if _, err := io.CopyBuffer(&out, r, rbuf); err != nil {
				t.Fatalf("wstep=%d rstep=%d: copy: %v", wstep, rstep, err)
			}
			if !bytes.Equal(out.Bytes(), pt) {
				t.Errorf("wstep=%d rstep=%d: mismatch", wstep, rstep)
			}
		}
	}
}

// TestFullSizeLastChunk covers the decrypt-retry path: when the plaintext
// is an exact multiple of the chunk size, the final chunk is full-size and
// the reader must discover it is the last one by retrying with the flag.
func TestFullSizeLastChunk(t *testing.T) {
	const cs = aesstream.MinChunkSize
	for _, mult := range []int{1, 2, 5} {
		cfg := baseConfig(cs)
		pt := pattern(mult * cs)
		got, err := aesstream.Open(cfg, mustSeal(t, cfg, pt))
		if err != nil {
			t.Fatalf("mult=%d: Open: %v", mult, err)
		}
		if !bytes.Equal(got, pt) {
			t.Errorf("mult=%d: mismatch", mult)
		}
	}
}

func TestEmptyPlaintextIsOneChunk(t *testing.T) {
	cfg := baseConfig(aesstream.MinChunkSize)
	ct := mustSeal(t, cfg, nil)
	if len(ct) != aesstream.TagSize {
		t.Fatalf("empty plaintext: ciphertext len = %d, want %d (one empty chunk)", len(ct), aesstream.TagSize)
	}
	got, err := aesstream.Open(cfg, ct)
	if err != nil {
		t.Fatalf("Open empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty plaintext: decrypted %d bytes, want 0", len(got))
	}
}

func TestTruncationFails(t *testing.T) {
	const cs = aesstream.MinChunkSize
	cfg := baseConfig(cs)
	pt := pattern(2*cs + 1000) // chunks: [cs] [cs] [1000] → 3 chunks
	ct := mustSeal(t, cfg, pt)
	enc := cs + aesstream.TagSize // a full ciphertext chunk

	cases := []struct {
		name    string
		ct      []byte
		wantErr error // nil → just require some error
	}{
		{"drop final chunk", ct[:2*enc], aesstream.ErrTruncated},
		{"cut final chunk tag", ct[:len(ct)-4], aesstream.ErrCorrupted},
		{"cut mid non-final chunk", ct[:enc+50], aesstream.ErrCorrupted},
		{"empty ciphertext", ct[:0], aesstream.ErrTruncated},
		{"single byte", ct[:1], aesstream.ErrCorrupted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := aesstream.Open(cfg, tc.ct)
			if err == nil {
				t.Fatalf("expected error, got %d bytes of plaintext", len(got))
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			// Whatever bytes were released before the error must be a
			// genuine prefix of the plaintext — never fabricated data.
			if !bytes.HasPrefix(pt, got) {
				t.Fatalf("released bytes are not a plaintext prefix")
			}
		})
	}
}

func TestReorderFails(t *testing.T) {
	const cs = aesstream.MinChunkSize
	cfg := baseConfig(cs)
	pt := pattern(2*cs + 500) // 3 chunks
	ct := mustSeal(t, cfg, pt)
	enc := cs + aesstream.TagSize

	// Swap the first two full chunks.
	swapped := make([]byte, len(ct))
	copy(swapped, ct)
	copy(swapped[0:enc], ct[enc:2*enc])
	copy(swapped[enc:2*enc], ct[0:enc])

	if _, err := aesstream.Open(cfg, swapped); !errors.Is(err, aesstream.ErrCorrupted) {
		t.Fatalf("reordered chunks: err = %v, want ErrCorrupted", err)
	}
}

func TestBitFlipFails(t *testing.T) {
	const cs = aesstream.MinChunkSize
	cfg := baseConfig(cs)
	pt := pattern(2*cs + 500)
	ct := mustSeal(t, cfg, pt)
	enc := cs + aesstream.TagSize

	positions := map[string]int{
		"first chunk body": 0,
		"first chunk tag":  cs + 4,      // inside chunk 0's tag
		"second chunk":     enc + 10,    // inside chunk 1
		"final chunk":      len(ct) - 1, // last byte (tag of final chunk)
	}
	for name, pos := range positions {
		t.Run(name, func(t *testing.T) {
			tampered := make([]byte, len(ct))
			copy(tampered, ct)
			tampered[pos] ^= 0x01
			if _, err := aesstream.Open(cfg, tampered); !errors.Is(err, aesstream.ErrCorrupted) {
				t.Fatalf("flip at %d: err = %v, want ErrCorrupted", pos, err)
			}
		})
	}
}

func TestTrailingDataFails(t *testing.T) {
	const cs = aesstream.MinChunkSize
	cfg := baseConfig(cs)

	// After a full-size final chunk the reader stops on the chunk boundary
	// and the look-ahead detects the extra bytes as trailing data.
	t.Run("after full-size final chunk", func(t *testing.T) {
		ct := append(mustSeal(t, cfg, pattern(2*cs)), 0xde, 0xad)
		if _, err := aesstream.Open(cfg, ct); !errors.Is(err, aesstream.ErrTrailingData) {
			t.Fatalf("err = %v, want ErrTrailingData", err)
		}
	})

	// After a short final chunk the trailing bytes are pulled into the
	// final chunk read, so they break authentication instead — still an
	// error, just reported as corruption.
	t.Run("after short final chunk", func(t *testing.T) {
		ct := append(mustSeal(t, cfg, pattern(1000)), 0xde, 0xad)
		if _, err := aesstream.Open(cfg, ct); err == nil {
			t.Fatal("expected an error, got nil")
		}
	})
}

func TestWrongKeyOrNonceOrAADFails(t *testing.T) {
	cfg := baseConfig(aesstream.MinChunkSize)
	pt := pattern(3000)
	ct := mustSeal(t, cfg, pt)

	t.Run("wrong key", func(t *testing.T) {
		bad := cfg
		bad.Key = bytes.Repeat([]byte{0x99}, aesstream.KeySize)
		if _, err := aesstream.Open(bad, ct); !errors.Is(err, aesstream.ErrCorrupted) {
			t.Fatalf("err = %v, want ErrCorrupted", err)
		}
	})
	t.Run("wrong base nonce", func(t *testing.T) {
		bad := cfg
		bad.BaseNonce = []byte{9, 9, 9, 9, 9, 9, 9}
		if _, err := aesstream.Open(bad, ct); !errors.Is(err, aesstream.ErrCorrupted) {
			t.Fatalf("err = %v, want ErrCorrupted", err)
		}
	})
	t.Run("wrong AAD", func(t *testing.T) {
		bad := cfg
		bad.AAD = []byte("different")
		if _, err := aesstream.Open(bad, ct); !errors.Is(err, aesstream.ErrCorrupted) {
			t.Fatalf("err = %v, want ErrCorrupted", err)
		}
	})
}

func TestNilAADRoundTrips(t *testing.T) {
	cfg := baseConfig(aesstream.MinChunkSize)
	cfg.AAD = nil
	pt := pattern(9000)
	got, err := aesstream.Open(cfg, mustSeal(t, cfg, pt))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(got, pt) {
		t.Fatal("nil-AAD round-trip mismatch")
	}
}

func TestDeterministic(t *testing.T) {
	cfg := baseConfig(aesstream.MinChunkSize)
	pt := pattern(10000)
	a := mustSeal(t, cfg, pt)
	b := mustSeal(t, cfg, pt)
	if !bytes.Equal(a, b) {
		t.Fatal("Seal is not deterministic for fixed key/nonce/AAD")
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*aesstream.Config)
		wantErr error
	}{
		{"short key", func(c *aesstream.Config) { c.Key = c.Key[:16] }, aesstream.ErrKeySize},
		{"long key", func(c *aesstream.Config) { c.Key = append(c.Key, 0) }, aesstream.ErrKeySize},
		{"short nonce", func(c *aesstream.Config) { c.BaseNonce = c.BaseNonce[:6] }, aesstream.ErrBaseNonceSize},
		{"long nonce", func(c *aesstream.Config) { c.BaseNonce = append(c.BaseNonce, 0) }, aesstream.ErrBaseNonceSize},
		{"chunk too small", func(c *aesstream.Config) { c.ChunkSize = aesstream.MinChunkSize - 1 }, aesstream.ErrChunkSize},
		{"chunk too large", func(c *aesstream.Config) { c.ChunkSize = aesstream.MaxChunkSize + 1 }, aesstream.ErrChunkSize},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig(aesstream.MinChunkSize)
			tc.mutate(&cfg)
			if _, err := aesstream.NewWriter(io.Discard, cfg); !errors.Is(err, tc.wantErr) {
				t.Errorf("NewWriter err = %v, want %v", err, tc.wantErr)
			}
			if _, err := aesstream.NewReader(bytes.NewReader(nil), cfg); !errors.Is(err, tc.wantErr) {
				t.Errorf("NewReader err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestChunkSizeAccessorAndDefault(t *testing.T) {
	w, err := aesstream.NewWriter(io.Discard, baseConfig(0))
	if err != nil {
		t.Fatal(err)
	}
	if w.ChunkSize() != aesstream.DefaultChunkSize {
		t.Errorf("default ChunkSize = %d, want %d", w.ChunkSize(), aesstream.DefaultChunkSize)
	}
	w2, err := aesstream.NewWriter(io.Discard, baseConfig(aesstream.MinChunkSize))
	if err != nil {
		t.Fatal(err)
	}
	if w2.ChunkSize() != aesstream.MinChunkSize {
		t.Errorf("ChunkSize = %d, want %d", w2.ChunkSize(), aesstream.MinChunkSize)
	}

	r, err := aesstream.NewReader(bytes.NewReader(nil), baseConfig(aesstream.MinChunkSize))
	if err != nil {
		t.Fatal(err)
	}
	if r.ChunkSize() != aesstream.MinChunkSize {
		t.Errorf("Reader.ChunkSize = %d, want %d", r.ChunkSize(), aesstream.MinChunkSize)
	}
}

// errWriter accepts bytes until failAfter, then fails (with a partial
// write on the boundary call).
type errWriter struct {
	n, failAfter int
}

var errWriteBoom = errors.New("boom: write failed")

func (w *errWriter) Write(p []byte) (int, error) {
	if w.n+len(p) > w.failAfter {
		avail := w.failAfter - w.n
		w.n = w.failAfter
		return avail, errWriteBoom
	}
	w.n += len(p)
	return len(p), nil
}

// TestWriteErrorPropagates checks that an underlying write failure is
// surfaced and then made sticky across later Write and Close calls.
func TestWriteErrorPropagates(t *testing.T) {
	const cs = aesstream.MinChunkSize
	w, err := aesstream.NewWriter(&errWriter{failAfter: 100}, baseConfig(cs))
	if err != nil {
		t.Fatal(err)
	}
	// Two chunks' worth forces a flush mid-Write, which hits the failure.
	if _, err := w.Write(make([]byte, 2*cs)); !errors.Is(err, errWriteBoom) {
		t.Fatalf("Write err = %v, want errWriteBoom", err)
	}
	if _, err := w.Write([]byte("x")); !errors.Is(err, errWriteBoom) {
		t.Fatalf("sticky Write err = %v, want errWriteBoom", err)
	}
	if err := w.Close(); !errors.Is(err, errWriteBoom) {
		t.Fatalf("sticky Close err = %v, want errWriteBoom", err)
	}
}

// errReader yields data then fails with a non-EOF error.
type errReader struct {
	data []byte
	off  int
}

var errReadBoom = errors.New("boom: read failed")

func (r *errReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, errReadBoom
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}

// TestReadErrorPropagates checks that a non-EOF error from the underlying
// reader is surfaced (not mistaken for truncation or corruption).
func TestReadErrorPropagates(t *testing.T) {
	r, err := aesstream.NewReader(&errReader{data: []byte("partial chunk bytes")}, baseConfig(aesstream.MinChunkSize))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(r); !errors.Is(err, errReadBoom) {
		t.Fatalf("Read err = %v, want errReadBoom", err)
	}
}

func TestWriteAfterCloseFails(t *testing.T) {
	w, err := aesstream.NewWriter(io.Discard, baseConfig(aesstream.MinChunkSize))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close should be a no-op, got %v", err)
	}
	if _, err := w.Write([]byte("more")); err == nil {
		t.Fatal("Write after Close should fail")
	}
}

func TestEncryptedSize(t *testing.T) {
	cases := []struct {
		pt, chunk int
		want      int64
	}{
		{0, 4096, 16},             // one empty chunk
		{1, 4096, 1 + 16},         // one tiny chunk
		{4096, 4096, 4096 + 16},   // exact one chunk
		{4097, 4096, 4097 + 2*16}, // two chunks
		{8192, 4096, 8192 + 2*16}, // exact two chunks
		{0, 0, 16},                // default chunk size, empty
	}
	for _, tc := range cases {
		if got := aesstream.EncryptedSize(int64(tc.pt), tc.chunk); got != tc.want {
			t.Errorf("EncryptedSize(%d, %d) = %d, want %d", tc.pt, tc.chunk, got, tc.want)
		}
		// Cross-check against an actual Seal for small cases.
		if tc.chunk != 0 {
			cfg := baseConfig(tc.chunk)
			ct := mustSeal(t, cfg, pattern(tc.pt))
			if int64(len(ct)) != tc.want {
				t.Errorf("Seal len for pt=%d chunk=%d = %d, want %d", tc.pt, tc.chunk, len(ct), tc.want)
			}
		}
	}
}

func TestNewBaseNonce(t *testing.T) {
	a, err := aesstream.NewBaseNonce()
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != aesstream.BaseNonceSize {
		t.Fatalf("len = %d, want %d", len(a), aesstream.BaseNonceSize)
	}
	b, err := aesstream.NewBaseNonce()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("two base nonces were identical")
	}
}

// TestAADIsCopiedOnConstruction verifies the Writer and Reader copy the
// AAD at construction, so mutating the caller's slice afterwards cannot
// change a stream's authentication.
func TestAADIsCopiedOnConstruction(t *testing.T) {
	const cs = aesstream.MinChunkSize
	pt := pattern(2000)
	const orig = "original-aad-value"
	const tampered = "TAMPERED-AAD-VALUE" // same length, so copy fully overwrites

	// Encrypt with an AAD slice we mutate right after constructing the
	// Writer. A copy means it encrypts under orig despite the mutation.
	aad := []byte(orig)
	encCfg := baseConfig(cs)
	encCfg.AAD = aad
	var buf bytes.Buffer
	w, err := aesstream.NewWriter(&buf, encCfg)
	if err != nil {
		t.Fatal(err)
	}
	copy(aad, tampered)
	if _, err := w.Write(pt); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Decrypting with orig only succeeds if the Writer copied the AAD.
	decCfg := baseConfig(cs)
	decCfg.AAD = []byte(orig)
	got, err := aesstream.Open(decCfg, buf.Bytes())
	if err != nil {
		t.Fatalf("Open with original AAD failed (Writer did not copy AAD?): %v", err)
	}
	if !bytes.Equal(got, pt) {
		t.Fatal("round-trip mismatch")
	}

	// The Reader must be immune to the same post-construction mutation.
	rAAD := []byte(orig)
	rCfg := baseConfig(cs)
	rCfg.AAD = rAAD
	r, err := aesstream.NewReader(bytes.NewReader(buf.Bytes()), rCfg)
	if err != nil {
		t.Fatal(err)
	}
	copy(rAAD, tampered)
	got2, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("Reader failed after AAD mutation (Reader did not copy AAD?): %v", err)
	}
	if !bytes.Equal(got2, pt) {
		t.Fatal("reader round-trip mismatch")
	}
}

// stuckWriter reports zero progress with no error on every call, which a
// compliant io.Writer never does. writeAll must not spin on it.
type stuckWriter struct{}

func (stuckWriter) Write(p []byte) (int, error) { return 0, nil }

func TestWriteAllShortWriteGuard(t *testing.T) {
	w, err := aesstream.NewWriter(stuckWriter{}, baseConfig(aesstream.MinChunkSize))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatal(err) // buffered; no flush yet
	}
	// Close flushes the final chunk, which the stuck writer never accepts;
	// writeAll must return ErrShortWrite rather than loop forever.
	if err := w.Close(); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Close err = %v, want io.ErrShortWrite", err)
	}
}

// patternReader emits an endless deterministic byte stream without
// allocating, so a huge plaintext can be produced with O(1) memory.
type patternReader struct{ pos uint64 }

func (r *patternReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(r.pos)
		r.pos++
	}
	return len(p), nil
}

func heapInuse() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// TestBoundedMemory streams far more data than the memory budget through
// encryption and decryption, asserting both that it round-trips (compared
// by hash, never buffered whole) and that live heap stays O(chunk size),
// not O(stream size) — the "encrypt larger than memory without OOM" AC.
func TestBoundedMemory(t *testing.T) {
	const (
		streamSize = 128 << 20 // 128 MiB, ~512 default chunks
		budget     = 32 << 20  // generous vs O(chunk)≈MiB, tiny vs O(stream)=128 MiB
	)
	cfg := baseConfig(0) // default 256 KiB chunks

	pr, pw := io.Pipe()

	// Encrypt side: hash the plaintext as it is produced, then send the
	// digest (and any error) once the whole stream has been written.
	type result struct {
		sum []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		inHash := sha256.New()
		w, err := aesstream.NewWriter(pw, cfg)
		if err != nil {
			pw.CloseWithError(err)
			done <- result{err: err}
			return
		}
		src := io.TeeReader(io.LimitReader(&patternReader{}, streamSize), inHash)
		_, err = io.Copy(w, src)
		if err == nil {
			err = w.Close()
		}
		pw.CloseWithError(err)
		done <- result{sum: inHash.Sum(nil), err: err}
	}()

	r, err := aesstream.NewReader(pr, cfg)
	if err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	baseline := heapInuse()
	var maxHeap uint64

	outHash := sha256.New()
	buf := make([]byte, 64*1024)
	var total int64
	for i := 0; ; i++ {
		n, rerr := r.Read(buf)
		outHash.Write(buf[:n])
		total += int64(n)
		if i%64 == 0 {
			if h := heapInuse(); h > maxHeap {
				maxHeap = h
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("decrypt: %v", rerr)
		}
	}

	res := <-done
	if res.err != nil {
		t.Fatalf("encrypt: %v", res.err)
	}
	if total != streamSize {
		t.Fatalf("decrypted %d bytes, want %d", total, streamSize)
	}
	if !bytes.Equal(outHash.Sum(nil), res.sum) {
		t.Fatal("decrypted stream hash != plaintext hash")
	}
	if delta := int64(maxHeap) - int64(baseline); delta > budget {
		t.Fatalf("heap grew by %d bytes (> %d budget) streaming %d bytes: not O(chunk)", delta, budget, streamSize)
	}
}
