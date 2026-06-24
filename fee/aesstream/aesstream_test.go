package aesstream_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"runtime"
	"testing"
	"testing/iotest"

	"github.com/fil-forge/ingot/fee/aesstream"
	"github.com/stretchr/testify/require"
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

func TestRoundTrip(t *testing.T) {
	// Each case round-trips plaintext sizes chosen around its own chunk
	// boundaries: empty (encodes as a single empty chunk, TagSize bytes),
	// sub-tag, tag-sized, just under/over one chunk, exact multiples (a
	// full-size final chunk, which forces the decrypt-side last-chunk
	// retry), and odd remainders.
	cases := []struct {
		name      string
		chunkSize int     // Config.ChunkSize; 0 selects DefaultChunkSize
		sizes     []int64 // plaintext lengths to seal and reopen
	}{
		{
			name:      "min chunk size",
			chunkSize: aesstream.MinChunkSize,
			sizes: []int64{
				0,
				1,
				15,
				16,
				17,
				aesstream.MinChunkSize - 1,
				aesstream.MinChunkSize,
				aesstream.MinChunkSize + 1,
				2 * aesstream.MinChunkSize,
				2*aesstream.MinChunkSize + 1,
				3 * aesstream.MinChunkSize,
				3*aesstream.MinChunkSize + 123,
			},
		},
		{
			name:      "default chunk size",
			chunkSize: 0, // exercises the 0 → DefaultChunkSize resolution
			sizes: []int64{
				0,
				1,
				aesstream.DefaultChunkSize,
				aesstream.DefaultChunkSize + 1,
				2 * aesstream.DefaultChunkSize,
				3*aesstream.DefaultChunkSize + 99,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig(tc.chunkSize)
			for _, n := range tc.sizes {
				pt := pattern(int(n))

				ct, err := aesstream.Seal(cfg, pt)
				require.NoErrorf(t, err, "n=%d: Seal", n)
				require.Equalf(t, aesstream.EncryptedSize(n, tc.chunkSize), int64(len(ct)),
					"n=%d: ciphertext length", n)

				got, err := aesstream.Open(cfg, ct)
				require.NoErrorf(t, err, "n=%d: Open", n)
				require.Equalf(t, pt, got, "n=%d: round-trip mismatch", n)
			}
		})
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
			require.NoError(t, err)
			for off := 0; off < len(pt); off += wstep {
				end := min(off+wstep, len(pt))
				_, werr := w.Write(pt[off:end])
				require.NoError(t, werr)
			}
			require.NoError(t, w.Close())

			r, err := aesstream.NewReader(bytes.NewReader(ctBuf.Bytes()), cfg)
			require.NoError(t, err)
			var out bytes.Buffer
			n, err := io.CopyBuffer(&out, r, make([]byte, rstep))
			require.NoErrorf(t, err, "wstep=%d rstep=%d: copy", wstep, rstep)
			require.Equalf(t, int64(len(pt)), n, "wstep=%d rstep=%d: copied byte count", wstep, rstep)
			require.Equalf(t, pt, out.Bytes(), "wstep=%d rstep=%d: mismatch", wstep, rstep)
		}
	}

	// Decrypt once more as a true read-side stream: feed the ciphertext one
	// byte at a time, forcing the Reader to assemble each chunk from many
	// short underlying reads (the matrix above only varies the caller's read
	// sizes, never the underlying reader's).
	ct, err := aesstream.Seal(cfg, pt)
	require.NoError(t, err)
	r, err := aesstream.NewReader(iotest.OneByteReader(bytes.NewReader(ct)), cfg)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, pt, got, "one-byte-read decrypt mismatch")
}

// TestDecryptFailures gathers every way Open must reject a ciphertext:
// truncation, mid-stream cuts, reordering, bit flips, trailing data, and
// decryption under the wrong key, nonce or AAD. Each case either corrupts a
// good ciphertext or decrypts it with an altered config; every case also
// checks the streaming-safety invariant that whatever plaintext was released
// before the error is a genuine prefix — never fabricated bytes.
func TestDecryptFailures(t *testing.T) {
	const cs = aesstream.MinChunkSize
	cfg := baseConfig(cs)
	enc := cs + aesstream.TagSize // one full ciphertext chunk

	// Base ciphertext: three chunks with a short final chunk, [cs][cs][1000].
	pt := pattern(2*cs + 1000)
	ct, err := aesstream.Seal(cfg, pt)
	require.NoError(t, err)

	// A ciphertext whose final chunk is full-size, so appended bytes land
	// past the boundary and must be caught as trailing data rather than
	// pulled into the final chunk.
	fullPT := pattern(2 * cs)
	fullCT, err := aesstream.Seal(cfg, fullPT)
	require.NoError(t, err)

	// reordered swaps the first two (full) chunks.
	reordered := append([]byte(nil), ct...)
	copy(reordered[0:enc], ct[enc:2*enc])
	copy(reordered[enc:2*enc], ct[0:enc])

	// flip returns a copy of the base ciphertext with one bit toggled.
	flip := func(pos int) []byte {
		c := append([]byte(nil), ct...)
		c[pos] ^= 0x01
		return c
	}

	cases := []struct {
		name    string
		in      []byte                                  // ciphertext fed to Open
		plain   []byte                                  // genuine plaintext for the prefix check; nil → pt
		decfg   func(aesstream.Config) aesstream.Config // decrypt-config override; nil → cfg
		wantErr error                                   // nil → just require some error
	}{
		// Truncated or cut ciphertext.
		{name: "drop final chunk", in: ct[:2*enc], wantErr: aesstream.ErrTruncated},
		{name: "cut final chunk tag", in: ct[:len(ct)-4], wantErr: aesstream.ErrCorrupted},
		{name: "cut mid non-final chunk", in: ct[:enc+50], wantErr: aesstream.ErrCorrupted},
		{name: "empty ciphertext", in: ct[:0], wantErr: aesstream.ErrTruncated},
		{name: "single byte", in: ct[:1], wantErr: aesstream.ErrCorrupted},

		// Reordered chunks.
		{name: "swapped chunks", in: reordered, wantErr: aesstream.ErrCorrupted},

		// Single-bit flips at representative offsets.
		{name: "flip first chunk body", in: flip(0), wantErr: aesstream.ErrCorrupted},
		{name: "flip first chunk tag", in: flip(cs + 4), wantErr: aesstream.ErrCorrupted},
		{name: "flip second chunk", in: flip(enc + 10), wantErr: aesstream.ErrCorrupted},
		{name: "flip final chunk", in: flip(len(ct) - 1), wantErr: aesstream.ErrCorrupted},

		// Trailing data after the final chunk.
		{
			name:    "trailing after full-size final chunk",
			in:      append(append([]byte(nil), fullCT...), 0xde, 0xad),
			plain:   fullPT,
			wantErr: aesstream.ErrTrailingData,
		},
		{
			// Bytes after a short final chunk are pulled into it and break
			// authentication, so this is reported as corruption rather than
			// trailing data — still an error.
			name: "trailing after short final chunk",
			in:   append(append([]byte(nil), ct...), 0xde, 0xad),
		},

		// Correct ciphertext, wrong decryption parameters.
		{
			name: "wrong key",
			in:   ct,
			decfg: func(c aesstream.Config) aesstream.Config {
				c.Key = bytes.Repeat([]byte{0x99}, aesstream.KeySize)
				return c
			},
			wantErr: aesstream.ErrCorrupted,
		},
		{
			name:    "wrong base nonce",
			in:      ct,
			decfg:   func(c aesstream.Config) aesstream.Config { c.BaseNonce = []byte{9, 9, 9, 9, 9, 9, 9}; return c },
			wantErr: aesstream.ErrCorrupted,
		},
		{
			name:    "wrong AAD",
			in:      ct,
			decfg:   func(c aesstream.Config) aesstream.Config { c.AAD = []byte("different"); return c },
			wantErr: aesstream.ErrCorrupted,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dcfg := cfg
			if tc.decfg != nil {
				dcfg = tc.decfg(cfg)
			}
			plain := pt
			if tc.plain != nil {
				plain = tc.plain
			}
			got, err := aesstream.Open(dcfg, tc.in)
			require.Error(t, err)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			}
			// Any bytes released before the error must be a genuine prefix
			// of the plaintext — the Reader never fabricates output.
			require.True(t, bytes.HasPrefix(plain, got), "released bytes are not a plaintext prefix")
		})
	}
}

func TestNilAADRoundTrips(t *testing.T) {
	cfg := baseConfig(aesstream.MinChunkSize)
	cfg.AAD = nil
	pt := pattern(9000)
	ct, err := aesstream.Seal(cfg, pt)
	require.NoError(t, err)
	got, err := aesstream.Open(cfg, ct)
	require.NoError(t, err)
	require.Equal(t, pt, got, "nil-AAD round-trip mismatch")
}

func TestDeterministic(t *testing.T) {
	cfg := baseConfig(aesstream.MinChunkSize)
	pt := pattern(10000)
	a, err := aesstream.Seal(cfg, pt)
	require.NoError(t, err)
	b, err := aesstream.Seal(cfg, pt)
	require.NoError(t, err)
	require.Equal(t, a, b, "Seal is not deterministic for fixed key/nonce/AAD")
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
			_, werr := aesstream.NewWriter(io.Discard, cfg)
			require.ErrorIs(t, werr, tc.wantErr)
			_, rerr := aesstream.NewReader(bytes.NewReader(nil), cfg)
			require.ErrorIs(t, rerr, tc.wantErr)
		})
	}
}

func TestChunkSizeAccessorAndDefault(t *testing.T) {
	w, err := aesstream.NewWriter(io.Discard, baseConfig(0))
	require.NoError(t, err)
	require.Equal(t, aesstream.DefaultChunkSize, w.ChunkSize())

	w2, err := aesstream.NewWriter(io.Discard, baseConfig(aesstream.MinChunkSize))
	require.NoError(t, err)
	require.Equal(t, aesstream.MinChunkSize, w2.ChunkSize())

	r, err := aesstream.NewReader(bytes.NewReader(nil), baseConfig(aesstream.MinChunkSize))
	require.NoError(t, err)
	require.Equal(t, aesstream.MinChunkSize, r.ChunkSize())
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
	require.NoError(t, err)
	// Two chunks' worth forces a flush mid-Write, which hits the failure.
	_, err = w.Write(make([]byte, 2*cs))
	require.ErrorIs(t, err, errWriteBoom)
	_, err = w.Write([]byte("x"))
	require.ErrorIs(t, err, errWriteBoom)
	require.ErrorIs(t, w.Close(), errWriteBoom)
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
	require.NoError(t, err)
	_, err = io.ReadAll(r)
	require.ErrorIs(t, err, errReadBoom)
}

func TestWriteAfterCloseFails(t *testing.T) {
	w, err := aesstream.NewWriter(io.Discard, baseConfig(aesstream.MinChunkSize))
	require.NoError(t, err)
	_, err = w.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close(), "second Close should be a no-op")
	_, err = w.Write([]byte("more"))
	require.Error(t, err, "Write after Close should fail")
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
		require.Equalf(t, tc.want, aesstream.EncryptedSize(int64(tc.pt), tc.chunk),
			"EncryptedSize(%d, %d)", tc.pt, tc.chunk)
		// Cross-check against an actual Seal for small cases.
		if tc.chunk != 0 {
			cfg := baseConfig(tc.chunk)
			ct, err := aesstream.Seal(cfg, pattern(tc.pt))
			require.NoError(t, err)
			require.Equalf(t, tc.want, int64(len(ct)), "Seal len for pt=%d chunk=%d", tc.pt, tc.chunk)
		}
	}
}

func TestNewBaseNonce(t *testing.T) {
	a, err := aesstream.NewBaseNonce()
	require.NoError(t, err)
	require.Len(t, a, aesstream.BaseNonceSize)
	b, err := aesstream.NewBaseNonce()
	require.NoError(t, err)
	require.NotEqual(t, a, b, "two base nonces were identical")
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
	require.NoError(t, err)
	copy(aad, tampered)
	_, err = w.Write(pt)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Decrypting with orig only succeeds if the Writer copied the AAD.
	decCfg := baseConfig(cs)
	decCfg.AAD = []byte(orig)
	got, err := aesstream.Open(decCfg, buf.Bytes())
	require.NoError(t, err, "Open with original AAD failed (Writer did not copy AAD?)")
	require.Equal(t, pt, got)

	// The Reader must be immune to the same post-construction mutation.
	rAAD := []byte(orig)
	rCfg := baseConfig(cs)
	rCfg.AAD = rAAD
	r, err := aesstream.NewReader(bytes.NewReader(buf.Bytes()), rCfg)
	require.NoError(t, err)
	copy(rAAD, tampered)
	got2, err := io.ReadAll(r)
	require.NoError(t, err, "Reader failed after AAD mutation (Reader did not copy AAD?)")
	require.Equal(t, pt, got2)
}

// stuckWriter reports zero progress with no error on every call, which a
// compliant io.Writer never does. writeAll must not spin on it.
type stuckWriter struct{}

func (stuckWriter) Write(p []byte) (int, error) { return 0, nil }

func TestWriteAllShortWriteGuard(t *testing.T) {
	w, err := aesstream.NewWriter(stuckWriter{}, baseConfig(aesstream.MinChunkSize))
	require.NoError(t, err)
	_, err = w.Write([]byte("hello"))
	require.NoError(t, err) // buffered; no flush yet
	// Close flushes the final chunk, which the stuck writer never accepts;
	// writeAll must return ErrShortWrite rather than loop forever.
	require.ErrorIs(t, w.Close(), io.ErrShortWrite)
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
	require.NoError(t, err)

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
		require.NoError(t, rerr, "decrypt")
	}

	res := <-done
	require.NoError(t, res.err, "encrypt")
	require.Equal(t, int64(streamSize), total, "decrypted byte count")
	require.Equal(t, res.sum, outHash.Sum(nil), "decrypted stream hash != plaintext hash")

	delta := int64(maxHeap) - int64(baseline)
	require.LessOrEqualf(t, delta, int64(budget),
		"heap grew by %d bytes (> %d budget) streaming %d bytes: not O(chunk)", delta, budget, streamSize)
}
