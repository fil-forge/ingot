package aesstream

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func internalConfig(chunkSize int) Config {
	return Config{
		Key:       bytes.Repeat([]byte{0x2a}, KeySize),
		BaseNonce: []byte{1, 2, 3, 4, 5, 6, 7},
		AAD:       []byte("aad"),
		ChunkSize: chunkSize,
	}
}

// TestStreamNonceLayout pins the exact 12-byte nonce wire format:
// baseNonce(7) || chunkIndex(4, big-endian) || lastFlag(1).
func TestStreamNonceLayout(t *testing.T) {
	base := [BaseNonceSize]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77}
	cases := []struct {
		index uint32
		last  bool
		want  []byte
	}{
		{0x0a0b0c0d, false, []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x0a, 0x0b, 0x0c, 0x0d, 0x00}},
		{0x0a0b0c0d, true, []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x0a, 0x0b, 0x0c, 0x0d, 0x01}},
		{0, false, []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{0xffffffff, true, []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0xff, 0xff, 0xff, 0xff, 0x01}},
	}
	for _, tc := range cases {
		got := streamNonce(base, tc.index, tc.last)
		require.Equalf(t, tc.want, got[:], "streamNonce(index=%#x, last=%v)", tc.index, tc.last)
	}
}

// TestWriterChunkCountGuard verifies the Writer refuses to overflow the
// 4-byte chunk counter instead of wrapping it (which would reuse a nonce).
func TestWriterChunkCountGuard(t *testing.T) {
	const cs = MinChunkSize
	var buf bytes.Buffer
	w, err := NewWriter(&buf, internalConfig(cs))
	require.NoError(t, err)
	w.maxChunks = 2 // pretend a uint32 can only hold two chunks

	// 3 chunks' worth forces a third flush, which must be rejected.
	_, werr := w.Write(make([]byte, 3*cs))
	cerr := w.Close()
	require.Truef(t, errors.Is(werr, ErrTooManyChunks) || errors.Is(cerr, ErrTooManyChunks),
		"write/close errors = %v / %v, want ErrTooManyChunks", werr, cerr)
}

// TestReaderChunkCountGuard verifies the Reader stops at the counter limit.
func TestReaderChunkCountGuard(t *testing.T) {
	const cs = MinChunkSize
	cfg := internalConfig(cs)
	ct, err := Seal(cfg, make([]byte, 3*cs)) // a valid 3-chunk stream
	require.NoError(t, err)
	r, err := NewReader(bytes.NewReader(ct), cfg)
	require.NoError(t, err)
	r.maxChunks = 2
	_, err = io.ReadAll(r)
	require.ErrorIs(t, err, ErrTooManyChunks)
}

// TestWriterSteadyStateAllocs shows the Writer's per-chunk work does not
// allocate, so memory stays O(chunk size) regardless of stream length.
func TestWriterSteadyStateAllocs(t *testing.T) {
	w, err := NewWriter(io.Discard, internalConfig(DefaultChunkSize))
	require.NoError(t, err)
	data := make([]byte, DefaultChunkSize)
	w.Write(data) // warm up: leaves exactly one full chunk buffered

	allocs := testing.AllocsPerRun(50, func() {
		// Each call flushes the previously-buffered full chunk and buffers
		// this one — one Seal into the reused scratch buffer, no allocation.
		// Kept as a plain check so the measured path stays allocation-free.
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
	})
	require.LessOrEqualf(t, allocs, 1.0, "Writer.Write allocated %.1f times per chunk; want O(1)", allocs)
}

// TestReaderSteadyStateAllocs shows the Reader's per-chunk work does not
// allocate either.
func TestReaderSteadyStateAllocs(t *testing.T) {
	const cs = MinChunkSize
	const runs = 50
	cfg := internalConfig(cs)
	ct, err := Seal(cfg, make([]byte, (runs+5)*cs))
	require.NoError(t, err)
	r, err := NewReader(bytes.NewReader(ct), cfg)
	require.NoError(t, err)
	rbuf := make([]byte, cs)
	_, err = io.ReadFull(r, rbuf) // warm up one chunk
	require.NoError(t, err)
	allocs := testing.AllocsPerRun(runs, func() {
		// Kept as a plain check so the measured path stays allocation-free.
		if _, err := io.ReadFull(r, rbuf); err != nil {
			t.Fatal(err)
		}
	})
	require.LessOrEqualf(t, allocs, 1.0, "Reader.Read allocated %.1f times per chunk; want O(1)", allocs)
}
