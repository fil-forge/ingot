package aesstream

import (
	"errors"
	"testing"
)

// TestChunkLayout pins the chunk-geometry math for the boundary lengths
// the public range API depends on.
func TestChunkLayout(t *testing.T) {
	const cs = MinChunkSize
	enc := int64(cs) + TagSize

	cases := []struct {
		name                                   string
		ciphertextSize                         int64
		wantChunks, wantLastLen, wantPlaintext int64
	}{
		{"empty stream (one tag-only chunk)", TagSize, 1, TagSize, 0},
		{"single partial chunk", TagSize + 100, 1, TagSize + 100, 100},
		{"single full chunk", enc, 1, enc, cs},
		{"full + empty final", enc + TagSize, 2, TagSize, cs},
		{"full + partial final", enc + TagSize + 7, 2, TagSize + 7, cs + 7},
		{"two full chunks", 2 * enc, 2, enc, 2 * cs},
	}
	for _, c := range cases {
		n, last, plain, err := chunkLayout(c.ciphertextSize, cs)
		if err != nil {
			t.Errorf("%s: chunkLayout(%d): %v", c.name, c.ciphertextSize, err)
			continue
		}
		if n != c.wantChunks || last != c.wantLastLen || plain != c.wantPlaintext {
			t.Errorf("%s: chunkLayout(%d) = (%d,%d,%d), want (%d,%d,%d)",
				c.name, c.ciphertextSize, n, last, plain, c.wantChunks, c.wantLastLen, c.wantPlaintext)
		}
	}
}

// TestChunkLayoutTooManyChunks verifies the MaxChunks guard, which the
// public API cannot reach without an impractically large ciphertext.
func TestChunkLayoutTooManyChunks(t *testing.T) {
	const cs = MinChunkSize
	enc := int64(cs) + TagSize
	// One chunk past the uint32 counter's capacity.
	size := (int64(MaxChunks) + 1) * enc
	if _, _, _, err := chunkLayout(size, cs); !errors.Is(err, ErrTooManyChunks) {
		t.Fatalf("chunkLayout(%d) = %v, want ErrTooManyChunks", size, err)
	}
}
