package bucket

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"io"
	"path/filepath"
	"testing"

	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
)

func testSpool(t *testing.T) *blockstore.Spool {
	t.Helper()
	s, err := blockstore.NewSpool(filepath.Join(t.TempDir(), "spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	return s
}

func makeData(n int) []byte {
	d := make([]byte, n)
	for i := range d {
		d[i] = byte(i*31 + 7)
	}
	return d
}

// TestSplitBody_StreamingRoundTrip splits a multi-blob body through the spool and
// reads it back, asserting the blob boundaries, the whole-body digests, and a
// byte-exact round trip — all via the streaming WriteBlob/OpenBlob path.
func TestSplitBody_StreamingRoundTrip(t *testing.T) {
	ctx := context.Background()
	sp := testSpool(t)
	const max = int64(4096)
	data := makeData(10000) // → blobs of 4096, 4096, 1808

	body, err := SplitBody(ctx, sp, bytes.NewReader(data), max)
	if err != nil {
		t.Fatalf("SplitBody: %v", err)
	}

	if body.Size != int64(len(data)) {
		t.Fatalf("Size = %d, want %d", body.Size, len(data))
	}
	wantSHA := sha256.Sum256(data)
	if !bytes.Equal(body.SHA256, wantSHA[:]) {
		t.Errorf("whole-body SHA256 mismatch")
	}
	wantMD5 := md5.Sum(data)
	if !bytes.Equal(body.MD5, wantMD5[:]) {
		t.Errorf("whole-body MD5 mismatch")
	}

	wantBounds := []struct{ off, length int64 }{{0, 4096}, {4096, 4096}, {8192, 1808}}
	if len(body.Blobs) != len(wantBounds) {
		t.Fatalf("got %d blobs, want %d", len(body.Blobs), len(wantBounds))
	}
	for i, w := range wantBounds {
		b := body.Blobs[i]
		if b.Offset != w.off || b.Length != w.length {
			t.Errorf("blob %d = {off:%d len:%d}, want {off:%d len:%d}", i, b.Offset, b.Length, w.off, w.length)
		}
		// The recorded digest must be the sha256 multihash of exactly that slice.
		want, _ := mh.Sum(data[w.off:w.off+w.length], mh.SHA2_256, -1)
		if !bytes.Equal(b.Digest, want) {
			t.Errorf("blob %d digest mismatch", i)
		}
	}

	got, err := io.ReadAll(OpenBody(ctx, sp, body))
	if err != nil {
		t.Fatalf("OpenBody read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

// TestOpenBodyRange covers ranged reads that start mid-blob and span blob
// boundaries — exercising the seek-into-blob path of the streaming reader.
func TestOpenBodyRange(t *testing.T) {
	ctx := context.Background()
	sp := testSpool(t)
	const max = int64(4096)
	data := makeData(10000)
	body, err := SplitBody(ctx, sp, bytes.NewReader(data), max)
	if err != nil {
		t.Fatalf("SplitBody: %v", err)
	}

	cases := []struct{ start, end int64 }{
		{0, 9999},    // whole object
		{0, 0},       // first byte
		{4095, 4096}, // straddles the first/second blob boundary
		{5000, 6000}, // wholly inside the second blob (mid-blob start)
		{8192, 9999}, // the whole last (short) blob
		{9999, 9999}, // last byte
		{100, 8500},  // spans all three blobs, mid-blob start
	}
	for _, c := range cases {
		got, err := io.ReadAll(OpenBodyRange(ctx, sp, body, c.start, c.end))
		if err != nil {
			t.Fatalf("range [%d,%d]: %v", c.start, c.end, err)
		}
		want := data[c.start : c.end+1]
		if !bytes.Equal(got, want) {
			t.Errorf("range [%d,%d]: got %d bytes, want %d (mismatch)", c.start, c.end, len(got), len(want))
		}
	}
}

// TestSplitBody_Empty: a zero-byte body yields no blobs and the empty digests.
func TestSplitBody_Empty(t *testing.T) {
	ctx := context.Background()
	sp := testSpool(t)
	body, err := SplitBody(ctx, sp, bytes.NewReader(nil), 4096)
	if err != nil {
		t.Fatalf("SplitBody: %v", err)
	}
	if body.Size != 0 || len(body.Blobs) != 0 {
		t.Fatalf("empty body: Size=%d Blobs=%d, want 0/0", body.Size, len(body.Blobs))
	}
	emptySHA := sha256.Sum256(nil)
	if !bytes.Equal(body.SHA256, emptySHA[:]) {
		t.Errorf("empty SHA256 mismatch")
	}
}
