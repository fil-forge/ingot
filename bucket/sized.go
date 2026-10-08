package bucket

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/multiformats/go-multihash"
)

// ErrBodyShort and ErrBodyLong report a body whose length differs from the one
// declared for it.
var (
	ErrBodyShort = errors.New("bucket: body is shorter than its declared length")
	ErrBodyLong  = errors.New("bucket: body is longer than its declared length")
)

// SizedBlobWriter writes a blob whose length is known before its first byte,
// which is what lets it start sending the blob somewhere before it has read
// it all. r yields exactly n bytes and then EOF, or fails.
type SizedBlobWriter interface {
	WriteSizedBlob(ctx context.Context, r io.Reader, n int64) (multihash.Multihash, error)
}

// SplitSizedBody is SplitBody for a body whose length is declared up front: it
// splits size bytes of r into blobs of min(maxBlobSize, remaining) bytes, so
// each blob's length is known when w starts writing it. A body that ends early
// fails with ErrBodyShort, and one with bytes past size fails with ErrBodyLong.
// The check for trailing bytes happens inside the last blob's stream, before
// it reports EOF, so a writer that is sending the blob elsewhere learns of the
// failure before the send completes; the same goes for an error r itself only
// reports at its end, such as a failed checksum.
//
// The Body it returns is the one SplitBody would return for the same bytes and
// options.
func SplitSizedBody(ctx context.Context, w SizedBlobWriter, r io.Reader, size, maxBlobSize int64, opts ...SplitOption) (Body, error) {
	max := maxBlobSize
	if max <= 0 {
		max = DefaultMaxBlobSize
	}
	if size < 0 {
		return Body{}, fmt.Errorf("bucket: negative body size %d", size)
	}
	cfg := splitConfig{md5: true, tree: true}
	for _, o := range opts {
		o(&cfg)
	}

	hashes := newBodyHashes(cfg)
	defer hashes.stop()
	src := io.TeeReader(r, hashes.writer())

	var blobs []BlobRef
	var total int64
	for total < size {
		n := min(max, size-total)
		br := &exactReader{src: src, remaining: n, last: total+n == size}
		digest, err := w.WriteSizedBlob(ctx, br, n)
		if err != nil {
			return Body{}, fmt.Errorf("put blob: %w", err)
		}
		if br.remaining > 0 {
			return Body{}, fmt.Errorf("put blob: writer stopped %d bytes short of the blob", br.remaining)
		}
		blobs = append(blobs, BlobRef{Digest: digest, Start: total, End: total + n - 1})
		total += n
	}
	if size == 0 {
		// No blob carried the end-of-body check.
		if err := expectEOF(src); err != nil {
			return Body{}, err
		}
	}

	return hashes.body(total, blobs)
}

// exactReader yields exactly remaining bytes of src. The body's last blob also
// checks that src ends there, before reporting EOF.
type exactReader struct {
	src       io.Reader
	remaining int64
	last      bool
	done      bool // src's end has been seen, or checked
}

func (e *exactReader) Read(p []byte) (int, error) {
	if e.remaining == 0 {
		if e.last && !e.done {
			if err := expectEOF(e.src); err != nil {
				return 0, err
			}
			e.done = true
		}
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > e.remaining {
		p = p[:e.remaining]
	}
	n, err := e.src.Read(p)
	e.remaining -= int64(n)
	switch {
	case errors.Is(err, io.EOF):
		if e.remaining > 0 {
			return n, ErrBodyShort
		}
		e.done = true
		return n, nil
	case err != nil:
		return n, err
	}
	return n, nil
}

// expectEOF checks that src has nothing left, surfacing any error src reports
// at its end.
func expectEOF(src io.Reader) error {
	var b [1]byte
	for range 100 {
		n, err := src.Read(b[:])
		if n > 0 {
			return ErrBodyLong
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return io.ErrNoProgress
}
