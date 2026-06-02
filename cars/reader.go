package cars

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// ErrTorn is returned by ScanFile when the trailing bytes of the CAR
// look like an incomplete frame (truncated varint, mismatched frame
// length, or short read on payload). Callers can use the LastGoodEnd
// field of the returned ScanResult to truncate the file back to the
// last fully-fsynced batch boundary.
var ErrTorn = errors.New("cars: torn trailing frame")

// Frame is one block read from a CAR file along with its on-disk
// position. Offset/Length describe the payload bytes (post-CID
// prefix), matching the convention used by BlockPosition / Write.
type Frame struct {
	Block  block.Block
	Offset uint64
	Length uint64
}

// ScanResult is the outcome of ScanFile.
type ScanResult struct {
	// Frames are every block read in file order.
	Frames []Frame
	// LastGoodEnd is the byte offset just past the last fully-read
	// frame. If the file is intact, equals the file size; if a torn
	// frame was detected, equals the start of that torn frame so
	// callers can truncate to it.
	LastGoodEnd int64
	// HeaderEnd is the byte offset just past the CAR v1 header (i.e.,
	// the offset of the first frame).
	HeaderEnd int64
}

// ScanFile reads a CAR v1 file from path and returns every fully
// readable block + its on-disk position. If the file ends in a torn
// frame, ScanFile returns the frames it could read along with
// ErrTorn and LastGoodEnd pointing at the start of the torn frame.
//
// This is the recovery primitive: callers can `os.Truncate(path,
// LastGoodEnd)` to drop a torn tail, then re-derive the in-memory
// index from Frames.
func ScanFile(path string) (*ScanResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cars: open %s: %w", path, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("cars: stat %s: %w", path, err)
	}
	size := st.Size()

	br := bufio.NewReader(f)
	headerLen, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("cars: read header len: %w", err)
	}
	headerVarintBytes := uvarintLen(headerLen)
	if _, err := br.Discard(int(headerLen)); err != nil {
		return nil, fmt.Errorf("cars: skip header: %w", err)
	}
	headerEnd := int64(headerVarintBytes) + int64(headerLen)

	res := &ScanResult{HeaderEnd: headerEnd, LastGoodEnd: headerEnd}
	pos := headerEnd

	for pos < size {
		frameStart := pos
		frameLen, varSize, terr := readUvarint(br)
		if terr != nil {
			if errors.Is(terr, io.EOF) || errors.Is(terr, io.ErrUnexpectedEOF) {
				return res, fmt.Errorf("%w at offset %d", ErrTorn, frameStart)
			}
			return nil, fmt.Errorf("cars: read frame len at %d: %w", frameStart, terr)
		}
		// Bound check: frame must fit in remaining bytes.
		if int64(frameLen)+int64(varSize)+frameStart > size {
			res.LastGoodEnd = frameStart
			return res, fmt.Errorf("%w at offset %d (frame len %d exceeds file)", ErrTorn, frameStart, frameLen)
		}

		// Read frame body: CID prefix + block bytes.
		body := make([]byte, frameLen)
		if _, err := io.ReadFull(br, body); err != nil {
			res.LastGoodEnd = frameStart
			return res, fmt.Errorf("%w at offset %d: %w", ErrTorn, frameStart, err)
		}
		c, cidLen, err := cidFromBytes(body)
		if err != nil {
			return nil, fmt.Errorf("cars: parse cid at offset %d: %w", frameStart, err)
		}
		payload := body[cidLen:]
		blk, err := block.NewBlockWithCid(payload, c)
		if err != nil {
			return nil, fmt.Errorf("cars: new block at offset %d: %w", frameStart, err)
		}

		dataOffset := uint64(frameStart) + uint64(varSize) + uint64(cidLen)
		res.Frames = append(res.Frames, Frame{
			Block:  blk,
			Offset: dataOffset,
			Length: uint64(len(payload)),
		})
		pos = frameStart + int64(varSize) + int64(frameLen)
		res.LastGoodEnd = pos
	}
	return res, nil
}

// uvarintLen returns the encoded byte length of n.
func uvarintLen(n uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], n)
}

// readUvarint pulls a varint from br and reports how many bytes it
// consumed. Wraps the bufio reader's ReadByte so we can count.
func readUvarint(br *bufio.Reader) (uint64, int, error) {
	var (
		x uint64
		s uint
		n int
	)
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, n, err
		}
		n++
		if b < 0x80 {
			if n > binary.MaxVarintLen64 || (n == binary.MaxVarintLen64 && b > 1) {
				return 0, n, fmt.Errorf("cars: uvarint overflow")
			}
			return x | uint64(b)<<s, n, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}

// cidFromBytes parses a binary CID from the head of buf and returns
// the CID and the number of bytes consumed.
func cidFromBytes(buf []byte) (cid.Cid, int, error) {
	n, c, err := cid.CidFromBytes(buf)
	if err != nil {
		return cid.Cid{}, 0, err
	}
	return c, n, nil
}
