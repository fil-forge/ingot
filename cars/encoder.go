// Package cars writes CAR v1 (Content Addressable aRchive) files.
//
// The format is intentionally simple:
//
//	[varint: header_len][DAG-CBOR header bytes]
//	[varint: frame_len][CID bytes][block bytes]
//	[varint: frame_len][CID bytes][block bytes]
//	...
//
// Header is `{ "roots": [<cid>...], "version": 1 }` in DAG-CBOR
// (deterministic key order: by length, then bytewise — "roots" before
// "version").
//
// Each block frame's varint length covers the CID bytes plus the raw
// block bytes that follow.
package cars

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// BlockPosition records where a block's raw payload bytes live within
// an encoded CAR. Offset and Length are measured against the **block
// data**, NOT the frame header or the CID prefix — i.e. they describe
// the slice of the CAR you'd seek to and read from to recover the
// raw block bytes.
//
// This is the convention `blobindex.Position` expects.
type BlockPosition struct {
	CID    cid.Cid
	Offset uint64
	Length uint64
}

// Write encodes a CAR v1 file with the given roots and blocks. Block
// ordering is preserved.
func Write(w io.Writer, roots []cid.Cid, blocks []block.Block) error {
	_, err := WriteWithPositions(w, roots, blocks)
	return err
}

// WriteHeader writes only the CAR v1 header (root array + version)
// and returns the number of bytes written. Used by callers that
// build a CAR incrementally — e.g. an append-only log segment that
// emits one header at open and many block frames over time.
func WriteHeader(w io.Writer, roots []cid.Cid) (int64, error) {
	if len(roots) == 0 {
		return 0, fmt.Errorf("cars: at least one root required")
	}
	cw := &countingWriter{w: w}
	headerBytes, err := encodeHeader(roots)
	if err != nil {
		return 0, fmt.Errorf("cars: encode header: %w", err)
	}
	if err := writeUvarint(cw, uint64(len(headerBytes))); err != nil {
		return cw.n, fmt.Errorf("cars: write header len: %w", err)
	}
	if _, err := cw.Write(headerBytes); err != nil {
		return cw.n, fmt.Errorf("cars: write header: %w", err)
	}
	return cw.n, nil
}

// WriteBlocksAt writes only block frames (no header) at fileOffset
// and returns the absolute byte positions of each block's payload
// within the file. Use this to extend an already-open CAR built by
// WriteHeader. fileOffset must equal the current end-of-file size of
// the underlying writer; positions returned reflect that origin so
// they can be used as ReadAt offsets directly.
func WriteBlocksAt(w io.Writer, fileOffset int64, blocks []block.Block) ([]BlockPosition, error) {
	cw := &countingWriter{w: w, n: fileOffset}
	positions := make([]BlockPosition, 0, len(blocks))
	for i, blk := range blocks {
		pos, err := writeBlock(cw, blk)
		if err != nil {
			return positions, fmt.Errorf("cars: write block %d (%s): %w", i, blk.Cid(), err)
		}
		positions = append(positions, pos)
	}
	return positions, nil
}

// WriteWithPositions is like Write, but additionally returns the byte
// position of each block's payload within the encoded CAR. Used by the
// Forge uploader to build a `blobindex.ShardedDagIndexView` mapping
// inner CIDs to their slices of the outer CAR blob.
func WriteWithPositions(w io.Writer, roots []cid.Cid, blocks []block.Block) ([]BlockPosition, error) {
	if len(roots) == 0 {
		return nil, fmt.Errorf("cars: at least one root required")
	}

	cw := &countingWriter{w: w}

	headerBytes, err := encodeHeader(roots)
	if err != nil {
		return nil, fmt.Errorf("cars: encode header: %w", err)
	}
	if err := writeUvarint(cw, uint64(len(headerBytes))); err != nil {
		return nil, fmt.Errorf("cars: write header len: %w", err)
	}
	if _, err := cw.Write(headerBytes); err != nil {
		return nil, fmt.Errorf("cars: write header: %w", err)
	}

	positions := make([]BlockPosition, 0, len(blocks))
	for i, blk := range blocks {
		pos, err := writeBlock(cw, blk)
		if err != nil {
			return nil, fmt.Errorf("cars: write block %d (%s): %w", i, blk.Cid(), err)
		}
		positions = append(positions, pos)
	}
	return positions, nil
}

func encodeHeader(roots []cid.Cid) ([]byte, error) {
	var buf bytes.Buffer
	cw := cbg.NewCborWriter(&buf)

	if err := cw.WriteMajorTypeHeader(cbg.MajMap, 2); err != nil {
		return nil, err
	}

	if err := writeMapKey(cw, "roots"); err != nil {
		return nil, err
	}
	if err := cw.WriteMajorTypeHeader(cbg.MajArray, uint64(len(roots))); err != nil {
		return nil, err
	}
	for _, c := range roots {
		if err := cbg.WriteCid(cw, c); err != nil {
			return nil, err
		}
	}

	if err := writeMapKey(cw, "version"); err != nil {
		return nil, err
	}
	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, 1); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func writeMapKey(cw *cbg.CborWriter, key string) error {
	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(key))); err != nil {
		return err
	}
	_, err := cw.WriteString(key)
	return err
}

// writeBlock emits one frame and returns the position of the block's
// payload (post-CID-prefix bytes) within the surrounding CAR.
func writeBlock(cw *countingWriter, blk block.Block) (BlockPosition, error) {
	cidBytes := blk.Cid().Bytes()
	data := blk.RawData()
	frameLen := uint64(len(cidBytes) + len(data))

	if err := writeUvarint(cw, frameLen); err != nil {
		return BlockPosition{}, err
	}
	if _, err := cw.Write(cidBytes); err != nil {
		return BlockPosition{}, err
	}

	dataOffset := cw.n
	if _, err := cw.Write(data); err != nil {
		return BlockPosition{}, err
	}
	return BlockPosition{
		CID:    blk.Cid(),
		Offset: uint64(dataOffset),
		Length: uint64(len(data)),
	}, nil
}

func writeUvarint(w io.Writer, n uint64) error {
	var buf [binary.MaxVarintLen64]byte
	sz := binary.PutUvarint(buf[:], n)
	_, err := w.Write(buf[:sz])
	return err
}

// countingWriter forwards writes to an underlying io.Writer while
// tracking the total number of bytes written. Used to compute block
// payload offsets for the index.
type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}
