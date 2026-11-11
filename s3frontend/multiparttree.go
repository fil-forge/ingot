package s3frontend

import (
	"github.com/fil-forge/ingot/blake3tree"
	"github.com/fil-forge/ingot/registry"
)

// assumedPartOffset is the object byte offset UploadPart hashes a part's
// tree at. The server learns a part's true offset only once every
// lower-numbered part exists, so:
//
//   - part 1 is at 0;
//   - when every lower part is recorded, the offset is their sizes' sum
//     (exact);
//   - when some lower part is recorded, parts are assumed uniform and the
//     lowest recorded part's size stands in for each missing one;
//   - otherwise the part's own size does.
//
// Clients upload parts of one size in parallel with a shorter final part,
// so the assumed offset is the true one for every part of such an upload
// except a final part that lands before any of its predecessors. Complete
// checks each assumed offset and re-hashes the parts whose assumption was
// wrong. prior is the session's recorded parts; a part being superseded is
// ignored.
func assumedPartOffset(prior []registry.MultipartPart, partNumber int, size int64) int64 {
	if partNumber == 1 {
		return 0
	}
	var sum, unit int64
	below, lowest := 0, 0
	for _, p := range prior {
		if p.PartNumber >= partNumber {
			continue
		}
		sum += p.Size
		below++
		if lowest == 0 || p.PartNumber < lowest {
			lowest, unit = p.PartNumber, p.Size
		}
	}
	switch {
	case below == partNumber-1:
		return sum
	case below > 0:
		return int64(partNumber-1) * unit
	default:
		return int64(partNumber-1) * size
	}
}

// partTreeOf serializes a part's finished tree for its record.
func partTreeOf(rng blake3tree.Range) *registry.PartTree {
	t := &registry.PartTree{
		Offset:   rng.Offset,
		ChunkLog: rng.ChunkLog,
		Nodes:    blake3tree.EncodeSubtrees(rng.Subtrees),
		Blocks:   blake3tree.EncodeSubtrees(rng.BlockSubtrees()),
		Head:     append([]byte(nil), rng.Head...),
		Tail:     append([]byte(nil), rng.Tail...),
	}
	if rng.HasRoot {
		t.Root = append([]byte(nil), rng.Root[:]...)
	}
	return t
}
