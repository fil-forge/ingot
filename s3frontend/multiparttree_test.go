package s3frontend

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"

	"github.com/fil-forge/ingot/blake3tree"
	"github.com/fil-forge/ingot/queue/multipartqueue"
	"github.com/fil-forge/ingot/registry"
)

// wantCID is the x-cid a body must produce.
func wantCID(t *testing.T, data []byte) string {
	t.Helper()
	sum := blake3.Sum256(data)
	digest, err := mh.Encode(sum[:], mh.BLAKE3)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, digest).String()
}

// assertObjectTree checks a completed object's x-cid against its bytes and
// its stored tree against the single-pass reference: chunk log, block count and
// every block.
func assertObjectTree(t *testing.T, b *Backend, key string, data []byte) {
	t.Helper()
	bucket := "bk"
	rc := newRequestCtx()
	_, err := b.HeadObject(rc, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "HeadObject")
	require.Equal(t, wantCID(t, data), string(rc.Response.Header.Peek(cidHeader)), "x-cid")
	rv, err := b.resolveVersion(context.Background(), bucket, key, "")
	require.NoError(t, err)
	h, _ := blake3tree.NewHasher(0)
	h.Write(data)
	want := h.FinishObject()
	body := rv.mf.Body
	require.Equal(t, want.ChunkLog, body.TreeChunkLog, "TreeChunkLog")
	blocks, err := body.TreeBlockCVs()
	require.NoError(t, err)
	require.Len(t, blocks, len(want.Blocks), "%d blocks", len(blocks))
	for i := range blocks {
		require.Equal(t, want.Blocks[i], blocks[i], "block %d differs from the single-pass tree", i)
	}
}

// completeAll uploads parts in the given order and completes the upload.
func completeAll(t *testing.T, b *Backend, key string, parts [][]byte, order []int32) []byte {
	t.Helper()
	uploadID, completed, whole := uploadAll(t, b, key, parts, order)
	completeUpload(t, b, key, uploadID, completed)
	return whole
}

// uploadAll creates an upload for key and sends parts in order, returning
// the upload id, the completed-part list, and the whole body.
func uploadAll(t *testing.T, b *Backend, key string, parts [][]byte, order []int32) (string, []types.CompletedPart, []byte) {
	t.Helper()
	uploadID := mpCreate(t, b, key, "", "")
	etags := make([]*string, len(parts))
	for _, pn := range order {
		// versitygw always passes the declared length; the assumed offset of a
		// part with no recorded predecessor is made from it.
		n := int64(len(parts[pn-1]))
		out, err := mpUploadPart(t, b, key, uploadID, pn, parts[pn-1], func(in *s3.UploadPartInput) { in.ContentLength = &n })
		require.NoError(t, err, "UploadPart %d", pn)
		etags[pn-1] = out.ETag
	}
	var completed []types.CompletedPart
	var whole []byte
	for i := range parts {
		pn := int32(i + 1)
		completed = append(completed, types.CompletedPart{PartNumber: &pn, ETag: etags[i]})
		whole = append(whole, parts[i]...)
	}
	return uploadID, completed, whole
}

// completeUpload completes the upload and checks that Complete dropped the
// parts' tree rows: the part records stay for the re-Complete replay, the
// trees are dead once the manifest holds the object's.
func completeUpload(t *testing.T, b *Backend, key, uploadID string, completed []types.CompletedPart) {
	t.Helper()
	_, err := mpComplete(t, b, key, uploadID, completed, nil)
	require.NoError(t, err, "Complete")
	recs, err := b.multipart.ListParts(context.Background(), uploadID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(recs), len(completed), "%d part records retained after Complete", len(recs))
	trees, err := b.multipart.ListPartTrees(context.Background(), uploadID)
	require.NoError(t, err, "ListPartTrees")
	require.Empty(t, trees, "part trees kept after Complete")
}

// recordedParts returns the open upload's recorded part trees by part
// number, having checked that it holds parts part records and that each
// tree's blocks are at the block size of the body up to the part's end.
func recordedParts(t *testing.T, b *Backend, uploadID string, parts int) map[int]*registry.PartTree {
	t.Helper()
	recs, err := b.multipart.ListParts(context.Background(), uploadID)
	require.NoError(t, err)
	require.Len(t, recs, parts, "%d part records for %s", len(recs), uploadID)
	trees, err := b.multipart.ListPartTrees(context.Background(), uploadID)
	require.NoError(t, err)
	for _, p := range recs {
		tree := trees[p.PartNumber]
		if tree == nil {
			continue
		}
		want := blake3tree.ChunkLog(tree.Offset + p.Size)
		require.Equal(t, want, tree.ChunkLog, "part %d at offset %d: tree blocks at chunk log %d", p.PartNumber, tree.Offset, tree.ChunkLog)
	}
	return trees
}

// TestMultipartTree covers the paths Complete takes to the object's tree:
// uniform parts in order (every record used), a short final part arriving
// first (its assumed offset is wrong, so it alone is re-hashed), a non-final
// part of odd length (the body from it on is re-hashed as one range), a
// single part (the digest is the part's own root), and a part copied from
// another object.
func TestMultipartTree(t *testing.T) {
	// Blobs of 1 MiB split each 5 MiB part into five.
	b, _, _ := newRefTestBackend(t, 1<<20)
	minPart := int(backend.MinPartSize)

	t.Run("in order", func(t *testing.T) {
		parts := [][]byte{taggedBody(minPart, 0x01), taggedBody(minPart, 0x02), taggedBody(70_000, 0x03)}
		uploadID, completed, whole := uploadAll(t, b, "in-order", parts, []int32{1, 2, 3})
		recs := recordedParts(t, b, uploadID, 3)
		require.Equal(t, [3]int64{0, int64(minPart), 2 * int64(minPart)}, [3]int64{recs[1].Offset, recs[2].Offset, recs[3].Offset},
			"recorded offsets: every assumed offset should have been exact")
		completeUpload(t, b, "in-order", uploadID, completed)
		assertObjectTree(t, b, "in-order", whole)
	})

	t.Run("final part first", func(t *testing.T) {
		// The short final part lands first: with nothing recorded its own
		// size stands in, the assumed offset is wrong, and Complete re-hashes it.
		const final = 70 << 10
		parts := [][]byte{taggedBody(minPart, 0x11), taggedBody(minPart, 0x12), taggedBody(final, 0x13)}
		uploadID, completed, whole := uploadAll(t, b, "final-first", parts, []int32{3, 1, 2})
		recs := recordedParts(t, b, uploadID, 3)
		require.NotNil(t, recs[3], "part 3 recorded no tree")
		require.Equal(t, int64(2*final), recs[3].Offset, "part 3 offset: with no earlier part its own size should have stood in")
		require.Equal(t, int64(minPart), recs[2].Offset, "part 2 offset: part 1 was recorded, so the assumed offset should be exact")
		completeUpload(t, b, "final-first", uploadID, completed)
		assertObjectTree(t, b, "final-first", whole)
	})

	t.Run("final part first with an unaligned assumed offset", func(t *testing.T) {
		// A final part whose size is not a whole number of chunks assumes an
		// offset inside a chunk; the tree is recorded with head bytes, the
		// offset check finds it wrong, and the part alone is re-hashed.
		parts := [][]byte{taggedBody(minPart, 0x61), taggedBody(minPart, 0x62), taggedBody(70_000, 0x63)}
		uploadID, completed, whole := uploadAll(t, b, "final-first-odd", parts, []int32{3, 1, 2})
		recs := recordedParts(t, b, uploadID, 3)
		require.NotNil(t, recs[3], "part 3 recorded no tree")
		require.Equal(t, int64(2*70_000), recs[3].Offset, "part 3 recorded as %+v", recs[3])
		require.Len(t, recs[3].Head, 1024-(2*70_000)%1024, "part 3 recorded as %+v", recs[3])
		completeUpload(t, b, "final-first-odd", uploadID, completed)
		assertObjectTree(t, b, "final-first-odd", whole)
	})

	t.Run("middle part assumed from a sibling", func(t *testing.T) {
		parts := [][]byte{taggedBody(minPart, 0x21), taggedBody(minPart, 0x22), taggedBody(minPart, 0x23), taggedBody(1024, 0x24)}
		uploadID, completed, whole := uploadAll(t, b, "sibling", parts, []int32{1, 3, 4, 2})
		recs := recordedParts(t, b, uploadID, 4)
		require.Equal(t, [2]int64{2 * int64(minPart), 3 * int64(minPart)}, [2]int64{recs[3].Offset, recs[4].Offset},
			"recorded offsets: parts 3 and 4 should have been assumed from part 1's size")
		completeUpload(t, b, "sibling", uploadID, completed)
		assertObjectTree(t, b, "sibling", whole)
	})

	t.Run("odd length parts", func(t *testing.T) {
		// Part lengths that are not chunk multiples (the AWS SDK for Go above
		// its part limit) put chunk boundaries inside parts. The records
		// carry the bytes either side, every assumed offset is exact for a
		// sequential upload, and nothing is re-hashed.
		parts := [][]byte{taggedBody(minPart+100, 0x31), taggedBody(minPart+100, 0x32), taggedBody(3000, 0x33)}
		uploadID, completed, whole := uploadAll(t, b, "odd", parts, []int32{1, 2, 3})
		recs := recordedParts(t, b, uploadID, 3)
		for n := 1; n <= 3; n++ {
			require.NotNil(t, recs[n], "part %d recorded no tree", n)
		}
		require.Equal(t, [3]int64{0, int64(minPart + 100), 2 * int64(minPart+100)}, [3]int64{recs[1].Offset, recs[2].Offset, recs[3].Offset},
			"assumed offsets are not exact")
		require.Len(t, recs[1].Tail, 100, "part 1 tail")
		require.Len(t, recs[2].Head, 1024-100, "part 2 head")
		completeUpload(t, b, "odd", uploadID, completed)
		assertObjectTree(t, b, "odd", whole)
	})

	t.Run("final part is re-hashed regardless of the budget", func(t *testing.T) {
		// The final part landing first is the straggler the design expects;
		// it is covered even with no budget at all.
		limits := storeFor(b).limits
		saved := limits.RehashBudget
		limits.RehashBudget = 0
		defer func() { limits.RehashBudget = saved }()
		parts := [][]byte{taggedBody(minPart, 0x91), taggedBody(minPart, 0x92), taggedBody(70<<10, 0x93)}
		whole := completeAll(t, b, "final-exempt", parts, []int32{3, 1, 2})
		assertObjectTree(t, b, "final-exempt", whole)
	})

	t.Run("re-hash over budget refuses the completion", func(t *testing.T) {
		// Variable sizes uploaded in reverse: part 2 (non-final) is assumed
		// wrongly, and with a budget below its size Complete is refused as
		// InvalidPartLayout before any part is accepted or its version id
		// reported. The upload stays open, and the same Complete within the
		// default budget commits the object with its digest.
		limits := storeFor(b).limits
		saved := limits.RehashBudget
		limits.RehashBudget = 1
		defer func() { limits.RehashBudget = saved }()
		parts := [][]byte{taggedBody(minPart+4096, 0x81), taggedBody(minPart, 0x82), taggedBody(70<<10, 0x83)}
		bucket, key := "bk", "over-budget"
		uploadID := mpCreate(t, b, key, "", "")
		etags := make([]*string, len(parts))
		for _, pn := range []int32{3, 2, 1} {
			n := int64(len(parts[pn-1]))
			out, err := mpUploadPart(t, b, key, uploadID, pn, parts[pn-1], func(in *s3.UploadPartInput) { in.ContentLength = &n })
			require.NoError(t, err, "UploadPart %d", pn)
			etags[pn-1] = out.ETag
		}
		var completed []types.CompletedPart
		var whole []byte
		for i := range parts {
			pn := int32(i + 1)
			completed = append(completed, types.CompletedPart{PartNumber: &pn, ETag: etags[i]})
			whole = append(whole, parts[i]...)
		}
		reported, _, err := completeReporting(t, b, key, uploadID, completed, func(string) {})
		var apiErr s3err.APIError
		require.ErrorAs(t, err, &apiErr, "Complete over budget: err = %v, want InvalidPartLayout", err)
		require.Equal(t, "InvalidPartLayout", apiErr.Code, "Complete over budget: err")
		require.Equal(t, http.StatusBadRequest, apiErr.HTTPStatusCode, "Complete over budget: err")
		// Refused before it reports, so a streamed response is still
		// uncommitted and the refusal keeps its 400.
		require.Empty(t, reported, "Complete over budget reported %q before refusing", reported)
		require.Contains(t, apiErr.Description, strconv.Itoa(minPart), "refusal names %q, want the %d bytes to re-hash", apiErr.Description, minPart)
		_, err = b.HeadObject(newRequestCtx(), &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
		require.Error(t, err, "object exists after a refused Complete")
		// A refusal keeps the parts' trees, so the retry merges them.
		trees := recordedParts(t, b, uploadID, 3)
		require.Len(t, trees, 3, "%d part trees after a refused Complete", len(trees))
		limits.RehashBudget = multipartqueue.DefaultRehashBudget
		_, err = mpComplete(t, b, key, uploadID, completed, nil)
		require.NoError(t, err, "Complete within budget after the refusal")
		assertObjectTree(t, b, key, whole)
	})

	t.Run("duplicate Complete replays after the trees are gone", func(t *testing.T) {
		// Complete deletes the parts' trees, so a duplicate request sees
		// none. It must replay the committed ETag, never weigh the missing
		// trees against the budget: with no budget at all, the two
		// non-final parts would otherwise count as a re-hash of the whole
		// object.
		parts := [][]byte{taggedBody(minPart, 0xa1), taggedBody(minPart, 0xa2), taggedBody(70<<10, 0xa3)}
		key := "replay"
		uploadID, completed, whole := uploadAll(t, b, key, parts, []int32{1, 2, 3})
		first, err := mpComplete(t, b, key, uploadID, completed, nil)
		require.NoError(t, err, "Complete")
		limits := storeFor(b).limits
		saved := limits.RehashBudget
		limits.RehashBudget = 0
		defer func() { limits.RehashBudget = saved }()
		again, err := mpComplete(t, b, key, uploadID, completed, nil)
		require.NoError(t, err, "duplicate Complete")
		require.Equal(t, *first.ETag, *again.ETag, "duplicate Complete ETag")
		assertObjectTree(t, b, key, whole)
	})

	t.Run("single part", func(t *testing.T) {
		for _, n := range []int{0, 1, 1024, 3000, 100_000} {
			key := "single"
			parts := [][]byte{taggedBody(n, 0x41)}
			whole := completeAll(t, b, key, parts, []int32{1})
			assertObjectTree(t, b, key, whole)
		}
	})

	t.Run("single part that is not part 1", func(t *testing.T) {
		// S3 requires the completed parts to ascend, not to start at 1. A
		// lone part 2 was hashed at an assumed offset of one part, so the
		// offset check re-hashes it at 0 and that re-hash supplies the root.
		key := "lone-part-2"
		uploadID := mpCreate(t, b, key, "", "")
		n1, n2 := int64(minPart), int64(70<<10)
		_, err := mpUploadPart(t, b, key, uploadID, 1, taggedBody(minPart, 0x71), func(in *s3.UploadPartInput) { in.ContentLength = &n1 })
		require.NoError(t, err)
		part2 := taggedBody(int(n2), 0x72)
		p2, err := mpUploadPart(t, b, key, uploadID, 2, part2, func(in *s3.UploadPartInput) { in.ContentLength = &n2 })
		require.NoError(t, err)
		two := int32(2)
		_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &two, ETag: p2.ETag}}, nil)
		require.NoError(t, err, "Complete")
		assertObjectTree(t, b, key, part2)
	})

	t.Run("copied part", func(t *testing.T) {
		src := taggedBody(minPart+4096, 0x51)
		putObj(t, b, "copy-src", src)
		key := "copied"
		uploadID := mpCreate(t, b, key, "", "")
		p1, err := mpUploadPart(t, b, key, uploadID, 1, taggedBody(minPart, 0x52), nil)
		require.NoError(t, err)
		n := int32(2)
		cp, err := b.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
			Bucket: strPtr("bk"), Key: &key, UploadId: &uploadID, PartNumber: &n, CopySource: strPtr("bk/copy-src"),
		})
		require.NoError(t, err, "UploadPartCopy")
		one, two := int32(1), int32(2)
		_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{
			{PartNumber: &one, ETag: p1.ETag}, {PartNumber: &two, ETag: cp.ETag},
		}, nil)
		require.NoError(t, err, "Complete")
		whole := append(bytes.Clone(taggedBody(minPart, 0x52)), src...)
		assertObjectTree(t, b, key, whole)
	})
}

func TestAssumedPartOffset(t *testing.T) {
	part := func(n int, size int64) registryPart { return registryPart{n, size} }
	cases := []struct {
		name  string
		prior []registryPart
		num   int
		size  int64
		want  int64
	}{
		{"part 1", nil, 1, 500, 0},
		{"no earlier part", nil, 3, 500, 1000},
		{"all earlier parts", []registryPart{part(1, 100), part(2, 200)}, 3, 50, 300},
		{"some earlier parts", []registryPart{part(1, 100), part(3, 100)}, 4, 50, 300},
		{"lowest recorded sets the unit", []registryPart{part(2, 100), part(3, 100)}, 5, 50, 400},
		{"superseded same number ignored", []registryPart{part(1, 100), part(2, 999)}, 2, 50, 100},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := assumedPartOffset(toParts(c.prior), c.num, c.size)
			require.Equal(t, c.want, got, "assumedPartOffset")
		})
	}
}

type registryPart struct {
	num  int
	size int64
}

func toParts(ps []registryPart) []registry.MultipartPart {
	out := make([]registry.MultipartPart, len(ps))
	for i, p := range ps {
		out[i] = registry.MultipartPart{PartNumber: p.num, Size: p.size}
	}
	return out
}
