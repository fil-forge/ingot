package s3frontend

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/valyala/fasthttp"
	"lukechampine.com/blake3"

	"github.com/fil-forge/ingot/blake3tree"
	"github.com/fil-forge/ingot/registry"
)

// wantCID is the x-cid a body must produce.
func wantCID(t *testing.T, data []byte) string {
	t.Helper()
	sum := blake3.Sum256(data)
	digest, err := mh.Encode(sum[:], mh.BLAKE3)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, digest).String()
}

// assertObjectTree checks a completed object's x-cid against its bytes and
// its stored tree against the single-pass reference: chunk log, block count and
// every block.
func assertObjectTree(t *testing.T, b *Backend, key string, data []byte) {
	t.Helper()
	bucket := "bk"
	rc := &fasthttp.RequestCtx{}
	if _, err := b.HeadObject(rc, &s3.HeadObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if got, want := string(rc.Response.Header.Peek(cidHeader)), wantCID(t, data); got != want {
		t.Fatalf("x-cid = %q, want %q", got, want)
	}
	rv, err := b.resolveVersion(context.Background(), bucket, key, "")
	if err != nil {
		t.Fatal(err)
	}
	h, _ := blake3tree.NewHasher(0)
	h.Write(data)
	want := h.FinishObject()
	body := rv.mf.Body
	if body.TreeChunkLog != want.ChunkLog {
		t.Fatalf("TreeChunkLog = %d, want %d", body.TreeChunkLog, want.ChunkLog)
	}
	blocks, err := body.TreeBlockCVs()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != len(want.Blocks) {
		t.Fatalf("%d blocks, want %d", len(blocks), len(want.Blocks))
	}
	for i := range blocks {
		if blocks[i] != want.Blocks[i] {
			t.Fatalf("block %d differs from the single-pass tree", i)
		}
	}
}

// completeAll uploads parts in the given order and completes the upload.
func completeAll(t *testing.T, b *Backend, key string, parts [][]byte, order []int32) []byte {
	t.Helper()
	uploadID := mpCreate(t, b, key, "", "")
	etags := make([]*string, len(parts))
	for _, pn := range order {
		// versitygw always passes the declared length; the assumed offset of a
		// part with no recorded predecessor is made from it.
		n := int64(len(parts[pn-1]))
		out, err := mpUploadPart(t, b, key, uploadID, pn, parts[pn-1], func(in *s3.UploadPartInput) { in.ContentLength = &n })
		if err != nil {
			t.Fatalf("UploadPart %d: %v", pn, err)
		}
		etags[pn-1] = out.ETag
	}
	var completed []types.CompletedPart
	var whole []byte
	for i := range parts {
		pn := int32(i + 1)
		completed = append(completed, types.CompletedPart{PartNumber: &pn, ETag: etags[i]})
		whole = append(whole, parts[i]...)
	}
	if _, err := mpComplete(t, b, key, uploadID, completed, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	return whole
}

// recordedParts returns the completed upload's part records by part number.
func recordedParts(t *testing.T, b *Backend, key string, parts int) map[int]registry.MultipartPart {
	t.Helper()
	// Sessions are retained through completion; find this key's.
	sessions, err := b.multipart.ListSessions(context.Background(), "bk")
	if err != nil {
		t.Fatal(err)
	}
	out := map[int]registry.MultipartPart{}
	for _, s := range sessions {
		if s.ObjectKey != key {
			continue
		}
		recs, err := b.multipart.ListParts(context.Background(), s.UploadID)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range recs {
			out[p.PartNumber] = p
		}
	}
	if len(out) != parts {
		t.Fatalf("%d part records for %s, want %d", len(out), key, parts)
	}
	return out
}

// TestMultipartTree covers the paths Complete takes to the object's tree:
// uniform parts in order (every record used), a short final part arriving
// first (its assumed offset is wrong, so it alone is re-hashed), a non-final
// part of odd length (the body from it on is re-hashed as one range), a
// single part (the digest is the part's own root), and a part copied from
// another object.
func TestMultipartTree(t *testing.T) {
	b, _, _ := newRefTestBackend(t, 64<<10)
	minPart := int(backend.MinPartSize)

	t.Run("in order", func(t *testing.T) {
		parts := [][]byte{taggedBody(minPart, 0x01), taggedBody(minPart, 0x02), taggedBody(70_000, 0x03)}
		whole := completeAll(t, b, "in-order", parts, []int32{1, 2, 3})
		assertObjectTree(t, b, "in-order", whole)
		recs := recordedParts(t, b, "in-order", 3)
		if recs[1].Tree.Offset != 0 || recs[2].Tree.Offset != int64(minPart) || recs[3].Tree.Offset != 2*int64(minPart) {
			t.Fatalf("recorded offsets %d %d %d: every assumed offset should have been exact", recs[1].Tree.Offset, recs[2].Tree.Offset, recs[3].Tree.Offset)
		}
	})

	t.Run("final part first", func(t *testing.T) {
		// The short final part lands first: with nothing recorded its own
		// size stands in, the assumed offset is wrong, and Complete re-hashes it.
		const final = 70 << 10
		parts := [][]byte{taggedBody(minPart, 0x11), taggedBody(minPart, 0x12), taggedBody(final, 0x13)}
		whole := completeAll(t, b, "final-first", parts, []int32{3, 1, 2})
		assertObjectTree(t, b, "final-first", whole)
		recs := recordedParts(t, b, "final-first", 3)
		if recs[3].Tree == nil || recs[3].Tree.Offset != 2*final {
			t.Fatalf("part 3 recorded at offset %d: with no earlier part its own size should have stood in", recs[3].Tree.Offset)
		}
		if recs[2].Tree.Offset != int64(minPart) {
			t.Fatalf("part 2 recorded offset %d: part 1 was recorded, so the assumed offset should be exact", recs[2].Tree.Offset)
		}
	})

	t.Run("final part first with an unaligned assumed offset", func(t *testing.T) {
		// A final part whose size is not a whole number of chunks assumes an
		// offset inside a chunk; the tree is recorded with head bytes, the
		// offset check finds it wrong, and the part alone is re-hashed.
		parts := [][]byte{taggedBody(minPart, 0x61), taggedBody(minPart, 0x62), taggedBody(70_000, 0x63)}
		whole := completeAll(t, b, "final-first-odd", parts, []int32{3, 1, 2})
		assertObjectTree(t, b, "final-first-odd", whole)
		recs := recordedParts(t, b, "final-first-odd", 3)
		if recs[3].Tree == nil || recs[3].Tree.Offset != 2*70_000 || len(recs[3].Tree.Head) != 1024-(2*70_000)%1024 {
			t.Fatalf("part 3 recorded as %+v", recs[3].Tree)
		}
	})

	t.Run("middle part assumed from a sibling", func(t *testing.T) {
		parts := [][]byte{taggedBody(minPart, 0x21), taggedBody(minPart, 0x22), taggedBody(minPart, 0x23), taggedBody(1024, 0x24)}
		whole := completeAll(t, b, "sibling", parts, []int32{1, 3, 4, 2})
		assertObjectTree(t, b, "sibling", whole)
		recs := recordedParts(t, b, "sibling", 4)
		if recs[3].Tree.Offset != 2*int64(minPart) || recs[4].Tree.Offset != 3*int64(minPart) {
			t.Fatalf("recorded offsets %d %d: parts 3 and 4 should have been assumed from part 1's size", recs[3].Tree.Offset, recs[4].Tree.Offset)
		}
	})

	t.Run("odd length parts", func(t *testing.T) {
		// Part lengths that are not chunk multiples (the AWS SDK for Go above
		// its part limit) put chunk boundaries inside parts. The records
		// carry the bytes either side, every assumed offset is exact for a
		// sequential upload, and nothing is re-hashed.
		parts := [][]byte{taggedBody(minPart+100, 0x31), taggedBody(minPart+100, 0x32), taggedBody(3000, 0x33)}
		whole := completeAll(t, b, "odd", parts, []int32{1, 2, 3})
		assertObjectTree(t, b, "odd", whole)
		recs := recordedParts(t, b, "odd", 3)
		for n := 1; n <= 3; n++ {
			if recs[n].Tree == nil {
				t.Fatalf("part %d recorded no tree", n)
			}
		}
		if recs[1].Tree.Offset != 0 || recs[2].Tree.Offset != int64(minPart+100) || recs[3].Tree.Offset != 2*int64(minPart+100) {
			t.Fatalf("assumed offsets %d %d %d are not exact", recs[1].Tree.Offset, recs[2].Tree.Offset, recs[3].Tree.Offset)
		}
		if len(recs[1].Tree.Tail) != 100 || len(recs[2].Tree.Head) != 1024-100 {
			t.Fatalf("part 1 tail %d bytes, part 2 head %d bytes", len(recs[1].Tree.Tail), len(recs[2].Tree.Head))
		}
	})

	t.Run("final part is re-hashed regardless of the budget", func(t *testing.T) {
		// The final part landing first is the straggler the design expects;
		// it is covered even with no budget at all.
		saved := b.rehashBudget
		b.rehashBudget = 0
		defer func() { b.rehashBudget = saved }()
		parts := [][]byte{taggedBody(minPart, 0x91), taggedBody(minPart, 0x92), taggedBody(70<<10, 0x93)}
		whole := completeAll(t, b, "final-exempt", parts, []int32{3, 1, 2})
		assertObjectTree(t, b, "final-exempt", whole)
	})

	t.Run("re-hash over budget completes without a digest", func(t *testing.T) {
		// Variable sizes uploaded in reverse: part 2 (non-final) is assumed
		// wrongly, and with a budget below its size the object commits with
		// its ETag and bytes but no digest.
		saved := b.rehashBudget
		b.rehashBudget = 1
		defer func() { b.rehashBudget = saved }()
		parts := [][]byte{taggedBody(minPart+4096, 0x81), taggedBody(minPart, 0x82), taggedBody(70<<10, 0x83)}
		whole := completeAll(t, b, "over-budget", parts, []int32{3, 2, 1})
		bucket, key := "bk", "over-budget"
		rc := &fasthttp.RequestCtx{}
		head, err := b.HeadObject(rc, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
		if err != nil {
			t.Fatal(err)
		}
		if got := rc.Response.Header.Peek(cidHeader); len(got) != 0 {
			t.Fatalf("x-cid %q set on an object completed over budget", got)
		}
		if *head.ContentLength != int64(len(whole)) {
			t.Fatalf("ContentLength %d, want %d", *head.ContentLength, len(whole))
		}
		res, err := b.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{Bucket: &bucket, Key: &key})
		if err != nil || res.Blake3 != nil {
			t.Fatalf("Blake3 attribute present or error: %v", err)
		}
		out, err := b.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &bucket, Key: &key})
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := io.ReadAll(out.Body); !bytes.Equal(got, whole) {
			t.Fatal("body differs")
		}
		// The same upload within the default budget gets its digest.
		b.rehashBudget = DefaultRehashBudget
		whole = completeAll(t, b, "within-budget", parts, []int32{3, 2, 1})
		assertObjectTree(t, b, "within-budget", whole)
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
		if _, err := mpUploadPart(t, b, key, uploadID, 1, taggedBody(minPart, 0x71), func(in *s3.UploadPartInput) { in.ContentLength = &n1 }); err != nil {
			t.Fatal(err)
		}
		part2 := taggedBody(int(n2), 0x72)
		p2, err := mpUploadPart(t, b, key, uploadID, 2, part2, func(in *s3.UploadPartInput) { in.ContentLength = &n2 })
		if err != nil {
			t.Fatal(err)
		}
		two := int32(2)
		if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{{PartNumber: &two, ETag: p2.ETag}}, nil); err != nil {
			t.Fatalf("Complete: %v", err)
		}
		assertObjectTree(t, b, key, part2)
	})

	t.Run("copied part", func(t *testing.T) {
		src := taggedBody(minPart+4096, 0x51)
		putObj(t, b, "copy-src", src)
		key := "copied"
		uploadID := mpCreate(t, b, key, "", "")
		p1, err := mpUploadPart(t, b, key, uploadID, 1, taggedBody(minPart, 0x52), nil)
		if err != nil {
			t.Fatal(err)
		}
		n := int32(2)
		cp, err := b.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
			Bucket: strPtr("bk"), Key: &key, UploadId: &uploadID, PartNumber: &n, CopySource: strPtr("bk/copy-src"),
		})
		if err != nil {
			t.Fatalf("UploadPartCopy: %v", err)
		}
		one, two := int32(1), int32(2)
		if _, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{
			{PartNumber: &one, ETag: p1.ETag}, {PartNumber: &two, ETag: cp.ETag},
		}, nil); err != nil {
			t.Fatalf("Complete: %v", err)
		}
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
			if got := assumedPartOffset(toParts(c.prior), c.num, c.size); got != c.want {
				t.Fatalf("assumedPartOffset = %d, want %d", got, c.want)
			}
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
