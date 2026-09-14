package s3frontend

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
)

// taggedBody is testBody with every byte XORed by tag, so parts of equal
// length carry distinct content and a body assembled in the wrong part order
// cannot pass a byte comparison.
func taggedBody(n int, tag byte) []byte {
	data := testBody(n)
	for i := range data {
		data[i] ^= tag
	}
	return data
}

// TestMultipartPartsArriveOutOfOrder: part numbers, not arrival order, define
// the object. Parts uploaded 3, 1, 2 list ascending by part number, Complete
// assembles the body and the md5-of-md5s ETag in part-number order, and
// ?partNumber=N addresses the part by its number. A small blob ceiling makes
// every non-final part span several internal blobs, so the assembled blob
// list is genuinely re-sequenced rather than a one-blob-per-part passthrough.
func TestMultipartPartsArriveOutOfOrder(t *testing.T) {
	b, _, _ := newRefTestBackend(t, 64<<10)
	bucket, key := "bk", "out-of-order"

	// Non-final parts meet S3's minimum; the final part is small (exempt).
	parts := [][]byte{
		taggedBody(int(backend.MinPartSize), 0x11),
		taggedBody(int(backend.MinPartSize)+4096, 0x22),
		taggedBody(9<<10, 0x33),
	}
	uploadID := mpCreate(t, b, key, "", "")

	etags := make([]*string, len(parts))
	for _, pn := range []int32{3, 1, 2} {
		out, err := mpUploadPart(t, b, key, uploadID, pn, parts[pn-1], nil)
		if err != nil {
			t.Fatalf("UploadPart %d: %v", pn, err)
		}
		etags[pn-1] = out.ETag
	}

	// ListParts orders by part number regardless of arrival order.
	lp, err := b.ListParts(context.Background(), &s3.ListPartsInput{Bucket: &bucket, Key: &key, UploadId: &uploadID})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(lp.Parts) != len(parts) {
		t.Fatalf("ListParts returned %d parts, want %d", len(lp.Parts), len(parts))
	}
	for i, p := range lp.Parts {
		want := int32(i + 1)
		if int32(p.PartNumber) != want {
			t.Fatalf("ListParts[%d].PartNumber = %d, want %d", i, p.PartNumber, want)
		}
		if p.Size != int64(len(parts[i])) {
			t.Fatalf("ListParts part %d size = %d, want %d", want, p.Size, len(parts[i]))
		}
		if strings.Trim(p.ETag, `"`) != strings.Trim(*etags[i], `"`) {
			t.Fatalf("ListParts part %d ETag = %q, want %s", want, p.ETag, *etags[i])
		}
	}

	var completed []types.CompletedPart
	var whole []byte
	etagCat := md5.New()
	for i := range parts {
		pn := int32(i + 1)
		completed = append(completed, types.CompletedPart{PartNumber: &pn, ETag: etags[i]})
		whole = append(whole, parts[i]...)
		sum := md5.Sum(parts[i])
		etagCat.Write(sum[:])
	}
	wantETag := hex.EncodeToString(etagCat.Sum(nil)) + "-3"

	res, err := mpComplete(t, b, key, uploadID, completed, nil)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if res.ETag == nil {
		t.Fatal("Complete returned no ETag")
	}
	if got := strings.Trim(*res.ETag, `"`); got != wantETag {
		t.Fatalf("Complete ETag = %q, want %q (md5-of-md5s in part-number order)", got, wantETag)
	}

	if got := getRange(t, b, key, ""); !bytes.Equal(got, whole) {
		t.Fatalf("GET body mismatch after out-of-order upload (%d bytes, want %d)", len(got), len(whole))
	}
	// A range across the part-1→part-2 boundary reads from the re-sequenced
	// blob list, not the arrival order.
	p1 := len(parts[0])
	if got := getRange(t, b, key, fmt.Sprintf("bytes=%d-%d", p1-10, p1+2000)); !bytes.Equal(got, whole[p1-10:p1+2001]) {
		t.Fatalf("ranged GET across the part boundary mismatch (%d bytes)", len(got))
	}
	// ?partNumber=2 is the part uploaded last, addressed by its number.
	two := int32(2)
	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &bucket, Key: &key, PartNumber: &two})
	if err != nil {
		t.Fatalf("GetObject partNumber=2: %v", err)
	}
	defer out.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(out.Body); err != nil {
		t.Fatalf("read part 2: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), parts[1]) {
		t.Fatalf("GET partNumber=2 returned %d bytes that are not part 2", buf.Len())
	}
}
