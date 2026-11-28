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
	"github.com/stretchr/testify/require"
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
		require.NoError(t, err, "UploadPart %d", pn)
		etags[pn-1] = out.ETag
	}

	// ListParts orders by part number regardless of arrival order.
	lp, err := b.ListParts(context.Background(), &s3.ListPartsInput{Bucket: &bucket, Key: &key, UploadId: &uploadID})
	require.NoError(t, err, "ListParts")
	require.Len(t, lp.Parts, len(parts), "ListParts")
	for i, p := range lp.Parts {
		want := int32(i + 1)
		require.Equal(t, want, int32(p.PartNumber), "ListParts[%d].PartNumber", i)
		require.Equal(t, int64(len(parts[i])), p.Size, "ListParts part %d size", want)
		require.Equal(t, strings.Trim(*etags[i], `"`), strings.Trim(p.ETag, `"`), "ListParts part %d ETag", want)
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
	require.NoError(t, err, "Complete")
	require.NotNil(t, res.ETag, "Complete returned no ETag")
	got := strings.Trim(*res.ETag, `"`)
	require.Equal(t, wantETag, got, "Complete ETag (md5-of-md5s in part-number order)")

	body := getRange(t, b, key, "")
	require.True(t, bytes.Equal(whole, body), "GET body mismatch after out-of-order upload (%d bytes, want %d)", len(body), len(whole))
	// A range across the part-1→part-2 boundary reads from the re-sequenced
	// blob list, not the arrival order.
	p1 := len(parts[0])
	body = getRange(t, b, key, fmt.Sprintf("bytes=%d-%d", p1-10, p1+2000))
	require.True(t, bytes.Equal(whole[p1-10:p1+2001], body), "ranged GET across the part boundary mismatch (%d bytes)", len(body))
	// ?partNumber=2 is the part uploaded last, addressed by its number.
	two := int32(2)
	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &bucket, Key: &key, PartNumber: &two})
	require.NoError(t, err, "GetObject partNumber=2")
	defer out.Body.Close()
	var buf bytes.Buffer
	_, err = buf.ReadFrom(out.Body)
	require.NoError(t, err, "read part 2")
	require.True(t, bytes.Equal(parts[1], buf.Bytes()), "GET partNumber=2 returned %d bytes that are not part 2", buf.Len())
}
