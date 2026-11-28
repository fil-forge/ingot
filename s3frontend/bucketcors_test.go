package s3frontend

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/registry"
)

// Bucket CORS tests over the refindex_test.go harness, covering
// docs/s3-cors.md: the gateway-wide document as the default, a bucket's own
// document replacing it for that bucket alone, the delete round-trip, the
// size cap, and the missing bucket outranking everything.

const corsDoc = `<CORSConfiguration><CORSRule><AllowedOrigin>https://app.example</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`

func TestBucketCors_DisabledWithoutDocument(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	// CORS disabled gateway-wide and no document of the bucket's own: the
	// sentinel versitygw's middlewares fall through on.
	_, err := b.GetBucketCors(ctx, "bk")
	wantAPIErr(t, err, s3err.ErrNoSuchCORSConfiguration)

	// Storing a document turns CORS on for this bucket alone.
	require.NoError(t, b.PutBucketCors(ctx, "bk", []byte(corsDoc)), "PutBucketCors")
	got, err := b.GetBucketCors(ctx, "bk")
	require.NoError(t, err, "GetBucketCors")
	require.Equal(t, []byte(corsDoc), got, "GetBucketCors")
}

func TestBucketCors_OwnDocumentReplacesTheDefault(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	gateway := []byte(`<CORSConfiguration><CORSRule><AllowedOrigin>https://console.example</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`)
	b.cors = gateway
	require.NoError(t, mem.Create(ctx, "other", did.Undef, registry.CreateState{}), "create bucket")

	// Without a document of its own, every bucket answers the gateway's.
	for _, name := range []string{"bk", "other"} {
		got, err := b.GetBucketCors(ctx, name)
		require.NoError(t, err, "GetBucketCors %q", name)
		require.Equal(t, gateway, got, "GetBucketCors %q", name)
	}

	// A document on one bucket replaces the default there and nowhere else.
	require.NoError(t, b.PutBucketCors(ctx, "bk", []byte(corsDoc)), "PutBucketCors")
	got, err := b.GetBucketCors(ctx, "bk")
	require.NoError(t, err, "GetBucketCors")
	require.Equal(t, []byte(corsDoc), got, "GetBucketCors = %q, want the bucket's own document", got)
	got, err = b.GetBucketCors(ctx, "other")
	require.NoError(t, err, "GetBucketCors other")
	require.Equal(t, gateway, got, "GetBucketCors other = %q, want the gateway document still", got)

	// Deleting it restores the default. Deleting again is a no-op.
	for range 2 {
		require.NoError(t, b.DeleteBucketCors(ctx, "bk"), "DeleteBucketCors")
	}
	got, err = b.GetBucketCors(ctx, "bk")
	require.NoError(t, err, "GetBucketCors after delete")
	require.Equal(t, gateway, got, "GetBucketCors after delete")
}

// corsDocWithRules is a document of n minimal, valid rules.
func corsDocWithRules(n int) []byte {
	var buf bytes.Buffer
	buf.WriteString("<CORSConfiguration>")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&buf, "<CORSRule><AllowedOrigin>https://origin-%d.example</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule>", i)
	}
	buf.WriteString("</CORSConfiguration>")
	return buf.Bytes()
}

func TestBucketCors_RuleLimit(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	// S3's hundred rules is the most a document may hold.
	hundred := corsDocWithRules(maxCORSRules)
	require.NoError(t, b.PutBucketCors(ctx, "bk", hundred), "PutBucketCors with %d rules", maxCORSRules)

	// One more is refused, and the stored document is untouched.
	err := b.PutBucketCors(ctx, "bk", corsDocWithRules(maxCORSRules+1))
	require.Error(t, err, "PutBucketCors with %d rules", maxCORSRules+1)
	wantAPIErr(t, err, s3err.ErrMalformedXML)
	got, err := b.GetBucketCors(ctx, "bk")
	require.NoError(t, err, "GetBucketCors")
	require.Equal(t, hundred, got, "GetBucketCors: want the %d-rule document still stored", maxCORSRules)
}

func TestBucketCors_SizeCap(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	// Over the byte cap, before any parse: EntityTooLarge. Under it, a body
	// that is not a CORS document is MalformedXML, as the controller would
	// have said; the backend trusts the controller's validation but not the
	// shape of whatever it is handed.
	err := b.PutBucketCors(ctx, "bk", bytes.Repeat([]byte("x"), maxCORSConfig+1))
	require.Error(t, err, "PutBucketCors over the cap")
	wantAPIErr(t, err, s3err.ErrEntityTooLarge)
	err = b.PutBucketCors(ctx, "bk", bytes.Repeat([]byte("x"), maxCORSConfig))
	require.Error(t, err, "PutBucketCors with a body that is not a document")
	wantAPIErr(t, err, s3err.ErrMalformedXML)
}

func TestBucketCors_MissingBucket(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	b.cors = []byte(corsDoc)

	_, err := b.GetBucketCors(ctx, "nope")
	wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	wantAPIErr(t, b.PutBucketCors(ctx, "nope", []byte(corsDoc)), s3err.ErrNoSuchBucket)
	wantAPIErr(t, b.DeleteBucketCors(ctx, "nope"), s3err.ErrNoSuchBucket)
}

// A bucket's document goes with its registry row, which DeleteBucket ends by
// removing: a bucket deleted and created again under the same name answers
// the default, not the document the earlier bucket stored.
func TestBucketCors_DoesNotSurviveTheBucket(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	gateway := []byte(`<CORSConfiguration/>`)
	b.cors = gateway

	require.NoError(t, b.PutBucketCors(ctx, "bk", []byte(corsDoc)), "PutBucketCors")
	require.NoError(t, mem.Delete(ctx, "bk", mem.bk()), "delete bucket")
	_, err := b.GetBucketCors(ctx, "bk")
	wantAPIErr(t, err, s3err.ErrNoSuchBucket)

	require.NoError(t, mem.Create(ctx, "bk", did.Undef, registry.CreateState{}), "create bucket again")
	got, err := b.GetBucketCors(ctx, "bk")
	require.NoError(t, err, "GetBucketCors after re-creation")
	require.Equal(t, gateway, got, "GetBucketCors after re-creation")
}
