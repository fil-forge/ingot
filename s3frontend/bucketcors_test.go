package s3frontend

import (
	"bytes"
	"context"
	"testing"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"

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
	if err := b.PutBucketCors(ctx, "bk", []byte(corsDoc)); err != nil {
		t.Fatalf("PutBucketCors: %v", err)
	}
	got, err := b.GetBucketCors(ctx, "bk")
	if err != nil {
		t.Fatalf("GetBucketCors: %v", err)
	}
	if !bytes.Equal(got, []byte(corsDoc)) {
		t.Fatalf("GetBucketCors = %q, want the stored document", got)
	}
}

func TestBucketCors_OwnDocumentReplacesTheDefault(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	gateway := []byte(`<CORSConfiguration><CORSRule><AllowedOrigin>https://console.example</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`)
	b.cors = gateway
	if err := mem.Create(ctx, "other", did.Undef, registry.CreateState{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// Without a document of its own, every bucket answers the gateway's.
	for _, name := range []string{"bk", "other"} {
		got, err := b.GetBucketCors(ctx, name)
		if err != nil {
			t.Fatalf("GetBucketCors %q: %v", name, err)
		}
		if !bytes.Equal(got, gateway) {
			t.Fatalf("GetBucketCors %q = %q, want the gateway document", name, got)
		}
	}

	// A document on one bucket replaces the default there and nowhere else.
	if err := b.PutBucketCors(ctx, "bk", []byte(corsDoc)); err != nil {
		t.Fatalf("PutBucketCors: %v", err)
	}
	got, err := b.GetBucketCors(ctx, "bk")
	if err != nil {
		t.Fatalf("GetBucketCors: %v", err)
	}
	if !bytes.Equal(got, []byte(corsDoc)) {
		t.Fatalf("GetBucketCors = %q, want the bucket's own document", got)
	}
	got, err = b.GetBucketCors(ctx, "other")
	if err != nil {
		t.Fatalf("GetBucketCors other: %v", err)
	}
	if !bytes.Equal(got, gateway) {
		t.Fatalf("GetBucketCors other = %q, want the gateway document still", got)
	}

	// Deleting it restores the default. Deleting again is a no-op.
	for range 2 {
		if err := b.DeleteBucketCors(ctx, "bk"); err != nil {
			t.Fatalf("DeleteBucketCors: %v", err)
		}
	}
	got, err = b.GetBucketCors(ctx, "bk")
	if err != nil {
		t.Fatalf("GetBucketCors after delete: %v", err)
	}
	if !bytes.Equal(got, gateway) {
		t.Fatalf("GetBucketCors after delete = %q, want the gateway document", got)
	}
}

func TestBucketCors_SizeCap(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	if err := b.PutBucketCors(ctx, "bk", bytes.Repeat([]byte("x"), maxCORSConfig+1)); err == nil {
		t.Fatal("PutBucketCors over the cap: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrEntityTooLarge)
	}
	if err := b.PutBucketCors(ctx, "bk", bytes.Repeat([]byte("x"), maxCORSConfig)); err != nil {
		t.Fatalf("PutBucketCors at the cap: %v", err)
	}
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
