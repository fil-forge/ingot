package s3frontend

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"github.com/filecoin-project/go-fee"
	"github.com/filecoin-project/go-fee/aesstream"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/uploader"
	"github.com/fil-forge/ucantone/did"
)

// This file is the streaming half of the body write path (the trailer-hash
// RFC). A blob whose plaintext length is known before its first byte has a
// known envelope length too, so it is allocated by size and hash function
// alone and its envelope goes to the provider while it is being spooled: the
// upload no longer waits for the body to land on disk. The spool still gets
// its copy, which is where the digest comes from and what the read path
// serves from, and a failed PUT falls back to uploading that copy by digest.
//
// Until the upload is parked or accepted the provider knows it only by its
// /blob/add task. A stream that fails aborts its allocation there and then.
// One abandoned by a request that died is never concluded, and the provider
// expires it within a day: an unaccepted blob counts toward no space's usage
// and is never served, so nothing needs to abort it sooner.

// WriteSizedBlob implements bucket.SizedBlobWriter: r yields exactly n
// plaintext bytes. Without a streaming uploader, or once the upload service
// has refused to add by digest code, it spools the blob as WriteBlob does.
func (w *bodyWriter) WriteSizedBlob(ctx context.Context, r io.Reader, n int64) (_ multihash.Multihash, err error) {
	if w.stream == nil || w.digestFirst {
		digest, got, err := w.writeBlob(ctx, r, n)
		if err != nil {
			return nil, err
		}
		if got != n {
			return nil, fmt.Errorf("s3frontend: blob of %d bytes yielded %d", n, got)
		}
		return digest, nil
	}

	ctx, span := tracing.Start(ctx, "blob.stream", attribute.Int64("ingot.blob.plaintext_bytes", n))
	defer func() { tracing.End(span, err) }()

	// What gets stored, and how long it is, is known before the first byte
	// is read: the plaintext itself, or for an encrypted body its envelope —
	// the descriptor's header, then the ciphertext of n bytes.
	body, stored := r, n
	var desc fee.BodyDescriptor
	var cek []byte
	if !w.plaintext {
		cek = make([]byte, 32)
		if _, err := rand.Read(cek); err != nil {
			return nil, fmt.Errorf("s3frontend: generate CEK: %w", err)
		}
		defer clear(cek)
		rc, d, err := fee.EncryptWithCEK(r, cek, w.recipients, fee.WithPlaintextLength(n))
		if err != nil {
			return nil, fmt.Errorf("s3frontend: encrypt blob: %w", err)
		}
		defer rc.Close()
		body, desc = rc, d
		chunkSize, err := desc.ChunkSize()
		if err != nil {
			return nil, fmt.Errorf("s3frontend: encrypt blob: %w", err)
		}
		stored = desc.HeaderLen + aesstream.EncryptedSize(n, chunkSize)
	}
	span.SetAttributes(attribute.Int64("ingot.blob.bytes", stored))

	sb, err := w.stream.StartBlob(ctx, w.space, stored)
	if errors.Is(err, uploader.ErrUnsupportedDigestCode) {
		// The upload service cannot take a blob without its digest: this
		// body falls back to spooling each blob first, and uploading it by
		// digest once spooled.
		span.SetAttributes(attribute.String("ingot.blob.result", "digest_first"))
		w.logger.Warn("upload service cannot add by digest code; spooling blobs before upload", zap.Error(err))
		w.digestFirst = true
		return w.spoolStored(ctx, body, desc, cek, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("s3frontend: start blob: %w", err)
	}
	// The stored bytes feed the spool, and every byte the spool reads is also
	// written to the PUT. A spool failure fails the PUT with it, so the
	// provider never receives a body that ends early and looks complete. A
	// PUT failure only detaches the PUT: the spool carries on, and its copy
	// is uploaded by digest instead.
	pr, pw := io.Pipe()
	putDone := make(chan error, 1)
	go func() {
		err := w.stream.PutBlob(ctx, sb, pr)
		// Nothing reads the pipe from here on; unblock the spool.
		if err != nil {
			pr.CloseWithError(err)
		} else {
			pr.CloseWithError(errPutFinished)
		}
		putDone <- err
	}()
	sink := &detachableWriter{w: pw}
	digest, err := w.spoolStored(ctx, io.TeeReader(body, sink), desc, cek, &sb)
	if err != nil {
		pw.CloseWithError(err)
		<-putDone
		// The request may have ended with the body; the abort should not.
		w.abandon(context.WithoutCancel(ctx), sb)
		return nil, err
	}
	pw.Close()
	if putErr := <-putDone; putErr != nil {
		// The spooled copy is whole; the allocation that took the failed PUT
		// is released and the blob uploads by digest like any spooled blob.
		span.SetAttributes(attribute.String("ingot.blob.result", "put_failed"))
		w.logger.Warn("streamed put failed; uploading the spooled blob by digest", zap.Error(putErr))
		res := w.results[string(digest)]
		res.streamed = nil
		w.results[string(digest)] = res
		w.abandon(ctx, sb)
		return digest, nil
	}
	span.SetAttributes(attribute.String("ingot.blob.result", "streamed"))
	return digest, nil
}

// spoolStored spools a blob's stored bytes (its envelope, or the plaintext
// itself), checks they are the length promised, and records the blob's
// state. streamed is the upload the bytes are being sent to, if any.
func (w *bodyWriter) spoolStored(ctx context.Context, stored io.Reader, desc fee.BodyDescriptor, cek []byte, streamed *uploader.StreamedBlob) (multihash.Multihash, error) {
	digest, storedSize, err := w.writeSpool(ctx, stored)
	if err != nil {
		return nil, fmt.Errorf("s3frontend: spool blob: %w", err)
	}
	if streamed != nil && storedSize != streamed.Size {
		return nil, fmt.Errorf("s3frontend: blob of %d bytes was allocated %d", storedSize, streamed.Size)
	}
	if err := w.record(ctx, digest, cek, encWrite{desc: desc, storedSize: storedSize, streamed: streamed}); err != nil {
		return nil, err
	}
	return digest, nil
}

// abandon releases a streamed upload's allocation on its provider. It is best
// effort: an allocation whose abort fails is expired by the provider.
func (w *bodyWriter) abandon(ctx context.Context, sb uploader.StreamedBlob) {
	if err := w.stream.AbortBlob(ctx, w.space, sb.AddTask); err != nil {
		w.logger.Warn("abandoning streamed upload failed; the provider expires it", zap.Stringer("add", sb.AddTask), zap.Error(err))
	}
}

// errPutFinished closes the PUT's end of the pipe once the PUT has returned.
var errPutFinished = errors.New("s3frontend: put finished")

// detachableWriter writes to w until a write fails, then discards what
// follows. It never fails itself, so the reader it tees from carries on.
type detachableWriter struct {
	w   io.Writer
	err error
}

func (d *detachableWriter) Write(p []byte) (int, error) {
	if d.err == nil {
		_, d.err = d.w.Write(p)
	}
	return len(p), nil
}

// abandonStream gives up a streamed upload whose parked bytes a release may
// have taken (see concludeStreamed): the allocation is aborted on its
// provider, best effort, as bodyWriter.abandon does.
func (b *Backend) abandonStream(ctx context.Context, space did.DID, sb uploader.StreamedBlob) {
	if err := b.streaming.AbortBlob(ctx, space, sb.AddTask); err != nil {
		b.logger.Warn("abandoning streamed upload failed; the provider expires it", zap.Stringer("add", sb.AddTask), zap.Error(err))
	}
}
