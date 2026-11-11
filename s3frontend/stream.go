package s3frontend

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/filecoin-project/go-fee"
	"github.com/filecoin-project/go-fee/aesstream"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/uploader"
)

// This file is the streaming body write path (the trailer-hash RFC). A
// blob's plaintext length is known before its first byte, so its envelope
// length is known too, and it is allocated by size and hash function alone:
// its stored bytes go to the provider as the body arrives, and the digest is
// computed on the way. A write that keeps a local copy tees the same bytes
// into the cache (see localCopy).
//
// Until the upload is parked or accepted the provider knows it only by its
// /blob/add task. A stream that fails aborts its allocation there and then.
// One abandoned by a request that died is never concluded, and the provider
// expires it within a day: an unaccepted blob counts toward no space's usage
// and is never served, so nothing needs to abort it sooner.

// WriteSizedBlob implements bucket.SizedBlobWriter: r yields exactly n
// plaintext bytes, which go to the provider as they are read. A failed send
// fails the write, and its allocation is aborted.
func (w *bodyWriter) WriteSizedBlob(ctx context.Context, r io.Reader, n int64) (_ multihash.Multihash, err error) {
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
		// EncryptWithCEK internalizes the key before returning and the wrap
		// copies it; wipe our copy on every path out.
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
	if err != nil {
		return nil, fmt.Errorf("s3frontend: start blob: %w", err)
	}
	local := w.localCopy()
	// Every stored byte is written to the PUT, the hash and the local copy.
	// A failed PUT stops the copy, and a body that fails or ends early fails
	// the PUT with it, so the provider never receives a body that ends early
	// and looks complete.
	pr, pw := io.Pipe()
	putDone := make(chan error, 1)
	go func() {
		err := w.stream.PutBlob(ctx, sb, pr)
		// Nothing reads the pipe from here on; unblock the copy.
		if err != nil {
			pr.CloseWithError(err)
		} else {
			pr.CloseWithError(errPutFinished)
		}
		putDone <- err
	}()
	hasher := sha256.New()
	got, err := io.Copy(io.MultiWriter(pw, hasher, local), body)
	if err != nil {
		pw.CloseWithError(err)
		<-putDone
		local.discard()
		// The request may have ended with the body; the abort should not.
		w.abandon(context.WithoutCancel(ctx), sb)
		return nil, fmt.Errorf("s3frontend: stream blob: %w", err)
	}
	pw.Close()
	if putErr := <-putDone; putErr != nil {
		local.discard()
		w.abandon(context.WithoutCancel(ctx), sb)
		return nil, fmt.Errorf("s3frontend: put blob: %w", putErr)
	}
	if got != sb.Size {
		local.discard()
		w.abandon(context.WithoutCancel(ctx), sb)
		return nil, fmt.Errorf("s3frontend: blob of %d bytes was allocated %d", got, sb.Size)
	}
	digest, err := multihash.Encode(hasher.Sum(nil), multihash.SHA2_256)
	if err != nil {
		local.discard()
		w.abandon(context.WithoutCancel(ctx), sb)
		return nil, fmt.Errorf("s3frontend: blob digest: %w", err)
	}
	if err := w.record(ctx, digest, cek, encWrite{desc: desc, storedSize: got, streamed: sb, local: local.kept()}); err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.String("ingot.blob.result", "streamed"))
	return digest, nil
}

// localCopy starts the blob's copy in the cache, when the write keeps one.
// The cache is only an optimisation, so a copy that cannot be started, or
// whose write fails, is dropped and the upload goes on without it.
func (w *bodyWriter) localCopy() *localCopy {
	if w.cache == nil {
		return &localCopy{}
	}
	t, err := w.cache.CreateTemp()
	if err != nil {
		w.logger.Warn("could not start a blob's local copy; it is not cached", zap.Error(err))
		return &localCopy{}
	}
	return &localCopy{t: t, logger: w.logger}
}

// localCopy writes to a blob's temporary file in the cache until a write
// fails, then discards the file and what follows. It never fails itself, so
// the stream it tees from carries on.
type localCopy struct {
	t      *blockstore.TempBlob
	logger *zap.Logger
}

func (l *localCopy) Write(p []byte) (int, error) {
	if l.t != nil {
		if _, err := l.t.Write(p); err != nil {
			l.logger.Warn("writing a blob's local copy failed; it is not cached", zap.Error(err))
			l.discard()
		}
	}
	return len(p), nil
}

// discard drops the copy.
func (l *localCopy) discard() {
	l.t.Discard()
	l.t = nil
}

// kept closes the finished copy and returns it, or nil when there is none:
// the copy waits for its object's other blobs without holding a file open.
func (l *localCopy) kept() *blockstore.TempBlob {
	if l.t == nil {
		return nil
	}
	if err := l.t.Close(); err != nil {
		l.logger.Warn("closing a blob's local copy failed; it is not cached", zap.Error(err))
		l.discard()
		return nil
	}
	return l.t
}

// abandonTimeLimit bounds how long a request waits for a streamed upload's
// abort, which may run after the request's own context has ended. An abort cut
// short is left to the provider's expiry. Package-level so tests can shrink
// it.
var abandonTimeLimit = 10 * time.Second

// abandon releases a streamed upload's allocation on its provider. It is best
// effort, and waits at most abandonTimeLimit: an allocation whose abort fails
// is expired by the provider.
func (w *bodyWriter) abandon(ctx context.Context, sb uploader.StreamedBlob) {
	ctx, cancel := context.WithTimeout(ctx, abandonTimeLimit)
	defer cancel()
	if err := w.stream.AbortBlob(ctx, w.space, sb.AddTask); err != nil {
		w.logger.Warn("abandoning streamed upload failed; the provider expires it", zap.Stringer("add", sb.AddTask), zap.Error(err))
	}
}

// abandonStreamed releases the allocation of every blob the body has
// streamed, for a body that failed after streaming some: nothing will
// conclude them. The aborts run at once, so the wait is one abandon's at
// most, and outlive the request, as abandon's does.
func (w *bodyWriter) abandonStreamed(ctx context.Context) {
	ctx = context.WithoutCancel(ctx)
	var wg sync.WaitGroup
	for _, sb := range w.streamed() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.abandon(ctx, sb)
		}()
	}
	wg.Wait()
}

// errPutFinished closes the PUT's end of the pipe once the PUT has returned.
var errPutFinished = errors.New("s3frontend: put finished")
