package s3frontend

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/filecoin-project/go-fee"
	"github.com/filecoin-project/go-fee/aesstream"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
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
// /blob/add task, so a blob_streams row records that task from allocation
// until the park or acceptance is recorded. A request that dies in between
// leaves the row, and the stream sweeper aborts the upload by that task.

// WriteSizedBlob implements bucket.SizedBlobWriter: r yields exactly n
// plaintext bytes. Without a streaming uploader, or once the upload service
// has refused to add by digest code, it spools the blob as WriteBlob does.
func (w *encryptingBlobWriter) WriteSizedBlob(ctx context.Context, r io.Reader, n int64) (_ multihash.Multihash, err error) {
	if w.stream == nil || w.digestFirst {
		digest, got, err := w.WriteBlob(ctx, r)
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

	cek := make([]byte, 32)
	if _, err := rand.Read(cek); err != nil {
		return nil, fmt.Errorf("s3frontend: generate CEK: %w", err)
	}
	defer clear(cek)

	// The descriptor is complete before any plaintext is read, and with it
	// the envelope's length: its header, then the ciphertext of n bytes.
	rc, desc, err := fee.EncryptWithCEK(r, cek, w.recipients)
	if err != nil {
		return nil, fmt.Errorf("s3frontend: encrypt blob: %w", err)
	}
	defer rc.Close()
	stored := desc.HeaderLen + aesstream.EncryptedSize(n, desc.ChunkSize)
	span.SetAttributes(attribute.Int64("ingot.blob.bytes", stored))

	sb, err := w.stream.StartBlob(ctx, w.space, stored)
	if errors.Is(err, uploader.ErrUnsupportedDigestCode) {
		// The upload service cannot take a blob without its digest: this
		// body falls back to spooling each blob first, and uploading it by
		// digest once spooled.
		span.SetAttributes(attribute.String("ingot.blob.result", "digest_first"))
		w.logger.Warn("upload service cannot add by digest code; spooling blobs before upload", zap.Error(err))
		w.digestFirst = true
		return w.spoolEnvelope(ctx, rc, desc, cek, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("s3frontend: start blob: %w", err)
	}
	if err := w.streams.PutStream(ctx, registry.BlobStream{
		AddTask: sb.AddTask.Bytes(),
		Space:   w.space,
		Bucket:  w.bucket,
		Size:    stored,
	}); err != nil {
		return nil, fmt.Errorf("s3frontend: record stream: %w", err)
	}
	w.lease.add(sb.AddTask.Bytes())

	// The envelope feeds the spool, and every byte the spool reads is also
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
	digest, err := w.spoolEnvelope(ctx, io.TeeReader(rc, sink), desc, cek, &sb)
	if err != nil {
		pw.CloseWithError(err)
		<-putDone
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

// spoolEnvelope spools an envelope, checks it is the length its descriptor
// promised, and records its encryption state. streamed is the upload the
// envelope is being sent to, if any.
func (w *encryptingBlobWriter) spoolEnvelope(ctx context.Context, envelope io.Reader, desc fee.BodyDescriptor, cek []byte, streamed *uploader.StreamedBlob) (multihash.Multihash, error) {
	digest, storedSize, err := w.spool.WriteBlob(ctx, envelope)
	if err != nil {
		return nil, fmt.Errorf("s3frontend: spool envelope: %w", err)
	}
	if streamed != nil && storedSize != streamed.Size {
		return nil, fmt.Errorf("s3frontend: envelope of %d bytes was allocated %d", storedSize, streamed.Size)
	}
	if err := w.record(ctx, digest, cek, encWrite{desc: desc, storedSize: storedSize, streamed: streamed}); err != nil {
		return nil, err
	}
	return digest, nil
}

// abandon releases a streamed upload's allocation on its provider. It is best
// effort: the blob_streams row stays when the abort fails, and the stream
// sweeper retries it.
func (w *encryptingBlobWriter) abandon(ctx context.Context, sb uploader.StreamedBlob) {
	if err := w.stream.AbortBlob(ctx, w.space, sb.AddTask); err != nil {
		w.logger.Warn("abandoning streamed upload failed; the stream sweeper retries", zap.Stringer("add", sb.AddTask), zap.Error(err))
		return
	}
	if err := w.streams.DeleteStream(ctx, sb.AddTask.Bytes()); err != nil {
		w.logger.Warn("dropping stream row failed; the stream sweeper drops it", zap.Stringer("add", sb.AddTask), zap.Error(err))
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

// finishStream drops a streamed upload's blob_streams row once its park or
// acceptance is recorded. A row that survives a failure here is dropped by
// the stream sweeper, whose abort the provider refuses for an accepted blob.
func (b *Backend) finishStream(ctx context.Context, sb uploader.StreamedBlob) {
	if err := b.streams.DeleteStream(ctx, sb.AddTask.Bytes()); err != nil {
		b.logger.Warn("dropping stream row failed; the stream sweeper drops it", zap.Stringer("add", sb.AddTask), zap.Error(err))
	}
}

const (
	// streamStaleAge is how long a blob_streams row's lease can go unrenewed
	// before the sweeper takes its request for dead. A live request renews it
	// every streamLeaseInterval, however long its body takes.
	streamStaleAge = 30 * time.Minute
	// streamForgetAge is when the sweeper stops trying to abort a stream and
	// drops the row: the provider has expired the allocation by then.
	streamForgetAge = 24 * time.Hour
	// streamSweepBatch bounds the rows one sweep aborts.
	streamSweepBatch = 100
)

// streamLeaseInterval is how often a request renews the lease on its
// blob_streams rows: well inside streamStaleAge, so the rows of a request that
// is still running never look stale.
var streamLeaseInterval = streamStaleAge / 3

// streamLease renews the lease on one request's blob_streams rows from its
// first streamed blob until end. A row whose blob is parked or accepted is
// deleted meanwhile, and renewing it is a no-op.
type streamLease struct {
	streams registry.StreamStore
	logger  *zap.Logger

	mu    sync.Mutex
	tasks [][]byte
	stop  chan struct{}
	done  chan struct{}
	ended sync.Once
}

func newStreamLease(streams registry.StreamStore, logger *zap.Logger) *streamLease {
	return &streamLease{streams: streams, logger: logger}
}

// add puts addTask's row under the lease, starting the renewals with the
// request's first row.
func (l *streamLease) add(addTask []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tasks = append(l.tasks, addTask)
	if l.stop == nil {
		l.stop, l.done = make(chan struct{}), make(chan struct{})
		go l.renew(l.stop, l.done)
	}
}

func (l *streamLease) renew(stop, done chan struct{}) {
	defer close(done)
	t := time.NewTicker(streamLeaseInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			l.mu.Lock()
			tasks := slices.Clone(l.tasks)
			l.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), streamLeaseInterval)
			if err := l.streams.TouchStreams(ctx, tasks); err != nil {
				l.logger.Warn("renewing stream leases failed; retrying next interval", zap.Error(err))
			}
			cancel()
		}
	}
}

// end stops the renewals. The rows left, if any, belong to a request that
// failed, and go stale for the sweeper.
func (l *streamLease) end() {
	if l == nil {
		return
	}
	l.ended.Do(func() {
		l.mu.Lock()
		stop, done := l.stop, l.done
		l.mu.Unlock()
		if stop != nil {
			close(stop)
			<-done
		}
	})
}

// SweepStaleStreams aborts the uploads of requests that died between
// allocating a blob by digest code and recording its park or acceptance, and
// drops their blob_streams rows. An upload the space has since accepted is
// not the sweeper's to abort; its row is simply dropped. Nor is one whose park
// was recorded and the row left behind: the multipart session still needs the
// parked blob. It returns the rows dropped.
func (b *Backend) SweepStaleStreams(ctx context.Context) (int, error) {
	if b.streaming == nil || b.streams == nil {
		return 0, nil
	}
	now := time.Now()
	rows, err := b.streams.ListStaleStreams(ctx, now.Add(-streamStaleAge), streamSweepBatch)
	if err != nil {
		return 0, fmt.Errorf("s3frontend: list stale streams: %w", err)
	}
	dropped := 0
	for _, row := range rows {
		parked := false
		if b.parks != nil {
			if parked, err = b.parks.HasParkFor(ctx, row.AddTask); err != nil {
				return dropped, fmt.Errorf("s3frontend: find stream's park: %w", err)
			}
		}
		cause, err := cid.Cast(row.AddTask)
		if err != nil {
			b.logger.Error("stream row has an undecodable add task; dropping it", zap.Binary("add", row.AddTask), zap.Error(err))
		} else if parked {
			b.logger.Info("stale stream's blob is parked; dropping its row", zap.Stringer("add", cause))
		} else if err := b.streaming.AbortBlob(ctx, row.Space, cause); err != nil &&
			!errors.Is(err, uploader.ErrBlobAccepted) && now.Sub(row.CreatedAt) < streamForgetAge {
			b.logger.Warn("aborting stale stream failed; retrying next sweep", zap.Stringer("add", cause), zap.Error(err))
			continue
		}
		if err := b.streams.DeleteStream(ctx, row.AddTask); err != nil {
			return dropped, fmt.Errorf("s3frontend: drop stream row: %w", err)
		}
		dropped++
	}
	return dropped, nil
}
