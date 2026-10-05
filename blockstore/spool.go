package blockstore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	mh "github.com/multiformats/go-multihash"
)

// Spool is where each object-body blob is written, keyed by its sha256
// digest, and where it waits until the provider holds it (docs/architecture.md
// §5): a write in progress (a .tmp-* file), then a finished blob the upload
// picks up. With streaming the upload runs as the blob is written; otherwise
// the finished file waits for it. Once the provider holds the blob, the S3
// layer moves it into the BlobCache (BlobCache.Take), or removes it.
//
// Spool deliberately holds no policy: it is a blockstore.BlockReader plus the
// streaming BlobReader/BlobWriter, plus running byte counts. It knows a blob
// only as bytes under a digest. Whether a blob may move or be removed depends
// on what refers to it — its upload state, the objects and multipart sessions
// that use it, and whether the provider holds it — and that is the S3 layer's
// model (s3frontend over registry), so that layer owns the blob's lifecycle
// and tells the spool what to do.
//
// At most one Spool may use a directory at a time. NewSpool deletes every
// unfinished write it finds, which would destroy another live Spool's
// in-flight writes, and the byte counts assume this Spool makes every change
// to the directory.
type Spool struct {
	*blobDir
	// inFlight is every byte written so far to a write in progress. It is
	// kept apart from the finished files' count, which a scan measures and
	// may reset, so a scan never measures a write mid-way. A finished write's
	// bytes are briefly in both, between its commit and WriteBlob's return,
	// so Usage can run high by up to one blob per concurrent write.
	inFlight atomic.Int64
}

// spoolTempPrefix names the files WriteBlob streams into before the rename to
// the digest path.
const spoolTempPrefix = ".tmp-"

// NewSpool opens (creating if needed) a spool rooted at dir, then recovers
// it. The caller must ensure no other Spool uses dir (see Spool).
func NewSpool(dir string) (*Spool, error) {
	d, err := newBlobDir(dir, "spool")
	if err != nil {
		return nil, err
	}
	s := &Spool{blobDir: d}
	if err := s.recoverDir(); err != nil {
		return nil, err
	}
	return s, nil
}

// recoverDir brings a newly opened spool to a known state: it deletes every
// leftover .tmp-* file it can (a write the previous process never finished;
// nothing can be writing one before the listener starts) and counts the blob
// files. A leftover it cannot delete is left for the sweeper's orphan pass,
// which retries and logs it, rather than failing startup; until then its
// bytes are in no count. Entries that are
// neither, such as lost+found on a dedicated filesystem, are ignored.
func (s *Spool) recoverDir() error {
	var temps []string
	if err := s.count(func(f BlobFile) {
		if f.Digest == nil {
			temps = append(temps, f.Name)
		}
	}); err != nil {
		return err
	}
	for _, name := range temps {
		_, _ = s.RemoveTemp(name)
	}
	return nil
}

// Usage returns the byte count of the spool: its finished blob files and the
// bytes written so far to writes in progress.
func (s *Spool) Usage() int64 {
	return s.finished() + s.inFlight.Load()
}

// InFlight returns the bytes written so far to writes in progress.
func (s *Spool) InFlight() int64 {
	return s.inFlight.Load()
}

// WriteBlob streams r to the spool, computing its sha256 digest as it writes so
// the blob is never held whole in memory (object-body blobs run up to
// max_blob_size ≈ 254 MiB; buffering them would put that × concurrency in RAM).
// The write is atomic (temp file → rename to the digest path), so a crash leaves
// no partial blob readable under its digest. Each byte counts as in flight as
// it is written; the rename moves the bytes to the finished count, and a failed
// write takes them off. A failed write's temp file that cannot be deleted is
// in no count until the sweeper's orphan pass removes it. An empty r writes nothing and returns a nil digest with
// n == 0 (a zero-byte object has no blob). Re-writing an identical blob is
// idempotent (same digest, rename overwrites in place).
func (s *Spool) WriteBlob(_ context.Context, r io.Reader) (mh.Multihash, int64, error) {
	tmp, err := os.CreateTemp(s.dir, spoolTempPrefix+"*")
	if err != nil {
		return nil, 0, fmt.Errorf("blockstore: spool tempfile: %w", err)
	}
	tmpName := tmp.Name()
	counted := &countingWriter{w: tmp, count: &s.inFlight}
	// However the write ends, its bytes stop being in flight: a commit has
	// counted them as finished by then, and a failure leaves them nowhere.
	defer func() { s.inFlight.Add(-counted.n) }()
	hasher := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(counted, hasher), r)
	if closeErr := tmp.Close(); closeErr != nil && copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		_ = os.Remove(tmpName)
		return nil, n, fmt.Errorf("blockstore: spool write: %w", copyErr)
	}
	if n == 0 {
		_ = os.Remove(tmpName)
		return nil, 0, nil
	}
	digest, err := mh.Encode(hasher.Sum(nil), mh.SHA2_256)
	if err != nil {
		_ = os.Remove(tmpName)
		return nil, n, fmt.Errorf("blockstore: spool digest: %w", err)
	}
	if err := s.commit(tmpName, digest, counted.n); err != nil {
		_ = os.Remove(tmpName)
		return nil, n, err
	}
	return digest, n, nil
}

// commit renames a finished temp file to its digest path and counts its size
// as finished, unless an identical blob already sat there (counted already).
// The check and the rename are not atomic against a Remove of the same
// digest; that cannot happen today, because every write encrypts under a
// fresh key and so has a digest of its own.
func (s *Spool) commit(tmpName string, digest mh.Multihash, size int64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	path := s.Path(digest)
	_, statErr := os.Stat(path)
	existed := statErr == nil
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("blockstore: spool rename: %w", err)
	}
	if !existed {
		s.usage.Add(size)
	}
	s.changes.Add(1)
	return nil
}

// countingWriter adds each byte it writes to count, and remembers how many so
// the caller can take them off again.
type countingWriter struct {
	w     io.Writer
	count *atomic.Int64
	n     int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	c.count.Add(int64(n))
	return n, err
}

// Compile-time assertions: Spool is a raw-block read tier and the streaming
// blob tier for object bodies.
var (
	_ BlockReader     = (*Spool)(nil)
	_ BlobReader      = (*Spool)(nil)
	_ BlobRangeReader = (*Spool)(nil)
	_ BlobWriter      = (*Spool)(nil)
)
