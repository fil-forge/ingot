package blockstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/fil-forge/ucantone/did"
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
	// holds counts, per space and digest, the writes that still need the
	// blob's file: from its spool write until its upload has read it. Remove
	// leaves a held blob alone, so a release of the same digest by another
	// object never takes a file a write in flight is about to upload. The
	// space says whose write holds it: a release reads its own space's hold
	// as a write waiting for it, and another space's as a reason to leave
	// the file and nothing else.
	holdMu sync.Mutex
	holds  map[holdKey]int
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
	s := &Spool{blobDir: d, holds: map[holdKey]int{}}
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
func (s *Spool) WriteBlob(ctx context.Context, r io.Reader) (mh.Multihash, int64, error) {
	digest, n, _, err := s.writeBlob(ctx, r, nil)
	return digest, n, err
}

// WriteBlobHeld is WriteBlob for a write that needs the blob's file after
// the write, to upload it: the file is published already held for space
// (see Hold), under the same lock as its rename, so no removal can take it
// between the two. unhold ends the hold; it is nil when nothing was
// written.
func (s *Spool) WriteBlobHeld(ctx context.Context, r io.Reader, space did.DID) (digest mh.Multihash, n int64, unhold func(), err error) {
	return s.writeBlob(ctx, r, &space)
}

// writeBlob is WriteBlob and WriteBlobHeld: hold, when set, is the space
// whose hold the committed file is published under.
func (s *Spool) writeBlob(_ context.Context, r io.Reader, hold *did.DID) (mh.Multihash, int64, func(), error) {
	tmp, err := os.CreateTemp(s.dir, spoolTempPrefix+"*")
	if err != nil {
		return nil, 0, nil, fmt.Errorf("blockstore: spool tempfile: %w", err)
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
		return nil, n, nil, fmt.Errorf("blockstore: spool write: %w", copyErr)
	}
	if n == 0 {
		_ = os.Remove(tmpName)
		return nil, 0, nil, nil
	}
	digest, err := mh.Encode(hasher.Sum(nil), mh.SHA2_256)
	if err != nil {
		_ = os.Remove(tmpName)
		return nil, n, nil, fmt.Errorf("blockstore: spool digest: %w", err)
	}
	unhold, err := s.commit(tmpName, digest, counted.n, hold)
	if err != nil {
		_ = os.Remove(tmpName)
		return nil, n, nil, err
	}
	return digest, n, unhold, nil
}

// commit renames a finished temp file to its digest path and counts its size
// as finished, unless an identical blob already sat there (counted already),
// and takes the hold for the space in hold, if any, before the file is
// visible under its digest. It holds the directory exclusively, so the
// check, the rename and the hold never overlap a Remove or a Take of the
// same digest, which the same bytes stored as received and written again
// can race: the count sees either the old file replaced in place or the new
// file arriving after the old one left, and a removal sees the file held or
// absent.
func (s *Spool) commit(tmpName string, digest mh.Multihash, size int64, hold *did.DID) (unhold func(), err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.Path(digest)
	_, statErr := os.Stat(path)
	existed := statErr == nil
	if err := os.Rename(tmpName, path); err != nil {
		return nil, fmt.Errorf("blockstore: spool rename: %w", err)
	}
	if !existed {
		s.usage.Add(size)
	}
	s.changes.Add(1)
	if hold != nil {
		unhold = s.holdLocked(holdKey{*hold, string(digest)})
	}
	return unhold, nil
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

// holdKey names one space's holds on one digest.
type holdKey struct {
	space  did.DID
	digest string
}

// Hold marks the blob with the given digest as needed by a write in flight
// for space, so Remove leaves its file alone, and returns the function that
// ends the hold. Ending it twice is harmless. A write holds each blob it
// spools from the spool write until its upload has read the file, which is
// what keeps the same bytes written as received by two objects safe: a
// release of the first object's blob can run while the second write is
// between spooling and uploading it.
//
// Hold takes the directory shared, as Remove takes it exclusively, so a hold
// never lands between Remove's check and its unlink.
func (s *Spool) Hold(space did.DID, digest mh.Multihash) (unhold func()) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.holdLocked(holdKey{space, string(digest)})
}

// holdLocked is Hold for a caller that holds mu, shared or exclusively.
func (s *Spool) holdLocked(k holdKey) (unhold func()) {
	s.holdMu.Lock()
	s.holds[k]++
	s.holdMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.holdMu.Lock()
			defer s.holdMu.Unlock()
			if s.holds[k] <= 1 {
				delete(s.holds, k)
			} else {
				s.holds[k]--
			}
		})
	}
}

// Held reports whether a write in flight, in any space, holds the blob.
func (s *Spool) Held(digest mh.Multihash) bool {
	s.holdMu.Lock()
	defer s.holdMu.Unlock()
	for k := range s.holds {
		if k.digest == string(digest) {
			return true
		}
	}
	return false
}

// HeldBy reports whether a write in flight for space holds the blob.
func (s *Spool) HeldBy(space did.DID, digest mh.Multihash) bool {
	s.holdMu.Lock()
	defer s.holdMu.Unlock()
	return s.holds[holdKey{space, string(digest)}] > 0
}

// ErrHeld is Remove's answer for a blob a write in flight holds (Hold): the
// file is left as it is and nothing is freed. A caller that would record
// the file as gone (an evicted mark, a deleted intent) must not.
var ErrHeld = errors.New("blockstore: blob held by a write in flight")

// Remove deletes the blob with the given digest, as blobDir.Remove does,
// unless a write in flight holds it (Hold): it then returns ErrHeld, having
// freed nothing. It holds the directory exclusively, so the check and the
// unlink admit no hold or commit between them.
func (s *Spool) Remove(digest mh.Multihash) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Held(digest) {
		return 0, ErrHeld
	}
	return s.removeLocked(digest)
}
