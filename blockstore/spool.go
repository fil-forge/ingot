package blockstore

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// Spool is the local on-disk blob store (docs/architecture.md §5): each
// object-body blob is written here, keyed by its sha256 digest, before it is
// uploaded to Forge. The local copy is the read-after-write copy: a
// just-written blob is served straight from disk, skipping the network read
// tier, until it is removed. Network reads never refill the spool.
//
// Spool is deliberately pure file I/O: a blockstore.BlockReader plus the
// streaming BlobReader/BlobWriter, a running byte count of its blob files, and
// the time each blob was last read from it. The lifecycle of a blob (the
// upload_intents state machine, eviction policy) is owned by the caller that
// has the registry handle — blockstore cannot import registry without a cycle
// (registry imports blockstore for the segment-metadata types).
type Spool struct {
	dir string
	// usage is the byte count of the finished blob files. In-flight .tmp-*
	// files are never counted.
	usage atomic.Int64
	reads *recencyMap
}

// spoolTempPrefix names the files WriteBlob streams into before the rename to
// the digest path.
const spoolTempPrefix = ".tmp-"

// recencyCapacity caps how many blobs' last-read times the spool remembers.
// The least recently read entry is dropped first.
const recencyCapacity = 65536

// NewSpool opens (creating if needed) a spool rooted at dir. It deletes every
// leftover .tmp-* file (a write the previous process never finished; nothing
// can be writing one before the listener starts) and sums the blob files into
// the usage count. Entries that are neither, such as lost+found on a dedicated
// filesystem, are ignored.
func NewSpool(dir string) (*Spool, error) {
	if dir == "" {
		return nil, errors.New("blockstore: spool dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("blockstore: spool mkdir: %w", err)
	}
	s := &Spool{dir: dir, reads: newRecencyMap(recencyCapacity)}
	var removeErr error
	total, err := s.Scan(func(e SpoolEntry) {
		if e.Digest != nil || removeErr != nil {
			return
		}
		removeErr = s.RemoveTemp(e.Name)
	})
	if err != nil {
		return nil, err
	}
	if removeErr != nil {
		return nil, removeErr
	}
	s.usage.Store(total)
	return s, nil
}

// SpoolEntry is one file Scan found: a finished blob (Digest set) or a .tmp-*
// file (Digest nil).
type SpoolEntry struct {
	Name    string
	Digest  mh.Multihash
	Size    int64
	ModTime time.Time
}

// Scan walks the spool directory and calls fn for each blob file and each
// .tmp-* file, skipping directories and names that are neither. It returns
// the total size of the blob files it saw. A file removed while the scan runs
// is skipped.
func (s *Spool) Scan(fn func(SpoolEntry)) (int64, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0, fmt.Errorf("blockstore: spool scan: %w", err)
	}
	var total int64
	for _, de := range entries {
		if !de.Type().IsRegular() {
			continue
		}
		name := de.Name()
		var digest mh.Multihash
		if !strings.HasPrefix(name, spoolTempPrefix) {
			digest = digestFromName(name)
			if digest == nil {
				continue
			}
		}
		info, err := de.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("blockstore: spool stat %s: %w", name, err)
		}
		if digest != nil {
			total += info.Size()
		}
		fn(SpoolEntry{Name: name, Digest: digest, Size: info.Size(), ModTime: info.ModTime()})
	}
	return total, nil
}

// digestFromName decodes a spool file name back into the multihash it was
// written under, or nil when the name is not a hex multihash.
func digestFromName(name string) mh.Multihash {
	raw, err := hex.DecodeString(name)
	if err != nil {
		return nil
	}
	digest, err := mh.Cast(raw)
	if err != nil {
		return nil
	}
	return digest
}

// Usage returns the byte count of the spool's finished blob files.
func (s *Spool) Usage() int64 {
	return s.usage.Load()
}

// ResetUsage replaces the usage count, for a caller that has just measured it
// with Scan. A write or remove that lands during the scan can leave the count
// off by that file's size until the next reset.
func (s *Spool) ResetUsage(n int64) {
	s.usage.Store(n)
}

// LastRead reports when the blob with the given digest was last served from
// the spool by this process, if it is still remembered.
func (s *Spool) LastRead(digest mh.Multihash) (time.Time, bool) {
	return s.reads.get(string(digest))
}

// RemoveTemp deletes one .tmp-* file by name. Idempotent. It refuses any other
// name, so it cannot be used to delete a finished blob behind the usage count.
func (s *Spool) RemoveTemp(name string) error {
	if !strings.HasPrefix(name, spoolTempPrefix) || filepath.Base(name) != name {
		return fmt.Errorf("blockstore: spool remove temp: %q is not a spool temp file", name)
	}
	err := os.Remove(filepath.Join(s.dir, name))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("blockstore: spool remove temp: %w", err)
	}
	return nil
}

// Path returns the on-disk path a blob with the given digest is stored at.
// Exposed so the caller can record it in upload_intents.local_path and hand it
// to the body uploader without re-deriving the layout.
func (s *Spool) Path(digest mh.Multihash) string {
	return filepath.Join(s.dir, hex.EncodeToString(digest))
}

// Remove deletes the blob with the given digest from the spool and returns how
// many bytes it freed. Idempotent: removing a blob that isn't spooled frees
// nothing and is not an error. Callers own the is-it-safe-to-delete question
// (shared, content-addressed blobs may be referenced by other parts or
// committed objects).
func (s *Spool) Remove(digest mh.Multihash) (int64, error) {
	path := s.Path(digest)
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("blockstore: spool remove: %w", err)
	}
	err = os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		// A concurrent Remove deleted it and counted it.
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("blockstore: spool remove: %w", err)
	}
	s.usage.Add(-info.Size())
	return info.Size(), nil
}

// WriteBlob streams r to the spool, computing its sha256 digest as it writes so
// the blob is never held whole in memory (object-body blobs run up to
// max_blob_size ≈ 254 MiB; buffering them would put that × concurrency in RAM).
// The write is atomic (temp file → rename to the digest path), so a crash leaves
// no partial blob readable under its digest. An empty r writes nothing and
// returns a nil digest with n == 0 (a zero-byte object has no blob). Re-writing
// an identical blob is idempotent (same digest, rename overwrites in place).
func (s *Spool) WriteBlob(_ context.Context, r io.Reader) (mh.Multihash, int64, error) {
	tmp, err := os.CreateTemp(s.dir, spoolTempPrefix+"*")
	if err != nil {
		return nil, 0, fmt.Errorf("blockstore: spool tempfile: %w", err)
	}
	tmpName := tmp.Name()
	hasher := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(tmp, hasher), r)
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
	// An identical rewrite replaces a file already counted.
	path := s.Path(digest)
	_, statErr := os.Stat(path)
	existed := statErr == nil
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return nil, n, fmt.Errorf("blockstore: spool rename: %w", err)
	}
	if !existed {
		s.usage.Add(n)
	}
	return digest, n, nil
}

// OpenBlob returns a streaming reader over the spooled blob with the given
// digest, or ErrNotFound. Unlike GetBlock it does not read the blob into memory —
// the body read path serves bytes straight off disk. The caller owns the reader
// and must Close it. The returned *os.File is seekable, which the body reader
// uses to start a ranged read mid-blob without reading-and-discarding.
func (s *Spool) OpenBlob(_ context.Context, _ did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	f, err := os.Open(s.Path(digest))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blockstore: spool open %s: %w", digest.B58String(), err)
	}
	s.reads.touch(string(digest), time.Now())
	return f, nil
}

// OpenBlobRange returns a reader over stored bytes [start, end] (inclusive)
// of the spooled blob, or ErrNotFound — OpenBlob restricted to a section,
// for the decrypting read path, which fetches only the ciphertext span a
// plaintext range needs. An end past the file's end yields a shorter stream.
func (s *Spool) OpenBlobRange(_ context.Context, _ did.DID, digest mh.Multihash, start, end int64) (io.ReadCloser, error) {
	f, err := os.Open(s.Path(digest))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blockstore: spool open %s: %w", digest.B58String(), err)
	}
	s.reads.touch(string(digest), time.Now())
	return readerCloser{Reader: io.NewSectionReader(f, start, end-start+1), Closer: f}, nil
}

// GetBlock returns the blob stored under c's multihash, or ErrNotFound. A miss
// is expected and cheap: it lets the layered read path fall through to the log
// (for catalog blocks, which are never spooled) or the network tier (for a body
// blob that has been evicted).
func (s *Spool) GetBlock(_ context.Context, _ did.DID, c cid.Cid) (block.Block, error) {
	data, err := os.ReadFile(s.Path(c.Hash()))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blockstore: spool read %s: %w", c, err)
	}
	s.reads.touch(string(c.Hash()), time.Now())
	return block.NewBlockWithCid(data, c)
}

// recencyMap is a bounded LRU of last-read times, keyed by digest bytes.
type recencyMap struct {
	mu      sync.Mutex
	cap     int
	order   *list.List // front = most recently read; values are *recencyEntry
	entries map[string]*list.Element
}

type recencyEntry struct {
	key string
	at  time.Time
}

func newRecencyMap(capacity int) *recencyMap {
	return &recencyMap{cap: capacity, order: list.New(), entries: map[string]*list.Element{}}
}

func (m *recencyMap) touch(key string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.entries[key]; ok {
		el.Value.(*recencyEntry).at = at
		m.order.MoveToFront(el)
		return
	}
	m.entries[key] = m.order.PushFront(&recencyEntry{key: key, at: at})
	if m.order.Len() > m.cap {
		oldest := m.order.Back()
		m.order.Remove(oldest)
		delete(m.entries, oldest.Value.(*recencyEntry).key)
	}
}

func (m *recencyMap) get(key string) (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	el, ok := m.entries[key]
	if !ok {
		return time.Time{}, false
	}
	return el.Value.(*recencyEntry).at, true
}

// Compile-time assertions: Spool is a raw-block read tier and the streaming
// blob tier for object bodies.
var (
	_ BlockReader     = (*Spool)(nil)
	_ BlobReader      = (*Spool)(nil)
	_ BlobRangeReader = (*Spool)(nil)
	_ BlobWriter      = (*Spool)(nil)
)
