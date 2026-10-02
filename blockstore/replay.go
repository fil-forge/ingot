package blockstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sync/atomic"
	"time"

	mh "github.com/multiformats/go-multihash"
	"golang.org/x/sync/semaphore"
)

// ErrReplayBusy is returned by ReplayBuffer.Acquire when the budget stayed
// full for the whole wait: the caller is asking to receive more body bytes
// than the node is prepared to hold for retries at once.
var ErrReplayBusy = errors.New("blockstore: replay buffer is full")

// ErrReplayTooLarge is returned by ReplayBuffer.Acquire for a blob that can
// never fit the budget, however long it waits.
var ErrReplayTooLarge = errors.New("blockstore: blob exceeds the replay buffer budget")

// ReplayBuffer holds, for each blob in flight to its provider, a copy of the
// bytes sent so far, so that a failed send can be repeated without asking the
// client for the body again. It is not a store: a copy lives only until its
// blob's send has succeeded or been given up on, and nothing reads it back
// except to resend.
//
// Each copy is an anonymous file — created, then unlinked at once — so a
// process exit of any kind leaves nothing on disk to sweep or to mistake for
// something worth keeping. The cost is that the files are invisible to ls and
// du; Stats reports what is held.
//
// The disk the buffer may use is bounded by a byte budget. A blob reserves its
// whole size before the first byte of its body is read, and releases it when
// its copy is closed, so a flood of uploads waits for the budget rather than
// filling the disk. Reserving the whole size up front, and never growing a
// reservation, means a blob that holds budget never waits for more: no two
// blobs can each hold part of what the other needs.
type ReplayBuffer struct {
	dir      string
	capacity int64 // 0 means unbounded
	wait     time.Duration
	sem      *semaphore.Weighted // nil when unbounded

	reserved atomic.Int64
	written  atomic.Int64
	files    atomic.Int64
}

// ReplayStats is a snapshot of a ReplayBuffer's usage.
type ReplayStats struct {
	// Capacity is the byte budget; 0 means unbounded.
	Capacity int64
	// Reserved is the budget held by open copies, in bytes.
	Reserved int64
	// Written is the bytes the open copies hold on disk so far.
	Written int64
	// Files is the number of open copies.
	Files int64
}

// NewReplayBuffer returns a buffer that keeps its copies under dir (created if
// missing; "" means the system temp dir). capacity is the byte budget, 0 for
// none. wait bounds how long Acquire waits for budget; 0 waits as long as the
// caller's context allows.
func NewReplayBuffer(dir string, capacity int64, wait time.Duration) (*ReplayBuffer, error) {
	if capacity < 0 {
		return nil, errors.New("blockstore: replay buffer capacity must not be negative")
	}
	if dir == "" {
		dir = os.TempDir()
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("blockstore: replay mkdir: %w", err)
	}
	r := &ReplayBuffer{dir: dir, capacity: capacity, wait: wait}
	if capacity > 0 {
		r.sem = semaphore.NewWeighted(capacity)
	}
	return r, nil
}

// Stats reports the buffer's current usage.
func (r *ReplayBuffer) Stats() ReplayStats {
	return ReplayStats{
		Capacity: r.capacity,
		Reserved: r.reserved.Load(),
		Written:  r.written.Load(),
		Files:    r.files.Load(),
	}
}

// Acquire reserves n bytes of budget and opens an empty copy for them. It
// waits for budget up to the buffer's wait (or the context), returning
// ErrReplayBusy if that passes and ErrReplayTooLarge if n could never fit.
// The caller must Close the copy.
func (r *ReplayBuffer) Acquire(ctx context.Context, n int64) (*ReplayFile, error) {
	if n < 0 {
		return nil, fmt.Errorf("blockstore: replay reservation of %d bytes", n)
	}
	if r.sem != nil {
		if n > r.capacity {
			return nil, fmt.Errorf("%w: %d bytes against a budget of %d", ErrReplayTooLarge, n, r.capacity)
		}
		waitCtx := ctx
		if r.wait > 0 {
			var cancel context.CancelFunc
			waitCtx, cancel = context.WithTimeout(ctx, r.wait)
			defer cancel()
		}
		if err := r.sem.Acquire(waitCtx, n); err != nil {
			if ctx.Err() == nil {
				return nil, fmt.Errorf("%w: waited %s for %d bytes", ErrReplayBusy, r.wait, n)
			}
			return nil, ctx.Err()
		}
	}
	f, err := r.create()
	if err != nil {
		if r.sem != nil {
			r.sem.Release(n)
		}
		return nil, err
	}
	r.reserved.Add(n)
	r.files.Add(1)
	return &ReplayFile{buf: r, f: f, reserved: n, hasher: sha256.New()}, nil
}

// create makes an anonymous file: named just long enough to be opened, then
// unlinked, so the open handle is all that keeps it.
func (r *ReplayBuffer) create() (*os.File, error) {
	f, err := os.CreateTemp(r.dir, "replay-*")
	if err != nil {
		return nil, fmt.Errorf("blockstore: replay tempfile: %w", err)
	}
	if err := os.Remove(f.Name()); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("blockstore: replay unlink: %w", err)
	}
	return f, nil
}

// ReplayFile is one blob's copy in a ReplayBuffer. It is written once, in
// order, as the blob is sent, hashing what it holds; it can then be read from
// the start as often as a resend needs. Not safe for concurrent use, except
// that Close may follow any other call.
type ReplayFile struct {
	buf      *ReplayBuffer
	f        *os.File
	reserved int64
	n        int64
	hasher   hash.Hash
	closed   atomic.Bool
}

// Write appends p, so a ReplayFile can sit in an io.MultiWriter beside the
// send. It never short-writes.
func (f *ReplayFile) Write(p []byte) (int, error) {
	n, err := f.f.Write(p)
	f.hasher.Write(p[:n])
	f.n += int64(n)
	f.buf.written.Add(int64(n))
	return n, err
}

// Digest returns the sha2-256 multihash of what has been written, and its
// length. The digest of nothing is nil.
func (f *ReplayFile) Digest() (mh.Multihash, int64, error) {
	if f.n == 0 {
		return nil, 0, nil
	}
	digest, err := mh.Encode(f.hasher.Sum(nil), mh.SHA2_256)
	if err != nil {
		return nil, f.n, fmt.Errorf("blockstore: replay digest: %w", err)
	}
	return digest, f.n, nil
}

// Reader returns a reader over everything written so far, from the first
// byte. Each call starts again from the beginning.
func (f *ReplayFile) Reader() io.Reader {
	return io.NewSectionReader(f.f, 0, f.n)
}

// Close discards the copy and returns its budget. It is safe to call more than
// once.
func (f *ReplayFile) Close() error {
	if !f.closed.CompareAndSwap(false, true) {
		return nil
	}
	err := f.f.Close()
	f.buf.written.Add(-f.n)
	f.buf.reserved.Add(-f.reserved)
	f.buf.files.Add(-1)
	if f.buf.sem != nil {
		f.buf.sem.Release(f.reserved)
	}
	return err
}
