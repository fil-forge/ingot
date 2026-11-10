package preload

import (
	"context"
	"sync"
)

// watermark tracks one bucket's bulk objects while several workers write
// them. take hands out counters in order, and the watermark advances past
// every counter below it that has completed, so a load that stops can resume
// from the watermark with nothing below it missing.
type watermark struct {
	mu       sync.Mutex
	next     uint64              // the next counter take hands out
	done     uint64              // every counter below this has completed
	limit    uint64              // take hands out counters below this
	finished map[uint64]struct{} // completed counters at or above done
	advanced chan struct{}       // closed, and replaced, when done advances
}

func newWatermark(done, limit uint64) *watermark {
	return &watermark{
		next:     done,
		done:     done,
		limit:    limit,
		finished: make(map[uint64]struct{}),
		advanced: make(chan struct{}),
	}
}

// take returns the next counter to write, or false once every counter below
// the limit has been handed out.
func (w *watermark) take() (uint64, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.next >= w.limit {
		return 0, false
	}
	n := w.next
	w.next++
	return n, true
}

// complete records that counter n was written.
func (w *watermark) complete(n uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if n < w.done {
		return
	}
	w.finished[n] = struct{}{}
	before := w.done
	for {
		if _, ok := w.finished[w.done]; !ok {
			break
		}
		delete(w.finished, w.done)
		w.done++
	}
	if w.done != before {
		close(w.advanced)
		w.advanced = make(chan struct{})
	}
}

// Done returns the watermark: every counter below it has completed.
func (w *watermark) Done() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.done
}

// wait blocks until the watermark reaches target or ctx ends.
func (w *watermark) wait(ctx context.Context, target uint64) error {
	for {
		w.mu.Lock()
		done, advanced := w.done, w.advanced
		w.mu.Unlock()
		if done >= target {
			return nil
		}
		select {
		case <-advanced:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
