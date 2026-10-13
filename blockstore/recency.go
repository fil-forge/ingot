package blockstore

import (
	"container/list"
	"sync"
	"time"
)

// recencyCapacity caps how many blobs' last-read times the cache remembers.
// The least recently read entry is dropped first.
const recencyCapacity = 65536

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

// forget drops key's entry, if any.
func (m *recencyMap) forget(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.entries[key]; ok {
		m.order.Remove(el)
		delete(m.entries, key)
	}
}
