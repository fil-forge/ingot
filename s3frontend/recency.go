package s3frontend

import (
	"container/list"
	"sync"
	"time"
)

// recencyMap is a bounded LRU of times, keyed by digest bytes: the backend's
// record of when it last renewed each cached blob.
type recencyMap struct {
	mu      sync.Mutex
	cap     int
	order   *list.List // front = most recently touched; values are *recencyEntry
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
	m.touchLocked(key, at)
}

func (m *recencyMap) touchLocked(key string, at time.Time) {
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

// claim records at as key's time and reports true, unless key's recorded
// time is less than within before at, which it leaves alone and reports
// false. The check and the record are one step, so of concurrent claims one
// wins.
func (m *recencyMap) claim(key string, at time.Time, within time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.entries[key]; ok && at.Sub(el.Value.(*recencyEntry).at) < within {
		return false
	}
	m.touchLocked(key, at)
	return true
}

// forget drops key's record.
func (m *recencyMap) forget(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.entries[key]; ok {
		m.order.Remove(el)
		delete(m.entries, key)
	}
}
