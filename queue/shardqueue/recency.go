package shardqueue

import (
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

// recencyMap is a bounded LRU of times, keyed by digest bytes: the queue's
// record of when it last renewed each cached blob.
type recencyMap struct {
	mu  sync.Mutex
	lru *simplelru.LRU[string, time.Time]
}

func newRecencyMap(capacity int) *recencyMap {
	lru, err := simplelru.NewLRU[string, time.Time](capacity, nil)
	if err != nil {
		// Only a capacity under one fails, and the queue's is a constant.
		panic(err)
	}
	return &recencyMap{lru: lru}
}

func (m *recencyMap) touch(key string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lru.Add(key, at)
}

func (m *recencyMap) get(key string) (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lru.Peek(key)
}

// claim records at as key's time and reports true, unless key's recorded
// time is less than within before at, which it leaves alone and reports
// false. The check and the record are one step, so of concurrent claims one
// wins.
func (m *recencyMap) claim(key string, at time.Time, within time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if last, ok := m.lru.Peek(key); ok && at.Sub(last) < within {
		return false
	}
	m.lru.Add(key, at)
	return true
}

// forget drops key's record.
func (m *recencyMap) forget(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lru.Remove(key)
}
