package shardqueue

import (
	"testing"
	"time"
)

func TestRecencyMapDropsLeastRecentlyTouched(t *testing.T) {
	m := newRecencyMap(2)
	now := time.Now()
	m.touch("a", now)
	m.touch("b", now)
	m.touch("a", now) // a is now the most recent
	m.touch("c", now) // evicts b
	_, hasA := m.get("a")
	_, hasB := m.get("b")
	_, hasC := m.get("c")
	if got := [3]bool{hasA, hasB, hasC}; got != [3]bool{true, false, true} {
		t.Fatalf("remembered [a b c] = %v, want [true false true]", got)
	}
}
