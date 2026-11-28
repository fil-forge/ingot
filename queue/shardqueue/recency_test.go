package shardqueue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	require.Equal(t, [3]bool{true, false, true}, [3]bool{hasA, hasB, hasC}, "remembered [a b c]")
}
