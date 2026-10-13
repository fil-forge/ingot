package ingot

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/config"
)

// TestStopWaitsForSweepers: Stop cancels the background sweepers and returns
// only once they have exited, even while they sweep on every tick.
func TestStopWaitsForSweepers(t *testing.T) {
	// A 2ms session TTL makes the multipart sweeper tick every millisecond.
	s := newInmemServer(t, config.ServerConfig{MultipartSessionTTL: 2 * time.Millisecond})
	require.NoError(t, s.Start(t.Context()))
	// A sweeper that is still busy for a while after the cancellation:
	// Stop must not return before it does.
	var exited atomic.Bool
	s.goSweep(func() {
		<-s.sweepCtx.Done()
		time.Sleep(time.Second)
		exited.Store(true)
	})
	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Stop's error includes a timeout waiting for the sweepers.
	require.NoError(t, s.Stop(ctx))
	require.True(t, exited.Load(), "Stop returned before a sweeper exited")
}
