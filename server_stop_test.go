package ingot

import (
	"context"
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
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Stop's error includes a timeout waiting for the sweepers.
	require.NoError(t, s.Stop(ctx))
	require.ErrorIs(t, s.sweepCtx.Err(), context.Canceled)
}
