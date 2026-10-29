package ingot

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

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

// TestStopLogsNoListenerError: the listener's close is Stop's own doing, not
// a listener failure to report.
func TestStopLogsNoListenerError(t *testing.T) {
	s := newInmemServer(t, config.ServerConfig{})
	core, logs := observer.New(zap.ErrorLevel)
	s.logger = zap.New(core)
	served := make(chan struct{})
	go func() {
		defer close(served)
		s.serve()
	}()
	// A response, not just an accepted connection, means the listener is
	// serving, so Stop has it to close.
	client := &http.Client{Timeout: time.Second}
	require.Eventually(t, func() bool {
		resp, err := client.Get("http://" + s.cfg.Addr + "/health")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.Stop(ctx))
	<-served
	require.Empty(t, logs.All())
}
