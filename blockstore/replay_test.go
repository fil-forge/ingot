package blockstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestReplayFile_HoldsHashesAndReplays(t *testing.T) {
	dir := t.TempDir()
	rb, err := NewReplayBuffer(dir, 1<<20, 0)
	require.NoError(t, err)

	f, err := rb.Acquire(context.Background(), 11)
	require.NoError(t, err)
	_, err = f.Write([]byte("hello "))
	require.NoError(t, err)
	_, err = f.Write([]byte("world"))
	require.NoError(t, err)

	want := sha256.Sum256([]byte("hello world"))
	encoded, err := mh.Encode(want[:], mh.SHA2_256)
	require.NoError(t, err)
	digest, n, err := f.Digest()
	require.NoError(t, err)
	require.Equal(t, mh.Multihash(encoded), digest)
	require.EqualValues(t, 11, n)

	for range 2 {
		got, err := io.ReadAll(f.Reader())
		require.NoError(t, err)
		require.Equal(t, "hello world", string(got), "every replay starts from the first byte")
	}

	require.Equal(t, ReplayStats{Capacity: 1 << 20, Reserved: 11, Written: 11, Files: 1}, rb.Stats())
	require.NoError(t, f.Close())
	require.NoError(t, f.Close(), "closing twice is harmless")
	require.Equal(t, ReplayStats{Capacity: 1 << 20}, rb.Stats())
}

func TestReplayFile_LeavesNothingOnDisk(t *testing.T) {
	dir := t.TempDir()
	rb, err := NewReplayBuffer(dir, 0, 0)
	require.NoError(t, err)

	f, err := rb.Acquire(context.Background(), 4)
	require.NoError(t, err)
	_, err = f.Write([]byte("data"))
	require.NoError(t, err)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "the copy is anonymous while open")
	require.NoError(t, f.Close())
}

func TestReplayFile_DigestOfNothing(t *testing.T) {
	rb, err := NewReplayBuffer(t.TempDir(), 0, 0)
	require.NoError(t, err)
	f, err := rb.Acquire(context.Background(), 0)
	require.NoError(t, err)
	defer f.Close()
	digest, n, err := f.Digest()
	require.NoError(t, err)
	require.Nil(t, digest)
	require.Zero(t, n)
}

func TestReplayBuffer_WaitsForBudgetThenGivesUp(t *testing.T) {
	rb, err := NewReplayBuffer(t.TempDir(), 100, 50*time.Millisecond)
	require.NoError(t, err)
	ctx := context.Background()

	first, err := rb.Acquire(ctx, 80)
	require.NoError(t, err)

	_, err = rb.Acquire(ctx, 30)
	require.ErrorIs(t, err, ErrReplayBusy, "30 more bytes do not fit beside 80 of 100")

	// A release lets a waiter through.
	got := make(chan error, 1)
	go func() {
		f, err := rb.Acquire(ctx, 30)
		if err == nil {
			_ = f.Close()
		}
		got <- err
	}()
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, first.Close())
	require.NoError(t, <-got)
	require.Zero(t, rb.Stats().Reserved)
}

func TestReplayBuffer_NeverFits(t *testing.T) {
	rb, err := NewReplayBuffer(t.TempDir(), 100, 0)
	require.NoError(t, err)
	_, err = rb.Acquire(context.Background(), 101)
	require.ErrorIs(t, err, ErrReplayTooLarge)
}

func TestReplayBuffer_CallerCancelIsNotBusy(t *testing.T) {
	rb, err := NewReplayBuffer(t.TempDir(), 100, time.Minute)
	require.NoError(t, err)
	hold, err := rb.Acquire(context.Background(), 100)
	require.NoError(t, err)
	defer hold.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = rb.Acquire(ctx, 1)
	require.True(t, errors.Is(err, context.Canceled), "got %v", err)
	require.False(t, errors.Is(err, ErrReplayBusy))
}

func TestReplayBuffer_UnboundedNeverWaits(t *testing.T) {
	rb, err := NewReplayBuffer(t.TempDir(), 0, 0)
	require.NoError(t, err)
	f, err := rb.Acquire(context.Background(), 1<<40)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestReplayBuffer_ReportsMetrics(t *testing.T) {
	rb, err := NewReplayBuffer(t.TempDir(), 100, 20*time.Millisecond)
	require.NoError(t, err)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	reg, err := rb.RegisterMetrics(provider.Meter("test"))
	require.NoError(t, err)

	f, err := rb.Acquire(context.Background(), 60)
	require.NoError(t, err)
	_, err = f.Write([]byte("hello"))
	require.NoError(t, err)
	_, err = rb.Acquire(context.Background(), 60)
	require.ErrorIs(t, err, ErrReplayBusy)

	observed := func() map[string]int64 {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		got := map[string]int64{}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				switch d := m.Data.(type) {
				case metricdata.Gauge[int64]:
					got[m.Name] = d.DataPoints[0].Value
				case metricdata.Sum[int64]:
					got[m.Name] = d.DataPoints[0].Value
				}
			}
		}
		return got
	}
	require.Equal(t, map[string]int64{
		"ingot.replay.written":  5,
		"ingot.replay.reserved": 60,
		"ingot.replay.capacity": 100,
		"ingot.replay.files":    1,
		"ingot.replay.waits":    1,
		"ingot.replay.refusals": 1,
	}, observed())

	require.NoError(t, f.Close())
	got := observed()
	require.Zero(t, got["ingot.replay.written"])
	require.Zero(t, got["ingot.replay.files"])

	require.NoError(t, reg.Unregister())
}
