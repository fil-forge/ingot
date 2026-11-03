package revocation_test

import (
	"context"
	"iter"
	"sync"
	"testing"
	"time"

	jsg "github.com/alanshaw/dag-json-gen"
	"github.com/fil-forge/swarf/pkg/api"
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/revocation"
)

func testCid(t *testing.T, s string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.DagCBOR, mh)
}

func record(t *testing.T, name string, at time.Time) api.FirehoseRevocation {
	t.Helper()
	return api.FirehoseRevocation{
		Revoke:     testCid(t, name),
		Cause:      testCid(t, "cause-"+name),
		RecordedAt: jsg.DagJsonTime(at),
	}
}

// streamScript is one Stream call's yields: records then an optional error.
// A nil-error script simply ends (a server-side close).
type streamScript struct {
	records []api.FirehoseRevocation
	err     error
}

// fakeSource plays one streamScript per Stream call, recording the since
// cursor each call was made with. When the scripts run out, Stream blocks
// until ctx cancel (a healthy idle connection).
type fakeSource struct {
	mu      sync.Mutex
	scripts []streamScript
	sinces  []time.Time
}

func (f *fakeSource) Stream(ctx context.Context, since time.Time) iter.Seq2[api.FirehoseRevocation, error] {
	f.mu.Lock()
	f.sinces = append(f.sinces, since)
	var script *streamScript
	if len(f.scripts) > 0 {
		script = &f.scripts[0]
		f.scripts = f.scripts[1:]
	}
	f.mu.Unlock()
	return func(yield func(api.FirehoseRevocation, error) bool) {
		if script == nil {
			<-ctx.Done()
			return
		}
		for _, rec := range script.records {
			if !yield(rec, nil) {
				return
			}
		}
		if script.err != nil {
			yield(api.FirehoseRevocation{}, script.err)
		}
	}
}

func (f *fakeSource) calls() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.sinces...)
}

// fakeInvalidator records every Revoke call.
type fakeInvalidator struct {
	mu      sync.Mutex
	revoked []cid.Cid
}

func (f *fakeInvalidator) Revoke(revoked cid.Cid) []did.DID {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revoked = append(f.revoked, revoked)
	return nil
}

func (f *fakeInvalidator) calls() []cid.Cid {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]cid.Cid(nil), f.revoked...)
}

// run starts the consumer and returns a stop func that cancels and joins it.
func run(c *revocation.Consumer) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()
	return func() {
		cancel()
		<-done
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached in time")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestConsumerSinceDefaultsToNow(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	src := &fakeSource{}
	c := revocation.NewConsumer(src, inmem.NewMemStore(), &fakeInvalidator{},
		revocation.WithClock(func() time.Time { return now }))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) == 1 })
	require.Equal(t, now, src.calls()[0], "no stored cursor: subscribe from now")
}

func TestConsumerResumesFromStoredCursor(t *testing.T) {
	ctx := context.Background()
	stored := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	store := inmem.NewMemStore()
	require.NoError(t, store.PutRevocationCursor(ctx, registry.RevocationCursor{
		RecordedAt: stored, Revoke: testCid(t, "earlier"),
	}))

	src := &fakeSource{}
	c := revocation.NewConsumer(src, store, &fakeInvalidator{})
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) == 1 })
	require.Equal(t, stored, src.calls()[0], "stored cursor wins over now")
}

func TestConsumerInvalidatesAndPersistsCursor(t *testing.T) {
	ctx := context.Background()
	at1 := time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC)
	at2 := at1.Add(time.Second)
	rec1 := record(t, "dlg-1", at1)
	rec2 := record(t, "dlg-2", at2)

	store := inmem.NewMemStore()
	inv := &fakeInvalidator{}
	src := &fakeSource{scripts: []streamScript{{records: []api.FirehoseRevocation{rec1, rec2}}}}
	c := revocation.NewConsumer(src, store, inv,
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(inv.calls()) == 2 })
	require.Equal(t, []cid.Cid{rec1.Revoke, rec2.Revoke}, inv.calls())

	cur, err := store.GetRevocationCursor(ctx)
	require.NoError(t, err)
	require.True(t, cur.RecordedAt.Equal(at2), "cursor tracks the last record's recorded_at")
	require.True(t, cur.Revoke.Equals(rec2.Revoke))
}

func TestConsumerReconnectsFromLastRecord(t *testing.T) {
	at := time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC)
	rec := record(t, "dlg-1", at)

	src := &fakeSource{scripts: []streamScript{
		// First connection delivers one record then dies with an error.
		{records: []api.FirehoseRevocation{rec}, err: context.DeadlineExceeded},
		// Second connection ends cleanly (server close), forcing a third.
		{},
	}}
	c := revocation.NewConsumer(src, inmem.NewMemStore(), &fakeInvalidator{},
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) >= 3 })
	calls := src.calls()
	require.Equal(t, at, calls[1], "reconnect resumes from the last processed record")
	require.Equal(t, at, calls[2], "clean stream end also resumes from the cursor")
}

func TestConsumerStopsOnCancel(t *testing.T) {
	src := &fakeSource{} // no scripts: Stream blocks until ctx cancel
	c := revocation.NewConsumer(src, inmem.NewMemStore(), &fakeInvalidator{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()

	waitFor(t, func() bool { return len(src.calls()) == 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}
