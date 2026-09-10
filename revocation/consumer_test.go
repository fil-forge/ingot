package revocation_test

import (
	"context"
	"iter"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/revocation"
)

// testTenant is the tenant every principal event in these tests names.
var testTenant = did.MustParse("did:plc:ewvi7nxzyoun6zhxrhs64oiz")

func testCid(t *testing.T, s string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.DagCBOR, mh)
}

// record builds a revocation event: it withdraws the named delegation.
func record(t *testing.T, name string, at time.Time) revocation.Event {
	t.Helper()
	return revocation.Event{
		Revoke:     testCid(t, name),
		Cause:      testCid(t, "cause-"+name),
		RecordedAt: at,
	}
}

// principalRecord builds a principal-invalidation event for the test tenant.
func principalRecord(t *testing.T, principal string, at time.Time) revocation.Event {
	t.Helper()
	return revocation.Event{
		Tenant:     testTenant,
		Principal:  principal,
		Cause:      testCid(t, "cause-"+principal),
		RecordedAt: at,
	}
}

// streamScript is one Stream call's yields: events then an optional error.
// A nil-error script simply ends (a server-side close).
type streamScript struct {
	records []revocation.Event
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

func (f *fakeSource) Stream(ctx context.Context, since time.Time) iter.Seq2[revocation.Event, error] {
	f.mu.Lock()
	f.sinces = append(f.sinces, since)
	var script *streamScript
	if len(f.scripts) > 0 {
		script = &f.scripts[0]
		f.scripts = f.scripts[1:]
	}
	f.mu.Unlock()
	return func(yield func(revocation.Event, error) bool) {
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
			yield(revocation.Event{}, script.err)
		}
	}
}

func (f *fakeSource) calls() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.sinces...)
}

// invCall is one recorded Invalidator call: a revoked CID for Revoke, a
// (tenant, principal) pair for InvalidatePrincipal.
type invCall struct {
	revoked   cid.Cid
	tenant    did.DID
	principal string
}

// fakeInvalidator records every call to either method, in order. affected is
// what both methods return: nil stands for an event matching nothing cached.
type fakeInvalidator struct {
	mu       sync.Mutex
	recorded []invCall
	affected []did.DID
}

func (f *fakeInvalidator) Revoke(revoked cid.Cid) []did.DID {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recorded = append(f.recorded, invCall{revoked: revoked})
	return f.affected
}

func (f *fakeInvalidator) InvalidatePrincipal(tenant did.DID, principal string) []did.DID {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recorded = append(f.recorded, invCall{tenant: tenant, principal: principal})
	return f.affected
}

// calls returns every recorded call, in the order the consumer made them.
func (f *fakeInvalidator) calls() []invCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]invCall(nil), f.recorded...)
}

// revocations returns just the revoked CIDs, in order.
func (f *fakeInvalidator) revocations() []cid.Cid {
	var out []cid.Cid
	for _, c := range f.calls() {
		if c.principal == "" {
			out = append(out, c.revoked)
		}
	}
	return out
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
	src := &fakeSource{scripts: []streamScript{{records: []revocation.Event{rec1, rec2}}}}
	c := revocation.NewConsumer(src, store, inv,
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(inv.calls()) == 2 })
	require.Equal(t, []cid.Cid{rec1.Revoke, rec2.Revoke}, inv.revocations())

	cur, err := store.GetRevocationCursor(ctx)
	require.NoError(t, err)
	require.True(t, cur.RecordedAt.Equal(at2), "cursor tracks the last record's recorded_at")
	require.True(t, cur.Revoke.Equals(rec2.Revoke))
}

func TestConsumerInvalidatesPrincipalAndPersistsCursor(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC)
	ev := principalRecord(t, "alice", at)

	store := inmem.NewMemStore()
	inv := &fakeInvalidator{affected: []did.DID{testTenant}}
	src := &fakeSource{scripts: []streamScript{{records: []revocation.Event{ev}}}}
	c := revocation.NewConsumer(src, store, inv,
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(inv.calls()) == 1 })
	require.Equal(t, []invCall{{tenant: testTenant, principal: "alice"}}, inv.calls(),
		"a principal event dispatches to InvalidatePrincipal, not Revoke")

	waitFor(t, func() bool {
		cur, err := store.GetRevocationCursor(ctx)
		return err == nil && cur.RecordedAt.Equal(at)
	})
	cur, err := store.GetRevocationCursor(ctx)
	require.NoError(t, err)
	require.False(t, cur.Revoke.Defined(), "a principal event revokes nothing, so the cursor holds no CID")
}

func TestConsumerPreservesInterleavedOrder(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC)
	rec1 := record(t, "dlg-1", at)
	prin1 := principalRecord(t, "alice", at.Add(time.Second))
	rec2 := record(t, "dlg-2", at.Add(2*time.Second))
	prin2 := principalRecord(t, "bob", at.Add(3*time.Second))

	store := inmem.NewMemStore()
	inv := &fakeInvalidator{}
	src := &fakeSource{scripts: []streamScript{
		{records: []revocation.Event{rec1, prin1, rec2, prin2}},
	}}
	c := revocation.NewConsumer(src, store, inv,
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(inv.calls()) == 4 })
	require.Equal(t, []invCall{
		{revoked: rec1.Revoke},
		{tenant: testTenant, principal: "alice"},
		{revoked: rec2.Revoke},
		{tenant: testTenant, principal: "bob"},
	}, inv.calls(), "both kinds dispatch in firehose order")

	waitFor(t, func() bool {
		cur, err := store.GetRevocationCursor(ctx)
		return err == nil && cur.RecordedAt.Equal(prin2.RecordedAt)
	})
}

func TestConsumerPrincipalNoMatchIsNoOp(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC)
	ev := principalRecord(t, "nobody", at)
	next := record(t, "dlg-1", at.Add(time.Second))

	store := inmem.NewMemStore()
	// affected stays nil: nothing cached belonged to the principal.
	inv := &fakeInvalidator{}
	src := &fakeSource{scripts: []streamScript{{records: []revocation.Event{ev, next}}}}
	c := revocation.NewConsumer(src, store, inv,
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	stop := run(c)
	defer stop()

	// The consumer keeps going and the cursor still advances past the
	// unmatched event, so it is not re-delivered on reconnect.
	waitFor(t, func() bool { return len(inv.calls()) == 2 })
	waitFor(t, func() bool {
		cur, err := store.GetRevocationCursor(ctx)
		return err == nil && cur.RecordedAt.Equal(next.RecordedAt)
	})
}

func TestConsumerReconnectsFromLastRecord(t *testing.T) {
	at := time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC)
	rec := record(t, "dlg-1", at)

	src := &fakeSource{scripts: []streamScript{
		// First connection delivers one record then dies with an error.
		{records: []revocation.Event{rec}, err: context.DeadlineExceeded},
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
