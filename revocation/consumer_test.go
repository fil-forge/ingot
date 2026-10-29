package revocation_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	jsg "github.com/alanshaw/dag-json-gen"
	"github.com/fil-forge/swarf/pkg/api"
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

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
		revocation.WithBackoff(time.Millisecond, time.Millisecond),
		revocation.WithSaveInterval(time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(inv.calls()) == 2 })
	require.Equal(t, []cid.Cid{rec1.Revoke, rec2.Revoke}, inv.calls())

	waitFor(t, func() bool {
		cur, err := store.GetRevocationCursor(ctx)
		return err == nil && cur.Revoke.Equals(rec2.Revoke)
	})
	cur, err := store.GetRevocationCursor(ctx)
	require.NoError(t, err)
	require.True(t, cur.RecordedAt.Equal(at2), "cursor tracks the last record's recorded_at")
}

// recordingCursorStore records every cursor it is asked to save. Like the
// Postgres store, it refuses a save whose ctx is already canceled. With
// release set, each save waits for release to close first; with failures
// set, that many saves fail before any succeeds.
type recordingCursorStore struct {
	mu       sync.Mutex
	saved    []registry.RevocationCursor
	release  chan struct{}
	failures int
}

func (s *recordingCursorStore) GetRevocationCursor(context.Context) (*registry.RevocationCursor, error) {
	return nil, registry.ErrNotFound
}

func (s *recordingCursorStore) PutRevocationCursor(ctx context.Context, cur registry.RevocationCursor) error {
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failures > 0 {
		s.failures--
		return errors.New("database unavailable")
	}
	s.saved = append(s.saved, cur)
	return nil
}

func (s *recordingCursorStore) calls() []registry.RevocationCursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]registry.RevocationCursor(nil), s.saved...)
}

// records returns n records one second apart, named any-dlg-0 onwards.
func records(t *testing.T, n int) []api.FirehoseRevocation {
	t.Helper()
	start := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	recs := make([]api.FirehoseRevocation, n)
	for i := range recs {
		recs[i] = record(t, fmt.Sprintf("any-dlg-%d", i), start.Add(time.Duration(i)*time.Second))
	}
	return recs
}

func TestConsumerAppliesEveryRecordWhileACursorSaveIsBlocked(t *testing.T) {
	recs := records(t, 3)
	store := &recordingCursorStore{release: make(chan struct{})}
	inv := &fakeInvalidator{}
	src := &fakeSource{scripts: []streamScript{{records: recs}}}
	c := revocation.NewConsumer(src, store, inv, revocation.WithSaveInterval(time.Millisecond))
	stop := run(c)
	defer stop()
	defer close(store.release)

	waitFor(t, func() bool { return len(inv.calls()) == len(recs) })
}

func TestConsumerSavesOnlyTheLastPositionWhenStoppedBeforeTheInterval(t *testing.T) {
	recs := records(t, 50)
	store := &recordingCursorStore{}
	inv := &fakeInvalidator{}
	src := &fakeSource{scripts: []streamScript{{records: recs}}}
	c := revocation.NewConsumer(src, store, inv, revocation.WithSaveInterval(time.Hour))
	stop := run(c)

	waitFor(t, func() bool { return len(inv.calls()) == len(recs) })
	stop()

	last := recs[len(recs)-1]
	require.Equal(t, []registry.RevocationCursor{{
		RecordedAt: last.RecordedAt.Time(),
		Revoke:     last.Revoke,
	}}, store.calls())
}

// A synctest.Test bubble's clock advances only once every goroutine in it is
// blocked, so after synctest.Wait a consumer run in one has done everything
// the elapsed intervals call for.

func TestConsumerSavesNothingMoreWhileTheFeedIsIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recs := records(t, 1)
		store := &recordingCursorStore{}
		src := &fakeSource{scripts: []streamScript{{records: recs}}}
		c := revocation.NewConsumer(src, store, &fakeInvalidator{},
			revocation.WithSaveInterval(time.Second))
		stop := run(c)
		defer stop()

		// Twenty intervals with no new record.
		time.Sleep(20 * time.Second)
		synctest.Wait()
		require.Len(t, store.calls(), 1)
	})
}

func TestConsumerRetriesAFailedSaveUntilItLands(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recs := records(t, 1)
		store := &recordingCursorStore{failures: 3}
		core, logs := observer.New(zap.InfoLevel)
		src := &fakeSource{scripts: []streamScript{{records: recs}}}
		c := revocation.NewConsumer(src, store, &fakeInvalidator{},
			revocation.WithSaveInterval(time.Second),
			revocation.WithLogger(zap.New(core)))
		stop := run(c)
		defer stop()

		// The feed is idle after the record, so only a later interval's
		// save can land it.
		time.Sleep(10 * time.Second)
		synctest.Wait()
		require.Equal(t, []registry.RevocationCursor{{
			RecordedAt: recs[0].RecordedAt.Time(),
			Revoke:     recs[0].Revoke,
		}}, store.calls())
		require.Equal(t, 1, logs.FilterMessage("revocation: persist cursor failed; retrying each interval").Len(),
			"the failures are logged once, when they start")
		require.Equal(t, 1, logs.FilterMessage("revocation: persist cursor recovered").Len())
	})
}

func TestConsumerLogsARecoveryAtStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recs := records(t, 1)
		store := &recordingCursorStore{failures: 1}
		core, logs := observer.New(zap.InfoLevel)
		src := &fakeSource{scripts: []streamScript{{records: recs}}}
		c := revocation.NewConsumer(src, store, &fakeInvalidator{},
			revocation.WithSaveInterval(time.Second),
			revocation.WithLogger(zap.New(core)))
		stop := run(c)

		// The first interval's save fails; the final save is the next one.
		time.Sleep(time.Second)
		synctest.Wait()
		stop()

		require.Len(t, store.calls(), 1, "the final save lands")
		require.Equal(t, 1, logs.FilterMessage("revocation: persist cursor recovered").Len())
	})
}

func TestConsumerLogsNoFailureForASaveCutShortByStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recs := records(t, 1)
		store := &recordingCursorStore{release: make(chan struct{})}
		core, logs := observer.New(zap.InfoLevel)
		src := &fakeSource{scripts: []streamScript{{records: recs}}}
		c := revocation.NewConsumer(src, store, &fakeInvalidator{},
			revocation.WithSaveInterval(time.Second),
			revocation.WithLogger(zap.New(core)))
		stop := run(c)

		// The first interval's save waits on release, so the stop cuts it
		// short; the final save then waits on release too.
		time.Sleep(time.Second)
		synctest.Wait()
		stopped := make(chan struct{})
		go func() { defer close(stopped); stop() }()
		synctest.Wait()
		close(store.release)
		<-stopped

		require.Len(t, store.calls(), 1, "the final save lands")
		require.Zero(t, logs.FilterLevelExact(zap.WarnLevel).Len(), "a save cut short by the stop is no failure")
	})
}

func TestConsumerIgnoresANonPositiveSaveInterval(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				recs := records(t, 1)
				store := &recordingCursorStore{}
				src := &fakeSource{scripts: []streamScript{{records: recs}}}
				c := revocation.NewConsumer(src, store, &fakeInvalidator{},
					revocation.WithSaveInterval(d))
				stop := run(c)

				synctest.Wait()
				stop()
				require.Len(t, store.calls(), 1, "the save at stop lands")
			})
		})
	}
}

// cancelingInvalidator cancels the consumer's context from inside the
// first Revoke, so the records after it arrive once the consumer is stopped.
type cancelingInvalidator struct {
	fakeInvalidator
	cancel context.CancelFunc
}

func (c *cancelingInvalidator) Revoke(revoked cid.Cid) []did.DID {
	c.cancel()
	return c.fakeInvalidator.Revoke(revoked)
}

func TestConsumerAppliesNoRecordAfterItIsStopped(t *testing.T) {
	recs := records(t, 3)
	ctx, cancel := context.WithCancel(context.Background())
	inv := &cancelingInvalidator{cancel: cancel}
	src := &fakeSource{scripts: []streamScript{{records: recs}}}
	c := revocation.NewConsumer(src, &recordingCursorStore{}, inv)

	c.Run(ctx)

	require.Equal(t, []cid.Cid{recs[0].Revoke}, inv.calls())
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
