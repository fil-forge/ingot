package revocation_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	jsg "github.com/alanshaw/dag-json-gen"
	"github.com/fil-forge/swarf/pkg/api"
	swarfclient "github.com/fil-forge/swarf/pkg/client"
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/revocation"
)

func testCid(t *testing.T, s string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.DagCBOR, mh)
}

func record(t *testing.T, name string, at time.Time) swarfclient.Event {
	t.Helper()
	return swarfclient.Event{
		Revocation: api.FirehoseRevocation{
			Revoke:     testCid(t, name),
			Cause:      testCid(t, "cause-"+name),
			RecordedAt: jsg.DagJsonTime(at),
		},
		Cursor: swarfclient.Cursor("cursor-" + name),
	}
}

// streamScript is one stream call's yields: records then an optional error.
// A nil-error script simply ends, which the swarf client never does (it
// reconnects an established stream itself and yields ctx.Err() on cancel);
// the consumer reconnects either way.
type streamScript struct {
	records []swarfclient.Event
	err     error
}

// testHead is the firehose head a fakeSource reports once its headErrs run
// out.
const testHead = swarfclient.Cursor("cursor-head")

// fakeSource plays one streamScript per StreamAfter call, recording the
// cursor each was opened after. When the scripts run out, a stream blocks
// until ctx cancel (a healthy idle connection), then yields ctx's error, as
// the swarf client does. Head fails with each of headErrs in turn, then
// reports testHead.
type fakeSource struct {
	mu       sync.Mutex
	scripts  []streamScript
	opened   []swarfclient.Cursor
	headErrs []error
	heads    int
}

func (f *fakeSource) Head(context.Context) (swarfclient.Cursor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heads++
	if len(f.headErrs) > 0 {
		err := f.headErrs[0]
		f.headErrs = f.headErrs[1:]
		return "", err
	}
	return testHead, nil
}

func (f *fakeSource) headCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.heads
}

func (f *fakeSource) StreamAfter(ctx context.Context, after swarfclient.Cursor) iter.Seq2[swarfclient.Event, error] {
	f.mu.Lock()
	f.opened = append(f.opened, after)
	var script *streamScript
	if len(f.scripts) > 0 {
		script = &f.scripts[0]
		f.scripts = f.scripts[1:]
	}
	f.mu.Unlock()
	return func(yield func(swarfclient.Event, error) bool) {
		if script == nil {
			<-ctx.Done()
			yield(swarfclient.Event{}, ctx.Err())
			return
		}
		for _, rec := range script.records {
			if !yield(rec, nil) {
				return
			}
		}
		if script.err != nil {
			yield(swarfclient.Event{}, script.err)
		}
	}
}

func (f *fakeSource) calls() []swarfclient.Cursor {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]swarfclient.Cursor(nil), f.opened...)
}

// fakeInvalidator records every Revoke and ClearAll call, running onClear,
// when set, on each ClearAll.
type fakeInvalidator struct {
	mu      sync.Mutex
	revoked []cid.Cid
	cleared int
	onClear func()
}

func (f *fakeInvalidator) ClearAll() int {
	if f.onClear != nil {
		f.onClear()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleared++
	return 0
}

func (f *fakeInvalidator) clears() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cleared
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

func TestConsumerStartsAtTheHeadWithNoCursor(t *testing.T) {
	ctx := context.Background()
	store := registry.NewPostgres(pgtest.New(t))
	src := &fakeSource{}
	c := revocation.NewConsumer(src, store, &fakeInvalidator{})
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) == 1 })
	require.Equal(t, testHead, src.calls()[0], "no stored cursor: start at the head as the service sees it")
	// Saved before the stream opened, so a restart before the first record
	// resumes from it rather than from a later head.
	cur, err := store.GetRevocationCursor(ctx)
	require.NoError(t, err)
	require.Equal(t, registry.RevocationCursor{Token: string(testHead)}, *cur)
}

func TestConsumerResumesAfterStoredCursor(t *testing.T) {
	ctx := context.Background()
	store := registry.NewPostgres(pgtest.New(t))
	require.NoError(t, store.PutRevocationCursor(ctx, registry.RevocationCursor{Token: "cursor-earlier"}))

	src := &fakeSource{}
	c := revocation.NewConsumer(src, store, &fakeInvalidator{})
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) == 1 })
	require.Equal(t, swarfclient.Cursor("cursor-earlier"), src.calls()[0])
	require.Zero(t, src.headCalls(), "a stored cursor needs no head")
}

// Until the service answers, the consumer keeps asking for the head; it has
// no earlier position to start from.
func TestConsumerRetriesTheHeadUntilTheServiceAnswers(t *testing.T) {
	src := &fakeSource{headErrs: []error{
		errors.New("connection refused"),
		errors.New("connection refused"),
	}}
	core, logs := observer.New(zap.WarnLevel)
	c := revocation.NewConsumer(src, registry.NewPostgres(pgtest.New(t)), &fakeInvalidator{},
		revocation.WithLogger(zap.New(core)),
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) == 1 })
	require.Equal(t, testHead, src.calls()[0])
	require.Equal(t, 3, src.headCalls())
	require.Len(t, logs.FilterMessage("revocation: read firehose head").All(), 2)
}

func TestConsumerStopsWhileWaitingForTheHead(t *testing.T) {
	src := &fakeSource{headErrs: []error{errors.New("connection refused")}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c := revocation.NewConsumer(src, registry.NewPostgres(pgtest.New(t)), &fakeInvalidator{},
		revocation.WithBackoff(time.Hour, time.Hour))
	go func() { defer close(done); c.Run(ctx) }()

	waitFor(t, func() bool { return src.headCalls() == 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
	require.Empty(t, src.calls())
}

// A cursor that can't be read is retried until it can, never replaced by the
// head: the head would skip what was recorded after the stored cursor.
func TestConsumerRetriesACursorItCouldNotRead(t *testing.T) {
	store := &recordingCursorStore{
		getErrs: []error{errors.New("database unavailable"), errors.New("database unavailable")},
		stored:  &registry.RevocationCursor{Token: "cursor-stored"},
	}
	src := &fakeSource{}
	core, logs := observer.New(zap.WarnLevel)
	c := revocation.NewConsumer(src, store, &fakeInvalidator{},
		revocation.WithLogger(zap.New(core)),
		revocation.WithBackoff(time.Millisecond, time.Millisecond),
		revocation.WithSaveInterval(time.Millisecond))
	stop := run(c)

	waitFor(t, func() bool { return len(src.calls()) == 1 })
	require.Equal(t, swarfclient.Cursor("cursor-stored"), src.calls()[0])
	require.Zero(t, src.headCalls())
	loads := logs.FilterMessage("revocation: load cursor").All()
	require.Len(t, loads, 2)
	require.Equal(t, zap.ErrorLevel, loads[0].Level, "no revocation is applied until the cursor can be read")
	stop()
	require.Empty(t, store.calls(), "nothing new to save")
}

// A head the consumer fails to save is saved by the background saver, so a
// restart before the first record still resumes from it.
func TestConsumerRetriesAFailedHeadSave(t *testing.T) {
	store := &recordingCursorStore{putErrs: []error{errors.New("database unavailable")}}
	src := &fakeSource{}
	c := revocation.NewConsumer(src, store, &fakeInvalidator{},
		revocation.WithSaveInterval(time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(store.calls()) == 1 })
	require.Equal(t, []registry.RevocationCursor{{Token: string(testHead)}}, store.calls())
	waitFor(t, func() bool { return len(src.calls()) == 1 })
	require.Equal(t, testHead, src.calls()[0])
}

// storedCursor returns a store holding a cursor from before a refusal.
func storedCursor(t *testing.T) *registry.Postgres {
	t.Helper()
	store := registry.NewPostgres(pgtest.New(t))
	require.NoError(t, store.PutRevocationCursor(context.Background(), registry.RevocationCursor{Token: "cursor-moved"}))
	return store
}

// refusal is a stream script the service refuses.
var refusal = streamScript{err: fmt.Errorf("opening revocation stream: %w", swarfclient.ErrCursorRefused)}

// After a refusal the consumer waits out its backoff before it reconnects,
// so a service that keeps refusing isn't asked again in a tight loop.
func TestConsumerWaitsBeforeReconnectingAfterARefusal(t *testing.T) {
	src := &fakeSource{scripts: []streamScript{refusal, refusal}}
	inv := &fakeInvalidator{}
	c := revocation.NewConsumer(src, storedCursor(t), inv,
		revocation.WithBackoff(time.Hour, time.Hour))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return inv.clears() >= 1 })
	time.Sleep(20 * time.Millisecond)
	require.Len(t, src.calls(), 1, "reconnected without waiting")
}

// The head read after a refusal is retried until the service answers, and
// the caches are cleared only once it has.
func TestConsumerRetriesTheHeadAfterARefusal(t *testing.T) {
	src := &fakeSource{
		scripts:  []streamScript{refusal},
		headErrs: []error{errors.New("connection refused")},
	}
	var headsAtClear atomic.Int32
	inv := &fakeInvalidator{onClear: func() { headsAtClear.Store(int32(src.headCalls())) }}
	c := revocation.NewConsumer(src, storedCursor(t), inv,
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) == 2 })
	require.Equal(t, testHead, src.calls()[1])
	require.Equal(t, 2, src.headCalls())
	require.Equal(t, 1, inv.clears())
	require.Equal(t, int32(2), headsAtClear.Load(), "cleared before the head was read")
}

// A cursor the service refuses is logged as an error, and the consumer clears
// every access key's caches, then starts over at the head and saves it.
// Retrying the refused cursor would skip every record recorded before new
// records passed it.
func TestConsumerStartsOverAtTheHeadWhenItsCursorIsRefused(t *testing.T) {
	ctx := context.Background()
	store := storedCursor(t)
	src := &fakeSource{scripts: []streamScript{refusal}}
	inv := &fakeInvalidator{}
	core, logs := observer.New(zap.WarnLevel)
	c := revocation.NewConsumer(src, store, inv,
		revocation.WithLogger(zap.New(core)),
		revocation.WithBackoff(time.Millisecond, time.Millisecond),
		revocation.WithSaveInterval(time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) == 2 })
	require.Equal(t, []swarfclient.Cursor{"cursor-moved", testHead}, src.calls())
	require.Equal(t, 1, inv.clears(), "a refusal clears every key's caches once")
	refused := logs.FilterMessage("revocation: cursor refused").All()
	require.Len(t, refused, 1)
	require.Equal(t, zap.ErrorLevel, refused[0].Level)
	fields := refused[0].ContextMap()
	require.Equal(t, "cursor-moved", fields["cursor"])
	require.NotEmpty(t, fields["action"])

	waitFor(t, func() bool {
		cur, err := store.GetRevocationCursor(ctx)
		return err == nil && cur.Token == string(testHead)
	})
	cur, err := store.GetRevocationCursor(ctx)
	require.NoError(t, err)
	require.Equal(t, registry.RevocationCursor{Token: string(testHead)}, *cur)
}

func TestConsumerInvalidatesAndPersistsCursor(t *testing.T) {
	ctx := context.Background()
	at1 := time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC)
	at2 := at1.Add(time.Second)
	rec1 := record(t, "dlg-1", at1)
	rec2 := record(t, "dlg-2", at2)

	store := registry.NewPostgres(pgtest.New(t))
	inv := &fakeInvalidator{}
	src := &fakeSource{scripts: []streamScript{{records: []swarfclient.Event{rec1, rec2}}}}
	c := revocation.NewConsumer(src, store, inv,
		revocation.WithBackoff(time.Millisecond, time.Millisecond),
		revocation.WithSaveInterval(time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(inv.calls()) == 2 })
	require.Equal(t, []cid.Cid{rec1.Revocation.Revoke, rec2.Revocation.Revoke}, inv.calls())

	waitFor(t, func() bool {
		cur, err := store.GetRevocationCursor(ctx)
		return err == nil && cur.Token == string(rec2.Cursor)
	})
}

// recordingCursorStore records every cursor it is asked to save. Like the
// Postgres store, it refuses a save whose ctx is already canceled. With
// release set, each save waits for release to close first. Reads fail with
// each of getErrs in turn, then report stored as the saved cursor, or no
// cursor; saves fail with each of putErrs in turn, recording nothing.
type recordingCursorStore struct {
	mu      sync.Mutex
	saved   []registry.RevocationCursor
	release chan struct{}
	stored  *registry.RevocationCursor
	getErrs []error
	putErrs []error
}

func (s *recordingCursorStore) GetRevocationCursor(context.Context) (*registry.RevocationCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.getErrs) > 0 {
		err := s.getErrs[0]
		s.getErrs = s.getErrs[1:]
		return nil, err
	}
	if s.stored != nil {
		cur := *s.stored
		return &cur, nil
	}
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
	if len(s.putErrs) > 0 {
		err := s.putErrs[0]
		s.putErrs = s.putErrs[1:]
		return err
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
func records(t *testing.T, n int) []swarfclient.Event {
	t.Helper()
	start := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	recs := make([]swarfclient.Event, n)
	for i := range recs {
		recs[i] = record(t, fmt.Sprintf("any-dlg-%d", i), start.Add(time.Duration(i)*time.Second))
	}
	return recs
}

func TestConsumerAppliesEveryRecordWhileACursorSaveIsBlocked(t *testing.T) {
	recs := records(t, 3)
	store := &recordingCursorStore{release: make(chan struct{}), stored: &registry.RevocationCursor{Token: "cursor-stored"}}
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
	store := &recordingCursorStore{stored: &registry.RevocationCursor{Token: "cursor-stored"}}
	inv := &fakeInvalidator{}
	src := &fakeSource{scripts: []streamScript{{records: recs}}}
	c := revocation.NewConsumer(src, store, inv, revocation.WithSaveInterval(time.Hour))
	stop := run(c)

	waitFor(t, func() bool { return len(inv.calls()) == len(recs) })
	stop()

	last := recs[len(recs)-1]
	require.Equal(t, []registry.RevocationCursor{{Token: string(last.Cursor)}}, store.calls())
}

// A synctest.Test bubble's clock advances only once every goroutine in it is
// blocked, so after synctest.Wait a consumer run in one has done everything
// the elapsed intervals call for.

// storedCursorStore returns a recordingCursorStore holding a cursor, so a
// consumer on it streams without saving the head first, and the only saves
// it records are the saver's.
func storedCursorStore() *recordingCursorStore {
	return &recordingCursorStore{stored: &registry.RevocationCursor{Token: "cursor-stored"}}
}

// errUnavailable is a save failure the tests inject.
var errUnavailable = errors.New("database unavailable")

// failing returns n injected save failures.
func failing(n int) []error {
	errs := make([]error, n)
	for i := range errs {
		errs[i] = errUnavailable
	}
	return errs
}

func TestConsumerSavesNothingMoreWhileTheFeedIsIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recs := records(t, 1)
		store := storedCursorStore()
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
		store := storedCursorStore()
		store.putErrs = failing(3)
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
		require.Equal(t, []registry.RevocationCursor{{Token: string(recs[0].Cursor)}}, store.calls())
		require.Equal(t, 1, logs.FilterMessage("revocation: persist cursor failed; retrying each interval").Len(),
			"the failures are logged once, when they start")
		require.Equal(t, 1, logs.FilterMessage("revocation: persist cursor recovered").Len())
	})
}

func TestConsumerLogsARecoveryAtStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recs := records(t, 1)
		store := storedCursorStore()
		store.putErrs = failing(1)
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
		store := storedCursorStore()
		store.release = make(chan struct{})
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
				store := storedCursorStore()
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

func TestConsumerLogsAFailedHeadSaveAsTheStartOfAFailingRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &recordingCursorStore{putErrs: failing(1)}
		core, logs := observer.New(zap.InfoLevel)
		c := revocation.NewConsumer(&fakeSource{}, store, &fakeInvalidator{},
			revocation.WithSaveInterval(time.Second),
			revocation.WithLogger(zap.New(core)))
		stop := run(c)
		defer stop()

		// The head's save fails; the first interval's save lands it.
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, []registry.RevocationCursor{{Token: string(testHead)}}, store.calls())
		require.Equal(t, 1, logs.FilterMessage("revocation: persist cursor failed; retrying each interval").Len())
		require.Equal(t, 1, logs.FilterMessage("revocation: persist cursor recovered").Len())
	})
}

func TestConsumerLogsNoFailureForAHeadSaveCutShortByStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &recordingCursorStore{release: make(chan struct{})}
		core, logs := observer.New(zap.InfoLevel)
		c := revocation.NewConsumer(&fakeSource{}, store, &fakeInvalidator{},
			revocation.WithSaveInterval(time.Second),
			revocation.WithLogger(zap.New(core)))
		stop := run(c)

		// The head's save waits on release, so the stop cuts it short; the
		// final save then waits on release too.
		synctest.Wait()
		stopped := make(chan struct{})
		go func() { defer close(stopped); stop() }()
		synctest.Wait()
		close(store.release)
		<-stopped

		require.Equal(t, []registry.RevocationCursor{{Token: string(testHead)}}, store.calls(), "the final save lands the head")
		require.Zero(t, logs.FilterLevelExact(zap.WarnLevel).Len(), "a save cut short by the stop is no failure")
		require.Zero(t, logs.FilterMessage("revocation: persist cursor recovered").Len(), "nothing failed to recover from")
	})
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

	require.Equal(t, []cid.Cid{recs[0].Revocation.Revoke}, inv.calls())
}

func TestConsumerReconnectsFromLastRecord(t *testing.T) {
	at := time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC)
	rec := record(t, "dlg-1", at)

	src := &fakeSource{scripts: []streamScript{
		// First connection delivers one record then dies with an error.
		{records: []swarfclient.Event{rec}, err: context.DeadlineExceeded},
		// Second connection ends cleanly (server close), forcing a third.
		{},
	}}
	c := revocation.NewConsumer(src, registry.NewPostgres(pgtest.New(t)), &fakeInvalidator{},
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) >= 3 })
	calls := src.calls()
	require.Equal(t, testHead, calls[0])
	require.Equal(t, rec.Cursor, calls[1], "reconnect resumes after the last processed record")
	require.Equal(t, rec.Cursor, calls[2], "clean stream end also resumes after the cursor")
}

func TestConsumerStopsOnCancel(t *testing.T) {
	src := &fakeSource{} // no scripts: the stream blocks until ctx cancel
	core, logs := observer.New(zap.WarnLevel)
	c := revocation.NewConsumer(src, registry.NewPostgres(pgtest.New(t)), &fakeInvalidator{},
		revocation.WithLogger(zap.New(core)))

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
	require.Empty(t, logs.All(), "a stop is not a stream fault")
}
