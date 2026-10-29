package revocation_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"testing"
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

// streamCall is how one stream was opened: StreamAfter with after, or
// StreamEvents with from.
type streamCall struct {
	after swarfclient.Cursor
	from  time.Time
}

// fakeSource plays one streamScript per stream call, recording how each call
// was made. When the scripts run out, a stream blocks until ctx cancel (a
// healthy idle connection).
type fakeSource struct {
	mu      sync.Mutex
	scripts []streamScript
	opened  []streamCall
}

func (f *fakeSource) StreamAfter(ctx context.Context, after swarfclient.Cursor) iter.Seq2[swarfclient.Event, error] {
	return f.stream(ctx, streamCall{after: after})
}

func (f *fakeSource) StreamEvents(ctx context.Context, from time.Time) iter.Seq2[swarfclient.Event, error] {
	return f.stream(ctx, streamCall{from: from})
}

func (f *fakeSource) stream(ctx context.Context, call streamCall) iter.Seq2[swarfclient.Event, error] {
	f.mu.Lock()
	f.opened = append(f.opened, call)
	var script *streamScript
	if len(f.scripts) > 0 {
		script = &f.scripts[0]
		f.scripts = f.scripts[1:]
	}
	f.mu.Unlock()
	return func(yield func(swarfclient.Event, error) bool) {
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
			yield(swarfclient.Event{}, script.err)
		}
	}
}

func (f *fakeSource) calls() []streamCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]streamCall(nil), f.opened...)
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

func TestConsumerStartsAtTheHeadWithNoCursor(t *testing.T) {
	src := &fakeSource{}
	c := revocation.NewConsumer(src, inmem.NewMemStore(), &fakeInvalidator{})
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) == 1 })
	require.Equal(t, streamCall{after: swarfclient.Now}, src.calls()[0],
		"no stored cursor: start at the head as the service sees it")
}

func TestConsumerResumesAfterStoredCursor(t *testing.T) {
	ctx := context.Background()
	store := inmem.NewMemStore()
	require.NoError(t, store.PutRevocationCursor(ctx, registry.RevocationCursor{
		Token:      "cursor-earlier",
		RecordedAt: time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC),
		Revoke:     testCid(t, "earlier"),
	}))

	src := &fakeSource{}
	c := revocation.NewConsumer(src, store, &fakeInvalidator{})
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) == 1 })
	require.Equal(t, streamCall{after: "cursor-earlier"}, src.calls()[0], "stored token wins over the head")
}

// A cursor saved before the service issued tokens holds only recorded_at.
// The consumer resumes it with the time query from there, as it did before
// tokens, and resumes after a token from the first record on.
func TestConsumerResumesACursorFromBeforeTokensOnce(t *testing.T) {
	ctx := context.Background()
	stored := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	store := inmem.NewMemStore()
	require.NoError(t, store.PutRevocationCursor(ctx, registry.RevocationCursor{
		RecordedAt: stored, Revoke: testCid(t, "earlier"),
	}))
	rec := record(t, "dlg-1", stored.Add(time.Second))

	inv := &fakeInvalidator{}
	src := &fakeSource{scripts: []streamScript{
		// A stream that ends before any record leaves nothing to resume
		// after but the stored time.
		{},
		{records: []swarfclient.Event{rec}},
	}}
	c := revocation.NewConsumer(src, store, inv,
		revocation.WithBackoff(time.Millisecond, time.Millisecond),
		revocation.WithSaveInterval(time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) == 3 })
	require.Equal(t, []streamCall{{from: stored}, {from: stored}, {after: rec.Cursor}}, src.calls())
	require.Equal(t, []cid.Cid{rec.Revocation.Revoke}, inv.calls())
	waitFor(t, func() bool {
		cur, err := store.GetRevocationCursor(ctx)
		return err == nil && cur.Token == string(rec.Cursor)
	})
}

// With no cursor, a stream at the head that ends before its first record —
// the service unreachable when ingot starts, say — must not be retried at a
// later head, which would skip the revocations recorded in between.
func TestConsumerFallsBackToTheTimeQueryFromItsStart(t *testing.T) {
	rec := record(t, "dlg-1", time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC))
	src := &fakeSource{scripts: []streamScript{
		{err: errors.New("connection refused")},
		{records: []swarfclient.Event{rec}, err: errors.New("connection reset")},
	}}
	inv := &fakeInvalidator{}
	c := revocation.NewConsumer(src, inmem.NewMemStore(), inv,
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	before := time.Now()
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) >= 3 })
	calls := src.calls()
	require.Equal(t, streamCall{after: swarfclient.Now}, calls[0])
	require.Empty(t, calls[1].after)
	require.False(t, calls[1].from.After(before), "the time query starts no later than the consumer did")
	require.WithinDuration(t, before, calls[1].from, 2*time.Minute)
	require.Equal(t, streamCall{after: rec.Cursor}, calls[2], "the first record's token takes over")
}

// A cursor the service refuses is logged as an error naming the fix, and
// retried: only an operator can tell where to start over.
func TestConsumerLogsARefusedCursor(t *testing.T) {
	ctx := context.Background()
	store := inmem.NewMemStore()
	require.NoError(t, store.PutRevocationCursor(ctx, registry.RevocationCursor{
		Token:      "cursor-moved",
		RecordedAt: time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC),
		Revoke:     testCid(t, "earlier"),
	}))
	src := &fakeSource{scripts: []streamScript{
		{err: fmt.Errorf("opening revocation stream: %w", swarfclient.ErrCursorRefused)},
	}}
	core, logs := observer.New(zap.WarnLevel)
	c := revocation.NewConsumer(src, store, &fakeInvalidator{},
		revocation.WithLogger(zap.New(core)),
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) >= 2 })
	require.Equal(t, streamCall{after: "cursor-moved"}, src.calls()[1])
	refused := logs.FilterMessage("revocation: cursor refused").All()
	require.NotEmpty(t, refused)
	require.Equal(t, zap.ErrorLevel, refused[0].Level)
}

// With no saved token, a refusal of the head's cursor on a reconnect is
// recovered by the fallback, so it is no operator's error.
func TestConsumerRecoversARefusalWithNoToken(t *testing.T) {
	src := &fakeSource{scripts: []streamScript{
		{err: fmt.Errorf("opening revocation stream: %w", swarfclient.ErrCursorRefused)},
	}}
	core, logs := observer.New(zap.WarnLevel)
	c := revocation.NewConsumer(src, inmem.NewMemStore(), &fakeInvalidator{},
		revocation.WithLogger(zap.New(core)),
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) >= 2 })
	require.NotZero(t, src.calls()[1].from, "the fallback time query")
	require.Empty(t, logs.FilterMessage("revocation: cursor refused").All())
}

func TestConsumerInvalidatesAndPersistsCursor(t *testing.T) {
	ctx := context.Background()
	at1 := time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC)
	at2 := at1.Add(time.Second)
	rec1 := record(t, "dlg-1", at1)
	rec2 := record(t, "dlg-2", at2)

	store := inmem.NewMemStore()
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
		return err == nil && cur.Revoke.Equals(rec2.Revocation.Revoke)
	})
	cur, err := store.GetRevocationCursor(ctx)
	require.NoError(t, err)
	require.Equal(t, string(rec2.Cursor), cur.Token, "cursor is the last record's token")
	require.True(t, cur.RecordedAt.Equal(at2), "cursor records the last record's recorded_at")
}

// recordingCursorStore records every cursor it is asked to save. Like the
// Postgres store, it refuses a save whose ctx is already canceled. With
// release set, each save waits for release to close first.
type recordingCursorStore struct {
	mu      sync.Mutex
	saved   []registry.RevocationCursor
	release chan struct{}
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
		Token:      string(last.Cursor),
		RecordedAt: last.Revocation.RecordedAt.Time(),
		Revoke:     last.Revocation.Revoke,
	}}, store.calls())
}

func TestConsumerSavesNothingMoreWhileTheFeedIsIdle(t *testing.T) {
	recs := records(t, 1)
	store := &recordingCursorStore{}
	src := &fakeSource{scripts: []streamScript{{records: recs}}}
	c := revocation.NewConsumer(src, store, &fakeInvalidator{},
		revocation.WithSaveInterval(time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(store.calls()) == 1 })
	// Twenty intervals with no new record.
	time.Sleep(20 * time.Millisecond)
	require.Len(t, store.calls(), 1)
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
	c := revocation.NewConsumer(src, inmem.NewMemStore(), &fakeInvalidator{},
		revocation.WithBackoff(time.Millisecond, time.Millisecond))
	stop := run(c)
	defer stop()

	waitFor(t, func() bool { return len(src.calls()) >= 3 })
	calls := src.calls()
	require.Equal(t, streamCall{after: rec.Cursor}, calls[1], "reconnect resumes after the last processed record")
	require.Equal(t, streamCall{after: rec.Cursor}, calls[2], "clean stream end also resumes after the cursor")
}

func TestConsumerStopsOnCancel(t *testing.T) {
	src := &fakeSource{} // no scripts: the stream blocks until ctx cancel
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
