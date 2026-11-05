package s3frontend

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
)

// testStore is what a test backend runs on: the Postgres registry in a
// database of the test's own, the task queues the backend's handlers run on,
// and a bucket authority standing in for hilt. It also answers the few
// questions tests ask that the registry does not, such as which space holds
// a digest.
type testStore struct {
	*registry.Postgres
	testfakes.Authority
	pool  *pgxpool.Pool
	tasks *taskrunner.Runner
	// started is set once a backend's handlers are registered and the queues
	// run; a store serves one backend's tasks.
	started bool
}

// stores maps each test backend to the store it runs on, for helpers that
// have only the backend, whose Deps a test may have wrapped.
var stores sync.Map

// storeFor returns the store b runs on.
func storeFor(b *Backend) *testStore {
	s, _ := stores.Load(b)
	return s.(*testStore)
}

// testNode is the node test runners place their tasks on.
const testNode = "did:web:ingot.test"

func newTestStore(t testing.TB) *testStore {
	t.Helper()
	pool := pgtest.New(t)
	r, err := taskrunner.New(pool, taskrunner.Options{
		Schema: "ingot",
		Queues: []taskrunner.QueueOptions{
			{Name: registry.UploadQueue, Workers: 2},
			// No workers: a release runs when a test sweeps (SweepPendingReleases)
			// or an S3 verb runs it inline, as the tests expect.
			{Name: registry.ReleaseQueue},
			{Name: ShardQueue, Workers: 2},
		},
		LeaseTTL:     time.Second,
		PollInterval: 10 * time.Millisecond,
		Node:         testNode,
		Logger:       zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatalf("task runner: %v", err)
	}
	if err := r.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate task queues: %v", err)
	}
	return &testStore{Postgres: registry.NewPostgres(pool, registry.WithTasks(r)), pool: pool, tasks: r}
}

// start registers b's task handlers and runs the queues until t ends. The
// first backend built over the store runs its tasks; a later one shares them.
func (s *testStore) start(t testing.TB, b *Backend) {
	t.Helper()
	if s.started {
		b.tasks, b.shards, b.releases = s.tasks, s.tasks, s.tasks
		b.cache.OnRead(b.renewCached)
		return
	}
	if err := b.RegisterUploadTasks(s.tasks); err != nil {
		t.Fatalf("register upload tasks: %v", err)
	}
	if err := b.RegisterShardTasks(s.tasks); err != nil {
		t.Fatalf("register shard tasks: %v", err)
	}
	if err := s.tasks.Handle(ShardQueue, shardFlush, func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("register shard flush: %v", err)
	}
	// A failed release waits for the next sweep (SweepPendingReleases), not
	// a delay, as the sweeper retried one at its next pass.
	b.releaseRetry = taskrunner.RetryPolicy{First: time.Hour, Max: time.Hour}
	if err := b.RegisterReleaseTasks(s.tasks); err != nil {
		t.Fatalf("register release tasks: %v", err)
	}
	if err := s.tasks.Start(context.Background()); err != nil {
		t.Fatalf("start task queues: %v", err)
	}
	s.started = true
	stores.Store(b, s)
	t.Cleanup(func() { _ = s.tasks.Stop(context.Background()) })
}

// shardFlush is a queue-shard action that does nothing, which settleCache
// runs to wait for the tasks queued on a digest before it.
const shardFlush = "flush"

// settleCache returns once each digest's due queue-shard tasks, such as its
// move into the cache, have run: a sync task on a subject runs after the due
// tasks queued there before it. A delayed eviction is not due, and stays
// queued.
func settleCache(t testing.TB, b *Backend, digests ...multihash.Multihash) {
	t.Helper()
	for _, d := range digests {
		if _, err := b.shards.Run(t.Context(), taskrunner.Spec{Queue: ShardQueue, Subject: hex.EncodeToString(d), Action: shardFlush}); err != nil {
			t.Fatalf("settle the cache tasks of %x: %v", d, err)
		}
	}
}

// Create creates the bucket as the registry does, giving it a space and a
// tenant of its own when the test names neither.
func (s *testStore) Create(ctx context.Context, name string, space did.DID, init registry.CreateState) error {
	if !space.Defined() {
		space = randomDID()
	}
	if !init.Tenant.Defined() {
		init.Tenant = randomDID()
	}
	return s.Postgres.Create(ctx, name, space, init)
}

// bk returns the space of the bucket the test backends create, "bk".
func (s *testStore) bk() did.DID {
	st, err := s.Get(context.Background(), "bk")
	if err != nil {
		panic(fmt.Sprintf("bucket bk: %v", err))
	}
	return st.Space
}

// randomDID mints a DID for a bucket's space or tenant.
func randomDID() did.DID {
	id, err := ed25519.Generate()
	if err != nil {
		panic(err)
	}
	return id.KeyDID()
}

// drainUploads waits until the upload queue has sent every object-count
// change queued so far.
func (s *testStore) drainUploads(t testing.TB) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		tasks, err := s.queued(context.Background(), registry.UploadQueue)
		if err != nil {
			t.Fatalf("list queued uploads: %v", err)
		}
		n := 0
		for _, q := range tasks {
			if q.State == "pending" {
				n++
			}
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d object-count changes still queued after 10s", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// spaceOf returns the one space whose row in table names digest.
func (s *testStore) spaceOf(ctx context.Context, table string, digest multihash.Multihash) (did.DID, error) {
	rows, err := s.pool.Query(ctx, `SELECT space FROM ingot.`+table+` WHERE digest = $1`, []byte(digest))
	if err != nil {
		return did.Undef, err
	}
	defer rows.Close()
	var spaces []string
	for rows.Next() {
		var sp string
		if err := rows.Scan(&sp); err != nil {
			return did.Undef, err
		}
		spaces = append(spaces, sp)
	}
	if err := rows.Err(); err != nil {
		return did.Undef, err
	}
	switch len(spaces) {
	case 0:
		return did.Undef, registry.ErrNotFound
	case 1:
		return did.Parse(spaces[0])
	}
	return did.Undef, fmt.Errorf("digest %x has %s rows in %d spaces", digest, table, len(spaces))
}

// IntentByDigest returns the one intent for digest, in whichever space holds
// it, or ErrNotFound. Intents are keyed by (space, digest); a test that wrote
// the digest to one space need not look the space up.
func (s *testStore) IntentByDigest(ctx context.Context, digest multihash.Multihash) (*registry.UploadIntent, error) {
	space, err := s.spaceOf(ctx, "upload_intents", digest)
	if err != nil {
		return nil, err
	}
	return s.GetIntent(ctx, space, digest)
}

// ParkByDigest is IntentByDigest for parks.
func (s *testStore) ParkByDigest(ctx context.Context, digest multihash.Multihash) (*registry.BlobPark, error) {
	space, err := s.spaceOf(ctx, "blob_parks", digest)
	if err != nil {
		return nil, err
	}
	return s.GetPark(ctx, space, digest)
}

// AgeIntent moves the last state change of every intent for digest back by d,
// as if it had happened that much earlier.
func (s *testStore) AgeIntent(digest multihash.Multihash, d time.Duration) {
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE ingot.upload_intents SET updated_at = updated_at - $2 * interval '1 microsecond' WHERE digest = $1`,
		[]byte(digest), d.Microseconds()); err != nil {
		panic(err)
	}
}

// AgePart moves the part's creation back by d, as if it had been recorded
// that much earlier.
func (s *testStore) AgePart(uploadID string, part int32, d time.Duration) {
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE ingot.multipart_parts SET created_at = created_at - $3 * interval '1 microsecond' WHERE upload_id = $1 AND part_number = $2`,
		uploadID, part, d.Microseconds()); err != nil {
		panic(err)
	}
}

// AgeSession moves the session's creation and last state change back by d.
func (s *testStore) AgeSession(uploadID string, d time.Duration) {
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE ingot.multipart_sessions
		    SET created_at = created_at - $2 * interval '1 microsecond',
		        state_changed_at = state_changed_at - $2 * interval '1 microsecond'
		  WHERE upload_id = $1`,
		uploadID, d.Microseconds()); err != nil {
		panic(err)
	}
}

// JournaledBlocks returns the catalog blocks journaled under segment, by CID
// string.
func (s *testStore) JournaledBlocks(segment uint64) map[string][]byte {
	rows, err := s.pool.Query(context.Background(), `SELECT cid, data FROM ingot.catalog_journal WHERE seq = $1`, int64(segment))
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	out := map[string][]byte{}
	for rows.Next() {
		var c, data []byte
		if err := rows.Scan(&c, &data); err != nil {
			panic(err)
		}
		out[string(c)] = data
	}
	return out
}

// pendingRelease is a release still to run, as a test reads it.
type pendingRelease struct {
	Space     did.DID
	Digest    multihash.Multihash
	NotBefore time.Time
}

// queued lists the store's tasks on queue, read through the runner's test
// package.
func (s *testStore) queued(ctx context.Context, queue string) ([]taskrunnertest.QueuedTask, error) {
	return taskrunnertest.Tasks(ctx, s.pool, "ingot", queue)
}

// ListReleasesBySpace returns the space's pending releases, due or not,
// oldest first.
func (s *testStore) ListReleasesBySpace(ctx context.Context, space did.DID) ([]pendingRelease, error) {
	tasks, err := s.queued(ctx, registry.ReleaseQueue)
	if err != nil {
		return nil, err
	}
	var out []pendingRelease
	for _, t := range tasks {
		if t.Action != registry.ReleaseAction || t.State != "pending" {
			continue
		}
		sp, d, err := registry.ParseReleaseSubject(t.Subject)
		if err != nil {
			return nil, err
		}
		if sp != space {
			continue
		}
		at := t.Created
		if t.NotBefore != nil {
			at = *t.NotBefore
		}
		out = append(out, pendingRelease{Space: sp, Digest: d, NotBefore: at})
	}
	return out, nil
}

// EnqueueRelease queues the release of digest in space at notBefore, as a
// commit dropping its last claim would, with no authority of its own.
func (s *testStore) EnqueueRelease(ctx context.Context, space did.DID, digest multihash.Multihash, notBefore time.Time) error {
	return s.QueueReleases(ctx, space, []multihash.Multihash{digest}, notBefore, registry.ReleaseTask{})
}

// ReleasingDigests reports which of digests have a release pending or running
// in space.
func (s *testStore) ReleasingDigests(ctx context.Context, space did.DID, digests []multihash.Multihash) (map[string]bool, error) {
	subjects := make([]string, len(digests))
	for i, d := range digests {
		subjects[i] = registry.ReleaseSubject(space, d)
	}
	pending, err := s.tasks.Pending(ctx, registry.ReleaseQueue, subjects, registry.ReleaseAction)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for i, d := range digests {
		if len(pending[subjects[i]]) > 0 {
			out[string(d)] = true
		}
	}
	return out, nil
}

// SweepPendingReleases runs every release that is due, in this goroutine, as
// a sweep of due releases did, and returns how many removed their blob: a
// stale release or a deferred one does not count. It runs the queue's no-op
// on each subject with a release pending, which runs what is due there and
// leaves what is not. A release waiting to retry is brought forward first, as
// the sweeper retried a failed release at its next pass.
func (b *Backend) SweepPendingReleases(ctx context.Context) (int, error) {
	s := storeFor(b)
	tasks, err := s.queued(ctx, registry.ReleaseQueue)
	if err != nil {
		return 0, err
	}
	var subjects []string
	before := map[taskrunner.TaskID]bool{}
	seen := map[string]bool{}
	for _, t := range tasks {
		if t.Action != registry.ReleaseAction || t.State != "pending" {
			continue
		}
		before[t.ID] = true
		if !seen[t.Subject] {
			seen[t.Subject] = true
			subjects = append(subjects, t.Subject)
		}
		if t.Attempts > 0 {
			if _, err := b.releases.Expedite(ctx, registry.ReleaseQueue, t.Subject, registry.ReleaseAction); err != nil {
				return 0, err
			}
		}
	}
	for _, subject := range subjects {
		if err := b.flushRelease(ctx, subject, time.Now().Add(10*time.Second)); err != nil {
			return 0, err
		}
	}
	if tasks, err = s.queued(ctx, registry.ReleaseQueue); err != nil {
		return 0, err
	}
	n := 0
	for _, t := range tasks {
		if before[t.ID] && t.State == "succeeded" && string(t.Result) == string(releasedResult) {
			n++
		}
	}
	return n, nil
}
