package taskrunner_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
)

// leaseTakeDB runs every statement, but tampers with lease takes, each in
// one of two ways a take can reach the database without its caller learning
// so.
type leaseTakeDB struct {
	*pgxpool.Pool
	// lose makes each take's answer fail after the take has run, as when the
	// connection drops with the answer on its way back.
	lose bool
	// inFlight delays each take by this much on its way to the database,
	// while its caller returns as soon as its ctx ends, as pgx does: the
	// take lands after the caller has given up.
	inFlight time.Duration
	// onTake, if set, runs as each take arrives, before the take goes on.
	onTake func()
	takes  atomic.Int32
	// took counts the takes whose answer said they took the lease.
	took atomic.Int32
	// ran counts the handler's runs.
	ran    atomic.Int32
	landed chan struct{} // closed once a delayed take has run
	once   sync.Once
}

// isLeaseTake reports whether sql is one of the runner's lease takes, each of
// which inserts into a queue's leases table.
func isLeaseTake(sql string) bool { return strings.Contains(sql, `_leases" AS l (subject, owner`) }

func (d *leaseTakeDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if !isLeaseTake(sql) {
		return d.Pool.QueryRow(ctx, sql, args...)
	}
	d.takes.Add(1)
	if d.onTake != nil {
		d.onTake()
	}
	if d.lose {
		return lostAnswer{d.Pool.QueryRow(ctx, sql, args...), d}
	}
	sent := make(chan pgx.Row, 1)
	go func() {
		time.Sleep(d.inFlight)
		sent <- d.Pool.QueryRow(context.WithoutCancel(ctx), sql, args...)
	}()
	select {
	case <-ctx.Done():
		// The caller has its error; the take still runs, and nobody reads
		// its answer.
		go func() {
			_ = (<-sent).Scan()
			d.landedNow()
		}()
		return errRow{ctx.Err()}
	case row := <-sent:
		return landedRow{row, d}
	}
}

func (d *leaseTakeDB) landedNow() { d.once.Do(func() { close(d.landed) }) }

// landedRow hands a delayed take's answer to its caller, and reports the take
// landed once the caller has read it.
type landedRow struct {
	pgx.Row
	d *leaseTakeDB
}

func (r landedRow) Scan(dest ...any) error {
	defer r.d.landedNow()
	return r.Row.Scan(dest...)
}

// lostAnswer runs a take, then fails its answer. Each take's first column
// says whether it took the lease.
type lostAnswer struct {
	pgx.Row
	d *leaseTakeDB
}

func (r lostAnswer) Scan(dest ...any) error {
	if err := r.Row.Scan(dest...); err != nil {
		return err
	}
	if took, ok := dest[0].(*bool); ok && *took {
		r.d.took.Add(1)
	}
	return context.DeadlineExceeded
}

type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }

// newLeaseTakeRunner starts a runner over db with a no-op action on qTop and
// a lease far longer than the test, so that only a release clears one. With
// no workers, the test's own Run makes every take.
func newLeaseTakeRunner(t *testing.T, env taskrunnertest.Env, db *leaseTakeDB, workers int) *taskrunner.Runner {
	t.Helper()
	r, err := taskrunner.New(db, taskrunner.Options{
		Schema:       env.Schema,
		Queues:       []taskrunner.QueueOptions{{Name: qTop, Workers: workers}},
		LeaseTTL:     time.Minute,
		PollInterval: 20 * time.Millisecond,
		Logger:       zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Handle(qTop, "noop", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		db.ran.Add(1)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := r.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })
	return r
}

// leaseRows counts the lease rows on qTop's subject s.
func leaseRows(t *testing.T, env taskrunnertest.Env) int {
	t.Helper()
	var n int
	if err := env.DB.QueryRow(context.Background(), `SELECT count(*) FROM `+env.Table("queue_top_leases")+` WHERE subject = 's'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestALeaseTakeWhoseAnswerIsLostLeavesNoLease: a lease the database took for
// a take that then failed is held by no one, so nothing renews or releases
// it. The take removes it rather than leave the subject blocked until the
// lease expires.
func TestALeaseTakeWhoseAnswerIsLostLeavesNoLease(t *testing.T) {
	env := taskrunnertest.NewEnv(t)
	db := &leaseTakeDB{Pool: env.DB, lose: true}
	r := newLeaseTakeRunner(t, env, db, 0)

	_, err := r.Run(context.Background(), taskrunner.Spec{Queue: qTop, Subject: "s", Action: "noop"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want the lost answer's context.DeadlineExceeded", err)
	}
	if db.took.Load() == 0 {
		t.Fatal("no take took the lease; the test lost no lease's answer")
	}
	if n := leaseRows(t, env); n != 0 {
		t.Fatalf("%d lease rows left on the subject, want none", n)
	}
}

// TestAWorkersLeaseTakeWhoseAnswerIsLostLeavesNoLease is the same for a
// worker, which takes its lease by another path than Run.
func TestAWorkersLeaseTakeWhoseAnswerIsLostLeavesNoLease(t *testing.T) {
	env := taskrunnertest.NewEnv(t)
	db := &leaseTakeDB{Pool: env.DB, lose: true}
	r := newLeaseTakeRunner(t, env, db, 1)

	if _, err := r.Submit(context.Background(), taskrunner.Spec{Queue: qTop, Subject: "s", Action: "noop"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for db.took.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no worker took the subject's lease")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Once the workers have stopped, a lease row left is one no take tracks.
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := leaseRows(t, env); n != 0 {
		t.Fatalf("%d lease rows left on the subject, want none", n)
	}
}

// TestALeaseTakeOutlivesItsCaller: a take whose caller gives up while it is
// on its way to the database still lands there. It must finish, so that the
// runner learns it took the lease and releases it; cut off when the caller
// gave up, it would leave a lease no hold records.
func TestALeaseTakeOutlivesItsCaller(t *testing.T) {
	env := taskrunnertest.NewEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The caller gives up as soon as its take is on its way.
	db := &leaseTakeDB{Pool: env.DB, inFlight: 300 * time.Millisecond, onTake: cancel, landed: make(chan struct{})}
	r := newLeaseTakeRunner(t, env, db, 0)

	_, err := r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "noop"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want its ctx's context.Canceled", err)
	}
	if db.takes.Load() == 0 {
		t.Fatal("Run made no take")
	}
	if n := db.ran.Load(); n != 0 {
		t.Errorf("the handler ran %d time(s) for a caller that had given up", n)
	}
	select {
	case <-db.landed:
	case <-time.After(5 * time.Second):
		t.Fatal("the take never landed")
	}
	// Give a release that follows the take time to commit.
	deadline := time.Now().Add(5 * time.Second)
	for leaseRows(t, env) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("a lease row is left on the subject after the take landed, want none")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
