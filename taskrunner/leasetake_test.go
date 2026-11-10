package taskrunner_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
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
	takes    atomic.Int32
	landed   chan struct{} // closed once a delayed take has run
	once     sync.Once
}

func isLeaseTake(sql string) bool { return strings.Contains(sql, `_leases" AS l (subject, owner`) }

func (d *leaseTakeDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if !isLeaseTake(sql) {
		return d.Pool.QueryRow(ctx, sql, args...)
	}
	d.takes.Add(1)
	if d.lose {
		return lostAnswer{d.Pool.QueryRow(ctx, sql, args...)}
	}
	done := make(chan takeAnswer, 1)
	go func() {
		defer d.once.Do(func() { close(d.landed) })
		time.Sleep(d.inFlight)
		var a takeAnswer
		a.err = d.Pool.QueryRow(context.WithoutCancel(ctx), sql, args...).Scan(&a.taken, &a.tookOver, &a.created, &a.id)
		done <- a
	}()
	select {
	case <-ctx.Done():
		return errRow{ctx.Err()}
	case a := <-done:
		return a
	}
}

// takeAnswer is a lease take's answer, which it hands on as a pgx.Row.
type takeAnswer struct {
	taken, tookOver bool
	created         *time.Time
	id              *uuid.UUID
	err             error
}

func (a takeAnswer) Scan(dest ...any) error {
	if a.err != nil {
		return a.err
	}
	*dest[0].(*bool), *dest[1].(*bool) = a.taken, a.tookOver
	*dest[2].(**time.Time), *dest[3].(**uuid.UUID) = a.created, a.id
	return nil
}

type lostAnswer struct{ pgx.Row }

func (r lostAnswer) Scan(dest ...any) error {
	if err := r.Row.Scan(dest...); err != nil {
		return err
	}
	return context.DeadlineExceeded
}

type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }

// newLeaseTakeRunner starts a runner over db with a no-op action on qTop and
// a lease far longer than the test, so that only a release clears one. It
// runs no workers, so the test's own Run makes every take.
func newLeaseTakeRunner(t *testing.T, env taskrunnertest.Env, db taskrunner.DB) *taskrunner.Runner {
	t.Helper()
	r, err := taskrunner.New(db, taskrunner.Options{
		Schema:       env.Schema,
		Queues:       []taskrunner.QueueOptions{{Name: qTop}},
		LeaseTTL:     time.Minute,
		PollInterval: 20 * time.Millisecond,
		Logger:       zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Handle(qTop, "noop", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
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
	r := newLeaseTakeRunner(t, env, db)

	_, err := r.Run(context.Background(), taskrunner.Spec{Queue: qTop, Subject: "s", Action: "noop"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want the lost answer's context.DeadlineExceeded", err)
	}
	if db.takes.Load() == 0 {
		t.Fatal("Run took no lease; the test lost no answer")
	}
	if n := leaseRows(t, env); n != 0 {
		t.Fatalf("%d lease rows left on the subject, want none", n)
	}
}

// TestALeaseTakeOutlivesItsCallersDeadline: a take whose caller's deadline
// passes while it is on its way to the database still lands there. It must
// finish, so that the runner learns it took the lease and releases it; cut
// off when the deadline passed, it would leave a lease no hold records.
func TestALeaseTakeOutlivesItsCallersDeadline(t *testing.T) {
	env := taskrunnertest.NewEnv(t)
	db := &leaseTakeDB{Pool: env.DB, inFlight: 300 * time.Millisecond, landed: make(chan struct{})}
	r := newLeaseTakeRunner(t, env, db)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "noop"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want its ctx's context.DeadlineExceeded", err)
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
