package taskrunner_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
)

// The queues every test uses, in order.
const (
	qTop    = "queue-top"
	qMiddle = "queue-middle"
	qBottom = "queue-bottom"
)

type fixture struct {
	env taskrunnertest.Env
	r   *taskrunner.Runner
}

// newFixture returns an unstarted runner over a fresh schema holding the
// three test queues and an items table (k text, v int).
func newFixture(t *testing.T, workers int, tweak ...func(*taskrunner.Options)) *fixture {
	t.Helper()
	env := taskrunnertest.NewEnv(t)
	opts := taskrunner.Options{
		Schema: env.Schema,
		Queues: []taskrunner.QueueOptions{
			{Name: qTop, Workers: workers},
			{Name: qMiddle, Workers: workers},
			{Name: qBottom, Workers: workers},
		},
		LeaseTTL:     time.Second,
		PollInterval: 20 * time.Millisecond,
		Logger:       zaptest.NewLogger(t),
	}
	for _, f := range tweak {
		f(&opts)
	}
	r, err := taskrunner.New(env.DB, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.DB.Exec(context.Background(), `CREATE TABLE `+env.Table("items")+` (k text PRIMARY KEY, v int NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return &fixture{env: env, r: r}
}

func (f *fixture) handle(t *testing.T, queue, action string, h taskrunner.Handler) {
	t.Helper()
	if err := f.r.Handle(queue, action, h); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) start(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if err := f.r.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.r.Stop(context.Background()) })
}

// put returns a handler that sets item k to the task's args, parsed as an int.
func (f *fixture) put() taskrunner.Handler {
	return func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		return task.Args, ex.Write(ctx, `INSERT INTO `+f.env.Table("items")+` (k, v) VALUES ($1, $2)
			ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v`, task.Subject, string(task.Args))
	}
}

func (f *fixture) item(t *testing.T, k string) (int, bool) {
	t.Helper()
	var v int
	err := f.env.DB.QueryRow(context.Background(), `SELECT v FROM `+f.env.Table("items")+` WHERE k = $1`, k).Scan(&v)
	if err != nil {
		return 0, false
	}
	return v, true
}

func (f *fixture) state(t *testing.T, queue string, id taskrunner.TaskID) string {
	t.Helper()
	var s string
	tbl := f.env.Table(queueTable(queue) + "_tasks")
	if err := f.env.DB.QueryRow(context.Background(), `SELECT state FROM `+tbl+` WHERE id = $1`, id).Scan(&s); err != nil {
		t.Fatalf("read state of %s: %v", id, err)
	}
	return s
}

func queueTable(q string) string {
	out := []byte(q)
	for i, c := range out {
		if c == '-' {
			out[i] = '_'
		}
	}
	return string(out)
}

func waitCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestMigrate_Live(t *testing.T) {
	f := newFixture(t, 0)
	ctx := context.Background()
	for range 2 {
		if err := f.r.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, tbl := range []string{"queue_top_tasks", "queue_top_leases", "queue_bottom_tasks", "queue_bottom_leases"} {
		var ok bool
		if err := f.env.DB.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2)`, f.env.Schema, tbl).Scan(&ok); err != nil || !ok {
			t.Errorf("table %s missing after migrate (err %v)", tbl, err)
		}
	}
}

func TestRunCommitsWritesAtTheEnd_Live(t *testing.T) {
	f := newFixture(t, 0)
	var sawOwnWrite atomic.Bool
	f.handle(t, qTop, "put", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if err := ex.Write(ctx, `INSERT INTO `+f.env.Table("items")+` (k, v) VALUES ('a', 1)`); err != nil {
			return nil, err
		}
		var n int
		_ = ex.DB().QueryRow(ctx, `SELECT count(*) FROM `+f.env.Table("items")).Scan(&n)
		sawOwnWrite.Store(n > 0)
		return []byte("done"), nil
	})
	f.start(t)

	res, err := f.r.Run(waitCtx(t), taskrunner.Spec{Queue: qTop, Subject: "s", Action: "put"})
	if err != nil {
		t.Fatal(err)
	}
	if string(res) != "done" {
		t.Errorf("result %q, want done", res)
	}
	if sawOwnWrite.Load() {
		t.Error("handler saw its own write before the commit")
	}
	if v, ok := f.item(t, "a"); !ok || v != 1 {
		t.Errorf("item a = %d, %v after commit; want 1", v, ok)
	}
}

func TestHandlerErrorFailsTaskAndDropsWrites_Live(t *testing.T) {
	f := newFixture(t, 0)
	boom := errors.New("boom")
	f.handle(t, qTop, "fail", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		_ = ex.Write(ctx, `INSERT INTO `+f.env.Table("items")+` (k, v) VALUES ('a', 1)`)
		return nil, boom
	})
	f.start(t)
	ctx := waitCtx(t)

	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "fail"}); !errors.Is(err, boom) {
		t.Fatalf("Run error %v, want boom", err)
	}
	if _, ok := f.item(t, "a"); ok {
		t.Error("a failed task's write committed")
	}

	id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "fail"})
	if err != nil {
		t.Fatal(err)
	}
	// No workers: run it through a sync task queued behind it.
	f.handle(t, qTop, "noop", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) { return nil, nil })
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "noop"}); err != nil {
		t.Fatal(err)
	}
	_, err = f.r.Wait(ctx, qTop, id)
	var failed *taskrunner.HandlerError
	if !errors.As(err, &failed) || failed.Message != "boom" {
		t.Errorf("Wait error %v, want a HandlerError with message boom", err)
	}
}

func TestSubjectRunsOneTaskAtATimeInOrder_Live(t *testing.T) {
	f := newFixture(t, 8)
	var running, overlap atomic.Int32
	var mu sync.Mutex
	var order []int
	f.handle(t, qTop, "step", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if running.Add(1) > 1 {
			overlap.Add(1)
		}
		defer running.Add(-1)
		time.Sleep(2 * time.Millisecond)
		n, _ := strconv.Atoi(string(task.Args))
		mu.Lock()
		order = append(order, n)
		mu.Unlock()
		return nil, nil
	})
	f.start(t)
	ctx := waitCtx(t)

	var last taskrunner.TaskID
	for i := range 30 {
		id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "s", Action: "step", Args: []byte(strconv.Itoa(i))})
		if err != nil {
			t.Fatal(err)
		}
		last = id
	}
	if _, err := f.r.Wait(ctx, qTop, last); err != nil {
		t.Fatal(err)
	}
	if overlap.Load() > 0 {
		t.Errorf("%d tasks overlapped on one subject", overlap.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for i, n := range order {
		if n != i {
			t.Fatalf("ran in order %v", order)
		}
	}
}

func TestDelayedTaskDoesNotBlockLaterOnes_Live(t *testing.T) {
	f := newFixture(t, 2)
	f.handle(t, qTop, "put", f.put())
	f.start(t)
	ctx := waitCtx(t)

	later := time.Now().Add(time.Hour)
	delayed, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "a", Action: "put", Args: []byte("1"), NotBefore: &later})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "a", Action: "put", Args: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if v, _ := f.item(t, "a"); v != 2 {
		t.Errorf("item a = %d, want 2", v)
	}
	if s := f.state(t, qTop, delayed); s != "pending" {
		t.Errorf("delayed task is %s, want pending", s)
	}
}

func TestExpiredTaskDoesNotRun_Live(t *testing.T) {
	f := newFixture(t, 2)
	f.handle(t, qTop, "put", f.put())
	f.start(t)
	ctx := waitCtx(t)

	past := time.Now().Add(-time.Second)
	id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "a", Action: "put", Args: []byte("1"), Expires: &past})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Wait(ctx, qTop, id); !errors.Is(err, taskrunner.ErrExpired) {
		t.Errorf("Wait error %v, want ErrExpired", err)
	}
	if _, ok := f.item(t, "a"); ok {
		t.Error("an expired task ran")
	}
}

func TestSyncChildrenCommitWithTheirParent_Live(t *testing.T) {
	f := newFixture(t, 0)
	f.handle(t, qBottom, "put", f.put())
	f.handle(t, qMiddle, "put-two", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		for _, k := range []string{"x", "y"} {
			if _, err := ex.Run(ctx, taskrunner.Spec{Queue: qBottom, Subject: k, Action: "put", Args: []byte("7")}); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	f.handle(t, qTop, "parent", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if _, err := ex.Run(ctx, taskrunner.Spec{Queue: qMiddle, Subject: "m", Action: "put-two"}); err != nil {
			return nil, err
		}
		if string(task.Args) == "fail" {
			return nil, errors.New("parent failed after its children")
		}
		return nil, nil
	})
	f.start(t)
	ctx := waitCtx(t)

	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "t", Action: "parent", Args: []byte("fail")}); err == nil {
		t.Fatal("parent succeeded")
	}
	if _, ok := f.item(t, "x"); ok {
		t.Error("a child's write committed although its parent failed")
	}
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "t", Action: "parent"}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"x", "y"} {
		if v, _ := f.item(t, k); v != 7 {
			t.Errorf("item %s = %d, want 7", k, v)
		}
	}
}

func TestFailedChildFailsItsParent_Live(t *testing.T) {
	f := newFixture(t, 0)
	f.handle(t, qBottom, "put", f.put())
	f.handle(t, qBottom, "fail", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		_ = ex.Write(ctx, `INSERT INTO `+f.env.Table("items")+` (k, v) VALUES ('child', 1)`)
		return nil, errors.New("child failed")
	})
	f.handle(t, qTop, "swallow", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		_, _ = ex.Run(ctx, taskrunner.Spec{Queue: qBottom, Subject: "c", Action: "fail"})
		_, err := ex.Run(ctx, taskrunner.Spec{Queue: qBottom, Subject: "p", Action: "put", Args: []byte("1")})
		return nil, err
	})
	f.start(t)

	_, err := f.r.Run(waitCtx(t), taskrunner.Spec{Queue: qTop, Subject: "t", Action: "swallow"})
	if err == nil || err.Error() != "child failed" {
		t.Fatalf("Run error %v, want the child's", err)
	}
	for _, k := range []string{"child", "p"} {
		if _, ok := f.item(t, k); ok {
			t.Errorf("item %s committed in a failed tree", k)
		}
	}
}

func TestSpawnRules_Live(t *testing.T) {
	f := newFixture(t, 0)
	f.handle(t, qMiddle, "up", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		_, err := ex.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "t", Action: "noop"})
		return nil, err
	})
	f.handle(t, qMiddle, "across", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		_, err := ex.Run(ctx, taskrunner.Spec{Queue: qMiddle, Subject: "other", Action: "noop"})
		return nil, err
	})
	f.handle(t, qMiddle, "self", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		_, err := ex.Submit(ctx, taskrunner.Spec{Queue: qMiddle, Subject: "later", Action: "self"})
		return nil, err
	})
	noop := func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) { return nil, nil }
	f.handle(t, qTop, "noop", noop)
	f.handle(t, qMiddle, "noop", noop)
	f.start(t)
	ctx := waitCtx(t)

	for action, want := range map[string]error{"up": taskrunner.ErrQueueOrder, "across": taskrunner.ErrQueueOrder, "self": taskrunner.ErrRecursion} {
		if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qMiddle, Subject: "m", Action: action}); !errors.Is(err, want) {
			t.Errorf("%s: error %v, want %v", action, err, want)
		}
	}
}

func TestAsyncSubmitCommitsWithItsParent_Live(t *testing.T) {
	f := newFixture(t, 2)
	f.handle(t, qBottom, "put", f.put())
	var spawned taskrunner.TaskID
	f.handle(t, qTop, "spawn", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		id, err := ex.Submit(ctx, taskrunner.Spec{Queue: qBottom, Subject: "later", Action: "put", Args: []byte("3")})
		spawned = id
		if err != nil {
			return nil, err
		}
		if string(task.Args) == "fail" {
			return nil, errors.New("parent failed")
		}
		return nil, nil
	})
	f.start(t)
	ctx := waitCtx(t)

	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "t", Action: "spawn", Args: []byte("fail")}); err == nil {
		t.Fatal("parent succeeded")
	}
	if _, err := f.r.Wait(ctx, qBottom, spawned); !errors.Is(err, taskrunner.ErrNotFound) {
		t.Errorf("a failed parent's spawn exists: Wait error %v", err)
	}
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "t", Action: "spawn"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Wait(ctx, qBottom, spawned); err != nil {
		t.Fatal(err)
	}
	if v, _ := f.item(t, "later"); v != 3 {
		t.Errorf("item later = %d, want 3", v)
	}
}

// claimFixture has a qTop.claim handler whose conditional write misses until
// item ref exists, and counts its runs.
func claimFixture(t *testing.T, workers int) (*fixture, *atomic.Int32) {
	f := newFixture(t, workers)
	var runs atomic.Int32
	f.handle(t, qTop, "claim", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		runs.Add(1)
		return nil, ex.WriteConditional(ctx, `UPDATE `+f.env.Table("items")+` SET v = v + 1 WHERE k = 'ref'`)
	})
	f.start(t)
	return f, &runs
}

func (f *fixture) createRef(t *testing.T) {
	t.Helper()
	if _, err := f.env.DB.Exec(context.Background(), `INSERT INTO `+f.env.Table("items")+` (k, v) VALUES ('ref', 0)`); err != nil {
		t.Error(err)
	}
}

func TestRunRetriesACommitUntilItLands_Live(t *testing.T) {
	f, runs := claimFixture(t, 0)
	time.AfterFunc(300*time.Millisecond, func() { f.createRef(t) })

	if _, err := f.r.Run(waitCtx(t), taskrunner.Spec{Queue: qTop, Subject: "t", Action: "claim"}); err != nil {
		t.Fatal(err)
	}
	if v, _ := f.item(t, "ref"); v != 1 {
		t.Errorf("item ref = %d, want 1", v)
	}
	if n := runs.Load(); n <= 3 {
		t.Errorf("tree ran %d times, want more than one round of CommitAttempts", n)
	}
}

func TestRunEndsExpiredWhenACommitNeverLands_Live(t *testing.T) {
	f, runs := claimFixture(t, 1)
	ctx := waitCtx(t)

	expires := time.Now().Add(300 * time.Millisecond)
	_, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "t", Action: "claim", Expires: &expires})
	if !errors.Is(err, taskrunner.ErrExpired) {
		t.Fatalf("Run error %v, want ErrExpired", err)
	}
	// Nothing runs it again once it has expired.
	n := runs.Load()
	f.createRef(t)
	time.Sleep(2 * time.Second)
	if v, _ := f.item(t, "ref"); v != 0 || runs.Load() != n {
		t.Errorf("an expired task ran again: item ref = %d, %d runs after expiry", v, runs.Load()-n)
	}
}

func TestRunDefaultsExpiresToTheContextDeadline_Live(t *testing.T) {
	f, runs := claimFixture(t, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: "t", Action: "claim"}); err == nil {
		t.Fatal("Run succeeded")
	}
	// The abandoned task is past its deadline, so a worker records it as
	// expired instead of resuming it.
	n := runs.Load()
	f.createRef(t)
	time.Sleep(2 * time.Second)
	if v, _ := f.item(t, "ref"); v != 0 || runs.Load() != n {
		t.Errorf("a task abandoned at its deadline ran later: item ref = %d", v)
	}
}

func TestAsyncCommitMissIsResumed_Live(t *testing.T) {
	f, runs := claimFixture(t, 1)
	ctx := waitCtx(t)

	id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "t", Action: "claim"})
	if err != nil {
		t.Fatal(err)
	}
	for runs.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}
	// The task stays submitted; once the row exists, a worker resumes it
	// after the lease expires.
	f.createRef(t)
	if _, err := f.r.Wait(ctx, qTop, id); err != nil {
		t.Fatal(err)
	}
	if v, _ := f.item(t, "ref"); v != 1 {
		t.Errorf("item ref = %d, want 1", v)
	}
}

func TestCrashedTaskIsResumed_Live(t *testing.T) {
	f := newFixture(t, 1)
	f.handle(t, qTop, "put", f.put())
	ctx := waitCtx(t)
	if err := f.r.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// A process that took the lease and crashed: a pending task and a lease
	// nobody renews.
	id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "a", Action: "put", Args: []byte("5")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.env.DB.Exec(ctx, `INSERT INTO `+f.env.Table("queue_top_leases")+` (subject, owner, expires, tree_created, tree_id)
		VALUES ('a', $1, now() + interval '500 milliseconds', now(), $2)`, uuid.New(), uuid.New()); err != nil {
		t.Fatal(err)
	}
	f.start(t)
	if _, err := f.r.Wait(ctx, qTop, id); err != nil {
		t.Fatal(err)
	}
	if v, _ := f.item(t, "a"); v != 5 {
		t.Errorf("item a = %d, want 5", v)
	}
}

func TestSubmitTxFollowsTheCallersTransaction_Live(t *testing.T) {
	f := newFixture(t, 2)
	f.handle(t, qTop, "put", f.put())
	f.start(t)
	ctx := waitCtx(t)

	for _, commit := range []bool{false, true} {
		tx, err := f.env.DB.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		id, err := f.r.SubmitTx(ctx, tx, taskrunner.Spec{Queue: qTop, Subject: "a", Action: "put", Args: []byte("9")})
		if err != nil {
			t.Fatal(err)
		}
		if !commit {
			_ = tx.Rollback(ctx)
			if _, err := f.r.Wait(ctx, qTop, id); !errors.Is(err, taskrunner.ErrNotFound) {
				t.Errorf("rolled-back task: Wait error %v, want ErrNotFound", err)
			}
			continue
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := f.r.Wait(ctx, qTop, id); err != nil {
			t.Fatal(err)
		}
	}
	if v, _ := f.item(t, "a"); v != 9 {
		t.Errorf("item a = %d, want 9", v)
	}
}

// Two trees whose sync children take the same two subjects in opposite
// orders would deadlock if each waited for the other. The younger gives way.
func TestSiblingChildrenInOppositeOrderDoNotDeadlock_Live(t *testing.T) {
	f := newFixture(t, 0)
	f.handle(t, qBottom, "put", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		time.Sleep(20 * time.Millisecond)
		return nil, ex.Write(ctx, `INSERT INTO `+f.env.Table("items")+` (k, v) VALUES ($1, 1)
			ON CONFLICT (k) DO UPDATE SET v = `+f.env.Table("items")+`.v + 1`, task.Subject)
	})
	f.handle(t, qTop, "pair", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		keys := []string{"x", "y"}
		if string(task.Args) == "reverse" {
			keys = []string{"y", "x"}
		}
		for _, k := range keys {
			if _, err := ex.Run(ctx, taskrunner.Spec{Queue: qBottom, Subject: k, Action: "put"}); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	f.start(t)
	ctx := waitCtx(t)

	const rounds = 5
	var wg sync.WaitGroup
	errs := make(chan error, 2*rounds)
	for i := range 2 * rounds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			args := []byte("forward")
			if i%2 == 1 {
				args = []byte("reverse")
			}
			_, err := f.r.Run(ctx, taskrunner.Spec{Queue: qTop, Subject: fmt.Sprintf("t%d", i), Action: "pair", Args: args})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range []string{"x", "y"} {
		if v, _ := f.item(t, k); v != 2*rounds {
			t.Errorf("item %s = %d, want %d", k, v, 2*rounds)
		}
	}
}
