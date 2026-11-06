package taskrunner_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fil-forge/ingot/taskrunner"
)

// CancelPending cancels a subject's unstarted tasks of one action, a retrying
// one among them, and leaves other actions and subjects alone.
func TestCancelPendingCancelsTheSubjectsUnstartedTasksOfAnAction(t *testing.T) {
	f := newFixture(t, 1)
	var retried atomic.Int32
	if err := f.r.Handle(qTop, "release", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		retried.Add(1)
		return nil, errors.New("unavailable")
	}, taskrunner.WithRetry(taskrunner.RetryPolicy{First: time.Hour})); err != nil {
		t.Fatal(err)
	}
	f.handle(t, qTop, "expire", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) { return nil, nil })
	f.start(t)
	ctx := waitCtx(t)
	later := time.Now().Add(time.Hour)
	submit := func(subject, action string, at *time.Time) taskrunner.TaskID {
		t.Helper()
		id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: subject, Action: action, NotBefore: at})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	retrying := submit("sp/d", "release", nil)
	for retried.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	delayed := submit("sp/d", "release", &later)
	otherAction := submit("sp/d", "expire", &later)
	otherSubject := submit("sp/e", "release", &later)

	n, running, err := f.r.CancelPending(ctx, qTop, []string{"sp/d"}, "release")
	if err != nil || n != 2 || len(running) != 0 {
		t.Fatalf("CancelPending = %d, %v, %v; want 2 cancelled, none running", n, running, err)
	}
	for _, id := range []taskrunner.TaskID{retrying, delayed} {
		if _, err := f.r.Wait(ctx, qTop, id); !errors.Is(err, taskrunner.ErrCancelled) {
			t.Errorf("Wait %s: %v, want ErrCancelled", id, err)
		}
	}
	for _, id := range []taskrunner.TaskID{otherAction, otherSubject} {
		if got := f.state(t, qTop, id); got != "pending" {
			t.Errorf("task %s is %s, want left pending", id, got)
		}
	}
}

// CancelPending cannot cancel a task that is running, and says which subjects
// have one, while it cancels the others' in the same call.
func TestCancelPendingReportsARunningTask(t *testing.T) {
	f := newFixture(t, 1)
	started, release := make(chan struct{}), make(chan struct{})
	f.handle(t, qTop, "release", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		close(started)
		<-release
		return nil, nil
	})
	f.start(t)
	ctx := waitCtx(t)
	id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "sp/d", Action: "release"})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	later := time.Now().Add(time.Hour)
	if _, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "sp/e", Action: "release", NotBefore: &later}); err != nil {
		t.Fatal(err)
	}
	n, running, err := f.r.CancelPending(ctx, qTop, []string{"sp/e", "sp/d", "sp/f"}, "release")
	close(release)
	if err != nil || n != 1 || !slices.Equal(running, []string{"sp/d"}) {
		t.Fatalf("CancelPending = %d, %v, %v; want sp/e's cancelled and sp/d running", n, running, err)
	}
	if _, err := f.r.Wait(ctx, qTop, id); err != nil {
		t.Fatalf("the running task: %v, want it to finish", err)
	}
}

// CancelPendingTx's cancel commits or rolls back with the caller's own writes.
func TestCancelPendingTxRollsBackWithTheTransaction(t *testing.T) {
	f := newFixture(t, 0)
	f.handle(t, qTop, "release", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) { return nil, nil })
	f.migrate(t)
	ctx := waitCtx(t)
	later := time.Now().Add(time.Hour)
	id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "sp/d", Action: "release", NotBefore: &later})
	if err != nil {
		t.Fatal(err)
	}
	for _, commit := range []bool{false, true} {
		tx, err := f.env.DB.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		n, _, err := f.r.CancelPendingTx(ctx, tx, qTop, []string{"sp/d"}, "release")
		if err != nil || n != 1 {
			t.Fatalf("CancelPendingTx = %d, %v; want 1 cancelled", n, err)
		}
		if commit {
			err = tx.Commit(ctx)
		} else {
			err = tx.Rollback(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
		want := map[bool]string{false: "pending", true: "cancelled"}[commit]
		if got := f.state(t, qTop, id); got != want {
			t.Fatalf("after commit=%v the task is %s, want %s", commit, got, want)
		}
	}
}

// Expedite runs a group of subjects' delayed tasks of one action now, returns
// every one of them, due already or not, and leaves other actions and other
// subjects to their time.
func TestExpediteRunsAGroupOfSubjectsDelayedTasksNow(t *testing.T) {
	f := newFixture(t, 2)
	ran := make(chan string, 10)
	h := func(_ context.Context, _ *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		ran <- task.Subject + " " + task.Action
		return nil, nil
	}
	f.handle(t, qTop, "release", h)
	f.handle(t, qTop, "expire", h)
	f.migrate(t)
	ctx := waitCtx(t)
	later := time.Now().Add(time.Hour)
	submit := func(subject, action string, at *time.Time, replace bool) taskrunner.TaskID {
		t.Helper()
		id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: subject, Action: action, NotBefore: at, Replace: replace})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	a := submit("sp/a", "release", &later, true)
	b := submit("sp/b", "release", nil, false)
	otherAction := submit("sp/a", "expire", &later, false)
	// A wildcard in the prefix is matched literally.
	otherSpace := submit("spx/a", "release", &later, false)

	got, err := f.r.Expedite(ctx, qTop, "sp/*", "release")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !slices.Equal(got["sp/a"], []taskrunner.TaskID{a}) || !slices.Equal(got["sp/b"], []taskrunner.TaskID{b}) {
		t.Fatalf("Expedite = %v, want the group's two releases, by subject", got)
	}
	ids := []taskrunner.TaskID{a, b}
	// Brought forward, a replacement can no longer be pushed back.
	submit("sp/a", "release", &later, true)
	f.start(t)
	for _, id := range ids {
		if _, err := f.r.Wait(ctx, qTop, id); err != nil {
			t.Fatalf("Wait %s: %v", id, err)
		}
	}
	for _, id := range []taskrunner.TaskID{otherAction, otherSpace} {
		if got := f.state(t, qTop, id); got != "pending" {
			t.Errorf("task %s is %s, want left to its time", id, got)
		}
	}
}

// Expedite matches the whole subject unless it ends in *, and treats any other
// character, LIKE's wildcards among them, literally.
func TestExpediteMatchesTheSubjectOrAPrefix(t *testing.T) {
	f := newFixture(t, 0)
	f.handle(t, qTop, "release", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) { return nil, nil })
	f.migrate(t)
	ctx := waitCtx(t)
	later := time.Now().Add(time.Hour)
	for _, s := range []string{"a_b/1", "axb/1", "a%b/1", "a"} {
		if _, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: s, Action: "release", NotBefore: &later}); err != nil {
			t.Fatal(err)
		}
	}
	for subject, want := range map[string]int{"a": 1, "a_b/*": 1, "a%b/*": 1, "a*": 4, "a_b/1": 1, "a_b": 0} {
		got, err := f.r.Expedite(ctx, qTop, subject, "release")
		if err != nil || len(got) != want {
			t.Errorf("Expedite(%q) = %d subjects, %v; want %d", subject, len(got), err, want)
		}
	}
}

// A subject ending in * is refused: Expedite could never match it exactly.
func TestASubjectEndingInAStarIsRefused(t *testing.T) {
	f := newFixture(t, 0)
	f.handle(t, qTop, "release", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) { return nil, nil })
	f.migrate(t)
	if _, err := f.r.Submit(waitCtx(t), taskrunner.Spec{Queue: qTop, Subject: "sp/*", Action: "release"}); err == nil {
		t.Fatal("a subject ending in * was accepted")
	}
}

// Pending returns each subject's pending tasks of the action, delayed, due or
// running, oldest first, and changes nothing.
func TestPendingReportsSubjectsWithWorkLeft(t *testing.T) {
	// One worker to hold the running task, one to finish the others.
	f := newFixture(t, 2)
	started, release := make(chan struct{}), make(chan struct{})
	f.handle(t, qTop, "release", func(_ context.Context, _ *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
		if task.Subject == "sp/running" {
			close(started)
			<-release
		}
		return nil, nil
	})
	f.handle(t, qTop, "expire", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) { return nil, nil })
	f.start(t)
	ctx := waitCtx(t)
	later := time.Now().Add(time.Hour)
	submit := func(subject, action string, at *time.Time) taskrunner.TaskID {
		t.Helper()
		id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: subject, Action: action, NotBefore: at})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	running := submit("sp/running", "release", nil)
	<-started
	delayed := submit("sp/delayed", "release", &later)
	alsoDelayed := submit("sp/delayed", "release", &later)
	done := submit("sp/done", "release", nil)
	if _, err := f.r.Wait(ctx, qTop, done); err != nil {
		t.Fatal(err)
	}
	submit("sp/other-action", "expire", &later)

	got, err := f.r.Pending(ctx, qTop, []string{"sp/none", "sp/delayed", "sp/done", "sp/running", "sp/other-action"}, "release")
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]taskrunner.TaskID{"sp/delayed": {delayed, alsoDelayed}, "sp/running": {running}}
	if len(got) != len(want) || !slices.Equal(got["sp/delayed"], want["sp/delayed"]) || !slices.Equal(got["sp/running"], want["sp/running"]) {
		t.Fatalf("Pending = %v, want %v", got, want)
	}
	if f.state(t, qTop, delayed) != "pending" {
		t.Error("Pending changed the delayed task")
	}
	if _, err := f.r.Wait(ctx, qTop, running); err != nil {
		t.Fatal(err)
	}
}

// SubmitAllTx adds many tasks in one statement, in order, and keeps the later
// of two replacements for one subject and action, in the batch or already
// pending.
func TestSubmitAllTxAddsManyTasksAtOnce(t *testing.T) {
	f := newFixture(t, 0)
	f.handle(t, qTop, "release", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) { return nil, nil })
	f.migrate(t)
	ctx := waitCtx(t)
	at := func(d time.Duration) *time.Time { v := time.Now().Add(d).Truncate(time.Millisecond); return &v }
	pending, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "sp/later", Action: "release", NotBefore: at(2 * time.Hour), Replace: true})
	if err != nil {
		t.Fatal(err)
	}
	specs := []taskrunner.Spec{
		{Queue: qTop, Subject: "sp/a", Action: "release", NotBefore: at(time.Hour), Replace: true},
		{Queue: qTop, Subject: "sp/a", Action: "release", NotBefore: at(3 * time.Hour), Replace: true},
		{Queue: qTop, Subject: "sp/b", Action: "release"},
		{Queue: qTop, Subject: "sp/later", Action: "release", NotBefore: at(time.Hour), Replace: true},
	}
	tx, err := f.env.DB.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := f.r.SubmitAllTx(ctx, tx, specs)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 4 || ids[0] != ids[1] {
		t.Fatalf("ids = %v, want four, the two sp/a replacements sharing one", ids)
	}
	if got := f.notBefore(t, qTop, ids[0]); !got.Equal(*specs[1].NotBefore) {
		t.Errorf("sp/a due %v, want the later of the batch's two, %v", got, *specs[1].NotBefore)
	}
	if got := f.state(t, qTop, ids[2]); got != "pending" {
		t.Errorf("sp/b is %s, want pending", got)
	}
	if got := f.state(t, qTop, pending); got != "pending" {
		t.Errorf("the pending sp/later, due later than the batch's, is %s; want it kept", got)
	}
}

// waitForALockWait returns once a statement on the test database is waiting
// for a row lock.
func waitForALockWait(t *testing.T, f *fixture) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := f.env.DB.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no statement waited for a lock")
}

// CancelPending reports a task as running when its claim commits while the
// cancel is under way, rather than missing it in both answers.
func TestCancelPendingSeesAClaimCommittedMeanwhile(t *testing.T) {
	f := newFixture(t, 0)
	f.handle(t, qTop, "release", f.put())
	f.migrate(t)
	ctx := waitCtx(t)
	id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "sp/d", Action: "release"})
	if err != nil {
		t.Fatal(err)
	}
	// A worker's claim, held open until the cancel waits on it.
	claim, err := f.env.DB.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = claim.Rollback(ctx) }()
	if _, err := claim.Exec(ctx, `UPDATE `+f.env.Table(queueTable(qTop)+"_tasks")+` SET started = now() WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	type result struct {
		n       int
		running []string
		err     error
	}
	done := make(chan result, 1)
	go func() {
		n, running, err := f.r.CancelPending(ctx, qTop, []string{"sp/d"}, "release")
		done <- result{n, running, err}
	}()
	waitForALockWait(t, f)
	if err := claim.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.err != nil || got.n != 0 || !slices.Equal(got.running, []string{"sp/d"}) {
		t.Fatalf("CancelPending = %d, %v, %v; want none cancelled and sp/d running", got.n, got.running, got.err)
	}
}

// Expedite returns a replacement that commits while it runs by the
// replacement's own ID, so a caller waits on the task that will run.
func TestExpediteReturnsAReplacementCommittedMeanwhile(t *testing.T) {
	f := newFixture(t, 0)
	f.handle(t, qTop, "release", f.put())
	f.migrate(t)
	ctx := waitCtx(t)
	hour, twoHours := time.Now().Add(time.Hour), time.Now().Add(2*time.Hour)
	if _, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "sp/d", Action: "release", NotBefore: &hour, Replace: true}); err != nil {
		t.Fatal(err)
	}
	tx, err := f.env.DB.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	replacement, err := f.r.SubmitTx(ctx, tx, taskrunner.Spec{Queue: qTop, Subject: "sp/d", Action: "release", NotBefore: &twoHours, Replace: true})
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		due map[string][]taskrunner.TaskID
		err error
	}
	done := make(chan result, 1)
	go func() {
		due, err := f.r.Expedite(ctx, qTop, "sp/d", "release")
		done <- result{due, err}
	}()
	waitForALockWait(t, f)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.err != nil || !slices.Equal(got.due["sp/d"], []taskrunner.TaskID{replacement}) {
		t.Fatalf("Expedite = %v, %v; want the replacement %s", got.due, got.err, replacement)
	}
}

// Expedite brings forward a task waiting to retry, whether or not its retry
// holds the subject.
func TestExpediteBringsRetriesForward(t *testing.T) {
	for name, holds := range map[string]bool{"holding": true, "not holding": false} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, 1)
			var runs atomic.Int32
			if err := f.r.Handle(qTop, "release", func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
				if runs.Add(1) == 1 {
					return nil, errors.New("unavailable")
				}
				return nil, nil
			}, taskrunner.WithRetry(taskrunner.RetryPolicy{First: time.Hour, HoldsSubject: holds})); err != nil {
				t.Fatal(err)
			}
			f.start(t)
			ctx := waitCtx(t)
			id, err := f.r.Submit(ctx, taskrunner.Spec{Queue: qTop, Subject: "sp/d", Action: "release"})
			if err != nil {
				t.Fatal(err)
			}
			for runs.Load() == 0 {
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(100 * time.Millisecond) // the failure is recorded
			if _, err := f.r.Expedite(ctx, qTop, "sp/d", "release"); err != nil {
				t.Fatal(err)
			}
			if _, err := f.r.Wait(ctx, qTop, id); err != nil {
				t.Fatalf("Wait: %v, want the retry to run now and succeed", err)
			}
		})
	}
}
