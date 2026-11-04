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

	n, running, err := f.r.CancelPending(ctx, qTop, "sp/d", "release")
	if err != nil || n != 2 || running {
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

// CancelPending cannot cancel a task that is running, and says so.
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
	n, running, err := f.r.CancelPending(ctx, qTop, "sp/d", "release")
	close(release)
	if err != nil || n != 0 || !running {
		t.Fatalf("CancelPending = %d, %v, %v; want none cancelled, one running", n, running, err)
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
		n, _, err := f.r.CancelPendingTx(ctx, tx, qTop, "sp/d", "release")
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

	ids, err := f.r.Expedite(ctx, qTop, "sp/*", "release")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ids, []taskrunner.TaskID{a, b}) {
		t.Fatalf("Expedite = %v, want the group's two releases %v", ids, []taskrunner.TaskID{a, b})
	}
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
		ids, err := f.r.Expedite(ctx, qTop, subject, "release")
		if err != nil || len(ids) != want {
			t.Errorf("Expedite(%q) = %d tasks, %v; want %d", subject, len(ids), err, want)
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
