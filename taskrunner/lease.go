package taskrunner

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// hold is this process's lease on one (queue, subject).
type hold struct {
	q       *queue
	subject string
	token   uuid.UUID
	age     treeAge
	// lost is set when the lease is found taken over or expired. Nothing more
	// may run under it. Set it with lose, which also stops a running handler.
	lost atomic.Bool
	// until is when the lease can first expire, on this process's clock (see
	// extend).
	until atomic.Int64
	// guard is the guard of the tree whose handlers are running under the
	// lease, or nil.
	guard atomic.Pointer[leaseGuard]
	// turn is the place in the subject's line of the Run caller holding the
	// lease, or nil for a worker or a tree's child. Releasing the lease does
	// not wake it: it released the lease itself, and backs off.
	turn *runTurn
	// claimed is set when the statement that took the lease also claimed
	// the task to run first under it and recorded that task's tree age, so
	// the task need not record its age, nor check the lease, again if it
	// starts soon after (see runRoot). It holds for that one task.
	claimed bool
	// taken is when the statement that took the lease was sent: the lease
	// runs for a TTL from no earlier than this.
	taken time.Time
	// releaseOnCommit is set for a Run caller's hold: its task's commit
	// deletes the lease row in the same transaction, rather than extending
	// it, since the caller releases the lease once its task has run.
	releaseOnCommit bool
	// released is set once a commit has deleted the lease row, so ending the
	// hold has nothing left to delete.
	released bool
}

func (h *hold) key() subjectKey { return subjectKey{h.q.name, h.subject} }

// clockStart is the instant hold.until counts from, so that it is kept and
// compared on the monotonic clock.
var clockStart = time.Now()

// extend records that a statement sent at sent set the lease to expire a TTL
// from the database's now. The database ran it no earlier than sent, so the
// lease lasts until at least sent plus the TTL by this process's clock,
// whatever the two clocks read. Statements can finish out of order; the
// latest expiry stands, as it does in the row.
func (h *hold) extend(sent time.Time, ttl time.Duration) {
	until := int64(sent.Add(ttl).Sub(clockStart))
	for {
		cur := h.until.Load()
		if until <= cur || h.until.CompareAndSwap(cur, until) {
			return
		}
	}
}

// heldUntil is when the lease can first expire if nothing extends it.
func (h *hold) heldUntil() time.Time { return clockStart.Add(time.Duration(h.until.Load())) }

// lose marks the lease lost and stops the handlers running under it.
func (h *hold) lose() {
	h.lost.Store(true)
	if g := h.guard.Load(); g != nil {
		g.trip(h)
	}
}

// leaseMargin is how long before a lease can expire the handlers under it are
// stopped: a sixth of the TTL, 5 seconds of the default 30. Renewal extends a
// lease three times per TTL, so one renewal that fails changes nothing and a
// second stops the handlers. The margin is the time a request a handler has
// just sent has to arrive before another consumer can take the lease.
func (r *Runner) leaseMargin() time.Duration { return r.opts.LeaseTTL / 6 }

// leaseGuard ends the context a tree's handlers run under once any lease the
// tree holds is lost, or comes within the margin of expiring without having
// been extended. A handler's effects outside the database are not undone when
// its tree fails to commit, so it must not go on making them once another
// consumer may hold its subject. Renewal reports a lease taken over; the
// guard's own timer covers a renewal that cannot reach the database, or that
// the process was too stalled to send.
type leaseGuard struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	margin time.Duration
	root   *hold

	mu       sync.Mutex
	children []*hold
	timer    *time.Timer
	stopped  bool
}

// guardLeases returns a guard over root, the lease a tree's root task runs
// under. Its ctx is the one to run the tree's handlers with. Call stop once
// they have returned.
func (r *Runner) guardLeases(ctx context.Context, root *hold) *leaseGuard {
	g := &leaseGuard{margin: r.leaseMargin(), root: root}
	g.ctx, g.cancel = context.WithCancelCause(ctx)
	root.guard.Store(g)
	g.mu.Lock()
	g.timer = time.AfterFunc(time.Until(root.heldUntil().Add(-g.margin)), g.tick)
	g.mu.Unlock()
	if root.lost.Load() {
		g.trip(root)
	}
	return g
}

// watch adds a sync child's lease to the guard. A lease just taken expires
// later than any the timer is set for, so the timer stands.
func (g *leaseGuard) watch(h *hold) {
	g.mu.Lock()
	g.children = append(g.children, h)
	g.mu.Unlock()
	h.guard.Store(g)
	if h.lost.Load() {
		g.trip(h)
	}
}

// trip ends the guard's context for the loss of h: with errLeaseLost for the
// root's lease, whose task another consumer now runs, and a *childLostError
// for a child's, which the tree can take afresh.
func (g *leaseGuard) trip(h *hold) {
	if h == g.root {
		g.cancel(errLeaseLost)
		return
	}
	g.cancel(&childLostError{h.key()})
}

// lostTo returns the loss that ended the guard's context, or nil when it has
// not ended or ended for another reason, such as its parent ending.
func (g *leaseGuard) lostTo() error {
	if g.ctx.Err() == nil {
		return nil
	}
	cause := context.Cause(g.ctx)
	var child *childLostError
	if errors.Is(cause, errLeaseLost) || errors.As(cause, &child) {
		return cause
	}
	return nil
}

// next returns the first lease that is lost or within the margin of expiring
// at now, or else how long until the earliest one will be. g.mu must be held.
func (g *leaseGuard) next(now time.Time) (time.Duration, *hold) {
	earliest := g.root.heldUntil()
	for _, h := range append([]*hold{g.root}, g.children...) {
		until := h.heldUntil()
		if h.lost.Load() || !now.Before(until.Add(-g.margin)) {
			return 0, h
		}
		if until.Before(earliest) {
			earliest = until
		}
	}
	return earliest.Add(-g.margin).Sub(now), nil
}

// tick is the guard's timer: it trips the guard, or sets the timer for the
// time the leases, extended since it was set, now run to.
func (g *leaseGuard) tick() {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		return
	}
	wait, lapsed := g.next(time.Now())
	if lapsed == nil {
		g.timer.Reset(wait)
	}
	g.mu.Unlock()
	if lapsed != nil {
		g.trip(lapsed)
	}
}

// stop ends the guard once the tree's handlers have returned.
func (g *leaseGuard) stop() {
	g.mu.Lock()
	g.stopped = true
	g.timer.Stop()
	children := g.children
	g.mu.Unlock()
	g.root.guard.CompareAndSwap(g, nil)
	for _, h := range children {
		h.guard.CompareAndSwap(g, nil)
	}
	g.cancel(nil)
}

// treeAge orders task trees for deadlock avoidance: the root task's creation
// time, then its ID.
type treeAge struct {
	created time.Time
	id      TaskID
}

func (a treeAge) olderThan(b treeAge) bool {
	if !a.created.Equal(b.created) {
		return a.created.Before(b.created)
	}
	return bytes.Compare(a.id[:], b.id[:]) < 0
}

// dieError aborts a tree that asked for a lease held by an older tree. The
// tree releases every lease it holds and starts again, keeping its age, so it
// eventually becomes the oldest and waits instead. Two trees can never wait
// on each other this way, whatever order their sync children take leases in.
type dieError struct{ key subjectKey }

func (e *dieError) Error() string {
	return fmt.Sprintf("gave way to an older task holding %s/%s", e.key.queue, e.key.subject)
}

// errNotCommitted ends a hold without releasing it: the tree could not commit
// and its task is left for resumption once the lease expires.
var errNotCommitted = errors.New("not committed")

// errLeaseLost ends a hold whose lease another consumer has taken over. The
// task under it gets no outcome from this process: the new owner runs it.
var errLeaseLost = errors.New("lease lost to another consumer")

// errNotPending stops a run whose task another consumer finished first.
var errNotPending = errors.New("task no longer pending")

// freeSub is a set of waiters on one subject's lease being released in this
// process.
type freeSub struct {
	ch chan struct{}
	n  int
}

// tryLease takes the subject's lease for a tree of the given age, or reports
// the age of the tree holding it. Both results are nil when the holder's age
// could not be read.
func (r *Runner) tryLease(ctx context.Context, q *queue, subject string, age treeAge) (*hold, *treeAge, error) {
	token := uuid.New()
	var taken, tookOver bool
	var holderCreated *time.Time
	var holderID *uuid.UUID
	sent := time.Now()
	err := r.db.QueryRow(ctx, `
WITH taken AS (
	INSERT INTO `+q.leases+` AS l (subject, owner, expires, tree_created, tree_id)
	VALUES ($1, $2, now() + $3 * interval '1 millisecond', $4, $5)
	ON CONFLICT (subject) DO UPDATE
		SET owner = EXCLUDED.owner, expires = EXCLUDED.expires,
		    tree_created = EXCLUDED.tree_created, tree_id = EXCLUDED.tree_id
		WHERE l.expires < now()
	RETURNING xmax <> 0 AS took_over
)
SELECT true, took_over, NULL::timestamptz, NULL::uuid FROM taken
UNION ALL
SELECT false, false, tree_created, tree_id FROM `+q.leases+`
	WHERE subject = $1 AND NOT EXISTS (SELECT 1 FROM taken)`,
		subject, token, r.opts.LeaseTTL.Milliseconds(), age.created, age.id,
	).Scan(&taken, &tookOver, &holderCreated, &holderID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// The holder committed after this statement's snapshot was taken.
		return nil, nil, nil
	case err != nil:
		return nil, nil, fmt.Errorf("take lease %s/%s: %w", q.name, subject, err)
	}
	if !taken {
		return nil, &treeAge{*holderCreated, *holderID}, nil
	}
	if tookOver {
		// The row existed and had expired: its owner stopped renewing.
		r.metrics.observeTakeover(ctx, q)
	}
	h := &hold{q: q, subject: subject, token: token, age: age}
	h.extend(sent, r.opts.LeaseTTL)
	r.mu.Lock()
	r.holds[token] = h
	r.mu.Unlock()
	return h, nil, nil
}

// takeTurn is tryLease and claim in one statement, for a Run caller whose
// task, target with sequence number upTo, is to run first under the lease.
// It takes the lease only when that task is the subject's oldest due task.
// When another task is, it takes nothing, and returns an *aheadError for a
// task the workers are to run, so the caller can wake them. For one whose own
// caller is waiting for it, here or on another instance, one that waits to
// retry, or one placed on another instance, it returns nothing: the caller
// backs off until that task has gone. When the caller's task is next, it returns a
// nil hold if the lease is held, and otherwise the hold and the task, or the
// hold alone for the caller to claim under afresh, as claim does: the
// statement's view of the tasks is as old as the statement, and taking the
// lease may have waited for a commit that changed them.
func (r *Runner) takeTurn(ctx context.Context, q *queue, subject string, age treeAge, upTo int64, target TaskID) (_ *hold, _ *Task, ahead, err error) {
	token := uuid.New()
	var taken, tookOver bool
	var next *uuid.UUID
	var waited, backingOff *bool
	var node *string
	var c struct {
		seq                *int64
		id                 *uuid.UUID
		action             *string
		created            *time.Time
		notBefore, expires *time.Time
		args               []byte
		sync               *bool
		attempts           *int
		node               *string
	}
	sent := time.Now()
	// Every part reads the snapshot the statement started with. A write in
	// a WITH runs whether or not anything reads it, so the lease is taken
	// from mine, which is empty unless the caller's task is next, and the
	// claim joins taken. Taking the lease can wait for a holder's commit
	// that deletes its row, and the snapshot is then older than that
	// commit, so the claim stands only when the snapshot had no live lease
	// to wait for: no row, or one this statement took over once it expired.
	// The claim runs whether or not the lease was taken, and names its task
	// by subject and sequence number as well as ID, so whichever index the
	// planner picks finds the one row rather than scanning pending tasks.
	err = r.db.QueryRow(ctx, `
WITH prior AS (
	SELECT 1 FROM `+q.leases+` WHERE subject = $1
), next AS (
	SELECT id, coalesce(waiting_until > now(), false) AS waited,
		coalesce(retry_at > now(), false) AS backing_off, node FROM `+q.tasks+`
	WHERE subject = $1 AND state = 'pending' AND seq <= $6
	AND (not_before IS NULL OR not_before <= now())
	ORDER BY seq LIMIT 1
), mine AS (
	SELECT 1 FROM next WHERE id = $7 AND NOT backing_off AND (node IS NULL OR node = $8)
), taken AS (
	INSERT INTO `+q.leases+` AS l (subject, owner, expires, tree_created, tree_id)
	SELECT $1, $2, now() + $3 * interval '1 millisecond', $4, $5 FROM mine
	ON CONFLICT (subject) DO UPDATE
		SET owner = EXCLUDED.owner, expires = EXCLUDED.expires,
		    tree_created = EXCLUDED.tree_created, tree_id = EXCLUDED.tree_id
		WHERE l.expires < now()
	RETURNING xmax <> 0 AS took_over
), claimed AS (
	UPDATE `+q.tasks+` t SET started = coalesce(t.started, now())
	FROM taken
	WHERE t.subject = $1 AND t.seq = $6 AND t.id = $7 AND t.state = 'pending'
	AND (taken.took_over OR NOT EXISTS (SELECT 1 FROM prior))
	RETURNING t.seq, t.id, t.action, t.created, t.not_before, t.expires, t.args, t.sync, t.attempts, t.node
)
SELECT EXISTS (SELECT 1 FROM taken), coalesce((SELECT took_over FROM taken), false),
	next.id, next.waited, next.backing_off, next.node,
	claimed.seq, claimed.id, claimed.action, claimed.created, claimed.not_before,
	claimed.expires, claimed.args, claimed.sync, claimed.attempts, claimed.node
FROM (SELECT 1) one LEFT JOIN next ON true LEFT JOIN claimed ON true`,
		subject, token, r.opts.LeaseTTL.Milliseconds(), age.created, age.id, upTo, target, r.opts.Node,
	).Scan(&taken, &tookOver, &next, &waited, &backingOff, &node,
		&c.seq, &c.id, &c.action, &c.created, &c.notBefore, &c.expires, &c.args, &c.sync, &c.attempts, &c.node)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("take lease %s/%s: %w", q.name, subject, err)
	}
	if !taken {
		switch {
		case next == nil, *next == target, *waited, *backingOff, !r.mayRun(node):
			// Nothing is due, the caller's task is next and the lease is
			// held, or the oldest due task is its own caller's to run, or
			// holds the subject.
			return nil, nil, nil, nil
		default:
			return nil, nil, &aheadError{*next}, nil
		}
	}
	if tookOver {
		r.metrics.observeTakeover(ctx, q)
	}
	h := &hold{q: q, subject: subject, token: token, age: age, releaseOnCommit: true}
	h.extend(sent, r.opts.LeaseTTL)
	r.mu.Lock()
	r.holds[token] = h
	r.mu.Unlock()
	if c.id == nil {
		return h, nil, nil, nil
	}
	h.claimed, h.taken = true, sent
	return h, &Task{
		Queue: q.name, Subject: subject, ID: *c.id, Action: *c.action, Created: *c.created,
		NotBefore: c.notBefore, Expires: c.expires, Args: c.args, sync: *c.sync,
		Attempt: *c.attempts, Node: c.node, seq: *c.seq,
	}, nil, nil
}

// setAge records the tree about to run under h, for other trees' wait-or-die
// decisions. It is also the check, before each task's handler starts, that h
// still owns the lease, which it extends by a TTL from now, so the handler
// starts with the whole of it: it returns errLeaseLost when another consumer
// has taken the lease over or it has expired.
func (r *Runner) setAge(ctx context.Context, h *hold, age treeAge) error {
	sent := time.Now()
	tag, err := r.db.Exec(ctx, `UPDATE `+h.q.leases+` SET tree_created = $3, tree_id = $4,
		expires = now() + $5 * interval '1 millisecond'
		WHERE subject = $1 AND owner = $2 AND expires >= now()`, h.subject, h.token, age.created, age.id, r.opts.LeaseTTL.Milliseconds())
	if err != nil {
		return fmt.Errorf("record tree on lease %s/%s: %w", h.q.name, h.subject, err)
	}
	if tag.RowsAffected() == 0 {
		h.lose()
		return errLeaseLost
	}
	h.extend(sent, r.opts.LeaseTTL)
	h.age = age
	return nil
}

// release deletes the lease and wakes this process's waiters on the subject.
func (r *Runner) release(ctx context.Context, h *hold) {
	r.forget(h)
	ctx = context.WithoutCancel(ctx)
	if _, err := r.db.Exec(ctx, `DELETE FROM `+h.q.leases+` WHERE subject = $1 AND owner = $2`,
		h.subject, h.token); err != nil {
		r.log.Warn("release lease; it expires on its own",
			zapSubject(h.q.name, h.subject), zapErr(err))
	}
	r.wakeReleased(h)
}

// releasedInCommit ends h once a commit has deleted its lease row.
func (r *Runner) releasedInCommit(h *hold) {
	h.released = true
	r.forget(h)
	r.wakeReleased(h)
}

// wakeReleased wakes this process's waiters on h's subject once its lease
// row is gone.
func (r *Runner) wakeReleased(h *hold) {
	r.mu.Lock()
	if sub, ok := r.freed[h.key()]; ok {
		close(sub.ch)
		delete(r.freed, h.key())
	}
	if line := r.lines[h.key()]; len(line) > 0 && line[0] != h.turn {
		r.wakeFront(h.key())
	}
	r.mu.Unlock()
}

// runTurn is one Run caller's place in its subject's line.
type runTurn struct {
	seq int64
	// wake is signalled when the caller should look at the subject again:
	// the lease was released in this process, or the caller came to the
	// front. It holds one signal, so none is lost while the caller is busy.
	wake chan struct{}
}

// A subject's line holds this process's Run callers in their tasks' order.
// Their tasks run in that order, so only the caller at the front can make
// progress: the others wait without querying, and a release wakes the front
// caller alone rather than every caller on the subject, each of which would
// try the lease for one of them to win it. Callers in other processes, and
// tasks the workers run, are not in the line; the front caller's backoff
// covers them.

// joinLine puts a Run caller of the task with the given seq in key's line.
func (r *Runner) joinLine(key subjectKey, seq int64) *runTurn {
	t := &runTurn{seq: seq, wake: make(chan struct{}, 1)}
	r.mu.Lock()
	defer r.mu.Unlock()
	line := r.lines[key]
	i, _ := slices.BinarySearchFunc(line, seq, func(t *runTurn, seq int64) int { return cmp.Compare(t.seq, seq) })
	r.lines[key] = slices.Insert(line, i, t)
	return t
}

// leaveLine takes t out of key's line, waking the caller that comes to the
// front in its place.
func (r *Runner) leaveLine(key subjectKey, t *runTurn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	line := r.lines[key]
	i := slices.Index(line, t)
	if i < 0 {
		return
	}
	line = slices.Delete(line, i, i+1)
	if len(line) == 0 {
		delete(r.lines, key)
		return
	}
	r.lines[key] = line
	if i == 0 {
		r.wakeFront(key)
	}
}

// frontOfLine reports whether t is at the front of key's line.
func (r *Runner) frontOfLine(key subjectKey, t *runTurn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	line := r.lines[key]
	return len(line) > 0 && line[0] == t
}

// wakeFront wakes the caller at the front of key's line. r.mu must be held.
func (r *Runner) wakeFront(key subjectKey) {
	if line := r.lines[key]; len(line) > 0 {
		select {
		case line[0].wake <- struct{}{}:
		default:
		}
	}
}

// turnWait is how a Run caller's wait for its turn ended.
type turnWait struct {
	// done is set when the task has an outcome, or the caller must stop:
	// res and err are what Run returns.
	done bool
	res  []byte
	err  error
	// woken is set when t was woken.
	woken bool
	// finished is set when done came from the task's recorded outcome, or
	// from the task being gone: it no longer needs its caller's claim.
	finished bool
}

// hasOutcome reports whether err, from outcome, says the task has an outcome
// recorded or is gone, rather than that reading it failed.
func hasOutcome(err error) bool {
	var herr *HandlerError
	return err == nil || errors.As(err, &herr) || errors.Is(err, ErrExpired) ||
		errors.Is(err, ErrCancelled) || errors.Is(err, ErrNotFound)
}

// awaitTurn reads the task's outcome and, while it is still pending and
// before expires, waits until the task finishes in this process, t is woken,
// d passes, or ctx ends. It listens for the finish before it reads, as Wait
// does, so a finish committed in between still wakes it.
//
// ahead, when set, is a task the caller found due before its own. The wait
// ends as well once that task is over in this process, as when it is
// cancelled and no subject is released to wake t. It is read after the
// listening starts, so one over since the caller saw it ends the wait at
// once.
func (r *Runner) awaitTurn(ctx context.Context, q *queue, id TaskID, t *runTurn, d time.Duration, expires *time.Time, ahead TaskID) turnWait {
	ch := r.addWaiter(id)
	defer r.removeWaiter(id, ch)
	var gone chan struct{}
	if ahead != (TaskID{}) {
		gone = r.addWaiter(ahead)
		defer r.removeWaiter(ahead, gone)
	}
	if res, err, done := r.outcome(ctx, q, id); done {
		return turnWait{done: true, res: res, err: err, finished: hasOutcome(err)}
	}
	if gone != nil {
		if _, _, over := r.outcome(ctx, q, ahead); over {
			return turnWait{}
		}
	}
	// Past its Expires the task can no longer start: whoever takes the
	// subject next records it as expired, so the caller can stop waiting.
	if expires != nil {
		left := time.Until(*expires)
		if left <= 0 {
			return turnWait{done: true, err: ErrExpired}
		}
		d = min(d, left)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ch:
	case <-gone:
	case <-t.wake:
		return turnWait{woken: true}
	case <-timer.C:
	case <-ctx.Done():
		return turnWait{done: true, err: ctx.Err()}
	}
	return turnWait{}
}

// forget stops renewing h, leaving its row to expire.
func (r *Runner) forget(h *hold) {
	r.mu.Lock()
	delete(r.holds, h.token)
	r.mu.Unlock()
}

// endHold releases a root hold after a drain, or leaves it to expire when the
// drain stopped on a tree that could not commit or lost the lease.
func (r *Runner) endHold(ctx context.Context, h *hold, stop error) {
	if h.released {
		return
	}
	if errors.Is(stop, errNotCommitted) || errors.Is(stop, errLeaseLost) {
		r.forget(h)
		return
	}
	r.release(ctx, h)
}

// subscribeFree returns a channel closed when this process next releases
// key's lease, and a func that ends the subscription.
func (r *Runner) subscribeFree(key subjectKey) (<-chan struct{}, func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sub, ok := r.freed[key]
	if !ok {
		sub = &freeSub{ch: make(chan struct{})}
		r.freed[key] = sub
	}
	sub.n++
	return sub.ch, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.freed[key] != sub {
			return // released and removed already
		}
		if sub.n--; sub.n == 0 {
			delete(r.freed, key)
		}
	}
}

// awaitFree waits until this process releases key's lease, d passes or ctx
// ends.
func (r *Runner) awaitFree(ctx context.Context, key subjectKey, d time.Duration) {
	free, unsubscribe := r.subscribeFree(key)
	defer unsubscribe()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-free:
	case <-t.C:
	case <-ctx.Done():
	}
}

// renewLoop extends every lease this process holds and has not let expire, and every sync task's
// claim that its caller is waiting, three times per TTL. A lease found taken
// over is marked lost: the handlers running under it are stopped (see
// leaseGuard), and the tree under it stops before its next attempt and fails
// its commit.
func (r *Runner) renewLoop(ctx context.Context) {
	defer close(r.renewDone)
	t := time.NewTicker(r.opts.LeaseTTL / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		r.mu.Lock()
		byQueue := map[*queue][]uuid.UUID{}
		for token, h := range r.holds {
			byQueue[h.q] = append(byQueue[h.q], token)
		}
		waiting := map[*queue][]TaskID{}
		for id, q := range r.waiting {
			waiting[q] = append(waiting[q], id)
		}
		r.mu.Unlock()
		for q, tokens := range byQueue {
			r.renew(ctx, q, tokens)
		}
		for q, ids := range waiting {
			if _, err := r.db.Exec(ctx, `UPDATE `+q.tasks+` SET waiting_until = now() + $2 * interval '1 millisecond'
				WHERE id = ANY($1) AND state = 'pending' AND waiting_until IS NOT NULL`, ids, r.opts.LeaseTTL.Milliseconds()); err != nil && ctx.Err() == nil {
				r.log.Warn("renew waiting sync tasks", zapQueue(q.name), zapErr(err))
			}
		}
	}
}

func (r *Runner) renew(ctx context.Context, q *queue, tokens []uuid.UUID) {
	// A row a commit holds locked is skipped rather than waited for: the
	// commit extended it when it took the lock (lockLease), and waiting would
	// hold up the renewal of every other lease until that commit ends. A
	// skipped row is still ours, and counts as kept whatever expiry it shows:
	// this statement sees the expiry from before the commit extended it, which
	// a long commit outlasts. Only a row renewal could lock is judged by its
	// expiry.
	sent := time.Now()
	rows, err := r.db.Query(ctx, `WITH mine AS (
			SELECT subject, owner, expires FROM `+q.leases+` WHERE owner = ANY($1)
			FOR UPDATE SKIP LOCKED),
		renewed AS (
			UPDATE `+q.leases+` l SET expires = now() + $2 * interval '1 millisecond'
			FROM mine WHERE l.subject = mine.subject AND l.owner = mine.owner
				AND mine.expires >= now() RETURNING l.owner)
		SELECT owner, true FROM renewed
		UNION ALL
		SELECT owner, false FROM `+q.leases+` WHERE owner = ANY($1)
			AND owner NOT IN (SELECT owner FROM mine)`, tokens, r.opts.LeaseTTL.Milliseconds())
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("renew leases", zapQueue(q.name), zapErr(err))
		}
		return
	}
	// kept maps each lease still held to whether this statement extended it.
	kept := map[uuid.UUID]bool{}
	for rows.Next() {
		var token uuid.UUID
		var renewed bool
		if err := rows.Scan(&token, &renewed); err == nil {
			kept[token] = renewed
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return
	}
	for _, token := range tokens {
		renewed, ok := kept[token]
		r.mu.Lock()
		h := r.holds[token]
		if !ok {
			delete(r.holds, token)
		}
		r.mu.Unlock()
		if h == nil {
			continue
		}
		if renewed {
			h.extend(sent, r.opts.LeaseTTL)
		}
		if !ok {
			h.lose()
			r.log.Warn("lease lost to another consumer", zapSubject(q.name, h.subject))
		}
	}
}
