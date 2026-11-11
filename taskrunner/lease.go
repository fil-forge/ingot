package taskrunner

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
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
	// lost is set when renewal finds the lease taken over. Nothing more may
	// run under it.
	lost atomic.Bool
}

func (h *hold) key() subjectKey { return subjectKey{h.q.name, h.subject} }

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
	r.mu.Lock()
	r.holds[token] = h
	r.mu.Unlock()
	return h, nil, nil
}

// setAge records the tree about to run under h, for other trees' wait-or-die
// decisions. It is also the check, before each task's handler starts, that h
// still owns the lease: it returns errLeaseLost when another consumer has
// taken it over.
func (r *Runner) setAge(ctx context.Context, h *hold, age treeAge) error {
	tag, err := r.db.Exec(ctx, `UPDATE `+h.q.leases+` SET tree_created = $3, tree_id = $4
		WHERE subject = $1 AND owner = $2 AND expires >= now()`, h.subject, h.token, age.created, age.id)
	if err != nil {
		return fmt.Errorf("record tree on lease %s/%s: %w", h.q.name, h.subject, err)
	}
	if tag.RowsAffected() == 0 {
		h.lost.Store(true)
		return errLeaseLost
	}
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
	r.mu.Lock()
	if sub, ok := r.freed[h.key()]; ok {
		close(sub.ch)
		delete(r.freed, h.key())
	}
	r.wakeFront(h.key())
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

// awaitTurn waits until the task finishes in this process, t is woken, d
// passes, or ctx ends.
func (r *Runner) awaitTurn(ctx context.Context, id TaskID, t *runTurn, d time.Duration) error {
	ch := r.addWaiter(id)
	defer r.removeWaiter(id, ch)
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ch:
	case <-t.wake:
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
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
// over is marked lost; the tree under it stops before its next attempt and
// fails its commit.
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
	rows, err := r.db.Query(ctx, `WITH mine AS (
			SELECT subject, owner, expires FROM `+q.leases+` WHERE owner = ANY($1)
			FOR UPDATE SKIP LOCKED),
		renewed AS (
			UPDATE `+q.leases+` l SET expires = now() + $2 * interval '1 millisecond'
			FROM mine WHERE l.subject = mine.subject AND l.owner = mine.owner
				AND mine.expires >= now() RETURNING l.owner)
		SELECT owner FROM renewed
		UNION ALL
		SELECT owner FROM `+q.leases+` WHERE owner = ANY($1)
			AND owner NOT IN (SELECT owner FROM mine)`, tokens, r.opts.LeaseTTL.Milliseconds())
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("renew leases", zapQueue(q.name), zapErr(err))
		}
		return
	}
	kept := map[uuid.UUID]bool{}
	for rows.Next() {
		var token uuid.UUID
		if err := rows.Scan(&token); err == nil {
			kept[token] = true
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return
	}
	for _, token := range tokens {
		if !kept[token] {
			r.mu.Lock()
			h := r.holds[token]
			delete(r.holds, token)
			r.mu.Unlock()
			if h != nil {
				h.lost.Store(true)
				r.log.Warn("lease lost to another consumer", zapSubject(q.name, h.subject))
			}
		}
	}
}
