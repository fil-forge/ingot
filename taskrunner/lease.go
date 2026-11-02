package taskrunner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// tryLease takes the subject's lease for a tree of the given age, or reports
// the age of the tree holding it. Both results are nil when the holder's age
// could not be read.
func (r *Runner) tryLease(ctx context.Context, q *queue, subject string, age treeAge) (*hold, *treeAge, error) {
	token := uuid.New()
	var taken bool
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
	RETURNING 1
)
SELECT true, NULL::timestamptz, NULL::uuid FROM taken
UNION ALL
SELECT false, tree_created, tree_id FROM `+q.leases+`
	WHERE subject = $1 AND NOT EXISTS (SELECT 1 FROM taken)`,
		subject, token, r.opts.LeaseTTL.Milliseconds(), age.created, age.id,
	).Scan(&taken, &holderCreated, &holderID)
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
	h := &hold{q: q, subject: subject, token: token, age: age}
	r.mu.Lock()
	r.holds[token] = h
	r.mu.Unlock()
	return h, nil, nil
}

// setAge records the tree now running under h, for other trees' wait-or-die
// decisions.
func (r *Runner) setAge(ctx context.Context, h *hold, age treeAge) error {
	if h.age == age {
		return nil
	}
	if _, err := r.db.Exec(ctx, `UPDATE `+h.q.leases+` SET tree_created = $3, tree_id = $4
		WHERE subject = $1 AND owner = $2`, h.subject, h.token, age.created, age.id); err != nil {
		return fmt.Errorf("record tree on lease %s/%s: %w", h.q.name, h.subject, err)
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
	if ch, ok := r.freed[h.key()]; ok {
		close(ch)
		delete(r.freed, h.key())
	}
	r.mu.Unlock()
}

// forget stops renewing h, leaving its row to expire.
func (r *Runner) forget(h *hold) {
	r.mu.Lock()
	delete(r.holds, h.token)
	r.mu.Unlock()
}

// endHold releases a root hold after a drain, or leaves it to expire when the
// drain stopped on a tree that could not commit.
func (r *Runner) endHold(ctx context.Context, h *hold, stop error) {
	if errors.Is(stop, errNotCommitted) {
		r.forget(h)
		return
	}
	r.release(ctx, h)
}

func (r *Runner) freedChan(key subjectKey) <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch, ok := r.freed[key]
	if !ok {
		ch = make(chan struct{})
		r.freed[key] = ch
	}
	return ch
}

// awaitFree waits until this process releases key's lease, d passes or ctx
// ends.
func (r *Runner) awaitFree(ctx context.Context, key subjectKey, d time.Duration) {
	free := r.freedChan(key)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-free:
	case <-t.C:
	case <-ctx.Done():
	}
}

// renewLoop extends every lease this process holds, three times per TTL. A
// lease found taken over is dropped; the tree under it then fails its commit.
func (r *Runner) renewLoop(ctx context.Context) {
	defer r.wg.Done()
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
		r.mu.Unlock()
		for q, tokens := range byQueue {
			r.renew(ctx, q, tokens)
		}
	}
}

func (r *Runner) renew(ctx context.Context, q *queue, tokens []uuid.UUID) {
	rows, err := r.db.Query(ctx, `UPDATE `+q.leases+` SET expires = now() + $2 * interval '1 millisecond'
		WHERE owner = ANY($1) RETURNING owner`, tokens, r.opts.LeaseTTL.Milliseconds())
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
				r.log.Warn("lease lost to another consumer", zapSubject(q.name, h.subject))
			}
		}
	}
}
