// Package taskrunner runs durable tasks from queues kept in Postgres.
//
// Each queue serializes its tasks on a subject: at most one task per (queue,
// subject) runs at a time, held by a lease row in the queue's lease table, so
// exclusivity holds across processes. A subject's due tasks run oldest first,
// and a task waiting on its NotBefore never holds up the ones behind it.
//
// A task runs as the root of a tree. Its handler may spawn async tasks, which
// workers run later, and run sync children inline, each on a subject of a
// later queue (the order is the order of Options.Queues). Handlers add their
// Postgres writes to an ordered list instead of writing directly; once every
// handler in the tree has run, the list, the spawned tasks' rows, the sync
// children's rows and the root's completion commit in one short transaction.
// No transaction stays open across a handler's network calls.
//
// A task's row commits when it is submitted, so a crash while it runs leaves
// it pending, and it is resumed once its lease expires. An async task whose
// final commit fails is left the same way; a sync task's caller retries the
// commit until it lands or the task's Expires passes. Only an error a handler
// returns marks a task failed. Handlers must therefore be idempotent, and
// must not read rows their own tree has written: those rows reach Postgres
// only at the commit. taskrunnertest.CheckOwnWrites runs a tree both ways to
// catch a handler that does.
//
// A handler's context ends when a lease its tree runs under is lost or is
// about to expire unrenewed, so a handler that passes its context to its
// network calls makes none once another consumer may hold its subject.
// Exec.LeaseHeld checks the same directly.
//
// The runner stores a task's arguments and result as opaque bytes and knows
// nothing of what its handlers do.
package taskrunner
