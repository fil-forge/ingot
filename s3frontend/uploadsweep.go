package s3frontend

import (
	"context"
	"fmt"
	"time"

	uploadcmds "github.com/fil-forge/libforge/commands/upload"
	"github.com/fil-forge/ucantone/ucan"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// uploadRegistrationBatch is how many queued registrations one sweep claims.
// The sweep spends at most two round trips on them — one for the additions and
// one for the retractions — so this is the batch size, not a call count.
const uploadRegistrationBatch = 256

// uploadRegistrationDeadLetterAfter is how many failed attempts a row gets
// before it is dead-lettered once its authority is spent. The backoff caps at 30 minutes, so
// this spans about three hours — long enough for a write to the space to leave
// fresh authority behind, and short enough that a dead row stops holding its
// key back within a shift.
//
// That budget has to stay inside the overhang hilt adds to a delegation's life
// past the derived key's own (hilt's rpc.AsyncOverhang, four hours). Retrying
// for longer than the authority lasts turns an ordinary outage into a
// dead-lettered row: the change expires rather than completing, and the
// symptom shows up
// here rather than where the window is set. Raise this and the overhang moves
// with it.
const uploadRegistrationDeadLetterAfter = 10

// uploadRegistrationBackoff is how long a failed registration waits before the
// next attempt, growing with the attempt count up to the cap. The object count
// is a reporting number, so a Sprue outage is worth waiting out rather than
// hammering.
func uploadRegistrationBackoff(attempts int) time.Duration {
	d := time.Minute << min(attempts, 5)
	if d > 30*time.Minute {
		d = 30 * time.Minute
	}
	return d
}

// SweepUploadRegistrations drains the upload-registration outbox: the queued
// changes are replayed against the upload service and deleted once taken, and
// failures are rescheduled with backoff for the next sweep. Returns how many
// landed. Called periodically by the daemon's registration sweeper, and
// directly by tests as the drain.
//
// The claimed rows go out as two batches, every addition before any
// retraction. That ordering is what keeps the count honest, and it is safe for
// the same reason it is needed: a root is registered by the commit that
// creates the version and retracted by a later one, so an add always carries a
// lower seq than the remove that retires it. Sending the adds first therefore
// respects every dependency in the set, while sending them together would not
// — a container sorts its tokens bytewise, so the service may run a batch's
// invocations in any order, and a retraction that overtook its own addition
// would leave the root counted for good.
//
// Within a batch the changes are independent: each addition names the manifest
// of a distinct version, and so does each retraction.
func (b *Backend) SweepUploadRegistrations(ctx context.Context) (int, error) {
	due, err := b.uploadRegs.ListDueUploadRegistrations(ctx, time.Now(), uploadRegistrationBatch)
	if err != nil {
		return 0, fmt.Errorf("s3frontend: list due upload registrations: %w", err)
	}
	if len(due) == 0 {
		return 0, nil
	}

	var adds, removes []registry.UploadRegistration
	var unknown []int64
	for _, reg := range due {
		switch reg.Op {
		case registry.UploadRegistrationAdd:
			adds = append(adds, reg)
		case registry.UploadRegistrationRemove:
			removes = append(removes, reg)
		default:
			// Not retryable: a row nothing can act on would block its key
			// forever, so it goes.
			b.logger.Error("registration sweep: unknown queued operation; dropping",
				zap.String("op", string(reg.Op)),
				zap.Int64("seq", reg.Seq),
			)
			unknown = append(unknown, reg.Seq)
		}
	}
	if err := b.uploadRegs.DeleteUploadRegistrations(ctx, unknown); err != nil {
		return 0, fmt.Errorf("s3frontend: drop unusable upload registrations: %w", err)
	}

	done, stalled, err := b.applyRegistrations(ctx, adds, b.registrar.RegisterUploads, nil)
	if err != nil {
		return done, err
	}
	// A key whose addition was refused holds its retractions back: the refused
	// add will be retried, and a retraction sent ahead of it would be a no-op
	// that leaves the root registered when the add finally lands.
	n, _, err := b.applyRegistrations(ctx, removes, b.registrar.RetractUploads, stalled)
	return done + n, err
}

// applyRegistrations sends one op's worth of queued changes and settles every
// row by the outcome. Rows whose key is in held are deferred untried. It
// returns how many landed and the keys that did not.
//
// The rows are settled in two statements — one delete for everything that
// landed, one reschedule per distinct backoff for everything that did not —
// so a batched round trip is not undone by a database call per change.
func (b *Backend) applyRegistrations(
	ctx context.Context,
	regs []registry.UploadRegistration,
	send func(context.Context, []uploader.QueuedUpload) ([]error, error),
	held map[string]bool,
) (int, map[string]bool, error) {
	stalled := map[string]bool{}
	if len(regs) == 0 {
		return 0, stalled, nil
	}

	batch := make([]uploader.QueuedUpload, 0, len(regs))
	sent := make([]registry.UploadRegistration, 0, len(regs))
	var deferred []registry.UploadRegistration
	var deadLettered []int64
	for _, reg := range regs {
		if held[registrationKey(reg)] {
			// Its predecessor failed this sweep; try again once that has
			// landed, with the same delay so the order is kept.
			deferred = append(deferred, reg)
			stalled[registrationKey(reg)] = true
			continue
		}
		proofs, renewed, err := b.registrar.PrepareAuthority(ctx, reg.Space, registrationCommand(reg.Op), reg.Proofs)
		if err != nil {
			stalled[registrationKey(reg)] = true
			// Its authority is spent and the space has none to lend. Retrying
			// costs nothing for a while, in case the space is written again;
			// past that the row is dead-lettered, because a change that can
			// never be made must not hold back the ones behind it for good.
			if reg.Attempts+1 >= uploadRegistrationDeadLetterAfter {
				b.logger.Error("registration sweep: dead-lettering a change whose authority cannot be renewed; object count will be short by one",
					zap.String("bucket", reg.Bucket),
					zap.String("key", reg.ObjectKey),
					zap.String("op", string(reg.Op)),
					zap.Stringer("space", reg.Space),
					zap.Stringer("root", reg.Root),
					zap.Error(err),
				)
				deadLettered = append(deadLettered, reg.Seq)
				continue
			}
			b.logger.Warn("registration sweep: authority spent; will retry in case the space is written again",
				zap.String("bucket", reg.Bucket),
				zap.Stringer("space", reg.Space),
				zap.Int("attempts", reg.Attempts+1),
				zap.Error(err),
			)
			deferred = append(deferred, reg)
			continue
		}
		if renewed {
			// Persist it so the renewal survives a restart and later sweeps do
			// not repeat the work. Rare enough to be worth a statement of its
			// own: only a chain that outlived its own validity gets here.
			if err := b.uploadRegs.RefreshUploadRegistrationProofs(ctx, reg.Seq, proofs); err != nil {
				return 0, stalled, fmt.Errorf("s3frontend: refresh upload registration proofs: %w", err)
			}
		}
		batch = append(batch, uploader.QueuedUpload{Space: reg.Space, Root: reg.Root, Proofs: proofs})
		sent = append(sent, reg)
	}
	if err := b.uploadRegs.DeadLetterUploadRegistrations(ctx, deadLettered, "authority expired and could not be renewed"); err != nil {
		return 0, stalled, fmt.Errorf("s3frontend: dead-letter upload registrations: %w", err)
	}

	var landed []int64
	if len(batch) > 0 {
		results, err := send(ctx, batch)
		if err != nil {
			// The round trip failed, so nothing was answered: every row in it
			// waits for the next sweep.
			b.logger.Warn("registration sweep: upload service unreachable; batch requeued",
				zap.Int("changes", len(batch)),
				zap.Error(err),
			)
			for _, reg := range sent {
				stalled[registrationKey(reg)] = true
			}
			deferred = append(deferred, sent...)
		} else {
			for i, reg := range sent {
				if results[i] != nil {
					stalled[registrationKey(reg)] = true
					deferred = append(deferred, reg)
					b.logger.Warn("registration sweep: upload service refused a queued change; will retry",
						zap.String("bucket", reg.Bucket),
						zap.String("op", string(reg.Op)),
						zap.Stringer("space", reg.Space),
						zap.Stringer("root", reg.Root),
						zap.Int("attempts", reg.Attempts+1),
						zap.Error(results[i]),
					)
					continue
				}
				landed = append(landed, reg.Seq)
			}
		}
	}

	// The rows go only once the service has taken their changes, so a crash
	// mid-sweep replays them rather than losing them. Both capabilities are
	// idempotent, so a replay cannot double count.
	if err := b.uploadRegs.DeleteUploadRegistrations(ctx, landed); err != nil {
		return 0, stalled, fmt.Errorf("s3frontend: delete upload registrations: %w", err)
	}
	if err := b.deferRegistrations(ctx, deferred); err != nil {
		return len(landed), stalled, err
	}
	return len(landed), stalled, nil
}

// deferRegistrations holds every given row back until it has served the delay
// its attempt count has earned. The backoff is a step function of attempts, so
// the rows fall into a handful of groups however many there are, and each group
// is one statement.
func (b *Backend) deferRegistrations(ctx context.Context, regs []registry.UploadRegistration) error {
	if len(regs) == 0 {
		return nil
	}
	now := time.Now()
	byDelay := map[time.Duration][]int64{}
	for _, reg := range regs {
		d := uploadRegistrationBackoff(reg.Attempts)
		byDelay[d] = append(byDelay[d], reg.Seq)
	}
	for d, seqs := range byDelay {
		if err := b.uploadRegs.RescheduleUploadRegistrations(ctx, seqs, now.Add(d)); err != nil {
			return fmt.Errorf("s3frontend: reschedule upload registrations: %w", err)
		}
	}
	return nil
}

// registrationCommand is the capability a queued change invokes.
func registrationCommand(op registry.UploadRegistrationOp) ucan.Command {
	if op == registry.UploadRegistrationRemove {
		return uploadcmds.Remove.Command
	}
	return uploadcmds.Add.Command
}

// registrationKey identifies the object a queued change belongs to. Ordering
// is only owed within one key: two keys never share a manifest root.
func registrationKey(reg registry.UploadRegistration) string {
	return reg.Bucket + "\x00" + reg.ObjectKey
}
