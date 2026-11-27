package uploader

import (
	"context"
	"fmt"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
)

// AuthorityCapturer captures the authority a task queued by a request runs
// with later, such as a blob's release. The task has no request to borrow
// from and may act on a space that will never be written again, so it
// carries its own, captured while the request was still in hand.
type AuthorityCapturer interface {
	// CaptureAuthority encodes the delegation chain authorizing cmd on space
	// into the container a queued task carries.
	CaptureAuthority(ctx context.Context, space did.DID, cmd ucan.Command) ([]byte, error)
	// PrepareAuthority returns the authority to run a queued task with,
	// renewing the stored chain when it has run out or is about to. It reports
	// whether the chain it returns is a new one, so the caller can persist it.
	//
	// The stored chain has room to spare: hilt expires its delegation to the
	// gateway at the next UTC midnight plus an async overhang, so even a
	// task queued at 23:59 has hours to run in. Renewal is for the
	// queue that outlasts that, which is the upload service being away for
	// longer than a night. It comes from the space's live authority, which a
	// body upload to that space leaves behind for an hour if its store is
	// borrowable, so it rescues a space still in use. A space that has gone
	// quiet has none, and the error says so.
	PrepareAuthority(ctx context.Context, space did.DID, cmd ucan.Command, stored []byte) ([]byte, bool, error)
}

// CaptureAuthority resolves the proof chain for cmd on space and encodes it.
// The store is request-scoped when present (the write committing the version)
// and otherwise the space's ship authority: the borrowable store its latest
// body upload left, within the hour.
func (u *Forge) CaptureAuthority(ctx context.Context, space did.DID, cmd ucan.Command) ([]byte, error) {
	// An empty chain is not an error to ProofChain, but it is one here
	// (authorityFor refuses it): the invocation would go out proofless and be
	// refused, and a queued task carrying it can never run for anybody.
	// Failing now puts the problem where it can be read — the request holds no
	// chain for this command on this space — instead of queueing a task that
	// fails forever and takes a bucket delete with it.
	_, proofs, err := u.authorityFor(ctx, space, cmd)
	if err != nil {
		return nil, err
	}
	// Gzipped raw: the chain is stored with each task that carries it and
	// outlives the request that produced it.
	encoded, err := container.Encode(container.RawGzip, container.New(container.WithDelegations(proofs...)))
	if err != nil {
		return nil, fmt.Errorf("uploader: encoding proof chain: %w", err)
	}
	return encoded, nil
}

// authorityRenewMargin is how much life a stored chain must have left to be
// worth sending. A chain expiring inside the margin is renewed first, so a
// batch is not spent on authority that lapses between the check and the
// upload service reading it.
const authorityRenewMargin = 5 * time.Minute

// PrepareAuthority renews a stored chain that has run out, or is about to.
func (u *Forge) PrepareAuthority(ctx context.Context, space did.DID, cmd ucan.Command, stored []byte) ([]byte, bool, error) {
	if authorityUsable(stored, time.Now().Add(authorityRenewMargin)) {
		return stored, false, nil
	}
	fresh, err := u.CaptureAuthority(ctx, space, cmd)
	if err != nil {
		return nil, false, fmt.Errorf("uploader: stored authority for %s is spent and cannot be renewed: %w", space, err)
	}
	return fresh, true, nil
}

// authorityUsable reports whether every delegation in the encoded chain is
// still valid at the given instant. A chain that will not decode is not
// usable; one whose delegations carry no expiry never stops being.
func authorityUsable(proofs []byte, at time.Time) bool {
	ct, err := container.Decode(proofs)
	if err != nil {
		return false
	}
	dlgs := ct.Delegations()
	if len(dlgs) == 0 {
		// Nothing to check and nothing to send with. Treat it as spent so the
		// caller renews rather than issuing a proofless invocation.
		return false
	}
	for _, d := range dlgs {
		exp := d.Expiration()
		if exp == nil {
			continue
		}
		if !time.Unix(int64(*exp), 0).After(at) {
			return false
		}
	}
	return true
}

var _ AuthorityCapturer = (*Forge)(nil)
