package uploader

import (
	"context"
	"fmt"
	"time"

	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/ipfs/go-cid"

	"github.com/fil-forge/ingot/forgeclient"
)

// UploadRegistrar keeps the upload service's content-entry list for a space in
// step with the bucket catalog, one entry per committed object version. The
// entry is what sprue counts to report the space's object count; the bytes are
// accounted separately, per blob, by BodyUploader and BlobRemover.
//
// A version's root is its manifest CID, so the count follows S3: a new version
// registers, a retired one retracts, and a bucket that retains noncurrent
// versions keeps counting them. Delete markers are versions too and count the
// same way, which is what AWS reports.
//
// The calls are made by the registration sweeper, off the request path, from
// rows the write path queued. Each row carries its own authority — captured
// with CaptureAuthority while the request that committed the version was still
// in hand — because the sweeper has no request to borrow from and may be
// draining a space that will never be written again.
type UploadRegistrar interface {
	// CaptureAuthority encodes the delegation chain authorizing cmd on space
	// into the container a queued registration carries.
	CaptureAuthority(ctx context.Context, space did.DID, cmd ucan.Command) ([]byte, error)
	// PrepareAuthority returns the authority to send a queued change with,
	// renewing the stored chain when it has run out or is about to. It reports
	// whether the chain it returns is a new one, so the caller can persist it.
	//
	// Renewal matters because the stored chain is short lived: hilt expires
	// its delegation to the gateway at the next UTC midnight, so a change
	// queued late in the day can outlive its own authority within minutes of
	// the upload service being unavailable. The renewal comes from the space's
	// live authority, which a recent write to that space leaves behind, so it
	// rescues a space still in use. A space that has gone quiet has none, and
	// the error says so.
	PrepareAuthority(ctx context.Context, space did.DID, cmd ucan.Command, stored []byte) ([]byte, bool, error)
	// RegisterUploads records each change as a content entry in one round
	// trip, reporting the outcomes in the order given: a nil entry means the
	// upload service took the change. The returned error is a transport
	// failure, where nothing was answered.
	RegisterUploads(ctx context.Context, changes []QueuedUpload) ([]error, error)
	// RetractUploads drops each change's content entry, with the same
	// reporting as RegisterUploads.
	RetractUploads(ctx context.Context, changes []QueuedUpload) ([]error, error)
}

// QueuedUpload is one queued content-entry change: the root to register or
// retract in a space, and the encoded authority the queued row carries.
type QueuedUpload struct {
	Space  did.DID
	Root   cid.Cid
	Proofs []byte
}

// CaptureAuthority resolves the proof chain for cmd on space and encodes it.
// The store is request-scoped when present (the write committing the version)
// and otherwise the authority captured from a recent write to the space.
func (u *Forge) CaptureAuthority(ctx context.Context, space did.DID, cmd ucan.Command) ([]byte, error) {
	store, ok := u.shipProofStore(ctx, space)
	if !ok {
		return nil, fmt.Errorf("uploader: no proof store for space %s (no request scope and no captured write authority)", space)
	}
	proofs, _, err := store.ProofChain(ctx, u.client.Issuer().DID(), cmd, space)
	if err != nil {
		return nil, fmt.Errorf("uploader: building proof chain: %w", err)
	}
	// Gzipped raw: the chain is stored once per object version and outlives
	// the request that produced it.
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

// proofStore rebuilds a proof store from a queued row's container.
func proofStoreFrom(proofs []byte) (ucanlib.ProofStore, error) {
	ct, err := container.Decode(proofs)
	if err != nil {
		return nil, fmt.Errorf("uploader: decoding queued proof chain: %w", err)
	}
	return ucanlib.NewContainerProofStore(ct), nil
}

// RegisterUploads records the changes as content entries via /upload/add on
// the upload service, in one round trip per MaxUploadBatch. Idempotent: sprue
// upserts by root and counts only the first add, so a retried row cannot
// double count.
func (u *Forge) RegisterUploads(ctx context.Context, changes []QueuedUpload) ([]error, error) {
	return u.uploadChanges(ctx, changes, u.client.UploadAddBatch)
}

// RetractUploads drops the changes' content entries via /upload/remove. It
// does not touch the blobs those roots covered — the reference index releases
// those per digest, since a blob can be claimed by more than one version.
// Idempotent.
func (u *Forge) RetractUploads(ctx context.Context, changes []QueuedUpload) ([]error, error) {
	return u.uploadChanges(ctx, changes, u.client.UploadRemoveBatch)
}

func (u *Forge) uploadChanges(
	ctx context.Context,
	changes []QueuedUpload,
	send func(context.Context, []forgeclient.UploadChange) ([]error, error),
) ([]error, error) {
	if len(changes) == 0 {
		return nil, nil
	}
	out := make([]error, len(changes))
	batch := make([]forgeclient.UploadChange, 0, len(changes))
	// idx maps a batch position back to the caller's, since a change whose
	// stored authority will not decode never reaches the wire.
	idx := make([]int, 0, len(changes))
	for i, ch := range changes {
		store, err := proofStoreFrom(ch.Proofs)
		if err != nil {
			out[i] = err
			continue
		}
		batch = append(batch, forgeclient.UploadChange{Space: ch.Space, Root: ch.Root, Proofs: store})
		idx = append(idx, i)
	}
	if len(batch) == 0 {
		return out, nil
	}
	results, err := send(ctx, batch)
	if err != nil {
		return out, err
	}
	for j, res := range results {
		out[idx[j]] = res
	}
	return out, nil
}

var _ UploadRegistrar = (*Forge)(nil)
