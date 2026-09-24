package forgeclient

import (
	"context"
	"fmt"

	uploadcmds "github.com/fil-forge/libforge/commands/upload"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution/batch"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/ipfs/go-cid"
)

// MaxUploadBatch caps the content-entry changes sent in one round trip. A UCAN
// container holds at most 8192 tokens; each change costs one invocation plus
// its proof chain in the request (chains dedupe by link, so rows from one
// access key share theirs) and one receipt in the response. 512 leaves room
// for the longest chains without approaching the ceiling.
const MaxUploadBatch = 512

// UploadChange is one content-entry change to send: a root to register or
// retract in a space, with the authority to do it.
type UploadChange struct {
	Space  did.DID
	Root   cid.Cid
	Proofs ucanlib.ProofStore
}

// UploadAddBatch invokes /upload/add for every change in as few round trips as
// MaxUploadBatch allows, and reports each one's outcome in the order given: a
// nil entry means the upload service took the change.
//
// Every change in a batch must be independent of the others. Container
// encoding sorts tokens bytewise, so the service sees no particular order
// within a batch and may run them in any — which is why adds and retractions
// of the same root can never share one (see SweepUploadRegistrations).
func (c *Client) UploadAddBatch(ctx context.Context, changes []UploadChange) ([]error, error) {
	return c.uploadBatch(ctx, changes, uploadcmds.Add.Command, func(ch UploadChange, links []cid.Cid) (ucan.Invocation, error) {
		return uploadcmds.Add.Invoke(
			c.signer, ch.Space,
			// Shards stay empty: ingot's bodies are raw blobs rather than the
			// CAR archives the capability describes, and /blob/add already
			// registered each of them.
			&uploadcmds.AddArguments{Root: ch.Root},
			invocation.WithAudience(c.serviceID),
			invocation.WithProofs(links...),
		)
	})
}

// UploadRemoveBatch invokes /upload/remove for every change, with the same
// batching and independence rules as UploadAddBatch.
func (c *Client) UploadRemoveBatch(ctx context.Context, changes []UploadChange) ([]error, error) {
	return c.uploadBatch(ctx, changes, uploadcmds.Remove.Command, func(ch UploadChange, links []cid.Cid) (ucan.Invocation, error) {
		return uploadcmds.Remove.Invoke(
			c.signer, ch.Space,
			&uploadcmds.RemoveArguments{Root: ch.Root},
			invocation.WithAudience(c.serviceID),
			invocation.WithProofs(links...),
		)
	})
}

func (c *Client) uploadBatch(
	ctx context.Context,
	changes []UploadChange,
	cmd ucan.Command,
	build func(UploadChange, []cid.Cid) (ucan.Invocation, error),
) ([]error, error) {
	out := make([]error, len(changes))
	for start := 0; start < len(changes); start += MaxUploadBatch {
		end := min(start+MaxUploadBatch, len(changes))
		if err := c.uploadBatchChunk(ctx, changes[start:end], out[start:end], cmd, build); err != nil {
			return out, err
		}
	}
	return out, nil
}

// uploadBatchChunk sends one container's worth and fills its slice of results.
// A transport failure is returned as the call's error: nothing was answered, so
// nothing can be reported per change.
func (c *Client) uploadBatchChunk(
	ctx context.Context,
	changes []UploadChange,
	results []error,
	cmd ucan.Command,
	build func(UploadChange, []cid.Cid) (ucan.Invocation, error),
) error {
	invs := make([]ucan.Invocation, 0, len(changes))
	// tasks[i] is the invocation built for changes[i], or undefined when the
	// change could not be prepared (its result is already set).
	tasks := make([]cid.Cid, len(changes))
	var dlgs []ucan.Delegation

	for i, ch := range changes {
		proofs, links, err := ch.Proofs.ProofChain(ctx, c.signer.DID(), cmd, ch.Space)
		if err != nil {
			results[i] = fmt.Errorf("building proof chain: %w", err)
			continue
		}
		inv, err := build(ch, links)
		if err != nil {
			results[i] = fmt.Errorf("creating invocation: %w", err)
			continue
		}
		invs = append(invs, inv)
		tasks[i] = inv.Task().Link()
		dlgs = append(dlgs, proofs...)
	}
	if len(invs) == 0 {
		return nil
	}

	// The container deduplicates delegations by link, so the chains shared by
	// changes from one access key are carried once.
	res, err := c.ucanClient.ExecuteBatch(batch.NewRequest(ctx, invs, batch.WithDelegations(dlgs...)))
	if err != nil {
		return fmt.Errorf("executing batch: %w", err)
	}

	for i := range changes {
		if results[i] != nil || !tasks[i].Defined() {
			continue
		}
		rcpt, ok := res.Receipt(tasks[i])
		if !ok {
			results[i] = fmt.Errorf("no receipt for %s", tasks[i])
			continue
		}
		// Both capabilities answer with Unit, so only the error branch matters.
		if cmd == uploadcmds.Remove.Command {
			_, results[i] = uploadcmds.Remove.Unpack(rcpt)
		} else {
			_, results[i] = uploadcmds.Add.Unpack(rcpt)
		}
	}
	return nil
}
