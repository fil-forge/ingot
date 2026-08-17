// Carried/trimmed from github.com/fil-forge/guppy/pkg/client/blobadd.go.
//
// Ingot divergences from upstream:
//   - Proof chains come from a per-call ProofStore (WithProofStore) so an
//     invocation can be scoped to a request's per-access-key delegations,
//     not just the client's token store.
//   - No /blob/accept re-delegation: sprue owns accept (as it owns
//     allocate), so the conclude/put-receipt dance carries no space proof.
//   - BlobAdd accepts WithConclude(false) so multipart can defer the
//     conclude (park at UploadPart, accept at Complete); BlobConclude
//     finishes the parked add later.
//
// Also dropped from upstream: otel spans, go-log, ctxutil, the progress/stall
// readers, and the hard requirement that the accept receipt carry a PDP
// accept invocation.
package forgeclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"time"

	assertcmds "github.com/fil-forge/libforge/commands/assert"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	httpcmds "github.com/fil-forge/libforge/commands/http"
	ucancmds "github.com/fil-forge/libforge/commands/ucan"
	receipt_client "github.com/fil-forge/libforge/receipt"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	edm "github.com/fil-forge/ucantone/errors/datamodel"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ipld"
	"github.com/fil-forge/ucantone/ipld/datamodel"
	"github.com/fil-forge/ucantone/multikey"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/fil-forge/ucantone/ucan/receipt"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// BlobAddOption configures [Client.BlobAdd].
type BlobAddOption func(*BlobAddConfig)

// BlobAddConfig holds configuration for [Client.BlobAdd].
type BlobAddConfig struct {
	PutClient          *http.Client
	PrecomputedDigest  multihash.Multihash
	PrecomputedSizePtr *uint64
	// ProofStore, when set, supplies the delegation proof chain for this
	// call instead of the client's default token store — used to scope an
	// invocation to a request's per-access-key proofs.
	ProofStore ucanlib.ProofStore
	// Conclude controls whether BlobAdd concludes the /http/put receipt
	// (triggering /blob/accept) before returning. Default true.
	Conclude bool
}

// NewBlobAddConfig builds a BlobAddConfig from options.
func NewBlobAddConfig(options ...BlobAddOption) *BlobAddConfig {
	cfg := &BlobAddConfig{PutClient: &http.Client{}, Conclude: true}
	for _, opt := range options {
		opt(cfg)
	}
	return cfg
}

// WithPutClient sets the HTTP client used for the blob PUT.
func WithPutClient(client *http.Client) BlobAddOption {
	return func(cfg *BlobAddConfig) { cfg.PutClient = client }
}

// WithPrecomputedDigest supplies a precomputed digest/size so BlobAdd
// skips re-hashing and streams the content straight into the PUT.
func WithPrecomputedDigest(d multihash.Multihash, size uint64) BlobAddOption {
	return func(cfg *BlobAddConfig) {
		cfg.PrecomputedDigest = d
		cfg.PrecomputedSizePtr = &size
	}
}

// WithProofStore overrides the proof store used to build this call's
// delegation chains (default: the client's token store).
func WithProofStore(ps ucanlib.ProofStore) BlobAddOption {
	return func(cfg *BlobAddConfig) { cfg.ProofStore = ps }
}

// WithConclude controls whether BlobAdd concludes the upload before
// returning. WithConclude(false) leaves the blob parked — durable on the
// provider, but piri holds the bytes without aggregating them until
// /blob/accept fires: the returned AddedBlob has a nil Location and carries
// the state [Client.BlobConclude] needs to finish the upload later (or
// [Client.BlobAbort] to abandon it; AddTask is the abort Cause). Moot on
// dedup — when the provider already held accepted bytes for the content,
// accept already ran and the add completes regardless.
func WithConclude(conclude bool) BlobAddOption {
	return func(cfg *BlobAddConfig) { cfg.Conclude = conclude }
}

// AddedBlob is the result of a BlobAdd. Location is set once the blob is
// accepted; with WithConclude(false) it is nil until the deferred
// [Client.BlobConclude] — persist the task links + PutInvocation in between.
type AddedBlob struct {
	Digest multihash.Multihash
	Size   uint64
	// Location is the /assert/location commitment issued at accept; nil
	// while the add is unconcluded (parked).
	Location ucan.Invocation
	// AddTask is the /blob/add task link — the receipt-chain root the
	// upload service uses to locate the provider for abort.
	AddTask cid.Cid
	// AcceptTask is the /blob/accept task link BlobConclude polls.
	AcceptTask cid.Cid
	// PutInvocation is the issued /http/put invocation, populated only while
	// the add is unconcluded. Its metadata embeds the derived signer keys
	// needed to synthesize the put receipt at conclude time — treat it as
	// sensitive and delete it once concluded or rejected.
	PutInvocation []byte
}

// BlobAdd adds a blob to the upload service (sprue): invoke /blob/add,
// PUT the bytes, conclude a synthesized /http/put receipt, then poll the
// /blob/accept receipt for the location commitment. The issuer needs a
// /blob/add delegation proof over space. With WithConclude(false) it stops
// after the PUT — the blob stays parked until [Client.BlobConclude].
func (c *Client) BlobAdd(ctx context.Context, space did.DID, content io.Reader, options ...BlobAddOption) (AddedBlob, error) {
	cfg := NewBlobAddConfig(options...)
	added, err := c.blobAdd(ctx, space, content, cfg)
	if err != nil {
		return AddedBlob{}, err
	}
	// Already accepted (dedup) or deliberately unconcluded — done either way.
	if added.Location != nil || !cfg.Conclude {
		return added, nil
	}
	return c.BlobConclude(ctx, space, added)
}

// blobAdd runs the durable half of BlobAdd: /blob/add + PUT the bytes,
// WITHOUT concluding the /http/put receipt — the conclude is what makes the
// upload service trigger /blob/accept on the provider, so the blob stays
// parked (stored, unaggregated) until BlobConclude. The result's Location is
// nil unless the provider already held accepted bytes for this content
// (dedup: allocate returned no upload address and the put receipt was
// pre-issued, so accept already ran).
func (c *Client) blobAdd(ctx context.Context, space did.DID, content io.Reader, cfg *BlobAddConfig) (blob AddedBlob, err error) {
	putClient := cfg.PutClient
	contentReader := content
	contentHash := cfg.PrecomputedDigest
	contentSizePtr := cfg.PrecomputedSizePtr
	needsHash := contentSizePtr == nil || contentHash == nil || len(contentHash) == 0

	start := time.Now()
	defer func() {
		if err != nil {
			c.logger.Error("blob add failed", zap.Stringer("space", space), zap.Error(err), zap.Duration("duration", time.Since(start)))
		} else {
			c.logger.Debug("blob added", zap.Stringer("space", space), zap.Bool("parked", blob.Location == nil), zap.Duration("duration", time.Since(start)))
		}
	}()

	if needsHash {
		contentBytes, rerr := io.ReadAll(content)
		if rerr != nil {
			return AddedBlob{}, fmt.Errorf("reading content: %w", rerr)
		}
		contentHash, err = multihash.Sum(contentBytes, multihash.SHA2_256, -1)
		if err != nil {
			return AddedBlob{}, fmt.Errorf("computing content multihash: %w", err)
		}
		contentReader = bytes.NewReader(contentBytes)
		contentSize := uint64(len(contentBytes))
		contentSizePtr = &contentSize
	}

	proofStore := ucanlib.ProofStore(c.tokenStore)
	if cfg.ProofStore != nil {
		proofStore = cfg.ProofStore
	}

	proofs, proofLinks, err := proofStore.ProofChain(ctx, c.signer.DID(), blobcmds.Add.Command, space)
	if err != nil {
		return AddedBlob{}, fmt.Errorf("building proof chain: %w", err)
	}

	inv, err := blobcmds.Add.Invoke(
		c.signer, space,
		&blobcmds.AddArguments{Blob: blobcmds.Blob{Digest: contentHash, Size: *contentSizePtr}},
		invocation.WithAudience(c.serviceID),
		invocation.WithProofs(proofLinks...),
	)
	if err != nil {
		return AddedBlob{}, fmt.Errorf("generating invocation: %w", err)
	}

	addOK, _, meta, err := Execute[*blobcmds.AddOK](
		ctx, c.ucanClient, inv,
		execution.WithDelegations(proofs...),
	)
	if err != nil {
		return AddedBlob{}, fmt.Errorf("executing blob add: %w", err)
	}

	accInv, err := findInvocation(addOK.Site.Task, meta.Invocations())
	if err != nil {
		return AddedBlob{}, fmt.Errorf("finding /blob/accept invocation: %w", err)
	}
	var accArgs blobcmds.AcceptArguments
	if err := accArgs.UnmarshalCBOR(bytes.NewReader(accInv.ArgumentsBytes())); err != nil {
		return AddedBlob{}, fmt.Errorf("unmarshaling /blob/accept arguments: %w", err)
	}

	putInv, err := findInvocation(accArgs.Put.Task, meta.Invocations())
	if err != nil {
		return AddedBlob{}, fmt.Errorf("finding /http/put invocation: %w", err)
	}
	var putArgs httpcmds.PutArguments
	if err := putArgs.UnmarshalCBOR(bytes.NewReader(putInv.ArgumentsBytes())); err != nil {
		return AddedBlob{}, fmt.Errorf("unmarshaling /http/put arguments: %w", err)
	}
	putRcpt := maybeFindReceipt(accArgs.Put.Task, meta.Receipts())

	allocInv, err := findInvocation(putArgs.Destination.Task, meta.Invocations())
	if err != nil {
		return AddedBlob{}, fmt.Errorf("finding /blob/allocate invocation: %w", err)
	}
	var allocArgs blobcmds.AllocateArguments
	if err := allocArgs.UnmarshalCBOR(bytes.NewReader(allocInv.ArgumentsBytes())); err != nil {
		return AddedBlob{}, fmt.Errorf("unmarshaling /blob/allocate arguments: %w", err)
	}
	allocRcpt, err := findReceipt(putArgs.Destination.Task, meta.Receipts())
	if err != nil {
		return AddedBlob{}, fmt.Errorf("finding /blob/allocate receipt: %w", err)
	}
	o, x := allocRcpt.Out().Unpack()
	if allocRcpt.Out().IsErr() {
		var model edm.ErrorModel
		if err := model.UnmarshalCBOR(bytes.NewReader(x)); err != nil {
			return AddedBlob{}, fmt.Errorf("executing invocation")
		}
		return AddedBlob{}, fmt.Errorf("failure in allocation receipt: %w", model)
	}
	var allocOK blobcmds.AllocateOK
	if err := allocOK.UnmarshalCBOR(bytes.NewReader(o)); err != nil {
		return AddedBlob{}, fmt.Errorf("unmarshaling allocation receipt output: %w", err)
	}

	putSuccess := putRcpt != nil && putRcpt.Out().IsOK()

	// PUT the bytes when allocate handed us an address and we don't yet
	// have a put receipt. Content-Length is set explicitly so net/http
	// doesn't fall back to chunked transfer encoding for a file body —
	// piri's PUT endpoint requires it.
	if allocOK.Address != nil && !putSuccess {
		if err := putBlob(ctx, putClient, allocOK.Address.URL.URL(), allocOK.Address.Headers, contentReader, int64(*contentSizePtr)); err != nil {
			return AddedBlob{}, fmt.Errorf("putting blob: %w", err)
		}
	}

	// Dedup path: the provider already held accepted bytes for this content,
	// so the put receipt was pre-issued and the upload service ran accept
	// synchronously — the blob is not parked. Await the accept receipt and
	// return the completed AddedBlob.
	if putSuccess {
		location, aerr := c.awaitAccept(ctx, accInv.Task().Link())
		if aerr != nil {
			return AddedBlob{}, aerr
		}
		return AddedBlob{
			Digest:     contentHash,
			Size:       *contentSizePtr,
			Location:   location,
			AddTask:    inv.Task().Link(),
			AcceptTask: accInv.Task().Link(),
		}, nil
	}

	// Parked: durable on the provider, conclude deferred to BlobConclude.
	return AddedBlob{
		Digest:        contentHash,
		Size:          *contentSizePtr,
		AddTask:       inv.Task().Link(),
		AcceptTask:    accInv.Task().Link(),
		PutInvocation: putInv.Bytes(),
	}, nil
}

// BlobConclude finishes a parked (unconcluded) BlobAdd: it synthesizes and
// concludes the /http/put receipt (which makes the upload service trigger
// /blob/accept on the provider) and awaits the accept receipt's location
// commitment. Accept is owned by sprue (like allocate), so the conclude
// carries no space proof. Safe to retry — re-concluding an already-concluded
// put is tolerated upstream, and an AddedBlob whose Location is already set
// returns as-is. The result drops PutInvocation (spent — the caller should
// delete its persisted copy too).
func (c *Client) BlobConclude(ctx context.Context, space did.DID, added AddedBlob) (blob AddedBlob, err error) {
	if added.Location != nil {
		return added, nil
	}
	start := time.Now()
	defer func() {
		if err != nil {
			c.logger.Error("blob conclude failed", zap.Stringer("space", space), zap.Error(err), zap.Duration("duration", time.Since(start)))
		} else {
			c.logger.Debug("blob concluded", zap.Stringer("space", space), zap.Duration("duration", time.Since(start)))
		}
	}()

	putInv := new(invocation.Invocation)
	if err := putInv.UnmarshalCBOR(bytes.NewReader(added.PutInvocation)); err != nil {
		return AddedBlob{}, fmt.Errorf("decoding parked /http/put invocation: %w", err)
	}

	if err := c.sendPutReceipt(ctx, putInv); err != nil {
		return AddedBlob{}, fmt.Errorf("sending put receipt: %w", err)
	}

	location, err := c.awaitAccept(ctx, added.AcceptTask)
	if err != nil {
		return AddedBlob{}, err
	}
	return AddedBlob{
		Digest:     added.Digest,
		Size:       added.Size,
		Location:   location,
		AddTask:    added.AddTask,
		AcceptTask: added.AcceptTask,
	}, nil
}

// ConcludeResult is one blob's outcome from BlobConcludeBatch: the completed
// AddedBlob (Location set, PutInvocation dropped) or the error that kept it
// parked. Failed entries stay parked and are safe to resubmit.
type ConcludeResult struct {
	Blob AddedBlob
	Err  error
}

// BlobConcludeBatch finishes many parked BlobAdds in ONE round trip to the
// upload service: every parked blob's synthesized /http/put receipt and its
// /ucan/conclude invocation travel in a single UCAN container, which the
// service's ucantone server executes invocation-by-invocation, returning all
// the conclude receipts in one response container. Because the service runs
// each /blob/accept synchronously before answering, the follow-up accept
// receipt fetches (for the location commitments) succeed on their first
// attempt; they run bounded-parallel here. Outcomes are positional: one
// failed conclude does not fail the batch, and a transport-level error fails
// the whole call with no per-blob results (everything stays parked; conclude
// is idempotent, so resubmitting any subset is safe).
//
// Callers should chunk very large upload sessions (a put receipt + conclude
// invocation is a few KB, and the service concludes a container's invocations
// sequentially while the request hangs open) — see s3frontend's Complete for
// the chunking policy.
func (c *Client) BlobConcludeBatch(ctx context.Context, space did.DID, added []AddedBlob) ([]ConcludeResult, error) {
	results := make([]ConcludeResult, len(added))
	var invs []ucan.Invocation
	var rcpts []ucan.Receipt
	idxByTask := make(map[cid.Cid]int)
	for i, ab := range added {
		if ab.Location != nil {
			results[i] = ConcludeResult{Blob: ab}
			continue
		}
		putInv := new(invocation.Invocation)
		if err := putInv.UnmarshalCBOR(bytes.NewReader(ab.PutInvocation)); err != nil {
			results[i] = ConcludeResult{Err: fmt.Errorf("decoding parked /http/put invocation: %w", err)}
			continue
		}
		putRcpt, err := synthesizePutReceipt(putInv)
		if err != nil {
			results[i] = ConcludeResult{Err: fmt.Errorf("synthesizing put receipt: %w", err)}
			continue
		}
		inv, err := ucancmds.Conclude.Invoke(
			c.signer, c.signer.DID(),
			&ucancmds.ConcludeArguments{Receipt: putRcpt.Link()},
			invocation.WithAudience(c.serviceID),
		)
		if err != nil {
			results[i] = ConcludeResult{Err: fmt.Errorf("generating conclude invocation: %w", err)}
			continue
		}
		idxByTask[inv.Task().Link()] = i
		invs = append(invs, inv)
		rcpts = append(rcpts, putRcpt)
	}
	if len(invs) == 0 {
		return results, nil
	}

	start := time.Now()
	// The stock client sends one primary invocation; the rest ride the same
	// request container via WithInvocations and are executed all the same.
	// The response's metadata is the full response container, so every
	// conclude's receipt is recovered from it by task link.
	resp, err := c.ucanClient.Execute(execution.NewRequest(ctx, invs[0],
		execution.WithInvocations(invs[1:]...),
		execution.WithReceipts(rcpts...),
	))
	if err != nil {
		c.logger.Error("blob conclude batch failed",
			zap.Stringer("space", space), zap.Int("blobs", len(invs)),
			zap.Error(err), zap.Duration("duration", time.Since(start)))
		return nil, fmt.Errorf("executing conclude batch: %w", err)
	}
	container := resp.Metadata()

	// Match each conclude's receipt out of the response container, then fetch
	// the accept receipts (location commitments) for the successes in bounded
	// parallel.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concludeFetchConcurrency)
	for _, inv := range invs {
		i := idxByTask[inv.Task().Link()]
		rcpt := maybeFindReceipt(inv.Task().Link(), container.Receipts())
		if rcpt == nil {
			results[i] = ConcludeResult{Err: fmt.Errorf("conclude receipt missing for blob %s", added[i].Digest)}
			continue
		}
		if rcpt.Out().IsErr() {
			_, x := rcpt.Out().Unpack()
			var model edm.ErrorModel
			if err := model.UnmarshalCBOR(bytes.NewReader(x)); err != nil {
				results[i] = ConcludeResult{Err: fmt.Errorf("conclude failed with undecodable error")}
				continue
			}
			results[i] = ConcludeResult{Err: fmt.Errorf("conclude failed: %w", model)}
			continue
		}
		g.Go(func() error {
			location, aerr := c.awaitAccept(gctx, added[i].AcceptTask)
			if aerr != nil {
				results[i] = ConcludeResult{Err: aerr}
				return nil
			}
			results[i] = ConcludeResult{Blob: AddedBlob{
				Digest:     added[i].Digest,
				Size:       added[i].Size,
				Location:   location,
				AddTask:    added[i].AddTask,
				AcceptTask: added[i].AcceptTask,
			}}
			return nil
		})
	}
	_ = g.Wait()

	failed := 0
	for _, r := range results {
		if r.Err != nil {
			failed++
		}
	}
	c.logger.Debug("blob conclude batch",
		zap.Stringer("space", space), zap.Int("blobs", len(invs)),
		zap.Int("failed", failed), zap.Duration("duration", time.Since(start)))
	return results, nil
}

// concludeFetchConcurrency bounds BlobConcludeBatch's parallel accept-receipt
// fetches. The receipts already exist by the time the batch returns, so these
// are quick single GETs — the bound just keeps a large chunk polite.
const concludeFetchConcurrency = 16

// awaitAccept polls the /blob/accept receipt and extracts the
// /assert/location commitment from its metadata.
func (c *Client) awaitAccept(ctx context.Context, acceptTask cid.Cid) (ucan.Invocation, error) {
	accRcpt, accMeta, err := c.receiptsClient.Poll(ctx, acceptTask, receipt_client.WithRetries(5))
	if err != nil {
		return nil, fmt.Errorf("polling accept receipt: %w", err)
	}
	o, x := accRcpt.Out().Unpack()
	if accRcpt.Out().IsErr() {
		var model edm.ErrorModel
		if err := model.UnmarshalCBOR(bytes.NewReader(x)); err != nil {
			return nil, fmt.Errorf("executing invocation")
		}
		return nil, fmt.Errorf("failure in accept receipt: %w", model)
	}
	var accOK blobcmds.AcceptOK
	if err := accOK.UnmarshalCBOR(bytes.NewReader(o)); err != nil {
		return nil, fmt.Errorf("unmarshaling accept receipt output: %w", err)
	}

	var locationCommitment ucan.Invocation
	for _, inv := range accMeta.Invocations() {
		if inv.Command() == assertcmds.Location.Command {
			locationCommitment = inv
		}
	}
	if locationCommitment == nil {
		return nil, fmt.Errorf("blob accept receipt missing location commitment invocation")
	}
	return locationCommitment, nil
}

func putBlob(ctx context.Context, client *http.Client, url *url.URL, headers map[string]string, body io.Reader, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url.String(), body)
	if err != nil {
		return fmt.Errorf("creating upload request: %w", err)
	}
	if size >= 0 {
		req.ContentLength = size
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("uploading blob: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("uploading blob: %s", resp.Status)
	}
	return nil
}

// synthesizePutReceipt issues the /http/put success receipt for a parked put
// invocation, signing with the derived key embedded in the invocation's
// metadata (the same key piri expects the receipt from).
func synthesizePutReceipt(putInv ucan.Invocation) (ucan.Receipt, error) {
	var putMeta datamodel.Map
	if err := putMeta.UnmarshalCBOR(bytes.NewReader(putInv.MetadataBytes())); err != nil {
		return nil, fmt.Errorf("unmarshaling /http/put invocation metadata: %w", err)
	}
	keysMap, ok := putMeta["keys"].(ipld.Map)
	if !ok {
		return nil, fmt.Errorf("invalid put metadata, missing 'keys' field")
	}
	id, ok := keysMap["id"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid put metadata, missing 'id' field in 'keys'")
	}
	keysKeysMap, ok := keysMap["keys"].(ipld.Map)
	if !ok {
		return nil, fmt.Errorf("invalid put metadata, missing 'keys' field in 'keys'")
	}
	keyBytes, ok := keysKeysMap[id].([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid put metadata, missing key for %s", id)
	}
	signer, err := ed25519.Decode(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("decoding key for %q: %w", id, err)
	}
	issuer := multikey.KeyIssuer(signer)
	putRcpt, err := receipt.IssueOK(issuer, putInv.Task().Link(), &httpcmds.PutOK{}, receipt.WithIssuedAt(ucan.Now()))
	if err != nil {
		return nil, fmt.Errorf("generating receipt: %w", err)
	}
	return putRcpt, nil
}

func (c *Client) sendPutReceipt(ctx context.Context, putInv ucan.Invocation, opts ...execution.RequestOption) error {
	putRcpt, err := synthesizePutReceipt(putInv)
	if err != nil {
		return err
	}

	inv, err := ucancmds.Conclude.Invoke(
		c.signer, c.signer.DID(),
		&ucancmds.ConcludeArguments{Receipt: putRcpt.Link()},
		invocation.WithAudience(c.serviceID),
	)
	if err != nil {
		return fmt.Errorf("generating invocation: %w", err)
	}

	opts = slices.Clone(opts)
	opts = append(opts, execution.WithReceipts(putRcpt))
	_, rcpt, _, err := Execute[*ucancmds.ConcludeOK](ctx, c.ucanClient, inv, opts...)
	if err != nil {
		return fmt.Errorf("executing invocation: %w", err)
	}
	if rcpt.Out().IsErr() {
		_, x := rcpt.Out().Unpack()
		var model edm.ErrorModel
		if err := model.UnmarshalCBOR(bytes.NewReader(x)); err != nil {
			return fmt.Errorf("conclude failed with unknown error: %w", err)
		}
		return fmt.Errorf("conclude failed: %w", model)
	}
	return nil
}

func findInvocation(task cid.Cid, invocations []ucan.Invocation) (ucan.Invocation, error) {
	for _, inv := range invocations {
		if inv.Task().Link() == task {
			return inv, nil
		}
	}
	return nil, fmt.Errorf("missing invocation for task: %s", task)
}

func findReceipt(task cid.Cid, receipts []ucan.Receipt) (ucan.Receipt, error) {
	for _, rcpt := range receipts {
		if rcpt.Ran() == task {
			return rcpt, nil
		}
	}
	return nil, fmt.Errorf("missing receipt for task: %s", task)
}

func maybeFindReceipt(task cid.Cid, receipts []ucan.Receipt) ucan.Receipt {
	for _, rcpt := range receipts {
		if rcpt.Ran() == task {
			return rcpt
		}
	}
	return nil
}
