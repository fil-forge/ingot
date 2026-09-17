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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	assertcmds "github.com/fil-forge/libforge/commands/assert"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	httpcmds "github.com/fil-forge/libforge/commands/http"
	ucancmds "github.com/fil-forge/libforge/commands/ucan"
	"github.com/fil-forge/libforge/digestutil"
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
func (c *Client) BlobConclude(ctx context.Context, space did.DID, added AddedBlob) (AddedBlob, error) {
	out, err := c.BlobConcludeBatch(ctx, space, []AddedBlob{added})
	if err != nil {
		return AddedBlob{}, err
	}
	return out[0], nil
}

// MaxConcludeBatch caps the receipts delivered in one /ucan/conclude. A UCAN
// container holds at most 8192 tokens, and each blob costs 2 in the request
// (its put invocation and receipt) and 4 in the response (the accept
// invocation and receipt, plus the location commitment and PDP promise the
// node attaches). The response is the tighter of the two, which puts the
// ceiling near 2000; 1000 keeps a comfortable margin under it, so a batch is
// always answered in full rather than leaving blobs to be polled for.
const MaxConcludeBatch = 1000

// BlobConcludeBatch finishes many parked uploads in as few round trips as
// MaxConcludeBatch allows, and returns them in the order given with their
// location commitments filled in.
//
// It delivers every blob's /http/put receipt in one /ucan/conclude, which
// fires their /blob/accept invocations upload-service side, and reads each
// acceptance out of the response rather than polling for it — a completion
// with thousands of parts pays a couple of round trips instead of thousands.
// An AddedBlob whose Location is already set is returned as-is, and the
// results drop PutInvocation (spent — the caller should delete its persisted
// copy too).
//
// On error the returned slice still holds every blob, and each one whose
// acceptance was read before the failure carries its Location. The upload
// service has run those accepts whether or not the rest of the batch
// succeeded, so the caller must record them before retrying: a park left
// standing for an accepted blob is concluded again on the next attempt, or
// aborted on the node when its session expires. A blob left unresolved comes
// back as it went in, PutInvocation included, ready for that retry.
func (c *Client) BlobConcludeBatch(ctx context.Context, space did.DID, added []AddedBlob) (blobs []AddedBlob, err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			located := 0
			for _, b := range blobs {
				if b.Location != nil {
					located++
				}
			}
			c.logger.Error("blob conclude batch failed", zap.Stringer("space", space), zap.Int("blobs", len(added)), zap.Int("located", located), zap.Error(err), zap.Duration("duration", time.Since(start)))
		} else {
			c.logger.Debug("blobs concluded", zap.Stringer("space", space), zap.Int("blobs", len(added)), zap.Duration("duration", time.Since(start)))
		}
	}()

	out := make([]AddedBlob, len(added))
	// Blobs already accepted (a dedup hit at add time) need no conclusion.
	var pending []int
	for i, a := range added {
		out[i] = a
		if a.Location != nil {
			out[i].PutInvocation = nil
			continue
		}
		pending = append(pending, i)
	}
	if len(pending) == 0 {
		return out, nil
	}

	for start := 0; start < len(pending); start += MaxConcludeBatch {
		chunk := pending[start:min(start+MaxConcludeBatch, len(pending))]
		accepts, err := c.concludePuts(ctx, added, chunk)
		if err != nil {
			return out, err
		}
		// Index the response once: matching every blob to its acceptance by
		// scanning the container would be quadratic in the chunk size.
		index := indexAccepts(accepts)
		// The chunk's accepts have all run, so every blob in it is resolved
		// before a failure among them is reported: one blob's refused
		// acceptance says nothing about its neighbours, and the caller records
		// them. A blob the response did not answer for is polled for. A failed
		// poll means the service is not answering, which polling once more per
		// remaining blob would only confirm slowly, so it ends the polling;
		// the acceptances the response did carry are still read out, and the
		// blobs left unanswered come back as they went in.
		var failed []error
		polling := true
		for _, i := range chunk {
			var location ucan.Invocation
			if rcpt, ok := index.rcptsByRan[added[i].AcceptTask]; ok {
				location, err = locationFromAccept(rcpt, index)
				if err != nil {
					failed = append(failed, fmt.Errorf("blob %s: %w", digestutil.Format(added[i].Digest), err))
					continue
				}
			} else if !polling {
				continue
			} else {
				location, err = c.awaitAccept(ctx, added[i].AcceptTask)
				if err != nil {
					failed = append(failed, fmt.Errorf("blob %s: %w", digestutil.Format(added[i].Digest), err))
					polling = false
					continue
				}
			}
			out[i] = AddedBlob{
				Digest:     added[i].Digest,
				Size:       added[i].Size,
				Location:   location,
				AddTask:    added[i].AddTask,
				AcceptTask: added[i].AcceptTask,
			}
		}
		if len(failed) > 0 {
			return out, errors.Join(failed...)
		}
	}
	return out, nil
}

// concludePuts delivers the put receipts of the blobs at the given indices in
// one invocation, and returns the response container the upload service
// answered with — the acceptances it ran as a consequence.
func (c *Client) concludePuts(ctx context.Context, added []AddedBlob, idx []int) (ucan.Container, error) {
	putInvs := make([]ucan.Invocation, 0, len(idx))
	putRcpts := make([]ucan.Receipt, 0, len(idx))
	links := make([]cid.Cid, 0, len(idx))
	for _, i := range idx {
		putInv := new(invocation.Invocation)
		if err := putInv.UnmarshalCBOR(bytes.NewReader(added[i].PutInvocation)); err != nil {
			return nil, fmt.Errorf("decoding parked /http/put invocation: %w", err)
		}
		putRcpt, err := putReceipt(putInv)
		if err != nil {
			return nil, fmt.Errorf("generating put receipt: %w", err)
		}
		putInvs = append(putInvs, putInv)
		putRcpts = append(putRcpts, putRcpt)
		links = append(links, putRcpt.Link())
	}

	inv, err := ucancmds.Conclude.Invoke(
		c.signer, c.signer.DID(),
		&ucancmds.ConcludeArguments{Receipts: links},
		invocation.WithAudience(c.serviceID),
	)
	if err != nil {
		return nil, fmt.Errorf("generating invocation: %w", err)
	}

	// The put invocations ride along so the upload service need not read each
	// one back out of its own store.
	_, rcpt, meta, err := Execute[*ucancmds.ConcludeOK](ctx, c.ucanClient, inv,
		execution.WithReceipts(putRcpts...),
		execution.WithInvocations(putInvs...),
	)
	if err != nil {
		return nil, fmt.Errorf("executing invocation: %w", err)
	}
	if rcpt.Out().IsErr() {
		_, x := rcpt.Out().Unpack()
		var model edm.ErrorModel
		if err := model.UnmarshalCBOR(bytes.NewReader(x)); err != nil {
			return nil, fmt.Errorf("conclude failed with unknown error: %w", err)
		}
		return nil, fmt.Errorf("conclude failed: %w", model)
	}
	return meta, nil
}

// acceptIndex is a conclude response's acceptances, keyed for lookup: the
// receipts by the task they ran, and the invocations by their own link (which
// is what an acceptance names its location commitment by).
type acceptIndex struct {
	rcptsByRan map[cid.Cid]ucan.Receipt
	invsByLink map[cid.Cid]ucan.Invocation
}

func indexAccepts(c ucan.Container) acceptIndex {
	idx := acceptIndex{
		rcptsByRan: map[cid.Cid]ucan.Receipt{},
		invsByLink: map[cid.Cid]ucan.Invocation{},
	}
	if c == nil {
		return idx
	}
	for _, r := range c.Receipts() {
		idx.rcptsByRan[r.Ran()] = r
	}
	for _, inv := range c.Invocations() {
		idx.invsByLink[inv.Link()] = inv
	}
	return idx
}

// awaitAccept polls the /blob/accept receipt and extracts the
// /assert/location commitment from its metadata. It is the fallback for a
// blob whose acceptance the conclude response did not carry, as when the
// upload service found the response too large to answer in one container.
func (c *Client) awaitAccept(ctx context.Context, acceptTask cid.Cid) (ucan.Invocation, error) {
	accRcpt, accMeta, err := c.receiptsClient.Poll(ctx, acceptTask, receipt_client.WithRetries(5))
	if err != nil {
		return nil, fmt.Errorf("polling accept receipt: %w", err)
	}
	return locationFromAccept(accRcpt, indexAccepts(accMeta))
}

// locationFromAccept unpacks an accept receipt and returns the
// /assert/location commitment it issued, found in the accompanying container
// by the link the receipt names (AcceptOK.Site is the commitment invocation's
// own link, not its task link). A container may hold the acceptances of many
// blobs, so the commitment is matched by link rather than by command; the
// command is then checked, so the location recorded for a blob is never some
// other invocation the receipt happened to point at.
func locationFromAccept(accRcpt ucan.Receipt, meta acceptIndex) (ucan.Invocation, error) {
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
	inv, ok := meta.invsByLink[accOK.Site]
	if !ok {
		return nil, fmt.Errorf("blob accept receipt missing location commitment invocation")
	}
	if inv.Command() != assertcmds.Location.Command {
		return nil, fmt.Errorf("blob accept receipt names a %s invocation as its location commitment, want %s", inv.Command(), assertcmds.Location.Command)
	}
	return inv, nil
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

// putReceipt issues the /http/put receipt for a parked upload, signing it with
// the digest-derived key the upload service embedded in the put invocation's
// metadata.
func putReceipt(putInv ucan.Invocation) (ucan.Receipt, error) {
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
	return receipt.IssueOK(multikey.KeyIssuer(signer), putInv.Task().Link(), &httpcmds.PutOK{}, receipt.WithIssuedAt(ucan.Now()))
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
