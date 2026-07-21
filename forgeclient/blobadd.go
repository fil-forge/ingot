// Carried/trimmed from github.com/fil-forge/guppy/pkg/client/blobadd.go.
//
// Ingot divergences from upstream:
//   - Proof chains come from a per-call ProofStore (WithProofStore) so an
//     invocation can be scoped to a request's per-access-key delegations,
//     not just the client's token store.
//   - No /blob/accept re-delegation: sprue owns accept (as it owns
//     allocate), so the conclude/put-receipt dance carries no space proof.
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
}

// NewBlobAddConfig builds a BlobAddConfig from options.
func NewBlobAddConfig(options ...BlobAddOption) *BlobAddConfig {
	cfg := &BlobAddConfig{PutClient: &http.Client{}}
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

// AddedBlob is the result of a successful BlobAdd.
type AddedBlob struct {
	Digest   multihash.Multihash
	Size     uint64
	Location ucan.Invocation // the /assert/location commitment
}

// BlobAdd adds a blob to the upload service (sprue): invoke /blob/add,
// PUT the bytes, conclude a synthesized /http/put receipt, then poll the
// /blob/accept receipt for the location commitment. The issuer needs a
// /blob/add delegation proof over space.
func (c *Client) BlobAdd(ctx context.Context, space did.DID, content io.Reader, options ...BlobAddOption) (blob AddedBlob, err error) {
	cfg := NewBlobAddConfig(options...)

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
			c.logger.Debug("blob added", zap.Stringer("space", space), zap.Duration("duration", time.Since(start)))
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

	// Conclude a synthesized /http/put receipt so /blob/accept can resolve.
	// Accept is owned by sprue (like allocate), so no /blob/accept
	// re-delegation is attached — the conclude is issued agent→sprue and
	// carries no space proof.
	if !putSuccess {
		if err := c.sendPutReceipt(ctx, putInv); err != nil {
			return AddedBlob{}, fmt.Errorf("sending put receipt: %w", err)
		}
	}

	accRcpt, accMeta, err := c.receiptsClient.Poll(ctx, accInv.Task().Link(), receipt_client.WithRetries(5))
	if err != nil {
		return AddedBlob{}, fmt.Errorf("polling accept receipt: %w", err)
	}
	o, x = accRcpt.Out().Unpack()
	if accRcpt.Out().IsErr() {
		var model edm.ErrorModel
		if err := model.UnmarshalCBOR(bytes.NewReader(x)); err != nil {
			return AddedBlob{}, fmt.Errorf("executing invocation")
		}
		return AddedBlob{}, fmt.Errorf("failure in accept receipt: %w", model)
	}
	var accOK blobcmds.AcceptOK
	if err := accOK.UnmarshalCBOR(bytes.NewReader(o)); err != nil {
		return AddedBlob{}, fmt.Errorf("unmarshaling accept receipt output: %w", err)
	}

	var locationCommitment ucan.Invocation
	for _, inv := range accMeta.Invocations() {
		if inv.Command() == assertcmds.Location.Command {
			locationCommitment = inv
		}
	}
	if locationCommitment == nil {
		return AddedBlob{}, fmt.Errorf("blob accept receipt missing location commitment invocation")
	}

	return AddedBlob{Digest: contentHash, Size: *contentSizePtr, Location: locationCommitment}, nil
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

func (c *Client) sendPutReceipt(ctx context.Context, putInv ucan.Invocation, opts ...execution.RequestOption) error {
	var putMeta datamodel.Map
	if err := putMeta.UnmarshalCBOR(bytes.NewReader(putInv.MetadataBytes())); err != nil {
		return fmt.Errorf("unmarshaling /http/put invocation metadata: %w", err)
	}
	keysMap, ok := putMeta["keys"].(ipld.Map)
	if !ok {
		return fmt.Errorf("invalid put metadata, missing 'keys' field")
	}
	id, ok := keysMap["id"].(string)
	if !ok {
		return fmt.Errorf("invalid put metadata, missing 'id' field in 'keys'")
	}
	keysKeysMap, ok := keysMap["keys"].(ipld.Map)
	if !ok {
		return fmt.Errorf("invalid put metadata, missing 'keys' field in 'keys'")
	}
	keyBytes, ok := keysKeysMap[id].([]byte)
	if !ok {
		return fmt.Errorf("invalid put metadata, missing key for %s", id)
	}
	signer, err := ed25519.Decode(keyBytes)
	if err != nil {
		return fmt.Errorf("decoding key for %q: %w", id, err)
	}
	issuer := multikey.KeyIssuer(signer)
	putRcpt, err := receipt.IssueOK(issuer, putInv.Task().Link(), &httpcmds.PutOK{}, receipt.WithIssuedAt(ucan.Now()))
	if err != nil {
		return fmt.Errorf("generating receipt: %w", err)
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
