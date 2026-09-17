package forgeclient

import (
	"crypto/rand"
	"github.com/fil-forge/libforge/digestutil"
	ucanerrors "github.com/fil-forge/ucantone/errors"
	"net/http"
	"net/url"
	"sync"
	"testing"

	assertcmds "github.com/fil-forge/libforge/commands/assert"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	httpcmds "github.com/fil-forge/libforge/commands/http"
	ucancmds "github.com/fil-forge/libforge/commands/ucan"
	receipt_client "github.com/fil-forge/libforge/receipt"
	"github.com/fil-forge/ucantone/binding"
	"github.com/fil-forge/ucantone/client"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ipld/datamodel"
	"github.com/fil-forge/ucantone/multikey"
	ed25519signer "github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/server"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/fil-forge/ucantone/ucan/promise"
	"github.com/fil-forge/ucantone/ucan/receipt"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// Small fixtures, matching how the other tests in this package generate
// identities rather than pulling in a shared testutil.
func randomIssuer(t *testing.T) ucan.Issuer {
	t.Helper()
	iss, err := ed25519signer.GenerateIssuer()
	require.NoError(t, err)
	return iss
}

func randomDID(t *testing.T) did.DID {
	t.Helper()
	return randomIssuer(t).DID()
}

func randomDigest(t *testing.T) multihash.Multihash {
	t.Helper()
	b := make([]byte, 32)
	_, err := rand.Read(b)
	require.NoError(t, err)
	digest, err := multihash.Sum(b, multihash.SHA2_256, -1)
	require.NoError(t, err)
	return digest
}

func randomCID(t *testing.T) cid.Cid {
	t.Helper()
	return cid.NewCidV1(cid.Raw, randomDigest(t))
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}

// fakeSprue answers /ucan/conclude the way the upload service does: it runs
// the accept each delivered put receipt implies, and returns those receipts
// with their location commitments in the response. It records the size of
// every conclude it receives, which is what the chunking is judged on.
type fakeSprue struct {
	node ucan.Issuer
	// acceptFor maps a put task to the accept task the client awaits for it.
	acceptFor map[cid.Cid]cid.Cid

	mu         sync.Mutex
	deliveries []int
	// failDelivery fails the nth conclude (1-based) outright, as an upload
	// service that cannot serve the request does; zero fails none.
	failDelivery int
	// reject holds the accept tasks the node refuses: each answers with a
	// failure receipt while the rest of the batch is accepted.
	reject map[cid.Cid]bool
	// omit holds the accept tasks whose acceptance is left out of the
	// response, as an upload service does when the full answer would not
	// fit in one container; the client is expected to poll for those.
	omit map[cid.Cid]bool
	// polls counts the receipt fetches the client made; the fixture's
	// receipt endpoint answers every one with 503.
	polls int
}

// RoundTrip is the fixture's receipt endpoint: an upload service that is not
// answering receipt fetches, so a poll fails on its first attempt.
func (f *fakeSprue) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.polls++
	f.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Status:     "503 Service Unavailable",
		Body:       http.NoBody,
		Request:    r,
	}, nil
}

func (f *fakeSprue) pollCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.polls
}

func (f *fakeSprue) sizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.deliveries...)
}

func (f *fakeSprue) conclude(req *binding.Request[*ucancmds.ConcludeArguments], res *binding.Response[*ucancmds.ConcludeOK]) error {
	delivered := req.Task().Arguments().Receipts
	f.mu.Lock()
	f.deliveries = append(f.deliveries, len(delivered))
	fail := len(f.deliveries) == f.failDelivery
	f.mu.Unlock()

	if fail {
		return res.SetFailure(ucanerrors.New("Unavailable", "upload service unavailable"))
	}

	// Conclude arguments name receipts by their own link, not by the task
	// they ran, so the container is indexed the same way the upload service
	// indexes it.
	byLink := map[cid.Cid]ucan.Receipt{}
	for _, rcpt := range req.Metadata().Receipts() {
		byLink[rcpt.Link()] = rcpt
	}

	var invs []ucan.Invocation
	var rcpts []ucan.Receipt
	for _, link := range delivered {
		putRcpt, ok := byLink[link]
		if !ok {
			// Every delivered receipt must travel in the request container.
			return res.SetFailure(ucancmds.ErrConclusionReceiptNotFound)
		}
		acceptTask, ok := f.acceptFor[putRcpt.Ran()]
		if !ok {
			return res.SetFailure(ucancmds.ErrConclusionReceiptNotFound)
		}
		if f.omit[acceptTask] {
			continue
		}
		if f.reject[acceptTask] {
			accRcpt, err := receipt.IssueErr(f.node, acceptTask, datamodel.Map{
				"name":    "BlobNotFound",
				"message": "the bytes never arrived",
			})
			if err != nil {
				return err
			}
			rcpts = append(rcpts, accRcpt)
			continue
		}

		claim, err := invocation.Invoke(f.node, f.node.DID(), assertcmds.Location.Command,
			datamodel.Map{"accept": acceptTask.String()})
		if err != nil {
			return err
		}
		accRcpt, err := receipt.IssueOK(f.node, acceptTask, &blobcmds.AcceptOK{
			Site: claim.Link(),
			PDP:  promise.AwaitOK{Task: claim.Task().Link()},
		})
		if err != nil {
			return err
		}
		invs = append(invs, claim)
		rcpts = append(rcpts, accRcpt)
	}
	if err := res.SetMetadata(container.New(
		container.WithInvocations(invs...),
		container.WithReceipts(rcpts...),
	)); err != nil {
		return err
	}
	return res.SetSuccess(&ucancmds.ConcludeOK{})
}

// concludeFixture stands up a client pointed at a fake upload service.
func concludeFixture(t *testing.T) (*Client, *fakeSprue) {
	t.Helper()
	service := randomIssuer(t)
	node := randomIssuer(t)
	fake := &fakeSprue{node: node, acceptFor: map[cid.Cid]cid.Cid{}, reject: map[cid.Cid]bool{}, omit: map[cid.Cid]bool{}}

	srv := server.NewHTTP(service)
	srv.Handle(ucancmds.Conclude.Command, ucancmds.Conclude.Handler(fake.conclude))

	serviceURL := mustURL(t, "http://upload.example")
	c, err := New(randomIssuer(t), service.DID(), *serviceURL,
		WithUCANClientOptions(client.WithHTTPClient(&http.Client{Transport: srv})),
		WithReceiptsClient(receipt_client.NewClient(serviceURL.JoinPath("/receipt/"),
			receipt_client.WithHTTPClient(&http.Client{Transport: fake}))))
	require.NoError(t, err)
	return c, fake
}

// parkBlob builds what a WithConclude(false) upload leaves behind: a put
// invocation carrying the digest-derived signing key in its metadata, exactly
// as the upload service issues it, since that key is what the client uses to
// sign the put receipt it delivers.
func parkBlob(t *testing.T, fake *fakeSprue) AddedBlob {
	t.Helper()
	digest := randomDigest(t)
	blobProvider := deriveBlobSigner(t, digest)

	allocTask := randomCID(t)
	putInv, err := httpcmds.Put.Invoke(
		blobProvider,
		blobProvider.DID(),
		&httpcmds.PutArguments{
			Body:        blobcmds.Blob{Digest: digest, Size: 1024},
			Destination: promise.AwaitOK{Task: allocTask},
		},
		invocation.WithAudience(blobProvider.DID()),
		invocation.WithMetadata(datamodel.Map{
			"keys": datamodel.Map{
				"id": blobProvider.DID().String(),
				"keys": datamodel.Map{
					blobProvider.DID().String(): blobProvider.Bytes(),
				},
			},
		}),
	)
	require.NoError(t, err)

	acceptTask := randomCID(t)
	fake.acceptFor[putInv.Task().Link()] = acceptTask

	return AddedBlob{
		Digest:        digest,
		Size:          1024,
		AddTask:       randomCID(t),
		AcceptTask:    acceptTask,
		PutInvocation: putInv.Bytes(),
	}
}

func deriveBlobSigner(t *testing.T, digest multihash.Multihash) multikey.Issuer {
	t.Helper()
	require.GreaterOrEqual(t, len(digest), 32)
	signer, err := ed25519signer.FromRaw(digest[len(digest)-32:])
	require.NoError(t, err)
	return multikey.KeyIssuer(signer)
}

// TestBlobConcludeBatchDeliversOnce pins the shape of the request: every
// parked blob's receipt goes in one conclude, and each blob comes back with
// the location its own acceptance named.
func TestBlobConcludeBatchDeliversOnce(t *testing.T) {
	c, fake := concludeFixture(t)

	parked := []AddedBlob{parkBlob(t, fake), parkBlob(t, fake), parkBlob(t, fake)}
	out, err := c.BlobConcludeBatch(t.Context(), randomDID(t), parked)
	require.NoError(t, err)

	require.Equal(t, []int{len(parked)}, fake.sizes(), "one conclude carrying every receipt")
	require.Len(t, out, len(parked))
	for i, blob := range out {
		require.Equal(t, parked[i].Digest, blob.Digest, "results must stay in the order given")
		require.NotNil(t, blob.Location, "blob %d got no location", i)
		require.Nil(t, blob.PutInvocation, "the spent put invocation must be dropped")
	}
}

// TestBlobConcludeBatchSkipsLocated pins that a blob already accepted at add
// time (a dedup hit) is returned as-is and never re-delivered.
func TestBlobConcludeBatchSkipsLocated(t *testing.T) {
	c, fake := concludeFixture(t)

	located := parkBlob(t, fake)
	claim, err := invocation.Invoke(fake.node, fake.node.DID(), assertcmds.Location.Command,
		datamodel.Map{"already": "accepted"})
	require.NoError(t, err)
	located.Location = claim

	parked := []AddedBlob{parkBlob(t, fake), located, parkBlob(t, fake)}
	out, err := c.BlobConcludeBatch(t.Context(), randomDID(t), parked)
	require.NoError(t, err)

	require.Equal(t, []int{2}, fake.sizes(), "only the unconcluded blobs are delivered")
	require.Same(t, claim, out[1].Location, "an already-located blob keeps its own location")
	require.NotNil(t, out[0].Location)
	require.NotNil(t, out[2].Location)
}

// TestBlobConcludeBatchChunks pins the chunk boundary: more receipts than fit
// in one conclude are split, and every blob is still answered.
func TestBlobConcludeBatchChunks(t *testing.T) {
	c, fake := concludeFixture(t)

	parked := make([]AddedBlob, MaxConcludeBatch+1)
	for i := range parked {
		parked[i] = parkBlob(t, fake)
	}

	out, err := c.BlobConcludeBatch(t.Context(), randomDID(t), parked)
	require.NoError(t, err)

	require.Equal(t, []int{MaxConcludeBatch, 1}, fake.sizes(),
		"one past the cap must split into a full conclude and a remainder")
	require.Len(t, out, len(parked))
	for i, blob := range out {
		require.NotNil(t, blob.Location, "blob %d of %d got no location", i, len(parked))
	}
}

// TestBlobConcludeBatchKeepsCompletedChunks pins what a failure partway
// leaves behind: the blobs of every chunk concluded before it come back
// located, so the caller can record acceptances the upload service has
// already run, and the rest come back as they went in, ready to retry.
func TestBlobConcludeBatchKeepsCompletedChunks(t *testing.T) {
	c, fake := concludeFixture(t)
	fake.failDelivery = 2

	parked := make([]AddedBlob, MaxConcludeBatch+2)
	for i := range parked {
		parked[i] = parkBlob(t, fake)
	}

	out, err := c.BlobConcludeBatch(t.Context(), randomDID(t), parked)
	require.Error(t, err)
	require.Equal(t, []int{MaxConcludeBatch, 2}, fake.sizes())
	require.Len(t, out, len(parked), "every blob comes back, located or not")
	for i := 0; i < MaxConcludeBatch; i++ {
		require.NotNil(t, out[i].Location, "blob %d was concluded before the failure and must come back located", i)
	}
	for i := MaxConcludeBatch; i < len(parked); i++ {
		require.Nil(t, out[i].Location, "blob %d was never concluded", i)
		require.Equal(t, parked[i].PutInvocation, out[i].PutInvocation, "an unconcluded blob keeps what a retry needs")
	}
}

// TestBlobConcludeBatchKeepsChunkAroundRefusedAccept pins that one blob's
// refused acceptance does not cost its chunk: its neighbours come back
// located, and the error names the blob that failed.
func TestBlobConcludeBatchKeepsChunkAroundRefusedAccept(t *testing.T) {
	c, fake := concludeFixture(t)

	parked := []AddedBlob{parkBlob(t, fake), parkBlob(t, fake), parkBlob(t, fake)}
	fake.reject[parked[1].AcceptTask] = true

	out, err := c.BlobConcludeBatch(t.Context(), randomDID(t), parked)
	require.ErrorContains(t, err, digestutil.Format(parked[1].Digest))
	require.Equal(t, []int{3}, fake.sizes(), "one conclude carried the whole chunk")
	require.NotNil(t, out[0].Location)
	require.Nil(t, out[1].Location, "the refused blob is not located")
	require.NotNil(t, out[2].Location, "a blob after the refused one is still resolved")
}

// TestBlobConcludeBatchDrainsChunkPastFailedPoll pins what a chunk yields
// when the response leaves a blob unanswered and the poll for it fails: the
// acceptances the response did carry are still read out, so the caller can
// record them, and no further blob is polled for. Unanswered blobs come back
// as they went in, ready for the retry.
func TestBlobConcludeBatchDrainsChunkPastFailedPoll(t *testing.T) {
	c, fake := concludeFixture(t)

	parked := []AddedBlob{parkBlob(t, fake), parkBlob(t, fake), parkBlob(t, fake)}
	fake.omit[parked[0].AcceptTask] = true
	fake.omit[parked[2].AcceptTask] = true

	out, err := c.BlobConcludeBatch(t.Context(), randomDID(t), parked)
	require.ErrorContains(t, err, digestutil.Format(parked[0].Digest))
	require.Equal(t, []int{3}, fake.sizes(), "one conclude carried the whole chunk")
	require.Equal(t, 1, fake.pollCount(), "a failed poll ends the polling; the next unanswered blob is not polled for")

	require.Nil(t, out[0].Location, "the unanswered blob is not located")
	require.Equal(t, parked[0].PutInvocation, out[0].PutInvocation, "an unanswered blob keeps what a retry needs")
	require.NotNil(t, out[1].Location, "an acceptance the response carried is read out past the failed poll")
	require.Nil(t, out[1].PutInvocation, "the spent put invocation must be dropped")
	require.Nil(t, out[2].Location, "a later unanswered blob is left for the retry")
	require.Equal(t, parked[2].PutInvocation, out[2].PutInvocation)
}
