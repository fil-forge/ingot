package forgeclient

import (
	"crypto/rand"
	"net/http"
	"net/url"
	"sync"
	"testing"

	assertcmds "github.com/fil-forge/libforge/commands/assert"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	httpcmds "github.com/fil-forge/libforge/commands/http"
	ucancmds "github.com/fil-forge/libforge/commands/ucan"
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
	// silent answers with no acceptances, as an upload service that predates
	// batched conclusion does, forcing the client back to polling.
	silent bool
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
	silent := f.silent
	f.mu.Unlock()

	if silent {
		return res.SetSuccess(&ucancmds.ConcludeOK{})
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
	fake := &fakeSprue{node: node, acceptFor: map[cid.Cid]cid.Cid{}}

	srv := server.NewHTTP(service)
	srv.Handle(ucancmds.Conclude.Command, ucancmds.Conclude.Handler(fake.conclude))

	c, err := New(randomIssuer(t), service.DID(), *mustURL(t, "http://upload.example"),
		WithUCANClientOptions(client.WithHTTPClient(&http.Client{Transport: srv})))
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
