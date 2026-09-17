package forgeclient

import (
	"testing"

	assertcmds "github.com/fil-forge/libforge/commands/assert"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/ucantone/ipld/datamodel"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/fil-forge/ucantone/ucan/promise"
	"github.com/fil-forge/ucantone/ucan/receipt"
	"github.com/stretchr/testify/require"
)

// acceptance issues what a storage node returns for one accepted blob: the
// location commitment, and an accept receipt naming it. The receipt's Site is
// the commitment invocation's own link, not its task link — matching on the
// wrong one is why an early version of the batched conclude could not find
// any commitment.
func acceptance(t *testing.T, node ucan.Issuer, acceptTask ucan.Invocation) (ucan.Invocation, ucan.Receipt) {
	t.Helper()
	claim, err := invocation.Invoke(node, node.DID(), assertcmds.Location.Command, datamodel.Map{
		"blob": acceptTask.Task().Link().String(),
	})
	require.NoError(t, err)
	rcpt, err := receipt.IssueOK(node, acceptTask.Task().Link(), &blobcmds.AcceptOK{
		Site: claim.Link(),
		PDP:  promise.AwaitOK{Task: claim.Task().Link()},
	})
	require.NoError(t, err)
	return claim, rcpt
}

// acceptInvocation issues a stand-in /blob/accept invocation for a blob.
func acceptInvocation(t *testing.T, service ucan.Issuer, node ucan.Issuer, n int) ucan.Invocation {
	t.Helper()
	inv, err := invocation.Invoke(service, node.DID(), blobcmds.Accept.Command, datamodel.Map{
		"n": int64(n),
	})
	require.NoError(t, err)
	return inv
}

// TestLocationFromBatchedAccepts pins how a blob's location commitment is
// found in a conclude response that carries many blobs' acceptances: by the
// link the blob's own accept receipt names, never by position or by command.
func TestLocationFromBatchedAccepts(t *testing.T) {
	service, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	node, err := ed25519.GenerateIssuer()
	require.NoError(t, err)

	const blobs = 5
	var (
		accepts []ucan.Invocation
		claims  []ucan.Invocation
		rcpts   []ucan.Receipt
	)
	for i := range blobs {
		acc := acceptInvocation(t, service, node, i)
		claim, rcpt := acceptance(t, node, acc)
		accepts = append(accepts, acc)
		claims = append(claims, claim)
		rcpts = append(rcpts, rcpt)
	}
	// One container holding every blob's acceptance, as sprue answers a
	// batched conclude.
	resp := container.New(
		container.WithInvocations(append(append([]ucan.Invocation{}, accepts...), claims...)...),
		container.WithReceipts(rcpts...),
	)
	idx := indexAccepts(resp)

	// Every blob resolves to its OWN commitment.
	for i := range blobs {
		got, err := locationFromAccept(rcpts[i], idx)
		require.NoError(t, err, "blob %d", i)
		require.Equal(t, claims[i].Link(), got.Link(), "blob %d got another blob's commitment", i)
	}
}

// A receipt whose commitment is absent from the container is an error, not a
// silently wrong commitment picked from a neighbouring blob.
func TestLocationFromAcceptMissingCommitment(t *testing.T) {
	service, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	node, err := ed25519.GenerateIssuer()
	require.NoError(t, err)

	acc := acceptInvocation(t, service, node, 0)
	_, rcpt := acceptance(t, node, acc)
	// Another blob's commitment is present; this blob's is not.
	otherAcc := acceptInvocation(t, service, node, 1)
	otherClaim, _ := acceptance(t, node, otherAcc)

	idx := indexAccepts(container.New(container.WithInvocations(otherClaim)))
	_, err = locationFromAccept(rcpt, idx)
	require.ErrorContains(t, err, "missing location commitment")
}

// A failed acceptance surfaces as an error carrying the node's reason.
func TestLocationFromFailedAccept(t *testing.T) {
	node, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	service, err := ed25519.GenerateIssuer()
	require.NoError(t, err)

	acc := acceptInvocation(t, service, node, 0)
	rcpt, err := receipt.IssueErr(node, acc.Task().Link(), datamodel.Map{
		"name":    "BlobNotFound",
		"message": "blob not delivered",
	})
	require.NoError(t, err)

	// The node's reason reaches the caller: an accept that failed must not
	// look like a missing commitment.
	_, err = locationFromAccept(rcpt, indexAccepts(nil))
	require.ErrorContains(t, err, "failure in accept receipt")
	require.ErrorContains(t, err, "blob not delivered")
}

// A receipt whose Site names an invocation of some other command is an
// error: the location recorded for a blob is only ever an /assert/location
// commitment, never whatever the link happened to resolve to.
func TestLocationFromAcceptRejectsForeignCommitment(t *testing.T) {
	service, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	node, err := ed25519.GenerateIssuer()
	require.NoError(t, err)

	acc := acceptInvocation(t, service, node, 0)
	// The receipt points at another /blob/accept invocation instead of a
	// location commitment.
	foreign := acceptInvocation(t, service, node, 1)
	rcpt, err := receipt.IssueOK(node, acc.Task().Link(), &blobcmds.AcceptOK{
		Site: foreign.Link(),
		PDP:  promise.AwaitOK{Task: foreign.Task().Link()},
	})
	require.NoError(t, err)

	idx := indexAccepts(container.New(container.WithInvocations(foreign)))
	_, err = locationFromAccept(rcpt, idx)
	require.ErrorContains(t, err, blobcmds.Accept.Command.String())
	require.ErrorContains(t, err, assertcmds.Location.Command.String())
}
