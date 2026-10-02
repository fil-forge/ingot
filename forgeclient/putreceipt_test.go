package forgeclient

import (
	"bytes"
	"testing"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	httpcmds "github.com/fil-forge/libforge/commands/http"
	"github.com/fil-forge/ucantone/ipld/datamodel"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/fil-forge/ucantone/ucan/promise"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// putInvocation issues a /http/put for body the way the upload service does,
// with the signing key in its metadata.
func putInvocation(t *testing.T, body blobcmds.BlobSpec) ucan.Invocation {
	t.Helper()
	signer := deriveBlobSigner(t, randomDigest(t))
	inv, err := httpcmds.Put.Invoke(
		signer,
		signer.DID(),
		&httpcmds.PutArguments{Body: body, Destination: promise.AwaitOK{Task: randomCID(t)}},
		invocation.WithAudience(signer.DID()),
		invocation.WithMetadata(datamodel.Map{
			"keys": datamodel.Map{
				"id":   signer.DID().String(),
				"keys": datamodel.Map{signer.DID().String(): signer.Bytes()},
			},
		}),
	)
	require.NoError(t, err)
	return inv
}

func putResult(t *testing.T, rcpt ucan.Receipt) httpcmds.PutOK {
	t.Helper()
	out, _ := rcpt.Out().Unpack()
	var ok httpcmds.PutOK
	require.NoError(t, ok.UnmarshalCBOR(bytes.NewReader(out)))
	return ok
}

func TestPutReceipt(t *testing.T) {
	t.Run("a put by digest reports nothing", func(t *testing.T) {
		digest := randomDigest(t)
		inv := putInvocation(t, blobcmds.SpecFromDigest(digest, 1024))
		rcpt, err := putReceipt(inv, digest)
		require.NoError(t, err)
		require.Nil(t, putResult(t, rcpt).Blob)
	})

	t.Run("a put by digest code reports the digest sent", func(t *testing.T) {
		inv := putInvocation(t, blobcmds.SpecFromDigestCode(multihash.SHA2_256, 1024))
		digest := randomDigest(t)
		rcpt, err := putReceipt(inv, digest)
		require.NoError(t, err)
		ok := putResult(t, rcpt)
		require.NotNil(t, ok.Blob)
		require.Equal(t, digest, ok.Blob.Digest)
		require.Equal(t, inv.Task().Link(), rcpt.Ran())
	})

	t.Run("a put by digest code needs a digest", func(t *testing.T) {
		inv := putInvocation(t, blobcmds.SpecFromDigestCode(multihash.SHA2_256, 1024))
		_, err := putReceipt(inv, nil)
		require.Error(t, err)
	})
}
