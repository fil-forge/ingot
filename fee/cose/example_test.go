package cose_test

import (
	"bytes"
	"fmt"

	"github.com/fil-forge/ingot/fee/cose"
)

// Example shows the envelope lifecycle: build a detached COSE_Encrypt, derive
// the AAD for the body AEAD, append the (already-encrypted) ciphertext, then
// decode the blob back and recover the recipient's wrapped key. The actual
// encryption and key wrap are the caller's responsibility — cose only handles
// the envelope and the Enc_structure.
func Example() {
	const exampleType = "application/example"

	env := &cose.Encrypt{
		Headers: cose.Headers{
			Protected: cose.Header{}.
				Set(cose.HeaderLabelAlg, cose.AlgA256GCM).
				Set(cose.HeaderLabelType, exampleType),
			Unprotected: cose.Header{}.
				Set(cose.HeaderLabelIV, bytes.Repeat([]byte{0x00}, 12)),
		},
		Recipients: []*cose.Recipient{{
			Headers: cose.Headers{
				Protected:   cose.Header{}.Set(cose.HeaderLabelAlg, cose.AlgECDHESA256KW),
				Unprotected: cose.Header{}.Set(cose.HeaderLabelKID, []byte("key-1")),
			},
			Ciphertext: []byte("wrapped-key-bytes"),
		}},
	}

	// AAD that the body AEAD must authenticate.
	aad, err := env.EncStructure(nil)
	if err != nil {
		panic(err)
	}
	_ = aad // pass to your AEAD as additional data

	// Serialize the envelope and append the detached ciphertext.
	envelope, err := env.Encode()
	if err != nil {
		panic(err)
	}
	blob := append(envelope, []byte("...detached ciphertext...")...)

	// Decode, pinning the expected type, and recover the trailing payload.
	decoded, ciphertext, err := cose.DecodeEncrypt(blob, cose.WithExpectedType(exampleType))
	if err != nil {
		panic(err)
	}
	kid, _ := decoded.Recipients[0].Headers.Unprotected.Bytes(cose.HeaderLabelKID)

	fmt.Printf("recipients: %d\n", len(decoded.Recipients))
	fmt.Printf("kid: %s\n", kid)
	fmt.Printf("wrapped key: %s\n", decoded.Recipients[0].Ciphertext)
	fmt.Printf("detached ciphertext: %s\n", ciphertext)
	// Output:
	// recipients: 1
	// kid: key-1
	// wrapped key: wrapped-key-bytes
	// detached ciphertext: ...detached ciphertext...
}
