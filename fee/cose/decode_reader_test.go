package cose

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

// TestDecodeReader exercises the streaming decoder: it decodes both envelope
// forms off a reader, streams back the detached ciphertext that follows the
// header, and — critically — produces the same Enc_structure (AAD) and the same
// trailing bytes as the byte-based Decode, so a caller can decrypt an envelope
// read either way.
func TestDecodeReader(t *testing.T) {
	ciphertext := []byte("detached-stream-ciphertext-bytes-0123456789")

	t.Run("with recipients (COSE_Encrypt, tag 96)", func(t *testing.T) {
		enc := &Envelope{
			Headers: Headers{
				Protected: Header{}.
					Set(HeaderLabelType, exampleType).
					Set(HeaderLabelAlg, int64(-65793)),
				Unprotected: Header{}.Set(HeaderLabelIV, []byte("0123456789ab")),
			},
			Recipients: []*Recipient{{
				Headers: Headers{
					Protected:   Header{}.Set(HeaderLabelAlg, AlgA256KW),
					Unprotected: Header{}.Set(HeaderLabelKID, []byte("kid-1")),
				},
				Ciphertext: []byte("wrapped-cek-0123456789abcdef"),
			}},
		}
		header, err := enc.Encode()
		require.NoError(t, err)
		blob := append(append([]byte{}, header...), ciphertext...)

		env, rest, err := DecodeReader(bytes.NewReader(blob))
		require.NoError(t, err)
		require.Len(t, env.Recipients, 1, "recipient presence marks the tag-96 form")
		gotRest, err := io.ReadAll(rest)
		require.NoError(t, err)
		require.Equal(t, ciphertext, gotRest)

		// The streaming decode agrees with the byte-based Decode on both the
		// trailing ciphertext and the AAD (context "Encrypt").
		byteEnv, byteRest, err := Decode(blob)
		require.NoError(t, err)
		require.Equal(t, ciphertext, byteRest)
		want, err := byteEnv.EncStructure(nil)
		require.NoError(t, err)
		got, err := env.EncStructure(nil)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("recipient-less (COSE_Encrypt0, tag 16)", func(t *testing.T) {
		enc0 := &Envelope{
			Headers: Headers{
				Protected: Header{}.
					Set(HeaderLabelType, exampleType).
					Set(HeaderLabelAlg, int64(-65793)),
				Unprotected: Header{}.Set(HeaderLabelIV, []byte("0123456789ab")),
			},
		}
		header, err := enc0.Encode()
		require.NoError(t, err)
		blob := append(append([]byte{}, header...), ciphertext...)

		env, rest, err := DecodeReader(bytes.NewReader(blob))
		require.NoError(t, err)
		require.Empty(t, env.Recipients, "recipient absence marks the tag-16 form")
		gotRest, err := io.ReadAll(rest)
		require.NoError(t, err)
		require.Equal(t, ciphertext, gotRest)

		// Agrees with the byte-based Decode (context "Encrypt0").
		byteEnv, byteRest, err := Decode(blob)
		require.NoError(t, err)
		require.Equal(t, ciphertext, byteRest)
		want, err := byteEnv.EncStructure(nil)
		require.NoError(t, err)
		got, err := env.EncStructure(nil)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("ciphertext split across the decoder buffer and the reader", func(t *testing.T) {
		enc0 := &Envelope{
			Headers: Headers{Protected: Header{}.Set(HeaderLabelType, exampleType)},
		}
		header, err := enc0.Encode()
		require.NoError(t, err)
		blob := append(append([]byte{}, header...), ciphertext...)

		// One byte at a time forces the header to be reassembled from many reads
		// and the ciphertext to straddle the decoder's read-ahead and the source.
		env, rest, err := DecodeReader(iotest.OneByteReader(bytes.NewReader(blob)))
		require.NoError(t, err)
		require.Empty(t, env.Recipients)
		gotRest, err := io.ReadAll(rest)
		require.NoError(t, err)
		require.Equal(t, ciphertext, gotRest)
	})

	t.Run("expected type mismatch", func(t *testing.T) {
		enc0 := &Envelope{
			Headers: Headers{Protected: Header{}.Set(HeaderLabelType, "application/other")},
		}
		header, err := enc0.Encode()
		require.NoError(t, err)

		env, rest, err := DecodeReader(bytes.NewReader(header), WithExpectedType(exampleType))
		require.Nil(t, env)
		require.Nil(t, rest)
		require.ErrorIs(t, err, ErrUnexpectedType)
	})

	t.Run("not a COSE tag", func(t *testing.T) {
		env, rest, err := DecodeReader(bytes.NewReader([]byte{0x01}))
		require.Nil(t, env)
		require.Nil(t, rest)
		require.ErrorIs(t, err, ErrNotEncrypt)
	})

	t.Run("empty input", func(t *testing.T) {
		env, rest, err := DecodeReader(bytes.NewReader(nil))
		require.Nil(t, env)
		require.Nil(t, rest)
		require.ErrorIs(t, err, ErrMalformed)
	})
}
