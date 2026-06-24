package cose

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeMalformed(t *testing.T) {
	cases := []struct {
		name string
		hex  string
		want error
	}{
		{"empty input", "", ErrMalformed},
		{"truncated array", "d86084", ErrMalformed},
		{"truncated mid-item", "d8608443a101", ErrMalformed},
		{"not a tag (bare array)", "8440a0f6818340a041aa", ErrNotEncrypt},
		{"bare integer, not a tag", "01", ErrNotEncrypt},
		{"wrong tag (16, Encrypt0)", "d08440a0f6818340a041aa", ErrNotEncrypt},
		{"array too short (3 elems)", "d8608340a0f6", ErrMalformed},
		{"array too long (5 elems)", "d8608540a0f6818340a041aa40", ErrMalformed},
		{"protected not a byte string", "d86084a0a0f6818340a041aa", ErrMalformed},
		{"protected content not a map", "d860844103a0f6818340a041aa", ErrMalformed},
		{"unprotected not a map", "d860844040f6818340a041aa", ErrMalformed},
		{"body ciphertext not null", "d8608440a041ff818340a041aa", ErrDetachedPayload},
		{"recipients not an array", "d8608440a0f640", ErrMalformed},
		{"recipients empty", "d8608440a0f680", ErrNoRecipients},
		{"recipient not an array", "d8608440a0f68140", ErrMalformed},
		{"recipient array too short (2)", "d8608440a0f6818240a0", ErrMalformed},
		{"recipient nested (4 elems)", "d8608440a0f6818440a041aa80", ErrMalformed},
		{"recipient ciphertext wrong type", "d8608440a0f6818340a001", ErrMalformed},
		{"duplicate protected label", "d8608445a201030104a0f6818340a041aa", ErrMalformed},
		{"duplicate unprotected label", "d8608440a201030104f6818340a041aa", ErrMalformed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Tolerate spaces in the hex literals above for readability.
			raw := hexDec(t, removeSpaces(tc.hex))
			env, rest, err := Decode(raw)
			require.ErrorIs(t, err, tc.want)
			require.Nil(t, env)
			require.Nil(t, rest)
		})
	}
}

func removeSpaces(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != ' ' {
			out = append(out, s[i])
		}
	}
	return string(out)
}

func TestDecodeExpectedType(t *testing.T) {
	withType := func(typ any) []byte {
		t.Helper()
		env := &Encrypt{
			Headers:    Headers{Protected: Header{}.Set(HeaderLabelType, typ)},
			Recipients: []*Recipient{{Ciphertext: []byte{0xAA}}},
		}
		b, err := env.Encode()
		require.NoError(t, err)
		return b
	}
	withoutType := func() []byte {
		t.Helper()
		env := &Encrypt{
			Headers:    Headers{Protected: Header{}.Set(HeaderLabelAlg, 3)},
			Recipients: []*Recipient{{Ciphertext: []byte{0xAA}}},
		}
		b, err := env.Encode()
		require.NoError(t, err)
		return b
	}

	t.Run("matching type", func(t *testing.T) {
		_, _, err := Decode(withType(exampleType), WithExpectedType(exampleType))
		require.NoError(t, err)
	})

	t.Run("wrong type", func(t *testing.T) {
		_, _, err := Decode(withType("application/other"), WithExpectedType(exampleType))
		require.ErrorIs(t, err, ErrUnexpectedType)
	})

	t.Run("missing type", func(t *testing.T) {
		_, _, err := Decode(withoutType(), WithExpectedType(exampleType))
		require.ErrorIs(t, err, ErrUnexpectedType)
	})

	t.Run("type present but not a string", func(t *testing.T) {
		_, _, err := Decode(withType(int64(7)), WithExpectedType(exampleType))
		require.ErrorIs(t, err, ErrUnexpectedType)
	})

	t.Run("no check when option omitted", func(t *testing.T) {
		_, _, err := Decode(withoutType())
		require.NoError(t, err)
	})
}
