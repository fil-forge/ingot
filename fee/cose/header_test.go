package cose

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHeader groups the Header accessor and integer-normalization behaviors:
// typed getters, label normalization across integer widths, int/uint
// boundaries, and value-width normalization.
func TestHeader(t *testing.T) {
	t.Run("accessors", func(t *testing.T) {
		h := Header{}.
			Set(HeaderLabelAlg, 3).
			Set(HeaderLabelType, "application/x").
			Set(HeaderLabelKID, []byte("kid")).
			Set("flag", true)

		iv, ok := h.Int(HeaderLabelAlg)
		require.True(t, ok)
		require.Equal(t, int64(3), iv)
		uv, ok := h.Uint(HeaderLabelAlg)
		require.True(t, ok)
		require.Equal(t, uint64(3), uv)
		tv, ok := h.Text(HeaderLabelType)
		require.True(t, ok)
		require.Equal(t, "application/x", tv)
		bv, ok := h.Bytes(HeaderLabelKID)
		require.True(t, ok)
		require.Equal(t, []byte("kid"), bv)
		require.True(t, h.Has("flag"))
		gv, ok := h.Get("flag")
		require.True(t, ok)
		require.Equal(t, true, gv)

		// Absent label.
		_, ok = h.Int(HeaderLabelIV)
		require.False(t, ok)
		// Type mismatches.
		_, ok = h.Int(HeaderLabelType)
		require.False(t, ok)
		_, ok = h.Text(HeaderLabelAlg)
		require.False(t, ok)
		_, ok = h.Bytes(HeaderLabelType)
		require.False(t, ok)
	})

	t.Run("label normalization", func(t *testing.T) {
		// A label Set as an untyped int must be retrievable as int64, and vice
		// versa: int(1), int64(1) and uint(1) name the same COSE label.
		h := Header{}.Set(1, "a")
		v, ok := h.Text(int64(1))
		require.True(t, ok)
		require.Equal(t, "a", v)
		v, ok = h.Text(uint(1))
		require.True(t, ok)
		require.Equal(t, "a", v)

		// Overwriting via a differently-typed-but-equal label updates in place.
		h.Set(int64(1), "b")
		v, _ = h.Text(1)
		require.Equal(t, "b", v)
		require.Len(t, h, 1)
	})

	t.Run("int uint boundaries", func(t *testing.T) {
		h := Header{}.
			Set("neg", int64(-5)).
			Set("big", uint64(math.MaxUint64))

		v, ok := h.Int("neg")
		require.True(t, ok)
		require.Equal(t, int64(-5), v)
		// A negative value is not a valid Uint.
		_, ok = h.Uint("neg")
		require.False(t, ok)
		// A uint64 too large for int64 is preserved and only readable as Uint.
		uv, ok := h.Uint("big")
		require.True(t, ok)
		require.Equal(t, uint64(math.MaxUint64), uv)
		_, ok = h.Int("big")
		require.False(t, ok)
	})

	t.Run("integer width normalization", func(t *testing.T) {
		// Every Go integer width, as a value, normalizes to the same int64 and is
		// read back identically.
		h := Header{}.
			Set("i8", int8(-1)).
			Set("i16", int16(-2)).
			Set("i32", int32(-3)).
			Set("i", int(-4)).
			Set("i64", int64(-5)).
			Set("u8", uint8(6)).
			Set("u16", uint16(7)).
			Set("u32", uint32(8)).
			Set("u", uint(9)).
			Set("u64", uint64(10))

		want := map[string]int64{
			"i8": -1, "i16": -2, "i32": -3, "i": -4, "i64": -5,
			"u8": 6, "u16": 7, "u32": 8, "u": 9, "u64": 10,
		}
		for k, v := range want {
			got, ok := h.Int(k)
			require.True(t, ok)
			require.Equal(t, v, got)
		}

		// Every integer width is also accepted as a label and addresses the same
		// entry as its int64 form.
		for i, label := range []any{int8(20), int16(20), int32(20), int(20), int64(20), uint8(20), uint16(20), uint32(20), uint(20), uint64(20)} {
			hh := Header{}.Set(label, i)
			got, ok := hh.Int(int64(20))
			require.True(t, ok)
			require.Equal(t, int64(i), got)
		}
	})
}

// TestHeaderSetPanics groups Set's label-validation panics: a label that
// overflows int64 and a label of an unsupported type both panic at Set time.
func TestHeaderSetPanics(t *testing.T) {
	t.Run("overflow label", func(t *testing.T) {
		require.Panics(t, func() {
			Header{}.Set(uint64(math.MaxUint64), "value")
		})
	})

	t.Run("invalid label", func(t *testing.T) {
		require.Panics(t, func() {
			Header{}.Set([]byte{0x01}, "value")
		})
	})
}

func TestEncodeRejectsInvalidLabel(t *testing.T) {
	// Bypass Set to plant an invalid label, then confirm Encode reports it
	// rather than producing bad CBOR.
	env := &Encrypt{
		Headers:    Headers{Protected: Header{float64(1.5): "x"}},
		Recipients: []*Recipient{{Ciphertext: []byte{0xAA}}},
	}
	_, err := env.Encode()
	require.Error(t, err)
}
