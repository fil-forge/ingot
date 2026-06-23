package cose

import (
	"bytes"
	"math"
	"testing"
)

func TestHeaderAccessors(t *testing.T) {
	h := Header{}.
		Set(HeaderLabelAlg, 3).
		Set(HeaderLabelType, "application/x").
		Set(HeaderLabelKID, []byte("kid")).
		Set("flag", true)

	if v, ok := h.Int(HeaderLabelAlg); !ok || v != 3 {
		t.Errorf("Int(alg) = %d, %v; want 3, true", v, ok)
	}
	if v, ok := h.Uint(HeaderLabelAlg); !ok || v != 3 {
		t.Errorf("Uint(alg) = %d, %v; want 3, true", v, ok)
	}
	if v, ok := h.Text(HeaderLabelType); !ok || v != "application/x" {
		t.Errorf("Text(typ) = %q, %v; want application/x, true", v, ok)
	}
	if v, ok := h.Bytes(HeaderLabelKID); !ok || !bytes.Equal(v, []byte("kid")) {
		t.Errorf("Bytes(kid) = %x, %v; want 'kid', true", v, ok)
	}
	if !h.Has("flag") {
		t.Error("Has(flag) = false; want true")
	}
	if v, ok := h.Get("flag"); !ok || v != true {
		t.Errorf("Get(flag) = %v, %v; want true, true", v, ok)
	}

	// Absent label.
	if _, ok := h.Int(HeaderLabelIV); ok {
		t.Error("Int(absent) ok = true; want false")
	}
	// Type mismatches.
	if _, ok := h.Int(HeaderLabelType); ok {
		t.Error("Int on a text value ok = true; want false")
	}
	if _, ok := h.Text(HeaderLabelAlg); ok {
		t.Error("Text on an int value ok = true; want false")
	}
	if _, ok := h.Bytes(HeaderLabelType); ok {
		t.Error("Bytes on a text value ok = true; want false")
	}
}

func TestHeaderLabelNormalization(t *testing.T) {
	// A label Set as an untyped int must be retrievable as int64, and vice
	// versa: int(1), int64(1) and uint(1) name the same COSE label.
	h := Header{}.Set(1, "a")
	if v, ok := h.Text(int64(1)); !ok || v != "a" {
		t.Errorf("Text(int64(1)) = %q, %v; want a, true", v, ok)
	}
	if v, ok := h.Text(uint(1)); !ok || v != "a" {
		t.Errorf("Text(uint(1)) = %q, %v; want a, true", v, ok)
	}

	// Overwriting via a differently-typed-but-equal label updates in place.
	h.Set(int64(1), "b")
	if v, _ := h.Text(1); v != "b" {
		t.Errorf("after overwrite Text(1) = %q; want b", v)
	}
	if len(h) != 1 {
		t.Errorf("len = %d; want 1 (no duplicate key created)", len(h))
	}
}

func TestHeaderIntUintBoundaries(t *testing.T) {
	h := Header{}.
		Set("neg", int64(-5)).
		Set("big", uint64(math.MaxUint64))

	if v, ok := h.Int("neg"); !ok || v != -5 {
		t.Errorf("Int(neg) = %d, %v; want -5, true", v, ok)
	}
	// A negative value is not a valid Uint.
	if _, ok := h.Uint("neg"); ok {
		t.Error("Uint(neg) ok = true; want false")
	}
	// A uint64 too large for int64 is preserved and only readable as Uint.
	if v, ok := h.Uint("big"); !ok || v != math.MaxUint64 {
		t.Errorf("Uint(big) = %d, %v; want MaxUint64, true", v, ok)
	}
	if _, ok := h.Int("big"); ok {
		t.Error("Int(big) ok = true; want false (overflows int64)")
	}
}

func TestHeaderIntegerWidthNormalization(t *testing.T) {
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
		if got, ok := h.Int(k); !ok || got != v {
			t.Errorf("Int(%q) = %d, %v; want %d, true", k, got, ok, v)
		}
	}

	// Every integer width is also accepted as a label and addresses the same
	// entry as its int64 form.
	for i, label := range []any{int8(20), int16(20), int32(20), int(20), int64(20), uint8(20), uint16(20), uint32(20), uint(20), uint64(20)} {
		hh := Header{}.Set(label, i)
		if got, ok := hh.Int(int64(20)); !ok || got != int64(i) {
			t.Errorf("label %T: Int(20) = %d, %v; want %d", label, got, ok, i)
		}
	}
}

func TestHeaderSetPanicsOnOverflowLabel(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Set with a label overflowing int64 did not panic")
		}
	}()
	Header{}.Set(uint64(math.MaxUint64), "value")
}

func TestHeaderSetPanicsOnInvalidLabel(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Set with a non-int/string label did not panic")
		}
	}()
	Header{}.Set([]byte{0x01}, "value")
}

func TestEncodeRejectsInvalidLabel(t *testing.T) {
	// Bypass Set to plant an invalid label, then confirm Encode reports it
	// rather than producing bad CBOR.
	env := &Encrypt{
		Headers:    Headers{Protected: Header{float64(1.5): "x"}},
		Recipients: []*Recipient{{Ciphertext: []byte{0xAA}}},
	}
	if _, err := env.Encode(); err == nil {
		t.Fatal("Encode with invalid label: want error, got nil")
	}
}
