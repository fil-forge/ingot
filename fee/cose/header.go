package cose

import (
	"fmt"
	"math"

	"github.com/fxamacker/cbor/v2"
)

// Header is a COSE header map: a set of label→value pairs. Per RFC 9052 a
// label is either an integer or a text string; values are arbitrary CBOR.
// Values are stored in their decoded Go form (int64, uint64, string, []byte,
// bool, float64, []any, map[any]any, …) so that unknown or
// application-specific parameters — including nested maps such as a metadata
// bucket — round-trip without loss.
//
// Integer labels are normalized to int64. Use the int64 HeaderLabel*
// constants (or plain integer / string literals) as keys; the accessor
// methods normalize the label they are given before looking it up.
type Header map[any]any

// Set stores value under label and returns the receiver for chaining. The
// label is normalized to int64 (if integral) or kept as a string; integral
// values are likewise normalized to int64. Set panics if label is neither an
// integer nor a string, which is a construction-time programming error.
//
// Set must not be called on a nil Header; construct one with Header{} first.
func (h Header) Set(label, value any) Header {
	nl, err := normalizeLabel(label)
	if err != nil {
		panic(err)
	}
	h[nl] = normalizeValue(value)
	return h
}

// Get returns the raw value stored under label and whether it was present.
func (h Header) Get(label any) (any, bool) {
	nl, err := normalizeLabel(label)
	if err != nil {
		return nil, false
	}
	v, ok := h[nl]
	return v, ok
}

// Has reports whether label is present.
func (h Header) Has(label any) bool {
	_, ok := h.Get(label)
	return ok
}

// Int returns the value under label as an int64. The second result is false
// if the label is absent or the value is not an integer that fits in int64.
func (h Header) Int(label any) (int64, bool) {
	v, ok := h.Get(label)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int64:
		return n, true
	case uint64:
		if n <= math.MaxInt64 {
			return int64(n), true
		}
	}
	return 0, false
}

// Uint returns the value under label as a uint64. The second result is false
// if the label is absent or the value is not a non-negative integer.
func (h Header) Uint(label any) (uint64, bool) {
	v, ok := h.Get(label)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case uint64:
		return n, true
	case int64:
		if n >= 0 {
			return uint64(n), true
		}
	}
	return 0, false
}

// Bytes returns the value under label as a byte string. The second result is
// false if the label is absent or the value is not a []byte.
func (h Header) Bytes(label any) ([]byte, bool) {
	v, ok := h.Get(label)
	if !ok {
		return nil, false
	}
	b, ok := v.([]byte)
	return b, ok
}

// Text returns the value under label as a text string. The second result is
// false if the label is absent or the value is not a string.
func (h Header) Text(label any) (string, bool) {
	v, ok := h.Get(label)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// normalized returns a copy of h with every label and integral value
// normalized, validating that each label is an integer or text string. It is
// the encode-time boundary: even a Header built by bypassing Set is checked
// here. A nil or empty Header yields a non-nil empty Header.
func (h Header) normalized() (Header, error) {
	out := make(Header, len(h))
	for k, v := range h {
		nl, err := normalizeLabel(k)
		if err != nil {
			return nil, err
		}
		if _, dup := out[nl]; dup {
			return nil, fmt.Errorf("%w: %v", errDuplicateLabel, nl)
		}
		out[nl] = normalizeValue(v)
	}
	return out, nil
}

// errDuplicateLabel is reported when two distinct Go keys normalize to the
// same COSE label (e.g. int(1) and int64(1)).
var errDuplicateLabel = fmt.Errorf("%w: duplicate label", ErrMalformed)

// normalizeLabel converts an integer label to int64 or passes a string label
// through, rejecting anything else.
func normalizeLabel(label any) (any, error) {
	switch v := label.(type) {
	case string:
		return v, nil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return uintToLabel(uint64(v))
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return uintToLabel(v)
	default:
		return nil, fmt.Errorf("%w: %T", ErrInvalidLabel, label)
	}
}

func uintToLabel(v uint64) (any, error) {
	if v > math.MaxInt64 {
		return nil, fmt.Errorf("%w: %d overflows int64", ErrInvalidLabel, v)
	}
	return int64(v), nil
}

// normalizeValue normalizes integral values to int64 (or uint64 when too
// large to fit int64) so that a value set in Go matches the form produced by
// decode. It recurses into []any and map[any]any — including map keys — so
// nested integers, such as those inside a COSE_Key or an application metadata
// map, are normalized at every depth. Non-integer scalars are returned
// unchanged.
func normalizeValue(value any) any {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return uintToValue(uint64(v))
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return uintToValue(v)
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = normalizeValue(e)
		}
		return out
	case map[any]any:
		out := make(map[any]any, len(v))
		for k, e := range v {
			out[normalizeValue(k)] = normalizeValue(e)
		}
		return out
	default:
		return value
	}
}

func uintToValue(v uint64) any {
	if v > math.MaxInt64 {
		return v
	}
	return int64(v)
}

// encodeMap serializes a header map as a CBOR map in core deterministic form.
// A nil/empty map encodes as an empty CBOR map (0xa0).
func encodeMap(h Header) ([]byte, error) {
	n, err := h.normalized()
	if err != nil {
		return nil, err
	}
	return encMode.Marshal(n)
}

// decodeHeaderMap decodes a CBOR map into a normalized Header. It requires the
// item to be a CBOR map and rejects duplicate or non-label keys.
func decodeHeaderMap(raw cbor.RawMessage) (Header, error) {
	if cborMajor(raw) != majorMap {
		return nil, fmt.Errorf("%w: header is not a map", ErrMalformed)
	}
	var m map[any]any
	if err := decMode.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	h := make(Header, len(m))
	for k, v := range m {
		nl, err := normalizeLabel(k)
		if err != nil {
			return nil, err
		}
		if _, dup := h[nl]; dup {
			return nil, fmt.Errorf("%w: %v", errDuplicateLabel, nl)
		}
		h[nl] = normalizeValue(v)
	}
	return h, nil
}

// cborMajor returns the CBOR major type of the first byte of an encoded item,
// or 0xff if the item is empty.
func cborMajor(raw cbor.RawMessage) byte {
	if len(raw) == 0 {
		return 0xff
	}
	return raw[0] >> 5
}

// bstr returns b, or an empty non-nil slice when b is nil, so it always
// encodes as a CBOR byte string (a nil slice would encode as CBOR null).
func bstr(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// isNull reports whether raw is the single-byte CBOR null (0xf6).
func isNull(raw cbor.RawMessage) bool {
	return len(raw) == 1 && raw[0] == 0xf6
}
