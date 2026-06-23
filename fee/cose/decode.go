package cose

import (
	"bytes"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// decodeConfig holds optional decode-time validations.
type decodeConfig struct {
	checkType    bool
	expectedType string
}

// DecodeOption configures Decode.
type DecodeOption func(*decodeConfig)

// WithExpectedType requires the decoded protected header to carry a "typ"
// parameter (label 16) equal to typ; otherwise Decode fails with
// ErrUnexpectedType. This is how a profile such as FEE pins its envelope type
// without cose having to know that type. Without this option Decode performs
// no typ check.
func WithExpectedType(typ string) DecodeOption {
	return func(c *decodeConfig) {
		c.checkType = true
		c.expectedType = typ
	}
}

// Decode parses a detached COSE_Encrypt (CBOR tag 96) from the front of data
// and returns the envelope together with rest: the bytes that follow the
// self-delimited envelope item, i.e. the detached ciphertext. rest is empty
// when nothing follows the envelope.
//
// Decode is strict: it requires tag 96 wrapping a 4-element array, a byte-
// string protected header, map headers without duplicate labels, a null body
// ciphertext (the payload is detached), and at least one well-formed,
// 3-element recipient. Any deviation returns an error (wrapping one of the
// package sentinels) and a nil envelope — never a partially populated one.
func Decode(data []byte, opts ...DecodeOption) (env *Encrypt, rest []byte, err error) {
	var cfg decodeConfig
	for _, o := range opts {
		o(&cfg)
	}

	// Read exactly one CBOR item; the remainder is the detached payload.
	dec := decMode.NewDecoder(bytes.NewReader(data))
	var first cbor.RawMessage
	if err := dec.Decode(&first); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	rest = data[dec.NumBytesRead():]

	// The item must be a tag-96 wrapper.
	var tag cbor.RawTag
	if err := decMode.Unmarshal(first, &tag); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrNotEncrypt, err)
	}
	if tag.Number != TagCOSEEncrypt {
		return nil, nil, fmt.Errorf("%w: got tag %d", ErrNotEncrypt, tag.Number)
	}

	// The tag content must be a 4-element array.
	if cborMajor(tag.Content) != majorArray {
		return nil, nil, fmt.Errorf("%w: tag content is not an array", ErrMalformed)
	}
	var arr []cbor.RawMessage
	if err := decMode.Unmarshal(tag.Content, &arr); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if len(arr) != 4 {
		return nil, nil, fmt.Errorf("%w: array has %d elements, want 4", ErrMalformed, len(arr))
	}

	headers, err := decodeHeaders(arr[0], arr[1])
	if err != nil {
		return nil, nil, err
	}

	// Detached payload: the body ciphertext must be null.
	if !isNull(arr[2]) {
		return nil, nil, ErrDetachedPayload
	}

	recipients, err := decodeRecipients(arr[3])
	if err != nil {
		return nil, nil, err
	}

	env = &Encrypt{Headers: headers, Recipients: recipients}

	if cfg.checkType {
		got, ok := env.Headers.Protected.Text(HeaderLabelType)
		if !ok || got != cfg.expectedType {
			return nil, nil, fmt.Errorf("%w: got %q, want %q", ErrUnexpectedType, got, cfg.expectedType)
		}
	}

	return env, rest, nil
}

// decodeHeaders decodes a [protected, unprotected] pair. The protected element
// is a byte string whose content (when non-empty) is itself a CBOR map; its
// raw bytes are preserved on RawProtected for AAD stability.
func decodeHeaders(protRaw, unprotRaw cbor.RawMessage) (Headers, error) {
	if cborMajor(protRaw) != majorByteString {
		return Headers{}, fmt.Errorf("%w: protected header is not a byte string", ErrMalformed)
	}
	var protContent []byte
	if err := decMode.Unmarshal(protRaw, &protContent); err != nil {
		return Headers{}, fmt.Errorf("%w: protected header: %v", ErrMalformed, err)
	}

	protected := Header{}
	rawProtected := []byte{}
	if len(protContent) > 0 {
		m, err := decodeHeaderMap(protContent)
		if err != nil {
			return Headers{}, fmt.Errorf("protected header: %w", err)
		}
		protected = m
		rawProtected = protContent
	}

	unprotected, err := decodeHeaderMap(unprotRaw)
	if err != nil {
		return Headers{}, fmt.Errorf("unprotected header: %w", err)
	}

	return Headers{
		Protected:    protected,
		Unprotected:  unprotected,
		RawProtected: rawProtected,
	}, nil
}

// decodeRecipients decodes the recipients array, requiring a non-empty array
// of well-formed recipients.
func decodeRecipients(raw cbor.RawMessage) ([]*Recipient, error) {
	if cborMajor(raw) != majorArray {
		return nil, fmt.Errorf("%w: recipients is not an array", ErrMalformed)
	}
	var rawRecipients []cbor.RawMessage
	if err := decMode.Unmarshal(raw, &rawRecipients); err != nil {
		return nil, fmt.Errorf("%w: recipients: %v", ErrMalformed, err)
	}
	if len(rawRecipients) == 0 {
		return nil, ErrNoRecipients
	}
	recipients := make([]*Recipient, len(rawRecipients))
	for i, rr := range rawRecipients {
		rec, err := decodeRecipient(rr)
		if err != nil {
			return nil, fmt.Errorf("recipient %d: %w", i, err)
		}
		recipients[i] = rec
	}
	return recipients, nil
}

// decodeRecipient decodes a single 3-element COSE_recipient. A 4-element
// recipient (nested recipients) is rejected: this package does not support
// recipient nesting.
func decodeRecipient(raw cbor.RawMessage) (*Recipient, error) {
	if cborMajor(raw) != majorArray {
		return nil, fmt.Errorf("%w: recipient is not an array", ErrMalformed)
	}
	var arr []cbor.RawMessage
	if err := decMode.Unmarshal(raw, &arr); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if len(arr) != 3 {
		return nil, fmt.Errorf("%w: recipient array has %d elements, want 3", ErrMalformed, len(arr))
	}

	headers, err := decodeHeaders(arr[0], arr[1])
	if err != nil {
		return nil, err
	}

	var ciphertext []byte
	if !isNull(arr[2]) {
		if cborMajor(arr[2]) != majorByteString {
			return nil, fmt.Errorf("%w: ciphertext must be a byte string or null", ErrMalformed)
		}
		if err := decMode.Unmarshal(arr[2], &ciphertext); err != nil {
			return nil, fmt.Errorf("%w: ciphertext: %v", ErrMalformed, err)
		}
	}

	return &Recipient{Headers: headers, Ciphertext: ciphertext}, nil
}
