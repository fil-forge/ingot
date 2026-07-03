package cose

import (
	"bytes"
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
)

// decodeConfig holds optional decode-time validations.
type decodeConfig struct {
	checkType    bool
	expectedType string
}

// DecodeOption configures [Decode].
type DecodeOption func(*decodeConfig)

// WithExpectedType requires the decoded protected header to carry a "typ"
// parameter (label 16) equal to typ; otherwise decoding fails with
// ErrUnexpectedType. This is how a profile such as FEE pins its envelope type
// without cose having to know that type. Without this option no typ check is
// performed.
func WithExpectedType(typ string) DecodeOption {
	return func(c *decodeConfig) {
		c.checkType = true
		c.expectedType = typ
	}
}

func newDecodeConfig(opts []DecodeOption) decodeConfig {
	var cfg decodeConfig
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// checkTyp enforces WithExpectedType against a decoded protected header.
func (cfg decodeConfig) checkTyp(protected Header) error {
	if !cfg.checkType {
		return nil
	}
	got, ok := protected.Text(HeaderLabelType)
	if !ok || got != cfg.expectedType {
		return fmt.Errorf("%w: got %q, want %q", ErrUnexpectedType, got, cfg.expectedType)
	}
	return nil
}

// Decode parses a detached COSE envelope from the front of data — either a
// COSE_Encrypt (CBOR tag 96) or a COSE_Encrypt0 (CBOR tag 16), dispatching on
// the tag it finds — and returns the decoded [Envelope] together with rest: the
// bytes that follow the self-delimited envelope item, i.e. the detached
// ciphertext. rest is empty when nothing follows the envelope.
//
// Decode is strict: tag 96 must wrap a 4-element array with at least one
// well-formed, 3-element recipient; tag 16 must wrap a 3-element array with no
// recipients; both require a byte-string protected header, map headers without
// duplicate labels, and a null (detached) body ciphertext. Any deviation returns
// an error (wrapping one of the package sentinels) and a nil envelope — never a
// partially populated one. Because a valid tag-96 always carries recipients and
// tag-16 never does, the resulting Envelope's recipient presence mirrors the
// tag; [Envelope] relies on exactly that invariant.
func Decode(data []byte, opts ...DecodeOption) (env *Envelope, rest []byte, err error) {
	// Read exactly one CBOR item; the remainder is the detached payload.
	dec := decMode.NewDecoder(bytes.NewReader(data))
	var first cbor.RawMessage
	if err := dec.Decode(&first); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	rest = data[dec.NumBytesRead():]

	tag, arr, err := decodeTagArray(first)
	if err != nil {
		return nil, nil, err
	}
	env, err = decodeEnvelope(tag, arr)
	if err != nil {
		return nil, nil, err
	}
	if err := newDecodeConfig(opts).checkTyp(env.Headers.Protected); err != nil {
		return nil, nil, err
	}
	return env, rest, nil
}

// decodeTagArray unmarshals one already-read CBOR item into the tag number and
// element array it wraps: the item must be a tag whose content is an array. It
// is the shared preamble of [Decode] — element-count and per-element validation
// is left to [decodeEnvelope] — factored out so a streaming decoder can reuse
// the same tag/array extraction without duplicating it.
func decodeTagArray(first cbor.RawMessage) (tag uint64, arr []cbor.RawMessage, err error) {
	var t cbor.RawTag
	if err := decMode.Unmarshal(first, &t); err != nil {
		return 0, nil, fmt.Errorf("%w: %v", ErrNotEncrypt, err)
	}
	if cborMajor(t.Content) != majorArray {
		return 0, nil, fmt.Errorf("%w: tag content is not an array", ErrMalformed)
	}
	if err := decMode.Unmarshal(t.Content, &arr); err != nil {
		return 0, nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	return t.Number, arr, nil
}

// decodeEnvelope validates an already-decoded (tag, element-array) pair into an
// [Envelope]. It is the tag-dispatch core of [Decode]: tag 96 requires a
// 4-element array with a non-empty recipients array; tag 16 requires a 3-element
// array and yields a recipient-less envelope; both require a byte-string
// protected header and a null body. Any other tag is ErrNotEncrypt.
func decodeEnvelope(tag uint64, arr []cbor.RawMessage) (*Envelope, error) {
	switch tag {
	case TagCOSEEncrypt:
		if len(arr) != 4 {
			return nil, fmt.Errorf("%w: array has %d elements, want 4", ErrMalformed, len(arr))
		}
		headers, err := decodeHeaders(arr[0], arr[1])
		if err != nil {
			return nil, err
		}
		if !isNull(arr[2]) {
			return nil, ErrDetachedPayload
		}
		recipients, err := decodeRecipients(arr[3])
		if err != nil {
			return nil, err
		}
		return &Envelope{Headers: headers, Recipients: recipients}, nil
	case TagCOSEEncrypt0:
		if len(arr) != 3 {
			return nil, fmt.Errorf("%w: array has %d elements, want 3", ErrMalformed, len(arr))
		}
		headers, err := decodeHeaders(arr[0], arr[1])
		if err != nil {
			return nil, err
		}
		if !isNull(arr[2]) {
			return nil, ErrDetachedPayload
		}
		return &Envelope{Headers: headers}, nil
	default:
		return nil, fmt.Errorf("%w: got tag %d", ErrNotEncrypt, tag)
	}
}

// PeekTag reads the CBOR tag number of the item at the front of data without
// otherwise decoding it. [Decode] dispatches on the tag itself, so PeekTag is
// only needed when a caller wants to inspect the on-wire form (tag 96
// [TagCOSEEncrypt] vs tag 16 [TagCOSEEncrypt0]) without decoding. It returns
// ErrMalformed if data does not begin with a CBOR tag.
func PeekTag(data []byte) (uint64, error) {
	dec := decMode.NewDecoder(bytes.NewReader(data))
	var tag cbor.RawTag
	if err := dec.Decode(&tag); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	return tag.Number, nil
}

// DecodedEnvelope is a decoded detached COSE envelope header returned by
// [DecodeReader]: either a COSE_Encrypt (tag 96, carrying Recipients) or a
// COSE_Encrypt0 (tag 16, no recipients), distinguished by Tag.
type DecodedEnvelope struct {
	// Tag is [TagCOSEEncrypt] (96) or [TagCOSEEncrypt0] (16).
	Tag uint64
	// Headers is the body protected/unprotected header pair.
	Headers Headers
	// Recipients are the per-recipient wrapped-key entries; nil for a
	// COSE_Encrypt0 (tag 16).
	Recipients []*Recipient
}

// EncStructure returns the body AAD Enc_structure for this envelope, using the
// context that matches its tag: "Encrypt" for a COSE_Encrypt (tag 96),
// "Encrypt0" for a COSE_Encrypt0 (tag 16). See [Encrypt.EncStructure].
func (e *DecodedEnvelope) EncStructure(externalAAD []byte) ([]byte, error) {
	ctx := contextEncrypt
	if e.Tag == TagCOSEEncrypt0 {
		ctx = contextEncrypt0
	}
	prot, err := e.Headers.protectedBytes()
	if err != nil {
		return nil, fmt.Errorf("cose: building Enc_structure: %w", err)
	}
	return encStructureBytes(ctx, prot, externalAAD)
}

// DecodeReader reads one detached COSE_Encrypt (tag 96) or COSE_Encrypt0 (tag
// 16) from the front of r and returns the decoded envelope header together with
// rest: a reader over the bytes that follow the self-delimited envelope item —
// the detached ciphertext. rest draws first from whatever the decoder buffered
// past the envelope, then from r, so only the (small) header is held in memory
// and an arbitrarily large ciphertext can be streamed.
//
// It is the streaming, tag-dispatching counterpart to [Decode] and
// [DecodeEncrypt0], and is as strict as they are: a byte-string protected
// header, map headers without duplicate labels, a null detached body, and — for
// tag 96 — at least one well-formed 3-element recipient. Any deviation returns
// an error (wrapping a package sentinel) and a nil envelope.
func DecodeReader(r io.Reader, opts ...DecodeOption) (env *DecodedEnvelope, rest io.Reader, err error) {
	var cfg decodeConfig
	for _, o := range opts {
		o(&cfg)
	}

	// Read exactly one CBOR item. Whatever the decoder buffered past that item,
	// followed by the unread remainder of r, is the detached payload.
	dec := decMode.NewDecoder(r)
	var first cbor.RawMessage
	if err := dec.Decode(&first); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	rest = io.MultiReader(dec.Buffered(), r)

	var tag cbor.RawTag
	if err := decMode.Unmarshal(first, &tag); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrNotEncrypt, err)
	}
	if tag.Number != TagCOSEEncrypt && tag.Number != TagCOSEEncrypt0 {
		return nil, nil, fmt.Errorf("%w: got tag %d", ErrNotEncrypt, tag.Number)
	}

	if cborMajor(tag.Content) != majorArray {
		return nil, nil, fmt.Errorf("%w: tag content is not an array", ErrMalformed)
	}
	var arr []cbor.RawMessage
	if err := decMode.Unmarshal(tag.Content, &arr); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}

	// A COSE_Encrypt is a 4-element array (with recipients); a COSE_Encrypt0 is
	// a 3-element array (no recipients).
	wantLen := 3
	if tag.Number == TagCOSEEncrypt {
		wantLen = 4
	}
	if len(arr) != wantLen {
		return nil, nil, fmt.Errorf("%w: array has %d elements, want %d", ErrMalformed, len(arr), wantLen)
	}

	headers, err := decodeHeaders(arr[0], arr[1])
	if err != nil {
		return nil, nil, err
	}

	// Detached payload: the body ciphertext must be null.
	if !isNull(arr[2]) {
		return nil, nil, ErrDetachedPayload
	}

	env = &DecodedEnvelope{Tag: tag.Number, Headers: headers}
	if tag.Number == TagCOSEEncrypt {
		recipients, err := decodeRecipients(arr[3])
		if err != nil {
			return nil, nil, err
		}
		env.Recipients = recipients
	}

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
