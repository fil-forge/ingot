# FEE cross-implementation test vectors

Fixed fixtures that pin the **FEE (FilOne File Encryption Envelope)** wire format
across two implementations:

- **Go** — this repo's `fee/cose`, `fee/aesstream`, `fee/ecdhkw`, `fee/aeskw`.
- **TypeScript** — the reference `foc-encryption`
  ([`Kubuxu/foc-encryption-demo`](https://github.com/Kubuxu/foc-encryption-demo),
  `packages/foc-encryption`), **pinned** at commit
  `f0eac6ea1f54bd9100c9d328f39e7509b5bcfdf4` in
  [`pull-foc-encryption.sh`](./pull-foc-encryption.sh).

The reference is the **source of truth** for the wire format; the Go side was
reconciled to it (see [Wire format](#wire-format) and
[Divergences reconciled](#divergences-reconciled-on-the-go-side)).

## What's covered (acceptance criteria)

| Fixture | Direction | Envelope |
|---|---|---|
| `single-chunk-go` | Go seals → TS decrypts | tag 16 (COSE_Encrypt0) |
| `multi-chunk-ts` | TS seals → Go decrypts | tag 16 (COSE_Encrypt0) |
| `multi-recipient-go` | Go seals → TS parses recipients + decrypts body | tag 96 (COSE_Encrypt) |
| `multi-chunk-go` | Go seals → TS decrypts (extra multi-chunk coverage) | tag 16 |

Each `testdata/<name>/` holds `blob.bin` (`envelope‖ciphertext`),
`plaintext.bin`, and `meta.json`.

Both directions are exercised:

- **Go decrypts every fixture** — `go test ./fee/vectors` (`TestVectors`). For
  the tag-96 fixture it also *unwraps* each recipient's CEK (ECDH-ES+A256KW over
  X25519, and A256KW) and checks it equals the shared CEK — the assertion the
  reference can't make, since it has no key-unwrap code.
- **The real `foc-encryption` decrypts every fixture** — `pull-foc-encryption.sh`
  drives the pinned reference to decrypt each `blob.bin` from the CEK in
  `meta.json` and to (re)generate `multi-chunk-ts`.

## Wire format

`blob = envelope ‖ ciphertext` (detached payload). The COSE envelope is
self-delimiting CBOR; the bytes after it are the STREAM ciphertext.

```
envelope    = 16([ protected, unprotected, null ])              # no recipients
            | 96([ protected, unprotected, null, recipients ])  # with recipients
protected   = { 1: -65793, 16: "application/vnd.foc-envelope+cose" }
unprotected = { 5: baseNonce(7B), -65790: chunkSize, -65791: chunkCount }
recipient   = [ {1: alg}, {4: kid, ...}, wrappedKey ]           # alg -31 or -5
```

- **Body cipher** — chunked AES-256-GCM-STREAM, alg `-65793`. Per-chunk nonce is
  `baseNonce[7] ‖ chunkIndex[4, big-endian] ‖ lastFlag[1]` (`0x01` on the final
  chunk), tag 16 bytes.
- **Body AAD** — `Enc_structure = [ "Encrypt0", protected, "" ]`, the **same**
  for every chunk, and always the `"Encrypt0"` context **even for a tag-96
  multi-recipient envelope**. AAD interop is order-independent: both sides key
  the AAD off the on-wire raw protected bytes.
- **Recipients** — `wrappedKey` is carried opaquely; the reference never unwraps
  it (decryption takes the CEK directly). The Go side does real
  ECDH-ES+A256KW / A256KW wrap and unwrap.

## Divergences reconciled on the Go side

Reconciling `fee/*` to the reference (this issue, FIL-473) required, in
`fee/cose`:

- **COSE_Encrypt0 (tag 16)** — added `Encrypt0` (encode + decode + `PeekTag`);
  it's the reference's envelope for a keyless/no-recipient file. Previously
  `cose` only did tag 96.
- **`"Encrypt0"` AAD context** — the reference's body AAD always uses the
  `"Encrypt0"` context; `cose` gained `EncStructureBytes` + `Encrypt0.EncStructure`
  so callers can build it (the tag-96 body uses it too).

The FEE profile values — `typ` (`application/vnd.foc-envelope+cose`), body alg
(`-65793`), and the private-use unprotected labels (`chunkSize -65790`,
`chunkCount -65791`) — live in `helpers_test.go` (the caller), matching the
reference's `src/cose/headers.ts`. `fee/aesstream`'s STREAM framing already
matched byte-for-byte and was unchanged.

> A top-level `fee` composer (PR #14 / FIL-569) is still open and currently
> encodes a few of these differently (`typ`, chunk-size label/location, AAD
> context). It should adopt the values pinned here.

## Regenerating

Fixtures are checked in; tests are deterministic (they only read fixed files and
run deterministic decrypt/unwrap). To recreate them:

```bash
# Go-produced fixtures (single-chunk-go, multi-chunk-go, multi-recipient-go):
FEE_VECTORS_REGEN=1 GOWORK=off go test ./fee/vectors -run TestGenerate -v

# TS-produced fixture (multi-chunk-ts) + verify every fixture decrypts under the
# real, pinned foc-encryption:
./fee/vectors/pull-foc-encryption.sh
```

`pull-foc-encryption.sh` vendors the pinned reference into `ts/vendor/`
(gitignored — never committed) via `git clone` at the pinned SHA, falling back to
fetching the pinned source files from `raw.githubusercontent.com` where `git` is
unavailable. It requires [`bun`](https://bun.sh) to run the TypeScript.

The base nonces (Go's fixed, the reference's random) mean a regenerated blob may
differ byte-for-byte from the committed one while remaining a valid vector; the
committed files are the fixed reference.

## Test key material

All keys (CEKs, the X25519 tenant key, the A256KW KEK, base nonces) are
**non-secret**, derived deterministically from fixed labels, and recorded in
each `meta.json`. They exist only to pin these vectors — never reuse them.
