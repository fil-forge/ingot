# ingot: how it works

ingot is an **S3 gateway over the [Forge](https://github.com/fil-forge)
network**. It presents each S3 bucket as a per-bucket Merkle Search Tree
(MST), stores object bodies on Forge storage nodes as content-addressed
blobs, journals the catalog (MST nodes and manifests) to a local per-bucket
log, and serves reads back through local tiers with the network as the last
resort.

This is the architecture as it operates today. The target design and its
rationale live in [`docs/architecture.md`](./docs/architecture.md); the
as-built diagram set is [`docs/diagrams.md`](./docs/diagrams.md); subsystem
detail lives in package READMEs (notably
[`logstore/README.md`](./logstore/README.md)); how to work in the repo lives
in [`CLAUDE.md`](./CLAUDE.md).

## Running it

- **Library.** A host imports the fx `Module(cfg)` (or `ServerModule` plus
  the non-fx `New(ctx, ServerConfig, ServerDeps)`) and supplies a logger, a
  Postgres pool, and the **agent** identity (libforge `identity.Identity`); config names
  the sprue endpoint (`upload_service_url`/`_did`) and the hilt endpoint
  (`auth_service_url`/`_did`).
- **Daemon.** `ingot serve` builds the same wiring from a config file
  (cobra/viper/fx): Postgres, the sprue edge client, and hilt are all
  required. The CLI is `serve`, `whoami`, and `version`. Docker-native;
  ships as a smelt system.

There is no standalone or in-memory mode: the deployment under test is
always the real gateway. Tenancy is owned by **hilt**: an operator
provisions tenants and access keys through hilt's tenant API, hilt mints
each bucket's space and issues the S3 credentials, and ingot never
self-provisions.

## Write path

A PUT streams the body into the local **spool** (sha256 and md5 in one
pass), splits it into blobs of at most `max_blob_size`, and uploads each
blob to the network before anything commits: `/blob/add` against sprue, an
HTTP PUT of the bytes to the allocated piri, a concluded receipt, and the
`/blob/accept` location commitment. Only then does the short per-bucket
critical section run: allocate the version seq, write the manifest, splice
the MST, fsync one `AppendBatch` of the new catalog blocks, and
compare-and-swap the bucket root in Postgres. The reference index
(`blob_refs`) reconciles after the commit, releasing superseded blobs whose
claim count reaches zero. The full trace is the
[PutObject diagram](./docs/diagrams.md#putobject-spool-and-upload-off-the-lock-commit-under-it).

**Every body blob is encrypted at ingest** (the FilOne encryption design's
write side, `s3frontend/encrypt.go`). Each plaintext piece SplitBody cuts
gets a fresh CEK and streams through FEE into a recipient-less
`COSE_Encrypt0` envelope (AES-256-GCM STREAM, 256 KiB chunks); the envelope
is what the spool stores and the network receives, under its **ciphertext**
digest. The CEK is wrapped by `regionkey.Provider` bound to (space, digest)
and stored in the blob's `blob_encryption_params` row before any manifest
can reference the digest. The split geometry, manifest spans, `Body.Size`,
sha256/md5 and ETag are all plaintext values — only the digest and the
stored sizes (`upload_intents.Size`, `blob_locations.Size`) name ciphertext.
Consequences, per the RFC: content **dedup is gone** for bodies (a fresh CEK
makes every stored digest unique), a DELETE that releases a blob's last
claim also deletes its params row (crypto-shred — the ciphertext is
unreadable even where copies survive), and **cross-space CopyObject is
rejected** `NotImplemented` (the CEK wrap is space-bound; a rewrap flow is a
filed follow-up). The tenant/insurance recipient of the RFC waits on Hilt's
wrap-key registry and DID-document publication; FEE's multi-recipient model
lets new writes add it without changing this layer.

A `200` therefore means the body is durable and accepted on the network and
the catalog mutation is fsynced locally; the catalog becomes durable on
Forge through the background ship below.

## The catalog log

The log journals only the catalog, and it is segregated per bucket:
`logstore.Manager` holds one `Store` per bucket, so a sealed segment holds
exactly one bucket's blocks and ships to that bucket's Forge space. The
load-bearing rules ([`logstore/README.md`](./logstore/README.md) has the
full lifecycle):

- A successful `AppendBatch` (fsynced CAR plus `.ops` record) is what
  licenses the caller's bucket-root CAS.
- **`forge_root_cid` advances only when a segment ships, and only guarded**:
  the update lands in the same transaction as the shipped stamp, and only
  where `buckets.root_cid` still equals the op-root.
- **The catalog is location-free**: manifests and MST nodes reference
  content by CID only; byte location resolves at read time through the
  locator, never embedded in the DAG.

## Shipping: the edge-client flow

`uploader.Forge` ships every blob the same way, whether an object-body blob
at PUT time or a sealed catalog CAR from the background flush:

1. `/blob/add` against **sprue** (which allocates against a piri).
2. HTTP **PUT** the bytes to the allocated piri (skipped on dedup).
3. `/ucan/conclude` a synthesized `/http/put` receipt: piri has no conclude
   handler, sprue does, and this is the step that triggers `/blob/accept`.
4. Poll the `/blob/accept` receipt for the `/assert/location` commitment.
5. For catalog CARs only: build a 1-shard sharded-dag-index, `/blob/add` it,
   then `/index/add` it (best-effort; sprue republishes to the
   indexing-service). The shard's location and every inner block's byte
   range are recorded locally (`blob_locations` and `shard_inclusions`)
   before the segment is marked shipped.

Multipart parts stop after step 2 (**parked**: durable, unaccepted) and run
steps 3 and 4 at `CompleteMultipartUpload`; an abort unwinds a parked blob
with `/blob/abort`.

## Read path

A GET resolves the bucket root (registry), walks the MST to the manifest
(through the per-key version tree when the key is versioned), and serves
each covering blob from the first tier that has it: the spool, the catalog
log (catalog blocks only), then the network (`blockstore.Forge`). Network
resolution uses the **local locator**: a whole-blob hit in `blob_locations`,
or an inner-block hit in `shard_inclusions` joined to its shard's location;
the retrieval is a ranged UCAN `content/retrieve` against the provider named
by the location commitment. The indexing-service query path is implemented
but unwired. The full trace is the
[GetObject diagram](./docs/diagrams.md#getobject-version-resolution-local-tiers-network-retrieval).

**Encrypted blobs** (the FilOne encryption design's read side) decrypt inside
the per-blob open, leaving every other read-path value plaintext: the
manifest's `BlobRef.Start/End`, `Body.Size`, ETag and Content-Length are
plaintext coordinates, while `BlobRef.Digest` names the stored FEE envelope.
A `blob_encryption_params` row marks a blob encrypted and carries what its
decryptor needs; the read unwraps the region-wrapped CEK through
`regionkey.Provider` (OpenBao transit in production, bound to the blob's
(space, digest)), maps the plaintext range to one contiguous ciphertext span
(`aesstream.CiphertextRange`), fetches only that span (ranged from the spool
or piri via `OpenBlobRange`), and decrypts it as it streams
(`aesstream.SpanReader`). A tampered chunk fails authentication mid-stream.
The encryption-params store and region key provider are required
dependencies; only the provider implementation (openbao vs inprocess) is
configuration.
HEAD never decrypts. See `s3frontend/decrypt.go`.

## Identity & auth

- **agent**: the libforge `identity.Identity` the host provides (daemon: the `identity.key_file` PEM
  key, wrapped as the `identity.service_id` did:web when set), the issuer of
  every outbound invocation to sprue, hilt, and piri. The agent's DID
  document is served at `/.well-known/did.json` on the S3 listener; peers
  resolve it to the signing key. The hilt→agent delegations and hilt's
  provider registration name the agent's DID, so changing it (did:key to
  did:web) re-issues both.
- **space**: per bucket, a `did:plc` minted by hilt at bucket create and
  stored on the bucket row; the subject of every blob and retrieve
  invocation.
- **access key**: the S3 access key ID is a `did:key`. Every non-root
  request is authorized through hilt (`/s3/request/authorize`, with a local
  fast path over cached delegations); hilt re-delegates the key's grant to
  the agent, and the per-key `DelegationCache` carries those proofs into the
  request via `internal/reqscope`, where the uploader and the network read
  tier spend them. The uploader also captures a per-space ship authority
  (1h TTL) for the async catalog flush.
- The **root account** (versitygw root credentials) bypasses hilt: bucket
  administration works, but with no proof store it can neither write to
  spaces nor read through the network tier.

The [principals diagram](./docs/diagrams.md#principals-and-proof-stores)
draws the chains and the stores.

## State & durability

- **Postgres (`ingot` schema)** is the mutable index: per-bucket roots and
  versioning state, segment metadata and op-roots, the blob claim ledger
  (`blob_refs`), locations and inclusions, upload intents, multipart
  sessions/parts/parks, and GC candidates. goose tracks its version at
  `ingot.goose_db_version` (never collides with a host's own migrations).
- **The MST is the data**: immutable, content-addressed, self-verifying,
  shipped to Forge as-is.
- A bucket is single-writer-correct via the per-bucket in-process lock plus
  the Postgres root CAS.

## Known gaps

- **No HA.** A bucket is single-writer through an in-process lock; nothing
  coordinates across instances beyond the root CAS.
- **The spool is unbounded** (#48): nothing evicts local body blobs, and
  DeleteObject releases network-side only, so local disk grows with every
  body byte written.
- **Spool crash recovery is not built**: reconciling `upload_intents`
  against `blob_refs` after a crash between commit and reconcile is a later
  phase; the window leaks rather than loses referenced data.
- **No catalog GC**: `gc_candidates` is write-only; superseded MST nodes
  accumulate on Forge with mutation volume.
- **Reads carry no bucket context**: `Manager.Get` linear-scans every open
  bucket store; threading the bucket into the read path removes the scan.
- **The ship authority expires**: the async flush signs with a per-space
  proof store captured at the last in-request write (1h TTL); a bucket idle
  longer than that cannot ship a newly sealed segment until its next write.
- **The indexer read path is unwired**: `LocalLocator` serves all reads; the
  network index is still published (best-effort) but never consulted.
- **Carried Forge-client copies**: `forgeclient/`, `tokenstore/`,
  `blockstore/locator/`, and `internal/ucanexec/` duplicate guppy/sprue code
  to stay cycle-free (ingot must never import guppy or sprue). A shared
  forge-client library would remove them.
