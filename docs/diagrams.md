# Ingot diagrams

These diagrams draw ingot **as it is built in this tree**. Where the code
deliberately stops short of the target architecture, the divergence appears as
a labeled annotation at the exact point (a dotted edge in a flowchart, a Note
in a sequence); the target itself lives in [`architecture.md`](./architecture.md).
Service names are lowercase (`ingot`, `sprue`, `piri`, `hilt`,
`indexing-service`), and external services are drawn dashed and opaque with
their contracts on the edges, so the diagram sets piri, sprue, and hilt
publish later can join on the same nodes. Each diagram ends with a `Sources:`
line naming its ground-truth files, and the
[package-to-diagram table](#package-to-diagram-map) at the bottom maps a code
change to the diagrams to re-check. The maintenance rule lives in
[`CLAUDE.md`](../CLAUDE.md).

| id | Diagram | Shows | Home |
|---|---|---|---|
| `context` | [System context](#system-context-services-and-contracts) | every service ingot talks to, and the contract on each edge | here |
| `packages` | [Package map](#package-map-and-interface-seams) | internal packages and the interface seams between them | here |
| `block-routes` | [Two block routes](#two-block-routes-body-blobs-and-catalog-blocks) | how body blobs and catalog blocks travel to Forge by different paths | here |
| `put` | [PutObject](#putobject-spool-and-upload-off-the-lock-commit-under-it) | the write path: off-lock ingest and upload, locked commit | here |
| `get` | [GetObject](#getobject-version-resolution-local-tiers-network-retrieval) | version resolution, tier fallthrough, network retrieval | here |
| `multipart` | [Multipart upload](#multipart-upload-park-on-write-accept-in-the-background) | park at UploadPart, accept in the background, Complete's wait, abort unwind | here |
| `part-states` | [Part states](#part-states-the-acceptance-queue) | the part rows as the acceptor's queue | here |
| `multipart-states` | [Session states](#session-states-and-the-completeabort-latch) | the session state machine and the single-winner latch | here |
| `blob-lifecycle` | [Blob lifecycle](#blob-lifecycle-spooled-parked-accepted-released) | intent states and the claim ledger gating deletion | here |
| `version-tree` | [Per-key version storage](#per-key-version-storage-manifest-arm-leaf-arm-prev-tree) | the value union, the leaf, the prev tree | here |
| `gc-candidates` | [Catalog GC candidates](#catalog-gc-candidates-what-gets-remembered-for-removal) | every path that records a superseded catalog block for removal | here |
| `principals` | [Principals and proof stores](#principals-and-proof-stores) | every DID in play and which store proves what | here |
| `authorize` | [Request authorization](#request-authorization-and-proof-capture) | the per-request hilt authorize flow and proof capture | here |
| `schema` | [Postgres schema](#postgres-schema-as-migrated) | the `ingot` schema's tables and joins | here |
| `delete-bucket` | [DeleteBucket teardown](#deletebucket-teardown-order) | the teardown ordering across every seam | here |
| `logstore-pipeline` | Catalog pipeline | append, seal, ship, guarded root advance | [`logstore/README.md`](../logstore/README.md#write-path) |
| `segment-states` | Segment lifecycle | open, sealed, shipped, retired, recovery | [`logstore/README.md`](../logstore/README.md#segment-lifecycle) |

## System context: services and contracts

Ingot is an S3 gateway over the Forge network: versitygw serves S3 REST
(`:8080` by default; `:80` in the smelt stack, where `did:web:ingot` resolves
to the listener's `/.well-known/did.json`), hilt authorizes requests and owns
tenancy, sprue brokers every blob
operation, and piri stores and serves the bytes. The indexing-service read
path is designed but unwired; a local locator (over `blob_locations` and
`shard_inclusions`) serves reads instead.

```mermaid
flowchart LR
    client["S3 client<br/>SigV4, path-style"]
    ingot["ingot<br/>S3 gateway, /health, /.well-known/did.json<br/>did:web:ingot"]
    hilt["hilt<br/>auth + tenant service<br/>did:web:hilt"]
    sprue["sprue<br/>upload service<br/>did:web:upload"]
    piri["piri<br/>storage node<br/>provider DID from location commitments"]
    pg[("postgres<br/>ingot schema")]
    idx["indexing-service"]

    client -->|"S3 REST"| ingot
    ingot -->|"/s3/request/authorize (every request on a local-cache miss)<br/>/s3/bucket/info (lazy chain completion)<br/>/s3/bucket/create, delete, list<br/>/s3/bucket/policy (?policy, the signed request)"| hilt
    ingot -->|"/blob/add, /ucan/conclude, GET /receipt/:task<br/>/blob/abort, /blob/remove, /index/add<br/>/upload/add, /upload/remove"| sprue
    ingot -->|"HTTP PUT blob bytes (allocated URL)"| piri
    ingot -->|"content/retrieve (UCAN, on read miss)"| piri
    ingot -->|"pgx + goose migrations"| pg
    ingot -.->|"QueryClaims: designed, unwired;<br/>LocalLocator serves reads"| idx
    sprue -->|"/blob/allocate, /blob/accept<br/>/blob/release, /blob/reject"| piri
    sprue -->|"republishes /index/add"| idx

    classDef external stroke-dasharray: 5 5
    class hilt,sprue,piri,pg,idx external
```

- Ingot never invokes `/blob/accept`: sprue owns accept (and allocate), which
  is why the conclude call carries no space proof.
- Versitygw's root account is disabled (no `WithRootUser` option), so every
  access key resolves through `iam.Service`. A plain (non-`did:key`) access
  key, the shape a root credential would have, is rejected there as
  InvalidAccessKeyId before any hilt round-trip.
- `ListBuckets` is served entirely from hilt; the local `ingot.buckets` table
  backs every other verb.

Cross-references: [`architecture.md` §9](./architecture.md#9-the-system-contract-piri--sprue--indexer)
(the system contract), [§12](./architecture.md#12-implementation-status--postponed-items)
(implementation status).

Sources: `module.go`, `bucketauthority/service.go`, `iam/service.go`,
`s3frontend/policy.go`, `forgeclient/`, `uploader/blob.go`, `blockstore/forge.go`.
Review when these change.

## Package map and interface seams

An edge follows the dependency direction; a label names the interface seam
crossed where one stands between the packages. To add an implementation,
satisfy the seam and rewire it in `module.go`.

```mermaid
flowchart TB
    vgw["versitygw s3api<br/>(S3 REST front end)"]

    subgraph edge["S3 edge"]
        s3f["s3frontend"]
        iam["iam"]
        bop["bucketop<br/>Coordinator, Tx"]
    end

    subgraph model["catalog model"]
        bkt["bucket<br/>ObjectManifest, ObjectLeaf, ValueUnion"]
        mst["mst<br/>forked MST"]
    end

    subgraph store["local storage"]
        bs["blockstore<br/>Spool, BlobCache, OpStaging, Layered, Cached, Forge"]
        ls["logstore<br/>Manager, Store, PlaneLog, Segment"]
        cars["cars"]
    end

    subgraph meta["metadata"]
        reg["registry<br/>Postgres, LocalLocator"]
        mig["migrations"]
    end

    subgraph net["network clients"]
        up["uploader<br/>Forge"]
        fc["forgeclient<br/>sprue edge client"]
        ba["bucketauthority<br/>hilt bucket ops"]
    end

    subgraph host["host"]
        root["ingot (root)<br/>Server, Module"]
        cmd["cmd<br/>serve, whoami, version, blake3"]
    end

    vgw -->|"backend.Backend"| s3f
    vgw -->|"auth.IAMService +<br/>RequestIAMService"| iam
    s3f --> bop
    s3f --> bkt
    s3f --> reg
    s3f --> up
    s3f --> ba
    bop -->|"blockstore.Log"| ls
    bop -.->|"bucketop.ShipAuthority<br/>(uploader.Forge injected)"| up
    bop --> mst
    mst --> bs
    bkt --> bs
    ls --> cars
    ls -->|"logstore.Meta"| reg
    bs -.->|"locator.Locator<br/>(LocalLocator injected)"| reg
    up --> fc
    iam --> reg
    reg --> mig
    cmd --> root
    root --> vgw
    root -->|"BlockReader =<br/>Cached(Forge)"| bs
```

- `registry.Postgres` is one struct satisfying the registry seams
  (`Registry` plus the intent, location, inclusion, blob-ref, GC, multipart,
  and park stores) and `logstore.Meta`.
- `uploader.Forge` is one struct behind `Uploader`, `BodyUploader`,
  `DeferredBodyUploader`, and `BlobRemover`.
- `inmem` (test-only fakes) and `tokenstore` (empty; only the dormant login
  paths would read it) are omitted. `blockstore/locator`'s indexer-backed
  locator compiles but is never injected.
- `iam` and the network tiers never import each other; they meet at
  `internal/reqscope` (the request-scoped proof store, drawn in
  [principals](#principals-and-proof-stores)).

Sources: the `go list` import graph, `module.go`, `server.go`,
`s3frontend/backend.go`. Review on package add/remove or `module.go` provider
changes.

## Two block routes: body blobs and catalog blocks

A write produces two kinds of blocks, and they reach Forge by different
routes. Body blobs travel synchronously, each as its own network object,
before the commit; catalog blocks (MST nodes, manifests, leaf blocks) commit
locally first and ship later, packed into shared CAR segments.

```mermaid
flowchart TB
    put["PutObject / UploadPart body"]

    subgraph bodyr["the body route (raw blobs, synchronous)"]
        split["SplitSizedBody at the declared length:<br/>coarse split at max_blob_size,<br/>BLAKE3 tree + md5 in one streaming pass"]
        spool["Spool (DataDir/spool)<br/>+ upload_intents row<br/>(each blob streams its PUT<br/>to the provider while it is spooled)"]
        upload["per-blob upload before the commit:<br/>conclude, accept (streamed), or<br/>/blob/add, HTTP PUT, conclude, accept<br/>(a blob_locations hit skips it: dedup)"]
        bloc["blob_locations row: the whole blob,<br/>(space, digest) to provider URL"]
        cache["BlobCache (DataDir/cache):<br/>the blob's file moves here by rename"]
        split --> spool --> upload --> bloc --> cache
    end

    subgraph catr["the catalog route (dag-cbor blocks, asynchronous ship)"]
        commit["commit: manifest + MST splice into OpStaging,<br/>one fsynced AppendBatch into the bucket's log,<br/>guarded root swap journaling the new blocks<br/>in Postgres (catalog_journal), 200 to the client"]
        seal["open segment seals at SealBytes / SealAge<br/>(CAR + .idx + .ops)"]
        ship["background ship: /blob/add the sealed CAR,<br/>PUT, conclude, accept"]
        incl["blob_locations (the CAR) + shard_inclusions<br/>(inner byte ranges), then the guarded<br/>forge_root_cid advance, the segment's journal<br/>rows deleted, and retention"]
        commit --> seal --> ship --> incl
    end

    piri["piri: bytes stored by digest"]

    put --> split
    bloc -->|"every body blob durable and accepted"| commit
    upload --> piri
    ship --> piri

    classDef external stroke-dasharray: 5 5
    class piri external
```

- The `200` means different things per route: body blobs are durable and
  accepted on Forge at ack; the catalog is fsynced locally and journaled in
  Postgres at ack, and becomes durable on Forge only when its segment ships
  (`forge_root_cid` lags `root_cid` until then). The journal is what puts the
  unshipped catalog in any copy of Postgres; shipping the segment deletes its
  rows.
- The ordering is the crash-safety invariant: the catalog route starts only
  after every body blob is durable, so a crash never leaves a catalog entry
  pointing at non-durable data.
- The network unit differs: a body blob is retrieved whole by its own digest
  (a `blob_locations` hit); catalog blocks share a CAR, so a network read
  resolves `shard_inclusions` to a byte range inside the shard.
- Reads mirror the split: `OpenBlob` (bodies) checks local disk (cache,
  then spool) then network, skipping the log; `GetBlock` (catalog) checks
  local disk, log, then network
  (the [GetObject diagram](#getobject-version-resolution-local-tiers-network-retrieval)).
- Removal mirrors it too: bodies are reference-counted
  ([blob lifecycle](#blob-lifecycle-spooled-parked-accepted-released));
  superseded catalog blocks queue for a future collector
  ([catalog GC candidates](#catalog-gc-candidates-what-gets-remembered-for-removal)).
- A body streams: each blob is allocated by size
  and hash function (`/blob/add` with a digest code) and its envelope goes to
  the provider while the spool writes it, so the upload overlaps the body's
  arrival instead of following it. The spool copy still supplies the digest,
  which the put receipt reports and the node checks at accept.
- Multipart parts ride the body route with the conclude deferred (parked);
  the upload's acceptor finishes it
  (the [multipart diagram](#multipart-upload-park-on-write-accept-in-the-background)).

Cross-references: [`architecture.md` §4](./architecture.md#4-the-catalog-layer),
[§5](./architecture.md#5-the-data-layer);
[`logstore/README.md`](../logstore/README.md).

Sources: `s3frontend/object.go` (ingestBody), `s3frontend/stream.go`,
`bucket/sized.go`, `blockstore/spool.go`, `blockstore/blobcache.go`,
`blockstore/staging.go`, `logstore/`, `uploader/forge.go`, `uploader/blob.go`,
`server.go` (newBucketFlushFunc). Review when these change.

## PutObject: spool and upload off the lock, commit under it

All network I/O runs outside the per-bucket lock; the critical section is a
manifest write, an MST splice, one fsynced `AppendBatch`, and a guarded root
swap. A `200` means the body is durable and accepted on the network, not
merely buffered.

```mermaid
sequenceDiagram
    autonumber
    actor C as S3 client
    participant B as s3frontend.Backend
    participant SP as blockstore.Spool
    participant R as registry<br/>(Postgres)
    participant U as sprue
    participant P as piri
    participant TX as bucketop.Tx
    participant L as logstore.Manager

    Note over C,L: off the lock: ingest, hash, encrypt, upload (an upload service that cannot<br/>add by digest code gets every blob spooled first, then uploaded by digest)
    C->>B: PutObject(bucket, key, body)
    B->>R: reg.Get(bucket), precondition pre-check
    B->>B: tenant recipient: resolve the tenant's #35;wrap key<br/>(tenant DID from the request, did:plc doc via the cached PLC resolver)#59;<br/>no recipient → the write fails
    loop SplitSizedBody: each plaintext piece of min(max_blob_size, remaining) bytes
        B->>B: fresh CEK → FEE envelope (COSE_Encrypt, AES-256-GCM STREAM,<br/>one recipient: ECDH-ES+A256KW to the tenant wrap key)#59;<br/>its length is known from the header + plaintext length
        B->>U: /blob/add (digestCode sha2-256, envelope size, random nonce)
        U-->>B: allocation address, put and accept tasks
        B->>R: PutStream(add task): the upload's name until parked or accepted
        par
            B->>SP: spool under the CIPHERTEXT digest<br/>(BLAKE3 tree + md5 of the plaintext in the same pass)
        and
            B->>P: HTTP PUT the same envelope bytes as they are spooled
        end
        Note over B,P: a failed PUT detaches: the spool finishes, the allocation is<br/>aborted and the blob uploads by digest#59; UnsupportedDigestCode<br/>switches the rest of the body to spool-first
    end
    B->>R: PutIntent(digest, stored size, uploading) +<br/>PutEncryptionParams(region-wrapped CEK, FEE geometry) per blob
    loop each streamed blob (uploadBlobs → concludeStreamed)
        B->>U: /ucan/conclude the put receipt, reporting the spooled digest
        U-->>B: accept receipt + /assert/location commitment
        B->>R: PutLocation, then SetIntentState(accepted)#59; DeleteStream
        B->>B: BlobCache.Take: the file moves from the spool to the cache
    end
    loop each spooled-first blob (uploadBlobs)
        alt blob_locations already has (space, digest)
            B->>R: SetIntentState(accepted), skip upload, BlobCache.Take<br/>(never hits for fresh writes — every envelope digest is new)
        else upload
            B->>U: /blob/add (digest, size)
            U-->>B: allocation address (none on provider-side dedup)
            B->>P: HTTP PUT bytes
            B->>U: /ucan/conclude the put receipt, then poll /blob/accept receipt
            U-->>B: /assert/location commitment
            B->>R: PutLocation, then SetIntentState(accepted)
            B->>B: BlobCache.Take: the file moves from the spool to the cache
        end
    end
    Note over B,U: UploadBlob also captures the request proof store as the<br/>space's ship authority (captureShipProofs, 1h TTL)
    Note over B,L: under the per-bucket lock: commitVersion via Coordinator.WithTx
    B->>TX: Begin (lock, snapshot root)
    TX->>R: AllocVersionSeq (a multipart Complete in an Enabled bucket<br/>reserved its seq before the lock: see the multipart diagram)
    TX->>TX: mint versionId ("null" unless versioning Enabled),<br/>manifest into OpStaging, re-check If-Match under the lock,<br/>MST splice (new nodes into staging)
    Note over TX: uploader.CaptureShipAuthority: every commit captures the request<br/>proof store as the space's ship authority, so a write with no body<br/>(a delete, a tag change) leaves one for its segment's ship
    TX->>L: AppendBatch(catalog blocks, OpRoot) with fsync
    TX->>R: CommitRoot(bucket, snapshot, newRoot), guarded, one transaction:<br/>the batch's catalog blocks journaled under its segment (catalog_journal,<br/>skipped once the segment has shipped), blob_refs gains this version,<br/>superseded version rows removed, a release record per digest whose<br/>claims reached 0#59;<br/>/upload/add(root = this version's manifest CID)<br/>+ /upload/remove per discarded version queued (upload_registrations)
    TX-->>B: committed, lock released
    B-->>C: 200 + ETag (+ x-amz-version-id when versioning is configured)
    Note over B,U: the release sweeper runs each record once its grace passes:<br/>/blob/remove (+ crypto-shred: its blob_encryption_params row deleted)
```

- A request that dies after allocating a streamed blob leaves its
  `blob_streams` row; the stream sweeper (on the release sweeper's tick)
  aborts such an upload by its add task once the row's lease (`touched_at`,
  renewed every 10 minutes by the request that owns it) has gone 30 minutes
  unrenewed, unless its blob was parked, and
  drops the row once the abort lands or the provider reports the blob
  accepted.
- Encryption makes the stored digest a ciphertext digest: **content dedup is
  gone for bodies** (fresh CEK per write ⇒ unique envelope), by design per
  the encryption RFC. Manifest spans, `Body.Size`, the BLAKE3 tree, md5 and ETag stay
  plaintext values; `upload_intents.Size` and `blob_locations.Size` are
  stored (envelope) sizes.
- The envelope's one COSE recipient is the tenant wrap key (kid = the key's
  fingerprint), the RFC's insurance copy: the tenant's private key alone
  recovers the plaintext. The IAM layer stashes the tenant DID from Hilt's
  authorize response on the request; `tenantkey` resolves the `#wrap` key
  from the tenant's did:plc document.
- The supersession rule (which prior version is retained, replaced, or
  discarded) is [`s3-versioning.md`](./s3-versioning.md) §5; the resulting
  storage shape is the [version tree](#per-key-version-storage-manifest-arm-leaf-arm-prev-tree).
- The claim ledger and the zero-claims release are the
  [blob lifecycle](#blob-lifecycle-spooled-parked-accepted-released).
- `CopyObject` runs the same `commitVersion`. Within one space its manifest
  pins the source's digests: no spool, no upload, claims incremented. Across
  spaces (every bucket has its own) the CEK wrap bound to (space, digest)
  rules out sharing, so the source's plaintext streams through the decrypting
  read path into `ingestBody` and the copy gets its own blobs and claims. A
  source bucket owned by another tenant is `AccessDenied` before any key
  lookup (hilt refuses it when authorizing the request; ingot compares the
  tenant on the two bucket rows as well). The copy's ETag is the md5 of its
  bytes even for a multipart source.
- Supersession also records each replaced catalog block for future removal:
  the [catalog GC candidates](#catalog-gc-candidates-what-gets-remembered-for-removal)
  diagram shows every entry path.
- The `/upload/add` is what the upload service counts to report the space's
  object count: one content entry per committed version, keyed by the manifest
  CID. A discarded version retracts, a retained noncurrent one does not, and a
  delete marker registers like any other version — the same set AWS reports for
  `NumberOfObjects`. Best-effort, as the index publication is: the object is
  durable and the response must say so, so a failure logs and the count
  under-reports rather than failing a write that succeeded.
- The write does **not** call the upload service. It queues the change in
  `ingot.upload_registrations` in the same transaction as the root CAS, and the
  registration sweeper (`SweepUploadRegistrations`, every 30s) applies it. The
  count is reporting data, so a Sprue round trip does not belong in a PUT, and
  a Sprue outage delays the count instead of failing writes or losing it.
- The row exists exactly when the version committed, and `seq` replays a key's
  changes in commit order. A sweep spends **two round trips**, not one per
  change: every queued addition in one batch of invocations, then every
  retraction in another. (A batch in ucantone's sense — many invocations, a
  receipt each. `/ucan/conclude` is the other shape, one invocation carrying
  many receipts.)
  Both halves are needed — a container sorts its tokens bytewise, so the
  service may run a batch in any order, and a retraction sharing a batch with
  the addition it retires could overtake it and leave the root counted for
  good. Splitting by op is safe because a root is added by the commit that
  creates the version and retracted by a later one, so an addition always
  carries the lower `seq`.
- A key with a refused change is held whole until it is due again, within the
  sweep and across sweeps, so a later row can never overtake an earlier one.
  Each row carries the delegation chain that authorizes it, captured from the
  request that committed the version, because the sweeper has no request to
  borrow authority from.
- That chain has room to spare — hilt expires its delegation to the gateway at
  the next UTC midnight plus an async overhang, so even a change queued at
  23:59 has hours to be drained in. For the queue that outlasts that, the
  sweeper renews from the space's live authority, which a recent write leaves
  behind. A space that has gone quiet has none, and after
  `uploadRegistrationDeadLetterAfter` attempts the row is **dead-lettered**:
  out of the sweep, and out of the way of its key's later rows. (Nothing to do
  with a parked blob, which is durable and awaiting its accept.) The count is then short by
  that one change rather than frozen for that key, and `dead_letter_reason` records
  why.

Cross-references: [`architecture.md` §7.1](./architecture.md#71-write-single-shot-putobject).

Sources: `s3frontend/object.go` (PutObject, ingestBody, uploadBlobs,
concludeStreamed), `s3frontend/stream.go` (WriteSizedBlob,
SweepStaleStreams), `bucket/sized.go`, `uploader/stream.go`,
`s3frontend/copy.go` (CopyObject, copySourceBucket),
`s3frontend/version.go` (commitVersion, enqueueRegistration),
`s3frontend/uploadsweep.go` (SweepUploadRegistrations), `bucketop/bucketop.go`,
`blockstore/staging.go`, `uploader/blob.go`, `uploader/forge.go`,
`uploader/upload.go`. Review when these change.

## GetObject: version resolution, local tiers, network retrieval

A read resolves the version through the MST, then serves each covering blob
from the first tier that has it: local disk (the cache, then the spool), the
catalog log (catalog blocks only), then the network. Network resolution goes through the local locator
tables, never the indexing-service.

```mermaid
sequenceDiagram
    autonumber
    actor C as S3 client
    participant B as s3frontend.Backend
    participant R as registry<br/>(Postgres)
    participant LY as blockstore.Layered
    participant LG as logstore.Manager
    participant LC as registry.LocalLocator
    participant K as regionkey.Provider<br/>(OpenBao / in-process)
    participant P as piri

    C->>B: GetObject(bucket, key, versionId?)
    B->>R: reg.Get(bucket): space, root, versioning
    B->>LY: resolveVersion: MST get(key), decode the value union
    alt manifest-valued key
        Note over B,LY: the value block is the manifest<br/>(answers every versionId class)
    else leaf-valued key
        Note over B,LY: Current for the head, otherwise seek the prev tree at<br/>revSeqKey(seq) and confirm the stored VersionID (see version-tree)
    end
    Note over B: delete marker: 404 NoSuchKey (current) or 405 (versioned),<br/>then preconditions and selectBytes (Range, or partNumber via Body.PartSizes)
    B->>R: bodyOpener: unless the manifest marks the body plaintext,<br/>blob_encryption_params + blob_locations for the blobs the range covers,<br/>one query each (a missing row fails the read)
    loop each covering blob or catalog block
        opt encrypted blob (FEE)
            B->>K: Unwrap region-wrapped CEK, bound to (space, digest)
            Note over B: aesstream.CiphertextRange maps the plaintext range to<br/>one contiguous ciphertext span past the envelope header
        end
        B->>LY: read (whole blob, or only the ciphertext span via OpenBlobRange)
        alt local hit (blockstore.LocalBlobs)
            LY-->>B: bytes from the cache, else the spool<br/>(then the cache again: a blob can move between the two)
        else catalog log hit (GetBlock only)
            LY->>LG: Get: linear scan of open bucket stores
            LG-->>B: block from an open or sealed segment
        else network (blockstore.Forge)
            LY->>LC: Locate(space, digest)
            alt whole blob in blob_locations
                LC-->>LY: provider URL + whole-blob range
            else inner block via shard_inclusions
                LC-->>LY: shard location + inclusive byte range
            end
            LY->>P: content/retrieve (UCAN, audience = the commitment's provider,<br/>proofs from reqscope.ProofStore#59;<br/>narrowed to the ciphertext span for an encrypted blob)
            P-->>LY: ranged bytes, length-checked
        end
        opt encrypted blob (FEE)
            Note over B: aesstream.SpanReader decrypts the span as it streams#59;<br/>a tampered chunk fails authentication mid-stream (ErrCorrupted)
        end
    end
    B-->>C: 200 or 206 body (plaintext byte counts throughout)
```

- `OpenBlob` (body blobs) checks local disk then network; only `GetBlock`
  (catalog blocks) consults the log tier, and only `GetBlock` is fronted by
  the `Cached` LRU.
- The indexer-backed locator (`blockstore/locator`) compiles but is never
  injected; `module.go` always wires `LocalLocator`.
- Manifest coordinates are plaintext: blob offsets, `Body.Size`, ETag and
  Content-Length never change with encryption. A blob's digest names the
  stored — for an encrypted blob, ciphertext — bytes; only the per-blob open
  translates between the two.
- The covering blobs come from the manifest's inline list, or from the one
  extent block whose offset range holds the read position (see the version
  storage diagram).
- HEAD needs no decryption at all (every value it reports is a plaintext
  manifest field).

Cross-references: [`architecture.md` §7.4](./architecture.md#74-read-getobject),
[§8](./architecture.md#8-retrieval-addressing-when-bodies-need-a-sharded-dag-index).

Sources: `s3frontend/object.go` (GetObject, HeadObject, selectBytes),
`s3frontend/version.go` (resolveVersion), `s3frontend/decrypt.go`
(bodyOpener, decryptingOpener), `bucket/chunker.go` (BlobRangeOpener),
`blockstore/layered.go`, `blockstore/blobcache.go` (LocalBlobs),
`blockstore/forge.go` (doRetrieve), `blockstore/cache.go`,
`registry/locallocator.go`, `logstore/manager.go` (Get). Review when these
change.

## Multipart upload: park on write, accept in the background

Parts upload to the provider at `UploadPart` and park there (durable,
unaccepted). Each upload's acceptor concludes its parked parts in batches
while the client is still uploading, so `Complete` waits only for the last
few and its cost stays flat as the part count grows.

```mermaid
sequenceDiagram
    autonumber
    actor C as S3 client
    participant B as s3frontend.Backend
    participant A as the upload's acceptor<br/>(goroutine)
    participant R as registry<br/>(sessions, parts, parks, intents)
    participant U as sprue
    participant P as piri
    participant TX as bucketop.Tx

    C->>B: CreateMultipartUpload
    B->>R: CreateSession(open) with headers + checksum algorithm
    B-->>C: uploadId
    C->>B: UploadPart(n) / UploadPartCopy(n)
    B->>B: openSession (non-open: NoSuchUpload)
    opt UploadPartCopy
        B->>B: vet the source: copySourceBucket (tenant), resolveVersionIn,<br/>range within the object, copy-source preconditions (412)#59;<br/>the body is the source's plaintext range through the decrypting reader
    end
    B->>B: ingestPart: splitSpool<br/>(resolve the tenant recipient, then encrypt per piece:<br/>fresh CEK → FEE envelope → spool under the ciphertext<br/>digest + params row, as in the PutObject diagram)
    B->>R: PutPart(spooled, span_lengths): open sessions only, the row held FOR SHARE<br/>against a concurrent latch#59; writes of one part number serialised on an advisory lock#59;<br/>a release record, due now, for each blob the replaced row named and this part<br/>does not, in the same transaction, returned to run#59; a refused part records its blobs' releases
    loop each part blob (parkBlobs)
        alt streamed while spooled
            B->>R: PutPark(AddTask, AcceptTask, PutInvocation), intent parked,<br/>DeleteStream: the PUT already went to piri in splitSpool
            B->>B: removeLocal (the part's local copy#59; the acceptor concludes from the park)
        else blob_locations already has the digest
            B->>R: intent accepted (dedup, no park)
        else already parked (GetPark hit)
            B->>R: reuse the park
        else park
            B->>U: /blob/add with WithConclude(false)
            B->>P: HTTP PUT bytes
            B->>R: PutPark(AddTask, AcceptTask, PutInvocation), intent parked
            B->>B: removeLocal (the part's local copy)
        end
    end
    B->>R: MoveParts(spooled to parked), guarded by the part's digests
    opt no row moved: a later upload of the part number replaced it,<br/>or a teardown took the session, while the blobs were parking
        B->>R: EnqueueReleases of this part's blobs: a no-op beside the replacer's record,<br/>which the park left standing (awaitRelease)#59; they run below rather than on the next sweep
    end
    B->>A: nudge (start the acceptor, or ask for another pass)
    B->>U: releaseNow: the records above, due at once (/blob/abort of the parks#59;<br/>a blob another part or the session still names waits on that reference)
    B-->>C: part ETag (part md5#59; for a copy, of the copied bytes)
    loop while a pass finds parked parts
        A->>R: GetSession (stop unless open or completing)#59; ListParkedParts
        A->>U: acceptParked: up to 1000 blobs' put receipts in one /ucan/conclude,<br/>accept receipts read from the response (poll only as fallback)
        A->>R: per blob, under its write lock: ConcludePark (park swapped for the<br/>location, only while the concluded park, by AddTask, stands) + intent accepted
        A->>R: MoveParts(parked to accepted)
    end
    Note over A,R: a failed pass backs off (1 s to 30 s) and retries#59; the accept sweeper<br/>nudges every live upload with parked parts at start and every 30 s
    C->>B: CompleteMultipartUpload(parts)
    B->>B: validate parts (ascending, ETags, checksums, MinPartSize)#59;<br/>an error here is answered with its own status
    alt session already completed
        B-->>C: report the version id recorded on the session (none for a null version),<br/>then its ETag (idempotent re-Complete)
    else another Complete holds the latch
        B-->>C: report no version id (the winner's isn't known yet), then wait (bounded):<br/>replay its result, or take the latch if it failed
    else latch won
        B->>R: LatchSession(open to completing), single winner
        B->>R: ListPartTrees#59; re-hash over budget: 400 InvalidPartLayout,<br/>the deferred revert reopens the session
        alt versioning Enabled, nothing reported yet
            B->>R: reserveVersion: AllocVersionSeq off the lock, mint the versionId
            B-->>C: report it: versitygw commits the 200 with x-amz-version-id<br/>and x-amz-server-side-encryption
        else
            B-->>C: report no version id: the 200 commits with x-amz-server-side-encryption only
        end
        Note over B,C: from here the body is padded with a newline every<br/>complete_multipart_keepalive (5 s), and any error goes in the 200's body
        B->>R: manifest spans from each part's span_lengths<br/>(rows without them: intent size + FEE geometry)
        B->>A: awaitPass until no named part is parked, up to acceptWaitBudget (30 s)
        B->>R: ListParts: the named parts' states
        alt a named part still parked
            B-->>C: OperationAborted in the 200's body#59; the deferred revert reopens the session
        else every part accepted, or spooled
            B->>U: spooled parts only: concludeBlobs (conclude, or upload a never-parked blob)
            B->>TX: commitVersionAt under what was reported (see the PutObject diagram)#59;<br/>refused (errReservationSuperseded) if the key gained a later seq,<br/>or the versioning no longer fits the report#59; the claims land<br/>in the root swap's transaction, set-based
            B->>R: LatchSession(completing to completed), best-effort
            B-->>C: the result ends the body: ETag = md5-of-part-md5s + "-N"
        end
    end
    C->>B: AbortMultipartUpload
    B->>R: LatchSession(open to aborting)#59; EnqueueReleases for every<br/>unreferenced part blob#59; then DeleteSession (parts cascade)
    B->>U: releaseNow, per record: crypto-shred, then /blob/abort a parked blob<br/>(cause = AddTask) or /blob/remove an accepted one#59; local rows dropped<br/>once the network step succeeds#59; the release sweeper retries the rest
    Note over B,R: a background sweeper tears down sessions whose state has not<br/>changed for MultipartSessionTTL (default 7d)#59; a Complete's latch restarts the clock
```

- A part re-upload and an abort record a release for every blob of theirs
  that is not still live in the session; the release itself decides whether
  the blob is free to go — a claimed digest drops the record (`CountClaims`),
  and one a part of an in-flight session references waits
  (`CountLivePartRefs`, checked first: a Complete claims before it leaves
  'completing'). A commit's claim publishes the blob's upload intent, so
  the reap of a retained session releases only what was never committed.
- A release can take a blob while the acceptor concludes it. The provider
  refuses whichever of accept and abort comes second, and the release drops
  the park row before anything else, so `ConcludePark` finds no park and the
  acceptor records nothing for a blob a release took. `ConcludePark`
  matches the park's `/blob/add` task, so a later park of the same bytes
  stays for its own conclude.
- The acceptor never cancels a release record (`ReleasingDigests`, read
  only); a part holding such a blob goes back to `spooled` for Complete.
  Each pass runs under `acceptPassTimeout`.
- No release runs while its blob's conclude is in flight: sprue registers
  an accepted blob only when it answers, so an earlier remove would miss it.
  A teardown waits for the acceptor's pass in flight (`quiesceAcceptor`)
  before it records releases; any other release defers while the blob is
  marked (`markConcluding`, held 2 min past a failed conclude) and runs from
  the release sweeper.
- A part is written only while its session is open, in the statement that
  checks the state; a teardown that took the session refuses it, and the
  upload records releases for the blobs it had spooled.
- A never-parked blob at Complete falls back to a full synchronous
  `UploadBlob`, after rechecking for a park under the blob's lock: an
  UploadPart still parking the blob when Complete classified it holds that
  lock, and its park is concluded rather than allocated beside.
- Complete reports its response headers through versitygw's
  `CompleteMultipartUploadReportingHeaders` before it accepts the parts,
  and the response commits to 200 at the report, as S3's does. A reserved
  version id is binding, and a Complete that reported none commits only a
  null version, so the client always learns the version it created
  ([`s3-versioning.md`](./s3-versioning.md) §5.1).

Cross-references: [`architecture.md` §7.2](./architecture.md#72-multipart),
[§7.3](./architecture.md#73-the-session-latch-the-abortcomplete-race).

Sources: `s3frontend/multipart.go` (all verbs, ingestPart, parkBlobs,
recordStreamedPark, acceptRequested, acceptParked, recordConcluded,
concludeBlobs, enqueuePartReleases, SweepStaleMultipartSessions),
`s3frontend/version.go` (reserveVersion, commitVersionAt),
`s3frontend/acceptor.go`, `s3frontend/stream.go`,
`s3frontend/object.go` (runRelease, executeRelease),
`s3frontend/uploadpartcopy.go`, `registry/stores.go`,
`server.go` (startMultipartSweeper, startAcceptSweeper). Review when these change.

## Part states: the acceptance queue

A part row's `state` is the upload's acceptance queue. Every move is a
guarded update that also matches the part's `blob_digests`, so a request
never moves a row a later upload of the same part number replaced.

```mermaid
stateDiagram-v2
    [*] --> spooled : PutPart (UploadPart, before its blobs park)
    spooled --> parked : every blob parked or located (MoveParts, then nudge)
    parked --> accepted : acceptor pass concludes and records the blobs
    parked --> spooled : acceptor finds a blob with a release record, or neither park nor location
    spooled --> accepted : Complete finishes it (conclude, or upload from the spool)
    parked --> accepted : Complete, when no acceptors run
    spooled --> [*] : a later UploadPart of the same number replaces the row#59;<br/>the blobs it drops get release records in the same transaction
    parked --> [*] : replaced (releases recorded with the replacement),<br/>or the session's teardown cascades
    accepted --> [*] : replaced (releases recorded with the replacement),<br/>or the session's teardown cascades
```

- `spooled` is the state of a part whose request is still parking its
  blobs, or died doing so. The acceptor never takes it; Complete does.
- A request whose `spooled` row is replaced while it parks moves no row to
  `parked`. The releases the replacement recorded stand: a part's park
  leaves a pending release of its digest to the part row, which defers it
  while the row stands and lets it run once the row is replaced, so the
  blobs go even if the request dies there. The request runs them itself
  when it can, rather than leave them to the next sweep.
- Complete requires every part it names to be `accepted` before the commit:
  it waits until none of them is queued (parts it omits are not waited
  for), finishes `spooled` parts itself, and answers a part still `parked`
  with a 409 that botocore clients treat as final. A failed pass accepts
  every part of its batch but those holding a failed blob.
- Rows from before migration 00030 are `parked` or `accepted` and have no
  `span_lengths`; Complete derives their lengths per blob.

Sources: `registry/stores.go` (Part* consts, MoveParts, ListParkedParts,
ListUploadsWithParkedParts), `registry/stores_postgres.go`,
`s3frontend/acceptor.go`, `s3frontend/multipart.go` (ingestPart,
acceptRequested), `migrations/sql/00030_part_accept_queue.sql`. Review when
these change.

## Session states and the Complete/Abort latch

The latch is one guarded update: `UPDATE multipart_sessions SET state = $3
WHERE upload_id = $1 AND state = $2`; exactly one caller sees
`RowsAffected == 1`.

```mermaid
stateDiagram-v2
    [*] --> open : CreateSession
    open --> completing : Complete wins the latch
    completing --> open : pre-commit failure (deferred revert)
    completing --> completed : commit ok (best-effort stamp)
    open --> aborting : Abort, DeleteBucket implicit abort, or sweeper
    aborting --> [*] : EnqueueReleases, then DeleteSession (parts cascade)
    completed --> [*] : sweeper past TTL
```

- A Complete that loses the latch reports no version id, then waits
  (bounded) for the winner's terminal state: `completed` replays the ETag
  and version id the winner recorded on the session; `aborting` or a
  vanished row is `NoSuchUpload`; a winner still running past the wait
  budget is `OperationAborted`, inside the 200's body since the headers
  have gone out. A winner that fails reopens the session, and the waiting
  Complete may then take the latch; in an Enabled bucket its commit is
  refused, since it reported no id, and the client's retry reports one.
- `ListParts` and `Abort` reject any non-`open` session as `NoSuchUpload`;
  `Complete` alone accepts `completed`, for idempotency.
- The sweeper also reaps rows stuck in `completing` or `aborting` past the
  TTL.

Sources: `s3frontend/multipart.go`, `registry/stores_postgres.go`
(LatchSession). Review when these change.

## Blob lifecycle: spooled, parked, accepted, released

Dedup stores bytes once; the claim ledger lets them be deleted once. Intents
track the disk-and-network state of each digest; `blob_refs` counts which
versions still reference it.

```mermaid
flowchart TB
    W["spool write<br/>(PutObject ingest, UploadPart)"] --> spooled
    W -->|"streamed: the PUT ran alongside the spool write"| uploading

    subgraph intents["upload_intents, per digest"]
        spooled([spooled])
        uploading([uploading])
        parked([parked])
        accepted([accepted])
        published([published])
    end

    spooled -->|"dedup: blob_locations hit;<br/>file moves to the cache"| accepted
    spooled -->|"first network call begins<br/>(uploadBlobs, parkBlobs, concludeBlobs fallback)"| uploading
    uploading -->|"single-shot upload:<br/>/blob/add, PUT, conclude, accept;<br/>file moves to the cache (BlobCache.Take)"| accepted
    uploading -->|"parkBlobs: /blob/add WithConclude(false),<br/>PUT (or already streamed); blob_parks row written;<br/>local copy removed (removeLocal)"| parked
    parked -->|"acceptParked (the upload's acceptor, or Complete for a spooled part):<br/>/ucan/conclude; ConcludePark swaps the blob_parks row for blob_locations"| accepted
    spooled -->|"release record; executeRelease, local only<br/>(the blob never left this node):<br/>DeleteIntent + removeLocal"| gone([deleted])
    uploading -->|"release record; executeRelease: /blob/remove<br/>(idempotent: the accept may have landed, its location not);<br/>DeleteIntent + removeLocal"| gone
    parked -->|"release record; executeRelease: /blob/abort (cause AddTask),<br/>or /blob/remove if the provider says accepted;<br/>DeleteIntent + removeLocal"| gone
    accepted -->|"release record (never committed);<br/>executeRelease: /blob/remove;<br/>DeleteIntent + removeLocal"| gone
    published -->|"release record (committed);<br/>executeRelease: /blob/remove;<br/>DeleteIntent + removeLocal"| gone

    accepted -->|"commit: the claim, in the root swap's transaction"| published

    parked -.->|"SweepLocalBlobs over budget (blob_parks row):<br/>removeLocal + MarkEvicted; state unchanged"| evicted[["local file evicted<br/>(evicted_at set)"]]
    accepted -.->|"cache_writes false: at accept,<br/>once the location is recorded;<br/>removeLocal + MarkEvicted"| evicted
    accepted -.->|"SweepLocalBlobs over budget (blob_locations row):<br/>removeLocal + MarkEvicted; state unchanged"| evicted
    published -.->|"SweepLocalBlobs over budget (blob_locations row):<br/>removeLocal + MarkEvicted; state unchanged"| evicted
    published -->|"commit: the root swap's transaction adds this version's claim"| refs["blob_refs rows<br/>(digest, bucket, key, version_id)"]
    refs -->|"version delete or overwrite removes its row"| zero{"CountClaims == 0<br/>for (space, digest)?"}
    zero -->|yes| rel["release record enqueued, due after release_grace<br/>(the published → deleted edge runs it)"]
    zero -->|no| keep["blob retained<br/>(still referenced)"]
```

- `published` is written with the blob's first reference claim and never
  changes until the blob's release, which removes the local copy and the
  intent as it does for a never-committed blob. `blob_parks` is a
  presence machine (a row exists while a conclude is owed), not a state
  column.
- A blob's file lives in the spool until the provider holds it, then moves
  to the cache: once its location and `accepted` state are both recorded,
  by rename (`BlobCache.Take`). A failed move leaves it in the spool. With
  `cache_writes: false` the copy is removed at that point instead, and the
  intent gains `evicted_at`.
- Eviction (the dotted edges) is orthogonal to the state: `SweepLocalBlobs`
  runs every 30s and, when the spool and cache together are over
  `local_blob_max_bytes`, removes the local files of blobs the provider
  already holds (a location row for `accepted` and `published`, a park row
  for `parked`), oldest state change first, down to 90% of the budget,
  passing over blobs inside `cache_min_residency` or `cache_read_retention`
  unless usage stays over budget without them (then evicting inside the
  windows only down to the budget): from
  the cache, or from the spool for a blob whose move never happened. The
  intent keeps its row and state and gains `evicted_at`; reads fall through
  to the network tier. `spooled` and `uploading` files are never evicted.
  The parked-part drop at UploadPart marks `evicted_at` the same way. A
  committed blob's release removes its local copy with the intent; once at
  startup, the released pass does the same for `published` intents nothing
  names any more (no location, claim, pending release or live part), left
  by versions whose releases kept the copy.
- Digests present in both the old and new version sets never churn: the new
  generation's claim lands before the old one's drops in the same
  transaction, so the digest keeps a claim throughout and no release is
  recorded.
- Parked-blob reclamation is guarded: a digest live in another session, part,
  or committed object is left alone.
- A same-space copy pins the source's body: its claims are pins
  (`registry.RootCommit.Pins`), each written only while the space still
  holds a claim on the digest, under its lock, so a copy racing the source's
  delete and release fails with NoSuchKey instead of claiming a blob about
  to go.

Cross-references: [`architecture.md` §5](./architecture.md#5-the-data-layer),
[`s3-versioning.md`](./s3-versioning.md) §8.

Sources: `registry/stores.go` (state consts, ListEvictable, ListReleasedPublished), `s3frontend/object.go`
(ingestBody, addClaims, dropClaims), `s3frontend/localblob_sweep.go` (SweepLocalBlobs),
`s3frontend/localblobs.go` (cacheHeld, removeLocal),
`s3frontend/multipart.go`
(parkBlobs, acceptParked, recordConcluded, concludeBlobs, enqueuePartReleases),
`s3frontend/acceptor.go`, `s3frontend/object.go`
(runRelease, executeRelease), `uploader/blob.go` (UploadBlob,
AbortBlob, RemoveBlob). Review when these change.

## Per-key version storage: manifest arm, leaf arm, prev tree

Every catalog value block is a keyed union naming its own format. A key holds
its manifest directly until its first retained supersession, then gains an
`ObjectLeaf` and keeps it for the rest of its life.

```mermaid
flowchart TB
    root["bucket MST root<br/>(buckets.root_cid)"]
    leafk["MST leaf: plain object key"]
    union["ValueUnion<br/>(exactly one arm)"]
    mans["arm /objectmanifest/1<br/>single-version key:<br/>the manifest itself, one-block read<br/>(/objectmanifest/0 decodes into it)"]
    leaf["arm /objectleaf/0<br/>ObjectLeaf"]
    cur["Current: VersionNode<br/>(Seq, VersionID, Manifest CID)"]
    prev["Prev: per-key sub-MST of noncurrent versions<br/>keyed revSeqKey(seq): %016x of bit-inverted seq,<br/>so a forward walk is newest-first"]
    nulls["NullSeq: a noncurrent null<br/>version's seq (0 = none)"]
    em["EnvelopedManifest<br/>one per noncurrent version"]
    mf["ObjectManifest<br/>Seq, VersionID, DeleteMarker, ETag, headers,<br/>Body(Size, BLAKE3, TreeChunkLog, TreeBlocks,<br/>MD5, Blobs, PartSizes)"]
    ext["BlobExtent (/blobextent/0)<br/>up to 1,024 [digest, offset] tuples<br/>past the manifest's inline 1,024"]

    root --> leafk --> union
    union --> mans
    union --> leaf
    mans -.->|"first retained supersession;<br/>never reverts (deleting the last<br/>version deletes the key)"| leaf
    leaf --> cur
    leaf --> prev
    leaf --> nulls
    prev --> em
    cur --> mf
    em --> mf
    mf -->|"Blobs.Extents: [offset, CID]"| ext
```

- At most one null version exists per key; it may sit anywhere in the stack,
  and `NullSeq` locates (and lets a new null evict) a noncurrent one.
- The version token is a ULID whose low bytes embed the seq, but the embedded
  seq is only a locator hint: resolution always confirms the stored
  `VersionID`.
- Deleting the current version promotes the prev-tree head into a new leaf
  (clearing `NullSeq` when the promoted seq was the null); deleting the last
  version deletes the key.
- The bucket versioning state itself is three values with one guarded SQL
  update: `unversioned` moves to `enabled` or `suspended`, which then toggle;
  there is no path back to `unversioned`.
- Blocks this tree sheds (replaced leaf blocks, discarded manifests) are
  recorded for future removal; the entry paths are the
  [catalog GC candidates](#catalog-gc-candidates-what-gets-remembered-for-removal)
  diagram.

Cross-references: [`s3-versioning.md`](./s3-versioning.md) (the full design:
value union §2.1, prev tree §2.2, token §3, write rule §5, deletes §7);
[`architecture.md` §4](./architecture.md#4-the-catalog-layer) keeps the ASCII
manifest figure this complements.

Sources: `bucket/leaf.go`, `bucket/manifest.go`, `s3frontend/version.go`
(mintVersionID, revSeqKey, commitVersion, deleteVersionScoped). Review when
`bucket/` types change (and re-run `make gen`).

## Catalog GC candidates: what gets remembered for removal

`gc_candidates` is the memory of the catalog collector to come: every write
that supersedes a catalog block records that block's CID here, inside the
commit critical section, so nothing the tree sheds is ever forgotten. Nothing
consumes the table yet.

```mermaid
flowchart TB
    cv["commitVersion<br/>every version-creating write: PutObject, CopyObject,<br/>CompleteMultipartUpload, delete-marker insertion"]
    dok["deleteObjectKey<br/>DELETE without versionId, unversioned bucket"]
    dvs["deleteVersionScoped<br/>DELETE with versionId"]

    gc[("gc_candidates<br/>(cid, bucket, created_at)")]

    cv -->|"null replaced in place (Suspended/Unversioned):<br/>the discarded manifest block"| gc
    cv -->|"a new null evicts the noncurrent null:<br/>the evicted prev-entry manifest"| gc
    cv -->|"key already held a leaf:<br/>the replaced leaf block"| gc
    dok -->|"the key's value block, plus the current<br/>manifest when the key held a leaf"| gc
    dvs -->|"the removed version's manifest,<br/>plus the replaced leaf or value block"| gc

    gc -.->|"no reader exists yet"| sweep["catalog GC collector (future):<br/>removes superseded blocks from Forge"]

    classDef external stroke-dasharray: 5 5
    class sweep external
```

- Recording is mandatory, never best-effort: the inserts run inside the
  per-bucket commit closure, so a failed insert fails the whole write before
  the root swap.
- A retained supersession queues only the replaced **leaf block**; the old
  manifest is not queued because it lives on as a prev-tree entry. Only
  blocks nothing references anymore land here.
- As built, the table holds superseded **value blocks** (manifests and leaf
  blocks). Interior MST path nodes rewritten by a splice are not recorded;
  [`architecture.md` §4](./architecture.md#4-the-catalog-layer) scopes the
  eventual collector to those as well.
- The body bytes a removed version referenced are handled separately, by the
  claim ledger in the
  [blob lifecycle](#blob-lifecycle-spooled-parked-accepted-released):
  `gc_candidates` remembers catalog blocks, `blob_refs` counts body blobs.

Cross-references: [`architecture.md` §4](./architecture.md#4-the-catalog-layer),
[`s3-versioning.md`](./s3-versioning.md) §7.

Sources: `s3frontend/version.go` (commitVersion supersession,
deleteVersionScoped), `s3frontend/object.go` (deleteObjectKey),
`registry/stores_postgres.go` (AddGCCandidate). Review when these change.

## Principals and proof stores

The agent signs every outbound invocation, but the authority to act on a
space arrives per request: hilt re-delegates the access key's grant to the
agent, the IAM layer caches it per access key, and `internal/reqscope`
carries it into the write and read paths.

```mermaid
flowchart TB
    subgraph chain["delegation chain (hilt-issued)"]
        space["bucket space<br/>did:plc, minted by hilt at bucket create"]
        ak["access key<br/>did:key (the S3 access key ID)"]
        agent["ingot agent<br/>identity.key_file key, wrapped as the<br/>identity.service_id did:web; issuer of every outbound invocation"]
        space -->|"grant at access-key creation"| ak
        ak -->|"re-delegation per authorize<br/>(expires next UTC midnight)"| agent
    end

    hilt["hilt did:web:hilt"] -->|"/s3/request/authorize and<br/>/s3/bucket/info responses"| kp

    subgraph stores["proof stores"]
        kp["iam.KeyProofs: one DelegationCache<br/>per access key, 24h idle eviction;<br/>also the key's effective S3 action set per bucket"]
        ship["uploader.Forge shipProofs:<br/>per-space store, 1h TTL"]
        commit["uploader.Forge commitProofs:<br/>per-space store, 1h TTL"]
        static["AuthServiceProofs:<br/>static container from config"]
        tok["tokenstore (tokens.cbor):<br/>empty; dormant login paths only"]
    end

    kp -->|"reqscope.ProofStore<br/>on the request ctx"| writes["uploader:<br/>/blob/add, abort, remove, /index/add<br/>/upload/add, /upload/remove"]
    kp -->|"same store"| reads["blockstore.Forge:<br/>content/retrieve"]
    kp -->|"captureShipProofs<br/>at UploadBlob and StartBlob"| ship
    kp -->|"CaptureShipAuthority<br/>at every bucketop commit"| commit
    commit --> catship["async catalog ship"]
    ship -->|"no commit capture<br/>holding a chain"| catship
    ship --> async["sweepers' releases, aborts and<br/>registration renewals; DeleteBucket aborts"]
    static --> hc["hilt client:<br/>every /s3/* invocation"]

    classDef external stroke-dasharray: 5 5
    class hilt external
```

- The piri provider DID is never configured: retrieval audiences come from
  the `/assert/location` commitment each read resolves.
- Every authenticated S3 request carries a proof store: versitygw's root
  account is disabled, so no access-key auth path skips the hilt-backed IAM
  lookup. `/health` and the DID document are outside the S3 auth chain, and a
  rejected access key never receives one.
- Every write captures the ship authority when it commits, so a segment,
  sealed seconds after its writes, ships under a fresh capture. That capture
  is kept apart from the upload's: hilt grants each catalog-changing
  operation the ship's commands, but a narrower key's grant (tagging,
  retention) lacks the release, abort and registration commands the
  upload's capture also serves. The ship uses the commit's capture only when
  it holds a `/blob/add` chain for the space (an older key's grant, or an
  expired or revoked delegation, may not), and the upload's otherwise. Both
  last an hour and live in memory: a segment whose ship keeps failing past
  that, or across a restart, has no authority until the bucket's next
  write.
- Each access key gets its own `DelegationCache`, so a proof chain can never
  assemble across keys.
- The same store holds the effective S3 action set hilt reported for the key
  on each bucket, expiring with the keys and the tenant (next UTC midnight
  plus clock skew). One invalidation therefore drops the chains and the set
  together. The set is what the fast path enforces: several S3 actions map to
  the same Forge commands, and `s3:PutObject`'s commands are a superset of
  `s3:GetObject`'s, `s3:ListBucket`'s and `s3:AbortMultipartUpload`'s, so a
  chain probe alone would let a put-only key read and abort on the bucket, or
  read a copy's source.
- A copy's source and the headers that add a permission to a write (a lock
  or tags on the version it creates) are decided from headers, and a
  DeleteObjects on its body, which iam attaches for the operations hilt
  names (`Operation.ReadsBody`) and no other. Hilt's `RequirementsFor`
  refuses a header the signature does not cover or a body it does not bind,
  as hilt itself does, so the fast path decides nothing from such a
  request: it goes to hilt, which refuses it as `UnsignedHeader` or
  `InvalidBody`.

Cross-references: [`architecture.md` §9](./architecture.md#9-the-system-contract-piri--sprue--indexer).

Sources: `iam/service.go`, `iam/keyproofs.go`, `iam/proofcache.go`,
`internal/reqscope/reqscope.go`, `uploader/forge.go` (shipProofs,
captureShipProofs, CaptureShipAuthority), `bucketop/bucketop.go` (Tx.Commit),
`module.go`, `config/config.go` (auth_service fields).
Review when these change.

## Request authorization and proof capture

Every request is authorized against hilt (or its cached delegations), and
the proofs captured here are what the rest of the request spends. Versitygw's
root account is disabled, so no access key bypasses this path. The bucket
policy operations (GET, PUT and DELETE on `/{bucket}?policy`) take it too,
through versitygw's own controllers; the backend (`s3frontend/policy.go`)
then forwards the signed request to `/s3/bucket/policy`, where hilt
authenticates and authorizes it again and keeps the document.

```mermaid
sequenceDiagram
    autonumber
    actor C as S3 client
    participant G as versitygw s3api
    participant I as iam.Service
    participant K as KeyProofs<br/>(DelegationCache)
    participant H as hilt
    participant B as backend handler

    C->>G: signed S3 request
    G->>G: middleware stashes the raw request on ctx<br/>(reqscope, ahead of auth)
    G->>I: GetUserAccountForRequest (every access key#59; root disabled)
    I->>I: access key ID parsed as a did:key
    alt local fast path (authorizeLocal)
        I->>K: cached derived key verifies SigV4,<br/>per bucket: cached action set permits the action,<br/>every command chains to the agent
        break action outside a cached set
            I-->>G: AccessDenied, hilt not consulted
            G-->>C: 403
        end
    else hilt authorize
        I->>H: /s3/request/authorize (the signed request)
        H-->>I: account, derived SigV4 key,<br/>effective action set, fresh delegations
        I->>K: cacheProofs (re-delegations, bucket info<br/>per bucket) + the action set, keyed by each bucket hilt named<br/>(the addressed bucket and a copy's source)
        opt chain incomplete
            I->>H: /s3/bucket/info
            I->>K: cache the bucket chain
        end
    end
    I-->>G: auth.Account with SigningKey (RoleAdmin)
    G->>G: re-verify the signature with the derived key<br/>(covers streaming per-chunk signatures hilt never sees)
    G->>B: handler runs with reqscope.ProofStore on ctx
```

- The fast path has three outcomes: authorized, refused, or undecided. It
  refuses only on a cached action set that excludes the action a bucket
  needs — hilt's own answer for that key and bucket, so re-asking would
  repeat it (access keys are immutable; a deleted key's revocations clear
  the store). Everything else undecided (no cached set for a bucket, an
  expired one, an operation that maps to no Forge command) goes to hilt,
  whose response refills the caches.
- `RoleAdmin` is deliberate: authorization already happened (at hilt or the
  fast path), so versitygw's role and ACL layers must defer entirely.
- Authorize failures map to S3 errors in `mapAuthError`; unrecognized errors
  stay 500-class on purpose.
- The signing key never leaves hilt as a secret: ingot receives a derived
  SigV4 key per request (the versitygw fork's `auth.Account.SigningKey`).
- A policy document reaches hilt only after versitygw's PutBucketPolicy
  controller has validated its AWS shape; `iam.Service.GetUserAccount`
  answers that validation's principal lookups for any well-formed user ARN.
  GetBucketPolicy forwards only a signed GET ?policy: versitygw's own access
  checks call it too and get the stub answer, so versitygw never evaluates a
  policy. A hilt rejection renders through the same `mapAuthError` mapping;
  a gateway middleware (`policyHeaders`) adds the response ETag the backend
  leaves in a reqscope holder, and a GET's JSON content type.

Sources: `iam/service.go` (GetUserAccountForRequest, authorizeLocal,
cacheProofs, mapAuthError), `iam/proofcache.go` (PutPermissions, Permits),
`server.go` (buildS3API middleware, policyHeaders), `s3frontend/policy.go`.
Review when `iam/`, `s3frontend/policy.go` or the hilt client changes.

## Postgres schema as migrated

Everything relational lives in the `ingot` schema; manifests and MST nodes
are content-addressed blocks in the catalog log, not rows, apart from the
catalog journal's copy of those not yet shipped. Attributes here
are trimmed to keys, states, and the columns other diagrams reference; the
full DDL is `migrations/sql/`.

```mermaid
erDiagram
    buckets {
        text name PK
        bytea root_cid "committed MST root"
        bytea forge_root_cid "root durable on Forge"
        text space "Forge space DID"
        text tenant "owning tenant DID"
        text versioning "unversioned, enabled, suspended"
        text encryption "AES256, none; NULL = unconfigured, encrypts"
        bigint next_version_seq
    }
    segments {
        bigint seq PK "from segment_seq"
        text plane "'data' arm is dead"
        text state "open, sealed"
        text bucket
        bytea sha256
        bigint shipped_at
        bytea index_digest
        bigint retired_at
    }
    segment_op_roots {
        bigint seq PK, FK
        int seq_within PK
        text bucket
        bytea root_cid
    }
    catalog_journal {
        bigint seq PK "segment the block went into"
        bytea cid PK
        text bucket FK
        bytea data "the block, until its segment ships"
        timestamptz created_at
    }
    blob_refs {
        bytea digest PK
        text bucket PK
        text object_key PK
        text version_id PK
        text space "claim index (space, digest)"
    }
    upload_intents {
        text space PK "the upload's space: one file, an intent per space"
        bytea digest PK
        text local_path
        bigint size
        text state "spooled, uploading, parked, accepted, published (claimed by a commit)"
        text bucket
        timestamptz updated_at "last state change: the budget pass evicts oldest first"
        timestamptz evicted_at "set once the local copy is removed (sweeper or parked-part drop)"
    }
    blob_locations {
        text space PK
        bytea digest PK
        text provider "provider DID"
        text url
        bigint size
    }
    blob_encryption_params {
        text space PK
        bytea digest PK "ciphertext blob multihash"
        bytea region_wrapped_cek "never the raw CEK; deleting the row crypto-shreds the blob"
        text region_key_version
        bigint header_len "the ciphertext starts here"
        bytea aad "COSE Enc_structure; its protected header holds the base nonce and chunk size"
    }
    shard_inclusions {
        text space PK
        bytea digest PK "inner block multihash"
        bytea shard_digest
        bigint range_start
        bigint range_end "inclusive"
    }
    blob_parks {
        text space PK "the allocation's space"
        bytea digest PK
        bytea add_task
        bytea accept_task
        bytea put_invocation
        bigint size
    }
    blob_streams {
        bytea add_task PK "an upload allocated by digest code"
        text space
        text bucket
        bigint size
        timestamptz created_at "when the sweeper stops retrying the abort"
        timestamptz touched_at "the owning request's lease, renewed while it runs; the stream sweeper's clock"
    }
    multipart_sessions {
        text upload_id PK
        text bucket
        text space "the bucket's space, required: the subject of every release of the session's blobs, never resolved from the bucket name"
        text object_key
        text state "open, completing, aborting, completed"
        timestamptz state_changed_at "the sweeper's clock"
        boolean plaintext "store parts as received; decided at create"
        text checksum_algorithm
    }
    multipart_parts {
        text upload_id PK, FK
        int part_number PK
        bytea etag_md5
        bytea blob_digests "ordered array"
        bigint span_lengths "each blob's span in the object, bytes before encryption (digests are of the stored blob); NULL before 00030"
        text checksum
        text state "spooled, parked, accepted: the acceptor's queue"
    }
    multipart_part_trees {
        text upload_id PK, FK
        int part_number PK, FK
        bigint tree_offset "object offset the part's BLAKE3 tree was hashed at (assumed by UploadPart)"
        smallint chunk_log "block size of blocks, base-2 exponent of chunks"
        bytea nodes "the part's aligned subtrees (blake3tree.EncodeSubtrees)"
        bytea blocks "the part's blocks at chunk_log; up to 320 KiB for a 5 GiB part"
        bytea head "bytes before the part's first chunk boundary (< 1 KiB)"
        bytea tail "bytes after its last chunk boundary (< 1 KiB)"
        bytea root "the part's own BLAKE3 hash, when hashed at offset 0"
    }
    blob_release_intents {
        text space PK
        bytea digest PK
        timestamptz not_before "earliest execution: claim drop + grace, or at once for a part blob"
        timestamptz started_at "set under the claim-key lock once the release owns the blob; a reusing write cancels an unstarted record or waits out a started one"
    }
    gc_candidates {
        bytea cid PK "superseded MST node"
        text bucket
    }
    upload_registrations {
        bigint seq PK "commit order"
        text bucket FK
        text object_key
        text space
        bytea root "the version's manifest CID"
        text op "add, remove"
        timestamptz next_at "the sweeper's clock"
        timestamptz dead_lettered_at "set once retrying stops"
    }
    revocation_cursor {
        boolean id PK "single-row latch"
        timestamptz recorded_at "resume point in the revocation stream"
    }

    segments ||--o{ segment_op_roots : "ON DELETE CASCADE"
    multipart_sessions ||--o{ multipart_parts : "ON DELETE CASCADE"
    multipart_parts ||--o| multipart_part_trees : "ON DELETE CASCADE; deleted by Complete once the object commits"
    buckets ||--o{ upload_registrations : "ON DELETE CASCADE"
    buckets ||--o{ catalog_journal : "ON DELETE CASCADE"
    segments ||..o{ catalog_journal : "by seq, deliberately no FK"
    buckets ||..o{ segments : "by bucket name, no FK"
    buckets ||..o{ blob_refs : "by bucket name, no FK"
    blob_locations ||..o{ shard_inclusions : "by shard_digest"
    blob_locations ||..o| blob_encryption_params : "by (space, digest), deliberately no FK"
    multipart_parts ||..o{ blob_parks : "digests in blob_digests"
    multipart_parts ||..o{ blob_release_intents : "teardown and supersede records, by digest"
```

- The five solid relationships are the schema's only real foreign keys;
  dashed ones are soft joins the code performs. `revocation_cursor` stands
  alone.
- `catalog_journal` has no foreign key to `segments` on purpose: when a
  segment's CAR is missing from disk, recovery deletes its row if it was open
  (and retires it if it was sealed), and after a lost disk the journal rows
  are what that segment is rebuilt from.
- `gc_candidates` is write-only (no reader exists yet; its entry paths are the
  [catalog GC candidates](#catalog-gc-candidates-what-gets-remembered-for-removal)
  diagram); `segments.plane`
  still CHECK-allows the deleted `data` arm; `multipart_parts.accepted` is a
  CHECK arm no code writes.
- Session rows also carry the passthrough HTTP headers and checksum columns
  Complete writes into the manifest; intent and location rows carry
  timestamps. See the DDL for the full column lists.

Cross-references: [`architecture.md` Appendix C](./architecture.md#appendix-c--postgres-schema-the-ingot-schema)
(the annotated DDL).

Sources: `migrations/sql/*.sql`. Review whenever a migration is added.

## DeleteBucket teardown order

Teardown crosses every seam in order: prove the bucket empty, commit its
root empty, unwind in-flight multipart, quiesce the log, release every
network registration, then delete at hilt and locally.

```mermaid
sequenceDiagram
    autonumber
    actor C as S3 client
    participant B as s3frontend.Backend
    participant R as registry<br/>(Postgres)
    participant L as logstore.Manager
    participant U as sprue
    participant H as hilt

    C->>B: DeleteBucket
    Note over B,R: the whole operation runs under the per-bucket lock
    B->>R: reg.Get
    B->>B: MST emptiness walk<br/>(ErrBucketNotEmpty, or the versioned variant)
    B->>R: CASRoot(root → empty), SetForgeRoot(empty)
    Note over B,R: nothing the bucket reads is released below
    loop each open multipart session
        B->>U: abortOpenSession: /blob/abort parked blobs
        Note over B,U: the request's own proofs sign the release legs<br/>(s3:DeleteBucket delegates blob.Abort + blob.Remove)
    end
    B->>L: QuiesceBucketLog (stop flushes, wait for in-flight ship)
    B->>L: ShippedSegmentDigests (each CAR + its index blob, retired<br/>segments included, sealed-but-unshipped over-listed,<br/>release is idempotent)
    loop each digest
        B->>U: /blob/remove (release the space's registration)
    end
    B->>U: drainSpaceReleases: /blob/remove each deleted<br/>object's blob still behind the reader grace
    B->>U: DrainSpaceRegistrations: send the space's queued<br/>/upload/add and /upload/remove changes
    B->>H: /s3/bucket/delete (hilt refuses while registrations remain)
    B->>R: reg.Delete (the local row, its catalog_journal rows cascading)
    B->>L: RemoveBucketLog, best-effort<br/>(directory + segment rows)
    B-->>C: 204
```

- The log seams (`QuiesceBucketLog`, `ShippedSegmentDigests`,
  `RemoveBucketLog`) are type-asserted on `blockstore.Log`: a plain `Store`
  without them silently no-ops, which is the trap this diagram documents.
- A failure after the root is committed empty (hilt refusing, a release
  erroring) leaves the bucket functional: it lists empty, the closed store
  reopens lazily on the next write, and a retried delete starts over.

Sources: `s3frontend/bucket.go` (DeleteBucket), `s3frontend/multipart.go`
(abortOpenSession), `s3frontend/object.go` (drainSpaceReleases),
`s3frontend/uploadsweep.go` (DrainSpaceRegistrations), `logstore/manager.go`.
Review when these change.

## Package-to-diagram map

The review table for changes: find the package you touched, re-check the
listed diagrams (each diagram's `Sources:` footer names its exact files).

| Package / file | Diagrams to review |
|---|---|
| `s3frontend/object.go` | put, get, blob-lifecycle, version-tree, gc-candidates |
| `s3frontend/version.go` | put, get, multipart, blob-lifecycle, version-tree, gc-candidates |
| `s3frontend/multipart.go` | multipart, part-states, multipart-states, blob-lifecycle |
| `s3frontend/acceptor.go` | multipart, part-states, blob-lifecycle |
| `s3frontend/stream.go` | put, multipart, block-routes, blob-lifecycle |
| `s3frontend/bucket.go` | context, delete-bucket |
| `bucketop/` | put, get, delete-bucket, packages, principals |
| `blockstore/` | get, packages, block-routes; `forge.go` also context, principals |
| `registry/` | schema, get, blob-lifecycle, logstore-pipeline |
| `logstore/` | logstore-pipeline, segment-states, get, block-routes, delete-bucket |
| `uploader/`, `forgeclient/` | context, put, multipart, principals, logstore-pipeline, block-routes |
| `iam/`, `internal/reqscope/` | principals, authorize, context |
| `bucket/`, `mst/` | version-tree, put, get |
| `migrations/sql/` | schema, plus any state diagram naming a changed CHECK |
| `module.go`, `server.go`, `config/` | packages, context, logstore-pipeline; `server.go` also authorize and multipart |
| `s3frontend/policy.go` | context, authorize |
