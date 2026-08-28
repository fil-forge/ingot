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
| `multipart` | [Multipart upload](#multipart-upload-park-on-write-conclude-on-complete) | park at UploadPart, conclude at Complete, abort unwind | here |
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
    ingot -->|"/s3/request/authorize (every non-root request)<br/>/s3/bucket/info (lazy chain completion)<br/>/s3/bucket/create, delete, list"| hilt
    ingot -->|"/blob/add, /ucan/conclude, GET /receipt/:task<br/>/blob/abort, /blob/remove, /index/add"| sprue
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
- The root account (versitygw `RootUserConfig`) bypasses hilt and holds no
  proof store, so it can manage buckets but cannot read through the network
  tier.
- `ListBuckets` is served entirely from hilt; the local `ingot.buckets` table
  backs every other verb.

Cross-references: [`architecture.md` §9](./architecture.md#9-the-system-contract-piri--sprue--indexer)
(the system contract), [§12](./architecture.md#12-implementation-status--postponed-items)
(implementation status).

Sources: `module.go`, `bucketauthority/service.go`, `iam/service.go`,
`forgeclient/`, `uploader/blob.go`, `blockstore/forge.go`. Review when these
change.

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
        bs["blockstore<br/>Spool, OpStaging, Layered, Cached, Forge"]
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
        cmd["cmd<br/>serve, whoami, version"]
    end

    vgw -->|"backend.Backend"| s3f
    vgw -->|"auth.IAMService +<br/>RequestIAMService"| iam
    s3f --> bop
    s3f --> bkt
    s3f --> reg
    s3f --> up
    s3f --> ba
    bop -->|"blockstore.Log"| ls
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
        split["SplitBody: coarse split at max_blob_size,<br/>sha256 + md5 in one streaming pass"]
        spool["Spool (DataDir/spool)<br/>+ upload_intents row"]
        upload["per-blob upload before the commit:<br/>/blob/add, HTTP PUT, conclude, accept<br/>(a blob_locations hit skips it: dedup)"]
        bloc["blob_locations row: the whole blob,<br/>(space, digest) to provider URL"]
        split --> spool --> upload --> bloc
    end

    subgraph catr["the catalog route (dag-cbor blocks, asynchronous ship)"]
        commit["commit: manifest + MST splice into OpStaging,<br/>one fsynced AppendBatch into the bucket's log,<br/>guarded root swap, 200 to the client"]
        seal["open segment seals at SealBytes / SealAge<br/>(CAR + .idx + .ops)"]
        ship["background ship: /blob/add the sealed CAR,<br/>PUT, conclude, accept"]
        incl["blob_locations (the CAR) + shard_inclusions<br/>(inner byte ranges), then the guarded<br/>forge_root_cid advance and retention"]
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
  accepted on Forge at ack; the catalog is fsynced locally at ack and becomes
  durable on Forge only when its segment ships (`forge_root_cid` lags
  `root_cid` until then).
- The ordering is the crash-safety invariant: the catalog route starts only
  after every body blob is durable, so a crash never leaves a catalog entry
  pointing at non-durable data.
- The network unit differs: a body blob is retrieved whole by its own digest
  (a `blob_locations` hit); catalog blocks share a CAR, so a network read
  resolves `shard_inclusions` to a byte range inside the shard.
- Reads mirror the split: `OpenBlob` (bodies) checks spool then network,
  skipping the log; `GetBlock` (catalog) checks spool, log, then network
  (the [GetObject diagram](#getobject-version-resolution-local-tiers-network-retrieval)).
- Removal mirrors it too: bodies are reference-counted
  ([blob lifecycle](#blob-lifecycle-spooled-parked-accepted-released));
  superseded catalog blocks queue for a future collector
  ([catalog GC candidates](#catalog-gc-candidates-what-gets-remembered-for-removal)).
- Multipart parts ride the body route with the conclude deferred (parked);
  `Complete` finishes it
  (the [multipart diagram](#multipart-upload-park-on-write-conclude-on-complete)).

Cross-references: [`architecture.md` §4](./architecture.md#4-the-catalog-layer),
[§5](./architecture.md#5-the-data-layer);
[`logstore/README.md`](../logstore/README.md).

Sources: `s3frontend/object.go` (ingestBody), `blockstore/spool.go`,
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

    Note over C,L: off the lock: ingest, hash, encrypt, upload
    C->>B: PutObject(bucket, key, body)
    B->>R: reg.Get(bucket), precondition pre-check
    B->>B: tenant recipient: resolve the tenant's #wrap key<br/>(tenant DID from the request, did:plc doc via the cached PLC resolver);<br/>no recipient → the write fails
    B->>SP: SplitBody: per plaintext piece, fresh CEK →<br/>FEE envelope (COSE_Encrypt, AES-256-GCM STREAM,<br/>one recipient: ECDH-ES+A256KW to the tenant wrap key) →<br/>spool under the CIPHERTEXT digest<br/>(sha256 + md5 of the plaintext in the same pass)
    B->>R: PutIntent(digest, stored size) +<br/>PutEncryptionParams(region-wrapped CEK, FEE geometry) per blob
    loop each body blob (uploadBlobs)
        alt blob_locations already has (space, digest)
            B->>R: SetIntentState(accepted), skip upload<br/>(never hits for fresh writes — every envelope digest is new)
        else upload
            B->>U: /blob/add (digest, size)
            U-->>B: allocation address (none on provider-side dedup)
            B->>P: HTTP PUT bytes
            B->>U: /ucan/conclude the put receipt, then poll /blob/accept receipt
            U-->>B: /assert/location commitment
            B->>R: SetIntentState(accepted) + PutLocation
        end
    end
    Note over B,U: UploadBlob also captures the request proof store as the<br/>space's ship authority (captureShipProofs, 1h TTL)
    Note over B,L: under the per-bucket lock: commitVersion via Coordinator.WithTx
    B->>TX: Begin (lock, snapshot root)
    TX->>R: AllocVersionSeq
    TX->>TX: mint versionId ("null" unless versioning Enabled),<br/>manifest into OpStaging, re-check If-Match under the lock,<br/>MST splice (new nodes into staging)
    TX->>L: AppendBatch(catalog blocks, OpRoot) with fsync
    TX->>R: CASRoot(bucket, snapshot, newRoot), guarded
    TX-->>B: committed, lock released
    Note over B,R: post-commit, off the lock
    B->>R: reconcileClaims: blob_refs gains this version,<br/>superseded version rows removed
    B->>U: /blob/remove per digest whose CountClaims reached 0<br/>(+ crypto-shred: its blob_encryption_params row deleted)
    B-->>C: 200 + ETag (+ x-amz-version-id when versioning is configured)
```

- Encryption makes the stored digest a ciphertext digest: **content dedup is
  gone for bodies** (fresh CEK per write ⇒ unique envelope), by design per
  the encryption RFC. Manifest spans, `Body.Size`, sha256/md5 and ETag stay
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
- `CopyObject` runs the same `commitVersion` with a manifest that pins the
  source's digests: no spool, no upload, claims incremented. Cross-space
  (today: cross-bucket) copies are rejected `NotImplemented` — the CEK wrap
  is bound to (space, digest), so they need a rewrap flow.
- Supersession also records each replaced catalog block for future removal:
  the [catalog GC candidates](#catalog-gc-candidates-what-gets-remembered-for-removal)
  diagram shows every entry path.

Cross-references: [`architecture.md` §7.1](./architecture.md#71-write-single-shot-putobject).

Sources: `s3frontend/object.go` (PutObject, ingestBody, uploadBlobs),
`s3frontend/version.go` (commitVersion), `bucketop/bucketop.go`,
`blockstore/staging.go`, `uploader/blob.go`, `uploader/forge.go`. Review when
these change.

## GetObject: version resolution, local tiers, network retrieval

A read resolves the version through the MST, then serves each covering blob
from the first tier that has it: spool, the catalog log (catalog blocks
only), then the network. Network resolution goes through the local locator
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
    B->>R: bodyOpener: blob_encryption_params + blob_locations per blob<br/>(row existence = encrypted)
    loop each covering blob or catalog block
        opt encrypted blob (FEE)
            B->>K: Unwrap region-wrapped CEK, bound to (space, digest)
            Note over B: aesstream.CiphertextRange maps the plaintext range to<br/>one contiguous ciphertext span past the envelope header
        end
        B->>LY: read (whole blob, or only the ciphertext span via OpenBlobRange)
        alt spool hit
            LY-->>B: bytes from the spool
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
            LY->>P: content/retrieve (UCAN, audience = the commitment's provider,<br/>proofs from reqscope.ProofStore, absent for root;<br/>narrowed to the ciphertext span for an encrypted blob)
            P-->>LY: ranged bytes, length-checked
        end
        opt encrypted blob (FEE)
            Note over B: aesstream.SpanReader decrypts the span as it streams;<br/>a tampered chunk fails authentication mid-stream (ErrCorrupted)
        end
    end
    B-->>C: 200 or 206 body (plaintext byte counts throughout)
```

- `OpenBlob` (body blobs) checks spool then network; only `GetBlock`
  (catalog blocks) consults the log tier, and only `GetBlock` is fronted by
  the `Cached` LRU.
- A root-account read has no request proof store, so it works only while the
  blocks are local.
- The indexer-backed locator (`blockstore/locator`) compiles but is never
  injected; `module.go` always wires `LocalLocator`.
- Manifest coordinates are plaintext: `BlobRef.Start/End`, `Body.Size`,
  ETag and Content-Length never change with encryption. `BlobRef.Digest`
  names the stored — for an encrypted blob, ciphertext — bytes; only the
  per-blob open translates between the two.
- HEAD needs no decryption at all (every value it reports is a plaintext
  manifest field).

Cross-references: [`architecture.md` §7.4](./architecture.md#74-read-getobject),
[§8](./architecture.md#8-retrieval-addressing-when-bodies-need-a-sharded-dag-index).

Sources: `s3frontend/object.go` (GetObject, HeadObject, selectBytes),
`s3frontend/version.go` (resolveVersion), `s3frontend/decrypt.go`
(bodyOpener, decryptingOpener), `bucket/chunker.go` (BlobRangeOpener),
`blockstore/layered.go`,
`blockstore/forge.go` (doRetrieve), `blockstore/cache.go`,
`registry/locallocator.go`, `logstore/manager.go` (Get). Review when these
change.

## Multipart upload: park on write, conclude on Complete

Parts upload to the provider at `UploadPart` but stay **parked** (durable,
unaccepted): the conclude that triggers `/blob/accept` is deferred to
`Complete`, so an `Abort` only ever unwinds parked blobs.

```mermaid
sequenceDiagram
    autonumber
    actor C as S3 client
    participant B as s3frontend.Backend
    participant R as registry<br/>(sessions, parts, parks, intents)
    participant U as sprue
    participant P as piri
    participant TX as bucketop.Tx

    C->>B: CreateMultipartUpload
    B->>R: CreateSession(open) with headers + checksum algorithm
    B-->>C: uploadId
    C->>B: UploadPart(n)
    B->>B: openSession (non-open: NoSuchUpload), then splitSpool<br/>(resolve the tenant recipient, then encrypt per piece:<br/>fresh CEK → FEE envelope → spool under the ciphertext<br/>digest + params row, as in the PutObject diagram)
    B->>R: PutPart(parked)
    loop each part blob (parkBlobs)
        alt blob_locations already has the digest
            B->>R: intent accepted (dedup, no park)
        else already parked (GetPark hit)
            B->>R: reuse the park
        else park
            B->>U: /blob/add with WithConclude(false)
            B->>P: HTTP PUT bytes
            B->>R: PutPark(AddTask, AcceptTask, PutInvocation), intent parked
        end
    end
    B-->>C: part ETag (part md5)
    C->>B: CompleteMultipartUpload(parts)
    B->>B: validate parts (ascending, ETags, checksums, MinPartSize)
    alt session already completed
        B-->>C: the stored result (idempotent re-Complete)
    else latch won
        B->>R: LatchSession(open to completing), single winner
        B->>R: manifest spans: per blob, plaintext length derived from<br/>the intent's stored size + FEE geometry (blobPlaintextLen)
        B->>U: concludeBlobs: /ucan/conclude per parked blob, poll accept
        B->>R: PutLocation + intent accepted + DeletePark per blob
        B->>TX: commitVersion (see the PutObject diagram)
        B->>R: LatchSession(completing to completed), best-effort
        B-->>C: 200, ETag = md5-of-part-md5s + "-N"
    end
    C->>B: AbortMultipartUpload
    B->>R: LatchSession(open to aborting), then DeleteSession (parts cascade)
    B->>U: cleanupPartBlobs: /blob/abort parked blobs (cause = AddTask),<br/>crypto-shred each blob's blob_encryption_params row
    Note over B,R: a background sweeper aborts open sessions older than<br/>MultipartSessionTTL (default 7d) and reaps terminal rows
```

- A part re-upload and an abort reclaim only blobs no other session, part, or
  committed object references (`cleanupPartBlobs` checks `CountPartRefs` and
  `CountClaims`).
- A never-parked blob at Complete falls back to a full synchronous
  `UploadBlob`.

Cross-references: [`architecture.md` §7.2](./architecture.md#72-multipart),
[§7.3](./architecture.md#73-the-session-latch-the-abortcomplete-race).

Sources: `s3frontend/multipart.go` (all verbs, parkBlobs, concludeBlobs,
cleanupPartBlobs, SweepStaleMultipartSessions), `registry/stores.go`,
`server.go` (startMultipartSweeper). Review when these change.

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
    aborting --> [*] : DeleteSession (parts cascade)
    completed --> [*] : sweeper past TTL
```

- There is no wait-for-in-flight: a racing Complete either sees `completed`
  (and returns the stored result) or loses the latch and gets `NoSuchUpload`.
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

    subgraph intents["upload_intents, per digest"]
        spooled([spooled])
        parked([parked])
        accepted([accepted])
    end

    spooled -->|"dedup: blob_locations hit"| accepted
    spooled -->|"single-shot upload:<br/>/blob/add, PUT, conclude, accept"| accepted
    spooled -->|"parkBlobs: /blob/add WithConclude(false),<br/>PUT; blob_parks row written"| parked
    parked -->|"concludeBlobs at Complete:<br/>/ucan/conclude; blob_parks row deleted"| accepted
    spooled -->|"cleanupPartBlobs:<br/>DeleteIntent + spool.Remove"| gone([deleted])
    parked -->|"cleanupPartBlobs: /blob/abort (cause AddTask),<br/>DeleteIntent + spool.Remove"| gone

    accepted -->|"commit: reconcileClaims adds this version"| refs["blob_refs rows<br/>(digest, bucket, key, version_id)"]
    refs -->|"version delete or overwrite removes its row"| zero{"CountClaims == 0<br/>for (space, digest)?"}
    zero -->|yes| rm["RemoveBlob: /blob/remove to sprue<br/>(space claim released)"]
    zero -->|no| keep["blob retained<br/>(still referenced)"]
```

- `published` is a declared intent state no code writes today; `blob_parks`
  is a presence machine (a row exists while a conclude is owed), not a state
  column.
- Digests present in both the old and new version sets never churn: the
  reconcile computes a set difference.
- Parked-blob reclamation is guarded: a digest live in another session, part,
  or committed object is left alone.

Cross-references: [`architecture.md` §5](./architecture.md#5-the-data-layer),
[`s3-versioning.md`](./s3-versioning.md) §8.

Sources: `registry/stores.go` (state consts), `s3frontend/object.go`
(ingestBody, reconcileClaims, releaseBlobs), `s3frontend/multipart.go`
(parkBlobs, concludeBlobs, cleanupPartBlobs), `uploader/blob.go` (UploadBlob,
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
    mans["arm /objectmanifest/0<br/>single-version key:<br/>the manifest itself, one-block read"]
    leaf["arm /objectleaf/0<br/>ObjectLeaf"]
    cur["Current: VersionNode<br/>(Seq, VersionID, Manifest CID)"]
    prev["Prev: per-key sub-MST of noncurrent versions<br/>keyed revSeqKey(seq): %016x of bit-inverted seq,<br/>so a forward walk is newest-first"]
    nulls["NullSeq: a noncurrent null<br/>version's seq (0 = none)"]
    em["EnvelopedManifest<br/>one per noncurrent version"]
    mf["ObjectManifest<br/>Seq, VersionID, DeleteMarker, ETag, headers,<br/>Body(Size, SHA256, MD5, Blobs, PartSizes)"]

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
        kp["iam.KeyProofs: one DelegationCache<br/>per access key, 24h idle eviction"]
        ship["uploader.Forge shipProofs:<br/>per-space store, 1h TTL"]
        static["AuthServiceProofs:<br/>static container from config"]
        tok["tokenstore (tokens.cbor):<br/>empty; dormant login paths only"]
    end

    kp -->|"reqscope.ProofStore<br/>on the request ctx"| writes["uploader:<br/>/blob/add, abort, remove, /index/add"]
    kp -->|"same store"| reads["blockstore.Forge:<br/>content/retrieve"]
    kp -->|"captureShipProofs<br/>at UploadBlob"| ship
    ship --> async["async catalog ship;<br/>sweeper and DeleteBucket aborts"]
    static --> hc["hilt client:<br/>every /s3/* invocation"]

    classDef external stroke-dasharray: 5 5
    class hilt external
```

- The piri provider DID is never configured: retrieval audiences come from
  the `/assert/location` commitment each read resolves.
- The root account holds no proof store: bucket administration works, network
  reads and space-scoped writes do not.
- A bucket whose last write is more than an hour old has an expired ship
  authority; a newly sealed segment then waits for the bucket's next write to
  re-capture it.
- Each access key gets its own `DelegationCache`, so a proof chain can never
  assemble across keys.

Cross-references: [`architecture.md` §9](./architecture.md#9-the-system-contract-piri--sprue--indexer).

Sources: `iam/service.go`, `iam/keyproofs.go`, `iam/proofcache.go`,
`internal/reqscope/reqscope.go`, `uploader/forge.go` (shipProofs,
captureShipProofs), `module.go`, `config/config.go` (auth_service fields).
Review when these change.

## Request authorization and proof capture

Every non-root request is authorized against hilt (or its cached
delegations), and the proofs captured here are what the rest of the request
spends.

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
    alt root access key
        G->>G: RootUserConfig match, IAM skipped
        Note over G,B: no proof store: bucket admin works,<br/>network-tier reads fail
    else non-root key
        G->>I: GetUserAccountForRequest
        I->>I: access key ID parsed as a did:key
        alt local fast path (authorizeLocal)
            I->>K: cached derived key verifies SigV4,<br/>every command chains to the agent
        else hilt authorize
            I->>H: /s3/request/authorize (the signed request)
            H-->>I: account, derived SigV4 key, fresh delegations
            I->>K: cacheProofs (re-delegations)
            opt chain incomplete
                I->>H: /s3/bucket/info
                I->>K: cache the bucket chain
            end
        end
        I-->>G: auth.Account with SigningKey (RoleAdmin)
        G->>G: re-verify the signature with the derived key<br/>(covers streaming per-chunk signatures hilt never sees)
    end
    G->>B: handler runs with reqscope.ProofStore on ctx
```

- `RoleAdmin` is deliberate: authorization already happened (at hilt or the
  fast path), so versitygw's role and ACL layers must defer entirely.
- Authorize failures map to S3 errors in `mapAuthError`; unrecognized errors
  stay 500-class on purpose.
- The signing key never leaves hilt as a secret: ingot receives a derived
  SigV4 key per request (the versitygw fork's `auth.Account.SigningKey`).

Sources: `iam/service.go` (GetUserAccountForRequest, authorizeLocal,
cacheProofs, mapAuthError), `server.go` (buildS3API middleware). Review when
`iam/` or the hilt client changes.

## Postgres schema as migrated

Everything relational lives in the `ingot` schema; manifests and MST nodes
are content-addressed blocks in the catalog log, not rows. Attributes here
are trimmed to keys, states, and the columns other diagrams reference; the
full DDL is `migrations/sql/`.

```mermaid
erDiagram
    buckets {
        text name PK
        bytea root_cid "committed MST root"
        bytea forge_root_cid "root durable on Forge"
        text space "Forge space DID"
        text versioning "unversioned, enabled, suspended"
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
    }
    segment_op_roots {
        bigint seq PK, FK
        int seq_within PK
        text bucket
        bytea root_cid
    }
    blob_refs {
        bytea digest PK
        text bucket PK
        text object_key PK
        text version_id PK
        text space "claim index (space, digest)"
    }
    upload_intents {
        bytea digest PK
        text local_path
        bigint size
        text state "spooled, parked, accepted; 'published' never written"
        text bucket
    }
    blob_locations {
        text space PK
        bytea digest PK
        text provider "provider DID"
        text url
        bigint size
    }
    shard_inclusions {
        text space PK
        bytea digest PK "inner block multihash"
        bytea shard_digest
        bigint range_start
        bigint range_end "inclusive"
    }
    blob_parks {
        bytea digest PK
        bytea add_task
        bytea accept_task
        bytea put_invocation
        bigint size
    }
    multipart_sessions {
        text upload_id PK
        text bucket
        text object_key
        text state "open, completing, aborting, completed"
        text checksum_algorithm
    }
    multipart_parts {
        text upload_id PK, FK
        int part_number PK
        bytea etag_md5
        bytea blob_digests "ordered array"
        text checksum
        text state "'accepted' never written"
    }
    gc_candidates {
        bytea cid PK "superseded MST node"
        text bucket
    }

    segments ||--o{ segment_op_roots : "ON DELETE CASCADE"
    multipart_sessions ||--o{ multipart_parts : "ON DELETE CASCADE"
    buckets ||..o{ segments : "by bucket name, no FK"
    buckets ||..o{ blob_refs : "by bucket name, no FK"
    blob_locations ||..o{ shard_inclusions : "by shard_digest"
    multipart_parts ||..o{ blob_parks : "digests in blob_digests"
```

- The two solid relationships are the schema's only real foreign keys;
  dashed ones are soft joins the code performs.
- `gc_candidates` is write-only (no reader exists yet; its entry paths are the
  [catalog GC candidates](#catalog-gc-candidates-what-gets-remembered-for-removal)
  diagram); `segments.plane`
  still CHECK-allows the deleted `data` arm; `upload_intents.published` and
  `multipart_parts.accepted` are CHECK arms no code writes.
- Session rows also carry the passthrough HTTP headers and checksum columns
  Complete writes into the manifest; intent and location rows carry
  timestamps. See the DDL for the full column lists.

Cross-references: [`architecture.md` Appendix C](./architecture.md#appendix-c--postgres-schema-the-ingot-schema)
(the annotated DDL).

Sources: `migrations/sql/*.sql`. Review whenever a migration is added.

## DeleteBucket teardown order

Teardown crosses every seam in order: prove the bucket empty, unwind
in-flight multipart, quiesce the log, release every network registration,
then delete at hilt and locally.

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
    loop each open multipart session
        B->>U: abortOpenSession: /blob/abort parked blobs
        Note over B,U: reqscope.WithoutProofStore masks the request store,<br/>the park-time space authority signs instead<br/>(s3:DeleteBucket delegates no blob commands)
    end
    B->>L: QuiesceBucketLog (stop flushes, wait for in-flight ship)
    B->>L: ShippedSegmentDigests (each CAR + its index blob,<br/>sealed-but-unshipped over-listed, release is idempotent)
    loop each digest
        B->>U: /blob/remove (release the space's registration)
    end
    B->>H: /s3/bucket/delete (hilt refuses while registrations remain)
    B->>R: reg.Delete (the local row)
    B->>L: RemoveBucketLog, best-effort<br/>(directory + segment rows)
    B-->>C: 204
```

- The log seams (`QuiesceBucketLog`, `ShippedSegmentDigests`,
  `RemoveBucketLog`) are type-asserted on `blockstore.Log`: a plain `Store`
  without them silently no-ops, which is the trap this diagram documents.
- A failure after the quiesce leaves the bucket functional: the closed store
  reopens lazily on the next use.

Sources: `s3frontend/bucket.go` (DeleteBucket), `s3frontend/multipart.go`
(abortOpenSession), `logstore/manager.go`. Review when these change.

## Package-to-diagram map

The review table for changes: find the package you touched, re-check the
listed diagrams (each diagram's `Sources:` footer names its exact files).

| Package / file | Diagrams to review |
|---|---|
| `s3frontend/object.go`, `s3frontend/version.go` | put, get, blob-lifecycle, version-tree, gc-candidates |
| `s3frontend/multipart.go` | multipart, multipart-states, blob-lifecycle |
| `s3frontend/bucket.go` | context, delete-bucket |
| `bucketop/` | put, get, delete-bucket, packages |
| `blockstore/` | get, packages, block-routes; `forge.go` also context, principals |
| `registry/` | schema, get, blob-lifecycle, logstore-pipeline |
| `logstore/` | logstore-pipeline, segment-states, get, block-routes, delete-bucket |
| `uploader/`, `forgeclient/` | context, put, multipart, principals, logstore-pipeline, block-routes |
| `iam/`, `internal/reqscope/` | principals, authorize, context |
| `bucket/`, `mst/` | version-tree, put, get |
| `migrations/sql/` | schema, plus any state diagram naming a changed CHECK |
| `module.go`, `server.go`, `config/` | packages, context, logstore-pipeline |
