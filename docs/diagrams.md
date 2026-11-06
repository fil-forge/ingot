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
| `put` | [PutObject](#putobject-stream-and-upload-in-the-request-commit-on-the-bucket-queue) | the write path: ingest and upload in the request, commit on the bucket queue | here |
| `get` | [GetObject](#getobject-version-resolution-local-tiers-network-retrieval) | version resolution, tier fallthrough, network retrieval | here |
| `multipart` | [Multipart upload](#multipart-upload-park-on-write-accept-in-the-background) | park at UploadPart, accept in the background, Complete's wait, abort unwind | here |
| `part-states` | [Part states](#part-states-the-acceptance-queue) | the part rows as the upload's accept queue | here |
| `multipart-states` | [Session states](#session-states-open-and-completed) | the session state machine, one change per upload at a time | here |
| `blob-lifecycle` | [Blob lifecycle](#blob-lifecycle-uploading-parked-accepted-released) | intent states and the claim ledger gating deletion | here |
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
        bs["blockstore<br/>BlobCache, OpStaging, Layered, Cached, Forge"]
        ls["logstore<br/>Manager, Store, PlaneLog, Segment"]
        cars["cars"]
    end

    subgraph meta["metadata"]
        reg["registry<br/>Postgres, LocalLocator"]
        tr["taskrunner<br/>Runner, Exec"]
        mig["migrations"]
    end

    subgraph net["network clients"]
        up["uploader<br/>Forge"]
        fc["forgeclient<br/>sprue edge client"]
        ba["bucketauthority<br/>hilt bucket ops"]
    end

    subgraph host["host"]
        root["ingot (root)<br/>Server, Module"]
        cmd["cmd<br/>serve, whoami, version, blake3, catalog"]
    end

    vgw -->|"backend.Backend"| s3f
    vgw -->|"auth.IAMService +<br/>RequestIAMService"| iam
    s3f --> bop
    s3f --> bkt
    s3f --> reg
    s3f --> up
    s3f --> ba
    s3f --> tr
    bop -->|"blockstore.Log"| ls
    bop -.->|"bucketop.ShipAuthority<br/>(uploader.Forge injected)"| up
    bop --> mst
    mst --> bs
    bkt --> bs
    ls --> cars
    reg -->|"logstore.Meta"| ls
    bs -.->|"locator.Locator<br/>(LocalLocator injected)"| reg
    up --> fc
    iam --> reg
    reg --> tr
    cmd --> root
    cmd -->|"catalog usage"| ls
    cmd --> reg
    cmd --> iam
    cmd --> bs
    cmd --> cars
    root --> vgw
    root --> tr
    root --> mig
    root -->|"BlockReader =<br/>Cached(Forge)"| bs
```

- `registry.Postgres` is one struct satisfying the registry seams
  (`Registry` plus the intent, location, inclusion, blob-ref, GC, multipart,
  park, and catalog usage stores) and `logstore.Meta`.
- `taskrunner` imports nothing else from ingot. `New` registers
  `s3frontend`'s handlers for every queue on the one runner
  `provideTaskRunner` builds, and `registry` returns the statements and task
  specs a handler commits with.
- `uploader.Forge` is one struct behind `Uploader`,
  `StreamingBodyUploader`, `DeferredBodyUploader`, and `BlobRemover`.
- `internal/testfakes` (test-only fakes), `tokenstore` (empty; only the
  dormant login paths would read it), `regionkey`, `tenantkey`, `revocation`,
  `blake3tree` and the `internal/` helpers are omitted. `blockstore/locator`'s indexer-backed
  locator compiles but is never injected.
- `cmd`'s `catalog usage` builds its own logstore walk, registry, Hilt
  authorization and Forge reader rather than going through the server.
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
        stream["each blob streams to its provider as it arrives:<br/>/blob/add by size and digest code, HTTP PUT,<br/>sha256 on the way<br/>(an object's blob is teed into a .tmp-* file<br/>in the cache)"]
        upload["per-blob queue-space-shard.add before the commit:<br/>/ucan/conclude, accept, upload_intents row<br/>(a blob_locations hit reuses it: dedup)"]
        bloc["blob_locations row: the whole blob,<br/>(space, digest) to provider URL"]
        cache["BlobCache (DataDir/cache):<br/>the temporary file is adopted<br/>under the blob's digest"]
        split --> stream --> upload --> bloc --> cache
    end

    subgraph catr["the catalog route (dag-cbor blocks, asynchronous ship)"]
        commit["queue-bucket.commit: manifest + MST splice into OpStaging,<br/>one fsynced AppendBatch into the bucket's log,<br/>guarded root advance journaling the new blocks<br/>in Postgres (catalog_journal), 200 to the client"]
        seal["open segment seals at SealBytes / SealAge<br/>(CAR + .idx + .ops)"]
        ship["queue-bucket.ship: /blob/add the sealed CAR,<br/>PUT, conclude, accept"]
        incl["blob_locations (the CAR) + shard_inclusions<br/>(inner byte ranges), then the guarded<br/>forge_root_cid advance, the segment's journal<br/>rows deleted, and retention"]
        commit --> seal --> ship --> incl
    end

    piri["piri: bytes stored by digest"]

    put --> split
    bloc -->|"every body blob durable and accepted"| commit
    stream --> piri
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
- Reads mirror the split: `OpenBlob` (bodies) checks local disk (the
  cache) then network, skipping the log; `GetBlock` (catalog) checks
  local disk, log, then network
  (the [GetObject diagram](#getobject-version-resolution-local-tiers-network-retrieval)).
- Removal mirrors it too: bodies are reference-counted
  ([blob lifecycle](#blob-lifecycle-uploading-parked-accepted-released));
  superseded catalog blocks queue for a future collector
  ([catalog GC candidates](#catalog-gc-candidates-what-gets-remembered-for-removal)).
- A body streams: each blob is allocated by size
  and hash function (`/blob/add` with a digest code) and its envelope goes to
  the provider as it arrives, so the upload overlaps the body's arrival
  instead of following it. The digest is computed on the way; the put
  receipt reports it and the node checks it at accept. An upload service
  that cannot add by digest code fails the write.
- Multipart parts ride the body route with the conclude deferred (parked);
  the upload's `queue-multipart-upload-shard.accept` finishes it
  (the [multipart diagram](#multipart-upload-park-on-write-accept-in-the-background)).

Cross-references: [`architecture.md` §4](./architecture.md#4-the-catalog-layer),
[§5](./architecture.md#5-the-data-layer);
[`logstore/README.md`](../logstore/README.md).

Sources: `s3frontend/object.go` (ingestBody), `s3frontend/stream.go`,
`s3frontend/shardref.go` (runAdd), `s3frontend/bucketqueue.go` (runCommit),
`s3frontend/bucketship.go` (runShip),
`bucket/sized.go`, `blockstore/blobcache.go`,
`blockstore/staging.go`, `logstore/`, `uploader/forge.go`, `uploader/blob.go`,
`server.go` (newBucketFlushFunc). Review when these change.

## PutObject: stream and upload in the request, commit on the bucket queue

All network I/O runs in the request, before the commit. The commit is a
`queue-bucket.commit` task on the bucket's subject, run sync for the request,
so one change to a bucket runs at a time. It is a manifest write, an MST
splice, one fsynced `AppendBatch`, and a guarded root advance written with
the task. A `200` means the body is durable and accepted on the network, not
merely buffered.

```mermaid
sequenceDiagram
    autonumber
    actor C as S3 client
    participant B as s3frontend.Backend
    participant BC as blockstore.BlobCache
    participant R as registry<br/>(Postgres)
    participant U as sprue
    participant P as piri
    participant Q as queue-bucket.commit<br/>(the bucket's subject)
    participant TX as bucketop.Tx
    participant L as logstore.Manager

    Note over C,L: in the request: ingest, hash, encrypt, stream each blob to its provider
    C->>B: PutObject(bucket, key, body)
    B->>R: reg.Get(bucket), precondition pre-check
    B->>B: tenant recipient: resolve the tenant's #35;wrap key<br/>(tenant DID from the request, did:plc doc via the cached PLC resolver)#59;<br/>no recipient → the write fails
    loop SplitSizedBody: each plaintext piece of min(max_blob_size, remaining) bytes
        B->>B: fresh CEK → FEE envelope (COSE_Encrypt, AES-256-GCM STREAM,<br/>one recipient: ECDH-ES+A256KW to the tenant wrap key)#59;<br/>its length is known from the header + plaintext length
        B->>U: /blob/add (digestCode sha2-256, envelope size, random nonce)
        U-->>B: allocation address, put and accept tasks
        par
            B->>P: HTTP PUT the envelope bytes as they arrive<br/>(sha256 of the envelope, BLAKE3 tree + md5 of the plaintext in the same pass)
        and
            B->>BC: CreateTemp: tee the same bytes into a .tmp-* file<br/>(none with cache_writes: false#59; a failed copy is dropped, the PUT goes on)
        end
        Note over B,P: a failed PUT fails the request: the allocation is aborted<br/>(within 10 s) and the client retries#59; so does UnsupportedDigestCode
    end
    B->>R: PutEncryptionParams(region-wrapped CEK, FEE geometry) per blob
    loop each blob (concludeBody → addBlob)
        Note over B,R: a sync queue-space-shard.add on the blob's release subject (space/hex digest),<br/>after any release ahead of it, within 30 s (else SlowDown)
        alt blob_locations already has (space, digest)
            B->>R: PutIntent(accepted)<br/>(only a plaintext bucket's repeated bytes hit: every envelope digest is new)
        else conclude
            B->>R: PutIntent(stored size, uploading)
            B->>U: /ucan/conclude the put receipt, reporting the digest
            U-->>B: accept receipt + /assert/location commitment
            B->>R: PutLocation, then SetIntentState(accepted)
        end
        B->>R: in the add's commit: the write's pending reference (blob_pending_refs)<br/>+ a queue-space-shard.expire due in 24 h
        opt the space held the blob already
            B->>U: /blob/abort the surplus allocation
        end
        B->>BC: Adopt: the .tmp-* file goes under the digest, before the 200#59;<br/>queue-shard.cache dates it and schedules its queue-shard.evict
    end
    Note over B,U: StartBlob also captures the request proof store, if borrowable,<br/>as the space's ship authority (captureShipProofs, 1h TTL)
    B->>Q: run sync: Put(key, manifest, blobs, pending references, If-Match/If-None-Match)<br/>+ the captured release and /upload/add, /upload/remove proofs
    Q->>TX: Begin (snapshot root)
    TX->>R: AllocVersionSeq (a multipart Complete in an Enabled bucket<br/>reserved its seq before the commit: see the multipart diagram)
    TX->>TX: applyPut: mint versionId ("null" unless versioning Enabled),<br/>manifest into OpStaging, re-check If-Match against the current version,<br/>MST splice (new nodes into staging), advance the working root
    Note over TX: uploader.CaptureShipAuthority: every append captures the request<br/>proof store, if borrowable and it holds a /blob/add chain, as the<br/>space's ship authority, so a write with no body<br/>(a delete, a tag change) leaves one for its segment's ship
    TX->>L: Tx.Append: AppendBatch(catalog blocks, OpRoot) with fsync
    Q->>R: RootCommitWrites, written with the task's own commit:<br/>the batch's catalog blocks journaled under its segment (catalog_journal,<br/>skipped once the segment has shipped), the root's advance (guarded: it must<br/>find the root it started from), blob_refs gains this version, each claim<br/>taking over the write's pending reference (row deleted, expiry cancelled#59;<br/>an expired one fails the change: SlowDown), superseded version rows removed,<br/>a queue-space-shard.release task, due after the grace, per digest whose<br/>claims reached 0#59;<br/>/upload/add(root = this version's manifest CID)<br/>+ /upload/remove per discarded version queued (queue-space-upload tasks)
    Q-->>B: the change's result (version id, seq)
    B-->>C: 200 + ETag (+ x-amz-version-id when versioning is configured)
    Note over B,U: a task runner worker runs each queue-space-shard.release once its grace passes:<br/>crypto-shred (its blob_encryption_params row deleted), then /blob/remove
```

- A streamed blob whose body fails aborts its allocation at once. One a
  dying request leaves behind is never concluded, and the provider expires
  it within a day: an unaccepted blob counts toward no space's usage and is
  never served.
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
  [blob lifecycle](#blob-lifecycle-uploading-parked-accepted-released).
- `CopyObject` commits the same Put change. Within one space its manifest
  pins the source's digests: no stream, no upload, claims incremented. Each
  source blob first takes a pending reference through a copy-mode
  `queue-space-shard.add`, which fails the copy with NoSuchKey unless the
  space has the blob located, and the copy's claims take them over; a source
  deleted and released after the adds leaves the copy's blobs referenced,
  and the copy commits. Across
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
- The write does **not** call the upload service. It queues the change as a
  `queue-space-upload` task in the same transaction as the root's advance, and a
  worker sends it. The count is reporting data, so a Sprue round trip does not
  belong in a PUT, and a Sprue outage delays the count instead of failing
  writes or losing it.
- The task exists exactly when the version committed. The queue's subject is
  the space, so a space's changes run in commit order, and a batch takes only
  consecutive changes of one kind: up to 256 additions in one round trip, then
  the retractions after them in another. (A batch in ucantone's sense — many
  invocations, a receipt each.) Splitting by kind is needed because a container
  sorts its tokens bytewise, so the service may run a batch in any order, and a
  retraction sharing a batch with the addition it retires could overtake it and
  leave the root counted for good.
- A refused change is retried with backoff and holds its space, so a later
  change can never overtake it. Each change carries the delegation chain that
  authorizes it, captured from the request that committed the version, because
  the task has no request to borrow authority from.
- That chain has room to spare — hilt expires its delegation to the gateway at
  the next UTC midnight plus an async overhang, so even a change queued at
  23:59 has hours to be sent in. For a queue that outlasts that, the handler
  renews from the space's live authority, which a recent body upload with a
  borrowable store leaves behind, less any delegation a revocation has since
  named. A space that has gone quiet has none, and after
  `uploadRegistrationDeadLetterAfter` attempts the change fails for good: it
  stays in the queue as a failed task, out of the way of the space's later
  changes. The count is then short (or high) by that one change rather than
  frozen for the space. A bucket delete resends the space's failed retractions
  with its own authority before the space goes.

Cross-references: [`architecture.md` §7.1](./architecture.md#71-write-single-shot-putobject).

Sources: `s3frontend/object.go` (PutObject, ingestBody, concludeBody),
`s3frontend/shardref.go` (addBlob, runAdd, runExpire), `s3frontend/stream.go` (WriteSizedBlob), `s3frontend/localblobs.go`
(cacheWritten), `blockstore/blobcache.go`, `bucket/sized.go`, `uploader/stream.go`,
`s3frontend/copy.go` (CopyObject, copySourceBucket),
`s3frontend/version.go` (applyPut), `s3frontend/bucketqueue.go` (commit, runCommit, register),
`registry/bucketcommit.go` (RootCommitWrites),
`s3frontend/uploadqueue.go` (the queue-space-upload handlers), `bucketop/bucketop.go`,
`blockstore/staging.go`, `uploader/blob.go`, `uploader/forge.go`,
`uploader/upload.go`. Review when these change.

## GetObject: version resolution, local tiers, network retrieval

A read resolves the version through the MST, then serves each covering blob
from the first tier that has it: local disk (the cache), the
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
        alt local hit (blockstore.BlobCache)
            LY-->>B: bytes from the cache
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
`blockstore/layered.go`, `blockstore/blobcache.go`,
`blockstore/forge.go` (doRetrieve), `blockstore/cache.go`,
`registry/locallocator.go`, `logstore/manager.go` (Get). Review when these
change.

## Multipart upload: park on write, accept in the background

Parts upload to the provider at `UploadPart` and park there (durable,
unaccepted). Each upload's `queue-multipart-upload-shard.accept` task
concludes its parked parts in batches while the client is still uploading,
so `Complete` has only the last few left to accept and its cost stays flat
as the part count grows.

```mermaid
sequenceDiagram
    autonumber
    actor C as S3 client
    participant B as s3frontend.Backend
    participant M as queue-multipart-upload<br/>(task, subject = uploadId)
    participant A as queue-multipart-upload-shard.accept<br/>(task, subject = uploadId)
    participant R as registry<br/>(sessions, parts, parks, intents)
    participant U as sprue
    participant P as piri
    participant Q as queue-bucket<br/>(the bucket's subject)

    C->>B: CreateMultipartUpload
    B->>M: run sync: queue-multipart-upload.create
    M->>Q: run sync as a child: queue-bucket.admit-upload<br/>(a closing bucket: OperationAborted)
    M->>R: with the task's commit: the session row (open) with headers + checksum algorithm#59;<br/>the auto-abort, queue-multipart-upload.abort, due one session TTL out (default 7 d)
    B-->>C: uploadId
    C->>B: UploadPart(n) / UploadPartCopy(n)
    B->>B: openSession (non-open: NoSuchUpload)
    opt UploadPartCopy
        B->>B: vet the source: copySourceBucket (tenant), resolveVersionIn,<br/>range within the object, copy-source preconditions (412)#59;<br/>the body is the source's plaintext range through the decrypting reader
    end
    B->>B: ingestPart: splitBody<br/>(resolve the tenant recipient, then encrypt per piece:<br/>fresh CEK → FEE envelope streamed to piri under its ciphertext<br/>digest + params row, as in the PutObject diagram#59; no local copy)
    loop each part blob (parkBlobs → addBlob: a sync queue-space-shard.add on the blob's release subject)
        alt blob_locations already has the digest
            B->>R: intent accepted (dedup, no park)
        else already parked (GetPark hit)
            B->>R: reuse the park, intent parked
        else park
            B->>R: PutPark(AddTask, AcceptTask, PutInvocation), intent parked:<br/>the PUT already went to piri in splitBody
        end
        B->>R: in the add's commit: the write's pending reference + a queue-space-shard.expire due in 24 h
        opt the space held the blob already
            B->>U: /blob/abort the surplus allocation
        end
    end
    B->>M: run sync: queue-multipart-upload.add-part, behind the upload's other changes<br/>(waiting past 30 s: SlowDown)
    M->>R: PartWrites, with the task's commit: the part row (parked, span_lengths), open sessions<br/>only (otherwise NoSuchUpload)#59; the adds' references taken over (rows deleted, expiries<br/>cancelled)#59; a queue-space-shard.release task, due now, for each blob the replaced row<br/>named and this part does not, returned to run#59; a refused part expires its references at once (dropRefsNow)
    M->>A: in the same commit: queue the upload's accept,<br/>a replacement due now (at most one pending per upload)
    B->>U: releaseNow: the tasks above, due at once, run inline (/blob/abort of the parks#59;<br/>a blob another part or the session still names is stale for that reference)
    B-->>C: part ETag (part md5#59; for a copy, of the copied bytes)
    A->>R: GetSession (nothing to do unless open)
    loop while a parked part is left after the part-number cursor
        A->>R: ListParkedParts after the cursor: a batch of up to 1000 blobs, a part never split
        A->>U: ConcludeBlobs: the batch's put receipts in one /ucan/conclude (2 min timeout),<br/>accept receipts read from the response (poll only as fallback)
        A->>A: writes for the task's commit: park swapped for the location while the concluded<br/>park (by AddTask) stands#59; intent accepted (published stays)#59; a park beside a standing<br/>location dropped#59; part accepted while its row names the same blobs, all located
    end
    A->>R: commit every batch's writes with the task
    Note over A,R: a failed batch fails the task and records nothing#59; the retry concludes again<br/>(a repeat to sprue)#59; 5 attempts from 5 s, doubling, up to 1 min
    C->>B: CompleteMultipartUpload(parts)
    B->>M: run sync: queue-multipart-upload.complete, to start within replayWaitBudget (10 s)<br/>behind the upload's other changes, another Complete included
    alt it did not start in time
        B-->>C: 409 OperationAborted
    else session already completed
        M->>M: validate parts (the conditional-write precheck is skipped)
        B-->>C: report the version id recorded on the session (none for a null version),<br/>then its ETag (idempotent re-Complete)
    else session open
        M->>M: conditional-write precheck#59; validate parts (ascending, ETags, checksums,<br/>MinPartSize)#59; an error here is answered with its own status
        M->>R: ListPartTrees#59; re-hash over budget: 400 InvalidPartLayout, the upload stays open
        alt versioning Enabled
            M->>R: reserveVersion: AllocVersionSeq before the commit, mint the versionId
            B-->>C: report it: versitygw commits the 200 with x-amz-version-id<br/>and x-amz-server-side-encryption
        else
            B-->>C: report no version id: the 200 commits with x-amz-server-side-encryption only
        end
        Note over B,C: from here the body is padded with a newline every<br/>complete_multipart_keepalive (5 s), and any error goes in the 200's body
        M->>R: manifest spans from each part's span_lengths<br/>(rows without them: intent size + FEE geometry)
        M->>A: run the accept as a task of its own, not a child, limited to the named parts:<br/>after any running or pending accept, to start within acceptWaitBudget (30 s)#59;<br/>its writes commit before Complete reads the parts and assembles the tree
        alt the accept did not start in time
            B-->>C: OperationAborted in the 200's body#59; the upload stays open
        else the accept failed
            B-->>C: its error in the 200's body#59; the upload stays open
        else a named part not accepted with the blobs the request validated
            B-->>C: InvalidPart in the 200's body#59; the upload stays open<br/>for the client to upload the part again
        else every named part accepted
            M->>Q: run sync as a child: queue-bucket.commit, a Put under what was reported<br/>(see the PutObject diagram), with its reserved version and If-Match/If-None-Match#59;<br/>refused (errReservationSuperseded) if the key gained a later seq,<br/>or the versioning no longer fits the report#59; the claims land<br/>with the root's advance, set-based
            M->>R: in the same commit: the session moves open to completed (guarded), the part trees<br/>are deleted, a release due now of each unlisted part's blob, and<br/>queue-multipart-upload.reap due one session TTL out
            B->>U: releaseNow: the unlisted parts' releases, within 10 s
            B-->>C: the result ends the body: ETag = md5-of-part-md5s + "-N"
        end
    end
    C->>B: AbortMultipartUpload
    B->>M: run sync: queue-multipart-upload.abort (completed or gone: NoSuchUpload)
    M->>R: with the task's commit: a queue-space-shard.release, due now, for each part blob still<br/>there to release (published or missing intents skipped)#59; the session deleted (parts cascade)
    B->>U: releaseNow, within 10 s in all: per blob, crypto-shred, then /blob/abort a parked blob<br/>(cause = AddTask) or /blob/remove an accepted one#59; local rows deleted<br/>in the task's commit#59; the task runner retries the rest
    Note over M,R: the auto-abort runs the same task, async, one session TTL after the create: an<br/>open upload only, nothing otherwise, retried without limit (1 s to 5 min)#59; the reap,<br/>one session TTL after a Complete, deletes the completed session and its parts and releases nothing
```

- A part re-upload and an abort queue a `queue-space-shard.release` task for
  every blob of theirs that is not still live in the session; the release
  itself decides whether the blob is free to go. Any reference in the space
  (a claim, a write's pending reference, a part of an in-flight session,
  counted in one statement by `CountReferences`) makes it stale and it does
  nothing; the session's teardown queues it again. A commit's claim
  publishes the blob's upload intent, so an abort releases only what was
  never committed, and the reap of a completed session releases nothing.
- A release can take a blob while an accept concludes it, when a part is
  replaced or dropped mid-conclude. Sprue decides the order: a released
  allocation refuses the accept, and an accepted blob refuses `/blob/abort`,
  which the release then follows with `/blob/remove`. The release deletes
  the park and location rows in its commit, so the accept's swap finds no
  park and records nothing for a blob a release took. The swap matches the
  park's `/blob/add` task, so a later park of the same bytes stays for its
  own conclude, and a part is marked accepted only while its row names the
  blobs the accept read, all located.
- Ingot treats a `/blob/remove` receipt as final. If sprue registers an
  accepted blob only after a remove that arrived earlier, that window is
  sprue's to fix, and Ingot does not guard it.
- A part is written only while its session is open, in the statement that
  checks the state. The add-part runs on the upload's subject, so no
  Complete or Abort runs beside it. One that ran first leaves the upload not
  open, the add-part answers NoSuchUpload, and the upload expires its pending
  references at once, which releases the blobs nothing else references
  (`dropRefsNow`).
- A blob with neither a park nor a location when the accept reaches it has
  gone from its provider. The accept leaves its part `parked`, and Complete
  answers `InvalidPart`; nothing keeps a part's bytes to upload them again.
- Complete reports its response headers through versitygw's
  `CompleteMultipartUploadReportingHeaders` before it accepts the parts,
  and the response commits to 200 at the report, as S3's does. A reserved
  version id is binding, and a Complete that reported none commits only a
  null version, so the client always learns the version it created
  ([`s3-versioning.md`](./s3-versioning.md) §5.1).

Cross-references: [`architecture.md` §7.2](./architecture.md#72-multipart),
[§7.3](./architecture.md#73-one-change-per-upload-the-abortcomplete-race).

Sources: `s3frontend/multipart.go` (all verbs, ingestPart, parkBlobs,
acceptRequested, sessionReleases), `s3frontend/multipartqueue.go`
(runCreateUpload, runAddPart, runAbort, runReap), `registry/multipartqueue.go`
(PartWrites, CompletedSession),
`s3frontend/version.go` (reserveVersion, applyPut), `s3frontend/bucketqueue.go` (putPreCheck),
`s3frontend/acceptor.go` (runAccept, acceptBatch), `registry/accept.go`,
`s3frontend/stream.go`,
`s3frontend/shardref.go` (addBlob, runAdd, dropRefsNow),
`s3frontend/releasequeue.go` (runRelease, releaseNow),
`s3frontend/uploadpartcopy.go`, `registry/stores.go`. Review when these change.

## Part states: the acceptance queue

A part row's `state` is the upload's acceptance queue. The accept marks a
part `accepted` only while its row still names the blobs the accept read, so
it never moves a row a later upload of the same part number replaced.

```mermaid
stateDiagram-v2
    [*] --> parked : queue-multipart-upload.add-part (UploadPart, once its blobs are parked)#59;<br/>queues the upload's accept in the same transaction
    parked --> accepted : queue-multipart-upload-shard.accept concludes the blobs,<br/>while the row names the same blobs and all are located
    parked --> [*] : replaced (releases queued with the replacement),<br/>or the session's teardown cascades
    accepted --> [*] : replaced (releases queued with the replacement),<br/>or the session's teardown cascades
```

- A part with a blob that has neither a park nor a location stays `parked`:
  its blob has gone from its provider, and Complete refuses the part
  (`InvalidPart`), since nothing keeps a part's bytes to upload them again.
- A replacement queues releases for the blobs the replaced row named and the
  new one does not, in the same transaction. Each is stale while a part, a
  claim or a pending reference in the space still names its blob. The
  request runs them itself when it can, rather than leave them to the
  runner's workers.
- Complete requires every part it names to be `accepted` before the commit.
  It runs the accept for those parts as a task of its own (parts it omits
  cannot fail it), answers an accept that cannot start within 30 s with a 409 that
  botocore clients treat as final, and fails with the accept's error when
  the accept fails. A failed batch fails the whole accept, which records
  nothing.
- Rows from before migration 00030 are `parked` or `accepted` and have no
  `span_lengths`; Complete derives their lengths per blob.

Sources: `registry/stores.go` (Part* consts), `registry/stores_postgres.go`
(ListParkedParts, OldestParkedPart), `registry/multipartqueue.go` (PartWrites),
`registry/accept.go`, `s3frontend/acceptor.go`, `s3frontend/multipart.go`
(ingestPart, acceptRequested), `migrations/sql/00030_part_accept_queue.sql`,
`migrations/sql/00033_task_runner.sql`. Review when these change.

## Session states: open and completed

Every change to an upload runs as a task on `queue-multipart-upload`, with
the upload ID as its subject, so a Complete and an Abort never overlap and
no part lands beside either.

```mermaid
stateDiagram-v2
    [*] --> open : queue-multipart-upload.create
    open --> completed : queue-multipart-upload.complete, in the version's commit (guarded)
    open --> [*] : queue-multipart-upload.abort (a client, DeleteBucket or the auto-abort)#59;<br/>releases queued, session deleted (parts cascade)
    completed --> [*] : queue-multipart-upload.reap (one session TTL out, or DeleteBucket)#59;<br/>releases nothing
```

- A Complete waits up to 10 s (`replayWaitBudget`) for the upload's other
  changes, another Complete included, before it reports anything; past
  that it answers `OperationAborted`, a 409. A Complete that runs after
  another one committed replays the ETag and version id recorded on the
  session, and skips the conditional-write precheck. A Complete after an
  Abort finds no session and answers `NoSuchUpload`.
- A Complete that fails before its commit leaves the upload open for a
  retry.
- The Complete's commit moves the session from `open` to `completed` in the
  same transaction as the version, guarded on it still being open.
- `ListParts` and `Abort` reject any non-`open` session as `NoSuchUpload`;
  `Complete` alone accepts `completed`, for idempotency. The auto-abort
  leaves a completed or missing upload alone.
- DeleteBucket aborts each open upload and reaps each completed one from
  the request, after `queue-bucket.close`.

Sources: `s3frontend/multipart.go`, `s3frontend/multipartqueue.go`,
`s3frontend/bucketqueue.go` (putPreCheck), `registry/multipartqueue.go`
(CompletedSession, DeletedSession, ReapedSession). Review when these change.

## Blob lifecycle: uploading, parked, accepted, released

Dedup stores bytes once; the claim ledger lets them be deleted once. Intents
track the disk-and-network state of each digest; `blob_refs` counts which
versions still reference it.

```mermaid
flowchart TB
    W["streamed write<br/>(PutObject ingest, UploadPart)"] -->|"the PUT runs as the body arrives"| uploading

    subgraph intents["upload_intents, per digest"]
        uploading([uploading])
        parked([parked])
        accepted([accepted])
        published([published])
    end

    uploading -->|"single-shot write's queue-space-shard.add: conclude, accept,<br/>or a blob_locations hit (dedup, the stream aborted);<br/>an object's local copy is adopted into the cache"| accepted
    uploading -->|"part's queue-space-shard.add (parkBlobs):<br/>the part streamed to its provider;<br/>blob_parks row written"| parked
    parked -->|"queue-multipart-upload-shard.accept (in the background, or run by Complete):<br/>/ucan/conclude; its commit swaps the blob_parks row for blob_locations"| accepted
    uploading -->|"queue-space-shard.release: /blob/remove<br/>(idempotent: the accept may have landed, its location not);<br/>rows deleted in the task's commit"| gone([deleted])
    parked -->|"queue-space-shard.release: /blob/abort (cause AddTask),<br/>or /blob/remove if the provider says accepted;<br/>rows deleted in the task's commit"| gone
    accepted -->|"queue-space-shard.release (never committed):<br/>/blob/remove;<br/>rows deleted in the task's commit;<br/>cached copy evicted"| gone
    published -->|"queue-space-shard.release (committed):<br/>/blob/remove;<br/>rows deleted in the task's commit;<br/>cached copy evicted"| gone

    accepted -->|"queue-bucket.commit: the claim, with the root's advance"| published

    accepted -.->|"queue-shard.evict at the TTL,<br/>or SweepLocalBlobs over budget;<br/>state unchanged"| evicted[["cached file evicted"]]
    published -.->|"queue-shard.evict at the TTL,<br/>or SweepLocalBlobs over budget;<br/>state unchanged"| evicted
    W -->|"each blob's queue-space-shard.add"| pend["blob_pending_refs row<br/>(the write's pending reference, 24 h)"]
    pend -->|"queue-bucket.commit: the claim takes it over<br/>(row deleted, expiry cancelled)"| refs
    pend -->|"queue-space-shard.expire at the deadline:<br/>row dropped; nothing else referencing<br/>the blob queues its release, due at once"| gone
    published -->|"queue-bucket.commit: the root's advance adds this version's claim"| refs["blob_refs rows<br/>(digest, bucket, key, version_id)"]
    refs -->|"version delete or overwrite removes its row"| zero{"no claim left<br/>for (space, digest)?"}
    zero -->|yes| rel["queue-space-shard.release queued in the same transaction,<br/>due after release_grace<br/>(the published → deleted edge runs it,<br/>stale if CountReferences finds any reference)"]
    zero -->|no| keep["blob retained<br/>(still referenced)"]
```

- `published` is written with the blob's first reference claim and never
  changes until the blob's release, which evicts the cached copy and removes
  the intent as it does for a never-committed blob. `blob_parks` is a
  presence machine (a row exists while a conclude is owed), not a state
  column.
- An object's blob is teed into a `.tmp-*` file in the cache as it streams
  (`BlobCache.CreateTemp`). Once its location and `accepted` state are both
  recorded, the write adopts the file under its digest (`BlobCache.Adopt`)
  before the PUT answers, and queues a `queue-shard.cache` task that dates
  the file and schedules a `queue-shard.evict` for `cache_ttl` later. A copy
  that could not be written or adopted is not cached. With
  `cache_writes: false` no copy is kept, and a multipart part never keeps
  one.
- Eviction (the dotted edges) is orthogonal to the state and touches only
  the cache. A read from the cache with less than half its `cache_ttl` left
  runs `queue-shard.cache` again, which replaces the pending eviction with a
  later one. `SweepLocalBlobs` runs every 30s and, when the cache, writes in
  progress included, is over `local_blob_max_bytes`, evicts cached files through sync
  `queue-shard.evict` tasks, oldest
  modification time (last renewal) first, down to 90% of the budget. The
  intent keeps its row and state; reads fall through to the network tier.
  Writes in progress are never evicted. A blob's release queues the
  eviction of its cached copy unless another space holds an intent for the
  digest.
- Digests present in both the old and new version sets never churn: the new
  generation's claim lands before the old one's drops in the same
  transaction, so the digest keeps a claim throughout and no release is
  queued.
- Parked-blob reclamation is guarded: a digest live in another session, part,
  committed object or write's pending reference is left alone.
- A write's pending reference is taken by its `queue-space-shard.add` on the
  blob's release subject, which never overlaps a release of the blob. A
  release that fails retries holding the subject, so an add behind it waits
  and then finds the blob unlocated and concludes its own stream. A write of
  a deleted object's bytes (plaintext bucket) while that object's release is
  within its reader grace keeps the blob: the release finds the reference
  and is stale.
- A same-space copy takes a pending reference to each source blob through a
  copy-mode `queue-space-shard.add`, which fails the copy with NoSuchKey
  unless the space has the blob located; its claims take the references
  over, so a source deleted and released after the adds leaves the copy's
  blobs referenced.

Cross-references: [`architecture.md` §5](./architecture.md#5-the-data-layer),
[`s3-versioning.md`](./s3-versioning.md) §8.

Sources: `registry/stores.go` (state consts), `s3frontend/object.go`
(ingestBody, addClaims, dropClaims), `s3frontend/localblob_sweep.go` (SweepLocalBlobs),
`s3frontend/stream.go` (WriteSizedBlob), `s3frontend/localblobs.go` (cacheWritten),
`blockstore/blobcache.go` (CreateTemp, Adopt), `s3frontend/shardqueue.go`
(queue-shard.cache, queue-shard.evict),
`s3frontend/multipart.go`
(parkBlobs, sessionReleases),
`s3frontend/acceptor.go`, `registry/accept.go`, `s3frontend/releasequeue.go`
(runRelease), `s3frontend/shardref.go` (runAdd, runExpire),
`registry/stores_postgres.go` (CountReferences, PendingRefByExpiry),
`registry/bucketcommit.go` (claimWrites, dropWrites), `registry/multipartqueue.go`
(refWrites),
`registry/release.go` (ReleasedRows, ReleasedIntent, PendingRef),
`uploader/blob.go` (ConcludeBlobs,
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
- The bucket versioning state itself is three values, set by
  `queue-bucket.set-versioning` on the bucket's subject: `unversioned` moves
  to `enabled` or `suspended`, which then toggle; there is no path back to
  `unversioned`.
- Blocks this tree sheds (replaced leaf blocks, discarded manifests) are
  recorded for future removal; the entry paths are the
  [catalog GC candidates](#catalog-gc-candidates-what-gets-remembered-for-removal)
  diagram.

Cross-references: [`s3-versioning.md`](./s3-versioning.md) (the full design:
value union §2.1, prev tree §2.2, token §3, write rule §5, deletes §7);
[`architecture.md` §4](./architecture.md#4-the-catalog-layer) keeps the ASCII
manifest figure this complements.

Sources: `bucket/leaf.go`, `bucket/manifest.go`, `s3frontend/version.go`
(mintVersionID, revSeqKey, applyPut, applyDeleteVersion),
`s3frontend/bucketadmin.go` (runSetVersioning). Review when
`bucket/` types change (and re-run `make gen`).

## Catalog GC candidates: what gets remembered for removal

`gc_candidates` is the memory of the catalog collector to come: every write
that supersedes a catalog block records that block's CID here, with the
`queue-bucket.commit` that sheds it, so nothing the tree sheds is ever
forgotten. Nothing
consumes the table yet.

```mermaid
flowchart TB
    cv["applyPut (a Put change)<br/>every version-creating write: PutObject, CopyObject,<br/>CompleteMultipartUpload, delete-marker insertion"]
    dok["applyDeleteKey (a Delete change)<br/>DELETE without versionId, unversioned bucket"]
    dvs["applyDeleteVersion (a Delete change)<br/>DELETE with versionId"]
    sa["applySetAttributes (a SetAttributes change)<br/>retention, legal hold, tags"]

    gc[("gc_candidates<br/>(cid, bucket, created_at)")]

    cv -->|"null replaced in place (Suspended/Unversioned):<br/>the discarded manifest block"| gc
    cv -->|"a new null evicts the noncurrent null:<br/>the evicted prev-entry manifest"| gc
    cv -->|"key already held a leaf:<br/>the replaced leaf block"| gc
    dok -->|"the key's value block, plus the current<br/>manifest when the key held a leaf"| gc
    dvs -->|"the removed version's manifest,<br/>plus the replaced leaf or value block"| gc
    dvs -->|"the removed version's<br/>version-state block"| gc
    sa -->|"the replaced version-state block,<br/>plus the replaced leaf block"| gc

    gc -.->|"no reader exists yet"| sweep["catalog GC collector (future):<br/>removes superseded blocks from Forge"]

    classDef external stroke-dasharray: 5 5
    class sweep external
```

- Recording is mandatory, never best-effort: the inserts are written with the
  root's advance in the task's commit, so a failed insert fails the whole
  commit and the root does not move. A change that fails on its own terms
  drops the candidates it recorded along with the rest of its work.
- A retained supersession queues only the replaced **leaf block**; the old
  manifest is not queued because it lives on as a prev-tree entry. Only
  blocks nothing references anymore land here.
- As built, the table holds superseded **value blocks** (manifests and leaf
  blocks) and version-state blocks (a version's lock and tags). Interior MST
  path nodes rewritten by a splice are not recorded;
  [`architecture.md` §4](./architecture.md#4-the-catalog-layer) scopes the
  eventual collector to those as well.
- The body bytes a removed version referenced are handled separately, by the
  claim ledger in the
  [blob lifecycle](#blob-lifecycle-uploading-parked-accepted-released):
  `gc_candidates` remembers catalog blocks, `blob_refs` counts body blobs.

Cross-references: [`architecture.md` §4](./architecture.md#4-the-catalog-layer),
[`s3-versioning.md`](./s3-versioning.md) §7.

Sources: `s3frontend/version.go` (applyPut supersession,
applyDeleteVersion), `s3frontend/object.go` (applyDeleteKey),
`s3frontend/objectlock.go` (applySetAttributes), `bucketop/bucketop.go`
(AddGCCandidate, Mark, Restore), `s3frontend/version.go` (pruneVersionState),
`registry/bucketcommit.go`
(RootCommitWrites). Review when these change.

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
    hilt -->|"the same, for a listing the<br/>command signs with an access key"| walk

    subgraph stores["proof stores"]
        kp["iam.KeyProofs: one DelegationCache<br/>per access key, 24h idle eviction;<br/>also the key's effective S3 action set per bucket"]
        ship["uploader.Forge shipProofs:<br/>per-space store, 1h TTL"]
        commit["uploader.Forge commitProofs:<br/>per-space store, 1h TTL"]
        static["AuthServiceProofs:<br/>static container from config"]
        walk["ingot catalog usage: one store<br/>per bucket walked (AuthorizeSigned)"]
        tok["tokenstore (tokens.cbor):<br/>empty; dormant login paths only"]
    end

    kp -->|"reqscope.ProofStore<br/>on the request ctx"| writes["uploader:<br/>/blob/add, abort, remove, /index/add<br/>/upload/add, /upload/remove"]
    kp -->|"same store"| reads["blockstore.Forge:<br/>content/retrieve"]
    walk -->|"reqscope.ProofStore<br/>on the walk's ctx"| reads
    kp -->|"captureShipProofs<br/>at StartBlob,<br/>borrowable stores only"| ship
    kp -->|"CaptureShipAuthority<br/>at every queue-bucket.commit append,<br/>borrowable stores only"| commit
    commit --> catship["queue-bucket.ship"]
    ship -->|"no commit capture<br/>holding a chain"| catship
    kp -->|"CaptureAuthority when a write<br/>queues a release"| relargs["queue-space-shard.release args:<br/>/blob/remove and /blob/abort chains"]
    ship --> async["releases, aborts and<br/>registration renewals"]
    relargs -->|"read first"| async
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
- The ship authority holds a request's store past the request only when the
  IAM layer marked it borrowable (`reqscope.BorrowableKey`, read through
  `reqscope.BorrowableProofStore`), which it does for the key's own
  `DelegationCache` only. A request authorized off a response carrying a
  revoked delegation spends a store of its own, marked not borrowable.
- A revocation drops the key's store from `KeyProofs`, but a captured store
  stays in use: no chain it assembles uses a delegation the revocation named,
  so the spaces its other grants cover keep shipping.
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
- `ingot catalog usage` is a command, not the daemon, and gets its stores
  another way: with an access key from its environment it signs a
  ListObjectsV2 of each bucket it must read from Forge, has hilt authorize it
  as it would a request the gateway serves, and keeps the delegations for that
  bucket's walk alone. The daemon holds no tenant's secret, so it has no such
  path.
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
`internal/reqscope/reqscope.go`, `cmd/catalog.go` (forgeSegments),
`uploader/forge.go` (shipProofs, captureShipProofs, CaptureShipAuthority),
`uploader/stream.go` (StartBlob), `bucketop/bucketop.go` (Tx.Append),
`s3frontend/releasequeue.go` (releaseAuthority, withReleaseAuthority),
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
full DDL is `migrations/sql/`, annotated in
[`architecture.md` Appendix C](./architecture.md#appendix-c--postgres-schema-the-ingot-schema),
which also covers the task runner's per-queue tables (left out here).

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
        boolean closing "set by DeleteBucket's queue-bucket.close"
    }
    segments {
        bigint seq PK "from segment_seq"
        text plane "'data' arm is dead"
        text state "open, sealed"
        text bucket
        bytea sha256
        bigint shipped_at
        bytea index_digest
        bigint dead_bytes "dead on arrival; NULL = unmeasured"
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
    blob_pending_refs {
        uuid add_task PK "the queue-space-shard.add that took it"
        uuid expire_task UK "the queue-space-shard.expire due at the deadline"
        text space
        bytea digest
        timestamptz deadline "24 h after the add"
    }
    upload_intents {
        text space PK "the upload's space: one cached file, an intent per space"
        bytea digest PK
        bigint size
        text state "uploading, parked, accepted, published (claimed by a commit)"
        text bucket
        timestamptz updated_at "last state change: dates a stalled upload"
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
    multipart_sessions {
        text upload_id PK
        text bucket
        text space "the bucket's space, required: the subject of every release of the session's blobs, never resolved from the bucket name"
        text object_key
        text state "open, completed"
        timestamptz state_changed_at "set at create and by Complete"
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
        text state "parked, accepted: the upload's accept queue"
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
    gc_candidates {
        bytea cid PK "superseded MST node"
        text bucket
    }
    revocation_cursor {
        boolean id PK "single-row latch"
        timestamptz recorded_at "resume point in the revocation stream"
    }

    segments ||--o{ segment_op_roots : "ON DELETE CASCADE"
    multipart_sessions ||--o{ multipart_parts : "ON DELETE CASCADE"
    multipart_parts ||--o| multipart_part_trees : "ON DELETE CASCADE; deleted by Complete once the object commits"
    buckets ||--o{ catalog_journal : "ON DELETE CASCADE"
    segments ||..o{ catalog_journal : "by seq, deliberately no FK"
    buckets ||..o{ segments : "by bucket name, no FK"
    buckets ||..o{ blob_refs : "by bucket name, no FK"
    blob_locations ||..o{ shard_inclusions : "by shard_digest"
    blob_locations ||..o| blob_encryption_params : "by (space, digest), deliberately no FK"
    multipart_parts ||..o{ blob_parks : "digests in blob_digests"
```

- The four solid relationships are the schema's only real foreign keys;
  dashed ones are soft joins the code performs. `revocation_cursor` stands
  alone. `blob_pending_refs` joins nothing: a release counts its rows with
  the `blob_refs` claims and the open sessions' parts by (space,
  digest).
- `catalog_journal` has no foreign key to `segments` on purpose: when a
  segment's CAR is missing from disk, recovery rebuilds its journaled blocks
  into new segments and moves the rows onto them, then deletes the lost row if
  it was open (and retires it if it was sealed).
- `gc_candidates` is write-only (no reader exists yet; its entry paths are the
  [catalog GC candidates](#catalog-gc-candidates-what-gets-remembered-for-removal)
  diagram); `segments.plane`
  still CHECK-allows the deleted `data` arm.
- Session rows also carry the passthrough HTTP headers and checksum columns
  Complete writes into the manifest; intent and location rows carry
  timestamps. See the DDL for the full column lists.

Cross-references: [`architecture.md` Appendix C](./architecture.md#appendix-c--postgres-schema-the-ingot-schema)
(the annotated DDL).

Sources: `migrations/sql/*.sql`. Review whenever a migration is added.

## DeleteBucket teardown order

Teardown crosses every seam in order: close the bucket, unwind in-flight
multipart, prove the bucket empty again, commit its root empty, quiesce the
log, release every network registration, then delete at hilt and locally.
The request runs `queue-bucket.close`, then each upload's abort or reap,
then a `queue-bucket.delete` task, all sync on the bucket's subject except
the multipart tasks, so no commit or ship of the bucket runs beside the
delete. The multipart tasks run from the request because
`queue-multipart-upload` is above `queue-bucket` in the runner's order.

```mermaid
sequenceDiagram
    autonumber
    actor C as S3 client
    participant B as s3frontend.Backend
    participant Q as queue-bucket<br/>(the bucket's subject)
    participant M as queue-multipart-upload<br/>(subject = uploadId)
    participant R as registry<br/>(Postgres)
    participant L as logstore.Manager
    participant U as sprue
    participant H as hilt

    C->>B: DeleteBucket
    B->>R: reg.Get
    B->>Q: run sync: queue-bucket.close
    Q->>R: MST emptiness walk (ErrBucketNotEmpty, or the versioned variant)#59;<br/>mark the bucket closing: from then on queue-bucket.commit and<br/>queue-bucket.admit-upload refuse it with OperationAborted
    loop each multipart session (in-flight uploads do not block the delete, as on AWS)
        B->>M: open: run sync queue-multipart-upload.abort, then releaseNow<br/>(/blob/abort parked blobs)#59; completed: run sync queue-multipart-upload.reap
        Note over B,U: the request's own proofs sign the release legs<br/>(s3:DeleteBucket delegates blob.Abort + blob.Remove)
    end
    B->>Q: run sync: queue-bucket.delete, the signed S3 request in the caller's context (never stored)
    Note over Q,H: the steps below run in the task#59; a delete resumed without<br/>its caller fails without acting
    Q->>R: reg.Get#59; MST emptiness walk again#59; no multipart session may remain
    Q->>R: EmptyRoot (one tx, ahead of the task's own commit: root → empty,<br/>forge root cleared, the bucket's catalog_journal rows dropped)
    Note over Q,R: nothing the bucket reads is released below
    Q->>L: QuiesceBucketLog (seal the open segment, close the store)#59;<br/>no ship is in flight, since ships share the subject
    loop ReleaseRetiredSegments: each retired segment
        L->>U: /blob/remove its CAR + index blob
        L->>R: DeleteSegment (its row, once both are released)
    end
    Q->>L: ShippedSegmentDigests (each unretired CAR + its index blob,<br/>sealed-but-unshipped over-listed, release is idempotent)
    loop each digest
        Q->>U: /blob/remove (release the space's registration)
    end
    Q->>R: drainSpaceReleases: Expedite the space's pending queue-space-shard.release<br/>tasks (subject space/*), run them inline (/blob/remove each)#59;<br/>a release still waiting fails the delete for the client to retry
    Q->>U: DrainSpaceRegistrations: send the space's pending queue-space-upload changes,<br/>then resend its failed retractions with the request's authority
    Q->>H: /s3/bucket/delete with the signed S3 request (hilt refuses while registrations remain)#59;<br/>a refusal clears the closing mark, not found finishes the delete,<br/>a failed call leaves the bucket closing for a retry
    Q->>R: reg.Delete (the local row, its catalog_journal rows cascading)
    Q->>L: RemoveBucketLog, best-effort<br/>(directory + segment rows)
    Q-->>B: done
    B-->>C: 204
```

- The log seams (`QuiesceBucketLog`, `ReleaseRetiredSegments`,
  `ShippedSegmentDigests`, `RemoveBucketLog`) are type-asserted on `blockstore.Log`: a plain `Store`
  without them silently no-ops, which is the trap this diagram documents.
- A failure after the root is committed empty (hilt refusing, a release
  erroring) leaves the bucket functional: it lists empty, the closed store
  reopens lazily on the next write, and a retried delete starts over,
  except for the retired segments already released, whose rows are gone.
  The quiesce leaves the last segment sealed and unshipped until that next
  write; its journal rows went with the root.

Sources: `s3frontend/bucket.go` (DeleteBucket, runDeleteBucket),
`s3frontend/bucketadmin.go` (runCloseBucket, runAdmitUpload),
`s3frontend/bucketqueue.go` (bucketSpec), `s3frontend/multipartqueue.go`
(abortUpload, runAbort, reapNow, runReap),
`s3frontend/releasequeue.go` (drainSpaceReleases),
`s3frontend/uploadqueue.go` (DrainSpaceRegistrations),
`logstore/manager.go`. Review when these change.

## Package-to-diagram map

The review table for changes: find the package you touched, re-check the
listed diagrams (each diagram's `Sources:` footer names its exact files).

| Package / file | Diagrams to review |
|---|---|
| `s3frontend/object.go` | put, get, blob-lifecycle, version-tree, gc-candidates, delete-bucket |
| `s3frontend/version.go` | put, get, multipart, blob-lifecycle, version-tree, gc-candidates |
| `s3frontend/multipart.go`, `multipartqueue.go`, `registry/multipartqueue.go` | multipart, part-states, multipart-states, blob-lifecycle, delete-bucket |
| `s3frontend/acceptor.go`, `registry/accept.go` | multipart, part-states, blob-lifecycle |
| `s3frontend/releasequeue.go` | put, multipart, part-states, blob-lifecycle, delete-bucket |
| `s3frontend/shardref.go` | put, multipart, blob-lifecycle |
| `s3frontend/uploadqueue.go` | put, delete-bucket |
| `s3frontend/shardqueue.go`, `localblobs.go`, `localblob_sweep.go` | put, blob-lifecycle |
| `s3frontend/stream.go` | put, multipart, block-routes, blob-lifecycle |
| `s3frontend/bucket.go` | context, delete-bucket |
| `s3frontend/bucketqueue.go`, `bucketchanges.go`, `bucketadmin.go`, `bucketship.go` | put, multipart, gc-candidates, delete-bucket, logstore-pipeline |
| `bucketop/` | put, get, delete-bucket, packages, principals |
| `taskrunner/` | packages, put, multipart, multipart-states, delete-bucket |
| `blockstore/` | get, packages, block-routes; `forge.go` also context, principals |
| `registry/` | schema, get, blob-lifecycle, logstore-pipeline |
| `logstore/` | logstore-pipeline, segment-states, get, block-routes, delete-bucket |
| `uploader/`, `forgeclient/` | context, put, multipart, principals, logstore-pipeline, block-routes |
| `iam/`, `internal/reqscope/` | principals, authorize, context |
| `cmd/catalog.go` | packages, principals |
| `bucket/`, `mst/` | version-tree, put, get |
| `migrations/sql/` | schema, plus any state diagram naming a changed CHECK |
| `module.go`, `server.go`, `config/` | packages, context, logstore-pipeline; `server.go` also authorize and multipart |
| `s3frontend/policy.go` | context, authorize |
