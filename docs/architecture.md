# Ingot — upload, storage & delete architecture

How Ingot stores S3 objects on the Forge network: the layers it is built from, the data it keeps,
and the flows that move bytes through it. This is the target architecture; it supersedes the
bootstrap MVP that currently lives in the tree.

Companion docs (this directory): [`aggregation-gate.md`](./aggregation-gate.md) — why this S3 design
is gated on Piri's aggregation; [`rfc-pdp-minimum-piece-size.md`](./rfc-pdp-minimum-piece-size.md) —
the measured PDP gas model behind the size knobs (headline figures inlined in
[§2](#2-background--forces) / [§6](#6-the-forgechain-layer)); and its
[`pdp-cost-calculator.html`](./pdp-cost-calculator.html). The catalog-log internals live in the
[`logstore`](../logstore) package.

---

## 1. Overview & principles

Ingot is an **S3 gateway over the Forge network**. It runs as an embeddable Go library (a host like
piri/guppy/sprue imports it in-process) and as a standalone daemon (`ingot serve`). It presents each
S3 bucket as a per-bucket Merkle Search Tree (MST), stores object bodies on Forge storage nodes, and
serves reads back through the same network.

Each **bucket is associated with a Forge space**; the bucket's MST and the objects it references live
in that space. Ingot is a Forge **edge client**: it talks to **Sprue** (the control plane), which
brokers uploads to **Piri** (storage nodes that also run PDP proving and submit to chain), and
publishes blob locations to the **indexing-service**.

The architecture rests on five principles:

- **Object-aligned storage.** An object body is stored as an ordered list of content-addressed
  blobs that belong to that object alone — not interleaved with other objects. Small objects are a
  single blob; large ones are coarsely split.
- **Aggregation is a chain-only concern.** Piri batches blobs into on-chain proof "pieces" for gas
  efficiency, but it stores and serves each blob whole and by digest. Aggregation is therefore
  invisible to storage and retrieval — it matters only for proving and deletion.
- **The catalog names content; the indexer locates it.** Ingot's manifest pins body **digests**
  (stable across any chain-side reshuffling); the indexer maps a digest to its current physical
  location. A read joins the two.
- **Synchronous durability.** A `200` to the S3 client means the data is durable and accepted on
  Piri, not merely buffered locally.
- **Deduplicate, and reference-count to delete.** Identical bytes are stored once; a reverse index
  records which versions reference each digest so bytes are reclaimed only when nothing points at
  them.

---

## 2. Background & forces

The design is shaped by how the Forge upload pipeline works and by a handful of hard constraints.

**The Forge upload lifecycle.** To store a blob, a client drives Piri (via Sprue) through:
`allocate` → `PUT` → `accept`.
- `blob/allocate(digest, size)` reserves an upload slot. Piri returns an upload URL, or — if it
  already holds those bytes — nothing (dedup).
- The client `PUT`s the bytes to that URL. Piri verifies them against the declared digest and writes
  them to object storage (MinIO). The bytes are now **parked**: durable, but not yet proven.
- `blob/accept` commits the blob to Piri's **aggregation** pipeline, which folds blobs into proof
  **pieces**, registers each piece's Merkle root on chain (PDPVerifier), and proves possession over
  time. `accept` also publishes a **location commitment** to the indexer.

**The constraints this imposes:**

- **Digest before upload.** `allocate` requires the content hash and size up front; Piri verifies the
  `PUT` stream against them. A client must therefore know a blob's sha256 before sending it.
- **Bytes are content-addressed and global.** Piri keys blobs by digest in MinIO and dedups
  **across all spaces** (its `Has`/`Delete` take only a digest). Identical content from different
  buckets/spaces is stored once. On chain, all blobs — across every space and bucket — aggregate into
  a **single PDP data set per Piri node**; on-chain identity is node-wide, not per-tenant.
- **Accept is issued by the upload service.** Piri requires the `accept` invocation to come from the
  upload-service DID (Sprue). Today Sprue issues it the moment the client concludes the `PUT`
  receipt — so a client controls *when* accept happens by controlling when it concludes.
- **On-chain cost scales with piece *count*, not size, and nothing is O(live pieces).** Adding a
  piece costs ~120,700 gas at any size; the only size-dependent term is an O(log) proving cost
  (measured — see the gas RFC). Many pieces are batched into one on-chain transaction.
- **On-chain deletion is whole-piece and asymmetric.** A standalone piece deletes in ~88,800 gas
  with no off-chain work. Removing one blob from a multi-blob aggregate has no on-chain primitive:
  the whole piece is removed, the survivors are re-hashed off-chain into a new aggregate, and that is
  re-registered. This asymmetry is why the size knobs ([§6](#6-the-forgechain-layer)) matter.
- **The 256 MiB blob ceiling is a knob, not a wall.** Piri currently caps a blob at 256 MiB because
  it builds the commP Merkle tree in RAM; known improvements (streaming commP) lift this. Ingot
  treats it as a tunable maximum and splits larger objects.
- **Some primitives are not built yet.** There is no `unallocate` for a parked blob; `blob/remove`
  is declared but unhandled; the whole-root on-chain delete is wired but its signature path is
  incomplete; the indexer has no retraction. [§9](#9-the-system-contract-piri--sprue--indexer) enumerates the contract Ingot needs.

---

## 3. The S3 layer

Ingot implements the versitygw `backend.Backend`. It presents standard S3 semantics; the
non-obvious parts:

**Versioning.** Buckets carry a versioning state — `Unversioned` (default), `Enabled`, or
`Suspended`.
- Versions of a key are grouped and ordered **newest-first** in the MST by a composite key, so the
  current version is the head leaf and version-scoped reads are direct seeks — see *Versioned key
  encoding* below.
- `ListObjects` (V1/V2) returns only the current, non-delete-marked version per key (one head per
  key, skip to the next). `ListObjectVersions` walks all leaves. Delete markers are skipped by the
  former, surfaced by the latter (with the `x-amz-delete-marker` response header).
- Overwrite depends on state: `Enabled` creates a new version and retains the prior one;
  `Suspended`/`Unversioned` replaces the `null` version in place and drives the superseded data's
  delete through the reference index ([§5](#5-the-data-layer)).

**Versioned key encoding (`invertedVersionId`).** Because the MST iterates forward only, the current
version is made the head leaf of a key's group — reachable with a single seek — by storing versions
under the composite key `escape(objectKey) ++ TERM ++ invertedVersionId`, whose trailing component
sorts **newest-first**:
- **ordinal** — each version gets a per-bucket monotonic sequence (`buckets.next_version_seq`,
  advanced atomically in the commit path; gaps from retried commits are harmless). It is not
  wall-clock, which avoids clock skew and cross-instance disagreement.
- **`invertedVersionId`** — the ordinal's numeric inverse (`0xFFFF…FFFF − seq`) rendered as a
  **fixed-width 16-char lowercase hex** string. Hex is order-preserving, NUL-free, and valid UTF-8;
  the inverse makes the newest version (largest seq) the lexicographically smallest token, so it
  sorts first within the key's group.
- **`escape(objectKey) ++ TERM`** — an order-preserving, self-terminating encoding so a key's version
  group stays contiguous and is never entered by a prefix-sharing neighbor (`a.png` vs `a.png2`):
  escape `0x01` inside the key as `0x01 0x02`, then terminate with `TERM = 0x01 0x01`, which sorts
  below any continuation (both bytes are non-NUL and valid UTF-8).
- **client `versionId`** — the `x-amz-version-id` is the `invertedVersionId` token itself, so a
  version-scoped request reconstructs the exact key and resolves in a direct O(log n) seek, no scan.

Reads follow from the layout: current = the first leaf of the group; version-scoped = the
reconstructed key; `ListObjectVersions` walks the group (already newest-first); `ListObjects` reads
each head and seeks past the group to the next key. The token + terminator costs ~18 bytes (more if a
key itself contains `0x01`). `MaxKeyBytes` is currently 1024 to match S3's key limit; it can be
raised (e.g. to ~1056) so the version overhead does not shrink the usable S3 key length, at the cost
of a slightly larger maximum leaf.

> **Design decision (for review).** Two choices are baked into this encoding; the defaults below are
> chosen but flagged for the team to ratify.
>
> - **Composite key (chosen) vs. per-key version index.** Keying the MST by `(key, version)` keeps all
>   versions in one sorted space and amortizes well for many-versioned keys. The alternative keys the
>   MST by object key alone, with the leaf pointing at an explicit newest-first version index — which
>   removes the inversion, the escaping, and the key-budget pressure, but rewrites and grows that
>   index block on every new version. Revisit if hot keys accumulate very many versions.
> - **`versionId` as a direct locator (chosen) vs. opaque.** Setting `versionId = invertedVersionId`
>   gives a scan-free, version-scoped lookup, but it **leaks version ordering and approximate write
>   count** (a smaller id is newer). AWS-style opacity would require a random id plus a
>   `versionId → seq` side map (or an HMAC of the seq), losing the direct-locator property. We keep the
>   direct locator; the opacity trade-off is left for the team to weigh.

**ETags.** The ETag is MD5-based, never the sha256 content digest. A whole-object ETag is the MD5 of
the body; a multipart object's ETag is `hex(md5(concat of the N part MD5s)) + "-N"` (matching
versitygw's `GetMultipartMD5`); each part's ETag is the hex of its MD5. MD5 is computed during ingest
alongside sha256 at no extra pass, and stored in the manifest.

**Conditional requests.** versitygw parses `If-Match`/`If-None-Match`/`If-(Un)Modified-Since` and
`x-amz-copy-source-if-*` but delegates evaluation to the backend. Ingot evaluates them against the
resolved version's ETag/Last-Modified (`412`/`304`). A precondition that gates a mutation
(`If-None-Match: *` create-if-absent, `If-Match` optimistic concurrency) is **re-checked at commit**
under the MST critical section, not only at read, so it is race-safe.

**Multipart.** `CreateMultipartUpload` / `UploadPart` / `CompleteMultipartUpload` /
`AbortMultipartUpload` are first-class. The mechanism lives in [§7](#7-cross-cutting-durability-concurrency-retrieval).

**Copy.** `CopyObject` and `UploadPartCopy` are metadata-only operations under dedup: they resolve
the source manifest, write a new version manifest pinning the **same** digest(s), and increment the
reference index — no bytes move, no Piri upload. They honor `MetadataDirective`, cross-bucket sources
(same space), `x-amz-copy-source-if-*`, and (for `UploadPartCopy`) a copy-source range; a multipart
source copies its ordered part-digest list.

**Multi-object delete.** `DeleteObjects` mixes delete-marker insertions and specific-version deletes
(each driving the reference path), caps at 1000 keys, supports Quiet mode, and returns a per-entry
result (not atomic).

**Zero-byte objects.** A 0-byte object stores no blob: Ingot writes a manifest with `size=0`, the
well-known empty MD5 ETag, and no body digest. (Piri's aggregator has no piece for empty content.)

**Checksums.** versitygw threads `x-amz-checksum-*` (CRC32/C, SHA1, SHA256) to the backend; Ingot
validates/echoes them, independent of the internal sha256 content address.

---

## 4. The catalog layer

The catalog is the per-bucket namespace: the MST plus the object manifests it points at.

**The MST** maps each composite key to a manifest CID. It is the forked, go-cid-only MST.

**The manifest** describes one object version: an envelope — key, version id, created/last-modified,
the S3 `etag` stored verbatim (a multipart ETag cannot be re-derived from the bytes), content-type,
system and user headers, and a delete-marker flag for tombstone versions — plus a `Body`. The `Body`
carries the whole-object `size` and `sha256` (integrity) and an **ordered, contiguous list of body
blobs** `[{ digest, offset, length }]` that together cover `[0, size)`: one entry for a small object,
N for a split or multipart object. Each `digest` is the sha256 multihash Piri stores the blob under
and the indexer resolves to a node URL — so this blob list is what lets a ranged GET map a byte range
to the covering blob(s) with no external index.

```shell
MST (bucket)
  leaf key = compositeKey("photos/cat.jpg", invertedVersionId)
     │
     └──▶ CID ──▶ ObjectManifest        ← one dag-cbor block, catalog plane
                    ├ key          "photos/cat.jpg"
                    ├ versionId    "v_01J8QX…"          (MST key holds an inverted form)
                    ├ created      "2026-06-17T"…       (last-modified)
                    ├ etag         "9b2cf…-3"           (md5-of-md5s-N if multipart, else md5 hex)
                    ├ contentType + http headers + user metadata
                    ├ deleteMarker "false"              (true ⇒ tombstone, no body)
                    └ body
                        ├ size     629_145_600          (600 MiB total)
                        ├ sha256   <whole-body digest>  (integrity)
                        └ blobs[] ── ordered, contiguous, covers [0, size) ──┐
                                                                             │
          ┌───────────────────────┬───────────────────────┬──────────────────┘
          ▼                       ▼                       ▼
       BlobRef #0              BlobRef #1              BlobRef #2
       digest D0               digest D1               digest D2
       offset 0                offset 256 MiB          offset 512 MiB
       length 256 MiB          length 256 MiB          length 88 MiB
          │                       │                       │
   indexer.locate(Dn) ─────▶ Piri node URL ─────▶ ranged HTTP GET (per blob)
          │                       │                       │
   each blob: its own on-chain PIECE if ≥ min, else a subroot in an aggregate
```

*Example: a 600 MiB object split at `max_blob_size` = 256 MiB into three blobs. This is the **target**
manifest shape — today's `bucket/manifest.go` (single `Body.Content` DAG, MD5-derived ETag, no inline
`versionId`/`deleteMarker`) is the MVP it supersedes.*

**The catalog plane.** MST nodes and manifests are dag-cbor blocks. They are batched into CAR
segments and shipped to Piri so the catalog is recoverable, following the existing log-structured
plane model (`logstore`). A mutating operation ships **only the blocks it newly created** — the new
manifest and the MST nodes added by the splice — never MST nodes that already exist and are already
durable. The catalog plane is the per-operation delta.

Because many tiny blocks share a CAR, catalog retrieval uses the indexer's **index-claim /
sharded-dag-index path** (block CID → byte range within its CAR shard). This two-level lookup is
retained for the catalog; object bodies do not use it ([§8](#8-retrieval-addressing-why-bodies-need-no-sharded-dag-index)).

**Superseded MST nodes** (from overwrites and deletes) are recorded in a `gc_candidates` table.
They are not collected in this iteration — the catalog accumulates on Piri with mutation volume,
an accepted cost until catalog GC exists ([§9](#9-the-system-contract-piri--sprue--indexer)).

---

## 5. The data layer

The data layer turns an object body into stored blobs and tracks who references them.

**Object → blobs.** A body is hashed (sha256 for content addressing, md5 for the ETag) in a single
streaming pass and written to the local store. It becomes an ordered list of content-addressed
blobs, each `≤ max_blob_size`: one blob for objects within the ceiling, a coarse split (e.g. 256 MiB
granularity, not fine chunking) for larger ones. Each blob is uploaded to Piri by digest ([§7](#7-cross-cutting-durability-concurrency-retrieval)).

**The local store (spool + cache).** Each blob is written locally before upload — both because the
digest must be known before `allocate`, and because that local copy does double duty:
- **Read-after-write floor (mandatory):** a just-written object is served from the local copy until
  the indexer can durably resolve it. The hazard is a gap in the indexer's caching:
  1. At `accept`, Piri publishes the blob's location to the indexer, which caches it right away — so
     a normal GET resolves at once. But that cached **hit** has only a ~1 h TTL.
  2. The durable, long-term resolution comes from an IPNI advertisement that propagates **separately
     and more slowly**. If the hit expires before IPNI has caught up, there is a window where the
     indexer can find nothing.
  3. The catch: a lookup that lands in that window returns *not found*, and the indexer **caches that
     failure too** — a *negative* cache entry, also ~1 h. So one ill-timed miss makes a
     fully-stored object look missing for up to an hour, even after IPNI catches up.

  To stay out of that window, a blob's local copy is retained until the object is **published** —
  confirmed by an independent, cache-cold indexer probe actually resolving the digest (or a fixed
  margin past the TTL) — not merely until `accept` returns.
- **Read cache (optional, recommended):** beyond that floor, the local store serves hot reads
  directly, skipping the indexer→Piri round-trip. Read-after-write retains *recently written* data;
  a cache retains *recently read* data, so the two may use distinct eviction policies over a shared,
  bounded, size-configurable store. The alternative — a near-stateless Ingot that resolves every read
  through the indexer — trades latency for simpler horizontal scaling; it is a supported mode, but
  the read-after-write floor holds regardless.

The `upload_intents` table tracks each in-flight blob: `digest → { local_path, size, state:
spooled│parked│accepted│published, owner ref }`. It drives read-after-write, cache lookup, and crash
recovery. The Postgres schema for this and every other Ingot table is in **[Appendix C](#appendix-c--postgres-schema-the-ingot-schema)**.

**Dedup and the reference index.** Piri stores identical bytes once (it answers `allocate` with
"already have it" when the digest exists), so one blob can back many object versions — a re-PUT of
identical bytes, a `CopyObject`, the same content in two buckets. The reverse index makes deletion
safe: **dedup stores the bytes once; the index lets them be deleted once.**
- `blob_refs`: `(space, digest) → { referencing versions }`, in Postgres. It is the queryable
  reverse of what manifests already record.
- A commit adds `(bucket, key, versionId)` to `blob_refs[(space, digest)]` for each body digest.
  A version delete (or null-version overwrite, or lifecycle expiry) removes it; when the set empties
  for that space, the space releases its claim ([§9](#9-the-system-contract-piri--sprue--indexer)).

**Sprue's two dedup paths** (the data layer must model both):
- A **within-space** re-PUT short-circuits in Sprue, which replays the existing claim — no PUT, no
  new accept, same location returned. The reference index simply gains another version.
- A **cross-space** dedup hit (bytes present from another space) returns no upload URL but still
  mints a fresh per-space location claim.

Ingot calls `blob/add` uniformly and lets Sprue decide; the write path never assumes it re-accepts.

---

## 6. The Forge/chain layer

This layer is how blobs become proven storage, and how the size knobs govern deletion.

**Storage.** `allocate` → `PUT` parks a blob in MinIO keyed by its digest. `accept` commits it to
aggregation and publishes its location commitment to the indexer. Storage and retrieval are always
per-blob by digest; the chain layer below never moves bytes.

**Aggregation and the size knobs.** Two parameters, shared by Ingot and Piri, govern on-chain piece
composition (not storage — every blob is stored whole regardless):

- **`min_aggregate_size`** — the deletion-granularity knob (today a hardcoded 128 MiB). A blob
  **≥ min** becomes its **own** on-chain piece. A blob **< min** is folded with other small blobs
  into a shared aggregate piece (built up to ~min).
- **`max_blob_size`** — the largest blob Piri accepts (currently 256 MiB, liftable). Larger objects
  are split into `≤ max` blobs by the data layer.

`min` is the central lever because on-chain cost is **count-driven, not size-driven** (gas RFC). A
small `min` means most objects are their own piece, so most deletes are O(1) (below); the price is
more pieces — more `addPieces` transactions, a one-time registration tax, and an O(log) proving
ratchet under churn — all bounded, none a per-live-piece cost. The RFC's proposed knee is **8 MiB**
(with 16–32 MiB as a fallback if transaction count or base-fee spikes bite). The aggregator keeps
aggregates `≤ max`, and the batch submitter respects the contract's `extraData` cap — the binding
gate is the PDPVerifier's `EXTRA_DATA_MAX_SIZE` = 2048 B (~13 pieces/tx), not the larger WSS limit
(gas RFC).

> **Design decision (for review).** `min`/`max` are the central knobs: `min` trades on-chain piece
> count (transactions, the one-time registration tax, the proving ratchet) against deletion
> granularity (how many objects get O(1) deletes vs. compaction). Starting proposals are `min` = 8 MiB
> and `max` = 256 MiB; ratify against the base-fee and transaction-count budget.

**Deletion has two regimes,** decided by which side of `min` a blob falls:
- **Regime A — blob ≥ `min` (its own piece): O(1) delete.** Schedule the piece for deletion
  (~88,800 gas); no off-chain re-hash. This is the path most objects take.
- **Regime B — blob < `min` (aggregated): compaction.** Schedule the aggregate piece for deletion,
  re-hash the surviving sub-`min` blobs into a fresh aggregate (cheap — ≤ ~`min`, e.g. 8 MiB, not
  256 MiB), and register the new piece. The bytes never move; compaction consumes the persisted
  survivor subroot commPs + offsets to recompute a fresh Merkle root (the old inclusion proofs are
  discarded). This is the sub-`min` tail.

**The delete primitives** are deliberately distinct:
- **`unallocate(digest)`** retires a **parked** blob (PUT, never accepted): delete the MinIO bytes
  and the allocation record. No chain involvement.
- **`remove(digest)`** releases a **space's claim** on an **accepted** blob. Because dedup is global,
  Piri deletes the bytes and retires the piece only when the per-`(digest, space)` claim count
  reaches zero across **all** spaces (Piri keeps per-`(digest, space)` allocation rows to count on).
  At zero, an own-piece blob takes Regime A and an aggregated blob takes Regime B.

**Bucket teardown.** All blobs share one node-wide PDP data set, so `deleteDataSet` would tear down
the entire node's storage — it is not a per-bucket operation. Deleting a bucket is the ordinary
machinery fanned out: drop the bucket's manifests (its MST nodes go to `gc_candidates`), and `remove`
each distinct body digest its versions reference — each taking Regime A or Regime B by blob size, and
each physically deleted only when no other space still claims it. A large bucket's deletes spread
across proving periods under the `MAX_ENQUEUED_REMOVALS` cap.

**Indexer.** `accept` publishes a per-blob location commitment (`Content = digest`, whole-blob
range). Deletion requires the indexer to retract a commitment keyed by `(space, digest)` — the cache
entry and IPNI advertisement are keyed by `contextID = encode(space, content)`, so a digest alone
cannot name what to remove.

`MAX_ENQUEUED_REMOVALS = 2000` per proving period bounds bulk deletes (spread across periods).

---

## 7. Cross-cutting: durability, concurrency, retrieval

The layers above are static structure; here is how requests flow through them. The bucket-root
update is a **guarded root swap**: advance the bucket's MST root only if it still equals the snapshot
read at the start of the operation; on mismatch, reload and retry the (cheap) splice.

### 7.1 Write (single-shot `PutObject`)

```
0. PRECONDITIONS (no lock)   evaluate If-Match / If-None-Match / If-(Un)Modified-Since (§3)
1. INGEST  (no lock)         stream body → sha256 + md5 in one pass → local store;
                             if size==0 store a manifest only (§3);
                             else split into ceil(size / max_blob_size) blobs
2. UPLOAD  (no lock, per blob, keyed by digest)
     blob/add(digest,size) → PUT (unless deduped) → PARKED
     trigger accept (conclude the PUT receipt) → Sprue issues blob/accept
       → Piri publishes the location commitment; the blob enters aggregation
3. COMMIT  (short per-bucket critical section)
     load MST at base root; allocate versionId
     write the manifest; splice key+versionId → manifest CID (new MST nodes)
     blob_refs[(space,digest)] += this version
     if Suspended/Unversioned and a prior null-version existed:
        blob_refs[(space,oldDigest)] -= it; on empty → remove (§6)
     record superseded MST nodes → gc_candidates; guarded root swap
4. ACK     return 200 + ETag(md5)   — data is durable+accepted in Piri
```

Ingest, hashing, and the Piri upload all run **outside any lock** (upload is keyed by digest, not
bucket). The critical section does no large-body work — a manifest write, an MST splice, a few
Postgres writes, and the guarded root swap — so it is small and bounded (sub-millisecond order). No
object is ever held whole in RAM. The catalog manifest is written only after its body blobs are
durable and accepted in Piri, so a crash never leaves a catalog entry pointing at non-durable data.

### 7.2 Multipart

Parts upload concurrently; **accept is triggered only at Complete**, so an Abort only ever has to
clean up parked blobs.

```
CreateMultipartUpload → uploadId, session (state=open)
UploadPart(n)         → ingest + split + blob/add + PUT → PARKED (no accept); record part
                        ETag = hex(part MD5)
CompleteMultipartUpload([parts])
     latch session open→completing            (§7.3); validate parts (S3 rules)
     trigger accept for every part's blobs
     COMMIT (short lock): manifest pins ordered [blobs..]+ranges;
        object ETag = hex(md5(concat part MD5s)) + "-N";
        splice; blob_refs += version per digest; guarded root swap
AbortMultipartUpload
     latch session open→aborting
     parked blobs → unallocate;  already-accepted (deduped) blobs → remove (§6)
```

A completed multipart object is the ordered union of its parts' blobs plus a manifest of byte ranges;
parts are not restitched.

### 7.3 The session latch (the Abort/Complete race)

`Complete` and `Abort` can arrive concurrently for one `uploadId`. Without coordination they collide
on the parts — `Complete` triggering accept while `Abort` unallocates those same parts — leaving the
object half-built or half-deleted. A **single-winner latch** prevents it: an atomic state transition
on the session row (`UPDATE … SET state=? WHERE state='open'`). Exactly one of
`Complete`→`completing` or `Abort`→`aborting` wins; the loser observes the moved row and returns an
error. Accept is triggered only after the session is latched `completing`, so accept and unallocate
never touch the same part concurrently. A deduped part is already accepted (not parked), so Abort
removes it via the reference path rather than unallocating it.

### 7.4 Read (`GetObject`)

```
GET key[?versionId]   (after §3 precondition checks)
   MST → manifest → ordered [blobDigest, byteRange] list  (catalog via the index-claim path)
   per covering blob:
     in local cache/store → serve locally
     else indexer.locate(space, digest) → Piri URL → UCAN /content/retrieve, ranged GET, length-check
   stream to client (concatenating across blobs for split/multipart objects)
```

Object-body retrieval is per-blob by digest and aggregation-invisible — compaction can move a blob
between pieces with no effect on a read. A ranged GET is a ranged fetch of the covering blob(s), not
a walk over sub-blocks.

### 7.5 Concurrency, durability, and failure modes

The only serialized work is the commit critical section, and under versioning two writes to the same
key produce distinct versionIds — they contend only on the guarded root swap, which retries cheaply.
Across processes, the swap (a Postgres conditional update) is the cross-instance guard; `blob_refs`
updates are transactional with the commit.

| Case | Handling |
|---|---|
| Crash after PUT, before accept | Bytes parked; `upload_intents` (parked) drives resume or `unallocate`. No `200` was sent. |
| Crash after accept, before commit | Blob durable but unreferenced; `upload_intents` (accepted) drives commit-retry or `remove`. |
| Guarded-root-swap mismatch | Reload root, re-splice; blobs already durable, never re-uploaded. |
| Concurrent PUT, same key | Distinct versionIds; serialize only on the swap. |
| Concurrent PUT, same content | One blob; both versions reference it; `blob_refs` counts both. |
| Abort vs racing Complete | Single-winner latch ([§7.3](#73-the-session-latch-the-abortcomplete-race)). |
| Delete of a digest still referenced by any space | Claim non-empty → no physical delete; only the version/MST entry removed. |
| 0-byte object | No blob; manifest-only, empty MD5 ETag. |
| Conditional request | Backend-evaluated; re-checked at commit for mutations. |

---

## 8. Retrieval addressing (why bodies need no sharded-dag-index)

Object bodies resolve straight from a digest: `accept` publishes an `/assert/location` commitment
keyed by the blob's own digest with a whole-blob range, and the manifest's ordered blob list carries
the byte-range map for split/multipart objects. So body retrieval needs only a digest → location
lookup, no per-CAR sharded-dag-index.

The **catalog** is different — many tiny MST/manifest blocks share a CAR — so catalog blocks resolve
via the indexer's index-claim / sharded-dag-index path (block CID → byte range in its shard). That
path is retained for the catalog; only the data plane drops it.

Consuming a bare location commitment is a capability Ingot's locator must gain: today it surfaces a
location only via the inclusion → shard → commitment path and never returns a stored bare
commitment. The locator (`blockstore/locator/indexlocator.go`) and forge reader
(`blockstore/forge.go`) change so a query result whose commitment `Content` equals the queried digest
is surfaced directly as a location.

---

## 9. The system contract (Piri / Sprue / indexer)

Ingot's correct operation depends on these capabilities from the rest of the stack. Several exist;
several are partial or to-build (Ingot owns the whole stack, so these are design items, not
negotiations).

| Capability                                                                                     | Service                         | Status       | Notes                                                                                                                                                                                                               |
|------------------------------------------------------------------------------------------------|---------------------------------|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `allocate` / `PUT` / `accept` blob lifecycle                                                   | Piri/Sprue                      | **exists**   | The storage primitive Ingot builds on.                                                                                                                                                                              |
| Ingot-timed accept (PUT a part, defer the conclude until Complete)                             | Ingot + Sprue                   | **partial**  | Accept already fires on the client's conclude; needs the park-vs-conclude split client-side and a separable Sprue conclude. Ingot does **not** issue `accept` (Piri requires the upload-service DID).               |
| `unallocate(digest)` — drop a parked blob                                                      | Piri + Sprue + libforge         | **to-build** | `blob/remove` arg type exists in libforge; no handler.                                                                                                                                                              |
| `remove(digest)` — per-space claim release; physical delete/piece-retire at zero global claims | Piri + Sprue + libforge         | **to-build** | Piri keeps per-`(digest,space)` allocation rows to count on.                                                                                                                                                        |
| Configurable, adaptive size policy (`min`/`max`); batch guard for the `extraData` cap          | Piri                            | **partial**  | `MinAggregateSize` is hardcoded 128 MiB; lower to ~8 MiB and make configurable; the binding `extraData` cap is the PDPVerifier `EXTRA_DATA_MAX_SIZE`=2048 (~13 pieces/tx), so keep `BatchSize` ≤ ~13 (default 10 already complies) with an explicit 2048 guard (gas RFC). No contract change (lifting the cap itself needs a PDPVerifier upgrade). |
| Compaction (Regime B) + complete the on-chain delete signature                                 | Piri                            | **partial**  | Whole-root delete is wired but its `extraData` signature is incomplete; compaction (remove + re-hash survivors + re-add) is new.                                                                                    |
| De-dup at accept (don't re-aggregate a digest already a live piece)                            | Piri                            | **to-build** | Backstops one-piece-per-content once accept timing is Ingot-driven.                                                                                                                                                 |
| Parked-allocation GC + honor `Expires`                                                         | Piri                            | **to-build** | Bounds leakage when an abort never arrives.                                                                                                                                                                         |
| Indexer delete by `(space, digest)` / location-claim CID                                       | Indexer + Sprue                 | **to-build** | IPNI removal mechanics are the indexer's.                                                                                                                                                                           |
| Bare location commitment served by blob digest                                                 | Indexer                         | **exists**   | `accept` publishes it; the indexer caches it. Ingot's locator must learn to consume it ([§8](#8-retrieval-addressing-why-bodies-need-no-sharded-dag-index)).                                                        |
| Batch `allocate`/`accept`                                                                      | libforge + Sprue + Piri + Ingot | **to-build** | New batch command shapes; per-element receipts; RPC amortization, still per-blob claims.                                                                                                                            |
| Lift the 256 MiB blob ceiling (streaming commP)                                                | Piri                            | **optional** | Reduces large-object splitting; not required for the first cut.                                                                                                                                                     |
| `allocate-by-size` + bind-digest-after (remove the spool, same-rack)                           | Piri + Sprue + Ingot            | **optional** | [§10](#10-deployment-topology--the-digest-before-upload-cost). Needs a size-bounded, possibly authenticated upload route.                                                                                           |

**Determinism / idempotency Ingot must preserve.** The `accept` invocation is built deterministically
(stable CID, today via `WithNoNonce` over `{space, digest, size, put-task}`); re-driving accept must
reuse the same put-task link. `remove`/`unallocate` must be idempotent. The forge-root advance must
happen only after a successful guarded root swap — the catalog log currently advances it before the
swap, so a mismatch can leave the forge root pointing at a bucket root that was never adopted.

---

## 10. Deployment topology & the digest-before-upload cost

Piri requires the digest before issuing an upload slot, so Ingot writes each blob to the local store
and hashes it before the first byte reaches Piri (the local write is needed for the cache/read-after-
write anyway, so it is less pure overhead than it appears, but it does serialize hash-before-upload).

Whether to remove it is topology-dependent:
- **Same-rack (current): Ingot and Piri share a rack, the client is remote.** Pre-hashing buys no
  trust, so an `allocate-by-size` + bind-digest-after-verify protocol (Piri assigns a slot by size,
  hashes the stream during the PUT, binds the digest) would let Ingot pipeline hashing with upload —
  at the cost of a size-bounded, possibly authenticated upload route.
- **Client-colocated (future): Ingot in the client's datacenter, Piri the only remote hop.**
  Pre-hashing buys integrity, so digest-before-upload earns its keep.

The first cut keeps digest-before-upload; `allocate-by-size` is a same-rack fast-follow.

---

## 11. Known gaps & open questions

- **Object-size distribution through Ingot** decides how often deletes take Regime A (O(1)) vs
  Regime B (compaction), and the true `addPieces` transaction count — the main input to the `min`
  choice.
- **`min`/`max` final values** — 8 MiB / 256 MiB are the starting proposals; confirm against the
  base-fee and transaction-count budget.
- **Lifting the 256 MiB ceiling** (streaming commP) versus the simplicity of coarse splitting.
- **Local cache vs near-stateless** — cache sizing/eviction, or commit to near-stateless.
- **Catalog GC** — `gc_candidates` is a write-only log this iteration; the catalog CARs on Piri grow
  with mutation volume until a collector exists.
- **Monotonic `nextPieceId` ratchet** under heavy churn (gas RFC) — possibly periodic dataset
  rotation.
- **Compaction trigger policy**, the **`allocate-by-size`** security model, and the **batch**
  allocate/accept wire format.
- **Versioned key encoding** ([§3](#3-the-s3-layer)) — ratify composite-key vs. per-key version index, and
  direct-locator vs. opaque `versionId` (the direct locator leaks version ordering / approximate
  write count); and whether to raise `MaxKeyBytes` so versioning doesn't shrink the usable S3 key.

---

## Appendix A — Why the architecture looks like this

The MVP this supersedes had six structural problems; each is resolved by a layer above.

| Problem                                       | Resolved by                                                                                                                                                                                                         |
|-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| A per-bucket lock held across the whole write | Ingest and upload run off-lock; only the MST splice + guarded root swap is in the critical section. [§7.1](#71-write-single-shot-putobject)                                                                         |
| The whole object buffered in memory           | Hash-while-writing to the local store; stream to Piri; nothing held whole in RAM. [§5](#5-the-data-layer), [§7.1](#71-write-single-shot-putobject)                                                                  |
| No multipart, no abort                        | Parked part-blobs, accept-at-Complete, single-winner latch; abort = `unallocate` (parked) / `remove` (deduped). [§7.2](#72-multipart)–7.3, [§9](#9-the-system-contract-piri--sprue--indexer)                        |
| Objects chunked into CARs to obtain a digest  | Object body = one blob (≤ `max`) or a coarse `≤ max` split; no fine chunking, no CAR. [§5](#5-the-data-layer), [§10](#10-deployment-topology--the-digest-before-upload-cost)                                        |
| No delete of superseded data                  | Reference index + per-space `remove` + Piri's global claim gate + indexer delete; O(1) for ≥ `min` blobs. [§5](#5-the-data-layer), [§6](#6-the-forgechain-layer), [§9](#9-the-system-contract-piri--sprue--indexer) |
| Aggregation blocked partial deletes           | A small `min` makes most blobs their own piece (O(1) delete); compaction handles only the sub-`min` tail. [§6](#6-the-forgechain-layer)                                                                             |

## Appendix B — Glossary

- **Space** — a Forge tenancy/identity; each Ingot bucket is associated with one. Piri stores across
  many spaces and dedups bytes globally by digest.
- **Bucket** — an S3 bucket; one per-bucket MST, associated with a space.
- **Blob** — `(sha256 digest, bytes)`, the unit Piri stores; `≤ max_blob_size`.
- **Piece** — the on-chain proof unit (a CommP Merkle root). A blob ≥ `min` is its own piece; smaller
  blobs share an **aggregate** piece as **subroots**.
- **Parked** — a blob that has been PUT but not yet accepted: durable in MinIO, not yet proven.
- **Manifest** — the dag-cbor record for one object version: ordered blob list + size/sha256 +
  etag/metadata.
- **MST** — the per-bucket Merkle Search Tree mapping composite keys to manifests.
- **Plane** — a shipping pipeline. The **data** plane is object blobs (direct to Piri); the
  **catalog** plane is MST nodes + manifests (CAR-batched).
- **Claim** — a space's reference to a blob; `remove` releases it, and Piri deletes bytes when no
  space holds a claim.
- **Spool / local store** — Ingot's on-disk copy of a blob; the read-after-write source and the read
  cache.
- **Guarded root swap** — advancing the bucket MST root only if it still matches the snapshot read at
  the start of the operation.
- **Single-winner latch** — the atomic session-state transition that serializes multipart
  Complete vs Abort.
- **Reference index (`blob_refs`)** — `(space, digest) → versions`, the reverse map that gates
  deletion.
- **IPNI** — InterPlanetary Network Indexer; the indexer's durable, network-wide advertisement layer
  that backs long-term location resolution and propagates asynchronously (the read-after-write gap in
  [§5](#5-the-data-layer)).
- **commP** — the Filecoin piece commitment: the Merkle-root CID a PDP piece is identified and proven
  by.
- **subroot** — a member blob's commP inside an aggregate piece; the aggregate's on-chain root is the
  Merkle root over its subroots.
- **PDPVerifier** — the on-chain contract that records pieces (proof roots) in a data set and verifies
  possession proofs.
- **contextID** — the indexer/IPNI key for a published claim, `encode(space, content)`; a deletion
  must name it, so it is per-`(space, digest)`, not per-digest.

## Appendix C — Postgres schema (the `ingot` schema)

The relational tables the design relies on. Object **manifests** and **MST nodes** are not rows here —
they are content-addressed dag-cbor *blocks* in the catalog plane ([§4](#4-the-catalog-layer)). The catalog plane's CAR
**segment** metadata and per-operation **op-roots** (which advance `buckets.forge_root_cid` on ship)
are owned by the [`logstore`](../logstore) package; they are not
redefined here. CIDs and blob digests (sha256 multihashes) are stored as `bytea`, matching today's
`ingot` schema (`migrations/sql/*.sql`).

```sql
-- One row per bucket: the space its data lives in, the S3 versioning state, the
-- committed MST root, and the root that is durable on Forge (lags until the
-- catalog plane ships).
CREATE TABLE ingot.buckets (
    name            text PRIMARY KEY,
    space           text NOT NULL,                       -- Forge space DID
    versioning      text NOT NULL DEFAULT 'unversioned'
                        CHECK (versioning IN ('unversioned','enabled','suspended')),
    root_cid          bytea,                             -- committed MST root (locally durable)
    forge_root_cid    bytea,                             -- MST root durable on Forge (lags root_cid)
    next_version_seq  bigint NOT NULL DEFAULT 0,         -- per-bucket version ordinal (§3, invertedVersionId)
    created_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX buckets_space_idx ON ingot.buckets (space);

-- Reverse index (§5, §6): which object versions reference each blob. One row per
-- (digest, version). A blob's space-claim is released when no rows remain for
-- (space, digest); Piri then deletes the bytes once no space claims the digest at
-- all. `space` is denormalized from buckets for a direct claim query.
CREATE TABLE ingot.blob_refs (
    digest      bytea NOT NULL,                          -- sha256 multihash of the blob
    bucket      text  NOT NULL,
    object_key  text  NOT NULL,
    version_id  text  NOT NULL,
    space       text  NOT NULL,                          -- = buckets.space (denormalized)
    PRIMARY KEY (digest, bucket, object_key, version_id)
);
-- Drives "is (space, digest) still claimed?" — the gate on remove(digest).
CREATE INDEX blob_refs_claim_idx ON ingot.blob_refs (space, digest);

-- The local-store index (§5): every blob Ingot holds on disk, in-flight or
-- retained as cache. Drives read-after-write, cache lookup, and crash recovery.
-- state advances spooled → parked → accepted → published; cache eviction deletes
-- the row and the file.
CREATE TABLE ingot.upload_intents (
    digest      bytea PRIMARY KEY,                       -- sha256 multihash of the blob
    local_path  text   NOT NULL,
    size        bigint NOT NULL,
    state       text   NOT NULL
                    CHECK (state IN ('spooled','parked','accepted','published')),
    bucket      text,                                    -- owner ref (originating op), for cleanup
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- One row per in-flight multipart upload (§7.2). `state` is the single-winner
-- latch (§7.3): Complete and Abort race to move it off 'open'. content_type and
-- metadata are carried from CreateMultipartUpload so Complete can write the
-- manifest without the client resupplying them.
CREATE TABLE ingot.multipart_sessions (
    upload_id     text PRIMARY KEY,
    bucket        text NOT NULL,
    object_key    text NOT NULL,
    state         text NOT NULL DEFAULT 'open'
                      CHECK (state IN ('open','completing','aborting')),
    content_type  text,
    metadata      jsonb,                                 -- user metadata + system headers
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- One row per uploaded part. A part larger than max_blob_size maps to several
-- blobs, so blob_digests is an ordered array. state is 'parked' until Complete
-- accepts the part's blobs (or 'accepted' immediately on a dedup hit, §5).
CREATE TABLE ingot.multipart_parts (
    upload_id     text   NOT NULL REFERENCES ingot.multipart_sessions(upload_id) ON DELETE CASCADE,
    part_number   int    NOT NULL,
    etag_md5      bytea  NOT NULL,                       -- md5 of the part bytes (the part ETag)
    size          bigint NOT NULL,
    blob_digests  bytea[] NOT NULL,                      -- ordered; one entry per ≤ max_blob_size blob
    state         text   NOT NULL DEFAULT 'parked'
                      CHECK (state IN ('parked','accepted')),
    PRIMARY KEY (upload_id, part_number)
);

-- Superseded MST node CIDs, recorded on overwrite/delete (§4, §9). Write-only
-- this iteration — no catalog GC yet; a future collector consumes it.
CREATE TABLE ingot.gc_candidates (
    cid         bytea PRIMARY KEY,                       -- superseded MST node CID
    bucket      text,
    created_at  timestamptz NOT NULL DEFAULT now()
);
```

Relationships at a glance: `blob_refs`, `multipart_sessions`, `multipart_parts`, and `gc_candidates`
all reference a `buckets.name`; `multipart_parts` cascades from `multipart_sessions`. The reference
count for a blob is `count(*) from blob_refs where space = ? and digest = ?`; reaching zero triggers
`remove(digest)` ([§6](#6-the-forgechain-layer)). `upload_intents` is keyed by the blob digest and is independent of bucket
namespace — it is the disk-side index, shared across whatever objects reference the same bytes.
