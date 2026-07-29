# S3 versioning — the per-key version-tree design

Implementation spec for S3 bucket versioning in Ingot. It supersedes the *versioned key
encoding (`invertedVersionId`)* section of [`architecture.md` §3](./architecture.md#3-the-s3-layer):
the composite-key scheme (escape + `TERM` + inverted hex ordinal) is **dropped** in favor of the
per-key version-tree leaf proposed in review
([PR #2, r3440362078](https://github.com/fil-forge/ingot/pull/2/changes#r3440362078), Hannah
building on Peeja's leaf idea). Everything else in `architecture.md` — the write path, the
reference index, the catalog plane — stands and is extended, not replaced, by this doc.

---

## 1. Why the composite key lost

The composite-key design derived the client `versionId` from the per-bucket ordinal
(`invertedVersionId`) — one value doing two jobs. S3 keeps those jobs separate, and the **null
version** is where collapsing them breaks:

- **`seq`** — a per-bucket monotonic ordinal. Its only job is *ordering* (recency). Internal;
  never leaves the server.
- **`version_id`** — a string, the S3 client handle (`x-amz-version-id`). Its job is *identity*.
  It is opaque, and for suspended/unversioned writes it is the literal string `"null"`.

For numbered versions the two can coincide. They diverge for the null version: its `version_id`
is frozen at `"null"` by S3, but it still has a real position in the ordering (a `seq`). There is
only ever **one** null version per key, replaced in place — and it can sit anywhere in the
version stack (a bucket that was written unversioned, then Enabled, holds its null version *under*
newer numbered versions). An encoding that derives the id from the ordinal has to special-case all
of that; keeping `seq` and `version_id` as separate fields makes it fall out.

Dropping the composite key also deletes its costs: no `0x01` escaping, no terminator, no
key-budget pressure — **`mst.MaxKeyBytes` stays 1024** and the top-level MST keys remain plain S3
object keys.

---

## 2. Data model

### 2.1 The leaf

The top-level (per-bucket) MST continues to map **plain object keys** to CIDs. The value changes:
it now points at an **`ObjectLeaf`** block instead of directly at an `ObjectManifest`.

```go
// bucket/leaf.go (new; cborgen via gen/main.go, same style as ObjectManifest)

// VersionNode identifies one object version: its ordinal, its client id, and
// its manifest.
type VersionNode struct {
    Seq       uint64  `cborgen:"s"` // per-bucket ordinal; ordering only, never exposed
    VersionID string  `cborgen:"v"` // client handle: "null" or a ULID token (§3)
    Manifest  cid.Cid `cborgen:"m"` // ObjectManifest CID
}

// ObjectLeaf is the per-key version group. The top-level MST maps
// objectKey -> CID(ObjectLeaf).
type ObjectLeaf struct {
    // Current is the head version — what GET/HEAD/ListObjects resolve with a
    // single leaf read, no descent.
    Current VersionNode `cborgen:"c"`
    // Prev is the root of the per-key MST of noncurrent versions (§2.2), or
    // nil when there are none.
    Prev *cid.Cid `cborgen:"p"`
    // NullSeq is the seq of the null version *when it is noncurrent* (an entry
    // in Prev); 0 when the null version is current or absent. Needed because
    // Prev is keyed by seq and the null version's id ("null") does not encode
    // its seq.
    NullSeq uint64 `cborgen:"n"`
}
```

### 2.2 The prev tree

Noncurrent versions live in a **per-key sub-MST** (the same forked `mst` package, same space, same
blockstore — its nodes are ordinary catalog-plane blocks):

- **key** — `revSeqKey(seq)`: fixed-width 16-char lowercase hex of `math.MaxUint64 − seq`.
  Because the MST iterates forward-only (`mst.go:835 WalkLeavesFrom`), the inversion makes
  iteration **newest-first**. Scoped to one object key, the key is *just an integer rendered
  order-preservingly* — no composite-string escaping. Valid under `mst.IsValidKey` (hex is
  UTF-8, NUL-free, short).
- **value** — the version's `ObjectManifest` CID. `Seq` is recoverable from the key;
  `VersionID` comes from the manifest (§2.3), which every consumer of a prev entry fetches
  anyway (list rendering needs ETag/size/mtime; version-scoped reads need the body).

A direct version-scoped seek is `prev.Get(revSeqKey(seq))` — O(log n), no scan.

### 2.3 Manifest additions

`ObjectManifest` (bucket/manifest.go) gains two fields; `DeleteMarker bool \`cborgen:"dm"\``
already exists (manifest.go:25) and finally gets read:

```go
Seq       uint64 `cborgen:"sq"` // the version's ordinal (== leaf/prev position)
VersionID string `cborgen:"vi"` // "null" or the ULID token; "" only in pre-versioning blocks
```

A **delete marker** is an `ObjectManifest` with `DeleteMarker: true`, a zero `Body` (no blobs, no
digests — it contributes nothing to `blob_refs`), no ETag, and `Created` = marker time. Markers
are versions like any other: they occupy `Current` or a `Prev` slot and carry a `Seq`/`VersionID`.

Run `make gen` after the type changes (the `ObjectManifest` map header count changes; `gen/main.go`
must also register `ObjectLeaf` and `VersionNode`).

### 2.4 Invariants

1. **At most one null version per key.** It is either `Current` (`Current.VersionID == "null"`,
   `NullSeq == 0`) or the single prev entry named by `NullSeq != 0`. Never both; creating a new
   null evicts the old one (§5).
2. **`Current.Seq` is the maximum seq among the key's versions.** Writes always install the newest
   version as `Current`; deleting the current version promotes the largest remaining seq (§7.2).
3. **`Prev` holds exactly the noncurrent versions**, keyed newest-first.
4. **`seq` is strictly increasing in commit order per bucket.** Allocation happens inside the
   per-bucket critical section (§5.1); in-process the `bucketop` mutex serializes it, and the
   existing `CASRoot` conflict behavior covers cross-process races exactly as it does today.
   `seq` starts at 1; 0 is reserved to mean "none" (`NullSeq`).
5. **A version's identity never changes.** `Seq`/`VersionID` are minted once at commit; promotion
   moves a `VersionNode` between `Prev` and `Current` without rewriting its manifest.

---

## 3. The `version_id` token

Numbered versions get a **26-character Crockford-base32 ULID token** (via `oklog/ulid/v2`, already
in the module graph through versitygw):

- **mint** — `timestamp` = commit wall-clock (ms, ULID's 48-bit field); `entropy` (80 bits) =
  16 zero bits ++ big-endian 64-bit `seq`. Uniqueness within a bucket is guaranteed by `seq`
  alone; the timestamp is cosmetic (AWS-shaped ids, mtime hints for operators).
- **parse** — `ulid.Parse(token)`; candidate `seq` = the low 64 bits of the entropy field. The
  candidate is only a *locator hint*: resolution (§6.1) must confirm the stored
  `VersionID == token` before treating the version as found (a foreign ULID whose low bits
  collide must not resolve).
- **classify** (every versionId-taking request):

| input | meaning |
|---|---|
| absent / `""` | current version |
| `"null"` | the null version (§6.1) |
| `ulid.Parse` succeeds | numbered version; resolve by seq + verify token |
| anything else | `400` — `s3err.GetInvalidArgumentErr(s3err.InvalidArgVersionId, input)` |

This matches the versitygw conformance grammar exactly: `"invalid_version_id"` /
`"../../secret.txt"` → `InvalidArgument`; well-formed-but-unknown `"01G65Z755AFWAKHE12NY0CQ9FH"` →
`404 NoSuchVersion`.

The direct-locator trade-off (ids leak ordering/write volume) was already accepted in
`architecture.md` §3 and carries over unchanged.

---

## 4. Bucket versioning state

### 4.1 Registry

`buckets.versioning` and `buckets.next_version_seq` already exist in the schema
(migrations/sql/00003_stores.sql:18-23) — **no new migration**. Code changes:

- `registry.State` gains `Versioning VersioningState` (a `string` type with constants
  `VersioningUnversioned` (`"unversioned"`, the default), `VersioningEnabled`, `VersioningSuspended`);
  `Postgres.Get` (postgres.go:64) selects the column; `Create` leaves the default.
- `registry.Registry` gains two methods (implemented by `*registry.Postgres` and
  `inmem.MemStore`):

```go
// SetVersioning updates the bucket's versioning state. Only "enabled" and
// "suspended" are settable (S3 has no way back to unversioned).
SetVersioning(ctx context.Context, name string, v VersioningState) error
// AllocVersionSeq atomically advances and returns the bucket's version
// ordinal (first call returns 1). Gaps from failed commits are harmless.
AllocVersionSeq(ctx context.Context, name string) (uint64, error)
```

Postgres: `UPDATE ingot.buckets SET next_version_seq = next_version_seq + 1 WHERE name = $1
RETURNING next_version_seq` (missing row → `ErrNotFound`). Versioning state is local bucket
metadata — the Hilt forwarding path (forge-mode create/delete/list) is not involved.

### 4.2 S3 API

- **`PutBucketVersioning`** (new override; interface at versitygw backend.go:42) — the controller
  pre-validates the status to `Enabled|Suspended`; the backend resolves the bucket
  (`ErrNoSuchBucket` otherwise) and calls `SetVersioning`.
- **`GetBucketVersioning`** (bucket.go:143, currently a zero-value stub) — returns `Status` nil
  when the bucket is `unversioned` (never configured → empty `<VersioningConfiguration/>`,
  matching `GetBucketVersioning_empty_response`), else the corresponding
  `types.BucketVersioningStatus`. The stale doc comment at bucket.go:138-142 ("returns
  Suspended") gets fixed.

### 4.3 Version headers by bucket state

| bucket state | PUT / CompleteMPU / Copy response id | GET/HEAD response id | DeleteObject (marker) id |
|---|---|---|---|
| unversioned | omitted | omitted | omitted (no marker; real delete) |
| enabled | ULID token | resolved version's id | marker's ULID token |
| suspended | `"null"` | resolved version's id | `"null"` |

(`Versioning_PutObject_suspended_null_versionId_obj` pins the suspended-PUT `"null"` echo.
`ListObjectVersions` always reports ids — `"null"` for null versions — regardless of state.)

---

## 5. The write rule

One commit helper implements every version-creating mutation — `PutObject`, `CopyObject`
(destination), `CompleteMultipartUpload`, and delete-marker insertion — replacing the
`Add`-then-`Update`-on-exists splice duplicated at object.go:181-183 and multipart.go:289-292.

### 5.1 Sequence allocation

Inside the `bucketop.MutateFn` (per-bucket mutex held, root snapshot taken):

```
seq := reg.AllocVersionSeq(ctx, bucket)          // ordinal, commit-ordered under the lock
vid := state == Enabled ? ulidToken(now, seq) : "null"
```

Every write allocates a seq — including unversioned/suspended null writes — so the null version
has a real position if the bucket is later Enabled (`Versioning_PutObject_null_versionId_obj`
depends on this: a pre-versioning object must list *below* later numbered versions). If the
commit fails or retries, the seq is a gap; gaps are harmless (`architecture.md` §3).

### 5.2 Displacement

With `displaced` = the existing `leaf.Current` (if the key exists) and `new` = the incoming
version:

```
discards := []                                     // versions permanently removed this commit

if key exists:
    retain := (state == Enabled) || (displaced.VersionID != "null")
    if retain:
        prev.Put(revSeqKey(displaced.Seq), displaced.Manifest)
        if displaced.VersionID == "null": leaf.NullSeq = displaced.Seq
    else:
        discards += displaced                      // replace the null in place

if new.VersionID == "null" && leaf.NullSeq != 0:   // only one null per key: evict prev null
    discards += prev entry at NullSeq              // (fetch its manifest for digests, §8)
    prev.Delete(revSeqKey(leaf.NullSeq))
    leaf.NullSeq = 0

leaf.Current = {seq, vid, manifestCID}
write leaf block; splice top MST at objectKey; return new root
```

| state | displaced | disposition |
|---|---|---|
| Enabled | numbered or null | push to prev (a re-Enabled bucket keeps its null as a noncurrent `"null"`) |
| Suspended | numbered | push to prev |
| Suspended | null | discard — replace the null in place |
| Unversioned | null | discard (today's overwrite-in-place, now via the same rule) |

`prev` is written only when history is retained — never for a purely-unversioned bucket (its
leaves stay `{Current, nil, 0}`), always for Enabled, and only across the numbered→null boundary
for Suspended.

New-key writes skip displacement entirely: `leaf = {Current: new, Prev: nil, NullSeq: 0}`.

### 5.3 Cost

An Enabled overwrite adds a prev-tree insert (O(log versions-of-key) new nodes) plus the leaf
block on top of today's splice — more catalog blocks per write, but scoped to keys actually
accumulating versions. Reads are one extra block (the leaf) over today. All new blocks flow
through the existing `OpStaging` → catalog plane; a mutation still ships only the blocks it
created. Superseded leaf/prev/MST nodes join `gc_candidates` under the existing (write-only)
policy.

One rebuild note from review, confirmed against the tree: recovery does not depend on walking
history via `forge_root_cid` alone — `logstore` journals per-op roots and every created block, so
prev-tree state is recoverable the same way the top MST is today.

---

## 6. Reads

### 6.1 Resolution

`lookupManifest` (object.go:924) becomes version-aware — `resolveVersion(ctx, state, key,
versionId)` → `(leaf, VersionNode, *ObjectManifest, error)`:

```
leaf := topMST.Get(key)                  → miss: NoSuchKey
switch classify(versionId):              // §3
case current:   node = leaf.Current
case "null":    if leaf.Current.VersionID == "null" → leaf.Current
                else if leaf.NullSeq != 0 → prev entry at NullSeq
                else → NoSuchVersion
case ULID(seq): if leaf.Current.Seq == seq → leaf.Current
                else → prev.Get(revSeqKey(seq)), miss → NoSuchVersion
case invalid:   → InvalidArgument (InvalidArgVersionId)

fetch manifest; for the ULID case verify manifest.VersionID == token, else NoSuchVersion
```

### 6.2 Behavior table (pinned by upstream conformance)

| request | outcome |
|---|---|
| GET/HEAD, no versionId, current is a marker | `404` (`NoSuchKey` / HEAD `NotFound`); `x-amz-delete-marker: true` where the controller emits it |
| GET, versionId names a marker | `405 MethodNotAllowed` |
| HEAD, versionId names a marker | `405 MethodNotAllowed` |
| GET/HEAD, versionId well-formed but unknown | `404 NoSuchVersion` (HEAD surfaces `NotFound`) |
| GET/HEAD, versionId malformed | `400 InvalidArgument` (`InvalidArgVersionId`) |
| success | body/headers of the resolved version; `VersionId` set per §4.3 |

Version-scoped reads of noncurrent versions serve bodies through the same manifest→blobs path as
current reads — noncurrent manifests still pin their digests, and `blob_refs` still counts them
(§8), so the bytes are live.

`CopyObject` source resolution goes through the same helper (the copy-source versionId comes from
the parsed `CopySource`); `CopySourceVersionId` is set on the output for versioned source buckets.
Copying *from* a marker: without a versionId the current-marker case is `404 NoSuchKey`; naming a
marker's versionId is `400 InvalidRequest`. Conditional requests (`If-Match` etc.) keep evaluating
against the resolved **current** version, including the at-commit re-check — unchanged.

`GetObjectAttributes` remains unimplemented (`ErrNotImplemented`); its versioning conformance
rows stay out of scope with it.

---

## 7. Deletes

### 7.1 `DeleteObject` without versionId

| state | behavior |
|---|---|
| unversioned | today's permanent delete (object.go:607 `deleteObjectKey`): drop the leaf, release claims. No marker, no version headers. |
| enabled | insert a **numbered delete marker** via the write rule (§5). This happens **even if the key does not exist** (S3 semantics; `Versioning_DeleteObject_non_existing_object` deletes a nonexistent key and succeeds) — the leaf is created with the marker as `Current`. Response: `DeleteMarker: true`, `VersionId: <marker token>`. |
| suspended | insert a **null delete marker** via the write rule — as a null write it replaces the existing null (current) or evicts a prev null, per §5.2; repeatable idempotently (`Versioning_DeleteObject_suspended` runs it five times). Response: `DeleteMarker: true`, `VersionId: "null"`. |

### 7.2 `DeleteObject` with versionId

Permanent removal of one specific version (any state):

```
classify (§3): malformed → InvalidArgument; resolve (§6.1): miss → NoSuchVersion

if target == leaf.Current:
    if prev empty: delete the leaf from the top MST
    else:
        head := first prev entry (newest-first walk)      // max remaining seq
        fetch head manifest → {Seq, VersionID}
        if head.Seq == leaf.NullSeq: leaf.NullSeq = 0     // the null becomes current
        prev.Delete(revSeqKey(head.Seq)); leaf.Current = head
else:
    prev.Delete(revSeqKey(target.Seq))
    if target.Seq == leaf.NullSeq: leaf.NullSeq = 0
```

After commit: release the removed version's claims (§8) — unless it was a marker (no digests).
Response: `VersionId` = the requested id; `DeleteMarker: true` iff the removed version was a
marker (`Versioning_DeleteObject_delete_a_delete_marker` asserts both fields).

### 7.3 `DeleteObjects`

Each entry dispatches to §7.1 or §7.2 by the presence of `ObjectIdentifier.VersionId` (the loop
at object.go:689-704 currently drops it). Per-entry results populate `types.DeletedObject`
(`DeleteMarker`, `DeleteMarkerVersionId`, `VersionId`); per-entry errors (e.g.
`InvalidArgVersionId`) go to the error list. Batch cap and Quiet mode unchanged; still not atomic.

---

## 8. The reference index

`blob_refs` rows finally carry real version ids — the `registry.NullVersionID` sentinel at
object.go:336/:345 is replaced by each version's `VersionID` (`"null"` remains the id of null
versions, so unversioned buckets produce today's rows unchanged). The interfaces already carry
`versionID` everywhere (`registry.BlobRefStore`, stores.go:114-120); no schema or interface
change.

Rules, preserving the commit-then-reconcile ordering (reconcile strictly after a successful
`WithTx`, object.go:196-202):

- **New version** → `AddBlobClaim` for each body digest with the new `VersionID`. Retained
  displaced versions keep their rows untouched — under Enabled, an overwrite **releases nothing**.
- **Discarded versions** (§5.2 discards, §7.2 removals, unversioned overwrite/delete) →
  `DeleteBlobClaim` per digest with the *discarded* version's id; `CountClaims == 0` →
  `RemoveBlob`, exactly as today. The evicted-prev-null case fetches the evicted manifest during
  the commit to learn its digests.
- **Same-id replacement** (null replacing null — the only case where new and discarded share a
  `(bucket, key, version_id)` row key) → the existing set-diff `reconcileClaims`
  (object.go:327) so unchanged digests are never dropped-then-re-added. This is today's
  unversioned overwrite path, unchanged.
- **Delete markers** have no digests: no claims in, none out.

Dedup interactions stay safe by construction: N versions referencing one digest are N rows; the
physical `RemoveBlob` fires only when the last row for `(space, digest)` goes.

---

## 9. Lists

### 9.1 `ListObjects` / `ListObjectsV2`

`listWalk` (object.go:849) reads each leaf's `Current`, fetches its manifest (it already fetches
manifests for ETag/size), and **skips keys whose current version is a delete marker** — they
don't count toward `MaxKeys` and don't produce entries. Everything else (prefix, delimiter,
markers, truncation) is unchanged; one head per key, no descent into `Prev`.

### 9.2 `ListObjectVersions` (new; interface at versitygw backend.go:78)

The walk composes the top-MST key walk with the per-key newest-first version sequence
(`Current`, then the `Prev` walk):

- **Entries.** Non-marker versions → `s3response.ObjectVersion{Key, VersionId, IsLatest,
  ETag, Size, LastModified, StorageClass: STANDARD}` plus checksum fields mapped from the
  manifest as GET/HEAD do. Markers → `types.DeleteMarkerEntry{Key, VersionId, IsLatest,
  LastModified}`. Both land in one interleaved logical stream (newest-first per key, keys in
  lexicographic order) and are split into the `Versions` / `DeleteMarkers` arrays for the
  response. `IsLatest` is true exactly for `Current` nodes. Rendering fetches each version's
  manifest; the review's optional "lift ETag/size/mtime into the leaf" denormalization is
  deliberately deferred to keep the base shape minimal.
- **Pagination.** `MaxKeys` (default 1000) counts versions + markers combined. On truncation:
  `NextKeyMarker`/`NextVersionIdMarker` = the last emitted entry's key/id, `IsTruncated: true`
  (pinned by `ListObjectVersions_multiple_object_versions_truncated`). Resumption: with both
  markers, seek the top MST to `KeyMarker`, seek within its version sequence strictly past
  `VersionIdMarker` (its seq gives a direct prev-tree seek), then continue; with only
  `KeyMarker`, start at the first key strictly greater.
- **Prefix / delimiter** — same grouping semantics as `listWalk` V1; all versions of keys rolled
  into a `CommonPrefix` are subsumed by it.
- **Unversioned buckets** list every key's single null version (`VersionId: "null"`,
  `IsLatest: true`).
- Negative `MaxKeys` → the controller/`utils` error path, as V1/V2 (`ListObjectVersions_negative_max_keys`).

---

## 10. Compatibility & migration

**This is a catalog format break**: top-MST values become `ObjectLeaf` CIDs where they were
`ObjectManifest` CIDs. Per the repo's dev-only data posture (CLAUDE.md: "reshape migrations in
place and reset any persistent dev DB"), there is **no migration**: existing dev buckets are
reset. Implementations must **not** attempt to type-sniff old values at read time — cbor-gen's
map decoders skip unknown fields and zero-fill missing ones, so decoding an old manifest block as
an `ObjectLeaf` *succeeds* with garbage. The format change is declared, not detected.

Also explicitly retired from `architecture.md` §3: the escape/`TERM` encoding, the
`invertedVersionId` hex token, the "raise `MaxKeyBytes` to ~1056" question (moot — top-level keys
are unchanged), and the "composite key vs per-key index" open decision (resolved: per-key index,
this design). A follow-up architecture.md edit should point §3 at this doc.

Out of scope, unchanged: object tagging, object lock / retention / legal hold, `UploadPartCopy`,
`ListParts`/`ListMultipartUploads`, `GetObjectAttributes`, MFA delete, lifecycle expiration, and
multi-instance seq arbitration beyond the existing `CASRoot` conflict surface.

---

## 11. Implementation map

| Where | Change |
|---|---|
| `bucket/leaf.go` (new) + `gen/main.go` + `make gen` | `ObjectLeaf`, `VersionNode`; register with cborgen |
| `bucket/manifest.go` | `Seq`, `VersionID` fields (§2.3) |
| `registry/registry.go`, `postgres.go`, `inmem/store.go` | `State.Versioning`, `VersioningState`, `SetVersioning`, `AllocVersionSeq` (§4.1) |
| `s3frontend/version.go` (new) | token mint/parse/classify (§3), `revSeqKey`, `resolveVersion` (§6.1), the write rule (§5.2), prev-tree helpers |
| `s3frontend/object.go` | `PutObject` splice → write rule; `lookupManifest` → `resolveVersion`; `GetObject`/`HeadObject` versionId + marker semantics + output ids; `DeleteObject`/`deleteObjectKey`/`DeleteObjects` (§7); `reconcileClaims` call sites carry real version ids (§8); `listWalk` marker skip (§9.1) |
| `s3frontend/copy.go`, `multipart.go` | `commitManifest` → write rule; source-version resolution; `CopySourceVersionId`; Complete's `versionid` return |
| `s3frontend/bucket.go` | `PutBucketVersioning` (new), `GetBucketVersioning` (real states), `ListObjectVersions` (§9.2) |
| `s3frontend/conditions.go` | `currentObjectETag` via the leaf's current |
| unit tests | token codec + classification; write-rule table tests (all four displacement rows + null eviction + non-existent-key marker); promotion; per-version claim add/release on the `refindex_test.go` harness; list pagination |
| `itest/versity_versioning_test.go` (new) + `versity_test.go` categories | curate upstream `TestVersioning` / `ListObjectVersions_*` / `GetBucketVersioning_*` / `PutBucketVersioning_*` rows into pass/xfail tables (tagging, object-lock, `GetObjectAttributes`, `UploadPartCopy` rows → xfail/omitted; note the teardown-blocked caveat, itest/README.md:40-47) |
| `docs/architecture.md` | §3 versioned-key-encoding block → superseded pointer to this doc |
