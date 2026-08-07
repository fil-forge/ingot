# S3 versioning in Ingot

Implementation spec for S3 bucket versioning: version identity, the storage layout, writes,
reads, deletes, lists, and reference counting. It builds on
[`architecture.md`](./architecture.md), which specifies the write path, the reference index,
and the catalog plane this doc uses.

---

## 1. How versioning works

Each bucket is one MST mapping **plain S3 object keys** to values, and the value under a key
takes one of two forms (§2.1). A key holding a single version stores its **`ObjectManifest`**
CID directly — one block, one fetch, the layout an unversioned store would use. A key gains an
**`ObjectLeaf`** with its second version (§5.2) and keeps it from then on: a small block that
carries the key's current version inline plus the root of a second per-key MST (the **prev
tree**, §2.2) holding the noncurrent ones. Every value block is a keyed union naming its own
format (§2.1), so the forms are told apart exactly, never by guessing at field shapes. Both
trees use the same forked `mst` package, the same space, and the same blockstore; every node is
an ordinary catalog-plane block, and nesting stops at two levels. A bucket that never versions
therefore stores one manifest per key and nothing more.

Every version carries two identifiers with separate jobs:

- **`seq`** — a per-bucket ordinal, allocated once per write inside the bucket's commit
  critical section (§5.1). It orders a key's versions by recency and never leaves the server.
- **`version_id`** — the client handle (`x-amz-version-id`). On an Enabled bucket it is a ULID
  token whose low 64 entropy bits hold the seq (§3), so the id itself says where the version
  sits. On a Suspended or unversioned bucket, S3 fixes the id to the literal string `"null"`.

A key holds at most one **null version**; later null writes replace it in place. It can sit
anywhere in the version stack: a bucket written unversioned and then Enabled keeps its null
version where it is while numbered versions stack above it. Because the id `"null"` carries no
seq, the leaf records a noncurrent null version's position in its `NullSeq` field.

**Writes** (§5). Every version-creating request (PUT, CopyObject, CompleteMultipartUpload,
delete-marker insertion) allocates the next seq and installs the new version as the key's
current. On an Enabled bucket the superseded current version moves into the prev tree under its
seq — the key's first supersession creates the leaf and the prev tree (§5.2); the move
transfers the manifest's CID, and the manifest itself is never rewritten. On a Suspended or
unversioned bucket the write is a null write and replaces the existing null
version. A DELETE without a version id writes a **delete marker**: a version with no body that
makes the key read as absent. A DELETE naming a version id permanently removes that one
version, wherever it sits (§7).

**Reads** (§6). A GET or HEAD without a version id reads the key's value block — the manifest
itself for a single-version key, else the leaf's inline current version; the prev tree is never
touched. A version-scoped read against a single-version key checks the token against the one
manifest's stored `VersionID` (§6.1). Against a versioned key it composes the leaf's pieces.
Take `GET photos/cat.jpg?versionId=<token>` against a key holding versions with seqs 9
(current), 7, 4, and 2, where the token names seq 7:

1. Read the key's leaf from the top MST.
2. Parse the token; its low 64 bits give 7.
3. `Current.Seq` is 9, so seek the prev tree directly: `prev.Get(revSeqKey(7))`. The seek is
   O(log n); nothing is scanned.
4. Fetch the manifest that entry points at and confirm its stored `VersionID` equals the token
   (§6.1). This guard exists because step 2 trusts nothing: any well-formed ULID parses, so a
   client can send a token this bucket never minted (an id copied from another bucket, or
   fabricated) whose low 64 bits nonetheless equal the seq of a real version of this key. Step
   3 would then land on that version even though the token does not name it. Comparing the full
   26-character token against the manifest's stored `VersionID` catches the mismatch, and the
   read returns `NoSuchVersion` instead of serving a version the caller never asked for.
5. Serve the body through the same manifest-to-blobs path a current read uses.

On a leaf key, `versionId=null` resolves from the leaf alone: the current version when its id
is `"null"`, else the prev entry named by `NullSeq` (§6.1).

**Lists** (§9). `ListObjects`/`ListObjectsV2` read each key's current version off its value
block and skip keys whose current version is a delete marker. `ListObjectVersions` walks keys in
lexicographic order and, within a key, emits the current version and then the prev tree, which
iterates newest-first because its keys are inverted seqs.

**Space** (§8). Each version claims its body digests in `blob_refs`. Removing a version
releases only that version's claims, and the physical bytes go only when the last claim goes,
so deduplicated data survives partial deletes by construction.

---

## 2. Data model

### 2.1 The value union

The top-level (per-bucket) MST maps **plain object keys** to CIDs. S3 caps object-key names at
1024 bytes, which is exactly `mst.MaxKeyBytes`, so keys are stored as-is. Every block a key's
value CID points at is a **keyed union**: a single-entry CBOR map whose key names the payload's
format, with one arm per value kind:

```ipldsch
type ObjectValue union {
  | ObjectManifest "/objectmanifest/0"   # the key's single version (§2.3)
  | ObjectLeaf     "/objectleaf/0"       # the per-key version group, once superseded (§5.2)
} representation keyed
```

A key holding exactly one version stores its manifest arm — one block serves the whole read
(the manifest carries `Seq`/`VersionID`, §2.3, so even version-scoped requests resolve against
it directly), and most keys never leave this form. The key gains the leaf arm at its first
retained supersession (§5.2).

Decoding dispatches on the union key, so telling the forms apart is exact, not duck typing —
and a key the reader does not know is an **error** ("written by a newer format"), never a
zero-filled garbage decode. Either arm's format can be revised: a new leaf takes
`"/objectleaf/1"`, a new manifest takes `"/objectmanifest/1"`, a new value kind takes a new
name, and old and new blocks coexist under one reader with no rewrite pass. The union costs
each block its key string (~19 bytes) and buys the self-description.

cbor-gen generates the union codec directly — a two-arm struct of `omitempty` pointers whose
map keys are the union keys — but its generated decoder *skips* unknown map keys silently, so
`bucket/leaf.go` wraps it in strict types that require exactly one arm: `ObjectValue` (either
arm; the read-dispatch sites), and `EnvelopedManifest` / `EnvelopedLeaf` (one specific arm;
every site that knows which block it is reading or writing).

```go
// bucket/leaf.go (cborgen via gen/main.go; ValueUnion is generated, the
// strict wrappers are hand-written)

// VersionNode identifies one object version: its ordinal, its client id, and
// its manifest.
type VersionNode struct {
    Seq       uint64  `cborgen:"s"` // per-bucket ordinal; ordering only, never exposed
    VersionID string  `cborgen:"v"` // client handle: "null" or a ULID token (§3)
    Manifest  cid.Cid `cborgen:"m"` // the manifest block's CID
}

// ObjectLeaf is the per-key version group — the "/objectleaf/0" arm.
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

// ValueUnion is the union as cbor-gen encodes it; the strict wrappers above
// it are what call sites use.
type ValueUnion struct {
    Manifest *ObjectManifest `cborgen:"/objectmanifest/0,omitempty"`
    Leaf     *ObjectLeaf     `cborgen:"/objectleaf/0,omitempty"`
}
```

**Every manifest block is stored under its union key** — as a value block and as a prev-tree
entry (§2.2) alike — so a version's identity CID (invariant 5, §2.4) names one encoding
everywhere it appears.

**Why a union with a one-version fast path:** most keys are written once and never versioned;
the manifest arm keeps their reads at one block and their storage at one manifest — the cost of
versioning lands only on keys that use it. A versioned read needs the extra block anyway: the
leaf routes the seq lookup (§6.1), then the manifest serves the body.

**Why `VersionNode` repeats `Seq` and `VersionID` when the manifest also stores them (§2.3):**
the copies let the leaf answer questions without fetching a manifest. The write rule decides
whether a superseded current version is retained or discarded by reading `superseded.VersionID`
(§5.2), and resolution short-circuits on `Current.Seq` / `Current.VersionID` (§6.1); both work
from the leaf alone. Manifests are immutable, so the copies cannot drift: the manifest stays
the authoritative record, and the `VersionNode` fields are a two-field cache of it.

**Why the leaf points at the manifest rather than inlining it:** every version's manifest is
written as a standalone block at version creation — for a single-version key it *is* the
value — and its CID is the version's stable identity (invariant 5, §2.4): prev entries store
it, supersession pushes it down and promotion (§7.2) pulls it back, and the reference index
(§8) counts claims on the strength of it. Inlining a copy of the current manifest into the leaf would save the
second fetch on leaf-key reads, but it would put each object's blob list — which scales with
object size — on the block every list walk reads, and leaf keys are exactly the keys whose
lists carry many versions. The leaf stays a small pointer block; the extra fetch is a hot
catalog block the read cache absorbs.

### 2.2 The prev tree

Noncurrent versions live in a **per-key sub-MST** (the same forked `mst` package, same space, same
blockstore — its nodes are ordinary catalog-plane blocks):

- **key** — `revSeqKey(seq)`: fixed-width 16-char lowercase hex of `math.MaxUint64 − seq`.
  Because the MST iterates forward-only (`mst.go:835 WalkLeavesFrom`), the inversion makes
  iteration **newest-first**. Scoped to one object key, the key is *just an integer rendered
  order-preservingly*, valid under `mst.IsValidKey` (hex is UTF-8, NUL-free, short).
- **value** — the version's manifest CID (its `"/objectmanifest/0"` block, §2.1) and nothing
  else: a prev entry stores no
  `VersionNode`. For these entries the seq is recoverable from the MST key, and the version id
  lives only in the manifest (§2.3). That costs nothing in practice, because every consumer of
  a prev entry fetches the manifest anyway (list rendering needs ETag/size/mtime;
  version-scoped reads need the body).

A direct version-scoped seek is `prev.Get(revSeqKey(seq))` — O(log n), no scan.

### 2.3 Manifest additions

`ObjectManifest` (bucket/manifest.go) gains two fields; `DeleteMarker bool \`cborgen:"dm"\``
already exists (manifest.go:25) and finally gets read:

```go
Seq       uint64 `cborgen:"sq"` // the version's ordinal (== leaf/prev position)
VersionID string `cborgen:"vi"` // "null" or the ULID token; "" only in pre-versioning blocks
```

Both fields also exist on the leaf's `VersionNode` (§2.1 explains the split). The manifest is
the self-contained record: a prev entry stores only the manifest CID (§2.2), so for noncurrent
versions — and for a manifest-valued key's single version (§2.1) — the manifest is the only place the id
is stored. List rendering reads it there, the token check in §6.1 verifies against it,
supersession of a manifest-valued key reads the old version's position from it (§5.2), and promotion
(§7.2) rebuilds the leaf's `Current` node from these two fields.

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
   `CASRoot` conflict behavior covers cross-process races.
   `seq` starts at 1; 0 is reserved to mean "none" (`NullSeq`).
5. **A version's identity never changes.** `Seq`/`VersionID` are minted once at commit; promotion
   moves a `VersionNode` between `Prev` and `Current` without rewriting its manifest.
6. **A manifest-arm value holds exactly one version.** The leaf arm appears with the key's
   second version (§5.2) and persists until the key itself is deleted: version-scoped deletes
   that shrink a key back to one version keep its leaf (§7.2), so a leaf is never downgraded.

---

## 3. The `version_id` token

Numbered versions get a **26-character Crockford-base32 ULID token** (via `oklog/ulid/v2`, already
in the module graph through versitygw):

- **mint** — `timestamp` = commit wall-clock (ms, ULID's 48-bit field); `entropy` (80 bits) =
  16 zero bits ++ big-endian 64-bit `seq`. Uniqueness within a bucket is guaranteed by `seq`
  alone; the timestamp is cosmetic (AWS-shaped ids, mtime hints for operators).
- **parse** — `ulid.ParseStrict(token)` (strict, so near-ULID garbage outside the Crockford
  character set is malformed, never a lookup); candidate `seq` = the low 64 bits of the entropy field. The
  candidate is only a *locator hint*: resolution (§6.1) must confirm the stored
  `VersionID == token` before treating the version as found (a foreign ULID whose low bits
  collide must not resolve).
- **classify** (every versionId-taking request):

| input | meaning |
|---|---|
| absent / `""` | current version |
| `"null"` | the null version (§6.1) |
| `ulid.ParseStrict` succeeds | numbered version; resolve by seq + verify token |
| anything else | `400` — `s3err.GetInvalidArgumentErr(s3err.InvalidArgVersionId, input)` |

This matches the versitygw conformance grammar exactly: `"invalid_version_id"` /
`"../../secret.txt"` → `InvalidArgument`; well-formed-but-unknown `"01G65Z755AFWAKHE12NY0CQ9FH"` →
`404 NoSuchVersion`.

Embedding the seq makes tokens readable: anyone holding a bucket's version ids can recover
write order and estimate write volume. S3 defines version ids as opaque, so no client contract
depends on hiding this; the exposure is operational and accepted.

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

### 5.2 Supersession

With `superseded` = the key's existing current version (if the key exists) and `new` = the
incoming version. On a leaf key `superseded` is `leaf.Current`; on a manifest-valued key (§2.1) it is the
manifest itself, whose `Seq`/`VersionID` fields (§2.3) supply the same information — the write
path fetches the old manifest in either case that consumes it (retention needs its position,
discard needs its digests, §8).

```
discards := []                                     // versions permanently removed this commit

if key exists:
    retain := (state == Enabled) || (superseded.VersionID != "null")
    if retain:
        if value is a manifest: leaf = {Prev: empty}  // first supersession: create the leaf
        prev.Put(revSeqKey(superseded.Seq), superseded.Manifest)
        if superseded.VersionID == "null": leaf.NullSeq = superseded.Seq
    else:
        discards += superseded                     // replace the null in place; the value stays a manifest

if leaf exists:
    if new.VersionID == "null" && leaf.NullSeq != 0:  // only one null per key: evict prev null
        discards += prev entry at NullSeq             // (fetch its manifest for digests, §8)
        prev.Delete(revSeqKey(leaf.NullSeq))
        leaf.NullSeq = 0
    leaf.Current = {seq, vid, manifestCID}
    write leaf block; value = CID(leaf arm)
else:
    value = manifestCID                            // new key, or null over null: the manifest arm

splice top MST at objectKey; return new root
```

| state | superseded | disposition |
|---|---|---|
| Enabled | numbered or null | push to prev (a re-Enabled bucket keeps its null as a noncurrent `"null"`) |
| Suspended | numbered | push to prev |
| Suspended | null | discard — replace the null in place |
| Unversioned | null | discard — replace the null in place |

`prev` is written only when history is retained — never for a purely-unversioned bucket (its
keys stay manifest-valued), always for Enabled, and only across the numbered→null boundary for
Suspended. The leaf appears exactly at a key's first retained supersession and persists from
then on (invariant 6).

New-key writes skip supersession entirely: the value is the new manifest CID.

### 5.3 Cost

A never-superseded key costs what an unversioned store would charge: one manifest block behind
the top-MST splice every write performs, one block per read. An Enabled overwrite adds the leaf
block plus a prev-tree insert (O(log versions-of-key) new nodes) on top of that splice: more
catalog blocks per write, scoped to keys actually accumulating versions. A read of a leaf key
costs one extra block (the leaf) between the top MST and the manifest. All new blocks flow
through the existing `OpStaging` → catalog plane; a mutation still ships only the blocks it
created. Discarded and evicted manifests and superseded leaf blocks join `gc_candidates` under
the existing (write-only) policy; superseded MST interior nodes (top tree and prev tree alike)
are not tracked, matching the top MST's practice.

Recovery does not depend on walking history via `forge_root_cid` alone: `logstore` journals
per-op roots and every created block, so prev-tree state is recoverable the same way the top
MST is.

---

## 6. Reads

### 6.1 Resolution

Every versionId-taking read resolves through one helper, `resolveVersion(ctx, state, key,
versionId)` → `(leaf, VersionNode, *ObjectManifest, error)` (`leaf` is nil for a manifest-valued key):

```
val := topMST.Get(key)                   → miss: NoSuchKey
classify(versionId)                      // §3; invalid → InvalidArgument (InvalidArgVersionId)

value is a manifest (§2.1):              // single-version key: the manifest answers everything
    case current:   → it
    case "null":    manifest.VersionID == "null" → it, else NoSuchVersion
    case ULID(_):   manifest.VersionID == token  → it, else NoSuchVersion

value is a leaf:
    case current:   node = leaf.Current
    case "null":    if leaf.Current.VersionID == "null" → leaf.Current
                    else if leaf.NullSeq != 0 → prev entry at NullSeq
                    else → NoSuchVersion
    case ULID(seq): if leaf.Current.Seq == seq → leaf.Current
                    else → prev.Get(revSeqKey(seq)), miss → NoSuchVersion
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
marker's versionId is `400 InvalidRequest`. Conditional requests (`If-Match` etc.) evaluate
against the resolved **current** version, including the at-commit re-check.

`GetObjectAttributes` remains unimplemented (`ErrNotImplemented`); its versioning conformance
rows stay out of scope with it.

---

## 7. Deletes

### 7.1 `DeleteObject` without versionId

| state | behavior |
|---|---|
| unversioned | permanent delete (object.go:607 `deleteObjectKey`): drop the key and its manifest, release claims. No marker, no version headers. |
| enabled | insert a **numbered delete marker** via the write rule (§5). This happens **even if the key does not exist** (S3 semantics; `Versioning_DeleteObject_non_existing_object` deletes a nonexistent key and succeeds) — the key is created with the marker's manifest as its value. Response: `DeleteMarker: true`, `VersionId: <marker token>`. |
| suspended | insert a **null delete marker** via the write rule — as a null write it replaces the existing null (current) or evicts a prev null, per §5.2; repeatable idempotently (`Versioning_DeleteObject_suspended` runs it five times). Response: `DeleteMarker: true`, `VersionId: "null"`. |

### 7.2 `DeleteObject` with versionId

Permanent removal of one specific version (any state):

```
classify (§3): malformed → InvalidArgument
resolve (§6.1): miss (unknown key, or no such version) → success no-op — S3 deletes are
                idempotent (Versioning_DeleteObject_non_existing_objects); the response
                echoes the requested id. (GET/HEAD keep returning NoSuchVersion, §6.2.)

if value is a manifest (§2.1):                       // the match was the key's only version
    delete the key from the top MST
else if target == leaf.Current:
    if prev empty: delete the key (leaf and all) from the top MST
    else:                                            // promotion: the newest survivor becomes current
        head := first prev entry (newest-first walk)      // max remaining seq
        fetch head manifest → {Seq, VersionID}
        if head.Seq == leaf.NullSeq: leaf.NullSeq = 0     // the null becomes current
        prev.Delete(revSeqKey(head.Seq)); leaf.Current = head
else:
    prev.Delete(revSeqKey(target.Seq))
    if target.Seq == leaf.NullSeq: leaf.NullSeq = 0
```

A delete never converts a leaf back to the manifest arm: a key left holding one version keeps its
leaf (invariant 6), so the upgrade happens once per key and the write path never re-derives
form from count.

After commit: release the removed version's claims (§8) — unless it was a marker (no digests).
Response: `VersionId` = the requested id; `DeleteMarker: true` iff the removed version was a
marker (`Versioning_DeleteObject_delete_a_delete_marker` asserts both fields); a no-op miss
echoes the id with no marker flag.

### 7.3 `DeleteObjects`

Each entry dispatches to §7.1 or §7.2 by the presence of `ObjectIdentifier.VersionId` (the loop
at object.go:689-704 currently drops it). Per-entry results populate `types.DeletedObject`
(`DeleteMarker`, `DeleteMarkerVersionId`, `VersionId`); per-entry errors (e.g.
`InvalidArgVersionId`) go to the error list. Batch cap and Quiet mode follow the plain-delete
semantics; the batch is not atomic.

`DeleteBucket` on a versioned bucket that still holds versions or markers fails with
`VersionedBucketNotEmpty` (`Versioning_DeleteBucket_not_empty`). Delete preconditions (the
`If-Match` family) apply to marker insertion, evaluated against the superseded current version,
and to version-scoped deletes, evaluated against the target version.

---

## 8. The reference index

`blob_refs` rows carry each version's id: the `registry.NullVersionID` sentinel at
object.go:336/:345 is replaced by each version's `VersionID` (null versions carry the id
`"null"`, so rows for unversioned buckets keep that id). The interfaces already carry
`versionID` everywhere (`registry.BlobRefStore`, stores.go:114-120); no schema or interface
change.

Rules, preserving the commit-then-reconcile ordering (reconcile strictly after a successful
`WithTx`, object.go:196-202):

- **New version** → `AddBlobClaim` for each body digest with the new `VersionID`. Retained
  superseded versions keep their rows untouched — under Enabled, an overwrite **releases nothing**.
- **Discarded versions** (§5.2 discards, §7.2 removals, unversioned overwrite/delete) →
  `DeleteBlobClaim` per digest with the *discarded* version's id; `CountClaims == 0` →
  `RemoveBlob`. The evicted-prev-null case fetches the evicted manifest during
  the commit to learn its digests.
- **Same-id replacement** (null replacing null — the only case where new and discarded share a
  `(bucket, key, version_id)` row key) → the existing set-diff `reconcileClaims`
  (object.go:327) so unchanged digests are never dropped-then-re-added.
- **Delete markers** have no digests: no claims in, none out.

Dedup interactions stay safe by construction: N versions referencing one digest are N rows;
`RemoveBlob` fires only when the last row for `(space, digest)` goes. `RemoveBlob` itself is
the *ingot-side* release: it invokes `/blob/remove` (uploader/blob.go:248), which drops this
space's claim on the network. Whether the bytes are physically deleted is then Piri's decision,
made on its own accounting across every space that claims the blob (`architecture.md` §9) —
ingot's index answers only "does any version in this bucket still need the digest".

---

## 9. Lists

### 9.1 `ListObjects` / `ListObjectsV2`

`listWalk` (object.go:849) reads each key's current version — the manifest arm itself, or the
leaf's `Current` manifest (fetched anyway for ETag/size) — and **skips keys whose current
version is a delete marker**: they don't count toward `MaxKeys`, don't produce entries, and
don't seed a `CommonPrefix`. Prefix, delimiter, markers, and truncation keep their usual
semantics; the walk costs one manifest per manifest-valued key, or a leaf and a manifest per superseded
key, with no descent into `Prev`. The manifest fetch applies even to keys a delimiter rolls
into a `CommonPrefix`: a prefix whose keys all sit under delete markers must not appear.

### 9.2 `ListObjectVersions` (new; interface at versitygw backend.go:78)

The walk composes the top-MST key walk with the per-key newest-first version sequence — a
manifest-valued key's single version, or `Current` then the `Prev` walk:

- **Entries.** Non-marker versions → `s3response.ObjectVersion{Key, VersionId, IsLatest,
  ETag, Size, LastModified, StorageClass: STANDARD}` plus checksum fields mapped from the
  manifest as GET/HEAD do. Markers → `types.DeleteMarkerEntry{Key, VersionId, IsLatest,
  LastModified}`. Both land in one interleaved logical stream (newest-first per key, keys in
  lexicographic order) and are split into the `Versions` / `DeleteMarkers` arrays for the
  response. `IsLatest` is true exactly for `Current` nodes. Rendering fetches each version's
  manifest. Copying ETag/size/mtime into the leaf would save those fetches; the leaf stays
  minimal until list volume justifies the extra bytes.
- **Pagination.** `MaxKeys` (default 1000) counts versions + markers combined. On truncation:
  `NextKeyMarker`/`NextVersionIdMarker` = the last emitted entry's key/id, `IsTruncated: true`
  (pinned by `ListObjectVersions_multiple_object_versions_truncated`). Resumption: with both
  markers, seek the top MST to `KeyMarker`, seek within its version sequence strictly past
  `VersionIdMarker` (its seq gives a direct prev-tree seek), then continue; with only
  `KeyMarker`, start at the first key strictly greater. A `version-id-marker` needs a
  `key-marker` and must classify (§3); violations → `InvalidArgument`.
- **Prefix / delimiter** — same grouping semantics as `listWalk` V1; all versions of keys rolled
  into a `CommonPrefix` are subsumed by it.
- **Unversioned buckets** list every key's single null version (`VersionId: "null"`,
  `IsLatest: true`).
- Negative `MaxKeys` → the controller/`utils` error path, as V1/V2 (`ListObjectVersions_negative_max_keys`).

---

## 10. Compatibility & migration

The value union (§2.1) makes the format self-describing. Every value block names its own
format in its union key, and a reader that meets a key it does not know fails loudly instead of
decoding garbage (cbor-gen's map decoders skip unknown fields and zero-fill missing ones, so
*unguarded* cross-type decodes would succeed silently — the strict union wrappers are what rule
them out). Format revisions from here — a changed manifest, a changed leaf, a new value kind —
take a new union key and coexist with old blocks under one reader, with no rewrite pass.

Buckets written before versioning store manifests as bare blocks with no union key (and no
`Seq`/`VersionID`, §2.3); the union readers reject them. Per the repo's dev-only data posture
(CLAUDE.md: "reshape migrations in place and reset any persistent dev DB"), there is **no
migration**: existing dev buckets are reset rather than taught to read the pre-union form.

Out of scope: object tagging, object lock / retention / legal hold, `UploadPartCopy`,
`ListParts`/`ListMultipartUploads`, `GetObjectAttributes`, MFA delete, lifecycle expiration, and
multi-instance seq arbitration beyond the existing `CASRoot` conflict surface.

---

## 11. Implementation map

| Where | Change |
|---|---|
| `bucket/leaf.go` (new) + `gen/main.go` + `make gen` | `ObjectLeaf`, `VersionNode`, `ValueUnion` (cborgen-registered); the strict `ObjectValue` / `EnvelopedManifest` / `EnvelopedLeaf` wrappers (§2.1) |
| `bucket/manifest.go` | `Seq`, `VersionID` fields (§2.3) |
| `registry/registry.go`, `postgres.go`, `inmem/store.go` | `State.Versioning`, `VersioningState`, `SetVersioning`, `AllocVersionSeq` (§4.1) |
| `s3frontend/version.go` (new) | token mint/parse/classify (§3), `revSeqKey`, the value-union dispatch (§2.1), `resolveVersion` (§6.1), the write rule (§5.2), prev-tree helpers |
| `s3frontend/object.go` | `PutObject` splice → write rule; reads resolve via `resolveVersion`; `GetObject`/`HeadObject` versionId + marker semantics + output ids; `DeleteObject`/`deleteObjectKey`/`DeleteObjects` (§7); `reconcileClaims` call sites carry real version ids (§8); `listWalk` marker skip (§9.1) |
| `s3frontend/copy.go`, `multipart.go` | `commitManifest` → write rule; source-version resolution; `CopySourceVersionId`; Complete's `versionid` return |
| `s3frontend/bucket.go` | `PutBucketVersioning` (new), `GetBucketVersioning` (real states), versioned `DeleteBucket` guard (§7) |
| `s3frontend/listversions.go` (new) | `ListObjectVersions` (§9.2) |
| `s3frontend/conditions.go` | `currentObjectETag` resolves the current version via `resolveVersion` |
| unit tests | token codec + classification; union codec round-trip + unknown-key and pre-union-block rejection; write-rule table tests (all four supersession rows + first-supersession leaf creation + null eviction + non-existent-key marker); promotion; per-version claim add/release on the `refindex_test.go` harness; list pagination |
| `itest/versity_versioning_test.go` (new) + `versity_test.go` categories | curate upstream `TestVersioning` / `ListObjectVersions_*` / `GetBucketVersioning_*` / `PutBucketVersioning_*` rows into pass/xfail tables (tagging, object-lock, `GetObjectAttributes`, `UploadPartCopy` rows → xfail/omitted; note the teardown-blocked caveat, itest/README.md:40-47) |
| `docs/architecture.md` | point §3 at this doc for versioning |
