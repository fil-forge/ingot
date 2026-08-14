# S3 Object Lock in Ingot

Implementation spec for S3 Object Lock: the bucket lock configuration, per-version retention
and legal holds, the enforcement contract with versitygw, where lock state is stored, and the
conformance surface. It builds on [`s3-versioning.md`](./s3-versioning.md) (version identity,
resolution, the write rule) and [`architecture.md`](./architecture.md) (the registry and the
catalog plane).

---

## 1. How object lock works

Object Lock is WORM protection for object versions. A bucket carries a lock configuration:
lock enabled, plus an optional default retention rule. A version carries two independent
protections: a **retention** (a mode, `GOVERNANCE` or `COMPLIANCE`, with a retain-until date)
and a **legal hold** (a boolean with no expiry). A protected version cannot be removed by a
version-scoped delete until its retention expires and its hold is off. Writes above it are
unaffected: on a versioned bucket a PUT over a locked key stacks a new version and an unscoped
DELETE inserts a marker, while the locked version underneath stays readable and undeletable.
Object lock requires versioning. A bucket with lock enabled has versioning Enabled, and its
versioning can never be suspended afterward (§5).

versitygw's controller layer owns every WORM decision. `auth.CheckObjectAccess`
(versitygw `auth/object_lock.go:224`) runs before `DeleteObject`, `DeleteObjects`, `PutObject`,
`CopyObject`, and `CompleteMultipartUpload` reach the backend; it reads the bucket lock
configuration, the target version's retention, and its legal hold through `backend.Backend`
getters and answers `ErrObjectLocked` when a protected version would be mutated. The controller
also parses and validates every lock XML document, resolves the `x-amz-bypass-governance-retention`
header (§10), and evaluates the bucket default retention rule dynamically at check time. The
backend's whole job is storage and lookup: six methods that store and return opaque payloads,
plus exact sentinel errors for every absent case (§2). Ingot adds no enforcement logic to its
own write or delete paths. The versioning write rule gains lock stamping and cleanup inside
its existing commits (§7, §9); version identity (invariant 5 of the versioning design) and the
reference index are untouched.

---

## 2. The backend contract

The six methods (versitygw `backend/backend.go:94`):

```go
PutObjectLockConfiguration(ctx, bucket string, config []byte) error
GetObjectLockConfiguration(ctx, bucket string) ([]byte, error)
PutObjectRetention(ctx, bucket, object, versionId string, retention []byte) error
GetObjectRetention(ctx, bucket, object, versionId string) ([]byte, error)
PutObjectLegalHold(ctx, bucket, object, versionId string, status bool) error
GetObjectLegalHold(ctx, bucket, object, versionId string) (*bool, error)
```

Every payload crossing the boundary is JSON the controller produced, stored and returned
verbatim (the posix backend's model, which this design copies):

- **bucket config** = `json.Marshal(auth.BucketLockConfig{Enabled, DefaultRetention, CreatedAt})`.
  The controller validates the XML (mode, days/years bounds, `Enabled` status) before the
  backend sees it, and stamps `CreatedAt` when a default retention is present.
- **retention** = the JSON of `s3response.PutObjectRetentionInput{Mode, RetainUntilDate}` (a
  `PutObjectRetention` body) or of `types.ObjectLockRetention{Mode, RetainUntilDate}` (a
  creation-time header, §7). Both shapes unmarshal into `types.ObjectLockRetention`, which is
  what every reader uses.
- **legal hold** is a bare bool; `GetObjectLegalHold` returns nil when the version has never
  had a hold set, so the stored state is tri-valued (§4.1).

The sentinel errors are load-bearing: `CheckObjectAccess` tolerates exactly these and treats
any other error as a request failure, so returning the wrong one breaks every write to the
bucket.

| condition | error |
|---|---|
| bucket has no lock configuration | `ErrObjectLockConfigurationNotFound` (404) |
| version exists, no retention / never-set hold | `ErrNoSuchObjectLockConfiguration` (400) |
| target version is a delete marker | `ErrMethodNotAllowed` (405) |
| key or version absent | `ErrNoSuchKey` / `GetNoSuchVersionErr` |
| malformed versionId | `InvalidArgument` (`InvalidArgVersionId`) |
| lock op against a bucket without lock enabled | the four per-version methods answer `ErrMissingObjectLockConfiguration` (400); the creation-time header paths (PutObject, CopyObject, CreateMultipartUpload) answer the `...NoSpaces` variant — the wire messages differ by one space and the conformance suite distinguishes them |
| `PutObjectLockConfiguration` on a non-Enabled-versioning bucket | `ErrObjectLockConfigurationNotAllowed` (409) |
| `PutBucketVersioning(Suspended)` on a lock bucket | `ErrSuspendedVersioningNotAllowed` (400) |

`CheckObjectAccess` short-circuits before any per-version lookup in three cases, which is why
the backend never needs to reason about them: an overwrite on a versioning-Enabled bucket
(the write creates a new version), an unscoped delete on a versioning-Enabled bucket (the
delete creates a marker), and an expired retention date (the version is writable again).
Delete markers are skipped through the `ErrMethodNotAllowed` sentinel.

---

## 3. Where lock state lives

Per-version lock state is small (a retention document and a hold flag), mutable
(`PutObjectRetention` replaces the document, `PutObjectLegalHold` toggles the flag), and read
on every enforcement check. Manifests cannot host it: a version's manifest CID is its
identity (invariant 5 of the versioning design: prev-tree entries store it, promotion moves
it, GC tracks it), and lock mutations must not rewrite it. The versioning design already
separates its blocks into two classes, and that separation is what makes a catalog home
possible. Manifests are **identity-carrying** and immutable. The leaf and the tree nodes are
**positional**: the leaf is rewritten on every supersession and promotion, carries no
identity, and its superseded copies go to `gc_candidates`. Lock state belongs to the
positional class, keyed by `Seq`, the per-version ordinal that never changes and is never
reused.

**This design stores lock state in a per-key version-state tree inside the catalog** (§4.1):
a home for mutable per-version *service state* generally, with lock as its first tenant and
object tagging (planned follow-up) as its second. The registry alternative is specified
alongside because the choice trades implementation surface against what the catalog alone can
reconstruct, and that trade deserves review before it hardens. Appendix A holds the rejected
shapes.

**The version-state tree (this design).** Each key's leaf gains a second sub-tree beside
`Prev`: a per-key MST mapping `revSeqKey(seq)` to a small per-version state block. The
strongest case is recovery and portability paired with intact invariants: every version's
protections ride the catalog plane, ship to Forge, and are reconstructable without the
registry, while state mutations rewrite only positional blocks, so invariant 5 holds
verbatim. Stamping and cleanup are atomic with the commits that create and remove versions
(§7, §9): there is no window where a version exists without the lock state its request
carried. Enforcement reads add nothing for keys without state (the leaf says so directly),
and one tree seek plus one block for locked keys, absorbed by the read cache. The costs: a
new union arm and a leaf field (a `make gen` schema change), a third per-key tree class in
the write paths, and more implementation surface than rows in a table.

**Registry rows (the alternative).** One `object_locks` row per version with explicit state,
keyed `(bucket, object_key, version_id)`, holding the retention document and a tri-valued
hold column. The strongest case is economy and operability: no cborgen or MST changes, a
store interface with two implementations, and SQL that can answer operational questions
("every version under legal hold in this bucket") that tree walks answer only by full
traversal. The costs: `GetObjectRetention` must resolve the version anyway for the §2
sentinels and then adds a row fetch on top; creation-time stamping and delete cleanup are
post-commit writes, each with a crash window in which the catalog and the lock table
disagree; and the catalog alone cannot answer "is this version protected," so replicating or
recovering a bucket from Forge means carrying registry rows beside catalog blocks.

Under either option, bucket-level lock configuration stays in the registry beside
`versioning` and `next_version_seq` (§4.2). That row is the whole catalog-only replication
residue under the version-state tree; under registry rows the residue additionally spans
every locked version.

---

## 4. Data model

### 4.1 The version-state tree

Two schema additions in `bucket/` (cborgen-registered in `gen/main.go`; run `make gen`):

```go
// VersionState is one version's mutable service state, stored as its own
// catalog block under the "/versionstate/0" union key (decoded through a
// strict EnvelopedVersionState wrapper, like EnvelopedManifest /
// EnvelopedLeaf). Object lock is its first tenant; the block is the home
// for per-version state that mutates after the version is created.
type VersionState struct {
    // Retention is the stored retention document (§2), verbatim; nil when
    // retention has never been set.
    Retention []byte `cborgen:"r"`
    // LegalHold is tri-valued: 0 never set, 1 explicitly OFF, 2 ON. S3
    // distinguishes never-set (a 400 from GetObjectLegalHold) from an
    // explicit OFF (a 200).
    LegalHold uint8 `cborgen:"h"`
    // Tags is reserved for object tagging (planned follow-up). Declared now
    // so every rewrite round-trips it under the merge rule below; the
    // tagging feature adds handlers, not format. Nil until it lands.
    Tags map[string]string `cborgen:"t"`
}

// ObjectLeaf gains, beside Prev:
//
// State is the root of the per-key version-state MST: revSeqKey(seq) → the
// version's VersionState block CID. Nil when no version of this key carries
// explicit state, which is what makes state-free keys free to read (§6, §8).
State *cid.Cid `cborgen:"st"`
```

The tree is the same forked `mst` package, the same space, the same
`revSeqKey` rendering the prev tree uses (`s3frontend/version.go:78`); its nodes and the
state blocks are ordinary catalog-plane blocks. Point lookups are `state.Get(revSeqKey(seq))`.
A bucket default retention materializes nothing here (§7), so the tree holds exactly the
versions that received explicit state.

`VersionState` is not where `x-amz-meta-*` user metadata lives, and tagging will not move
it: S3 fixes user metadata at version creation (changing it requires writing a new version),
so it is canonical state on the immutable manifest (`ObjectManifest.Metadata`), while tags
mutate in place through their own API. The split is exactly the manifest/state-tree boundary.

The structural rules:

1. **State exists only on leaf-form values.** A manifest-arm key carries none, and its first
   state write upgrades it to a leaf whose `Current` is built from the manifest's own
   `Seq`/`VersionID` fields, with `Prev` nil. This amends invariant 6 of the versioning
   design: the leaf appears at a key's first retained supersession **or first version-state
   write**, and persists either way. The upgraded state (a leaf with nil `Prev`) already
   exists in the versioning design, so readers need no new cases.
2. **A state mutation rewrites only positional blocks**: the state block, the state-tree
   path, the leaf, and the top-MST path. Manifests and prev-tree entries are never
   rewritten, so invariant 5 (a version's identity never changes) holds verbatim.
3. **Mutations merge; empty blocks are elided.** Each operation replaces the fields it owns
   and carries every other field verbatim (§6). A mutation that leaves every field absent
   deletes the version's tree entry, and a tree left empty drops `State` to nil. Lock alone
   never empties a block (retention is never unset, and hold OFF is explicit state); the
   rule exists for operations that unset, such as tagging's delete.
4. **Replaced and removed state blocks join `gc_candidates`**; state-tree interior nodes are
   untracked, matching prev-tree and top-tree practice.
5. **`State` joins `ObjectLeaf` under the existing `"/objectleaf/0"` union key.** There is
   no compatibility shim: a pre-lock reader would silently drop the field on its next
   rewrite, and per the repo's dev-only data posture (reshape in place, reset dev data;
   `s3-versioning.md` §10) no such reader exists. The union's format-revision mechanism
   (`s3-versioning.md` §2.1) stays in reserve for revisions that need loud incompatibility.

### 4.2 Registry

```sql
-- The bucket's lock configuration: auth.BucketLockConfig JSON, stored verbatim.
-- NULL = never configured (GetObjectLockConfiguration => ObjectLockConfigurationNotFound).
ALTER TABLE ingot.buckets ADD COLUMN object_lock_config bytea;

-- CreateMultipartUpload's lock headers, carried to Complete (§7).
ALTER TABLE ingot.multipart_sessions
    ADD COLUMN lock_mode         text,
    ADD COLUMN lock_retain_until timestamptz,
    ADD COLUMN lock_legal_hold   text;
```

```go
// registry.State gains the raw config document.
ObjectLockConfig []byte // nil = never configured

// Registry gains:
// SetObjectLockConfig stores the bucket's lock configuration document verbatim.
SetObjectLockConfig(ctx context.Context, name string, cfg []byte) error

// Create gains an initial-state parameter so a bucket created with
// x-amz-bucket-object-lock-enabled is versioned and locked atomically
// (schema is dev-only; call sites are few).
Create(ctx context.Context, name string, space did.DID, init CreateState) error
type CreateState struct {
    Versioning       VersioningState // "" = unversioned
    ObjectLockConfig []byte          // nil = no lock
}
```

---

## 5. Bucket-level configuration

**CreateBucket.** `input.ObjectLockEnabledForBucket` (the
`x-amz-bucket-object-lock-enabled: true` header; currently ignored at `s3frontend/bucket.go:205`)
makes the create call `registry.Create` with `CreateState{Versioning: VersioningEnabled,
ObjectLockConfig: cfg}` where `cfg` is the JSON of
`auth.BucketLockConfig{Enabled: true, CreatedAt: &now}`. The Hilt forwarding path is
untouched: lock state is local bucket metadata, like versioning state.

**GetObjectLockConfiguration** returns `State.ObjectLockConfig` verbatim, or
`ErrObjectLockConfigurationNotFound` when nil. This replaces the stub at
`s3frontend/bucket.go:115` whose entire job was returning that sentinel; the method stays on
the per-request path (`CheckObjectAccess` calls it on every object PUT/DELETE) and costs the
one `registry.Get` the stub already performs.

**PutObjectLockConfiguration** requires `State.Versioning == Enabled` and answers
`ErrObjectLockConfigurationNotAllowed` (409) otherwise, then stores the controller's document
verbatim via `SetObjectLockConfig`. Enabling lock on an existing versioned bucket is allowed
(pinned by `Versioning_Enable_object_lock`); the controller has already validated the document
and rejects a non-Enabled status, so a stored config always has `Enabled: true`.

**PutBucketVersioning** gains one guard: `Suspended` against a bucket whose
`ObjectLockConfig` is set answers `ErrSuspendedVersioningNotAllowed` (pinned by
`Versioning_status_switch_to_suspended_with_object_lock`). This is what makes "a lock bucket
is always versioning-Enabled" an invariant rather than an initial condition, and it is why no
other path needs to consider lock state on an unversioned or suspended bucket: lock state can
only exist on buckets that were Enabled when it was written and can never leave Enabled.

---

## 6. The four per-version methods

All four run the same check order:

1. `registry.Get` misses: `ErrNoSuchBucket`.
2. The key is absent from the top MST: `ErrNoSuchKey`. Key existence outranks the lock
   gate — a missing key on a bucket without lock reports `NoSuchKey`, never the
   missing-configuration error (`GetObjectRetention_non_existing_object` pins it against a
   lock-free bucket, matching posix).
3. `State.ObjectLockConfig` nil or not Enabled: `ErrMissingObjectLockConfiguration`. This
   and step 2's precedence are the only lock-specific parts of the order; the rest is the
   skeleton any per-version state operation runs.
4. `classifyVersionID` rejects the token: `InvalidArgument` (`InvalidArgVersionId`).
5. The named version resolves per the §6.1 grammar of the versioning design (the current
   version for an empty versionId); a miss is `GetNoSuchVersionErr`.
6. The resolved version is a delete marker: `ErrMethodNotAllowed` (pinned by
   `Versioning_Put_GetObjectRetention_delete_marker` and the legal-hold twin).

**Reads** run it lock-free over `resolveVersion` (`s3frontend/version.go:144`), which already
has the leaf in hand, then seek the state tree:

- A manifest-arm key, a nil `State`, a tree miss at `revSeqKey(node.Seq)`, or a nil/zero
  lock field in the fetched `VersionState` all answer `ErrNoSuchObjectLockConfiguration`.
- **GetObjectRetention** returns `VersionState.Retention` verbatim. An expired retention is
  returned as stored; expiry is the controller's judgment.
- **GetObjectLegalHold** maps `LegalHold` 1/2 to `false`/`true`.

**Writes** run the same order inside a `bucketop` commit (per-bucket lock held, so the
read-modify-write below is serialized against every other mutation of the key):

1. Load the key's value; a manifest-arm key upgrades to a leaf (§4.1 rule 1).
2. Locate the target version on the leaf (current, null via `NullSeq`, or a prev seek) and
   fetch its manifest for the marker check.
3. Load the existing `VersionState` (if any) and merge per §4.1 rule 3: `PutObjectRetention`
   owns `Retention`; `PutObjectLegalHold` owns `LegalHold`; every other field is carried.
   The controller has already run `IsObjectLockRetentionPutAllowed` against the currently
   stored document (fetched through `GetObjectRetention`), so mode-transition policy never
   reaches the backend.
4. Write the state block, upsert `state.Add/Update(revSeqKey(seq), stateCid)`, queue the
   replaced state block and old leaf for GC, rewrite the leaf with the new tree root, splice
   the top MST.

---

## 7. Version-creating writes

`PutObject`, `CopyObject`, and `CreateMultipartUpload` inputs carry
`ObjectLockMode` / `ObjectLockRetainUntilDate` / `ObjectLockLegalHoldStatus`, already
validated by the controller (`utils.ParsObjectLockHdrs`: mode and date required together,
RFC3339, future-dated, known enum values). The backend's obligations:

- **Validation.** Lock headers against a bucket whose `ObjectLockConfig` is not enabled:
  `ErrMissingObjectLockConfigurationNoSpaces` from all three creation-time paths (§2's
  variant split; `PutObject_missing_bucket_lock` and `CopyObject_missing_bucket_lock` pin
  the wording). The controller populates the retain-until pointer unconditionally, so an
  absent header arrives as a pointer to the zero time and absence is judged on the zero
  value, never on nil.
- **Stamping, inside the commit.** When lock headers are present, `commitVersion` writes the
  new version's `VersionState` (retention =
  `json.Marshal(types.ObjectLockRetention{Mode, RetainUntilDate})` when a mode was supplied;
  hold when the header named `ON` or `OFF`) into the state tree in the same commit that
  installs the version, forcing the leaf form for a new key. The version and its protections
  land in one root swap: there is no state in which the version exists unprotected.
- **Multipart carry.** `CreateMultipartUpload` persists the three values on the session row
  (§4.2); `CompleteMultipartUpload` stamps them onto the version it commits, exactly as a
  single-shot PUT would have.
- **Defaults are not materialized.** A bucket default retention produces no per-version
  state. The controller applies the default dynamically inside `CheckObjectAccess`, and
  `GetObjectRetention` on a version without explicit retention reports unset even while a
  default protects it (pinned by `GetObjectRetention_unset_config`).

`CopyObject` copies bytes and metadata; lock state is never inherited from the source version.
Only the request's own lock headers stamp the destination, matching S3.

---

## 8. Reads

`HeadObject` and `GetObject` outputs gain `ObjectLockMode`, `ObjectLockRetainUntilDate`, and
`ObjectLockLegalHoldStatus`; the controller renders them as the `x-amz-object-lock-*` response
headers. After `resolveVersion`, when `State.ObjectLockConfig` is enabled and the resolved
value is a leaf with a non-nil `State`, one tree seek and one block fetch fill the three
fields (mode and date from the stored retention JSON, `ON`/`OFF` from the hold; absent fields
stay unset). Keys without state, and buckets without a lock configuration, skip the lookup
entirely, so the common read path costs nothing new. The same fetch is where future
state-block consumers (tagging's count header) read from. `ListObjectVersions` and the list
walks carry no lock fields.

---

## 9. Deletes and lifecycle

Enforcement happened in the controller before the backend was called, so the delete paths
change only in cleanup, and the cleanup rule is general: **a commit that permanently removes
a version also removes its state entry in the same commit.** A version-scoped delete that
removes version `S` from a leaf with a state tree runs `state.Delete(revSeqKey(S))` (a miss
is a no-op), queues the removed state block for GC, and drops `State` to nil when the tree
empties. Promotion and noncurrent removal already rebuild the leaf through `spliceLeaf`; the
state tree threads through it beside the prev tree. The rule also covers the write-rule
discard paths (`commitVersion`'s null replacement and null eviction), though lock alone
never exercises them: lock state requires a bucket that was Enabled when it was written and
can never leave Enabled (§5), while discards happen only on unversioned or suspended
buckets. State written by a future tenant without that guard (tags exist on unversioned
buckets) is covered by the same rule with no redesign. `DeleteBucket` needs no state sweep
for the same reason it needs no claim sweep: it refuses non-empty buckets, and an empty
bucket has no leaves left to carry state trees.

---

## 10. Governance bypass, deferred with bucket policies

The `x-amz-bypass-governance-retention` header is resolved entirely in the controller and is
authorized by the bucket policy: `CheckObjectAccess` fetches the policy document through
`GetBucketPolicy` and requires a grant of `s3:BypassGovernanceRetention`. Ingot does not model
bucket policies (`GetBucketPolicy` answers `ErrNoSuchBucketPolicy`, `s3frontend/bucket.go:130`),
and the controller maps a missing policy to `ErrObjectLocked`. Until bucket policies land,
bypass therefore always denies and `GOVERNANCE` retention is exactly as strict as
`COMPLIANCE`. The failure direction is safe (protected versions stay protected), and an
operator retains an escape hatch: the fork's `IsObjectLockRetentionPutAllowed` permits
same-mode replacement, so a mistaken retention can be rewritten with a near-term
retain-until date and waited out.

Bucket policies are planned follow-up work. When they land, bypass starts working with no
change to any code this document specifies (`CheckObjectAccess` already contains the policy
consult), and the conformance rows below that exercise bypass flip from XFail via the
ratchet.

---

## 11. Conformance and testing

The lock categories run under the versioned `S3Conf`: their buckets are created `withLock()`,
which makes them versioning-Enabled, so upstream teardown must walk `ListObjectVersions`.
Teardown of locked objects works without bypass: upstream's `cleanupLockedObjects` rewrites
each lock's retention to a few seconds out (same-mode replacement, which the fork allows even
for `COMPLIANCE`), sleeps past expiry, and deletes.

**New categories** (`itest/versity_lock_test.go`), from the upstream groups
`TestPutObjectLockConfiguration`, `TestGetObjectLockConfiguration`, `TestPutObjectRetention`,
`TestGetObjectRetention`, `TestPutObjectLegalHold`, `TestGetObjectLegalHold`, and
`TestWORMProtection`:

- The six Put/Get groups are pass-table rows, with two exceptions.
  `PutObjectLockConfiguration_not_enabled_on_bucket_creation` asserts the
  versioning-disabled gateway mode, where a lock configuration lands on an unversioned
  bucket with no versioning check (upstream's own comment calls it not S3 compatible);
  ingot answers the AWS 409 and the row is excluded, with
  `Versioning_object_lock_not_enabled_on_bucket_creation` pinning that behavior.
  `PutObjectRetention_overwrite_governance_with_permission` installs a bucket policy: XFail
  until bucket policies land.
- Lock-enabled-bucket cases from the plain-conf groups (`PutObject_with_object_lock`,
  `PutObject_racey_success`, the CopyObject lock-header cases,
  `CreateMultipartUpload_with_object_lock`) run under a dedicated versioned-conf category:
  their buckets are versioning-Enabled, and the plain teardown cannot empty a non-empty
  versioned bucket.
- `TestWORMProtection` contributes no pass rows yet. Its `checkWORMProtection`-based cases
  assert versitygw's unversioned-gateway lock model, where a plain PUT or unscoped DELETE
  against a locked object fails with `ErrObjectLocked`; a lock bucket in ingot is always
  versioning-Enabled, so those requests succeed by stacking a version or a marker (AWS
  semantics, §1). Those rows are excluded with this reason, the same treatment as the
  `VersioningDisabled_*` pair (`itest/versity_versioning_test.go:26`). The group's
  `*governance_bypass*` and `root_bypass` cases install bucket policies: XFail. Versioned
  WORM behavior is covered by the `Versioning_WORM_*` rows instead.

**Versioning-group additions.** The `Versioning_*` lock rows currently excluded by the
header note of `versity_versioning_test.go` move into the pass table: the configuration trio
(`Versioning_object_lock_not_enabled_on_bucket_creation`, `Versioning_Enable_object_lock`,
`Versioning_status_switch_to_suspended_with_object_lock`), the retention and legal-hold
grammar cases (invalid versionId, non-existing version, delete marker, success), and the
`Versioning_WORM_*` cases (version locked by hold / governance / compliance, marker
insertion over locked objects, overwrite via PUT / copy / multipart, marker removal under a
bucket default retention). None of them requires bucket policy (only the excluded
`Versioning_AccessControl_*` cases do).

**Promotions.** Existing XFail rows that flip once this lands, flagged by the ratchet:
`PutObject_with_object_lock`, `PutObject_missing_bucket_lock`,
`CopyObject_missing_bucket_lock`, `CopyObject_with_legal_hold`,
`CopyObject_with_retention_lock`, `CreateMultipartUpload_with_object_lock`,
`CreateMultipartUpload_with_object_lock_not_enabled`, `CreateBucket_default_object_lock`,
and `Versioning_DeleteObject_non_existing_objects` (its bucket is created `withLock()`).

**Unit tests** (inmem harness, `version_test.go` pattern): the §6 check order per method
(sentinel table tests: marker, unset state, disabled bucket, versionId grammar); the state
tree itself (manifest-arm upgrade on first state write, the merge carrying unowned fields
including the reserved `Tags`, tri-valued hold, cleanup through version-scoped delete and
promotion, `State` dropping to nil on the last removal); the §5 guards (versioning gate on
`PutObjectLockConfiguration`, suspend gate on `PutBucketVersioning`, create-with-lock
initial state); in-commit stamping across PUT, copy, and multipart complete, including the
missing-bucket-lock validation errors; Head/Get echo fields; a cborgen round-trip for
`VersionState` and the state-carrying leaf.

---

## 12. Out of scope

Each deferred feature has a landing path that leaves this design's format and invariants
unchanged:

- **Bucket policies** (and with them working governance bypass, §10): a per-bucket document
  stored beside `object_lock_config`, registry-shaped like every bucket-level configuration.
  The lock code needs no change when it lands (§10).
- **Object tagging**: its `Tags` field is reserved on `VersionState` (§4.1), so tagging adds
  handlers and conformance rows, not format. Specified in
  [`s3-object-tagging.md`](./s3-object-tagging.md).
- **ACL grants on lock operations**, and per-version object ACLs generally: a mutable
  per-version document is the shape `VersionState` hosts by an additive field and an arm
  bump.
- **S3 Batch Operations retention updates**: batch jobs drive the same per-object APIs this
  document specifies; scale is a throughput question (many state commits, foldable into
  shared root swaps), never a format one.
- **Lifecycle expiration**: an expirer runs outside the controller, so it enforces lock
  itself by reading the same per-version state and bucket configuration the controller
  reads; both are readable off the request path.
- **Materialized default retention**: versitygw evaluates the bucket default dynamically
  (§7), where AWS stamps it at version creation. If the controller ever adopts AWS
  semantics, the §7 in-commit stamping mechanism absorbs it: stamp the default when the
  request carries no lock headers.

The versioning design's own out-of-scope list (`s3-versioning.md` §10) drops object lock and
now points here.

---

## 13. Implementation map

| Where | Change |
|---|---|
| `bucket/leaf.go`, `gen/main.go`, `make gen` | `VersionState` + the `"/versionstate/0"` arm and `EnvelopedVersionState`; `ObjectLeaf.State` (§4.1) |
| `migrations/sql/` (new) | `buckets.object_lock_config`, `multipart_sessions` lock columns (§4.2) |
| `registry/registry.go`, `postgres.go`, `inmem/store.go` | `State.ObjectLockConfig`, `SetObjectLockConfig`, `Create` initial state (§4.2) |
| `s3frontend/objectlock.go` (new) | the four per-version methods: the §6 check order, the state-tree read and commit paths, the Head/Get echo helper (§8) |
| `s3frontend/version.go` | `commitVersion` in-commit stamping incl. forced leaf (§7); `deleteVersionScoped` + `spliceLeaf` state-tree threading and cleanup (§9) |
| `s3frontend/bucket.go` | `CreateBucket` lock header (§5), `PutObjectLockConfiguration` (new), `GetObjectLockConfiguration` (real config), `PutBucketVersioning` suspend guard |
| `s3frontend/object.go`, `copy.go`, `multipart.go` | lock-header validation + stamping via `commitVersion` (§7), Head/Get echo (§8), session carry (§7) |
| `itest/versity_lock_test.go` (new), `versity_{object,bucket,multipart,versioning}_test.go` | new categories, promotions, exclusions (§11) |
| `docs/s3-versioning.md` | §10 drops lock from out-of-scope, pointing here |

---

## Appendix A: rejected alternatives

**Manifest fields, rewritten on mutation.** Retention and hold live on `ObjectManifest`;
`PutObjectRetention` writes a new manifest block (same `Seq`, `VersionID`, `Body`) and
updates whichever pointer holds the old CID: the manifest-arm value, `leaf.Current`, or a
prev-tree entry. Its strongest case is zero extra blocks: the state sits on the version's own
record, and reads that already fetch the manifest get lock state free. Rejected because it
mutates the one block class the versioning design holds immutable: invariant 5 weakens to
"`Seq` and `VersionID` never change," every place that stores a manifest CID as an identity
must be spliced on each mutation, and each mutation adds a GC candidate for a block that did
not change in any way a reader cares about. The version-state tree delivers the same catalog
residency with the invariant intact.

**Inline state on the leaf.** An inline per-version list on `ObjectLeaf` (seq, mode,
retain-until, hold), rewritten with the leaf. Its strongest case is minimal machinery and
zero extra fetches: the resolver already holds the leaf, so enforcement reads and Head/Get
echo cost nothing. Rejected for its growth pattern: the leaf is fetched on every GET, HEAD,
and list touch of its key, and the versioning design deliberately keeps it a small pointer
block (it declines even ETag/size for list rendering). A writer supplying explicit lock
headers on every PUT of one key grows that key's leaf without bound, and every read pays for
it. The version-state tree is this option with the growth moved off the hot block: one
pointer on the leaf, one block per state-carrying version.

**A parallel per-bucket state MST.** A second root on `registry.State` mapping
`(key, seq)` to state blocks, isolating service state from the object tree entirely. Rejected
on plumbing: the entire pipeline assumes one root per operation. `bucketop.Tx` commits one
root, the segment `.ops` journal records `(bucket, newRoot)` pairs, recovery and
`forge_root_cid` advance on that single lineage, and `CASRoot` guards it. A second root needs
its own CAS, journal, and ship lineage, or a combined bucket-root record that changes what
`Root` means everywhere. A composite MST key also overflows `mst.MaxKeyBytes` for
maximum-length object keys. The per-key version-state tree reaches the same isolation one
level down, where the leaf already provides a single atomic attachment point.
