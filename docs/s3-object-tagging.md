# S3 Object Tagging in Ingot

Implementation spec for S3 object tagging: the three per-version tagging methods, tag
stamping at version creation, the tag-count echo, and the conformance surface. Tagging is the
second tenant of the per-key version-state tree that
[`s3-object-lock.md`](./s3-object-lock.md) §3 and §4.1 specify, and this document rides that
design: the `Tags` field already exists on `VersionState`, reserved for exactly this feature,
so tagging adds handlers and conformance rows with no format change.

---

## 1. How tagging works

A tag set is per-version mutable state: up to ten key/value pairs attached to one object
version, readable and replaceable at any time through `GetObjectTagging`,
`PutObjectTagging`, and `DeleteObjectTagging`, each taking an optional `versionId`. A version
can also be born tagged: `PutObject`, `CopyObject`, and `CreateMultipartUpload` accept an
`x-amz-tagging` header (a URL-encoded query string), and `CopyObject` chooses between
inheriting the source version's tags and taking the header via `x-amz-tagging-directive`.
`GET` and `HEAD` report only the count (`x-amz-tagging-count`); reading the tags themselves
takes the dedicated API.

Tagging differs from lock in three ways that shape the implementation. It has **no bucket
gate**: tags work on any bucket, unversioned included, so the check order is the lock
order minus the lock-enabled step. Its **absent state is a success**: a version with no tags
answers an empty tag set (200), where lock answers a 400 sentinel. And it has a **true unset
operation**: `DeleteObjectTagging` clears the set, which is what the state tree's
empty-block elision rule (`s3-object-lock.md` §4.1 rule 3) exists for — lock never
exercises it, tagging does. Because unversioned buckets carry tags, the write-rule discard
paths (null replacement, null eviction) now genuinely encounter state entries; the §9
cleanup rule of the lock design already covers them, so no delete path changes.

versitygw's controller owns validation and rendering: `PutObjectTagging` bodies are parsed
and validated there (`utils.ParseTagging`: at most ten tags, key length 128, value length
256, the duplicate-key and invalid-character checks), and the backend receives a clean
`map[string]string`. The `x-amz-tagging` header is the backend's to parse, through the
shared `backend.ParseObjectTags` helper (URL decoding plus the key/value length checks),
before any bytes are ingested.

---

## 2. The backend contract

The three methods (versitygw `backend/backend.go:90`):

```go
GetObjectTagging(ctx, bucket, object, versionId string) (map[string]string, error)
PutObjectTagging(ctx, bucket, object, versionId string, tags map[string]string) error
DeleteObjectTagging(ctx, bucket, object, versionId string) error
```

Creation-time inputs: `Tagging *string` on `s3response.PutObjectInput` and
`CreateMultipartUploadInput`, plus `Tagging` and `TaggingDirective` on `CopyObjectInput`.
The controller defaults the directive to `COPY` when the header is absent, so the backend
only ever sees `COPY` or `REPLACE`. Outputs: `TagCount *int32` on `HeadObjectOutput` and
`GetObjectOutput`, set only when the resolved version carries at least one tag.

The check order is `s3-object-lock.md` §6 without its lock-enabled step:

1. `registry.Get` misses: `ErrNoSuchBucket`.
2. The key is absent from the top MST: `ErrNoSuchKey`.
3. `classifyVersionID` rejects the token: `InvalidArgument` (`InvalidArgVersionId`).
4. The named version resolves per the versioning grammar; a miss is `GetNoSuchVersionErr`.
5. The resolved version is a delete marker: `ErrMethodNotAllowed` (all three methods; pinned
   by `Versioning_PutGetDeleteObjectTagging_delete_marker`).

Past the order, absence is never an error: `GetObjectTagging` on a version without tags
returns the empty map (the controller renders an empty `<TagSet/>`, pinned by
`GetObjectTagging_unset_tags`), and `DeleteObjectTagging` on a version without tags is an
idempotent success.

---

## 3. Storage

`VersionState.Tags` (`s3-object-lock.md` §4.1) holds the set, and every rule written there
applies unchanged:

- **Merge**: `PutObjectTagging` and `DeleteObjectTagging` own `Tags` and carry `Retention`
  and `LegalHold` verbatim; the lock methods carry `Tags` in return. A replaced set is
  last-write-wins on the one field.
- **Elision**: an empty replacement (`PutObjectTagging` with an empty `TagSet`, or
  `DeleteObjectTagging`) stores nil, and a state block left with every field absent is
  removed from the tree; the emptied tree drops `State` off the leaf.
- **Upgrade**: a manifest-arm key takes its leaf on the first tag write, the same
  first-state-write amendment to versioning invariant 6 the lock design made. This is now
  reachable on unversioned buckets.
- **Cleanup**: a commit that permanently removes a version removes its state entry, on every
  removal path. Tags on unversioned buckets make the discard paths live: a null-replacing
  PUT discards the old null version and prunes its tag entry in the same commit.

Reads resolve exactly as lock reads do: the resolver already holds the leaf, a nil `State`
answers without another fetch, and one tree seek plus one block fetch serves a tagged key.

---

## 4. Version-creating writes

- **PutObject** parses `input.Tagging` through `backend.ParseObjectTags` before ingest, so an
  invalid header fails fast and uploads nothing. The parsed set joins the version's initial
  `VersionState` beside any lock headers and stamps in the same commit
  (`s3-object-lock.md` §7): the version and its tags land in one root swap.
- **CopyObject** with directive `COPY` gives the destination version the source *version's*
  tags (the resolved source, current or version-scoped); with `REPLACE` it parses the
  request's own header. Lock state is never inherited on copy; tags are, exactly when the
  directive says so.
- **CreateMultipartUpload** parses the header for validation at create, carries the raw
  string on the session row, and `CompleteMultipartUpload` stamps the parsed set onto the
  version it commits, exactly as a single-shot PUT would have.

---

## 5. Reads

`HeadObject` and `GetObject` gain `TagCount`: after `resolveVersion`, the same single
state-block fetch that fills the lock echo fields counts the tags, and the field is set only
when the count is nonzero. Keys without state, and versions without tags, cost and report
nothing. The lock echo stays gated on the bucket's lock configuration; the tag count is not
(tagging has no gate), so the state fetch now happens whenever the resolved leaf carries a
state tree.

---

## 6. Conformance and testing

The tagging groups run under the plain conf: their buckets are neither lock-enabled nor
versioned, so the plain teardown applies. The versioned tagging behaviors are the
`Versioning_*` rows, which run in the existing versioned Versioning category.

**New categories** (`itest/versity_tagging_test.go`): `TestPutObjectTagging`
(non_existing_object, long_tags, duplicate_keys, tag_count_limit, invalid_tags, success),
`TestGetObjectTagging` (non_existing_object, unset_tags, invalid_parent, success), and
`TestDeleteObjectTagging` (non_existing_object, success_status, success,
expected_bucket_owner).

**Versioning-group additions**: the eight tagging rows
(`Versioning_{Put,Get,Delete}ObjectTagging_invalid_versionId` and
`_non_existing_object_version`, `Versioning_PutGetDeleteObjectTagging_delete_marker`,
`Versioning_PutGetDeleteObjectTagging_success`).
`Versioning_AccessControl_object_tagging_policy` stays excluded with the AccessControl
family (admin-API users).

**Promotions**: `PutObject_tagging`, `CreateMultipartUpload_with_tagging`,
`CopyObject_should_copy_tagging`, `CopyObject_should_replace_tagging`, and
`GetObject_success` (its ETag assertions passed all along; it XFailed on the `TagCount`
echo).

**Unit tests** (inmem harness): put/get/delete round-trip including the empty-set answers;
tagging on an unversioned bucket (the gate-free order, the manifest-arm upgrade, and the
null-replacement discard pruning the old version's tags); the versionId grammar and marker
sentinels; the merge carrying lock fields across tag writes and tags across lock writes;
header stamping on PUT, both copy directives, and the MPU carry; the `TagCount` echo.

---

## 7. Out of scope

Bucket tagging (`Put/Get/DeleteBucketTagging`: bucket-level documents, registry-shaped like
the lock configuration), tag-based authorization (policy conditions on tags), and the
`Versioning_AccessControl_object_tagging_policy` case (admin-API users). The versioning
design's out-of-scope list drops object tagging and points here, as does the lock design's.

---

## 8. Implementation map

| Where | Change |
|---|---|
| `s3frontend/objecttag.go` (new) | the three methods over the §2 check order; the tag half of the state-block echo |
| `s3frontend/objectlock.go` | the state-target resolution and state-write helpers take a require-lock flag (tagging passes false); the echo helper adds the tag count and fetches whenever the leaf carries state |
| `s3frontend/object.go`, `copy.go`, `multipart.go` | header parse via `backend.ParseObjectTags` (§4), copy-directive handling, session carry, `TagCount` on Head/Get |
| `migrations/sql/` (new) | `multipart_sessions.tagging` (the raw header string) |
| `registry/stores.go`, `stores_postgres.go`, `inmem/store.go` | `MultipartSession.Tagging` |
| `itest/versity_tagging_test.go` (new), `versity_{object,multipart,versioning}_test.go` | new categories, the versioning rows, promotions (§6) |
| `docs/s3-versioning.md`, `docs/s3-object-lock.md` | out-of-scope lists point here |
