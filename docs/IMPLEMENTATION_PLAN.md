# Ingot architecture migration — implementation plan

Working tracker for bringing Ingot in line with [`architecture.md`](./architecture.md).
Current state is [`../DESIGN_NOTES.md`](../DESIGN_NOTES.md). Breaking changes are preferred;
there is no backwards-compatibility requirement and any code may be rewritten. Every phase keeps
the in-memory `testing/` harness green at its boundary.

## Scope

Deferred (designed-toward, not built):

- **S3 versioning** — reserve manifest fields and route MST keys through an identity
  `encodeKey(key, versionId)` seam, but do **not** implement inverted-version-id encoding,
  `ListObjectVersions`, delete markers, or `next_version_seq`. The scheme is not finalized.
- **Deployment topology** — assume Ingot runs next to Piri. Keep digest-before-upload + the local
  spool; `allocate-by-size` is out.
- **Ingot-side aggregation** — ship blobs as they exist in the body. `min_aggregate_size`,
  Regime-A/B compaction, and subroots are Piri-side concerns. Ingot only ever calls `remove(digest)`.
  `max_blob_size` is a config knob (default 256 MiB). Multipart operates per the doc.

## Ratified decisions

1. **Multipart: staged fast-follow.** Single-shot core + reference index first; multipart tables
   created up front in the Phase 2 migration so the multipart code is purely additive (Phase 6).
2. **Read locate: local Postgres location table now, behind a `Locator` interface.** A new
   `blob_locations (space,digest) → piri location` table is populated at accept; reads resolve
   spool → cache → `Locator.Locate`. An indexer-backed `Locator` impl (with bare-commitment
   consumption) is a later swap-in (Phase 7). No indexer dependency in the first cut.
3. **Multi-shard manifest: flat list + reserved `IndexRoot`.** `Body.Blobs` is the working index;
   a nullable `IndexRoot` CID field is reserved so a UnixFS + sharded-dag-index credible-exit path
   can be added later without a manifest-codec break.
4. **Delete dependency: build the bookkeeping, stub the network call.** `blob_refs` ref-counting and
   the `remove`/`unallocate` call sites are built now; backed by an `inmem` fake (fully testable) and
   a flag-gated real Piri call that no-ops until the Piri/Sprue handler lands. libforge already ships
   the `blob.Allocate/Accept/Remove` client bindings.

## Locked defaults (not separately ratified; strong under breaking-changes-preferred)

- Delete the data `PlaneLog` outright — collapse `logstore` to one catalog pipeline; retire the
  "plane-unaware write path" invariant.
- Delete the `BodyCodec` seam — replace `FixedChunker` with `bucket.SplitBody` / `bucket.OpenBody`
  utilities (only one impl ever existed; the write path needs spool + uploader injection that a
  stateless codec fights).
- Spool at `$DataDir/spool/<digest>`; `upload_intents` keyed globally by digest (matches the dedup
  model); spool doubles as the read-after-write floor and read cache.
- Two-phase `PutObject`: off-lock ingest + per-blob upload, then a short locked commit
  (manifest + MST splice + `blob_refs` + guarded root swap). Accept failure → client-visible 5xx.
- `versionId` is a function of the MST key, not stored in the manifest.

## Five load-bearing shifts

1. **Manifest reshape** — `Body` becomes a flat ordered `Blobs []BlobRef{Digest,Offset,Length}`
   covering `[0,size)` + stored `etag` + reserved versioning fields. The schema linchpin.
2. **Data-plane inversion** — bodies stop flowing `OpStaging → data PlaneLog → CAR → background ship`;
   instead coarse-split → spool (`upload_intents`) → per-blob `allocate/PUT/accept` to Piri
   **synchronously before commit** (200 = durable + accepted). The catalog plane stays an LSM CAR
   pipeline shipping in the background. The data `PlaneLog` is deleted.
3. **Reference index** — `blob_refs (space,digest) → versions`, populated on commit, decremented on
   overwrite/delete; `remove(digest)` at zero claims.
4. **Read by digest** — GET iterates `Body.Blobs`, serving each from spool/cache else
   `Locator.Locate(space,digest) → ranged Piri GET`.
5. **Multipart + S3 correctness** — conditional requests, checksums, `DeleteObjects`, `CopyObject`,
   then multipart, layered on the spool/refs core.

## Phases

### Phase 1 — Manifest reshape *(no behavior change; pure model)* — ✅ DONE

- [x] `bucket/manifest.go`: `Body` → `{Size, SHA256, MD5, Blobs []BlobRef, IndexRoot *cid.Cid}`;
      new `BlobRef{Digest []byte, Offset int64, Length int64}`; dropped `Content`/`Format` +
      `FixedChunkerIndex`. Added `ObjectManifest.{ETag string, DeleteMarker bool}` (deleteMarker
      reserved, unused; versionId intentionally NOT stored — it is a function of the MST key).
- [x] `gen/main.go`: added `BlobRef`, dropped `FixedChunkerIndex`; `make gen` (idempotent regen,
      verified `*cid.Cid` + `[]BlobRef` encode cleanly).
- [x] `bucket/chunker.go`: rewrote `FixedChunker` → `BlobSplitter`, coarse-split at `MaxBlobSize`
      into one raw block per blob (via `io.CopyN`, so small objects don't allocate a max-sized
      buffer), sha256+md5 over the whole body in one streaming pass, `Body.Blobs` covering
      `[0,size)`. Reader (`blobBodyReader`) iterates `Body.Blobs` by offset/length, reconstructing
      each blob CID from its digest. **Fixed a latent buffer-reuse corruption bug** (the old code
      reused one `buf` across chunks while staging held them by reference — broke any multi-chunk
      object; never triggered because tests used sub-1MiB single-chunk objects). Blobs still route
      through the existing staging → data-plane path; only the manifest shape changed.
- [x] `s3frontend/object.go`: set `mf.ETag` at PutObject (hex md5); `etagOf()` returns stored ETag.
- [x] `config.go` + `server.go` + `testing/harness.go`: `ChunkSize` → `MaxBlobSize` (0 → 256 MiB);
      `WithChunkSize` → `WithMaxBlobSize`. (No `cmd/config.go` change needed — it embeds `Config`.)
- [x] `testing/blobsplit_test.go` (new): single/multi-blob (> max_blob_size, boundary-crossing
      ranged GETs) + zero-byte round-trips. `GetObject/large_object` promoted from xfail → passing
      (the multi-chunk fix makes it pass now).

Deliverable: `make build` + full `go test ./...` green; PUT/GET/HEAD round-trip for single-blob,
multi-blob, and zero-byte objects through the in-memory harness. Bytes still live in the data-plane
CAR path (inverted in Phase 3) — this was purely the model reshape.

### Phase 2 — Schema + fakes *(new surface, no callers)* — ✅ DONE

- [x] Migration `00003_stores.sql`: `blob_refs`, `upload_intents`, `blob_locations`,
      `multipart_sessions`, `multipart_parts`, `gc_candidates`; `buckets.space`
      (DEFAULT '' so existing inserts keep working) + reserved `versioning`/`next_version_seq` +
      `buckets_space_idx`.
- [x] `registry/stores.go`: focused interfaces (`BlobRefStore`, `IntentStore`, `LocationStore`,
      `MultipartStore`, `GCStore`) + row types + state constants + `NullVersionID` sentinel.
      `registry/stores_postgres.go`: Postgres impls (jsonb metadata, `bytea[]` digests, `ON CONFLICT`
      upserts, single-winner latch via `RowsAffected`). `State.Space` added (read-only for now).
- [x] `inmem/stores.go` + struct maps: MemStore mirrors all five stores with deep-copy semantics.
- [x] `inmem/stores_test.go`: ref-count-to-zero, intent state machine, ordered parts + FK cascade,
      location round-trip, and the single-winner latch under `-race` (exactly one of 32 racers wins).
- [x] `migrations/up_live_test.go` + `registry/postgres_live_test.go` (both DSN-gated on
      `INGOT_TEST_DSN`): validated against a throwaway docker Postgres — migration applies + is
      idempotent; all store SQL round-trips. (Caught one denormalization invariant: `blob_refs` PK
      excludes `space`, so a `(bucket,key,version)` belongs to one space.)

Deliverable: migrations apply (validated live); both `*registry.Postgres` and `inmem.MemStore`
satisfy the new interfaces; new methods unit-tested (inmem) and live-tested (Postgres). Smoke suite
still green — no production path calls the new methods yet.

> **Note — `buckets.space` population deferred.** The column exists and is read into `State.Space`,
> but `Create` does not yet set it (defaults to ''). It is threaded through bucket creation in
> Phase 3, when the space DID reaches the write path. `blob_refs.space` is supplied explicitly by
> callers, so it does not depend on `buckets.space` being populated.

### Phase 3 — Data-plane inversion *(the hard shift, sub-stepped)*

**3a — Spool + body-uploader seams** ✅ DONE (`4aee491`)
- [x] `blockstore/spool.go`: `Spool` — local on-disk blob store keyed by digest (atomic
      write+rename), a `BlockReader`+`BlockWriter`. Pure file I/O (blockstore can't import registry —
      registry imports blockstore — so the `upload_intents` lifecycle is owned by `s3frontend`).
- [x] `uploader/blob.go`: `BodyUploader` seam + `Forge.UploadBlob` (reuses `forgeclient.BlobAdd`,
      which already drives allocate→PUT→accept for single-shot). `inmem.NopUploader` gains a no-op.
- [x] Deleted the `BodyCodec` seam → `bucket.SplitBody`/`OpenBody`/`OpenBodyRange` package funcs.

**3b — Rewire write/read path; data PlaneLog goes dormant** ✅ DONE (`4aee491`)
- [x] `s3frontend.PutObject` is two-phase: off-lock ingest (split → spool → record `upload_intents`
      → upload each blob → mark accepted), then a short manifest-only commit. 200 ⇒ all blobs
      durable+accepted before commit.
- [x] `blockstore/layered.go`: read path is `spool → log → base`. Bodies resolve from the spool
      (read-after-write/cache); catalog blocks miss the spool and resolve from the log.
- [x] Wiring: `ServerDeps`/`serverParams`/module/harness gain `Intents` + `BodyUploader`.
- [x] Proven green: a new assertion confirms bodies are spooled by digest (and dedup'd), not logged.
      The data `PlaneLog` still exists but is dormant (no raw blocks reach it).

**3c — Delete the dead data PlaneLog** ✅ DONE
- [x] `logstore.Store` collapsed to one catalog `PlaneLog`; `Config.Data` / `ServerConfig.*Data` /
      `Config.DataPlane` knobs removed; `Log.AppendBatch(dataBlocks, …)` → catalog-only
      `AppendBatch(catalogBlocks, opRoot)`; `OpStaging` dropped the raw/cbor split (`isDataBlock`,
      `dataOrder`); `PlaneData` retired from the enum / `Planes`; `registry.MarkSegmentShipped`'s dead
      data branch removed. `logstore/store_test.go` reworked to single-plane (the two
      plane-independence tests removed, a `TestCatalogNeverShips` kept for standalone mode).
- [x] **Fixed a latent 3b bug:** `cmd/serve.go`'s `standaloneApp` was missing the `IntentStore` +
      `BodyUploader` providers (a build can't catch a missing fx provider). Added them + a
      `TestStandaloneApp_GraphValidates` guard (verified it fails when a provider is dropped).

**3d — Real forge location glue + commit-ordering fix** *(forge-mode behavior validated in smelt at Phase 7)*

**3d-1 — Conditional `forge_root_cid` advance (orphan-root fix)** ✅ DONE
- [x] `registry.MarkSegmentShipped` (Postgres + inmem) advances `forge_root_cid` to an op-root only
      `WHERE root_cid = $root` — a durable op-root the bucket never adopted (lost CASRoot race) no
      longer advances `forge_root` past the real root. Unit-tested (inmem) + live-tested (Postgres).

**3d-2 — Record `blob_locations` on upload** ✅ DONE *(build + harness green; URL/provider validated in smelt)*
- [x] `Forge.UploadBlob` parses the accept commitment (`added.Location`, an `/assert/location`
      invocation) for the provider DID + retrieval URL (same decode the index locator uses).
- [x] `s3frontend.ingestBody` records `blob_locations{space,digest,provider,url,size}` per accepted
      blob. Backend gains `Space` + `LocationStore`; wired through `ServerDeps`/fx (`ServerSpace`,
      optional so standalone/harness default to "")/harness/standalone.

**3d-3 — Local-table `Locator` read tier** ⏳ DEFERRED to Phase 7 *(forge-mode read; needs live validation)*
- [ ] A `LocationStore`-backed `locator.Locator` (local-first, indexer fall-through for catalog
      blocks) so forge-mode body reads resolve from `blob_locations`. Deferred because: it is not
      harness-testable (the spool serves all in-process reads), it is only exercised **after** spool
      eviction (not yet built), and the body-vs-catalog base-reader split + the `blockstore`↛`registry`
      cycle-avoiding package placement are best designed against the live smelt stack. The location
      **data** is already recorded (3d-2); Phase 7 wires the **consumer** and validates end-to-end.

> **Validate 3d in smelt at Phase 7:** real `UploadBlob` location parsing, the `blob_locations`
> contents, and the 3d-3 read tier are all forge-mode paths the in-process harness can't exercise —
> confirm them against the live sprue+piri+indexer round-trip when Phase 7's e2e lands.

Deliverable (3a–3b, done): PUT spools + uploads each blob synchronously, commits a catalog-only
manifest, returns 200 only after accept; GET reconstructs across blobs; zero-byte stores no blob;
full in-memory smoke suite green.

### Phase 4 — Reference index / delete — ✅ DONE

- [x] `s3frontend.reconcileClaims` diffs the prior vs new body-digest sets on every commit:
      a digest newly referenced gains a `blob_refs` claim, one no longer referenced loses its claim
      and (at zero `(space,digest)` claims) is queued for release; a digest in **both** is untouched
      (a re-PUT of identical bytes never churns the row). Wired into `PutObject` (overwrite-in-place
      loads the prior manifest's digests) and `DeleteObject` (releases all of them).
- [x] `uploader.BlobRemover` seam: `RemoveBlob(digest)` released **after** the commit, off the
      critical section. `Forge.RemoveBlob` is a logged no-op (libforge has the `blob.Remove` binding;
      the Piri/Sprue handler is to-build — §9) so the bookkeeping runs end-to-end without the network
      primitive. A recording remover validates the call sites.
- [x] Superseded manifest CIDs recorded → `gc_candidates` on overwrite/delete (write-only; precise
      superseded-MST-internal-node tracking deferred — the table has no collector this iteration).
- [x] Wired `BlobRefs`/`GC`/`Remover` through `ServerDeps`/fx/harness/standalone.
- [x] `s3frontend/refindex_test.go` (white-box, in-process): dedup across keys (claim 2), overwrite
      same content (claim held, no release), overwrite different content (old released exactly once),
      delete-to-zero (released), delete-one-of-two (held until the last reference drops).

Deliverable: overwrite and delete maintain `blob_refs` and emit `RemoveBlob` exactly at the last
claim; dedup keeps a blob alive while any version references it; verified in-process. (Real
`RemoveBlob` network behavior validated in smelt at Phase 7.)

> **Transactionality note.** Claim updates run inside the commit critical section (before the guarded
> root swap); `RemoveBlob` runs after. A cross-process CASRoot loss (single-writer in-process never
> hits it) could leave claims out of step with the committed root — reconciled by Phase 7's crash
> recovery (`upload_intents` × `blob_refs`). Claims-in-closure is the safe-side choice (leak, never
> data loss).

### Phase 5 — S3 correctness surface

**5a — Conditional requests** ✅ DONE (`24cd922`)
- [x] GET/HEAD/PUT/DELETE evaluate `If-Match`/`If-None-Match`/`If-(Un)Modified-Since` via versitygw's
      `backend.Evaluate*Preconditions` (same evaluators its posix backend uses). PUT checks once
      before ingest (fail fast) and RE-CHECKS under the per-bucket lock at commit (race-safe).
      `mapCommitError` surfaces a precondition error from inside a `WithTx` closure with its status
      intact. Flipped: PutObject/conditional_writes, GetObject/conditional_reads,
      DeleteObject/conditional_writes (HeadObject/conditional_reads now genuinely evaluated).

**5b — DeleteObjects (batch)** ✅ DONE (`f99d49b`)
- [x] Best-effort batch delete ≤1000 keys via a shared `deleteObjectKey` helper (reusing the Phase-4
      release path); Quiet mode; per-key Deleted/Error result. New `TestSmoke_DeleteObjects` (3 cases).

**5c — CopyObject (metadata-only dedup)** ✅ DONE (`f99d49b`)
- [x] Resolve source manifest → write a dest manifest pinning the SAME body digests + a reference
      claim per digest (no bytes move, no upload). MetadataDirective COPY/REPLACE,
      `x-amz-copy-source-if-*`, cross-bucket sources, copy-to-self rules. New `TestSmoke_CopyObject`
      (13 cases); `TestSmokeXFail_CopyObject` holds 3 out-of-scope cases (Expires header not stored;
      a posix-filesystem error that doesn't apply to ingot's flat keyspace). `UploadPartCopy` lands
      with multipart (Phase 6).

**5d — Checksums (`x-amz-checksum-*` validate/echo)** ✅ DONE (`a8c01f6`)
- [x] `ObjectManifest` gains `ChecksumAlgorithm` + `Checksum` (base64), `cbor_gen` regenerated.
      `PutObject` wraps the body with versitygw's `utils.HashReader` (computes + validates as it
      streams; mismatch → the right BadDigest API error), stores it, and echoes it. GET/HEAD echo the
      stored checksum under `ChecksumMode` (whole-object GET only). CopyObject carries it. Promoted 7
      xfail rows (PutObject checksums_success/dir_object_checksums_success/incorrect_checksums/
      invalid_credentials, GetObject checksums/dir_object_checksum, HeadObject checksums); verified
      stable across repeated full-package runs (the versitygw integration suite has test-ordering
      global state — always validate xfail flips in the full `./testing/` package run, not via `-run`).

Deliverable: correct 412/304 race-safe at commit; checksums validate + echo; batch delete +
metadata-only copy work; conditional + checksum xfail rows flipped, CopyObject/DeleteObjects coverage
added. **Phase 5 complete.**

### Phase 6 — Multipart *(fast-follow)*

- [ ] `CreateMultipartUpload`/`UploadPart`/`CompleteMultipartUpload`/`AbortMultipartUpload`;
      parked parts; single-winner Complete/Abort latch; accept-at-Complete; `-N` ETag
      (`hex(md5(concat part md5s))`). Tables exist from Phase 2.

Deliverable: full multipart lifecycle in the harness; latch serializes Complete vs Abort; Abort
reclaims parked/deduped blobs. Multipart xfail rows flip to pass.

### Phase 7 — Forge wiring hardening

- [ ] Conditional `forge_root_cid` advance (`UPDATE … WHERE name=? AND root_cid=?`) — fixes the
      orphan-root bug.
- [ ] Live `remove`/`unallocate` against real libforge/Piri; crash-recovery reconciliation
      (`upload_intents` × `blob_refs`); indexer-backed `Locator` impl (bare-commitment consumption).
- [ ] smelt e2e: multi-blob PUT/GET, overwrite, delete-then-remove, multipart against real
      sprue+piri+indexer. Update `DESIGN_NOTES.md` / `logstore/README.md` / `CLAUDE.md`.

## Critical path

Phases **1 → 2 → 3 → 4** deliver a working single-shot system (PUT/GET/HEAD/DELETE, object-aligned
blobs, synchronous durability, dedup, reference-counted delete) fully exercised in the in-memory
harness with zero external to-build dependencies. Phases 5–7 are leaves on that core.
