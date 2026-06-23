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

### Phase 2 — Schema + fakes *(new surface, no callers)*

- [ ] Migration `00003`: `upload_intents`, `blob_refs`, `blob_locations`, `multipart_sessions`,
      `multipart_parts`, `gc_candidates`; `buckets.space` (+ reserved `versioning`/`next_version_seq`).
- [ ] `registry` + `inmem.MemStore`: methods for ref-count, intent state machine, location
      upsert/lookup, multipart session/part CRUD + the single-winner latch. Unit-tested in isolation.

Deliverable: migrations apply; both `*registry.Postgres` and `inmem.MemStore` satisfy the expanded
interface; new methods unit-tested. Smoke suite still green (no production caller yet).

### Phase 3 — Data-plane inversion *(the hard shift)*

- [ ] Spool (`$DataDir/spool/<digest>`, global-by-digest, backed by `upload_intents`).
- [ ] Body uploader seam: `uploader.Forge` decomposed from `BlobAdd` into per-blob
      `allocate → PUT (skip on dedup) → conclude → accept`; write the `blob_locations` row at accept;
      `inmem.NopUploader` fakes allocate/accept in-memory so reads work.
- [ ] `OpStaging` becomes catalog-only; **delete the data `PlaneLog`**; `logstore.Open` builds one
      (catalog) pipeline; data-plane recovery removed; add `upload_intents`-driven recovery.
- [ ] Read path: spool → cache → `Locator.Locate(space,digest)` (local-table impl) → ranged Piri GET,
      iterating `Body.Blobs`; catalog reads unchanged.
- [ ] Commit ordering: blobs accepted before catalog `AppendBatch` + guarded root swap; guarded swap
      before op-root durability.

Deliverable: PUT spools + uploads each blob synchronously, commits a catalog-only manifest, returns
200 only after accept; GET reconstructs across blobs; zero-byte stores no blob. Data PlaneLog gone;
catalog plane still ships in the background. Full in-memory smoke suite green.

### Phase 4 — Reference index / delete

- [ ] Populate `blob_refs` on commit; overwrite-in-place dereferences the prior manifest's digests;
      `remove(digest)` when a `(space,digest)` claim hits zero (flag-gated seam + `pending_removes`
      drain so commit never blocks).
- [ ] `DeleteObject`; record superseded MST nodes → `gc_candidates` (write-only).

Deliverable: overwrite and delete maintain `blob_refs` and emit `remove(digest)` exactly at the last
claim; dedup keeps a blob alive while any version references it; smoke asserts ref-count transitions.

### Phase 5 — S3 correctness surface

- [ ] Conditional requests (read-time + re-checked under the MST critical section via a `Tx`
      precondition callback); `x-amz-checksum-*` validate/echo; `DeleteObjects` (≤1000, best-effort,
      Quiet); `CopyObject`/`UploadPartCopy` (metadata-only under dedup).

Deliverable: correct 412/304, race-safe at commit; checksums validate+echo; batch delete + copy work;
corresponding xfail smoke rows flip to pass.

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
