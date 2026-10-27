# logstore

`logstore` is ingot's **catalog journal**: the local, durable tier between the
S3 write path and the Forge network. Every S3 mutation's catalog blocks (the
dag-cbor MST nodes and `ObjectManifest`s) land here first (`AppendBatch`), are
fsynced to disk before the write is acked, and are later **sealed** and
**shipped** to Forge asynchronously. Reads consult the journal before falling
through to the network blockstore.

The log holds only the catalog. Object bodies are never journaled: they are
spooled and uploaded to Forge per blob before the catalog commit (see
`blockstore.Spool` and [`../docs/architecture.md`](../docs/architecture.md) §7.1),
so the data plane an earlier iteration shipped through this package is gone.

The log is **segregated per bucket**. `Manager` implements `blockstore.Log`
and owns one `Store` per bucket, each rooted under its own directory, so a
sealed segment holds exactly one bucket's blocks and ships to that bucket's
Forge space. A `Store` is a thin wrapper over one `PlaneLog`, the reusable
pipeline (seal trigger, ship transport, retention window). `AppendBatch`
routes by the op-root's bucket, lazily opening (and recovering) that bucket's
store on first write; `OpenManager` re-opens every bucket with leftover
segments at startup (the union of on-disk directories, segment rows and
buckets with journaled blocks), so unshipped sealed segments re-enqueue, and
lost ones are rebuilt from the journal, without waiting for the bucket's next
write.

The persistence backend (`Meta`) is `*registry.Postgres` in production and an
in-memory fake in tests; logstore never touches SQL directly. The two diagrams
below belong to the ingot diagram set; the set's index is
[`../docs/diagrams.md`](../docs/diagrams.md).

## On-disk layout

Each bucket owns a subdirectory; a segment is a single CAR plus its `.idx` and
`.ops` sidecars:

```
<DataDir>/segments/
  <bucket>/
    catalog/
      seg-<N>.car   seg-<N>.idx   seg-<N>.ops
```

| File | Contents |
|---|---|
| `seg-N.car` | CARv1 of the segment's dag-cbor blocks |
| `seg-N.idx` | JSON sidecar: `cid` to `offset,length`, size, sha256, sealed_at, op-roots |
| `seg-N.ops` | append-only log of per-batch `(bucket, newRoot)` records (length-prefixed CBOR) |

`N` is the zero-padded segment id from one **shared** allocator
(`ingot.segment_seq`), so ids are globally unique across buckets; the segment
row's `bucket` column names the owner. Each CAR carries a placeholder header
root; the real MST roots live in the `.ops` sidecar (and the `.idx`).

## Write path

```mermaid
sequenceDiagram
    autonumber
    actor C as bucketop.Tx<br/>(OpStaging.Commit)
    participant M as logstore.Manager
    participant S as bucket Store<br/>(one PlaneLog)
    participant FS as local disk
    participant DB as Meta<br/>(registry.Postgres)
    participant F as flush<br/>(newBucketFlushFunc)

    C->>M: AppendBatch(catalogBlocks, OpRoot{bucket, root})
    M->>S: route by OpRoot.Bucket (lazy open + recovery on first use)
    S->>FS: append CAR frames + .ops record, fsync both
    S-->>C: the segment's seq (licenses the caller's bucket-root CAS)
    C->>DB: CommitRoot: share-lock the segment row, journal the batch's blocks<br/>under it (none once it has shipped), CAS the root: one tx

    Note over S,FS: seal when size >= SealBytes or age >= SealAge<br/>(also at Close, and recovery force-seals)
    S->>FS: hash CAR, write .idx (tmp + fsync, rename, fsync dir)
    S->>DB: MarkSegmentSealed(size, sha256, op-roots) in one tx
    S->>F: enqueueFlush (queue cap 64; a full queue leaves it to the retry sweep)

    F->>F: SubmitShard: /blob/add the CAR to sprue, PUT, conclude, receipt<br/>(index publish is best-effort, space resolved via reg.Get at ship time)
    F->>DB: PutLocation(shard) + PutInclusions(every inner block)
    Note over F,DB: recorded before the shipped stamp, so retention<br/>can never retire blocks reads cannot resolve
    F-->>S: index digest
    S->>DB: MarkSegmentShipped: shipped_at + index_digest and, same tx,<br/>forge_root_cid per op-root (guarded: only while buckets.root_cid<br/>still equals the op-root), then the segment's journal rows deleted
    Note over S: marked shipped in memory only once the stamp lands
    Note over S: runRetention keeps the newest Retain shipped CARs,<br/>older ones retire (files unlinked, row stamped retired_at)
```

A batch may be empty of blocks: an MST mutation can produce a new root that
points only at nodes already materialized in a prior segment, so `AppendBatch`
is called with the op-root alone. A flush error keeps the segment unshipped
and retried (5 attempts, backoff 1s doubling to a 30s cap). A failed
`MarkSegmentShipped` after a successful ship takes the same attempts without
shipping again, and the segment counts as shipped only once the stamp lands,
so retention never retires a segment whose journal rows remain. After that,
and for a segment a full queue turned away, the **retry sweep** takes over:
every `ShipRetry` (1 minute by default; the first sweep lands at a random point
within it) the flush worker tries the bucket's unshipped sealed segments again,
oldest first, one attempt each, stopping at the first that fails. So a segment
waits out an outage, or its space's missing ship authority, without a restart.
A segment whose ship landed but whose stamp didn't is shipped again by the sweep. A bucket deleted
while segments were queued has nowhere to ship, so the closure returns nil and
the segment marks shipped and retires.

## Segment lifecycle

```mermaid
stateDiagram-v2
    [*] --> Open : createOpenSegment + InsertSegmentOpen
    Open --> Open : append (fsync, then index commit)
    Open --> Sealed : seal (hash CAR, write .idx, MarkSegmentSealed)
    Open --> Sealed : recovery, .idx on disk behind an open row (MarkSegmentSealed)
    [*] --> Sealed : recovery, row-less CAR with .idx (RehydrateSegment)
    Open --> [*] : recovery, CAR missing (journal rows rebuilt into a new segment, then DeleteSegment)
    Sealed --> Shipped : flush ok (MarkSegmentShipped: guarded forge_root advance, journal rows deleted)
    Sealed --> Retired : recovery, CAR missing (journal rows rebuilt into a new segment, then RetireSegment)
    Shipped --> Retired : retention past Retain (files unlinked, RetireSegment)
    Retired --> Sealed : recovery, a never-shipped retired segment's CAR and .idx back on disk (RehydrateSegment)
    Retired --> [*] : DeleteBucket (ReleaseRetiredSegments, or RemoveBucketLog)
```

- The DB `state` column holds only `open` and `sealed`. Shipped is the
  `shipped_at` stamp (plus `index_digest`); retired is the `retired_at`
  stamp, set once the files are unlinked. A retired row keeps its CAR
  `sha256` and `index_digest`, because those blobs stay registered in the
  bucket's space until `DeleteBucket` releases them; its op-root rows are
  dropped. Recovery and the startup sweep read only unretired rows.
- With `Ship=false` a segment stays sealed on disk forever: the sole source
  for local reads, with its blocks also journaled in Postgres for good, since
  nothing ships them.
- Recovery force-seals any recovered open segment, so each process starts
  with a brand-new open segment per bucket.

## Invariants

- **Durability before ack.** `Segment.append` fsyncs the CAR and `.ops` before
  committing the in-memory index, and a successful `AppendBatch` is what
  licenses the caller's bucket-root CAS. Body blobs are already durable and
  accepted on Forge before the commit that references them, so a crash never
  leaves a durable catalog entry pointing at non-durable data.
- **Journaled until shipped.** The commit that a batch's append licenses also
  writes the batch's blocks to `ingot.catalog_journal`, under the segment's
  seq, in the same transaction as the root swap; `MarkSegmentShipped` deletes
  them. So every block of a committed root that exists nowhere but this
  host's disk is also in Postgres, and in any copy of it: a standby, a
  restore. The commit share-locks the segment row and skips a shipped
  segment, and the ship locks that row before deleting, so no row outlives
  its segment. The rows have no foreign key to the segment row. When a
  segment's CAR is gone, recovery rebuilds its journaled blocks into new
  segments (below), then deletes its row if it was open and retires it if it
  was sealed. `DeleteBucket` drops the bucket's rows in the transaction
  that empties its root, since the empty tree needs none of them.
- **One bucket per segment.** The log is segregated per bucket; every segment
  ships to its own bucket's Forge space, and `DeleteBucket` can quiesce and
  release exactly one bucket's registrations.
- **`forge_root_cid` advances only on ship, and only guarded.**
  `MarkSegmentShipped` advances each op-root's bucket row in the same
  transaction as the shipped stamp, and only where `buckets.root_cid` still
  equals the op-root (the orphan-root guard).
- **Location before shipped.** The flush records the shard's location and
  every inner block's byte range (`blob_locations` + `shard_inclusions`)
  before the shipped stamp, so retention can never retire blocks the read
  tier cannot resolve.
- **The catalog is location-free.** Manifests and MST nodes reference content
  only by CID; byte location resolves at read time through the locator, never
  embedded in the DAG.

## Reads & tiers

`Manager.Get` consults every open bucket store and returns the first hit;
reads carry no bucket context today, so the lookup is linear in open buckets
(threading the bucket into the read path is the follow-up that removes the
scan). Each store checks its open segment, then its sealed segments
newest-first (`ReadAt` on the CAR). A miss, including a segment retired off
disk, returns `ErrNotFound`, and the layered reader (`blockstore.Layered`)
falls through to the network tier (`blockstore.Forge`: locate via the local
location/inclusion tables, then a ranged piri `content/retrieve`). LSM tiers:
**hot** is the open segment, **warm** is sealed-and-local, **cold** is shipped
(served from Forge once retired).

## Crash recovery

Opening a bucket's store reconciles its directory against the `Meta` rows
before any worker starts. Discovery keys on the CAR file:

| `Meta` row | On-disk CAR | Action |
|---|---|---|
| `open` | present, with a readable `.idx` | it sealed after the point `Meta` was restored to: `loadSealedFromIdx` + `MarkSegmentSealed`; re-enqueued |
| `open` | present, no readable `.idx` | `rebuildOpenFromDisk` (scan + truncate any torn trailing frame); then force-sealed |
| `sealed` | present | `loadSealedFromIdx`; re-enqueue when shipping and not yet shipped (newest first; what the queue can't hold, the retry sweep ships) |
| none | present, with a readable `.idx` | a sealed segment whose row was lost (`Meta` restored from behind the disk): `loadSealedFromIdx` + `RehydrateSegment`; re-enqueued |
| none | present, no readable `.idx` | orphan (crashed before the row): rebuild as open + `InsertSegmentOpen`, then force-sealed |
| `retired`, shipped | present | files left by a retire whose unlink didn't reach the disk: unlink them, keep the row |
| `retired`, never shipped | present | retired because its file was missing, and the file is back (a volume mounted late, say), possibly the only copy of its blocks: recovered as if it had no row. With a readable `.idx` that replaces the row; without one the CAR is rebuilt and ships under its retired row, whose files the next start removes |
| `sealed` | absent | retire the row (a crash between the unlink and the `retired_at` stamp, or a lost file) |
| `open` | absent | delete the row |
| — | sidecar only (`.idx`/`.ops`, no CAR) | stray; unlink |

Every recovered open segment is force-sealed, so each start opens a brand-new
one. There can be several: `Meta` restored from behind the disk, or a rebuild
from the journal that crashed partway.

Before the fileless rows go, recovery rebuilds any segment the journal still
files blocks under but whose CAR is gone (`rebuildFromJournal`): a lost or
replaced data volume, or a deleted file. The blocks are appended to new
segments, read from the journal a page at a time; a segment takes blocks until
the next would carry it past `SealBytes`. Each run of blocks' journal rows
move onto its new segment (`Meta.MoveJournal`, keeping their `created_at`)
once the run is fsynced there, so a crash partway leaves the rest for the
next start. The new segments are sealed and enqueued like any other; they
carry no op-roots, so `forge_root_cid` waits for the bucket's next shipped
write. A lost segment whose row is already gone is still found: the manager
also opens every bucket the journal names (`Meta.JournalBuckets`).

Before opening any bucket, the manager moves the segment seq allocator past
the highest seq on disk (`Meta.EnsureSegmentSeqAbove`). A database restored
from a point behind the disk would otherwise hand out a seq a segment file
already holds.

## Configuration

```go
type ManagerConfig struct {
    Dir      string                        // bucket b's segments live under <Dir>/<b>/
    Meta     Meta                          // persistence backend (registry.Postgres / fake)
    Catalog  PlaneConfig                   // pipeline template applied to every bucket store
    FlushFor func(bucket string) FlushFunc // per-bucket ship closure; required when Catalog.Ship
    Logger   *zap.Logger
}

type PlaneConfig struct {
    SealBytes int64         // open-segment CAR size threshold (0 -> 64 MiB)
    SealAge   time.Duration // max open age before sealing (0 -> 5s); ticker paces at SealAge/4
    Ship      bool          // false -> never ship, retained forever
    Flush     FlushFunc     // ships one sealed CAR; set by the Manager from FlushFor
    Retain    int           // shipped CARs kept locally (0 -> 6); ignored when !Ship
    ShipRetry time.Duration // how often unshipped sealed segments are retried (0 -> 1m)
}
```

The host (`server.go`) binds the per-bucket ship closure via
`newBucketFlushFunc(uploader, registry, locations, inclusions, bucket)`, which
builds a `uploader.CARShard` from the `Segment` accessors, calls
`uploader.SubmitShard`, and records the location/inclusion rows.

## Key symbols

| Where | Symbols |
|---|---|
| `manager.go` | `Manager` (per-bucket log, the production `blockstore.Log`): `ManagerConfig`, `OpenManager`, `AppendBatch`, `Get`, `Close`; `QuiesceBucketLog`, `ReleaseRetiredSegments`, `ShippedSegmentDigests`, `RemoveBucketLog` (DeleteBucket's seams) |
| `store.go` | `Store` (one bucket's catalog log): `Open`, `AppendBatch`, `Get`, `Close` |
| `planelog.go` | `PlaneLog` (the pipeline): `openPlaneLog`, `Append`, `Get`, `Close`; `sealOpenIfDue`, `flushLoop`, `flushOne`, `retryUnshipped`, `runRetention`, `sealTickerLoop` |
| `segment.go` | `Segment`: `createOpenSegment`, `append`, `seal`, `markShipped`, `retire`, `get`; `.idx`/`.ops` codecs |
| `recovery.go` | `(*PlaneLog).recover` (disk vs `Meta` reconciliation for one bucket store) |
| `types.go` | `State`, `SegmentMeta`, `Meta` |
| `config.go` | `Config`, `PlaneConfig`, `FlushFunc` |
| `../server.go` | `newBucketFlushFunc` (per-bucket flush: `uploader.SubmitShard` + location/inclusion rows) |
| `../uploader/forge.go` | `Uploader`, `CARShard`, `Forge.SubmitShard` |
