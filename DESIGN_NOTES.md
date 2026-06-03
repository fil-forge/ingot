# ingot — how it works

ingot is an **embeddable S3 gateway over the [Forge](https://github.com/fil-forge)
network**. It presents each S3 bucket as a per-bucket Merkle Search Tree (MST),
journals mutations to a local LSM-style log, and ships sealed segments to Forge
as a guppy-style edge client. Reads fall through local tiers and finally to the
network.

This is the architecture **as it operates today**. Subsystem detail lives in
package READMEs (notably [`logstore/README.md`](./logstore/README.md)); how to
work in the repo lives in [`CLAUDE.md`](./CLAUDE.md).

## Two ways to run it

- **Library.** A host (piri / guppy / sprue) imports the fx `Module(cfg)` (or
  `ServerModule` + the non-fx `New(ctx, ServerConfig, ServerDeps)`) and supplies
  a logger, a Postgres pool, the **agent** signer (`ServiceIdentity`), and the
  upload-service (sprue) endpoint.
- **Daemon.** `cmd/` builds an `ingot` binary (cobra/viper/fx). `ingot serve` has
  two modes:
  - **standalone** — in-memory registry/metadata (`inmem.MemStore`), no Forge, no
    Postgres; both planes retained on local disk. Local/dev S3.
  - **forge** — Postgres + the sprue edge client + a login-derived token store.
  Plus `ingot login <email>`, `ingot space generate` (provision + grant), and
  `whoami`. Docker-native; ships as a smelt system.

## Write path

```
S3 PUT (versitygw: sigv4, path-style)
 └─ s3frontend.Backend
     └─ bucketop.Tx              per-bucket lock; snapshot the bucket Root
         ├─ bucket.FixedChunker  chunk body → raw leaf blocks + a chunk index
         ├─ ObjectManifest       size, sha256/md5, S3 headers + user metadata, body ref
         ├─ mst Add/Update       new per-bucket root CID (only path nodes rewritten)
         └─ OpStaging.Commit     classify staged blocks by codec, then:
             └─ logstore.Store.AppendBatch(dataBlocks, catalogBlocks, opRoot)
                 ├─ data PlaneLog.Append    → fsync segments/data/seg-N.car
                 └─ catalog PlaneLog.Append → fsync segments/catalog/seg-N.car + .ops
         └─ registry.CASRoot     advance the bucket Root in Postgres (old → new)
```

A PUT is **acked** once both planes are fsynced locally and the root CAS lands in
Postgres; it is **durable on Forge** only after the background ship. Blocks split
into two planes by CID codec at `OpStaging.Put`: **`cid.Raw` → data** (object-body
chunks), **dag-cbor → catalog** (MST nodes, manifests, chunk indexes).

## The two planes

The catalog and data planes are **independent pipelines** built from one module
(`logstore.PlaneLog`) under a thin `logstore.Store` coordinator. Each plane seals
on its own threshold, ships through its own transport, and retains on its own
window — so the catalog can be configured never to ship (kept on local disk
forever) while the data plane ships and retires. Full lifecycle in
[`logstore/README.md`](./logstore/README.md). The load-bearing rules:

- `Store.AppendBatch` fsyncs **data before catalog**, so a crash never leaves a
  durable catalog entry referencing non-durable data.
- **`forge_root_cid` advances only when the catalog plane ships** — catalog roots
  are the MST roots that become durable on Forge. Op-roots (`(bucket, newRoot)`
  per batch) live only on catalog segments.
- **The catalog is location-free**: manifests / MST nodes reference data chunks by
  CID only; the digest → (piri, blob, byte-range) mapping is resolved at read time
  through the indexer, never embedded in the DAG. This is what lets the data
  plane's storage mechanism evolve without touching the catalog or read path.

## Shipping (forge mode): the edge-client flow

`uploader.Forge.SubmitShard(plane, shard)` ships one sealed CAR as a guppy-style
edge client — the data plane stays local, only the control plane crosses to
sprue:

1. `/blob/add` the CAR against **sprue** (which allocates against a piri).
2. HTTP **PUT** the CAR bytes to the allocated piri.
3. `/ucan/conclude` a synthesized `/http/put` receipt — piri has no conclude
   handler, sprue does; this is the step that lets `/blob/accept` resolve.
4. poll `/blob/accept` → the `/assert/location` commitment.
5. build a 1-shard sharded-dag-index, `/blob/add` it, then `/index/add` it (sprue
   republishes to the indexing-service).

Sprue witnesses every blob. **Byte-presence ≠ locator-presence**: the index is
published on every ship, never skipped just because the bytes are already there.

## Read path

```
S3 GET → resolve bucket Root (registry) → walk MST → ObjectManifest
       → stream body via blockstore.Layered:
           data / catalog PlaneLog (local segments, newest-first)
           → blockstore.Cached (byte-bounded LRU, Config.ReadCacheBytes)
             → blockstore.Forge: indexer locate(digest) → ranged piri /content/retrieve
```

Local hits serve from the warm tier; a retired segment (shipped, beyond its
Retain window) is unlinked locally, so its reads fall through to Forge.

## Identity & auth (single-space)

- **agent** = `ServiceIdentity.Signer` (host-injected; daemon: `identity.key_file`
  PEM). The issuer of every invocation to sprue.
- **space** = `<DataDir>/space.key` (ed25519, minted on first run). The subject of
  `/blob/add`, `/index/add`, `/content/retrieve`.
- The agent's authority over the space is delegations held in a **token store**
  (`tokens.cbor`): the daemon self-seeds `space → agent` (blob/add, allocate,
  accept, index/add, content/retrieve) at boot; `ingot login <email>` adds the
  `account → agent` delegations; `ingot space generate --provision-to <email>`
  provisions the space to the account on sprue (`/provider/add`) and grants access
  (`space → agent` + `space → account`, `/access/delegate`).
- One instance = one space = one tenant.

## State & durability

- **Postgres (`ingot` schema)** is the mutable index: per-bucket `root_cid` +
  `forge_root_cid`, and per-`(plane, seq)` segment metadata. goose tracks its
  version at `ingot.goose_db_version` (never collides with a host's own
  migrations). Standalone mode swaps Postgres for `inmem.MemStore`.
- **The MST is the data**: immutable, content-addressed, self-verifying, shipped
  to Forge as-is.
- A bucket is single-writer-correct via the per-bucket in-process lock + the
  Postgres root CAS.

## Known gaps

- **S3 conformance**: no multipart upload; no conditional requests
  (`If-Match`/`If-None-Match`); no `Range` on HEAD.
- **Single-space / single-tenant**: the `space.key`-on-disk model has no
  tenant → space mapping, and the space DID threads through reader/uploader/MST.
- **No HA / no GC**: a bucket is single-writer (in-process lock) → no HA without
  leader election; local CARs are a durability SPOF until shipped; a never-ship
  catalog (and anything reachable from `root_cid` but not `forge_root_cid`) grows
  with no sweeper.
- **Orphan `forge_root_cid`**: `registry.MarkSegmentShipped` advances
  `forge_root_cid` unconditionally, so a writer whose `AppendBatch` succeeds but
  whose root CAS fails can later advance `forge_root_cid` to a root the bucket
  never published. Needs a conditional update.
- **Carried Forge-client copies** — `forgeclient/`, `tokenstore/`,
  `blockstore/locator/`, `internal/ucanexec/` duplicate guppy/sprue code to stay
  cycle-free (ingot must never import guppy/sprue — guppy embeds ingot). A shared
  `forge-client` library would remove them.
- **Co-located data path (future)**: when ingot runs in the same rack as piri, the
  *data* plane may write straight into piri's shared store (eliding the network
  PUT) while still witnessing through sprue. The location-free catalog makes that
  a data-plane-only change.
