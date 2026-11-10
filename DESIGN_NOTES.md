# ingot: how it works

ingot is an **S3 gateway over the [Forge](https://github.com/fil-forge)
network**. It presents each S3 bucket as a per-bucket Merkle Search Tree
(MST), stores object bodies on Forge storage nodes as content-addressed
blobs, journals the catalog (MST nodes and manifests) to a local per-bucket
log, and serves reads back through local tiers with the network as the last
resort.

This is the architecture as it operates today. The target design and its
rationale live in [`docs/architecture.md`](./docs/architecture.md); the
as-built diagram set is [`docs/diagrams.md`](./docs/diagrams.md); subsystem
detail lives in package READMEs (notably
[`logstore/README.md`](./logstore/README.md)); how to work in the repo lives
in [`CLAUDE.md`](./CLAUDE.md).

## Running it

- **Library.** A host imports the fx `Module(cfg)` (or `ServerModule` plus
  the non-fx `New(ctx, ServerConfig, ServerDeps)`) and supplies a logger, a
  Postgres pool, and the **agent** identity (libforge `identity.Identity`); config names
  the sprue endpoint (`upload_service_url`/`_did`) and the hilt endpoint
  (`auth_service_url`/`_did`, with the `auth_service_proofs` delegation
  chains hilt issues to this agent).
- **Daemon.** `ingot serve` builds the same wiring from a config file
  (cobra/viper/fx): Postgres, the sprue edge client, and hilt are all
  required. The CLI is `serve`, `whoami`, `version` and `blake3`. Docker-native;
  ships as a smelt system.

There is no standalone or in-memory mode: the deployment under test is
always the real gateway. Tenancy is owned by **hilt**: an operator
provisions tenants and access keys through hilt's tenant API, hilt mints
each bucket's space and issues the S3 credentials, and ingot never
self-provisions.

## Write path

A PUT splits the body into blobs of at most `max_blob_size` as it arrives
(the BLAKE3 tree and md5 in the same pass) and streams each blob to the
network before anything commits: `/blob/add` against sprue by size, an HTTP
PUT of the bytes to the allocated piri as they arrive, with the sha256
digest computed on the way, then a `queue-space-shard.add` per blob, which
concludes the receipt, records the `/blob/accept` location commitment, and
takes the write's pending reference to the blob (see "References" under
Bucket encryption). The upload service must be able to add a blob by
digest code (the hash function, before the digest is known); a request
fails otherwise. A failed PUT fails the request, and its allocation is
aborted, so the client retries. Only then does the commit run, as a
`queue-bucket.commit` task on the bucket's subject (its space DID), run sync
for the request and placed on the instance that holds the bucket's catalog
log, so one change to a bucket runs at a time: allocate the version seq,
write the manifest, splice the MST, fsync one `AppendBatch` of the new
catalog blocks, and advance the bucket root in Postgres, guarded on the root
the commit started from, with the reference index (`blob_refs`) written in
the task's own transaction: the version's claims land and take over the
write's pending references, superseded versions' claims drop, and a blob
whose claim count reaches zero gets a `queue-space-shard.release` task,
queued in the same transaction and due after the release grace. The full
trace is the
[PutObject diagram](./docs/diagrams.md#putobject-stream-and-upload-in-the-request-commit-on-the-bucket-queue).

A commit's arguments are declarative: the bucket and an ordered list of key
changes (Put, Delete, SetAttributes), plus the authority the tasks it spawns
need (the release proofs and the `/upload/add` and `/upload/remove`
proofs). A Put covers PutObject, CopyObject, CompleteMultipartUpload and
delete markers; a Delete removes a version, or the current one, which the
commit turns into a marker or a removal of the key from the bucket's
versioning state; SetAttributes sets retention, legal hold and tags. A
within-bucket CopyObject reads its source and takes its references in the
request, so the commit sees an ordinary Put. The changes apply in order to
one `bucketop.Tx`, each advancing a working root. A change that fails on
its own terms (a precondition, a missing version, a version under lock, an
expired reference, a superseded reservation) fails only itself: the Tx
returns to its mark and the rest still apply. `DeleteObjects` sends every
key in one commit, so up to 1,000 deletes cost one fsync and one root
advance, and a key refused in the request (AccessDenied) or by the commit
fails alone.

**Every body blob is encrypted at ingest** (the FilOne encryption design's
write side, `s3frontend/encrypt.go`), unless its bucket opted out — see
"Bucket encryption" below. Each plaintext piece SplitBody cuts
gets a fresh CEK and streams through FEE into a `COSE_Encrypt` envelope
(AES-256-GCM STREAM, 256 KiB chunks); the envelope is what the network receives
and the cache stores, under its **ciphertext** digest. The CEK is wrapped
twice. The region wrap (`regionkey.Provider`, bound to (space, digest)) goes
into the blob's `blob_encryption_params` row before any manifest can
reference the digest; every read uses it. The tenant wrap is the envelope's
single COSE recipient: ECDH-ES+A256KW to the tenant's X25519 wrap key,
which Hilt publishes in the tenant's did:plc document at the `#wrap`
verification method. The recipient kid is the key's fingerprint (its
Multikey string), so the envelope names the exact key it was wrapped to and
the tenant's private key alone recovers the plaintext, with no region and no
database involved. The write path learns the tenant from Hilt's authorize
response (`AuthorizeOK.Tenant`, cached per access key for the local fast
path) and resolves its wrap key through `tenantkey` (PLC resolver, cached
for `tenantkey.cache_ttl`); a write that cannot obtain the recipient fails.
The split geometry, manifest spans, `Body.Size`, the BLAKE3 tree, md5 and ETag are all
plaintext values — only the digest and the stored sizes
(`upload_intents.Size`, `blob_locations.Size`) name ciphertext.
Consequences, per the RFC: content **dedup is gone** for bodies (a fresh CEK
makes every stored digest unique), a DELETE that releases a blob's last
claim also deletes its params row (crypto-shred — the ciphertext is
unreadable even where copies survive), and a **cross-space CopyObject
re-ingests** (the CEK wrap is space-bound, so the source's blobs cannot be
shared: the plaintext streams through the read path into new blobs under the
destination's space, as a PUT of those bytes would; within one space the copy
stays metadata-only). A copy source in **another tenant's bucket is refused**
`AccessDenied` before that: hilt refuses the source when it authorizes the
request, and ingot compares the tenant recorded on the two bucket rows as
well (a row whose owner predates the record matches none). Because hilt
resolves the source during authorization, ahead of the gateway's argument
validation, a copy whose source parses but names a missing or foreign bucket
(an invalid bucket name, an empty key) reports hilt's answer (`NoSuchBucket`,
`AccessDenied`) rather than `InvalidArgument`; a source hilt cannot parse
(bad percent-encoding) is authorized as a plain write and the gateway
reports the encoding error as S3 does.
Rotation: Hilt replaces `#wrap` in place and archives the
old key, so a write inside the cache TTL of a rotation still recovers.

A `200` therefore means the body is durable and accepted on the network and
the catalog mutation is fsynced locally and journaled in Postgres; the
catalog becomes durable on Forge through the background ship below.

## The catalog log

The log journals only the catalog, and it is segregated per bucket:
`logstore.Manager` holds one `Store` per bucket, so a sealed segment holds
exactly one bucket's blocks and ships to that bucket's Forge space. The
load-bearing rules ([`logstore/README.md`](./logstore/README.md) has the
full lifecycle):

- A successful `AppendBatch` (fsynced CAR plus `.ops` record) is what
  licenses the commit's guarded root advance.
- **The unshipped catalog is journaled in Postgres**: the root advance's
  transaction writes the batch's blocks to `ingot.catalog_journal` under the
  segment `AppendBatch` reported, and shipping that segment deletes them, as
  do `DeleteBucket` emptying the bucket's root and the bucket row's deletion.
  A copy of Postgres (a standby, a restore) therefore holds every block its
  roots need that this host's disk alone would otherwise hold. Recovery
  rebuilds a segment whose file is gone from its journaled blocks, so Ingot
  can start from a recovered Postgres alone (`logstore/README.md`, crash
  recovery).
- **`forge_root_cid` advances only when a segment ships, and only guarded**:
  the update lands in the same transaction as the shipped stamp, and only
  where `buckets.root_cid` still equals the op-root.
- **Shipping is a task**: the log seals on its own trigger (size or age) and
  reports each seal, and at open any sealed segment left unshipped, which
  queues a `queue-bucket.ship` on the bucket's subject. The ship sends the
  bucket's unshipped sealed segments oldest first, stops at the first
  failure and fails the task, which retries with no attempt limit (1 s
  doubling to 5 minutes). A ship and the bucket's commits share the subject,
  so no commit journals blocks under a segment while that segment ships.
- **The catalog is location-free**: manifests and MST nodes reference
  content by CID only; byte location resolves at read time through the
  locator, never embedded in the DAG.

## Shipping: the edge-client flow

`uploader.Forge` ships every blob the same way, whether an object-body blob
at PUT time or a sealed catalog CAR from a `queue-bucket.ship`:

1. `/blob/add` against **sprue** (which allocates against a piri).
2. HTTP **PUT** the bytes to the allocated piri (skipped on dedup).
3. `/ucan/conclude` a synthesized `/http/put` receipt: piri has no conclude
   handler, sprue does, and this is the step that triggers `/blob/accept`.
4. Poll the `/blob/accept` receipt for the `/assert/location` commitment.
5. For catalog CARs only: build a 1-shard sharded-dag-index, `/blob/add` it,
   then `/index/add` it (best-effort; sprue republishes to the
   indexing-service). The shard's location and every inner block's byte
   range are recorded locally (`blob_locations` and `shard_inclusions`)
   before the segment is marked shipped.

Multipart parts stop after step 2 (**parked**: durable, unaccepted) and
each blob's `queue-space-shard.accept`, queued by the upload's
`queue-multipart-upload.accept`, runs steps 3 and 4 in the background
while the client is still uploading; an abort unwinds a
parked blob with `/blob/abort` and an accepted one with `/blob/remove`. A
part keeps no local copy: the accept concludes each blob from its park row
and never reads its bytes. A part
copied from an existing object (`UploadPartCopy`) is ingested the same way:
the source's plaintext range streams through the decrypting read path into
new parked blobs, so the source may be in any bucket of the tenant and
nothing is shared with it.

Each part also records its BLAKE3 tree (`blake3tree`): the aligned
subtrees and blocks of its whole chunks, hashed at the object offset
`UploadPart` assumes from the parts recorded so far (exact once every lower
part exists, else assumed uniform), plus the raw bytes either side of those
chunks (under 1 KiB each, empty for the MiB-sized parts every mainstream
client sends). Complete checks each assumed offset against the part's true
one, re-hashes only the parts whose assumption was wrong (the final part always,
the others within `multipart_rehash_budget`, one maximum part by default;
past it Complete is refused with InvalidPartLayout and the upload stays open), hashes the chunk straddling each part boundary
from the raw bytes, merges the parts' subtrees into the object's digest,
builds its block list from their blocks, and checks the blocks against the
digest. A single-part object takes its digest from part 1's own root. The
object's `x-cid` and block list are therefore the same as a single PUT's
would be. The tree material lives in its own table (`multipart_part_trees`),
read only by Complete and deleted once the object commits; every other part
listing leaves it on disk.

The part rows are the acceptance queue. `UploadPart` parks every blob of
the part through its `queue-space-shard.add`, then records the part as
`parked`, with each blob's plaintext length. The same transaction queues the
upload's `queue-multipart-upload.accept` (`s3frontend/acceptor.go`,
`registry/accept.go`) as a replacement due now, so an upload has at most one
accept pending. That accept runs on the upload's own subject, in turn with
its other changes, and does nothing unless the session is `open`. It makes no network calls: it lists the upload's parked parts and
queues, with its commit, a `queue-space-shard.accept` for each of their
blobs (`registry.BlobAcceptSpec`), and marks a part with no blobs accepted.

A blob's accept runs under the blob's own subject (`registry.ReleaseSubject`:
the space and the digest), the one the blob's releases and a write's adds
run under, so a blob's conclude never overlaps its release or a reference
being taken. It is a replacement due now, so a blob has at most one accept
pending however many parts or uploads name it. A blob already located (a
dedup) needs nothing concluded. A parked blob is concluded on its own in one
`/ucan/conclude`, whose response carries the blob's `/blob/accept` receipt
and location commitment; step 4's polling remains the fallback. The
conclude has a 2-minute timeout (`acceptConcludeTimeout`), since the Forge
client sets no timeout of its own. A parked blob nothing in the space
references any more, as a dropped part's is, is left for its release, which
aborts the allocation. So is a blob with neither a park nor a location: it
has been released.

The blob's accept writes commit with the task, as SQL statements whose
`WHERE` clauses apply each one only while the rows it depends on stand
(`registry.ConcludedBlob`, `LocatedBlob`, `AcceptedParts`). A concluded
blob's park row is swapped for its location row only while the park the
conclude started from stands, matched by its `/blob/add` task. Its upload
intent is marked `accepted`, and a `published` intent stays `published`. A
blob found already located has any park left beside the location dropped,
only while that location stands. A parked part lists the blobs still to be
accepted (`multipart_parts.unaccepted`, every blob it names when it is
written). While the blob is located, its accept takes it off the list of
every parked part of an open session in the space that names it, and marks
`accepted` each part it leaves with none. That costs each accept one update
of each such part's row, not a check of every blob the part names. A part's
other blobs are accepted under other subjects, and their updates take turns
on the row, so the last sees the others' removals. A part written since it
was listed still lists all its blobs, each with an accept of its own to come,
and a part replaced or dropped meanwhile is not there to mark. A park of
the same bytes recorded since by another write names another allocation,
and stays for its own conclude. A failed conclude fails the blob's accept,
and nothing it wrote is recorded; the retry concludes again, which the
upload service treats as a repeat. A blob's accept retries 5 times, from
5 s, doubling, up to 1 minute (`acceptRetry`); a later part's accept, or
Complete's, queues whatever is left. `ingot.multipart.parked_age` (read from
the part rows when it is collected), `ingot.multipart.accept_passes` (blob
concludes, by outcome) and `ingot.multipart.complete_deferred` report the
queue's health; the README lists them.

Complete queues the accept of each blob of the parts it names that are still
parked, then brings those blobs' pending accepts forward from any retry's
backoff and waits for them within `acceptWaitBudget` (30 s), a sync no-op on
each blob's subject running once the tasks due before it there have run.
Past the budget it answers OperationAborted. A named part still parked after
the wait has a blob whose accept failed, and Complete fails for the client to
retry; a part whose blob has gone answers InvalidPart.

An abort, an auto-abort or a part re-upload queues a release of a blob the
upload's accept may be concluding. The release runs under the blob's
subject, so it runs before the conclude, and the accept finds the park gone
and records nothing, or after it, and removes the accepted blob. The release
deletes the park and location rows together in its commit, so a location is
never written for a blob a release has taken. Ingot treats a `/blob/remove`
receipt as final. If the upload service registers an accepted blob only
after a remove that arrived earlier, as it may when a conclude Ingot gave up
on is still running there, that window is the upload service's to fix, and
Ingot does not guard it. Two uploads in one space holding the same bytes
share a park, and its one accept concludes it once and marks both uploads'
parts.

Every change to a multipart upload runs as a task on
`queue-multipart-upload` (`s3frontend/multipartqueue.go`), with the upload
ID as its subject, so one change to an upload runs at a time: a part never
lands beside a Complete or an Abort, and a Complete and an Abort never
overlap. The queue is first in the runner's order. CreateMultipartUpload
runs `queue-multipart-upload.create`, which runs `queue-bucket.admit-upload`
as a sync child (it refuses a bucket a DeleteBucket has closed), writes the
session row, and schedules `queue-multipart-upload.abort`, the auto-abort,
one session TTL out (`multipart_session_ttl`, 7 days by default, negative
for never). UploadPart streams the part's blobs and takes their references
in the request, then runs `queue-multipart-upload.add-part`, which records
the part row. One that waits more than 30 s behind the upload's other
changes answers SlowDown, and one for an upload that is not open answers
NoSuchUpload.

`CompleteMultipartUpload` runs `queue-multipart-upload.complete`. It waits
up to 10 s (`replayWaitBudget`) for the upload's other changes, another
Complete included, and past that answers OperationAborted. An upload
already completed replays the result recorded on its session, the same
ETag and version id. Otherwise Complete validates the parts, checks the
re-hash budget, reserves and reports the version id, and queues the accept of
each blob of the parts it lists that are still parked, so a part it drops
cannot fail it. It queues them outside its own commit, and waits for them, so
their writes commit before Complete reads the parts again and assembles the
object's tree, which reads the accepted blobs back through their locations.
If they do not finish within 30 s (`acceptWaitBudget`), Complete answers
OperationAborted, a 409, and the upload stays open. The AWS CLI and other botocore-based
clients treat that 409 as final and abort the upload, so the 30 s budget is
the margin before a client sees a failed upload. An accept that fails fails
Complete with its error. After the accept, every listed part must be
`accepted` with the blobs the request validated. A part with a blob that
has gone from its provider is not the part the client named: nothing
keeps a part's bytes to upload them again, so Complete answers `InvalidPart`
and the client uploads the part again. Every part blob streams to its
provider as the part arrives, and an UploadPart whose blob does not stream
fails. Complete lays out the object's byte ranges from the part rows' blob
lengths and runs `queue-bucket.commit` as a sync child, which claims every
blob in the root advance's transaction, a few statements however many
blobs there are (`registry.Postgres.RootCommitWrites`). The same
transaction moves the session from open to completed (a guard), deletes
the part trees, queues the releases of the unlisted parts' blobs, and
schedules `queue-multipart-upload.reap` one session TTL out. The reap
deletes the completed session and its parts and releases nothing: the
object's claims hold the listed parts' blobs.

AbortMultipartUpload runs `queue-multipart-upload.abort`, as does the
auto-abort. It acts only on an open upload: it deletes the session, whose
parts and part trees cascade, and queues the releases of its parts' blobs.
An upload completed or gone is NoSuchUpload for a client's abort and
nothing for the auto-abort. DeleteBucket aborts the bucket's open uploads
and reaps its completed ones from the request, after it has closed the
bucket.

Every session teardown (an abort or auto-abort, a superseded part, the
parts a Complete omitted) queues a `queue-space-shard.release` task, due at
once, for each blob of the session that is not still live in it, in the
same commit as the change that drops the blob's part reference. Whether
the blob is in fact free to go is decided when the release runs, never at
teardown: a release of a digest the space still references (a committed
object's claim, a write's pending reference, a part of an in-flight session)
does nothing. Deciding at
teardown would race another teardown sharing the blob. A committed blob is
recognisable for good: its first reference claim marks its upload intent
`published` in the same transaction, so an abort leaves such blobs to the
object path's own releases whatever has become of the object. A blob with
no intent at all is one a release already took, intent and all: an abort
that still holds its part rows leaves it alone rather than queueing it again, since the second release
would find neither rows nor intent and ask the network to remove a blob that
may never have reached it.
The part rows are the only index to the blobs and cascade away with the
session, so the queued tasks are what make the teardown recoverable: an
abort that fails commits nothing and runs again, and the releases it
committed are tasks the runner retries. Both retry with no attempt limit and
a backoff from 1 s doubling to 5 min. Once the session row is gone, the
teardown waits, within 10 s (`releaseNow`), for the runner's workers to run
those releases, so an Abort has released its parked blobs on their
providers before it answers; whatever is left runs later. Each release carries its
own authority: the `/blob/remove` and `/blob/abort` proof chains captured
from the request that queued it, since it runs after that request has gone. The release reads the blob's
state from its rows rather than its intent. A location row means accepted
and the space's claim is removed; a park row alone means parked and the
allocation is aborted, or removed instead when the provider refuses the
abort because the blob was accepted after all (a conclude ran and ingot
never learned of it). A blob with neither row may still be on the network:
a conclude can be accepted and then fail to record its location. It gets a
network remove, which the upload service treats as success for a blob it
never registered. The crypto-shred goes first and the network step next.
After the network step nothing can fail but the commit: the location, park
and upload intent rows are deleted by writes committed with the task's
outcome, so a retry sees the same state. The cached copy is removed through
a `queue-shard.evict` task, unless another space still holds an intent for
the digest.
A release whose digest a part of an in-flight session still references is
stale: that session's Complete turns the part's reference into a claim, and
its teardown queues a release of its own.

## Local blob storage

Body blobs live on local disk in one directory, the **cache**
(`<data_dir>/cache`, `blockstore.BlobCache`), which holds copies of object
blobs the provider already holds. As an object's blob streams to its
provider, the write tees the same bytes into a temporary `.tmp-*` file in
the cache (`BlobCache.CreateTemp`). Once the blob's location and `accepted`
state are both recorded, the write adopts the file under its digest by
rename (`BlobCache.Adopt`) before the PUT answers, so the reads that follow
are served locally. A file that cannot be created or written costs only the
cached copy: the upload carries on, and reads go to the provider. A
multipart part keeps no local copy. With `cache_writes: false`, no write
keeps a copy, so the cache holds nothing and every read goes to the
provider. Reads try the cache, then the network.

A cached copy has a time to live, `cache_ttl` (an hour), counted from when
it was cached or last read. Once a write has adopted its copy, it queues a
`queue-shard.cache` task, serialized on the digest, which dates the file and
schedules a `queue-shard.evict` for when the time runs out. A read from the
cache with less than half the time left, by this process's in-memory record
of the last renewal, runs `queue-shard.cache` again; its eviction is
submitted as a replacement, so the later one is kept. Each renewal also sets
the file's modification time, which orders the budget pass. A read on a
node that has restarted renews on its first read, since the record is lost.

The cache keeps a running byte count of its blob files, and counts writes
in progress as their bytes land. With `local_blob_max_bytes` set, a sweeper
checks the total every 30 seconds and, when it is over the budget, evicts
cached copies, oldest modification time first, until usage is at 90% of the
budget, however much of their time to live is left: a time to live does not
bound the disk, and a burst of writes can fill it before anything expires.
It reads the cache directory to order it, and evicts each through
`queue-shard.evict` run as a sync task, so the removal takes its turn with
the digest's other tasks and usage has dropped before the pass moves on; the
eviction due later finds the file gone. Writes in progress never qualify;
the budget counts them, so the pass evicts cached blobs to make room for
them. The intents keep their rows and states, because a release and
Complete read them. A read of an evicted blob misses locally and goes to the
network tier. A file the pass cannot remove is logged and skipped.
Hourly, with or without a budget, the sweeper also deletes `.tmp-*` files
older than `local_blob_orphan_age` (24 hours), left by writes that never
finished; startup deletes every leftover one. A blob file enters the cache
only once its intent is accepted, so the cache is not checked against
intents. The scan takes no lock, so writes go on meanwhile, and a pass that
runs out of time or fails waits for the next hour. The byte count is set
when the cache opens, and every adoption and removal adjusts it under the
directory's lock, so an adoption never overlaps a removal of the same
digest. The hourly scan also corrects the count when no change overlapped
it, which catches files added or removed outside ingot; otherwise those stay
miscounted until the next quiet scan or a restart.

## Read path

A GET resolves the bucket root (registry), walks the MST to the manifest
(through the per-key version tree when the key is versioned), and serves
each covering blob from the first tier that has it: local disk (the
cache), the catalog log (catalog blocks only), then the network (`blockstore.Forge`). Network
resolution uses the **local locator**: a whole-blob hit in `blob_locations`,
or an inner-block hit in `shard_inclusions` joined to its shard's location;
the retrieval is a ranged UCAN `content/retrieve` against the provider named
by the location commitment. The indexing-service query path is implemented
but unwired. The full trace is the
[GetObject diagram](./docs/diagrams.md#getobject-version-resolution-local-tiers-network-retrieval).

**The blob list** (`bucket/blobs.go`) keeps a body's first 1,024 blobs in
the manifest as `[digest, offset]` tuples and the rest in extent blocks of
1,024, catalog blocks under `"/blobextent/0"` that the manifest points to
by `[offset, CID]`. A blob ends where the next begins. A ranged GET loads
the manifest and the extents its range covers, each once: one for any range
within an extent, which spans about 256 GiB at the default blob size. The write path builds a flat list (`BlobRefs`) and the commit's Put
(`applyPut`) cuts it into extents inside the commit's transaction, so a copy, which
writes the source's blobs into another bucket's catalog, hands the commit
the list rather than the source's extent links. One manifest holds at most
8,389,632 blobs (`bucket.MaxBodyBlobs`, cbor-gen's 8,192-entry cap on the
extent list); a Complete over it is refused with `EntityTooLarge` before
it reports a version id, and the upload stays open. A PutObject or CopyObject is at most 5 GiB and accepts
its blobs before the commit, so it is kept under the limit by configuration
instead: `max_blob_size` may not be below 1 KiB (`config.MinMaxBlobSize`). The 48.8 TiB S3 maximum fits whenever `max_blob_size`
is at least 6.11 MiB. `PartSizes` and `PartChecksums` hold 10,000 entries,
S3's part limit.

**Encrypted blobs** (the FilOne encryption design's read side) decrypt inside
the per-blob open, leaving every other read-path value plaintext: the
manifest's blob offsets, `Body.Size`, ETag and Content-Length are
plaintext coordinates, while each blob's digest names the stored FEE envelope.
A `blob_encryption_params` row marks a blob encrypted and carries what its
decryptor needs; the read unwraps the region-wrapped CEK through
`regionkey.Provider` (OpenBao transit in production, bound to the blob's
(space, digest)), maps the plaintext range to one contiguous ciphertext span
(`aesstream.CiphertextRange`), fetches only that span (ranged from local disk
or piri via `OpenBlobRange`), and decrypts it as it streams
(`aesstream.SpanReader`). A tampered chunk fails authentication mid-stream.
The encryption-params store and region key provider are required
dependencies; only the provider implementation (openbao vs inprocess) is
configuration.
The opener fetches the params rows and locations of the blobs a read covers
one extent at a time, two set-based queries per extent, and keeps only the
current extent's, so a read holds about 1,024 blobs' key material whatever
the body's size. The extent holding
the read's first byte is fetched before any response headers, so a missing
row there fails the request; a later extent of a long read is fetched when
the read reaches it, and a missing row there ends the response body.
HEAD never decrypts. See `s3frontend/decrypt.go`.

**Bucket encryption** (`s3frontend/sse.go`) is the S3 bucket-encryption API
over the same machinery. A bucket's default encryption
(`ingot.buckets.encryption`, set by PutBucketEncryption, cleared by
DeleteBucketEncryption, both through `queue-bucket.set-encryption`) is `AES256`, S3's SSE-S3 and the same envelope every
unconfigured bucket writes, or the non-standard `none`: objects stored as
received, with no envelope, no key wraps and no params row, content-addressed
by their own bytes. `none` is for tenants whose data is encrypted before it
reaches Ingot, and a deployment opts into accepting it
(`encryption.allow_none`); without that, PutBucketEncryption refuses it, and
a deployment that has never allowed it stores no plaintext. The knob is
consulted only by that request: a bucket keeps the setting it was given, so
turning the knob off later leaves existing `none` buckets storing as
received and only refuses new ones. An operator who wants such a bucket to
encrypt again sets its encryption to AES256 or deletes the configuration. The decision is per object at
write time: a `none` bucket stores as received unless the request names
`x-amz-server-side-encryption: AES256`, which encrypts that one object; a
multipart upload decides at CreateMultipartUpload and carries the choice on
its session (`multipart_sessions.plaintext`); a copy follows its destination,
re-ingesting whenever the source was stored the other way. The manifest
records the outcome (`ObjectManifest.Plaintext`), which is the read path's
only dispatch: a plaintext body is served as stored, every other body goes
through the decryptor and still fails closed on a blob without a params row.
Objects report `x-amz-server-side-encryption: AES256` when encrypted and no
header when stored as received; SSE-KMS and SSE-C are not implemented, and a
GET or HEAD carrying any encryption header is refused, as on S3. Plaintext
bodies bring content dedup back: two objects with the same bytes share a
blob, each holding its own claim, and two buckets of different spaces can
hold one digest. Every blob table is therefore keyed by `(space, digest)`,
the upload intent and the park included: each space uploads, parks, claims
and releases its own copy, and the one cached file between them goes with
the last intent for it.

**References.** Within a space, a write's use of a blob is a pending
reference (`queue/spaceshardqueue`), taken by a sync
`queue-space-shard.add` task on the blob's release subject, the space and
the digest. The subject's lease serializes the add with the blob's
`queue-space-shard.release`, so the two never overlap. It runs in one of
three modes. For an object blob (`streamed`) it concludes the stream's
allocation (`ConcludeBlobs`) and records the blob's location and upload
intent, unless the space has the blob located already, in which case the
allocation is aborted and the blob reused. For a part blob (`part`) it
records the park, unless the space has the blob parked or located already.
For a same-space CopyObject (`copy`) it sends nothing, and fails the copy
with NoSuchKey unless the space has the blob located. The add records the
blob's upload intent itself. The stream's allocation reaches the add through
the request context, never the task's stored arguments, since its put
invocation embeds derived signer keys. The add writes the reference to
`blob_pending_refs` and submits a `queue-space-shard.expire` due 24 hours
later (`pendingRefTTL`, about when the provider expires an allocation that
is never concluded), both in its own commit. An add waits at most 30 s
(`addWaitLimit`) for a release ahead of it; past that the request fails with
SlowDown for the client to retry.

The row the write records takes its references over. A PutObject's or
CopyObject's commit claims each blob with the add's task ID
(`BlobClaim.Ref`): in the root advance's transaction the pending reference is
deleted, its expiry cancelled, and the `blob_refs` claim inserted. A
reference that has expired fails that change with
`registry.ErrReferenceExpired`, and the client gets SlowDown. UploadPart
runs its blobs' adds before it writes the part row, and
`queue-multipart-upload.add-part` takes their references over in its commit
(`MultipartPart.Refs`, `registry.Postgres.PartWrites`); an add-part that
fails expires the write's references at once and runs the releases they
leave (`dropRefsNow`). Complete's claims carry no reference: the session's
parts reference the blobs until its teardown. Any other write that fails
before its commit leaves its references to expire. A
`queue-space-shard.expire` drops its reference if it still stands and, if
the space then holds no other reference to the digest, queues the blob's
release due at once: a blob nothing claimed was never readable, so no
reader grace is owed. It retries without limit, backing off from 1 s to
5 min.

A space's references to a digest are its claims, its pending references and
the parts of its open multipart sessions, counted in one
statement (`registry.CountReferences`), so a row that takes another's
reference over in one transaction is counted before or after the move, and
never missed. A release that finds any reference is stale and does nothing.
Each way a reference goes queues a release again: the drop of the last
claim, an expiry, a superseded part, a session's teardown. A release that
fails retries holding the subject (`RetryPolicy.HoldsSubject`), so an add
queued behind it waits: a failed attempt may already have removed the blob,
and the add then finds it unlocated and concludes its own stream (a space
can take a digest again once `/blob/remove` has its receipt). Every reference enters
through an add on the subject, so a release that may have acted never finds
one afterwards. A release runs only as a task: Complete queues its orphans'
releases with its own commit.

In a plaintext bucket, a write of a deleted object's bytes while that
object's release is still within its reader grace keeps the blob: the
write's add takes a reference, and the release is stale when it runs. If the
release is already due when the write arrives, the write's add runs it
first and then concludes its own stream. A same-space copy whose source is
deleted and released between its adds and its commit still commits, since
its references keep the blobs. Writes of one digest to one space take turns,
since each runs its add on the blob's release subject, so the second finds
the first's location or park; a park already recorded wins, and the loser
gives up its allocation. A release leaves the cached file while
another space still holds an intent for the digest; a file that two spaces'
releases at once both leave is reclaimed by the cache's time to live.

## Identity & auth

- **agent**: the libforge `identity.Identity` the host provides (daemon: the `identity.key_file` PEM
  key, wrapped as the `identity.service_id` did:web when set), the issuer of
  every outbound invocation to sprue, hilt, and piri. The agent's DID
  document is served at `/.well-known/did.json` on the S3 listener; peers
  resolve it to the signing key. The hilt→agent delegations and hilt's
  provider registration name the agent's DID, so changing it (did:key to
  did:web) re-issues both.
- **space**: per bucket, a `did:plc` minted by hilt at bucket create and
  stored on the bucket row; the subject of every blob and retrieve
  invocation.
- **tenant**: the `did:plc` hilt names as the caller's tenant in its
  authorize response; stored on the bucket row at create (the bucket's
  owner) and carried per request in `internal/reqscope` (the caller). The
  write path resolves the tenant's wrap key from the request's; the copy
  paths compare the two buckets'.
- **access key**: the S3 access key ID is a `did:key`. Every
  request is authorized through hilt (`/s3/request/authorize`, with a local
  fast path over the cached derived key, the key's effective S3 action set
  per bucket and its delegations); hilt re-delegates the key's grant to
  the agent, and the per-key `DelegationCache` carries those proofs into the
  request via `internal/reqscope`, where the uploader and the network read
  tier spend them. The uploader also captures a per-space ship authority
  (1h TTL) for the catalog's `queue-bucket.ship`, from every write that commits, and
  keeps an upload's capture apart for the background releases, aborts and
  registration renewals, which a narrower key's grant may not cover. Both
  take only stores the IAM layer marks borrowable. The
  same cache's action sets ride along (`reqscope.Permits`) for the one check
  hilt cannot make from the URL: a `DeleteObjects` entry naming a version
  needs `s3:DeleteObjectVersion`, beyond the request's `s3:DeleteObject`.
- There is **no root account**: versitygw's built-in root user is disabled
  (an empty `RootUserConfig`), so every access key resolves through hilt. A
  key hilt has not issued is rejected as InvalidAccessKeyId.

The [principals diagram](./docs/diagrams.md#principals-and-proof-stores)
draws the chains and the stores.

## State & durability

- **Postgres (`ingot` schema)** is the mutable index: per-bucket roots,
  versioning state and owning tenant, segment metadata and op-roots, the blob claim ledger
  (`blob_refs`), the writes' pending references (`blob_pending_refs`),
  locations and inclusions, upload intents, multipart
  sessions/parts/parks, and GC candidates. goose tracks its version at
  `ingot.goose_db_version` (never collides with a host's own migrations).
- **The MST is the data**: immutable, content-addressed, self-verifying,
  shipped to Forge as-is.
- A bucket is single-writer: every change to it (`queue-bucket.commit`,
  `.create`, `.set-versioning`, `.set-object-lock-config`, `.set-tagging`,
  `.set-cors`, `.set-encryption`, `.close`, `.admit-upload`, `.delete`,
  `.ship`) runs as a task on its `queue-bucket` subject, one at a time, and
  acts only on the bucket that still lives in that space, and
  the root's advance is guarded on the root the commit started from. The
  versioning state and the lock configuration change on the same subject, so
  set-versioning's refusal of Suspended under a lock configuration and
  set-object-lock-config's demand for Enabled cannot race.

## Known gaps

- **No HA.** A bucket's `queue-bucket` tasks are placed on the instance
  that holds its catalog log, and no other instance runs them.
- **Local blob storage is bounded only when configured** (#48): with
  `local_blob_max_bytes` set, the sweeper evicts cached blobs down to the
  budget. A delete frees its blobs' cached copies once their release runs,
  and a multipart part keeps none.
- **Crash recovery is not built**: a write that dies after its
  `queue-space-shard.add` tasks commit leaves pending references, which
  expire after 24 hours and release the blobs, but one that dies inside an
  add, after the conclude and before the add's commit, leaves an accepted
  blob with no reference. Reconciling `upload_intents` against the
  references after such a crash is a later phase; the window leaks rather
  than loses referenced data. A failed upload's intent stays `uploading`. It holds no local bytes (a
  crashed write's unfinished `.tmp-*` file goes at startup or in the
  sweeper's orphan pass), and the provider expires an allocation that is
  never concluded. The sweeper sums those intents each run, counting one
  unchanged for an hour as stalled, for the
  `ingot.local_blobs.stalled_bytes` gauge.
- **No catalog GC**: `gc_candidates` is write-only; superseded MST nodes
  accumulate on Forge with mutation volume. Each ship records how much of its
  segment was already dead (`segments.dead_bytes`), and the
  `ingot.catalog.*` gauges sum that and the shipped bytes per bucket; what
  dies after a segment ships is not measured.
- **Reads carry no bucket context**: `Manager.Get` linear-scans every open
  bucket store; threading the bucket into the read path removes the scan.
- **The ship authority expires**: `queue-bucket.ship` signs with a per-space
  proof store captured, in memory, from the bucket's last write (1h TTL).
  Every commit captures it, so a segment ships under its own writes'
  capture, but one whose ship keeps failing past the TTL, or across a
  restart, or whose grants in both captures a revocation has named, has no
  authority until the bucket's next write.
- **The indexer read path is unwired**: `LocalLocator` serves all reads; the
  network index is still published (best-effort) but never consulted.
- **Carried Forge-client copies**: `forgeclient/`, `tokenstore/`,
  `blockstore/locator/`, and `internal/ucanexec/` duplicate guppy/sprue code
  to stay cycle-free (ingot must never import guppy or sprue). A shared
  forge-client library would remove them.
