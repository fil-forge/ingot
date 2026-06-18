# S3-on-Forge: the aggregation gate

> **Thesis.** The committed Ingot S3 architecture cannot run on Forge until Piri stops aggregating
> everything into ~128 MiB cross-object pieces. S3 is a churn-heavy workload (overwrite, version
> expiry, lifecycle); **per-object deletion is the gate**, and it requires (1) rethinking Piri's
> aggregation and (2) proving small pieces work in the PDP contracts at scale. Until both land, the
> architecture is correct on paper but unbuildable in practice.

**Read this first; the depth lives in two companion docs:**
- *What we're building* — Ingot architecture: [`architecture.md`](./architecture.md)
- *Is it viable on-chain + the Piri policy* — [`rfc-pdp-minimum-piece-size.md`](./rfc-pdp-minimum-piece-size.md)
  (+ its [`pdp-cost-calculator.html`](./pdp-cost-calculator.html)).

---

## The dependency, in one line

Ingot S3 facade → requires **clean per-object delete** (S3 overwrites, version expiry, and lifecycle
all churn data) → requires **one S3 object ≈ one on-chain piece** → which Piri's current
**"size-pool everything to ~128 MiB, one node-wide proof set, whole-aggregate-only delete"** cannot
provide.

## Why the status quo breaks — a capability gap, not a tuning issue

Today Piri pools accepted blobs into ~128 MiB pieces (`MinAggregateSize`, up to a 256 MiB ceiling)
across **unrelated** objects, registers them in one node-wide proof set, and can delete only a
**whole** piece. So deleting one 8 MiB S3 object means: take down a ~128 MiB aggregate that also holds
~16 **other** objects, re-hash the survivors off-chain, re-register them under a new root, and
re-index. For a mutate/delete workload — i.e. S3 — that survivor re-hash is the **dominant** cost. No
tuning of the current target *size* fixes it: the bytes of unrelated objects are entangled in the same
on-chain piece.

## The model that fixes it: a floor / ceiling band

Two size knobs replace "size-pool everything" — the same knobs the
[architecture](./architecture.md) and [RFC](./rfc-pdp-minimum-piece-size.md) describe, just with
`MinAggregateSize` lowered:

- **`min_aggregate_size` (floor, ~8 MiB).** Any blob **≥ floor** becomes its **own** on-chain piece,
  so deleting it removes a whole piece (O(1)) with no survivors to re-hash. Any blob **< floor** joins
  a **cross-object** sub-floor aggregate built up to the floor — but **lazily**: never repacked on
  delete; dead bytes linger and compact periodically (or never). They are cheap bytes.
- **`max_blob_size` (ceiling, target several GiB).** The largest blob Piri stores; an object larger
  than the ceiling is **split** by the data layer into a handful of ≤ ceiling blobs — each, being
  ≥ floor, its own piece. (A multipart object is likewise stored as its part-blobs, each ≥ floor and
  thus its own piece — N standalone pieces, not one.)

The pivot is **lowering the floor to ≈ object size.** Most S3 objects land **≥ floor**, so an object
becomes one (or a few) **standalone** piece(s) — deleting it removes whole pieces with no survivors,
no re-hash, no re-index. The only aggregation that survives is the **cross-object sub-floor tail**, and
lazy compaction keeps even that off the churn path. Deletion goes from "the dominant cost" to "O(1)
for the bulk, deferred for the tail." Note this is *not* a new "group an object's parts together"
primitive — Piri's aggregator is already size-driven; the change is the floor value plus a lazy
sub-floor compaction path.

## The two asks

**1 — Rethink Piri's aggregation strategy.** Lower `MinAggregateSize` from 128 MiB to a configurable
~8 MiB floor (per-SP / per-dataset), keeping aggregation adaptive: blobs ≥ floor become their own
piece, only the sub-floor tail pools — and **lazily** (never repacked on delete). The RFC §7 carries
the spec. This is mostly a constant change plus a lazy-compaction path — Piri's aggregator is already
size-driven — not a new grouping primitive.

**2 — Prove small pieces are viable in the PDP contracts, at scale.** The RFC's *measured* gas already
shows the **economics** hold (registration linear, proving logarithmic, proof-fee size-neutral,
**no slashing**, registration gas-gatable). The remaining proof is **operational** — that Piri can
*register, prove, and delete* sub-256-MiB pieces routinely at PiB scale:
- **throughput** — ~11M `addPieces` txns per PiB at 8 MiB (the deployed verifier's ~13-piece/tx cap,
  not the 61 a naïve reading of the WSS limit suggests) ⇒ a sender fleet (the binding constraint);
- **the delete path is real end-to-end** — the contract accepts pieces down to 32 B, but Piri's
  whole-root delete (`schedulePieceDeletions`) is **wired but unsigned today** — it needs its
  extraData signature finished and exercised;
- **proving stays bounded under churn** — the monotonic `nextPieceId` ratchet must not drift proving
  cost or the chain-reconciliation view-getters into trouble at high churn.

De-risk with a scaled proof-of-capability **before** committing the S3 roadmap to it.

## Why this is *the* gate (the honest framing)

On-chain dollar cost is negligible at **every** piece size — so this is **not** a cost optimization,
it is a **capability gap**. The two real risks are exactly the two asks: (a) the off-chain repack on
churn — which **one-object-one-piece eliminates** (no survivors to re-hash); and (b) throughput — more
pieces means a sender fleet. "Piece = object" is simultaneously the cost-optimal *and* the only
delete-tractable model. (The numbers are in the RFC's §3 and the cost calculator; the "~99% margin"
there is on-chain margin only — it confirms piece size barely moves dollars, which is precisely why the
decision rides on *capability and throughput*, not cost.)

## What we still need to know (to set floor/ceiling and size the fleet)

Object-size distribution **by count and by bytes** (real S3 is heavy-tailed: most objects tiny, most
bytes in large objects), object lifespan by size class, multipart fraction and the actual part sizes
clients use, and the delete/overwrite/version/lifecycle mix. Pull initial shapes from object-store
workload literature (IBM COS / SNIA traces); **instrument Ingot from day one** so the floor/ceiling
become measured, not guessed.

---

### Sequencing (suggested)

1. **De-risk Ask 2 first** (a scaled small-piece register→prove→delete proof-of-capability + finish
   the `schedulePieceDeletions` signature). It is the cheapest way to find out if the whole approach
   is dead on arrival.
2. **Spec + build Ask 1** (configurable ~8 MiB floor + ceiling + lazy sub-floor compaction) in Piri,
   per a refined RFC §7.
3. **Instrument** object-size/lifespan/multipart/churn in Ingot to tune the floor/ceiling and size
   the sender fleet.
4. Only then is the Ingot S3 architecture executable as written.
