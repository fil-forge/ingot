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

## The model that fixes it: one object = one piece (no cross-object aggregation)

The proposal is to **stop pooling unrelated objects into shared pieces** and store each S3 object as
its own content-addressed blob(s): one on-chain piece if it fits under a ceiling, a handful of
*object-owned* pieces if it's larger (split at the proving-code ceiling — Piri's `MaxMemtreeSize`,
256 MiB today and liftable; per-part for multipart, which keeps us compatible with the encryption RFC's
per-part envelopes). **A piece belongs to exactly one object.** Then:

- **Deletion is O(1) per object, always** — retire the object's own piece(s); no survivors to re-hash,
  no compaction, no SLA window to chase. The entire "compact the cross-object tail" problem disappears.
- The only aggregation left is an **optional, generational** relief valve: a background process that
  combines *old, cold* pieces (unlikely to be deleted) to keep the proving-root count down — it rarely
  triggers a repack precisely because it only touches the cold set.

**The catch — and the real reason this is a gate.** Object=piece *maximizes* piece count, so it only
works if the PDP contracts and proving can take that volume in practice; we have never actually asked
FilOZ. **If the contracts can take it**, we drop cross-object aggregation and this is dramatically
simpler. **If they can't**, we fall back to a **size floor** (`min_aggregate_size`, e.g. 8 MiB): blobs
≥ floor are their own piece, only the sub-floor tail pools — the model the
[architecture](./architecture.md) and [RFC §7](./rfc-pdp-minimum-piece-size.md) document as the
fallback. So the floor stays specified, but as the fallback, not the default. (This is not a new
"group an object's parts" primitive either — Piri's aggregator is already size-driven; object=piece
mostly means *not* aggregating across objects.)

## The two asks

**1 — Rethink Piri's aggregation strategy.** The target is **no cross-object aggregation** (object =
piece), with a generational background aggregator as the optional relief valve for root-count/throughput,
and the size floor (`MinAggregateSize` lowered to a configurable ~8 MiB) as the **fallback** if the
contracts can't take the piece volume. RFC §7 carries both. Piri's aggregator is already size-driven,
so the floor-fallback is a small change; the object=piece target mostly means *not* aggregating across
objects.

**2 — Prove small pieces are viable in the PDP contracts, at scale.** The RFC's *measured* gas already
shows the **economics** hold (registration linear, proving logarithmic, proof-fee size-neutral,
**no slashing**, registration gas-gatable). The remaining proof is **operational** — that Piri can
*register, prove, and delete* sub-256-MiB pieces routinely at PiB scale:
- **throughput** — 1 PiB @ 8 MiB = 134.2M pieces, and the `addPieces` batch is no longer contract-capped
  (FWSS v1.3.0 removed the `extraData` cap), so the txn count — and the sender fleet it implies — is bound
  by the FVM `PiecesAdded` event-size + per-tx gas and is an open measurement (the binding constraint);
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
2. **Spec + build Ask 1** — object=piece (no cross-object aggregation) as the target, generational
   aggregation as the relief valve, the size floor + ceiling as the fallback — in Piri, per a refined
   RFC §7.
3. **Instrument** object-size/lifespan/multipart/churn in Ingot to tune the floor/ceiling and size
   the sender fleet.
4. Only then is the Ingot S3 architecture executable as written.
