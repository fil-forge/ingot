# RFC: Minimum On-Chain Piece Size for PDP (match deletion granularity; 8 MiB default)

| | |
|---|---|
| **Status** | Draft / for discussion |
| **Author** | forrest |
| **Date** | 2026-06-17 |
| **Scope** | Piri aggregation policy + PDP cost/economics model. **No smart-contract changes required.** (Verified against FWSS v1.3.0 / PDPVerifier v3.4.0; the earlier "raise the extraData cap" lever is moot — that cap was removed upstream.) |
| **Affected components** | Piri (`pkg/pdp/aggregation/*`, `pkg/pdp/service/*`, `pkg/pdp/tasks/*`), SP gas budget + sender provisioning |
| **Contracts referenced** | `PDPVerifier.sol`, `FilecoinWarmStorageService.sol`, `FilecoinPayV1.sol` |
| **Companion tool** | `pdp-cost-calculator.html` — interactive model of all costs/revenue below |

---

## Decision summary

> **Read this; §1–§10 are the supporting analysis and §3.1–§3.4 + the Appendices are the gas audit (skim for conclusions).**

> **Direction note (updated).** Since this RFC was written, the preferred direction shifted to
> **no cross-object aggregation** — store each object as its own piece(s) (see the *"Should Piri do
> cross-object aggregation at all?"* note and `aggregation-gate.md`). This RFC now serves two purposes:
> it documents the **size-floor fallback** (used if FilOZ confirms the contracts *can't* take the
> object=piece volume), and it carries the **gas/economics analysis that applies either way** (the
> floor is just object=piece with a minimum batching size for the sub-floor tail).

**The ask.** Prefer **no cross-object aggregation** (one object = its own piece(s)); pending FilOZ
confirmation that the contracts can take that piece volume. The documented **fallback** is to lower
Piri's `MinAggregateSize` from `128<<20` to a **configurable ~8 MiB floor** (`aggregator/jobqueue.go:190`),
keeping aggregation adaptive: any blob ≥ floor becomes its **own** on-chain piece (O(1) delete); only
sub-floor blobs pool up to the floor. **No smart-contract change is required** for either.

**Why 8 MiB (for the floor fallback).** The cost-optimal piece size ≈ the deletion/object granularity,
and the target S3 objects are ~8 MiB (AWS's default multipart threshold). Smaller wastes registration +
churn gas for no delete benefit; larger re-introduces the off-chain repack on every partial delete.

**The numbers that matter** (1 PiB @ 8 MiB; measured Foundry gas + calibnet anchors):

| Quantity | Value |
|---|---|
| Add a piece | **~120,700 EVM gas, size-independent** (5-gas spread across 32 B–256 MiB) — needs re-measure on v3.4.0 |
| Delete | standalone **~88,800 gas, O(1)**; from an aggregate `gDel + gAdd` **+ off-chain re-hash** — needs re-measure |
| Batch cap | **None at the contract level** (v1.3.0 removed `EXTRA_DATA_MAX_SIZE` / `MAX_ADD_PIECES_EXTRA_DATA_SIZE`); batch size is bound by the FVM `PiecesAdded` event-size + gas, so **pieces/tx is an open measurement** (134.2M pieces/PiB @ 8 MiB is fixed; the txn count is not) |
| Fleet | Sender-fleet sizing scales with the (now uncapped, measurement-bound) pieces/tx — TBD; throughput, not gas, is the wall |
| Margin | on-chain *gas* cost is a rounding error vs storage revenue (**~99%** at ~4 USDFC/TiB/mo) |

**The crux facts** (non-obvious and load-bearing):

1. **There is no slashing.** No proof-slashing exists in any contract; a missed proof merely forgoes
   that period's pay, nothing is taken from the SP. This makes registration and deletes freely
   **deferrable to cheap-gas windows** (Piri already gas-gates them) — which is what rescues small pieces
   on cost. *(One non-slash capital item in v1.3.0: a **0.1 FIL refundable cleanup deposit** posted per
   dataset at create, reclaimed by the SP on its own teardown — a one-time lock, not a fault penalty.)*
2. **The real case for small pieces is deletion, not gas dollars.** On-chain *dollar* cost always
   favors *bigger* pieces; small pieces win by avoiding the off-chain aggregate re-hash and the
   `gDel + gAdd` pure-delete asymmetry.
3. **Cost-optimal piece ≈ object size.** Below it you pay extra registration *and* extra churn; above
   it you re-introduce the repack.
4. **Throughput, not gas, is the wall.** Profit is gated by base fee × storage price, not piece size;
   the binding constraint is `addPieces` transaction volume → sender fleet.

---

## 1. Summary

Today Piri aggregates uploaded blobs into **128–256 MiB pieces** before registering their Merkle root on-chain (`MinAggregateSize = 128<<20`, `MaxMemtreeSize = 256<<20`). Aggregation makes **partial deletion expensive**: removing one blob from an aggregate requires removing the on-chain root, **re-hashing the entire surviving aggregate off-chain**, re-adding it, and re-indexing the new aggregate CID.

This RFC proposes **setting the minimum on-chain piece size equal to the typical deletion/object granularity** — and, for the current Ingot/S3-facade workload where that granularity is ~8 MiB, lowering the floor to **8 MiB** with **adaptive** aggregation: any blob ≥ the floor becomes its own on-chain piece (O(1) deletion); only sub-floor blobs aggregate up to the floor.

The proposal is backed by **measured gas** (Foundry, on the actual contracts), an **adversarial code audit** of every piece-iterating loop in both contracts + `SimplePDPService` + the Piri integration, and a verified study of Piri's gas-gating and the contracts' payment/penalty mechanics. The findings reshaped the recommendation from the earlier draft:

> 1. **On-chain *dollar* cost always favors larger pieces** (registration is linear in piece count, proving is logarithmic, proof fee is neutral). The case for *small* pieces is **not** an on-chain-gas argument.
> 2. **The real case for small pieces is (a) off-chain** — aggregates must be re-hashed on every partial delete — **and (b) the pure-delete asymmetry**: deleting a standalone piece costs only `gDel`, whereas deleting from an aggregate costs `gDel + gAdd` (you must re-add the survivors under a new root).
> 3. **The cost-optimal piece size ≈ the deletion/object granularity.** Below it you pay extra registration *and* extra churn gas; above it you re-introduce the off-chain repack. **8 MiB is the right default precisely because the target objects are ~8 MiB** — it is not a magic constant.
> 4. **At quiet base fee, piece size barely affects profit.** Profitability is gated by **base fee × storage price**, not piece size. The binding operational constraint of small pieces is **transaction throughput**, not gas dollars.

No contract change is required. The contract accepts pieces down to 32 B; Piri's floor is 128 B. 8 MiB is purely a Piri policy value.

---

## 2. Background: the data model and the deletion penalty

**What the chain sees.** The on-chain "piece" is a single CommPv2 CID. When Piri aggregates, the on-chain piece is the **aggregate root only**; the constituent blobs ("subroots") exist purely off-chain — in Piri's proof tree and DB rows (`pdp_proofset_roots`, `pdp_proofset_root_adds`). `PDPVerifier.addPieces` → `addOnePiece` stores one entry per submitted CID (`pieceCids[setId][pieceId] = piece`) and never sees subroots (`roots_add.go` packs only `addRootReq.Root.Bytes()` into the `addPieces` calldata).

**Why aggregation exists.** Each registered piece costs a fixed amount of gas to add (≈120,700 EVM gas, §3.1), independent of size. Registering millions of tiny blobs individually multiplies that fixed cost. Aggregation amortizes it over many bytes.

**The penalty aggregation creates.** Deletion. Piri's `RemoveRoot` (`root_remove.go`) schedules deletion of a *whole root* — there is no partial-aggregate path. To delete one blob inside a 256 MiB aggregate you must:
1. `schedulePieceDeletions` the aggregate root,
2. re-run `NewAggregate` over the surviving blobs (re-hash ≤256 MiB — `NewAggregate` is the only caller of the Merkle build, on the *add* path only),
3. `addPieces` the new aggregate root (re-paying `gAdd`),
4. re-index the new aggregate CID everywhere it is referenced.

For an S3-facade workload (Ingot) where objects are updated and deleted routinely, this is the dominant pain point.

---

## 3. The validated cost model

EVM-gas figures are **measured** with Foundry against the actual `PDPVerifier` (isolated, zero-address listener). Filecoin-gas anchors are from the calibnet benchmarks in `lib/pdp/docs/gas-benchmarks/`.

### 3.1 Adding a piece costs the same regardless of size

| Piece size | EVM gas to add 1 piece |
|---|---|
| 32 B (1 leaf) | 168,748 |
| 1 MiB | 168,751 |
| 256 MiB | 168,753 |

A **5-gas** difference across an 8-million-fold size range. Marginal cost of an extra piece in a batch (1000-vs-10 differencing): **~120,700 gas**, plus ~48k fixed per `addPieces` call. The mechanism: the CommPv2 CID is 39–46 B across the legal size range and always lands in the **same 3-slot Solidity `bytes` bucket** (1 length + 2 data words), so `pieceCids[...] = piece` is always 3 cold SSTOREs; the ~5 gas is extra `_readUvarint` iterations. So **piece count `N = dataset ÷ piece size`, not piece size, is the master cost variable.**

### 3.2 On-chain *dollar* cost always favors larger pieces

Every on-chain cost term is minimized (or unaffected) by larger pieces:

| Term | Scaling in piece size | Direction |
|---|---|---|
| Registration (add) | **linear** in piece count ∝ 1/size | favors **big** |
| Proving gas | **logarithmic** in piece count (§3.4) | favors **big** (weakly) |
| Proof fee | ∝ dataset bytes, **independent** of piece size | neutral |
| Churn on-chain gas (replace) | flat for piece ≤/≥ object | neutral/favors big |
| nextProvingPeriod | constant | neutral |

So a pure on-chain-cost optimization says "keep 256 MiB or go bigger," at every churn level. **The earlier draft's implication that small pieces save on-chain money was wrong.** The case for small pieces is off-chain + the pure-delete asymmetry (§3.5).

### 3.3 No on-chain hot path is O(live pieces)

Audited every loop in `PDPVerifier.sol`, `FilecoinWarmStorageService.sol`, `SimplePDPService.sol`, the interfaces, and the Piri call sites. Every state-changing path is bounded:

| Path | Bound |
|---|---|
| `addPieces` / `addOnePiece` | **O(batch added)** |
| `provePossession` | **O(5)** = `CHALLENGES_PER_PROOF` |
| `findOnePieceId`, `sumTreeAdd/Remove` | **O(log cumulative pieces)** (`top = 256 − clz(nextPieceId)`) |
| `nextProvingPeriod` removals / `schedulePieceDeletions` | **O(removals this period ≤ 2000)** |
| `deleteDataSet` | **O(1)** to enter cleanup mode; teardown is paginated **O(total pieces)** via `cleanupPieces(setId, maxPieces)` (bounded loop, incentivized by the 0.1 FIL cleanup deposit) |
| Listener `piecesAdded` | **O(batch × metadata keys ≤ 3)** (`MAX_KEYS_PER_PIECE = 3`, `FilecoinWarmStorageService.sol:220`) |
| Listener `validatePayment` / `_findProvenEpochs` | **O(proving periods settled)**, not pieces |

The only O(cumulative-piece) loops in the surface are two **`view` getters** — `getActivePieceCount` and `getActivePieces` (scan `[0, nextPieceId)` incl. deleted slots), off-chain `eth_call` only (no block gas). See §7 (Piri's repair path calls them).

### 3.4 Proving cost rises *slightly* with smaller pieces

Per proof of 5 challenges, each challenge does a **sumtree descent** of `log2(cumulative pieces)` **cold SLOADs** (~2,100 gas each) plus a **Merkle path** of `pieceHeight` **SHA-256** hashes (~84 gas each). Going 256 MiB → 8 MiB at fixed dataset bytes: the Merkle path shrinks ~5 hops but piece count rises 32×, deepening the sumtree ~5 levels — and a cold SLOAD ≫ a SHA-256 precompile, so **proving gets modestly more expensive (~+50k EVM gas/proof, logarithmic)**, not flat/cheaper as an earlier draft claimed. Matches calibnet: `ProvePossession` rose ~120M → ~177M Filecoin gas from 39 → 10,000 pieces; `NextProvingPeriod` stayed flat ~54M; proof fee depends only on bytes. **Caveat:** depth is keyed on `nextPieceId`, which is **monotonic** for a live dataset (incremented at `PDPVerifier.sol:800` in `addOnePiece`; reduced only during `cleanupPieces` dataset teardown) — churn ratchets proving cost up permanently.

### 3.5 The actual case for small pieces: deletion (off-chain + pure-delete)

| | Standalone 8 MiB piece | One blob in a 256 MiB aggregate |
|---|---|---|
| Pure delete (on-chain) | `gDel` ≈ **88,800 gas**, O(log) | `gDel + gAdd` (remove + re-add survivors) |
| Replace (on-chain) | `gDel + gAdd` | `gDel + gAdd` |
| Off-chain | **none** | **re-hash ≤256 MiB** + re-index |

Two distinct wins for small pieces, both verified against the contracts + Piri:
1. **Off-chain repack avoidance** — `NewAggregate` (the Merkle re-hash) runs only on the add path; `RemoveRoot` schedules deletion with no re-aggregation. A standalone piece deletes with zero re-hash.
2. **Pure-delete asymmetry** — deleting one blob from an aggregate forces re-adding the survivors under a new root (`gDel + gAdd`); a standalone piece is just `gDel`. (For *replace*/overwrite, both cost `gDel + gAdd`, so only the off-chain re-hash differs.)

This is why the cost-optimal piece sits at **≈ the object/deletion granularity**: small enough to avoid the repack, large enough to minimize the registration tax.

---

## 4. SP gas optimization (verified vs Piri + contracts)

The SP is not a passive gas payer. Two levers materially change the economics, and a third fact removes a feared penalty.

**No slashing (verified, v1.3.0).** `FilecoinWarmStorageService.sol`, `FilecoinPayV1.sol`, and `PDPVerifier.sol` contain **no proof-slashing**: `FaultRecord` (decl `FilecoinWarmStorageService.sol:92`, emit `:978`) is an event consumed nowhere, and `validatePayment` pays `proposedAmount × provenEpochs / totalEpochs` (`:1512`); faulted periods pay 0. **Missing a proof = the payer keeps that period's money; nothing is taken from the SP.** *(Wording note: v1.3.0 does add a **0.1 FIL refundable cleanup deposit** per dataset (`lib/pdp/src/Fees.sol`, posted at `createDataSet`, reclaimed on the SP's own teardown) and uses `forfeit` in abandonment paths — so "zero bond logic" is no longer literally true; the accurate claim is "no proof-slashing," and the deposit is refundable capital, not a fault penalty.)*

**Lever 1 — gate registration to cheap gas.** Registration (`addPieces`) and deletes are **deferrable** (no on-chain deadline). Piri already implements this: a per-message **max-fee cap in wei** (`pdp.gas.max_fee.add_roots` etc.; `sender_eth.go`), defer-and-retry every `retry_wait` (default 5 min), **no penalty, no nonce consumed**. So registration can be priced at `min(prevailing, gate) + premium` — near the floor regardless of how congested the network is for proving. **This is what rescues small pieces on cost:** registration is the deferrable, piece-count-heavy cost, so gating it to the floor neutralizes the base-fee-spike risk. (Defaults are 0 = no gating; operators must set caps.)

**Lever 2 — skip unprofitable proving.** Because there is no slashing, on a base-fee-spike period an SP can simply not prove, forgoing only that period's pay. Proving must land in its challenge window (`[deadline − challengeWindowSize, deadline]`), so it pays the **prevailing** fee, not a gated one — but it is economically optional per period. (Note: Piri does not yet *auto*-skip; setting a low `prove` cap achieves it crudely by deferring past the window. And `nextProvingPeriod` must still be called to keep the cycle alive — consecutive skips amortize to one call.)

**Modeling consequence — two effective gas prices:** deferrable ops at the gate (≈ floor), proving at the prevailing base fee + premium (Piri sets `GasFeeCap = baseFee + SuggestGasTipCap`).

---

## 5. The real wall: transaction throughput, not gas dollars

Because per-piece gas is fixed, smaller pieces mean **more transactions** — and that, not gas dollars, is the binding constraint. The *magnitude* of that wall, however, is now an open measurement (see the batch-cap note).

- **Batch cap — there isn't one at the contract level (changed).** PDPVerifier v3.4.0 removed `EXTRA_DATA_MAX_SIZE` and FWSS v1.3.0 removed `MAX_ADD_PIECES_EXTRA_DATA_SIZE`; `addPieces` does no extraData length check, and the FWSS listener notes (`FilecoinWarmStorageService.sol:760`) that *"PDPVerifier currently hits the FVM PiecesAdded event size limit before FWSS needs a byte cap."* So the binding limit is the **FVM `PiecesAdded` event-size limit + per-tx gas** — a protocol constraint, not a tunable constant — and the **maximum pieces/tx is an open empirical question** (needs an FVM/Foundry measurement against the current per-piece extraData ABI), not a fixed number. *(An earlier draft modeled a 2048-byte verifier cap ⇒ ~13 pieces/tx; that constant no longer exists, so those figures and everything derived from them are withdrawn pending re-measurement.)*
- **Registration tx count:** 1 PiB at 8 MiB = **134.2M pieces** (pure arithmetic, unchanged). The `addPieces` transaction count = pieces ÷ pieces-per-tx, and since pieces-per-tx is now event-size/gas-bound rather than capped, the txn count is **pending measurement**, not a settled figure.
- **Submission capacity:** ~1 tx per 30 s block ≈ **120 tx/hr per sender address** (~1.05M txns/yr); parallelize with multiple senders to multiply it.
- **Onboarding & churn:** the sender-fleet sizing for a given onboard window — and for steady-state churn (10%/mo on a PiB ≈ **161M events/yr**, re-add-dominated) — both scale with pieces-per-tx. With the cap removed and that count unmeasured, we can't yet size the fleet; what's robust is the *shape* — throughput, not gas dollars, is the wall, and smaller pieces push it harder.

**Onboarding is a revenue ramp.** Onboarding time = how fast data *arrives* (demand), independent of the node. Revenue accrues on the **growing registered base**: `actual onboarding = max(arrival time, capacity-limited time)`. If throughput can't keep up, onboarding stretches and the delayed data forgoes revenue — captured directly as a smaller revenue ramp (see the calculator's time chart), not a bolted-on penalty.

---

## 6. Profitability

With capex/power/hardware excluded (marginal on-chain margin):

- **Revenue is identical for every piece size** (it depends only on dataset size × storage price × time). So the profit-optimal piece = the cost-optimal piece = ≈ object size.
- **Profitability is gated by base fee × storage price, not piece size.** At near-floor base fee, registering a billion pieces costs single-digit dollars; piece size changes profit by *cents* on tens of thousands. At elevated base fee, *ungated* registration of small pieces can exceed revenue — but Lever 1 (gating) neutralizes that.
- **Worked target** (1 PiB, 8 MiB, base fee 2500, storage ~4 USDFC/TiB/mo): the on-chain *gas* margin is ~99% at any plausible pieces/tx — gas cost is a rounding error against storage revenue, and registration is gas-gatable. What's no longer quantifiable until pieces/tx is measured is the *throughput*-bound onboarding time and the sender count: a small fleet onboards comfortably and a single sender is throughput-bound, but the exact figures wait on the FVM event-size/gas measurement (the earlier 2048-cap math is withdrawn).

The contract's storage price is **2.5 USDFC/TiB/mo** — now an **immutable** constant `STORAGE_PRICE_PER_TIB_PER_MONTH` (`lib/PriceListUSDFC.sol:19`, `(5 × 10^18)/2`); the owner-mutable pricing API was removed, so changes ship via contract upgrade (read it via `FilecoinWarmStorageServiceStateView.getPriceList()`). Realistic targets are 2–10. Note v1.3.0 also adds a flat **0.024 USDFC/mo per-dataset** fee (Appendix C), so revenue is no longer purely size × price × time. Use the calculator to set your point.

---

## 7. Proposal

1. **Target: no cross-object aggregation** (one object = its own piece(s)), pending FilOZ confirmation the contracts can take the piece volume; add a generational background aggregator as the optional relief valve for root-count. **Fallback: a size floor** — set the minimum piece size to the typical deletion/object granularity, default 8 MiB. Change `MinAggregateSize` (`piri/pkg/pdp/aggregation/aggregator/jobqueue.go:190`) from `128<<20` to the floor, and fix the comment block (`:185–190`) that hard-assumes 128/256 MiB.
2. **Make aggregation adaptive** (mostly already true): blobs ≥ floor → own piece (O(1) deletion); sub-floor blobs aggregate up to the floor. `jobqueue.go:205` (`AggregatePiece`) already submits a piece standalone when its padded size **exceeds** the floor — note the test is a strict `>`, so a blob *exactly* equal to the floor still aggregates; word the policy accordingly.
3. **Keep per-piece metadata empty** (`roots_add.go:421–424`) to preserve the `extraData`/batch budget.
4. **Bound `BatchSize` by the FVM event-size + per-tx gas, and measure it.** There is no longer a contract `extraData` cap (PDPVerifier removed `EXTRA_DATA_MAX_SIZE`, FWSS removed `MAX_ADD_PIECES_EXTRA_DATA_SIZE`), so `addPieces` won't revert on batch size — the limit is the FVM `PiecesAdded` event-size + gas. The current default `BatchSize = 10` is safely within that; pick the production value from a measured per-tx ceiling rather than a constant.
5. **Provision sender parallelism for throughput** — size the number of sender addresses to cover onboarding **and** steady-state churn. This is the binding constraint, not gas dollars; the exact count scales with the (now measurement-bound) pieces-per-tx, so size it once that's measured.
6. **Configure registration gas-gating** (`pdp.gas.max_fee.add_roots`) so registration/deletes ride cheap-gas windows; leave proving ungated (or capped high enough to always land in-window).

**No contract change required.**

---

## 8. Caveats, risks, and mitigations

1. **Throughput is the primary constraint.** Registration is one `addPieces` tx per batch; with the contract `extraData` cap removed (v1.3.0), the batch size — and thus the txn count for 1 PiB @ 8 MiB — is bound by the FVM event-size + gas and needs measurement. Gate-induced low duty-cycle further limits the achievable rate. **Mitigation:** sender parallelism (§7 item 5); model it in the calculator once pieces/tx is measured.
2. **Churn throughput** (often missed): high churn on small pieces generates large *ongoing* tx volume (re-add-dominated; ≈161M events/yr at 10% churn on a PiB) that can exceed several senders. **Mitigation:** size senders for churn too; batch deletes (up to `MAX_ENQUEUED_REMOVALS = 2000`/period).
3. **Monotonic `nextPieceId` ratchet.** Sumtree depth and view-getter cost grow with cumulative adds and never shrink on deletion; high churn drifts proving up logarithmically. **Mitigation:** periodically rotate to a fresh dataset for extreme-churn sets.
4. **O(cumulative) view getters** (`getActivePieceCount`/`getActivePieces`): Piri's repair path (`proofset_repair.go`) walks these via `eth_call` and gets ~32× slower. **Mitigation:** use Piri's local DB as the enumeration source of truth.
5. **Proving cost up slightly** (§3.4), ~+50k EVM gas/proof vs 256 MiB; small but real and persistent.
6. **Base-fee spikes** raise *proving* cost (un-gateable). **Mitigation:** Lever 2 (skip unprofitable periods, no slashing). Registration is shielded by Lever 1.
7. **Piri-side state growth:** ~32× more DB rows; ensure indexing/retention scales.

None of these is a hidden per-live-piece loop; all are bounded (O(batch), O(removals ≤ 2000), O(log cumulative), throughput, or off-chain).

---

## 9. Alternatives considered

| Option | Verdict |
|---|---|
| **Status quo (128–256 MiB)** | Cheapest registration & fewest txns, but the partial-delete penalty (re-hash + re-add + re-index) is exactly what we want to eliminate. Best only for write-once/rarely-deleted data. |
| **Match piece = object size (this RFC)** | Profit-optimal under churn: no repack, minimal registration. 8 MiB for ~8 MiB objects. |
| **Smaller than object (1–4 MiB for 8 MiB objects)** | Strictly worse: more registration *and* more churn gas, more txns, for no deletion benefit. |
| **Larger than object (16–256 MiB)** | Cheaper registration/fewer txns, but re-introduces off-chain repack on every sub-piece delete. A fallback if throughput is the dominant pain and deletes are rare. |
| **The batch cap is already gone (v1.3.0)** | The contract `extraData` cap that throttled batch size (`EXTRA_DATA_MAX_SIZE` / `MAX_ADD_PIECES_EXTRA_DATA_SIZE`) was removed in PDPVerifier v3.4.0 / FWSS v1.3.0, so "many pieces/tx" is no longer contract-throttled. The remaining throughput ceiling is the FVM `PiecesAdded` event-size + per-tx gas — a protocol-level limit, not a service-contract lever to pull. |

---

## 10. Open questions

1. **Blob/object-size distribution through Ingot** — sets the deletion granularity and thus the floor; decides how often the adaptive path aggregates vs. emits standalone pieces.
2. **Sender-parallelism budget** — how many sender addresses ops will run; this, not gas, decides PiB-scale feasibility.
3. **Churn rate and delete-vs-replace mix** — drives ongoing throughput and the off-chain repack exposure.
4. **Should the floor be per-SP / per-dataset configurable** rather than one global constant?
5. **What is the measured pieces-per-tx** under the FVM `PiecesAdded` event-size + gas limit (now that the contract `extraData` cap is gone)? This sets the real registration-throughput ceiling and the sender-fleet sizing.

---

## Evidence (trust-but-verify — skip unless auditing)

The appendices below are the raw gas measurements, calibnet anchors, scaling laws, and contract
constants behind the model. They corroborate the **Decision summary** and §3–§6; a reviewer aligning
on the proposal does not need to read them.

## Appendix A: Measured gas (Foundry, `PDPVerifier`, zero-address listener)

- Add 1 piece (any size 32 B–256 MiB): **168,748–168,753 gas** (5-gas spread).
- Marginal add per piece (batch differencing): **~120,700 gas**; ~48k fixed per call.
- Delete 1 piece: `schedulePieceDeletions` 68,809 + `nextProvingPeriod` flush 19,987 = **~88,796 gas**.

## Appendix B: Calibnet anchors (`lib/pdp/docs/gas-benchmarks/`)

- `AddPieces` 39 pieces (one tx): 44.25M Filecoin gas ⇒ ~1M Filecoin gas/piece all-in (FEVM ≈ ~8× EVM).
- `ProvePossession`: ~120M (39 pieces) → ~177M (10,000 pieces) — the O(log cumulative) sumtree term.
- `NextProvingPeriod`: flat ~54M regardless of piece count.

## Appendix C: Cost-model scaling laws

| Quantity | Scales as | Frequency |
|---|---|---|
| Registration gas, tx count | **linear** in piece count (∝ 1/size) | one-time |
| Proving gas | **logarithmic** in cumulative piece count | per period |
| Proof fee (burned FIL) | **linear** in dataset bytes | per period |
| nextProvingPeriod gas | **constant** | per period |
| Deletion gas | linear in pieces deleted (≤2000/period) | on delete |
| Off-chain repack | ∝ aggregate size × delete rate (0 if piece ≤ object) | on delete |
| Revenue | ∝ dataset size × price × time (ramps during onboarding) | continuous |

`FIL = Filecoin-gas × baseFee ÷ 1e18`; `Filecoin-gas ≈ EVM-gas × ~8` (FEVM) — a directional approximation bracketed by ~6.7× (vs full-add gas) and ~9.4× (vs marginal-add gas), not a measured constant. Proving constants are calibnet-fit (directional). See `pdp-cost-calculator.html` to vary all parameters.

## Appendix D: Key constants

| Constant | Value | Location |
|---|---|---|
| `CHALLENGES_PER_PROOF` | 5 | `FilecoinWarmStorageService.sol:42` |
| `MAX_ENQUEUED_REMOVALS` | 2000 | `PDPVerifier.sol:48` |
| `MAX_PIECE_SIZE_LOG2` | 50 | `PDPVerifier.sol:47` |
| `MAX_KEYS_PER_PIECE` / per-dataset key cap | 3 / 10 | `FilecoinWarmStorageService.sol:220` / `:219` |
| addPieces batch cap | **removed in v1.3.0** (`EXTRA_DATA_MAX_SIZE` and `MAX_ADD_PIECES_EXTRA_DATA_SIZE` deleted); batch bound by FVM `PiecesAdded` event-size + gas | — |
| Surviving extraData caps (other ops) | `MAX_CREATE_DATA_SET_EXTRA_DATA_SIZE` 4096 / `MAX_SCHEDULE_PIECE_REMOVALS_EXTRA_DATA_SIZE` 256 / `MAX_TERMINATE_SERVICE_EXTRA_DATA_SIZE` 256 | `FilecoinWarmStorageService.sol:51,:57,:63` |
| `maxProvingPeriod` / `challengeWindowSize` (per network, init args not constants) | 2880/60 (mainnet), 240/20 (calibnet), 120/10 (devnet) | `tools/warm-storage-deploy-all.sh:56–75`; init `:404–405` |
| `STORAGE_PRICE_PER_TIB_PER_MONTH` (immutable; upgrade-only) | 2.5 USDFC (`(5×10^18)/2`; realistic 2–10) | `lib/PriceListUSDFC.sol:19` |
| USDFC fee layer (v1.3.0, new) | per-dataset 0.024/mo; create 0.025; add-pieces 0.0005 + 0.0003/piece; remove 0.002; terminate 0.00112; lifecycle reserve 0.10 | `lib/PriceListUSDFC.sol` |
| Cleanup deposit (refundable, FIL) | 0.1 FIL per dataset at `createDataSet` | `lib/pdp/src/Fees.sol` |
| `MinAggregateSize` (Piri, **to change → floor, e.g. 8<<20**) | 128 MiB | `aggregator/jobqueue.go:190` |
| `MaxMemtreeSize` (Piri, upload max) | 256 MiB | `proof/proof.go:114` |
| Piri input floor (`PaddedSize`) | 128 B | `aggregate.go:49` |
| Piri gas-gate keys (default 0 = off) | `pdp.gas.max_fee.{prove,proving_period,add_roots,default}` | `sender_eth.go`, `config/defaults.go` |
