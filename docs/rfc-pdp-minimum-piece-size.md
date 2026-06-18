# RFC: Minimum On-Chain Piece Size for PDP (match deletion granularity; 8 MiB default)

| | |
|---|---|
| **Status** | Draft / for discussion |
| **Author** | forrest |
| **Date** | 2026-06-17 |
| **Scope** | Piri aggregation policy + PDP cost/economics model. **No smart-contract changes required** (one optional contract lever noted in §9). |
| **Affected components** | Piri (`pkg/pdp/aggregation/*`, `pkg/pdp/service/*`, `pkg/pdp/tasks/*`), SP gas budget + sender provisioning |
| **Contracts referenced** | `PDPVerifier.sol`, `FilecoinWarmStorageService.sol`, `FilecoinPayV1.sol` |
| **Companion tool** | `pdp-cost-calculator.html` — interactive model of all costs/revenue below |

---

## Decision summary

> **Read this; §1–§10 are the supporting analysis and §3.1–§3.4 + the Appendices are the gas audit (skim for conclusions).**

**The ask.** Lower Piri's `MinAggregateSize` from `128<<20` to a **configurable ~8 MiB floor**
(`aggregator/jobqueue.go:190`), keeping aggregation adaptive: any blob ≥ floor becomes its **own**
on-chain piece (O(1) delete); only sub-floor blobs pool up to the floor. **No smart-contract change
is required** for the floor itself.

**Why 8 MiB.** The cost-optimal piece size ≈ the deletion/object granularity, and the target S3
objects are ~8 MiB (AWS's default multipart threshold). Smaller wastes registration + churn gas for
no delete benefit; larger re-introduces the off-chain repack on every partial delete.

**The numbers that matter** (1 PiB @ 8 MiB; measured Foundry gas + calibnet anchors):

| Quantity | Value |
|---|---|
| Add a piece | **~120,700 EVM gas, size-independent** (5-gas spread across 32 B–256 MiB) |
| Delete | standalone **~88,800 gas, O(1)**; from an aggregate `gDel + gAdd` **+ off-chain re-hash** |
| Batch cap | **~13 pieces/tx** — the PDPVerifier `EXTRA_DATA_MAX_SIZE = 2048` gate binds first ⇒ **~11M `addPieces` txns/PiB** |
| Fleet | **~12 senders** to onboard in 10 months; **~6** for 10%/mo steady-state churn |
| Margin | on-chain cost is a rounding error vs storage revenue (**~99%** at ~4 USDFC/TiB/mo) |

**The crux facts** (non-obvious and load-bearing):

1. **There is no slashing.** No slash/penalty/collateral/bond logic exists in any contract; a missed
   proof merely forgoes that period's pay. This makes registration and deletes freely **deferrable to
   cheap-gas windows** (Piri already gas-gates them) — which is what rescues small pieces on cost.
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
| `deleteDataSet` | **O(1)** (abandons per-piece storage) |
| Listener `piecesAdded` | **O(batch × metadata keys ≤ 5)** |
| Listener `validatePayment` / `_findProvenEpochs` | **O(proving periods settled)**, not pieces |

The only O(cumulative-piece) loops in the surface are two **`view` getters** — `getActivePieceCount` and `getActivePieces` (scan `[0, nextPieceId)` incl. deleted slots), off-chain `eth_call` only (no block gas). See §7 (Piri's repair path calls them).

### 3.4 Proving cost rises *slightly* with smaller pieces

Per proof of 5 challenges, each challenge does a **sumtree descent** of `log2(cumulative pieces)` **cold SLOADs** (~2,100 gas each) plus a **Merkle path** of `pieceHeight` **SHA-256** hashes (~84 gas each). Going 256 MiB → 8 MiB at fixed dataset bytes: the Merkle path shrinks ~5 hops but piece count rises 32×, deepening the sumtree ~5 levels — and a cold SLOAD ≫ a SHA-256 precompile, so **proving gets modestly more expensive (~+50k EVM gas/proof, logarithmic)**, not flat/cheaper as an earlier draft claimed. Matches calibnet: `ProvePossession` rose ~120M → ~177M Filecoin gas from 39 → 10,000 pieces; `NextProvingPeriod` stayed flat ~54M; proof fee depends only on bytes. **Caveat:** depth is keyed on `nextPieceId`, which is **monotonic** (incremented at `PDPVerifier.sol:477` in `addOnePiece`, never decremented) — churn ratchets proving cost up permanently.

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

**No slashing (verified).** `FilecoinWarmStorageService.sol`, `FilecoinPayV1.sol`, and `PDPVerifier.sol` contain **zero** slashing / penalty / collateral / bond logic. `FaultRecord` is an event consumed nowhere. `validatePayment` pays `proposedAmount × provenEpochs / totalEpochs`; faulted periods pay 0. **Missing a proof = the payer keeps that period's money; nothing is taken from the SP.**

**Lever 1 — gate registration to cheap gas.** Registration (`addPieces`) and deletes are **deferrable** (no on-chain deadline). Piri already implements this: a per-message **max-fee cap in wei** (`pdp.gas.max_fee.add_roots` etc.; `sender_eth.go`), defer-and-retry every `retry_wait` (default 5 min), **no penalty, no nonce consumed**. So registration can be priced at `min(prevailing, gate) + premium` — near the floor regardless of how congested the network is for proving. **This is what rescues small pieces on cost:** registration is the deferrable, piece-count-heavy cost, so gating it to the floor neutralizes the base-fee-spike risk. (Defaults are 0 = no gating; operators must set caps.)

**Lever 2 — skip unprofitable proving.** Because there is no slashing, on a base-fee-spike period an SP can simply not prove, forgoing only that period's pay. Proving must land in its challenge window (`[deadline − challengeWindowSize, deadline]`), so it pays the **prevailing** fee, not a gated one — but it is economically optional per period. (Note: Piri does not yet *auto*-skip; setting a low `prove` cap achieves it crudely by deferring past the window. And `nextProvingPeriod` must still be called to keep the cycle alive — consecutive skips amortize to one call.)

**Modeling consequence — two effective gas prices:** deferrable ops at the gate (≈ floor), proving at the prevailing base fee + premium (Piri sets `GasFeeCap = baseFee + SuggestGasTipCap`).

---

## 5. The real wall: transaction throughput, not gas dollars

Because per-piece gas is fixed, smaller pieces mean **more transactions**, and that — not dollars — is the binding constraint.

- **Batch cap (the binding gate):** the PDPVerifier checks `extraData.length ≤ EXTRA_DATA_MAX_SIZE = 2048`
  (`PDPVerifier.sol:47`, enforced at `:442`) **before** forwarding `extraData` to the listener's larger
  `MAX_ADD_PIECES_EXTRA_DATA_SIZE = 8 KiB` (`FilecoinWarmStorageService.sol:39`) — so the deployed
  verifier reverts above **~13 pieces/tx** (empty metadata), not the ~61 the WSS comment advertises.
  *The two contracts disagree: the WSS comment is unaware of the verifier's 2048 gate. Raising the cap
  is the highest-leverage throughput lever, but it requires a **PDPVerifier upgrade** (§9), not just a WSS change.*
- **Registration tx count:** 1 PiB at 8 MiB = 134.2M pieces ⇒ **~10.3M `addPieces` txns** (~11M; ~4.7× the
  ~2.2M a 61-piece batch would give).
- **Submission capacity:** ~1 tx per 30 s block ≈ **120 tx/hr per sender address** (~1.05M txns/yr); parallelize with multiple senders to multiply it.
- **Onboarding feasibility:** a 10-month onboard of 1 PiB at 8 MiB needs ≥ **~1,400 tx/hr (~12 senders)** for registration alone.
- **Churn throughput (often missed):** 10%/mo churn on 8 MiB pieces of a PiB ≈ **161M events/yr**. Under
  the calculator's default **50% replace / 50% delete** mix (deletes enqueued ≤ 2000/period; re-adds at
  ~13 pieces/tx and dominating) ⇒ **~6.2M txns/yr** (swings ~0.08M all-delete to ~12M all-replace) —
  roughly **6× one sender's ~1.05M txns/yr**. Real provisioning must cover onboarding **and** churn —
  on the order of **~12 sender addresses during onboarding, ~6 for steady-state churn**.

**Onboarding is a revenue ramp.** Onboarding time = how fast data *arrives* (demand), independent of the node. Revenue accrues on the **growing registered base**: `actual onboarding = max(arrival time, capacity-limited time)`. If throughput can't keep up, onboarding stretches and the delayed data forgoes revenue — captured directly as a smaller revenue ramp (see the calculator's time chart), not a bolted-on penalty. Even a *feasible* 10-month / ~12-sender ramp forgoes a meaningful slice of first-year revenue simply because data earns as it lands, not from day 0.

---

## 6. Profitability

With capex/power/hardware excluded (marginal on-chain margin):

- **Revenue is identical for every piece size** (it depends only on dataset size × storage price × time). So the profit-optimal piece = the cost-optimal piece = ≈ object size.
- **Profitability is gated by base fee × storage price, not piece size.** At near-floor base fee, registering a billion pieces costs single-digit dollars; piece size changes profit by *cents* on tens of thousands. At elevated base fee, *ungated* registration of small pieces can exceed revenue — but Lever 1 (gating) neutralizes that.
- **Worked target** (1 PiB, 8 MiB, base fee 2500, storage ~4 USDFC/TiB/mo, ~12 senders): feasible onboarding, **~99% margin** (on-chain cost rises ~4.7× with the corrected ~13-piece batch but is still low-thousands of dollars against tens of thousands of revenue). The same scenario at **one sender** is badly throughput-bound — registration alone takes **~9.8 years** (~3,600 d) — so onboarding is entirely capacity-limited and most first-year revenue is forgone via the ramp.

The contract's storage-price default is **2.5 USDFC/TiB/mo** (`FilecoinWarmStorageService.sol:425`, `(5 × 10^dec)/2`); realistic targets are 2–10. Use the calculator to set your point.

---

## 7. Proposal

1. **Set the minimum piece size to the typical deletion/object granularity; default 8 MiB** for the current ~8 MiB-object workload. Change `MinAggregateSize` (`piri/pkg/pdp/aggregation/aggregator/jobqueue.go:190`) from `128<<20` to the floor, and fix the comment block (`:185–190`) that hard-assumes 128/256 MiB.
2. **Make aggregation adaptive** (mostly already true): blobs ≥ floor → own piece (O(1) deletion); sub-floor blobs aggregate up to the floor. `jobqueue.go:205` (`AggregatePiece`) already submits a piece standalone when its padded size **exceeds** the floor — note the test is a strict `>`, so a blob *exactly* equal to the floor still aggregates; word the policy accordingly.
3. **Keep per-piece metadata empty** (`roots_add.go:421–424`) to preserve the `extraData`/batch budget.
4. **Keep `BatchSize` at/just under the real cap (~13), not ~50.** The binding gate is the PDPVerifier's `EXTRA_DATA_MAX_SIZE = 2048` (~13 pieces/tx, empty metadata) — **not** the WSS 8 KiB. The current default `BatchSize = 10` is already just under it; **raising toward 50 would make `addPieces` revert** at the verifier. Add an explicit guard against the 2048 cap; adding per-piece metadata lowers the limit further. (Lifting the cap requires a PDPVerifier upgrade — §9.)
5. **Provision sender parallelism for throughput** — size the number of sender addresses to cover onboarding **and** steady-state churn (target ~12 senders during onboarding and ~6 for steady-state 10% churn of 1 PiB @ 8 MiB). This is the binding constraint, not gas dollars.
6. **Configure registration gas-gating** (`pdp.gas.max_fee.add_roots`) so registration/deletes ride cheap-gas windows; leave proving ungated (or capped high enough to always land in-window).

**No contract change required.** (One optional contract lever in §9.)

---

## 8. Caveats, risks, and mitigations

1. **Throughput is the primary constraint.** ~10.3M registration txns for 1 PiB @ 8 MiB (at the real ~13-piece batch cap); gate-induced low duty-cycle further limits the achievable rate. **Mitigation:** sender parallelism (§7 item 5); model it in the calculator's throughput/latency view.
2. **Churn throughput** (often missed): high churn on small pieces generates large *ongoing* tx volume (≈6.2M txns/yr at 10% churn, 50/50 replace/delete) that exceeds several senders. **Mitigation:** size senders for churn too; batch deletes (up to `MAX_ENQUEUED_REMOVALS = 2000`/period).
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
| **Raise the verifier's `EXTRA_DATA_MAX_SIZE`** (2048, the binding gate — and the WSS `MAX_ADD_PIECES_EXTRA_DATA_SIZE` to match) | The one *contract* lever that directly relieves the throughput wall (more pieces/tx → fewer txns; ~13 → higher). Highest-leverage change if small pieces at PiB scale become routine. Requires a **PDPVerifier upgrade**. |

---

## 10. Open questions

1. **Blob/object-size distribution through Ingot** — sets the deletion granularity and thus the floor; decides how often the adaptive path aggregates vs. emits standalone pieces.
2. **Sender-parallelism budget** — how many sender addresses ops will run; this, not gas, decides PiB-scale feasibility.
3. **Churn rate and delete-vs-replace mix** — drives ongoing throughput and the off-chain repack exposure.
4. **Should the floor be per-SP / per-dataset configurable** rather than one global constant?
5. **Is the verifier's `EXTRA_DATA_MAX_SIZE`** (2048, the binding ~13-piece/tx gate) **worth raising** to relieve the throughput wall at scale (a PDPVerifier upgrade)?

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
| `CHALLENGES_PER_PROOF` | 5 | `FilecoinWarmStorageService.sol:26` |
| `MAX_ENQUEUED_REMOVALS` | 2000 | `PDPVerifier.sol:45` |
| `MAX_PIECE_SIZE_LOG2` | 50 | `PDPVerifier.sol:44` |
| `EXTRA_DATA_MAX_SIZE` (**binding** batch gate) | 2048 B (~13 pieces/tx, empty metadata) | `PDPVerifier.sol:47` (checked in `addPieces` at `:442`) |
| `MAX_ADD_PIECES_EXTRA_DATA_SIZE` (WSS listener) | 8 KiB (comment claims ~61/tx, but the 2048 verifier gate binds first) | `FilecoinWarmStorageService.sol:39` |
| `maxProvingPeriod` / `challengeWindowSize` | 2880 epochs (~1 day) / 60 (mainnet, also `SimplePDPService` default), 20 (calibnet), 10 (devnet) | `initialize` / deploy scripts |
| `storagePricePerTibPerMonth` (default) | 2.5 USDFC (`(5×10^dec)/2`; realistic 2–10) | `FilecoinWarmStorageService.sol:425` |
| `MinAggregateSize` (Piri, **to change → floor, e.g. 8<<20**) | 128 MiB | `aggregator/jobqueue.go:190` |
| `MaxMemtreeSize` (Piri, upload max) | 256 MiB | `proof/proof.go:114` |
| Piri input floor (`PaddedSize`) | 128 B | `aggregate.go:49` |
| Piri gas-gate keys (default 0 = off) | `pdp.gas.max_fee.{prove,proving_period,add_roots,default}` | `sender_eth.go`, `config/defaults.go` |
