# Proposed E1 Configuration

**Proposed artifacts only.** None has been run, parsed by Caliper or used by Fabric. Do not treat them as working configurations.

| File | Purpose | Unverified |
|---|---|---|
| `benchmark_fixed_count.yaml` | One `txNumber` round | Monitor key names on the pinned Caliper |
| `benchmark_duration.yaml` | One `txDuration` round | Same |
| `configtx_orderer_excerpt.yaml` | Orderer batch settings | Merge into the real `configtx.yaml` |
| `e1_per_tx_observer.js` | Per-transaction CSV | Base class path, method signatures, batch or single results |

`RATE`, `BLOCK` and `RUN_ID` are placeholders filled for each scheduled run.
