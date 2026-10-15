# HFBTP Raw Dataset — Provenance and Handling Rules

## What this directory holds

| File | Role | Bytes |
|---|---|---|
| `HFBTP.csv` | Raw HFBTP benchmark grid (Hyperledger Fabric Blockchain Transaction Performance, contributed by the LearningChain paper) | 988,672 |

## Verified identity (recomputed 2026-10-15 against the actual file)

- **MD5:** `E9275C49A2D7DC1F2A4015B6D8B28482` (matches the prior research-campaign record)
- **SHA-256:** `CE295D6451787FEFCFE2101579238CDA4BEDE8539BC302E6A482AA4E5782BD5E`
- **Dimensions:** 24,687 rows x 8 columns; 0 missing values; 0 duplicate rows; 0 duplicate configuration cells (exactly one observation per `(Set Transaction Arrival Rate, Block Size, Orderers)` cell)
- **Grid:** 39 x 159 x 4 = 24,804 possible cells; 117 absent (99.528% coverage)
- **Targets:** `Throughput` (tx/s), `Avg Latency` (seconds)

Full details: `docs/hfbtp_audit_report.md` and `data/manifests/hfbtp_manifest.yaml`.

## Origin and retrieval

- The file was **not obtained from a public download location in this workspace**; it was recovered from the prior research campaign at `D:\Plan_research\hfbtp_probe\HFBTP.csv` and copied here read-only.
- The dataset originates from the **LearningChain** paper (IEEE TSC), which collected it with **Hyperledger Fabric 2.3 + Hyperledger Caliper 0.5.0**, 3 organizations x 3 peers, and `{3, 5, 7, 9}` orderer nodes; arrival rates 10–200 tx/s step 5; block sizes 10–800 step 5. The formal literature citation is still **pending** (see `data/manifests/hfbtp_manifest.yaml`).

## Handling rules (binding)

1. **Immutable.** Never edit, filter, re-serialize, or re-sort this CSV. All transforms happen in code and write to `data/processed/`.
2. **Never committed to Git.** `.gitignore` excludes `data/raw/hfbtp/*` except this README.
3. **No imputation of the 117 absent grid cells.** All splits derive counts from present rows only.
4. **No timestamps or repeated trials may be assumed.** Row order is design order (block sizes ascending within run-blocks), not time.
5. Any downstream loader or notebook must re-verify checksums before treating the file as HFBTP.
