# HFBTP Dataset Audit Report

**Repository:** `AI-Assisted-Permissioned-Blockchain-Architecture-`
**File audited:** `data/raw/hfbtp/HFBTP.csv`
**Audit date:** 2026-10-14
**Status:** COMPLETE

---

## 1. File Provenance & Recovery

The raw dataset was **not present** in the repository's `data/raw/hfbtp/` directory.
It was located and recovered from the prior research campaign at:

```
D:\Plan_research\hfbtp_probe\HFBTP.csv
```

**Checksum verification against prior research record:**

| Algorithm | Recorded (audit report) | Computed from file | Match? |
|---|---|---|---|
| MD5  | `e9275c49a2d7dc1f2a4015b6d8b28482` | `E9275C49A2D7DC1F2A4015B6D8B28482` | YES |
| SHA-256 | (not previously recorded) | `CE295D6451787FEFCFE2101579238CDA4BEDE8539BC302E6A482AA4E5782BD5E` | — |

The file was copied read-only to `data/raw/hfbtp/HFBTP.csv` (988,672 bytes).
The original at `D:\Plan_research\hfbtp_probe\HFBTP.csv` was not modified.
Git correctly ignores the data file per `.gitignore`.

---

## 2. Dataset Schema (Actual Columns)

| # | Column Header (exact) | dtype | Role |
|---|---|---|---|
| 1 | `Set Transaction Arrival Rate` | float64 | Safe predictor — pre-execution workload parameter |
| 2 | `Actual Transaction Arrival Rate` | float64 | Caution — post-execution measurement (see Section 5) |
| 3 | `Block Size` | float64 | Safe predictor — configuration parameter |
| 4 | `Orderers` | int64 | Safe predictor — configuration parameter |
| 5 | `Throughput` | float64 | **Primary target** |
| 6 | `Avg Latency` | float64 | **Primary target** |
| 7 | `Min Latency` | float64 | Excluded — post-execution measurement |
| 8 | `Max Latency` | float64 | Excluded — post-execution measurement |

> [!IMPORTANT]
> The manifest previously used placeholder column names (`tx_rate`, `batch_size`, `batch_timeout`,
> `throughput`, `avg_latency`). These do **not** match the actual headers. The loader and any
> notebooks must use the exact headers listed above.

---

## 3. Dimensions & Data Quality

| Property | Value |
|---|---|
| Total rows | **24,687** |
| Total columns | 8 |
| Missing values | **0** (all columns, confirmed) |
| Exact duplicate rows | **0** |
| File size | 988,672 bytes |
| File timestamp | 2023-10-25 |

---

## 4. Grid Structure & Observation Type

**This is a static parameter grid — not a time series.**

| Dimension | Unique values | Range | Step |
|---|---|---|---|
| `Set Transaction Arrival Rate` | 39 | 10 – 200 tx/s | 5 tx/s |
| `Block Size` | 159 | 10 – 800 txs | 5 txs |
| `Orderers` | 4 | 3, 5, 7, 9 | — |

**Full grid would be:** 39 × 159 × 4 = **24,804 cells**
**Actual rows:** 24,687
**Missing cells:** 117 (coverage 99.528%)
**Rows per cell:** always 1 — no repeated trials per configuration

**Implication:** Each row is a single benchmark execution at a specific
`(Set Rate, Block Size, Orderers)` combination. There are no timestamps,
no trial replications, and no temporal ordering information in the data.

---

## 5. Predictor Eligibility Analysis

### Safe predictors (known at decision time)

| Column | Rationale |
|---|---|
| `Set Transaction Arrival Rate` | Set before the benchmark run. Reflects the workload the controller would choose or observe at configuration time. |
| `Block Size` | A Fabric configuration parameter directly under controller influence. |
| `Orderers` | A deployment configuration parameter. |

### Excluded from features (post-execution measurements)

| Column | Rationale |
|---|---|
| `Min Latency` | Measured during/after the benchmark run. Unknown at prediction time. |
| `Max Latency` | Same. Also note `Max Latency` correlates with `Avg Latency` (r=0.655). Using it as a predictor of `Avg Latency` is target leakage. |

### Caution: `Actual Transaction Arrival Rate`

- Correlation with `Throughput`: **r = 0.985** (extremely high).
- This is the *measured* arrival rate during the run, not the pre-execution setpoint.
- At saturation, throughput plateaus while actual rate diverges from set rate.
- **Using it as a predictor of Throughput is likely target leakage** — it may
  encode the saturation effect that defines the output variable.
- It is **not safe** to use as a predictor without controlled leakage analysis
  demonstrating it is genuinely available before the target is determined.
- **Recommendation:** Exclude from features for now; treat `Set Transaction Arrival Rate` as the workload predictor.

---

## 6. Target Variable Ranges

| Target | Min | Mean | Median | Max | Units |
|---|---|---|---|---|---|
| `Throughput` | 9.8 | 91.7 | 95.6 | 197.3 | tx/s |
| `Avg Latency` | 0.08 | 0.895 | 0.85 | 2.67 | **seconds** |
| `Min Latency` | 0.01 | 0.166 | 0.16 | 0.58 | seconds |
| `Max Latency` | 0.15 | 1.915 | 2.13 | 5.14 | seconds |

> [!NOTE]
> `Avg Latency` is confirmed to be in **seconds**, not milliseconds. The manifest
> previously recorded the unit as `s_or_ms`. Update any downstream code accordingly.

---

## 7. Notable Anomaly: High-Load Set vs Actual Divergence

**20 rows** show a `|Actual Rate - Set Rate| > 5 tx/s`. These are concentrated
at high workload levels (Set Rate 185–200 tx/s). This is a physical phenomenon:
at saturation, the ordering service cannot accept transactions at the requested
rate, so the measured throughput and actual arrival diverge. These rows are
**not errors** and should not be removed, but they must be considered when
designing workload-range splits.

---

## 8. Correlation Summary

| Predictor | Corr with Throughput | Corr with Avg Latency |
|---|---|---|
| `Set Transaction Arrival Rate` | **+0.985** | -0.674 |
| `Actual Transaction Arrival Rate` | **+0.985** | -0.675 |
| `Block Size` | +0.013 | **+0.292** |
| `Orderers` | -0.002 | +0.028 |
| `Min Latency` | +0.623 | -0.100 |
| `Max Latency` | -0.633 | **+0.655** |

**Key observations:**
1. Throughput is dominated by `Set Transaction Arrival Rate` (r=0.985). Block Size and Orderers have very low linear correlation, suggesting non-linear or interaction effects.
2. `Avg Latency` has moderate correlation with `Block Size` (r=0.292), suggesting batch size matters more for latency.
3. `Orderers` has near-zero linear correlation with both targets — likely has threshold/interaction effects rather than a simple linear relationship.

---

## 9. Manifest & Loader Alignment Findings

### Manifest (`data/manifests/hfbtp_manifest.yaml`)

The manifest has been updated to reflect the verified data. Key changes from the prior placeholder:

| Field | Previous (placeholder) | Verified |
|---|---|---|
| Filename | `hfbtp_raw.csv` | `HFBTP.csv` |
| SHA-256 | `TBD_DURING_AUDIT_MILESTONE` | `CE295D6...` |
| Feature names | `tx_rate`, `batch_size`, `batch_timeout` | `Set Transaction Arrival Rate`, `Block Size`, `Orderers` |
| Target names | `throughput`, `avg_latency` | `Throughput`, `Avg Latency` |
| Latency unit | `s_or_ms` | `s` (seconds, confirmed) |

### Loader (`src/fabric_perf/data/hfbtp.py`) — RESOLVED 2026-10-15

The loader now: (a) returns the verified exact headers via `get_feature_names()` / `get_target_names()`; (b) enforces the full 8-column contract in a strict `validate()` that raises `SchemaValidationError` naming the missing columns; (c) computes file checksums (`compute_checksums()`); (d) reports grid coverage, duplicates, dtypes and out-of-range counts without ever dropping rows or mutating the raw file (`describe_grid()`, `audit()`).

`validate_schema()` returns the structured audit report; the hard-contract variant is `validate()`. Both are covered by `tests/test_hfbtp_preprocessing.py`.

---

## 10. Open Questions

1. **117 missing grid cells**: Which `(Set Rate, Block Size, Orderers)` combinations are absent, and why? Is this a benchmark failure, a deliberate design, or a data corruption artifact?
2. **`Orderers` near-zero linear effect**: Is this because orderer count has a threshold effect or because the grid does not vary orderer count independently at high loads?
3. **Block Size non-linearity**: Throughput shows very low linear correlation with Block Size (r=0.013), but the scatter plots from the prior EDA (in `D:\Plan_research\hfbtp_probe\outputs\`) suggest strong non-linear curves. Modeling must use non-linear methods.
4. **Literature source**: The paper associated with this dataset has not been formally cited in the manifest. The file `D:\Plan_research\hfbtp_probe\learningchain_paper.pdf` and `paper_text.txt` likely contain the citation.
5. **`Actual Transaction Arrival Rate` policy**: Decide definitively whether to include or exclude it as a predictor before any model training begins.
6. **Loader column name mismatch**: `HFBTPDatasetLoader.get_feature_names()` returns incorrect names. Must be fixed before training.

---

## 11. Audit Verdict

| Category | Finding |
|---|---|
| **Observation type** | Static parameter grid, one-shot benchmark |
| **Time series** | No |
| **Repeated trials** | No (1 row per cell) |
| **Missing data** | Zero missing values |
| **Data integrity** | Checksum verified; no duplicates |
| **Grid completeness** | 99.5% (117 of 24,804 cells absent) |
| **Safe predictors** | `Set Transaction Arrival Rate`, `Block Size`, `Orderers` |
| **Excluded from features** | `Min Latency`, `Max Latency` (post-execution) |
| **Caution column** | `Actual Transaction Arrival Rate` (leakage risk) |
| **Target units** | Throughput: tx/s | Avg Latency: seconds |
| **Loader accuracy** | RESOLVED 2026-10-15 — returns exact headers; strict validation added |
| **Ready for EDA** | **YES** — data is clean and placed correctly |
| **Ready for model training** | **YES** — baseline workflow run under the approved protocol (see docs/evaluation_protocol.md) |
