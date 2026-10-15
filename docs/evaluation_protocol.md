# Evaluation Protocol: Leakage-Safe Validation Methodology

## 1. Principles of Experimental Rigor

In performance benchmarking datasets, simple random train/test splitting (`train_test_split(shuffle=True)`) creates significant **data leakage**. When identical or neighbouring configuration settings appear in both the training and test sets, models memorize specific configuration cells rather than learning the underlying response surface. This inflates evaluation scores and fails under real deployment.

Because HFBTP is a static configuration grid with **one observation per cell**, the purpose of every split is to prove **response-surface generalization**, not memorization. The estimand is honest if phrased as: *"the response of one historical run of the measured environment"* — conditional on (Set rate, Block size, Orderers). No claim about run-to-run variability, calibration, causality, or deployment generality can be supported at this stage.

## 2. Splitting Strategies

### Strategy A: Configuration-Aware / Group-Aware Split (generic tool)

Group observations by experimental configuration using `GroupShuffleSplit` (`fabric_perf.evaluation.splitters.create_configuration_aware_split`). Useful for sanity checks; **not** the approved protocol.

### Strategy B: Out-of-Range Workload Split (Stress Generalization)

Train on low/mid workload levels; test targeted levels or bands. Implemented concretely as **T2/D0** below.

### Strategy C: Cross-Dataset Independent Evaluation (MMBPD)

Train exclusively on HFBTP, evaluate without fine-tuning on MMBPD. **Not implemented in this phase** — MMBPD acquisition and its go/no-go checklist (feature semantics, units, deployment conditions) remain unresolved.

## 3. Approved HFBTP protocol (as implemented in `src/fabric_perf/evaluation/splitters.py`)

These constants were approved in the research-planning workspace and are **not tuning knobs**. Reproduced here because they define the implemented evaluation:

| Split | Train | Validation | Test | Claim permitted |
|---|---|---|---|---|
| **T1 interior band** | block sizes outside both bands (18,486 rows) | block [610..705] (3,120) | block [210..305] (3,081) | interpolation to unseen block sizes inside the trained range |
| **T1b right-edge band (primary)** | block sizes ≤ 605 (val band excluded) — 18,642 rows | block [610..705] (3,120) | block [710..800] (2,925) | right-edge block-size extrapolation within the measured grid |
| **T2 / D0 workload draw** | levels ⇒ train excludes {30,140,150,155,160} ∪ [165..200]; max trained level 145 (16,458 rows) | levels {150,160} (1,266) | levels {30,140} (1,266) | unseen-workload prediction (headline) |
| **T3 orderer transfer** | orderers {3,7} minus val levels (11,766 rows) | {150,200} @ orderers {3,7} (636) | orderers {5,9} (12,285) | unseen-discrete-configuration transfer (exploratory) |

Verified at split construction: bands map to test exactly; partitions are row-disjoint; T1/T1b/T2D0/T3 partition counts equal the protocol reference counts (asserted in notebook 03 and in `tests/test_hfbtp_preprocessing.py`).

Reporting rules:

- MAE is the **primary** metric (in tx/s and seconds); RMSE secondary; R² tertiary — R² on a near-deterministic surface is not evidence of practical benefit.
- All splits are reported; no split is dropped after seeing its numbers.
- Models are forbidden from using test-partition feedback for revision; any such relaunch becomes a labeled post-hoc exploration.
- Model families authorized for the baseline phase: DummyRegressor, LinearRegression, RandomForestRegressor. Extending the family list requires a documented protocol change.

## 4. Leakage rules (binding)

- **Allowed features**: pre-execution columns {Set Transaction Arrival Rate, Block Size, Orderers} + decision-time-safe derivations (log terms, block-per-load ratio, orderer interactions) fitted/derived without target information.
- **Forbidden features**: `Actual Transaction Arrival Rate` (measured during the run, Corr(Throughput)=0.985), `Min Latency`, `Max Latency`, and certainly the targets themselves (`Throughput`, `Avg Latency`).
- **Preprocessing**: any fitted preprocessing (e.g. the linear model's scaler) is fitted on the training partition only and applied frozen to validation/test — enforced by placing everything inside sklearn `Pipeline`.
- **Missing cells**: the 117 absent grid cells are never imputed; split counts derive from present rows.
- **No row-index, regime, or temporal features**: HFBTP has no timestamps; lag/rolling features are unsupported by construction.

## 5. Operational Performance Metrics (controllers — future phase)

When controllers (Static, Reactive, Predictive) are eventually evaluated: SLA Violation Rate (only if an explicit SLA is defined), Average Delivered Throughput, and a Control Stability Index (configuration-switch frequency). **None of these are computable from HFBTP.**

## 6. Reproducibility Checklist

Every experiment must record: random seed (default `42`), exact dependency versions (Python, sklearn, pandas, numpy, matplotlib), dataset checksums (MD5 + SHA-256 in `data/manifests/hfbtp_manifest.yaml`), split definitions, and model parameters. The baseline run manifest is saved at `experiments/results/hfbtp_baseline_run_manifest.json`; the audit at `experiments/results/hfbtp_audit_report.json`.
