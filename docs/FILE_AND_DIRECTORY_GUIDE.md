# FILE AND DIRECTORY GUIDE

**Project:** AI-Assisted Adaptive Performance in Permissioned Blockchain Networks (HFBTP stage)
**Repository:** `AI-Assisted-Permissioned-Blockchain-Architecture-` (canonical implementation, Git-connected)
**Last updated:** 2026-10-15 — verified against the actual files in this repository.

Legend: **[Implemented]** = runs today · **[Supporting]** = configuration/provenance/docs kept for reproducibility ·
**[Placeholder]** = interface or reserved location, no logic · **[Planned]** = documented future stage, not implemented here.

---

## 1. Directory structure and responsibility

```text
AI-Assisted-Permissioned-Blockchain-Architecture-
├── README.md                  Project overview, runbook, limitations (concise by design)
├── LICENSE                    MIT text
├── .gitignore                 Excludes data, caches, generated artifacts, secrets
├── pyproject.toml             Package + test config (src layout, pytest paths)
├── requirements.txt           Declared dependencies
├── data/                      (below — immutable raw data + manifests only)
├── notebooks/                 Sequential research notebooks (audit → EDA → baselines)
├── src/fabric_perf/           Reusable library implementation
├── experiments/               Generated metrics, figures, run logs (gitignored except .gitkeep)
├── docs/                      Methodology, audit report, evaluation protocol, THIS guide
└── tests/                     pytest suite for contracts, processing, metrics, splits
```

**What does NOT belong here** (and where it lives):

| Material | Where it lives |
|---|---|
| Research-planning documents, approved protocols, decision registers | `D:\Plan_research\` |
| Viva-preparation / reading materials | `D:\Plan_research\reading_materials\` |
| Prior-stage frozen-model artifacts + evidence trails (`fabric_predictor`) | `D:\Reseach_Model\` parent workspace (Stage-1 export/inference, not a Git repo) |
| Virtual environments, IDE/cache infrastructure | `D:\Reseach_Model\.venv\`, `.kilo\`, `.freebuff\` etc. (workspace infrastructure) |

---

## 2. Root files

| File | Status | What it does / how it works | Depends on | Safe to remove? |
|---|---|---|---|---|
| `README.md` | Supporting | Research problem, scope, layout, runbook (`python -m pytest`, three `jupyter nbconvert --execute` invocations), output locations, known limitations. No logic. | — | No |
| `pyproject.toml` | Supporting | setuptools src-layout config; `[tool.pytest.ini_options]` sets `testpaths=["tests"]` and `pythonpath=["src"]` so pytest imports `fabric_perf` without installation. | — | No |
| `requirements.txt` | Supporting | Declares numpy/pandas/scikit-learn/pyYAML/matplotlib/seaborn + dev tools. Matches the environment used for verification (Python 3.14.3, sklearn 1.9.1, pandas 3.0.2). | — | No |
| `.gitignore` | Supporting | Ignores `data/raw/**` (except READMEs), processed outputs, `experiments/*` except `.gitkeep`, caches, secrets. Keeps model artifacts out of Git. | — | No |
| `LICENSE` | Supporting | MIT. | — | No |

---

## 3. `data/` — the data contract

| File | Status | What it is / produces |
|---|---|---|
| `data/README.md` | Supporting | Dataset registration rules: raw immutability, no blind pooling, manifest requirements. |
| `data/raw/hfbtp/HFBTP.csv` | Supporting (data) | The raw dataset. **24,687 rows × 8 cols; MD5 `E9275C49A2D7DC1F2A4015B6D8B28482`; SHA-256 `CE295D64…82BD5E`.** Never modified; re-verified on every audit. |
| `data/raw/hfbtp/README.md` | Supporting | Provenance (LearningChain paper, Fabric 2.3 + Caliper 0.5.0), verified identity, handling rules. |
| `data/processed/.gitkeep` | Placeholder | Reserved reproducible-outputs location; currently empty by design. |
| `data/manifests/hfbtp_manifest.yaml` | Supporting | Canonical dataset identity: checksums, exact column roles (features/targets/excluded/caution), grid dimensions, quality findings. Referenced by the audit report. |
| `data/manifests/manifest_schema.json` | Supporting | JSON-Schema for manifests (shape validation). |
| `data/manifests/mmbpd_manifest.yaml` | Supporting | MMBPD placeholder manifest — purposeful future-stage registration stub (units must be verified before any merging). |
| `data/raw/{mmbpd,external}/.gitkeep` | Placeholder | Reserved locations for future datasets (documented in `data/README.md`). |

No code writes to `data/raw/`. The loader only ever **reads and re-verifies** it.

---

## 4. `src/fabric_perf/` — the library

| Module | Status | What it does | Consumes | Produces | Depends on |
|---|---|---|---|---|---|
| `__init__.py` | Implemented | Version/author metadata only. | — | — | — |
| `data/base.py` | Implemented | `BaseDatasetLoader` ABC (load/validate/feature-target contract) + `DatasetLoadError`, `SchemaValidationError`. | — | Exception types | — |
| `data/hfbtp.py` | Implemented | The dataset contract: `HFBTPDatasetLoader` — schema validation with informative missing-column errors, strict dtypes check, never drops rows, `compute_checksums()`, `describe_grid()`, `audit()`, value-range counts, role lists (features/targets/excluded/caution). | `data/raw/hfbtp/` | audit dicts (read-only) | `data/base.py` |
| `evaluation/splitters.py` | Implemented | Approved protocol splits with hard integrity checks: `t1_interior_band_split` (test [210-305]), `t1b_right_edge_band_split` (train ≤605, val [610-705], test [710-800]), `t2_d0_workload_split` (test {30,140}, highest trained level 145), `t3_orderer_split` ({3,7}→{5,9}), plus generic `create_configuration_aware_split` for exploratory use. Verifies band completeness, row disjointness, partition sums at construction. | DataFrame w/ HFBTP columns | (train, val, test, info) tuples | — |
| `evaluation/metrics.py` | Implemented | `calculate_regression_metrics`: MAE, RMSE, R², guarded MAPE with prefix support. | arrays | metric dict | sklearn |
| `models/baseline.py` | Implemented | Feature engineering (`add_engineered_features`: log_set_rate, log_block_size, block_per_unit_load, set_x_orderers, block_over_orderers), leakage-guarded `separate_features_targets` (asserts whitelist contains no post-execution column), `build_pipeline` for {dummy_median, linear, random_forest} (stable identifiers; `linear` is Multiple Linear Regression (8 Engineered Features), display names in `MODEL_LABELS`), `fit_and_evaluate`, `run_all_baselines`. Preprocessing lives inside sklearn Pipelines → fitted on train only. | split DataFrames | metrics dicts / fitted pipes | sklearn, `evaluation/metrics` |
| `models/base.py` | Placeholder | `BasePerformanceModel` interface (fit/predict/save/load) reserved for later-stage model wrapping. | — | — | — |
| `controllers/base.py` | Placeholder | `BaseController`/`StaticController` abstract interfaces only — **no closed-loop logic exists**. Kept as the documented seam where the planned testbed stage plugs in. | — | — | — |
| `data/mmbpd.py` | Placeholder | Minimal MMBPD loader shell; registration stub matching the manifest. No MMBPD data exists yet. | — | — | — |
| `utils/helpers.py` | Implemented | Path helpers (project-root-anchored), streamed MD5/SHA-256, `save_json`/`save_csv_rows`, UTC timestamp. | — | files under `experiments/` | — |
| `utils/seeds.py` | Implemented | `seed_everything(42)`: random, numpy, PYTHONHASHSEED. | — | returned seed | — |
| `utils/logging.py` | Implemented | Stdout logger setup. | — | — | — |

---

## 5. `notebooks/` — the workflow

Run in order. Each was executed against the real CSV and ships with its outputs.

| Notebook | What it does (1st-person) | Reads | Writes |
|---|---|---|---|
| `01_dataset_audit.ipynb` | Recomputed identity, schema, grid coverage, missing-cell structure, divergence flags; asserts all audit reference values; persists the audit. | CSV + loader | `experiments/results/hfbtp_audit_report.json` |
| `02_eda.ipynb` | Ten-section EDA: dataset identity and structure, data quality and coverage, univariate distributions, workload-performance response, load × block-size and orderer interactions, correlations with their limits, exploratory Multiple Linear Regression diagnostics (T2-D0 training partition only), implications for model selection, limitations; descriptive-only language. | CSV + loader + T2-D0 splitter + models | 12 figures (`hfbtp_eda_01` to `hfbtp_eda_12`) + `hfbtp_eda_summary.json` |
| `03_hfbtp_baseline_model.ipynb` | Builds the four registered splits, asserts protocol reference counts, trains Dummy / Multiple Linear Regression (8 engineered features) / RF on both targets, reports per-split tables, saves the run manifest with seed/versions/split definitions. | CSV + loader + splitters + models | `hfbtp_baseline_metrics.csv`, `hfbtp_baseline_run_manifest.json`, 2 figures |

`notebooks/README.md` documents execution order and reproducibility rules.

---

## 6. `experiments/` — generated evidence (gitignored except .gitkeep)

| Path | Contents |
|---|---|
| `results/hfbtp_audit_report.json` | Machine-readable audit incl. checksum matches and coverage. |
| `results/hfbtp_eda_summary.json` | Manifest checks, quality flags, skew/kurtosis, correlations, orderer-spread and block-size-plateau summaries, in-sample exploratory fit metrics (T2-D0 training rows), interpretation note. |
| `results/hfbtp_baseline_metrics.csv` | 24 rows: 4 splits × 3 models × 2 targets (MAE/RMSE/R² val+test). |
| `results/hfbtp_baseline_run_manifest.json` | Seed, library versions, dataset MD5+SHA256, feature list, RF params, full split definitions, interpretation limits. |
| `figures/hfbtp_eda_*.png` | 12 EDA figures (notebook 02). |
| `figures/hfbtp_baseline_*.png` | 2 baseline figures (notebook 03). |
| `figures/_archive_eda_v1/`, `results/_archive_eda_v1/` | Outputs of the previous EDA notebook (6 figures and a summary JSON), kept unchanged for reference. |

Regenerate everything with the three `nbconvert --execute` commands in the README.

---

## 7. `docs/`

| File | Role |
|---|---|
| `research_plan.md` | In-repo summary of the research plan (full authoritative plan lives in `D:\Plan_research\`). |
| `methodology.md` | Problem formulation, three control paradigms, leakage rules, operational considerations. |
| `dataset_catalog.md` | Registered datasets with verified HFBTP facts + MMBPD/testbed placeholders. |
| `hfbtp_audit_report.md` | The audit narrative (recomputations 2026-10-15, reconciliation of prior findings). |
| `evaluation_protocol.md` | The binding evaluation protocol: approved split constants, metric hierarchy, leakage rules, reproducibility checklist. |
| `FILE_AND_DIRECTORY_GUIDE.md` | This file. |

`fabric-testbed` note: `fabric-testbed/` contains only README + `.gitkeep` placeholders — reserved for the planned Milestone-8 testbed; no telemetry or experiment code exists there.

---

## 8. `tests/`

| File | Covers |
|---|---|
| `test_imports.py` | Package metadata imports. |
| `test_data_contract.py` | Loader inheritance, real-header schema validation, StaticController behavior, metric sanity, seed. |
| `test_hfbtp_preprocessing.py` | Missing file/dir errors, missing-column and non-numeric errors with informative messages, out-of-range reporting (no drops), raw-file immutability after load, engineered-feature purity, leakage guard on the feature whitelist, split integrity (band completeness, disjoint keys, protocol counts, full coverage). |
| `test_metrics.py` | Hand-computed MAE/RMSE/R²/MAPE, zero-actual guarding, prefixing. |
| `test_model_labels.py` | Naming contract: stable model identifiers, display labels for the 3-feature (frozen) and 8-feature (repository) Multiple Linear Regression, estimator unchanged. |
| `test_notebooks.py` | Every notebook is valid nbformat and every code cell parses. |

Run: `python -m pytest tests -q` from the repository root (config in `pyproject.toml`).

---

## 9. End-to-end workflow (one paragraph)

`HFBTP.csv` (checksum-pinned) → `HFBTPDatasetLoader` (schema, checksums, grid audit; never mutates) →
`01_dataset_audit.ipynb` (bonded audit report) → `02_eda.ipynb` (descriptive-only EDA figures) →
`03_hfbtp_baseline_model.ipynb`: `add_engineered_features` → four `splitters.py` protocol splits (asserted against
protocol reference counts) → `run_all_baselines` (Dummy / Multiple-Linear-Regression-in-Pipeline / RF, train-only fitting) →
metrics CSV + run manifest (seed, versions, split definitions) + figures → interpretation limited to
"for the measured environment" claims. Tests validate contracts, integrity, and metrics independently of the notebooks.

---

## 10. Sibling workspaces (NOT part of this repository)

| Location | Purpose | Relationship to this repo |
|---|---|---|
| `D:\Plan_research\` | Research-planning source of truth: `HFBTP_EVALUATION_PROTOCOL_FINAL.md`, `HFBTP_ML_MODELING_PLAN.md`, E5/E6 decision docs, pilot protocol, reading materials (`reading_materials/` incl. the viva handbook). | Authority for research decisions; never imported by code. |
| `D:\Reseach_Model\` (parent, not Git) | Stage-1 frozen-model export & inference workspace: `src/fabric_predictor/`, `models/T2_D0/{throughput,avg_latency}/model.joblib+meta.json`, `scripts/{export_selected_models,predict,verify_exports}.py`, `outputs/verification/*` evidence trails, its own `.venv` (35 tests passing). | **Different research stage**: artifact serving for the earlier frozen Phase-1C models. Distinct from this repo's baseline-model implementation stage; preserved untouched. `models/heavy_load, T1b, T2_D1-D3` mentioned in planning documents were never exported — only T2_D0 exists. |

If a future stage needs `fabric_predictor` inference inside this repo, that is a documented, tested integration —
not a silent copy.
