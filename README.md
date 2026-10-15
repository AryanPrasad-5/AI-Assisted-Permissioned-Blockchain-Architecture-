<p align="left">
  <img src="assets/research-logo.svg" alt="Research Logo" width="64" height="64" />
</p>

## AI-Assisted Adaptive Performance in Permissioned Blockchain Networks

**Authors:** Aryan Prasad and Anuj Patel &middot; **Year:** 2026 &middot; **Type:** Undergraduate research project

This repository supports an ongoing empirical study of performance prediction and adaptive configuration in Hyperledger Fabric. It is a research project, not a payment, settlement, or production financial system. No claim of performance improvement is made until experiments establish it.

**Central question:** Can performance prediction support better configuration decisions in Hyperledger Fabric than static configuration or purely reactive control?

### Background

Hyperledger Fabric is a permissioned blockchain framework for distributed ledger applications with known or authorized participants. Transactions follow an execute–order–validate flow:

- **Execute and endorse:** Endorsing peers simulate a transaction proposal and produce endorsements according to the applicable endorsement policy.
- **Order:** The ordering service establishes transaction order and packages transactions into blocks.
- **Validate and commit:** Peers validate transactions, including endorsement requirements and version conflicts, and commit blocks to their ledgers.

Performance may depend on workload intensity, transaction characteristics, endorsement requirements, block configuration, ordering behavior, peer validation, available computing resources, and network conditions. A fixed configuration may not perform equally well across workloads. A reactive strategy acts on measured conditions, possibly after degradation has begun. A predictive strategy may anticipate problems and support earlier decisions, but its value is a research hypothesis, not an established result.

### Research Objectives

- Characterize the relationship between workload, configuration, throughput, and latency.
- Investigate ML-based prediction of performance metrics using information available at decision time.
- Compare static, reactive, and predictive approaches.
- Evaluate whether predictive decisions improve performance under suitable operating conditions.
- Investigate generalization across compatible datasets and, if feasible, controlled Fabric experiments.

**Proposed research questions (none are answered yet):**

- **RQ1:** How are workload intensity and configuration parameters associated with Fabric throughput and latency?
- **RQ2:** How accurately can ML models estimate relevant performance metrics from information available at decision time?
- **RQ3:** Under which conditions can predictive control outperform static and reactive strategies?
- **RQ4:** To what extent do findings generalize across compatible datasets and experimental conditions?

### Three Approaches

| Approach | Mechanism | Limitation |
|---|---|---|
| **Static** | Uses a fixed configuration or a setting selected in advance | Cannot automatically adapt to changing conditions |
| **Reactive** | Responds to measured performance conditions or threshold crossings | May respond only after degradation becomes visible |
| **Predictive** | Uses estimates of future performance or overload risk to inform decisions | Depends on prediction quality, decision timing, and control stability |

The primary objectives are:

- **Throughput:** successfully committed transactions per unit of time, using a documented measurement convention.
- **Latency:** time until a defined transaction completion event, with the definition and units recorded for each experiment.

Accurate predictions alone do not establish that a predictive controller improves real system performance. That requires closed-loop evaluation.

### Datasets

The repository is designed to support multiple datasets. Raw datasets should remain separate and unchanged, each with its own provenance notes, schema documentation, and preprocessing rules.

| Dataset | Intended role | Status |
|---|---|---|
| **HFBTP** | Initial audit and baseline model development | Audited & validated; baselines complete (see `docs/hfbtp_audit_report.md`) |
| **MMBPD** | Candidate independent evaluation | Not acquired; compatibility checklist pending |
| **Future datasets** | Extend the research | Planned |

Each dataset must be audited before use: actual files, schema, feature and target columns, units, missing values, duplicates, configuration coverage, and observation structure. Timestamps, repeated trials, and temporal traces must not be assumed. MMBPD must not be assumed compatible with HFBTP until its schema, target definitions, units, and experimental conditions are verified. Datasets may need to be acquired separately because of size or licensing restrictions.

### Methodology

1. Audit dataset provenance, schema, units, and data quality.
2. Conduct exploratory data analysis.
3. Define leakage-safe evaluation splits.
4. Train baseline regression models.
5. Compare suitable ML models.
6. Investigate static, reactive, and predictive control strategies.
7. Validate controller behavior through controlled Fabric experiments if feasible.
8. Document results, limitations, and reproducibility information.

### Evaluation

**Prediction.** Candidate models include Linear Regression, Ridge Regression, Decision Tree, Random Forest, and gradient boosting, subject to implementation and data suitability. Candidate metrics are MAE, RMSE, and R².

Evaluation will follow these principles:

- **Target leakage prevention:** features must be available at decision time and must not encode the target.
- **Group-aware or configuration-aware splitting** where justified, so related observations do not span training and test sets.
- **Separation of training, model selection, and final testing.**
- **Reproducibility:** random seeds, dataset versions, and experiment configurations are recorded.

**Controller evaluation.** Candidate outcomes are throughput, latency, configuration-change frequency, stability, and recovery. SLA violations will be reported only if an explicit SLA is defined.

**Scope of evidence.** Dataset-based prediction and closed-loop controller validation are different. If HFBTP contains only static configuration-level observations, it may support configuration-to-performance prediction but cannot independently demonstrate temporal forecasting, recovery, or successful adaptive control. Those claims would require suitable temporal data or controlled Fabric experiments.

### Current Implementation Status (HFBTP stage — as of 2026-10-15)

| Component | Status |
|---|---|
| Dataset audit (HFBTP) | **Complete** — checksums, schema, grid coverage verified; `docs/hfbtp_audit_report.md` |
| Data loaders & contracts | **Complete** — `src/fabric_perf/data/` with strict validation and passing tests |
| Exploratory data analysis | **Complete** — `notebooks/02_eda.ipynb`, figures under `experiments/figures/` |
| Evaluation protocol | **Implemented** — approved leakage-safe splits (T1 / T1b / T2-D0 / T3) in `src/fabric_perf/evaluation/splitters.py` |
| Baseline models | **Complete** — Dummy / Linear / Random Forest on registered splits, `notebooks/03_hfbtp_baseline_model.ipynb` |
| MMBPD cross-dataset evaluation | Not started — dataset acquisition & compatibility checklist pending |
| Advanced models (HGB, quantile) | Not started — requires documented protocol extension |
| Controllers (static/reactive/predictive) | Interfaces only (`src/fabric_perf/controllers/base.py`) — no closed-loop evaluation |
| Fabric testbed & live experiments | Not started — future phase |

Findings so far are descriptive: they characterize the measured configuration-performance relationship and model generalization on historical data. They do **not** demonstrate temporal forecasting, recovery behavior, or that predictive control outperforms static or reactive control in an operational Hyperledger Fabric network.

### Repository Structure

A file-and-directory guide explaining every meaningful file, its role, dependencies, and how modules fit together is at `docs/FILE_AND_DIRECTORY_GUIDE.md`. Research-planning documents and reading materials (including the viva handbook) live outside this repository under `D:\Plan_research\`.

The intended layout is shown below. Directories may be absent or contain only placeholders until the corresponding work is done; presence of a directory does not imply completed functionality.

```text
data/              Multiple datasets, manifests, and processed data
notebooks/         Data analysis and model experiments
src/fabric_perf/   Reusable Python research code
tests/             Automated tests
experiments/       Experiment outputs and figures
fabric-testbed/    Planned or existing Fabric experiments
docs/              Research and methodology documentation
```

### Getting Started

The commands below use Windows PowerShell and assume the repository provides a requirements.txt and a pytest-based test suite. Check the repository root and adjust if your setup differs.

```powershell
git clone https://github.com/AryanPrasad-5/AI-Assisted-Permissioned-Blockchain-Architecture-.git
cd AI-Assisted-Permissioned-Blockchain-Architecture-

python -m venv .venv
.\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
python -m pytest

# Run the research workflow (in order):
jupyter nbconvert --to notebook --execute --inplace notebooks/01_dataset_audit.ipynb
jupyter nbconvert --to notebook --execute --inplace notebooks/02_eda.ipynb
jupyter nbconvert --to notebook --execute --inplace notebooks/03_hfbtp_baseline_model.ipynb
```

- The audit notebook writes `experiments/results/hfbtp_audit_report.json` (checksums, schema, grid coverage).
- The EDA notebook writes figures to `experiments/figures/hfbtp_*.png` and `experiments/results/hfbtp_eda_summary.json`.
- The baseline notebook evaluates the Dummy/Linear/Random-Forest ladder on the registered splits and writes `experiments/results/hfbtp_baseline_metrics.csv` and `hfbtp_baseline_run_manifest.json`.
- The dataset must be placed at `data/raw/hfbtp/HFBTP.csv` (it is not committed to Git). Its required checksums and handling rules are in `data/raw/hfbtp/README.md` and the loader re-verifies them on every audit.
- Tests run with `python -m pytest` (29 tests covering imports, data contracts, HFBTP loading/validation, metrics, and split integrity).

### Citation

```bibtex
@misc{ai-adaptive-permissioned-blockchain,
  title  = {AI-Assisted Adaptive Performance in Permissioned Blockchain Networks},
  author = {Aryan Prasad and Anuj Patel},
  year   = {2026},
  note   = {Undergraduate research project},
  url    = {https://github.com/AryanPrasad-5/AI-Assisted-Permissioned-Blockchain-Architecture-}
}
```
