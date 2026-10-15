# Research Plan: AI-Assisted Performance Prediction & Adaptive Control in Hyperledger Fabric

## 1. Executive Summary

This research investigates methods to accurately predict transaction processing performance and adaptively control configuration parameters in **Hyperledger Fabric**. The project compares three distinct operational paradigms:
1. **Static Approach**: Fixed baseline configurations selected in advance (e.g., constant batch size, timeout, endorsement policies).
2. **Reactive Approach**: Rule-based or heuristic adjustments triggered after observed metrics cross predefined degradation thresholds (e.g., queue length > threshold, latency spike).
3. **Predictive Approach**: Proactive ML models that forecast near-future performance (throughput/latency) or overload risk from workload trends and recommend optimal configurations before performance collapses.

### Primary Performance Objectives
1. **Maximize Transaction Throughput** (Transactions Per Second, TPS).
2. **Minimize Average Transaction Latency** (End-to-end execution and commit time in ms/s).

---

## 2. Methodology & Multi-Dataset Strategy

To avoid dataset bias and prevent overfitting to a single benchmarking environment:
- **Primary Training & Audit Dataset (`HFBTP`)**:
  Used for thorough data auditing, exploratory data analysis (EDA), feature analysis, and initial regression modeling.
- **Independent Evaluation Dataset (`MMBPD`)**:
  Kept strictly separate to evaluate model transferability and cross-dataset generalization.
- **Local Validation Testbed (`fabric-testbed`)**:
  Controlled, reproducible Hyperledger Fabric network runs on local laptop hardware to validate predictive and adaptive mechanisms under dynamic workloads.

> [!IMPORTANT]
> **Static vs. Dynamic Benchmark Safeguard**:
> Benchmark datasets derived from static parameter grids (such as HFBTP) lack temporal sequences and intra-cell replications. While static regression models can estimate response surfaces across parameter combinations, **static grid points alone cannot demonstrate real-time closed-loop reaction, state recovery, or dynamic stability**. Demonstrating reactive and predictive control requires time-series traces or live testbed experiments.

---

## 3. Research Milestones & Branch Roadmap

Work is organized strictly on feature branches branched from `main` and merged back upon completion without a `dev` branch.

| Milestone | Feature Branch | Deliverables |
|---|---|---|
| **0. Repo Setup** | `main` | Clean repository scaffold, documentation, configs, tests. |
| **1. Data Audit** | `feature/dataset-audit` | Auditing HFBTP & MMBPD: missing values, schema checks, parameter grid structure, SHA256 checksums. |
| **2. EDA & Visuals** | `feature/eda` | Target distributions, correlation heatmaps, feature scatter plots, response surface inspection. |
| **3. Eval Protocol** | `feature/evaluation-protocol` | Group-aware / configuration-aware splitters, target leakage checks, seed definitions. |
| **4. Baselines** | `feature/baseline-models` | Multiple Linear Regression, Ridge, Decision Tree baselines with standard metrics (MAE, RMSE, R²). |
| **5. Model Training** | `feature/model-training` | Random Forest, Gradient Boosting (XGBoost/LightGBM; the frozen Phase 1C pre-registered scikit-learn HistGradientBoostingRegressor, which is not integrated into this repository), hyperparameter tuning, residual analysis. |
| **6. Cross-Dataset** | `feature/cross-dataset-validation` | Testing models trained on HFBTP against MMBPD; distribution shift analysis. |
| **7. Controllers** | `feature/controllers` | Implementing Static, Reactive (threshold), and Predictive (ML-driven) controller interfaces. |
| **8. Fabric Testbed** | `feature/fabric-testbed` | Local Docker-based Fabric deployment, Caliper workloads, telemetry collection. |
| **9. Final Report** | `feature/research-report` | Paper-ready charts, statistical tables, limitations, and reproducible run scripts. |

---

## 4. Integrity and Publication Standards
- Every observation, metric, and data point must be verified against actual files.
- Random seeds and hardware notes must be recorded for every run.
- Negative or inconclusive results (e.g., if predictive control adds more overhead than benefit) are valid and will be reported faithfully.
