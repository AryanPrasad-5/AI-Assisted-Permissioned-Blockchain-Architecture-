# AI-Assisted Adaptive Performance in Permissioned Blockchain Networks

> Undergraduate research project investigating whether an AI-assisted adaptive mechanism can improve transaction-processing performance in a permissioned blockchain network modeling cross-border payment settlement.

[![Status](https://img.shields.io/badge/status-research--in--progress-yellow)]()
[![License](https://img.shields.io/badge/license-MIT-blue)]()
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)]()
[![Go](https://img.shields.io/badge/go-1.21%2B-00ADD8)]()
[![Docker](https://img.shields.io/badge/docker-required-2496ED)]()

---

## Table of Contents

- [Overview](#overview)
- [Research Question](#research-question)
- [System Architecture](#system-architecture)
- [Scope and Boundaries](#scope-and-boundaries)
- [Simulated Participants](#simulated-participants)
- [Experimental Design](#experimental-design)
- [Metrics](#metrics)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Reproducing Experiments](#reproducing-experiments)
- [Tech Stack](#tech-stack)
- [Research Deliverables](#research-deliverables)
- [Research Integrity](#research-integrity)
- [Roadmap](#roadmap)
- [Limitations](#limitations)
- [Contributors](#contributors)
- [Contributing](#contributing)
- [License](#license)
- [Citation](#citation)

---

## Overview

Cross-border payment settlement requires reliable transaction processing, controlled participation, auditability, and settlement finality across multiple financial institutions. Permissioned blockchains are well suited to this problem because participants operate within a controlled network with known identities and defined governance — but performance can degrade significantly as transaction workload increases, leading to higher latency, lower throughput, queue buildup, and increased resource utilization.

This project builds a small, reproducible experimental environment to test whether an **AI-assisted adaptive mechanism** — observing live workload and system conditions — can recommend or apply changes that improve transaction-processing performance, compared against a blockchain baseline with no AI involvement.

This is a **research benchmark and prototype**, not a production system. It does not process real money, connect to real banking infrastructure, or make production-readiness claims.

## Research Question

> **Can AI-assisted adaptation improve transaction-processing performance in a permissioned blockchain network under varying cross-border payment workloads?**

This question is provisional and may be narrowed following the literature review and research-gap analysis. The AI-assisted system is **not required to outperform the baseline** for the research to be considered a success — a well-documented negative or inconclusive result is a valid outcome.

## System Architecture

```
Payment Client
   │
   ▼
API Gateway
   │
   ▼
Transaction Processing Layer
   │
   ▼
Permissioned Blockchain Network ◄────────────┐
   │                                          │
   ▼                                          │
Monitoring / Metrics Layer                    │
   │                                          │
   ▼                                          │
AI Model                                      │
   │                                          │
   ▼                                          │
Adaptive Decision Mechanism ──────────────────┘
```

The blockchain network remains solely responsible for **transaction validation, ordering, consensus, ledger state, identity, auditability, and settlement/finality**. The AI component is strictly an **adaptive monitoring/control layer** — it observes system state and recommends or applies configuration/workload-handling adjustments. It does **not** replace consensus and does **not** independently approve or finalize transactions.

**Candidate AI inputs:**

| Signal | Description |
|---|---|
| Transaction rate | Incoming transactions per unit time |
| Queue length | Pending transactions awaiting processing |
| Recent transaction latency | Rolling latency window |
| CPU utilization | Node-level compute load |
| Memory utilization | Node-level memory load |
| Pending transactions | Backlog size |
| Historical workload | Recent workload trend |

The exact AI task, model family, and adaptive mechanism will be finalized after the literature review.

## Scope and Boundaries

**In scope**

- Permissioned blockchain experimental testbed
- Simulated financial institutions and synthetic cross-border transactions
- Reproducible, controllable workload generation
- A baseline system (no AI) and an AI-assisted comparative system
- Controlled experiments across multiple workload profiles
- Statistical comparison, error analysis, and reproducibility artifacts

**Out of scope**

- Real-money transactions or real banking integration
- Production financial deployment or production-readiness claims
- Claims that the project solves cross-border payments universally
- Claims of universal AI superiority
- AI replacing blockchain consensus or independently finalizing transactions
- Fabricated or manually invented experimental results
- Features added solely for demonstration or portfolio purposes

## Simulated Participants

| Node | Role |
|---|---|
| Bank A | Simulated institution — India |
| Bank B | Simulated institution — Singapore |
| Bank C | Simulated settlement institution |
| Bank D | Simulated verification / regulatory node |

All participants are simulated entities for experimental purposes only.

## Experimental Design

**Baseline** — the permissioned blockchain network operating with no AI-assisted adaptive mechanism.

**Proposed system** — the identical environment with the AI-assisted adaptive mechanism enabled.

**Workload profiles**

- Low transaction workload
- Medium transaction workload
- High transaction workload
- Congested workload
- Sudden workload-spike scenario (where feasible)

Baseline and proposed systems are evaluated under **matched** conditions: identical workloads, hardware, blockchain configuration, experiment duration, transaction inputs, repetition count, and measurement procedure.

## Metrics

**System performance**

- Average / median transaction latency
- P95 / P99 latency
- Throughput
- Transaction success and failure rate
- Queue length
- CPU and memory utilization
- Confirmation / finality time (where measurable)

**AI performance** — dependent on the final AI task; may include prediction accuracy, decision latency, and compute cost.

**Analysis**

- Statistical comparison with confidence intervals where appropriate
- Error and failure-case analysis
- Ablation / sensitivity analysis (should-have)

## Repository Structure

```text
.
├── README.md               # Research project overview and reproduction instructions
├── .gitignore              # Research-grade ignore rules (excluding raw data & model weights)
├── requirements.txt        # Pinned Python package dependencies
├── pyproject.toml          # PEP 517/621 project configuration and pytest setup
├── LICENSE                 # MIT License
├── data/                   # Data layer: raw, interim, processed datasets & manifests
│   ├── README.md           # Acquisition, licensing, and dataset registration manual
│   ├── raw/                # Immutable original datasets (HFBTP, MMBPD, external)
│   ├── interim/            # Intermediate transformations
│   ├── processed/          # ML-ready feature matrices
│   └── manifests/          # Formal metadata and provenance manifests
├── configs/                # Reproducible YAML experiment and model configurations
│   ├── datasets/           # Dataset loading and split configurations
│   ├── experiments/        # End-to-end experiment pipeline specs
│   └── models/             # ML model architectures and hyperparameters
├── notebooks/              # Sequential research notebooks (01_audit to 05_comparison)
├── src/                    # Source package: fabric_perf
│   └── fabric_perf/
│       ├── data/           # Dataset loaders implementing BaseDatasetLoader
│       ├── features/       # Feature engineering & transformers
│       ├── models/         # Performance prediction models (regression & ensembles)
│       ├── evaluation/     # Group/configuration-aware splitters and metrics
│       ├── controllers/    # Static, reactive, and predictive controllers
│       └── utils/          # Reproducibility seeds, logging, helpers
├── tests/                  # Unit and integration test suite
├── experiments/            # Experiment artifacts (results, figures, run logs)
├── fabric-testbed/         # Controlled local Hyperledger Fabric network & Caliper setup
└── docs/                   # Academic documentation and research protocols
    ├── research_plan.md    # Phased research plan and milestone map
    ├── dataset_catalog.md  # Multi-dataset registry and provenance records
    ├── methodology.md      # Mathematical formulation of the 3 control paradigms
    └── evaluation_protocol.md # Leakage-safe evaluation & splitting protocols
```

## Git Branch Strategy (No `dev` Branch)

To ensure linear, reproducible research history, we avoid long-lived unstructured development branches:

- **`main`**: Always contains stable, reproducible code, verified documentation, and passed test suites.
- **Short-Lived Feature Branches**: Created from `main` for one specific milestone, tested, and merged directly back into `main`:
  - `feature/dataset-audit`: Audit and validate HFBTP & MMBPD raw schemas.
  - `feature/eda`: Exploratory data analysis and response surfaces.
  - `feature/evaluation-protocol`: Leakage-safe splitting implementations.
  - `feature/baseline-models`: Linear regression and decision tree baselines.
  - `feature/model-training`: Advanced models and hyperparameter tuning.
  - `feature/cross-dataset-validation`: Cross-dataset evaluation on MMBPD.
  - `feature/controllers`: Static, reactive, and predictive controller implementations.
  - `feature/fabric-testbed`: Controlled local Fabric experiments.
  - `feature/research-report`: Final tables, figures, and publication deliverables.

## Getting Started

### 1. Environment Setup (Local Laptop)

```powershell
# Open Windows PowerShell in repository directory
cd d:\Reseach_Model\AI-Assisted-Permissioned-Blockchain-Architecture-

# Create virtual environment
python -m venv .venv

# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Install package in editable mode with development dependencies
pip install -e .[dev]
```

### 2. Verify Installation & Test Suite

```powershell
pytest
```

## Tech Stack

| Layer | Technology (provisional) |
|---|---|
| Blockchain | Hyperledger Fabric (or comparable permissioned framework) |
| Backend / API | Go |
| AI / ML | Python, scikit-learn |
| Benchmarking | Hyperledger Caliper (or comparable) |
| Monitoring | Prometheus / Grafana |
| Storage | PostgreSQL |
| Orchestration | Docker / Docker Compose |
| API protocols | REST and/or gRPC where justified |

Technology choices are finalized based on research requirements, not implementation convenience or portfolio appeal.

## Research Deliverables

- [ ] Literature review
- [ ] Research-gap analysis
- [ ] Final research question and hypothesis
- [ ] System architecture
- [ ] Permissioned blockchain testbed
- [ ] Workload-generation pipeline
- [ ] Baseline implementation
- [ ] AI-assisted mechanism
- [ ] Experimental evaluation pipeline
- [ ] Statistical analysis and error analysis
- [ ] Results tables and plots
- [ ] Reproducible repository
- [ ] Research paper

## Research Integrity

This project adheres to the following principles:

- No fabricated results or datasets
- No unsupported novelty or production claims
- Negative or inconclusive results are reported honestly
- All synthetic data is explicitly labeled as synthetic
- Limitations are documented explicitly, not omitted

## Roadmap

| Phase | Focus |
|---|---|
| 1 | Literature review & research-gap analysis |
| 2 | Testbed & workload-generation pipeline |
| 3 | Baseline implementation & instrumentation |
| 4 | AI task definition & adaptive mechanism |
| 5 | Controlled experiments across workload profiles |
| 6 | Statistical analysis, error analysis, write-up |

Approximate total timeline: **12 weeks**.

## Limitations

- Results are based on synthetic, not real-world, transaction data.
- The simulated consortium is small relative to real cross-border payment networks.
- Findings are scoped to the tested blockchain configuration and workload profiles and should not be generalized without further study.

## Contributors

- [Aryan Prasad](https://github.com/AryanPrasad-5)
- Anuj Patel

## Contributing

This is primarily a solo/academic research project. Issues and suggestions are welcome via GitHub Issues. If you'd like to propose a change, please open an issue to discuss it before submitting a pull request.

## License

This project is licensed under the [MIT License](LICENSE).

## Citation

If you reference this work, please cite it as:

```bibtex
@misc{ai-adaptive-permissioned-blockchain,
  title  = {AI-Assisted Adaptive Performance in Permissioned Blockchain Networks},
  author = { Aryan Prasad  and  Anuj Patel},
  year   = {2026},
  note   = {Undergraduate research project},
  url    = {https://github.com/AryanPrasad-5/AI-Assisted-Permissioned-Blockchain-Architecture-}
}
```
