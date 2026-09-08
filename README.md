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

```
.
├── docs/                    # Literature review, research-gap analysis, write-up
├── blockchain/               # Permissioned blockchain network configuration
├── workload-gen/              # Synthetic transaction & workload generation
├── api-gateway/               # Transaction ingestion layer
├── monitoring/                # Metrics collection (Prometheus/Grafana configs)
├── ai-model/                  # AI task definition, training, inference
├── adaptive-controller/        # Adaptive decision mechanism
├── experiments/                # Experiment configs and run scripts
├── analysis/                   # Statistical analysis, plots, notebooks
├── results/                    # Raw logs and generated results
├── docker-compose.yml
└── README.md
```

> Structure is provisional and will evolve as the architecture is finalized.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Go 1.21+
- Python 3.10+
- `make` (optional, for convenience commands)

### Setup

```bash
# Clone the repository
git clone https://github.com/AryanPrasad-5/AI-Assisted-Permissioned-Blockchain-Architecture-.git
cd AI-Assisted-Permissioned-Blockchain-Architecture-

# Start the permissioned blockchain network and supporting services
docker compose up -d

# Install Python dependencies for AI/analysis components
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Reproducing Experiments

```bash
# Run the baseline (no AI adaptation) under a given workload profile
python experiments/run_experiment.py --system baseline --workload medium

# Run the AI-assisted system under the same workload profile
python experiments/run_experiment.py --system ai-assisted --workload medium

# Generate statistical comparison and plots
python analysis/compare_results.py --baseline results/baseline_medium.json --proposed results/ai_medium.json
```

Every experiment run is defined by an explicit configuration file (workload profile, blockchain config, repetitions, hardware notes) stored alongside its results in `experiments/configs/` to support independent reproduction.

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
