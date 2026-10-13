# Dataset Catalog & Provenance Register

This document maintains the canonical catalog of all datasets evaluated within this project.

---

## Dataset Catalog Template

When registering a new dataset, copy this section and fill out all entries:

```markdown
### [Dataset ID] - Full Name

- **Identifier**: `<dataset_id>`
- **Source / Reference**: `<Paper link or repository URL>`
- **License**: `<License type>`
- **Storage Location**: `data/raw/<dataset_id>/`
- **Data Collection Environment**:
  - Blockchain Framework: `<e.g., Hyperledger Fabric v2.4>`
  - Consensus / Orderer: `<e.g., Raft>`
  - State Database: `<e.g., CouchDB or LevelDB>`
  - Workload Tool: `<e.g., Hyperledger Caliper>`
  - Hardware / Node Topology: `<e.g., 4 peers, 1 orderer, 8 vCPUs>`
- **Temporal Nature**: `<Static Grid | Dynamic Time-Series | Discrete Event Log>`
- **Replications**: `<Number of repeated trials per configuration>`
- **Target Variables**:
  - Target 1: `<Name>` (`<Units>`) - `<Definition>`
  - Target 2: `<Name>` (`<Units>`) - `<Definition>`
- **Key Features / Predictors**:
  - `<Feature 1>`: `<Type, Unit, Description>`
  - `<Feature 2>`: `<Type, Unit, Description>`
- **Target Comparability Notes**:
  - `<Notes on whether targets and features can be compared directly to other datasets>`
- **Audit Status**: `<Pending | In Progress | Audited & Validated>`
```

---

## Registered Datasets

### 1. HFBTP — Hyperledger Fabric Benchmark & Tuning Platform Dataset

- **Identifier**: `hfbtp`
- **Source / Reference**: Literature benchmark dataset for Hyperledger Fabric tuning
- **License**: Pending verification in `feature/dataset-audit`
- **Storage Location**: `data/raw/hfbtp/`
- **Data Collection Environment**:
  - Blockchain Framework: Hyperledger Fabric
  - Consensus: Raft
  - State Database: LevelDB / CouchDB (to be confirmed from raw schema)
  - Workload Tool: Benchmark generator
- **Temporal Nature**: **Static parameter grid** (observations represent individual benchmark executions with no timestamp traces).
- **Replications**: **0 replicated trials per configuration cell** (initial finding; pending verification during formal audit).
- **Target Variables**:
  - `throughput`: Transaction Throughput (Transactions Per Second, TPS)
  - `avg_latency`: Average Latency (seconds or milliseconds; must verify units during audit)
- **Key Features / Predictors**:
  - Transaction send rate (`tx_rate`)
  - Block batch size (`batch_size` / `MaxMessageCount`)
  - Block batch timeout (`batch_timeout`)
  - Endorsement policy / organization count
- **Target Comparability Notes**:
  - Fixed hardware benchmark. Cannot be directly pooled with other datasets without standardizing units and target scales.
- **Audit Status**: **Pending Audit** (Scheduled as the immediate next step in `feature/dataset-audit`).

---

### 2. MMBPD — Multi-scenario Permissioned Blockchain Dataset

- **Identifier**: `mmbpd`
- **Source / Reference**: Multi-scenario permissioned blockchain benchmark repository
- **License**: Pending verification
- **Storage Location**: `data/raw/mmbpd/`
- **Data Collection Environment**:
  - Blockchain Framework: Hyperledger Fabric
  - Workload Tool: Caliper / Scenario Harness
- **Temporal Nature**: Benchmark scenario executions.
- **Replications**: Pending verification.
- **Target Variables**:
  - `throughput`: Transaction Throughput
  - `avg_latency`: Transaction Latency
- **Target Comparability Notes**:
  - **Independent evaluation set only.** Used exclusively to evaluate zero-shot or few-shot transfer of models trained on HFBTP.
- **Audit Status**: **Pending Acquisition & Audit**.

---

### 3. Local Testbed Telemetry (Future)

- **Identifier**: `fabric_local_testbed`
- **Source / Reference**: Generated in-situ via `fabric-testbed/` on local hardware.
- **License**: Proprietary to this research / MIT.
- **Storage Location**: `fabric-testbed/telemetry/`
- **Data Collection Environment**:
  - Local laptop (Docker, Docker Compose, Hyperledger Fabric).
- **Temporal Nature**: **Real-time dynamic time-series** with Prometheus telemetry traces.
- **Target Comparability Notes**:
  - Directly models time-varying dynamics, enabling true evaluation of reactive and predictive control loops.
- **Audit Status**: **Planned for Milestone 8**.
