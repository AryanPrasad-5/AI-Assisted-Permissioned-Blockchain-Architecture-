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
- **Source / Reference**: Contributed by the **LearningChain** paper (IEEE TSC); collected with Hyperledger Fabric 2.3 + Hyperledger Caliper 0.5.0, 3 organizations x 3 peers, orderers {3, 5, 7, 9}. Formal citation entry still pending in the planning workspace's literature audit.
- **License**: Check repository license prior to distribution.
- **Storage Location**: `data/raw/hfbtp/HFBTP.csv` (988,672 bytes)
- **Checksums (recomputed 2026-10-15)**: MD5 `E9275C49A2D7DC1F2A4015B6D8B28482`, SHA-256 `CE295D6451787FEFCFE2101579238CDA4BEDE8539BC302E6A482AA4E5782BD5E` — both match the prior research-campaign record.
- **Dimensions (verified)**: 24,687 rows x 8 columns; 0 missing values; 0 duplicate rows; 0 duplicate configuration cells. Grid 39 x 159 x 4 = 24,804 cells with **117 absent (99.528% coverage)**.
- **Data Collection Environment**:
  - Blockchain Framework: Hyperledger Fabric 2.3
  - Consensus: Raft
  - Workload Tool: Hyperledger Caliper 0.5.0
- **Temporal Nature**: **Static parameter grid** — one observation per `(Set Transaction Arrival Rate, Block Size, Orderers)` cell; **no timestamps, zero replicates**. Row order is design order, not time.
- **Target Variables** (exact CSV headers):
  - `Throughput` (tx/s, range 9.8-197.3)
  - `Avg Latency` (seconds, confirmed — range 0.08-2.67 s)
- **Key Features / Predictors** (exact CSV headers; all available at decision time):
  - `Set Transaction Arrival Rate` — offered workload (tx/s)
  - `Block Size` — Fabric `MaxMessageCount` configuration
  - `Orderers` — ordering-node count
- **Excluded columns**:
  - `Actual Transaction Arrival Rate` — measured during the run; Corr(Throughput)=0.985; leakage risk → excluded from features.
  - `Min Latency`, `Max Latency` — post-execution measurements → excluded from features and targets.
- **Target Comparability Notes**:
  - Fixed hardware benchmark. Cannot be directly pooled with other datasets without standardizing units and target scales.
- **Audit Status**: **Audited & Validated 2026-10-15** — see `docs/hfbtp_audit_report.md` and `experiments/results/hfbtp_audit_report.json`.

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
