# Data Management & Registration Guidelines

This directory manages all datasets for the research project: **Machine Learning-Based Performance Prediction and Adaptive Control in Hyperledger Fabric**.

---

## 1. Directory Structure

```text
data/
├── README.md               # Acquisition, licensing, and registration instructions
├── raw/                    # Immutable original datasets as downloaded/received
│   ├── hfbtp/              # Hyperledger Fabric Benchmark Throughput & Latency Dataset
│   ├── mmbpd/              # Multimodal/Multi-scenario Blockchain Performance Dataset
│   └── external/           # Any auxiliary benchmarks or external reference logs
├── processed/              # Feature-engineered, train/test matrices ready for ML
└── manifests/              # Formal provenance manifests (YAML/JSON) for data audit
```

---

## 2. Strict Data Integrity Safeguards

1. **Raw Data Immutability**:
   Files placed in `data/raw/` must NEVER be edited manually or overwritten. Any transformations must be handled via reproducible Python scripts or Data Pipelines outputting to `data/processed/`.
2. **Never Commit Raw Data to Git**:
   Raw datasets and large processed arrays are tracked locally only (enforced via `.gitignore`).
3. **No Blind Data Pooling**:
   Different datasets (e.g., HFBTP vs MMBPD) originate from distinct testbed topologies, workloads, and measurement tools. **Do not combine them into a single table without verifying target definitions, units, and experimental conditions.**
4. **Dataset Provenance Records**:
   Every dataset placed in `data/raw/` must have a corresponding manifest record in `data/manifests/` and an entry in `docs/dataset_catalog.md`.

---

## 3. Dataset Registration Procedure

When adding a dataset to the repository:

1. **Create Subfolder in `data/raw/`**:
   e.g., `data/raw/<dataset_name>/`.
2. **Download / Place Files**:
   Save files in their native format (e.g., `.csv`, `.json`, `.parquet`).
3. **Record Metadata in `data/manifests/<dataset_name>.yaml`**:
   Document the following required fields:
   - `dataset_id`: Unique identifier (e.g., `hfbtp`)
   - `version`: Dataset version or date retrieved
   - `source_url`: URL or paper citation
   - `license`: Open source license or redistribution terms
   - `storage_hash`: SHA256 checksum of raw files for integrity verification
   - `target_variables`: Primary targets (e.g., `throughput_tps`, `latency_avg_ms`)
   - `input_features`: Workload and configuration parameters
   - `experimental_type`: Static parameter grid vs. dynamic time-series trace
4. **Implement Loader in `src/fabric_perf/data/`**:
   Inherit from `BaseDatasetLoader` to provide clean, standardized access.

---

## 4. Current Target Datasets

### A. HFBTP (Hyperledger Fabric Benchmark & Tuning Platform)
- **Role**: Primary development dataset for initial data auditing, EDA, feature analysis, and baseline regression.
- **Nature**: Configuration/workload benchmark grid. *Note: Verify during audit whether individual configuration cells have repeated trials or temporal sequences.*
- **Expected Targets**: Transaction Throughput (TPS), Average Latency (s or ms).
- **Location**: `data/raw/hfbtp/`

### B. MMBPD (Multi-scenario Permissioned Blockchain Dataset)
- **Role**: Independent evaluation dataset to test cross-dataset generalization.
- **Location**: `data/raw/mmbpd/`

### C. Future Datasets / Testbed Telemetry
- **Role**: Validating learned relationships on custom laptop-based Hyperledger Fabric network runs.
- **Location**: `data/raw/external/` or `fabric-testbed/telemetry/`
