# Jupyter Notebooks Workflow Guide

This directory houses structured exploration notebooks numbered sequentially according to research progression:

```text
notebooks/
├── 01_dataset_audit.ipynb          # Step 1: File integrity, schema checks, missingness, duplicate analysis
├── 02_eda.ipynb                    # Step 2: Target distributions, pairplots, response surfaces
├── 03_feature_analysis.ipynb       # Step 3: Correlation analysis, feature importance, mutual info
├── 04_baseline_models.ipynb        # Step 4: Linear baselines and configuration-aware split evaluation
└── 05_model_comparison.ipynb       # Step 5: Ensembles, non-linear models, residual error diagnosis
```

### Reproducibility Best Practices for Notebooks:
1. **Always Set Random Seeds** at the top cell:
   ```python
   from fabric_perf.utils import seed_everything
   seed_everything(42)
   ```
2. **Never Hardcode Absolutes Paths**:
   Use relative paths anchored to the repository root.
3. **No Direct Model Training Without Pre-Flight Audit**:
   `01_dataset_audit.ipynb` must be executed and reviewed before running modeling experiments.
