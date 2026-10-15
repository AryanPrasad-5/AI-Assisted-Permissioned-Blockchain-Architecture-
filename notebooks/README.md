# Jupyter Notebooks Workflow Guide

This directory houses the sequential notebooks for the HFBTP stage:

```text
notebooks/
├── 01_dataset_audit.ipynb          # Step 1: checksums, schema, quality, grid coverage (writes experiments/results/hfbtp_audit_report.json)
├── 02_eda.ipynb                    # Step 2: distributions, load-response, correlations, trade-off (figures -> experiments/figures/)
└── 03_hfbtp_baseline_model.ipynb   # Step 3: registered splits + Dummy/Linear/RF baselines (writes experiments/results/hfbtp_baseline_*.csv,json)
```

Execution order matters: run 01 first, then 02, then 03. Each notebook was executed against the real `data/raw/hfbtp/HFBTP.csv` (committed outputs included).

### Reproducibility best practices in these notebooks

1. **Seeds:** every notebook calls `from fabric_perf.utils import seed_everything; seed_everything(42)` before anything random.
2. **No absolute paths:** all paths are anchored to the repository root discovered from `Path.cwd()`.
3. **Reuse, don't duplicate:** loaders, splits, and model pipelines come from `src/fabric_perf` — the notebooks only orchestrate and visualize.
4. **No test-set feedback:** notebook 03 reports all registered splits and never revises models after seeing test numbers.

To re-execute all three from the repository root:

```powershell
pip install -r requirements.txt
jupyter nbconvert --to notebook --execute --inplace notebooks/01_dataset_audit.ipynb notebooks/02_eda.ipynb notebooks/03_hfbtp_baseline_model.ipynb
```
