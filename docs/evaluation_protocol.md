# Evaluation Protocol: Leakage-Safe Validation Methodology

## 1. Principles of Experimental Rigor

In performance benchmarking datasets, simple random train/test splitting (`train_test_split(shuffle=True)`) creates significant **data leakage**. When identical or neighbouring configuration settings appear in both the training and test sets, models memorize specific configuration cells rather than learning the underlying response surface. This inflates evaluation scores and fails under real deployment.

---

## 2. Splitting Strategies

### Strategy A: Configuration-Aware / Group-Aware Split (Recommended for HFBTP)
- **Mechanism**: Group observations by experimental configuration (e.g., combination of `batch_size` and `batch_timeout`) using `GroupKFold` or `GroupShuffleSplit`.
- **Purpose**: Tests whether the model generalizes to **unseen configurations**.
- **Rule**: Entire parameter combinations must reside exclusively in either the train set or test set.

### Strategy B: Out-of-Range Workload Split (Stress Generalization)
- **Mechanism**: Train on low-to-medium workload levels (e.g., transaction rates $r \le 500$ TPS); test on high/congested workload levels ($r > 500$ TPS).
- **Purpose**: Evaluates extrapolation ability near saturation points.

### Strategy C: Cross-Dataset Independent Evaluation (Generalization to MMBPD)
- **Mechanism**: Train exclusively on Dataset A (`HFBTP`), evaluate without any fine-tuning on Dataset B (`MMBPD`).
- **Rule**: Standardize column units and scales before evaluation.

---

## 3. Evaluation Metrics

For both regression targets ($T$: Throughput, $L$: Latency):

| Metric | Formula | Purpose |
|---|---|---|
| **MAE** | $\frac{1}{n} \sum |y - \hat{y}|$ | Robust, directly interpretable error in physical units (TPS or ms). |
| **RMSE** | $\sqrt{\frac{1}{n} \sum (y - \hat{y})^2}$ | Penalizes large prediction errors severely (critical for latency spikes). |
| **$R^2$** | $1 - \frac{\sum (y - \hat{y})^2}{\sum (y - \bar{y})^2}$ | Proportion of variance explained by the model relative to baseline mean. |
| **MAPE** | $\frac{100\%}{n} \sum \left|\frac{y - \hat{y}}{y}\right|$ | Percentage error; handles differing target scales across datasets. |

### Operational Performance Metrics (for Controllers)
When evaluating controllers (Static, Reactive, Predictive):
- **SLA Violation Rate**: Percentage of time latency exceeds maximum acceptable threshold ($L > L_{\max}$).
- **Average Delivered Throughput**: Total committed transactions divided by experiment duration.
- **Control Stability Index**: Frequency of configuration switching (measures controller oscillation).

---

## 4. Reproducibility Checklist

Every experiment must record:
- Random Seed (Default: `42`)
- Exact Git commit hash
- Python environment lockfile / versions
- Dataset manifest checksums (SHA256)
- Preprocessing scaler parameters fit **only** on the training split (preventing test set leakage).
