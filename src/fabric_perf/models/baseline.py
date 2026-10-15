"""Baseline regression workflow for the HFBTP stage (research-plan approved scope).

Covers exactly the transparent-baseline ladder authorized for this phase:
    DummyRegressor (median strategy)   — reference floor
    Multiple Linear Regression         — interpretable linear reference (sklearn
                                         LinearRegression on the 8 engineered features)
    RandomForestRegressor              — non-linear ensemble (plan §6 #5)

Scope. Only the three families above are implemented in this repository. The
frozen Phase 1C experiment (research-planning workspace, outside this
repository) pre-registered seven families, including scikit-learn
HistGradientBoostingRegressor, and ran and reported all of them. Integrating
HistGradientBoosting here is a separate decision; its absence from this
smaller ladder is a scope choice, not evidence that the model is unsuitable.
Extending this repository's family list requires a documented change to
docs/evaluation_protocol.md. In the frozen protocol, candidates beyond its
seven pre-registered families (for example quantile models) must be added
through a version bump with a documented reason. Also not implemented here:
quantile models, controller wrapping and hyperparameter search.

Model naming. The "linear" identifier is Multiple Linear Regression (8
Engineered Features): ordinary least squares on the three raw predictors plus
log10(Set), log10(Block), Block/Set, Set*Orderers and Block/Orderers, behind a
StandardScaler. It is not polynomial regression (no powers, no
PolynomialFeatures) and it is not the frozen Phase 1C baseline, which uses the
three raw predictors only (see MODEL_LABELS and FROZEN_PHASE1C_MLR_LABEL).

Leakage discipline (approved protocol §5, contract item 2):
  Pre-execution features  = {Set Transaction Arrival Rate, Block Size, Orderers}
  Forbidden as features   = {Actual Transaction Arrival Rate, Throughput,
                             Avg Latency, Min Latency, Max Latency}
  Derived features below use ONLY the three pre-execution columns.
"""

from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# ---------------------------------------------------------------- features ---#

RAW_FEATURES: List[str] = [
    "Set Transaction Arrival Rate",
    "Block Size",
    "Orderers",
]

TARGETS: List[str] = ["Throughput", "Avg Latency"]

TARGET_UNITS: Dict[str, str] = {
    "Throughput": "tx/s",
    "Avg Latency": "s",
}

BASELINE_MODELS = ("dummy_median", "linear", "random_forest")
RANDOM_SEED = 42

# Identifiers are stable keys in result files; labels are for tables, plots and prose.
MODEL_LABELS: Dict[str, str] = {
    "dummy_median": "Dummy Regressor (median)",
    "linear": "Multiple Linear Regression (8 Engineered Features)",
    "random_forest": "Random Forest Regressor",
}
MODEL_SHORT_LABELS: Dict[str, str] = {
    "dummy_median": "Dummy (median)",
    "linear": "MLR — 8 features",
    "random_forest": "Random Forest",
}
FROZEN_PHASE1C_MLR_LABEL = "Multiple Linear Regression (3 Raw Features, Frozen Phase 1C)"

FEATURE_ENGINEERING_NOTE = (
    "Derived features are deterministic functions of the three pre-execution "
    "columns: log10 of the offered rate and of the block size, the block-size-"
    "per-unit-load ratio, the Set*Orderers product and the Block/Orderers ratio. "
    "No target or post-execution column is used."
)


def add_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add decision-time-safe derived features; returns a new dataframe.

    Every derived column is a pure function of the three raw features, so no
    post-execution quantity can enter a holdout through this path.
    """
    out = df.copy()
    set_col = "Set Transaction Arrival Rate" if "Set Transaction Arrival Rate" in df.columns else "set_rate"
    blk_col = "Block Size" if "Block Size" in df.columns else "block_size"
    ord_col = "Orderers" if "Orderers" in df.columns else "orderers"
    out["log_set_rate"] = np.log10(out[set_col])
    out["log_block_size"] = np.log10(out[blk_col])
    out["block_per_unit_load"] = out[blk_col] / out[set_col]
    out["set_x_orderers"] = out[set_col] * out[ord_col]
    out["block_over_orderers"] = out[blk_col] / out[ord_col]
    return out


FEATURE_COLUMNS: List[str] = (
    RAW_FEATURES
    + [
        "log_set_rate",
        "log_block_size",
        "block_per_unit_load",
        "set_x_orderers",
        "block_over_orderers",
    ]
)


def separate_features_targets(
    df: pd.DataFrame,
    target: str = "Throughput",
) -> Tuple[pd.DataFrame, pd.Series]:
    """Explicit X/y separation against the dataset contract.

    X is built strictly from the whitelisted pre-execution FEATURE_COLUMNS —
    any other column (including post-execution and target columns) present in
    the frame is simply never selected. A loud assertion additionally guards
    the whitelist itself against accidental leakage-bearing entries.
    """
    if target not in TARGETS:
        raise ValueError(
            f"Unknown target {target!r}. Approved targets are {TARGETS}."
        )
    forbidden = ["Actual Transaction Arrival Rate", "Min Latency", "Max Latency"]
    leaked = [c for c in FEATURE_COLUMNS if c in forbidden or c == target]
    if leaked:
        raise ValueError(
            f"Leakage guard failure: feature whitelist contains {leaked}. "
            "Post-execution columns must never enter the design matrix."
        )
    X = df[FEATURE_COLUMNS]
    y = df[target]
    return X, y


def build_pipeline(model_name: str) -> Pipeline:
    """Build a training pipeline for one approved baseline family.

    Only the linear model is scaled; tree and dummy models receive the
    unscaled feature columns. Fitted preprocessing lives inside the
    pipeline, so it is fitted on the training partition only.
    """
    if model_name == "dummy_median":
        model = DummyRegressor(strategy="median")
        return Pipeline([("model", model)])
    if model_name == "linear":
        # Standardize only inside the pipeline; the scaler is fitted
        # exclusively on the training partition when .fit is called and then
        # applied frozen to validation and test partitions.
        model = LinearRegression()
        return Pipeline(
            [
                ("scaler", StandardScaler()),
                ("model", model),
            ]
        )
    if model_name == "random_forest":
        model = RandomForestRegressor(
            n_estimators=200,
            random_state=RANDOM_SEED,
            n_jobs=-1,
        )
        return Pipeline([("model", model)])
    raise ValueError(
        f"Unapproved model family {model_name!r}. This phase authorizes only "
        f"{BASELINE_MODELS}; extending the family list requires a documented "
        "change to the evaluation protocol (docs/evaluation_protocol.md)."
    )


def fit_and_evaluate(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    target: str,
    model_name: str,
    val_name: str = "validation",
) -> Dict[str, Any]:
    """Fit one baseline family on the training partition only, then evaluate
    once each on validation and test. Preprocessing is fitted inside the
    pipeline on training data and applied frozen to val/test."""
    X_train, y_train = separate_features_targets(train_df, target=target)
    X_val, y_val = separate_features_targets(val_df, target=target)
    X_test, y_test = separate_features_targets(test_df, target=target)
    pipeline = build_pipeline(model_name)
    pipeline.fit(X_train, y_train)

    pred_val = pipeline.predict(X_val)
    pred_test = pipeline.predict(X_test)
    return {
        "target": target,
        "unit": TARGET_UNITS[target],
        "model": model_name,
        "features": FEATURE_COLUMNS,
        "train_rows": len(X_train),
        "val_rows": len(X_val),
        "test_rows": len(X_test),
        f"{val_name}_mae": mean_absolute_error(y_val, pred_val),
        f"{val_name}_rmse": float(np.sqrt(mean_squared_error(y_val, pred_val))),
        f"{val_name}_r2": r2_score(y_val, pred_val),
        "test_mae": mean_absolute_error(y_test, pred_test),
        "test_rmse": float(np.sqrt(mean_squared_error(y_test, pred_test))),
        "test_r2": r2_score(y_test, pred_test),
        "test_residual_p50": float(np.median(pred_test - y_test)),
        "test_residual_p95_abs": float(np.percentile(np.abs(pred_test - y_test), 95)),
    }


def run_all_baselines(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    targets: Optional[List[str]] = None,
    model_names: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Run the full authorized baseline ladder over both approved targets."""
    targets = targets or TARGETS
    model_names = model_names or list(BASELINE_MODELS)
    rows = []
    for target in targets:
        for model_name in model_names:
            rows.append(
                fit_and_evaluate(
                    train_df, val_df, test_df, target=target, model_name=model_name
                )
            )
    return rows
