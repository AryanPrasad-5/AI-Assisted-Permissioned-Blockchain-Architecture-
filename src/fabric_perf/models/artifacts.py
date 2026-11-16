"""Reproducible HFBTP model artifacts and inference.

This module deliberately keeps training and inference separate from the
evaluation notebooks.  Artifacts contain one estimator per target, the exact
feature whitelist, and the raw-dataset checksum used to train them.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Mapping

import joblib
import pandas as pd
import sklearn
from sklearn.ensemble import RandomForestRegressor

from fabric_perf.data.hfbtp import HFBTPDatasetLoader
from fabric_perf.models.baseline import (
    FEATURE_COLUMNS,
    TARGETS,
    add_engineered_features,
    build_pipeline,
    separate_features_targets,
)


ARTIFACT_VERSION = 1


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def train_and_save_hfbtp(
    data_dir: Path,
    output_dir: Path,
    model_name: str = "random_forest",
) -> Dict[str, Any]:
    """Fit separate throughput/latency models on the verified raw dataset.

    This is a production/inference artifact builder, not an evaluation
    routine.  Generalization claims must continue to use the approved split
    workflow in ``baseline.py`` and the evaluation protocol.
    """
    loader = HFBTPDatasetLoader(data_dir)
    frame = loader.load_raw()
    loader.validate(frame)
    frame = add_engineered_features(frame)
    raw_path = loader._find_raw_file()
    metadata: Dict[str, Any] = {
        "artifact_version": ARTIFACT_VERSION,
        "dataset": loader.dataset_id,
        "dataset_filename": raw_path.name,
        "dataset_sha256": _sha256(raw_path),
        "rows": int(len(frame)),
        "features": list(FEATURE_COLUMNS),
        "targets": list(TARGETS),
        "model_name": model_name,
        "sklearn_version": sklearn.__version__,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    for target in TARGETS:
        X, y = separate_features_targets(frame, target=target)
        estimator = build_pipeline(model_name)
        estimator.fit(X, y)
        joblib.dump(estimator, output_dir / f"{target.lower().replace(' ', '_')}.joblib")
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    return metadata


def load_hfbtp_artifact(artifact_dir: Path, target: str):
    """Load one target estimator after validating its artifact metadata."""
    metadata = json.loads((artifact_dir / "metadata.json").read_text(encoding="utf-8"))
    if metadata.get("artifact_version") != ARTIFACT_VERSION:
        raise ValueError("Unsupported HFBTP artifact version")
    if metadata.get("features") != FEATURE_COLUMNS or metadata.get("targets") != TARGETS:
        raise ValueError("Artifact feature/target schema does not match this package")
    if metadata.get("dataset") != "hfbtp" or metadata.get("dataset_sha256") != "CE295D6451787FEFCFE2101579238CDA4BEDE8539BC302E6A482AA4E5782BD5E":
        raise ValueError("Artifact dataset identity does not match verified HFBTP")
    if metadata.get("sklearn_version") != sklearn.__version__:
        raise ValueError("Artifact sklearn version differs from installed sklearn; inference refused")
    if target not in TARGETS:
        raise ValueError(f"Unknown target {target!r}; expected one of {TARGETS}")
    estimator = joblib.load(artifact_dir / f"{target.lower().replace(' ', '_')}.joblib")
    if list(getattr(estimator, "feature_names_in_", ())) != FEATURE_COLUMNS:
        raise ValueError("Estimator feature ordering differs from artifact schema")
    if metadata.get("model_name") == "random_forest":
        model = getattr(estimator, "named_steps", {}).get("model", estimator)
        if not isinstance(model, RandomForestRegressor) or model.n_features_in_ != len(FEATURE_COLUMNS) or model.n_outputs_ != 1:
            raise ValueError("Artifact does not contain the declared single-target HFBTP random forest")
    return estimator, metadata


def predict_hfbtp(artifact_dir: Path, frame: pd.DataFrame) -> pd.DataFrame:
    """Predict both targets from decision-time-safe raw feature columns."""
    out = pd.DataFrame(index=frame.index)
    engineered = add_engineered_features(frame)
    for target in TARGETS:
        estimator, metadata = load_hfbtp_artifact(artifact_dir, target)
        expected = metadata["features"]
        if expected != FEATURE_COLUMNS:
            raise ValueError("Artifact feature schema does not match this package")
        out[target] = estimator.predict(engineered[expected])
    return out
