"""Inference with the frozen T2-D0 HFBTP artifacts. Predict only: never fit, never save.

Refuses to run if the artifact hash differs from its sidecar. HFBTP inputs only.
"""
from __future__ import annotations

import hashlib
import json

import joblib
import numpy as np

from cv_common import AVG_LAT, BLOCK, MODEL_DIR, ORDERERS, SET, TPUT

FEATURES = [SET, BLOCK, ORDERERS]


def _load(target_dir: str):
    path = MODEL_DIR / target_dir / "model.joblib"
    sidecar = json.loads((MODEL_DIR / target_dir / "model.meta.json").read_text(encoding="utf-8"))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != sidecar["artifact_sha256"]:
        raise ValueError(f"{path}: hash mismatch; refusing to load")
    return joblib.load(path)["estimator"], digest


def heavy_flatness(h) -> dict:
    summary = {}
    for name, target in (("throughput", TPUT), ("avg_latency", AVG_LAT)):
        estimator, digest = _load(name)
        predicted = estimator.predict(h[FEATURES].to_numpy())
        base = h.assign(p=predicted)[h[SET] == 145].set_index([BLOCK, ORDERERS])["p"]
        heavy = h.assign(p=predicted)[h[SET] >= 165]
        reference = heavy.set_index([BLOCK, ORDERERS]).index.map(base).to_numpy()
        summary[name] = {
            "artifact_sha256_prefix": digest[:16],
            "max_abs_difference_from_level_145": float(np.abs(heavy["p"].to_numpy() - reference).max()),
            "heavy_mae": float((heavy["p"] - heavy[target]).abs().mean()),
            "heavy_mean_prediction": float(heavy["p"].mean()),
        }
    return summary
