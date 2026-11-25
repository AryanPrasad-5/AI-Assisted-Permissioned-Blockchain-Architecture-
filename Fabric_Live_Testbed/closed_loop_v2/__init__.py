"""Closed-loop v2 (2026-11-25): live predictive control under scaled load. A NEW package: closed_loop/ and its frozen hashes are
untouched; v2 imports from it and adds only what the approved protocol needs.

  forecast.py        pure python: causal EWMA (alpha 0.005, H=60 s, >=600 s history), identical to the frozen recurrence
  predictor.py       pure python: Fabric performance predictor interpolated from the X0 crossover scan (no HFBTP, no extrapolation)
  scaling.py         pure python: per-minute rate traces -> evenly spaced Caliper rounds (the documented rate-trace scaling)
  protocol.py        pure python: the frozen constants and the pre-registered decision rules (crossover, k, reactive thresholds)
  window_selection.py numpy (Windows): deterministic window ranking from historical arrivals only
  policies_v2.py     stdlib: LivePredictivePolicy (repo PredictiveController + causal forecast + local predictor) and policy factory
  stage_v2.py        numpy (Windows): X0 and M stages (arm.json compatible with the unchanged closed_loop runner/analyzer)
  runner_v2.py       WSL: the unchanged closed_loop runner with the v2 policy factory (refuses without owner authorization)
  analysis_v2.py     stdlib: X0 surface extraction, forecast accuracy, decision regret
Nothing here starts WSL, Docker or Fabric when imported or tested.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.dont_write_bytecode = True

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_VERSION = "closed_loop_v2 0.1 (2026-11-25)"
SEGMENT_S = 60


def ensure_paths() -> None:
    from Fabric_Live_Testbed.closed_loop import ensure_paths as base
    base()
