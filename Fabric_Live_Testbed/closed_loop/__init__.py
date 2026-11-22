"""Closed-loop Static / Reactive / Predictive experiment on the live Fabric testbed (2026-11-22).

Pipeline: registered Flashbots window (historical ms arrival offsets) -> one Caliper benchmark with one replay-rate round per
60 s decision segment -> the per-tx recorder writes each round at round end -> the runner's control loop reads that round,
asks the arm's controller for the next segment's MaxMessageCount, and (only if it changes) applies it through the existing
e1_config_actuator (native verification + owner broadcast authorization + independent verification) while Caliper continues.

Split by where it runs:
  stage.py      Windows/offline (numpy, p4_replay): renders the per-run Caliper workspaces and arm files with SHA256SUMS
  segments.py   pure python: window -> segments (shared by stage and tests)
  policies.py   stdlib: Static, Reactive (repo ReactiveController), Predictive (stored decisions of the repo PredictiveController)
  telemetry.py  stdlib: recorder round CSV/manifest -> observation
  authorization.py  stdlib: owner experiment authorization (key=value, same parser as the actuator)
  runner.py     WSL live runner (stdlib only); refuses without --execute and a valid owner authorization
  analyze.py    stdlib: run-level and segment-level measurements, reconciliation, per-block configuration evidence
  compare.py    Windows/offline: cross-arm tables and figures
Nothing in this package starts WSL, Docker or Fabric when imported or tested.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.dont_write_bytecode = True

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_VERSION = "closed_loop 0.1 (2026-11-22)"
SEGMENT_S = 60


def ensure_paths() -> None:
    for path in (REPO_ROOT, REPO_ROOT / "src", REPO_ROOT / "scripts" / "e1"):
        text = str(path)
        if text not in sys.path:
            sys.path.insert(0, text)
