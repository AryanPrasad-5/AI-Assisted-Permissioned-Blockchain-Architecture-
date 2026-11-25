"""Closed-loop M2 (2026-11-25): the live Static vs Reactive vs Predictive comparison on one menu {2, 5, 10}. A NEW package.
closed_loop/, closed_loop_v2/ and closed_loop_x1/ and their frozen hashes are untouched; M2 imports from them.

  protocol_m2.py   pure python: frozen constants, the design derived from X1 only (surface, reactive thresholds, window and k), SESOI rules
  stage_m2.py      numpy (Windows): the comparison stage (4 arms x replicates) and the ACTUATION_CHECK stage; arm.json compatible with the runner
  runner_m2.py     WSL: the unchanged closed_loop runner with the v2 policy factory and the M2 actuator wrapper (range policy ACT-RANGE-M2-v1)
  analysis_m2.py   stdlib: per-run records, per-arm summary, pairwise comparison, decision agreement, forecast accuracy, the final table
  final_comparison_m2.py  CLI: evidence directory -> final comparison table (csv / md / json)
  ops/             check_auth_m2.py, verify_m2.py (independent), launch_m2.sh, run_phase_detached_m2.sh, freeze/copy/print helpers
Nothing here starts WSL, Docker or Fabric when imported or tested. Live use needs the owner's signed AUTH files (the assistant never writes them).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.dont_write_bytecode = True

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_VERSION = "closed_loop_m2 0.1 (2026-11-25)"
SEGMENT_S = 60


def ensure_paths() -> None:
    from Fabric_Live_Testbed.closed_loop import ensure_paths as base
    base()
