"""P4 historical Flashbots replay, Phase 1: OFFLINE implementation only.

Status labels used throughout this package:
  IMPLEMENTED / OFFLINE_TESTED / LIVE_UNTESTED / PROPOSED / OWNER_DECISION_REQUIRED.

Nothing here starts WSL, Docker, Fabric or Caliper, submits a transaction, or touches the configuration actuator.
Existing components are REUSED BY IMPORT and are never edited:
  src/fabric_perf/replay/*            window extraction, burst statistics, trace hash checks
  scripts/e1/e1_la2_render.py         Caliper network/connection-profile rendering, run plan
  scripts/e1/e1_quicklook_p1b.py      frozen P1b driver (gates, recorder, argv builders)
  fabric-testbed/caliper/workloads/e1_trace_replay.js   ms-offset workload with dispatch log

This directory is a new top-level folder on purpose: it is not part of any snapshot source group
(repo_src, repo_scripts, repo_tests, repo_data, repo_results, ...), so it adds no S1 source drift.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Importing the frozen e1 scripts must not drop .pyc files into their directories (nothing existing may change).
sys.dont_write_bytecode = True

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_VERSION = "p4_replay 0.1 (2026-11-14, Phase 1 offline)"


def ensure_paths() -> None:
    """Make the existing `fabric_perf` package and the frozen e1 scripts importable. Only sys.path is touched."""
    for path in (REPO_ROOT / "src", REPO_ROOT / "scripts" / "e1"):
        text = str(path)
        if text not in sys.path:
            sys.path.insert(0, text)
