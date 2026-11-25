"""X1 (2026-11-25): block-size decision-headroom follow-up to X0. A NEW, separately versioned package.

X1 is an EXPLORATORY follow-up. X0 (closed_loop_v2, protocol LPC-v1) compared B=10 with B=40 and found no crossover. X1 asks
whether a SMALLER block size (B=2, B=5) gives a rate-dependent ordering against B=10 and enough headroom for a controller to matter.

Nothing in closed_loop/ (v1) or closed_loop_v2/ is edited: X1 imports from them (stage builder, analyzer rows, runner) and adds only
what the new protocol needs.

  protocol_x1.py   pure python: frozen constants and the pre-registered rules (classification, rung verdicts, menu, headroom, gate)
  stage_x1.py      numpy (Windows, via v2 stage helpers): the X1 stage (9 static runs, arm.json compatible with the UNCHANGED runner)
  decide_x1.py     Windows: evidence -> rung table -> decision (GO_LIVE_DESIGN / STOP_NO_HEADROOM / STOP_NOT_ESTABLISHED)
  freeze_protocol_x1.py  writes the machine-readable protocol file generated from the same code
  ops/             WSL launch scripts and gates (owner authorization, source freeze, preflight, verification, continuation)
Nothing here starts WSL, Docker or Fabric when imported or tested.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.dont_write_bytecode = True

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_VERSION = "closed_loop_x1 0.1 (2026-11-25)"
SEGMENT_S = 60


def ensure_paths() -> None:
    from Fabric_Live_Testbed.closed_loop import ensure_paths as base
    base()
