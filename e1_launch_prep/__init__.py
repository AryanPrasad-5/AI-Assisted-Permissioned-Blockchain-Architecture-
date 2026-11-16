"""Experiment 1 launch preparation tools (master execution, 2026-11-15). OFFLINE.

What this package is: deterministic, create-exclusive generators and read-only verifiers for the launch artifacts that are mechanical
(source manifest, pins file, S1 drift list, staged dry-run tree) plus a post-run evidence manifest. Nothing here edits p4_replay or any
reviewed file, writes a series approval or an entry authorization, signs anything, or starts WSL, Docker, Fabric or Caliper.

This directory is a new top-level folder on purpose, as `p4_replay/` was: it is not part of any snapshot source group, so it adds no S1 drift.
"""

from __future__ import annotations

import sys

sys.dont_write_bytecode = True
