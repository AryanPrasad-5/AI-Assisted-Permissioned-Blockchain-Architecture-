#!/usr/bin/env python3
"""Actuator entry point for protocol M2-v1: the FROZEN e1_config_actuator with the versioned range policy ACT-RANGE-M2-v1 applied in-process.

Identical command line to e1_config_actuator.py. It refuses to run if the frozen actuator's SHA-256 differs from the pinned value, and
refuses targets outside the menu {2, 5, 10}. Every other safety property (owner broadcast authorization, channel selection, native
verification, independent verification, fence, cooldown) is the frozen actuator's own and is not touched.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_actuator_range_m2 as policy  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    import e1_config_actuator as actuator
    policy.check_cli(args)
    policy.apply_range(actuator)
    return actuator.main(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except policy.PolicyError as exc:
        print(f"REFUSED by {policy.POLICY_ID}: {exc}", file=sys.stderr)
        sys.exit(2)
