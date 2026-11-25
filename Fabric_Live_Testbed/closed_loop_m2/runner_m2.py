"""Live runner entry for M2 (WSL, stdlib only). It is the UNCHANGED closed_loop.runner with exactly three substitutions, all scoped to this
entry point (applied on entry, restored on exit, so importing it changes nothing for other code):

  1. policy factory  closed_loop.runner.build_policy -> closed_loop_v2.policies_v2.build_policy_v2 (adds predictive_live; menu comes from arm.json)
  2. actuator tool   e1_config_actuator.py -> e1_config_actuator_m2.py (the frozen actuator under range policy ACT-RANGE-M2-v1) with --min-b 2 --max-b 10
  3. actuator range  the in-process module constants DEFAULT_MIN_B/DEFAULT_MAX_B of the frozen actuator module (pin-checked by SHA-256), so that
                     the owner authorization (broadcast_targets=2,5,10) parses through the same code the actuator uses

All gates stay: refuses without --execute and an owner-signed authorization, a verified stage, a dedicated e1-* channel, an unused run id,
preflight (containers, >= 1,500 MB, disk), native-verified actuation.

  python3 -B -m Fabric_Live_Testbed.closed_loop_m2.runner_m2 --stage <stage> --run-id cc01 --authorization <owner file> --out-root <dir> [--execute]
"""

from __future__ import annotations

import contextlib
import sys

from . import ensure_paths

ensure_paths()

import e1_actuator_range_m2 as policy  # noqa: E402
import e1_config_actuator as frozen_actuator  # noqa: E402

from Fabric_Live_Testbed.closed_loop import runner as base  # noqa: E402
from Fabric_Live_Testbed.closed_loop_v2.policies_v2 import build_policy_v2  # noqa: E402

TOOL = "e1_config_actuator_m2.py"


def actuator_argv_m2(ctx, channel: str, command: list[str], out_dir) -> list[str]:
    return [ctx.python, "-B", ctx.tool(TOOL), "--network", str(ctx.network), "--channel", channel,
            "--orderer-admin-msp", str(ctx.orderer_admin_msp), "--out-dir", str(out_dir),
            "--command-timeout-s", str(ctx.actuator_command_timeout_s), "--commit-timeout-s", str(ctx.actuator_commit_timeout_s),
            "--cooldown-s", "20", "--min-b", str(policy.MIN_B), "--max-b", str(policy.MAX_B), *command]


@contextlib.contextmanager
def m2_environment(*, expected_actuator_sha256: str = policy.FROZEN_ACTUATOR_SHA256):
    previous = policy.apply_range(frozen_actuator, expected_sha256=expected_actuator_sha256)
    saved = (base.build_policy, base.actuator_argv)
    base.build_policy, base.actuator_argv = build_policy_v2, actuator_argv_m2
    try:
        yield
    finally:
        base.build_policy, base.actuator_argv = saved
        policy.restore_range(frozen_actuator, previous)


def execute(ctx, world, *, now=None, expected_actuator_sha256: str = policy.FROZEN_ACTUATOR_SHA256):
    with m2_environment(expected_actuator_sha256=expected_actuator_sha256):
        return base.execute(ctx, world, now=now)


def main(argv=None, *, expected_actuator_sha256: str = policy.FROZEN_ACTUATOR_SHA256) -> int:
    try:
        with m2_environment(expected_actuator_sha256=expected_actuator_sha256):
            return base.main(argv)
    except policy.PolicyError as exc:
        print(f"REFUSED by {policy.POLICY_ID}: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
