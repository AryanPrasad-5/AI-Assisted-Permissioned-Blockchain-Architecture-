"""Live runner entry for v2 (WSL, stdlib only). It is the UNCHANGED closed_loop.runner with one substitution: the policy factory
(closed_loop.runner.build_policy -> policies_v2.build_policy_v2), which adds the predictive_live arm and delegates every other arm to
the v1 factory. All gates stay: refuses without --execute and an owner-signed authorization (closed_loop.authorization), a verified
stage, a dedicated e1-* channel, an unused run id, preflight (containers, >=1,500 MB, disk), native-verified actuation.

  python3 -B -m Fabric_Live_Testbed.closed_loop_v2.runner_v2 --stage <stage> --run-id cx01 --authorization <owner file> \
      --out-root <dir> [--execute]
"""

from __future__ import annotations

import sys

from . import ensure_paths

ensure_paths()

from Fabric_Live_Testbed.closed_loop import runner as base  # noqa: E402

from .policies_v2 import build_policy_v2  # noqa: E402

base.build_policy = build_policy_v2

main = base.main
execute = base.execute

if __name__ == "__main__":
    sys.exit(main())
