"""Versioned actuator range policy ACT-RANGE-M2-v1 (2026-11-25). Pure python, no I/O at import.

The frozen actuator (scripts/e1/e1_config_actuator.py) accepts MaxMessageCount targets only in DEFAULT_MIN_B..DEFAULT_MAX_B = 10..800 (module
constants read at call time by BroadcastAuthorization.load and updated_config). The three-controller comparison (protocol M2-v1) needs the
menu {2, 5, 10}. This policy changes NOTHING in the frozen file: it pins the frozen file's SHA-256 and, only inside a process that asks for
it, sets the two module constants to the M2 range. Any other process (the v1/v2 runners, the X1 tools) still sees 10..800.

Used by scripts/e1/e1_config_actuator_m2.py (the actuator subprocess) and Fabric_Live_Testbed/closed_loop_m2/runner_m2.py (the runner process, which
parses the owner authorization through the same module). Changing a constant here is a new dated policy version, never an edit after a run.
"""

from __future__ import annotations

import contextlib
import hashlib
from pathlib import Path

POLICY_ID = "ACT-RANGE-M2-v1 (actuator range policy for protocol M2-v1, 2026-11-25)"
MENU_B = (2, 5, 10)
MIN_B, MAX_B = 2, 10
FROZEN_ACTUATOR = "e1_config_actuator.py"
# SHA-256 of the actuator exactly as it stands in the working tree on 2026-11-25 (uncommitted broadcast-authorization edits included).
FROZEN_ACTUATOR_SHA256 = "ecefe33e8e6a5b3fec2acd1d70fd3ca57845087a789f4083e256df0ea1e5bba0"
RANGE_ATTRS = ("DEFAULT_MIN_B", "DEFAULT_MAX_B")


class PolicyError(RuntimeError):
    pass


def policy_doc() -> dict:
    return {"policy_id": POLICY_ID, "menu_B": list(MENU_B), "min_b": MIN_B, "max_b": MAX_B,
            "frozen_actuator": FROZEN_ACTUATOR, "frozen_actuator_sha256": FROZEN_ACTUATOR_SHA256,
            "mechanism": "in-process module constants DEFAULT_MIN_B/DEFAULT_MAX_B set only by processes that call apply_range(); the frozen "
                         "file is never edited; targets outside the menu are refused here and by the owner's broadcast_targets",
            "native_verifier_and_independent_config_verifier": "contain no lower bound of 10 (grepped 2026-11-25); the live check of "
                                                                "2/5 targets is the separate ACTUATION_CHECK run"}


def sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def check_actuator_module(module, *, expected_sha256: str = FROZEN_ACTUATOR_SHA256) -> str:
    path = Path(module.__file__)
    if path.name != FROZEN_ACTUATOR:
        raise PolicyError(f"actuator module is {path.name}, expected {FROZEN_ACTUATOR}")
    digest = sha256_file(path)
    if digest != expected_sha256:
        raise PolicyError(f"frozen actuator SHA-256 {digest} differs from the pinned {expected_sha256}: the range policy refuses")
    for attr in RANGE_ATTRS:
        if not hasattr(module, attr):
            raise PolicyError(f"actuator module has no {attr}")
    return digest


def apply_range(module, *, expected_sha256: str = FROZEN_ACTUATOR_SHA256) -> dict:
    """Pin check, then set the module constants. Returns the previous values (for restore_range)."""
    check_actuator_module(module, expected_sha256=expected_sha256)
    previous = {a: getattr(module, a) for a in RANGE_ATTRS}
    module.DEFAULT_MIN_B, module.DEFAULT_MAX_B = MIN_B, MAX_B
    return previous


def restore_range(module, previous: dict) -> None:
    for a, v in previous.items():
        setattr(module, a, v)


@contextlib.contextmanager
def range_applied(module, *, expected_sha256: str = FROZEN_ACTUATOR_SHA256):
    previous = apply_range(module, expected_sha256=expected_sha256)
    try:
        yield
    finally:
        restore_range(module, previous)


def check_cli(argv: list[str]) -> None:
    """Refuse a command line that disagrees with the policy (the runner always passes --min-b 2 --max-b 10)."""
    def value(flag):
        return int(argv[argv.index(flag) + 1]) if flag in argv else None
    for flag, want in (("--min-b", MIN_B), ("--max-b", MAX_B)):
        got = value(flag)
        if got is not None and got != want:
            raise PolicyError(f"{flag} {got} differs from the policy value {want}")
    for flag in ("--target-b", "--approved-target", "--approved-start-b"):
        got = value(flag)
        if got is not None and got not in MENU_B:
            raise PolicyError(f"{flag} {got} is outside the M2 menu {list(MENU_B)}")
