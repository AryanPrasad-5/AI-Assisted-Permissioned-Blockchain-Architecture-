"""The live-path interlock of the Phase 2 orchestration. OFFLINE.

Phase 1 closes the live path by TYPE: `driver._guard_live` refuses an instance of `p1b.Backend` while `driver.LIVE_EXECUTION_ENABLED` is False. A class that does the same job without
inheriting from `p1b.Backend` would pass that test. So while the live path is closed, the series orchestration, the preflight and the keep-awake helper accept ONLY a backend that
declares itself an offline fake (`OFFLINE_FAKE = True`, which the test backend does and no live code does). Enabling the live path is a reviewed change of `driver.py` under a
separate owner authorisation; it is not done here.
"""

from __future__ import annotations

from . import driver as dr

MARKER = "OFFLINE_FAKE"


def require_offline_backend(be) -> None:
    """Refuse before any call. A real p1b.Backend (or subclass) is refused by the Phase 1 guard; anything else must declare MARKER = True while the live path is closed."""
    dr._guard_live(be)
    if not dr.LIVE_EXECUTION_ENABLED and getattr(be, MARKER, False) is not True:
        raise dr.LiveExecutionRefused(f"refused: the live path is closed and this backend does not declare {MARKER} = True (type {type(be).__name__})")
