"""Shared helpers for the A0h tests (2026-11-21). Synthetic values only; nothing here is a recommended or approved setting."""

from __future__ import annotations

import copy
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
for entry in (str(REPO), str(REPO / "scripts" / "e1"), str(REPO / "src")):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from tests import a0f_support as f  # noqa: E402,F401
from tests import a0g_support as g  # noqa: E402,F401
from fabric_perf.controllers import captured_history as ch  # noqa: E402
from fabric_perf.controllers import stage3_shadow_runner as runner  # noqa: E402

A0H_DIR = REPO / "Integration" / "system_integration" / "a0h_2026-11-21"
DAY = "2026-09-21"
DAY_START = datetime(2026, 9, 21, tzinfo=timezone.utc)
SERIES_09_21 = REPO / "data" / "processed" / "flashbots" / "series_2026-09-21_1s.csv.gz"
SERIES_10_12 = REPO / "data" / "processed" / "flashbots" / "series_2026-10-12_1s.csv.gz"
SERIES_09_21_SHA = "2d54edd894dd7ccda67d9df81de1a753d7b4cef4b6da0271f5fd418d2a98ffb6"
SERIES_10_12_SHA = "e0ce980226c381c039d3afbc4095f341d82a1b1104a7e902b3a9bb46e6601822"
SUCCESSOR_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0h.json"
A0G_MANIFEST = g.SUCCESSOR_MANIFEST


def iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def at(seconds: int) -> datetime:
    """The UTC moment ``seconds`` into the synthetic day."""
    return DAY_START + timedelta(seconds=seconds)


def source(day: str = DAY, source_id: str = "synthetic-test-day") -> ch.CapturedSource:
    return ch.CapturedSource(source_id=source_id, utc_day=day)


def binned(counts=None, *, day: str = DAY, base: int = 20) -> ch.BinnedDay:
    """A synthetic day of positive counts (never zero) unless ``counts`` (a dict {second: count|None} or a full list) overrides it."""
    values = [base + (i % 7) for i in range(ch.SECONDS_PER_DAY)]
    if isinstance(counts, dict):
        for second, value in counts.items():
            values[second] = value
    elif counts is not None:
        values = list(counts)
    return ch.BinnedDay(source(day), tuple(values), {"input": "synthetic", "duplicates_dropped": 0,
                                                    "duplicate_handling": "none (synthetic)"})


def history(counts=None, **kwargs) -> ch.CapturedHistory:
    return ch.CapturedHistory(binned(counts), **kwargs)


def policy_h60(**telemetry) -> dict:
    """A /2 policy whose predictive horizon is the frozen forecast's 60 s."""
    doc = g.policy_v2(**telemetry)
    doc["predictive"]["horizon_seconds"] = 60
    return doc


def captured_fixture(*, start_s: int = 12 * 3600, steps=None, forecast=55.0, day_start: datetime = DAY_START) -> dict:
    doc = f.fixture(steps=steps if steps is not None else f.steady(3))
    doc["start_utc"] = (day_start + timedelta(seconds=start_s)).isoformat()
    doc["fixture_forecast_tps"] = forecast
    return doc


def run_captured(tmp_path: Path, captured, *, steps=None, start_s: int = 12 * 3600, forecast=55.0, policy_doc=None, name="cap",
                 fixture_doc=None, wall_clock_steps=None, transform=None):
    """Write the three inputs and run Mode A with a captured history. Returns (manifest, rows, out_dir)."""
    inputs_dir = tmp_path / f"in_{name}"
    inputs_dir.mkdir(exist_ok=True)
    doc = fixture_doc if fixture_doc is not None else captured_fixture(start_s=start_s, steps=steps, forecast=forecast)
    p = f.write(inputs_dir / "params.json", f.params())
    c = f.write(inputs_dir / "policy.json", policy_doc if policy_doc is not None else policy_h60())
    x = f.write(inputs_dir / "fixture.json", doc)
    inputs = runner.load_inputs(p, c, x)
    out = tmp_path / f"out_{name}"
    manifest = runner.run_fixture_shadow(inputs, out, captured_history=captured, _wall_clock_steps=wall_clock_steps,
                                         _frame_transform=transform)
    return manifest, f.rows_of(out), out


def predictive_rows(rows: list) -> list:
    return [r for r in rows if r["controller"] == "predictive"]
