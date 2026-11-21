"""Shared helpers for the A0g tests (2026-11-21). Synthetic values only; nothing here is a recommended or approved setting."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
for entry in (str(REPO), str(REPO / "scripts" / "e1"), str(REPO / "src")):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from tests import a0f_support as f  # noqa: E402,F401  (re-exported helpers: params, fixture, step, steady, write, sha, tree, by_frame)
from fabric_perf.controllers import stage3_shadow_runner as runner  # noqa: E402

A0G_DIR = REPO / "Integration" / "system_integration" / "a0g_2026-11-21"
POLICY_V2_FILE = A0G_DIR / "fixtures" / "controller_policy_v2.json"
SUCCESSOR_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0g.json"
A0E_MANIFEST = f.CANDIDATE_MANIFEST
MODEL_PINS = REPO / "Integration" / "system_integration" / "offline_replay_inputs.json"


def policy_v2(**telemetry) -> dict:
    """A fresh /2 policy; keyword arguments replace keys of its ``telemetry`` section."""
    doc = copy.deepcopy(json.loads(POLICY_V2_FILE.read_text(encoding="utf-8")))
    doc["telemetry"].update(telemetry)
    return doc


def run(tmp_path: Path, *, steps=None, policy_doc=None, fixture_doc=None, params_doc=None, name="run", transform=None, hook=None,
        predictive_mode="fixture"):
    """Write the three inputs into ``tmp_path`` and run Mode A with a /2 policy by default. Returns (manifest, rows, out_dir)."""
    inputs_dir = tmp_path / f"in_{name}"
    inputs_dir.mkdir(exist_ok=True)
    doc = fixture_doc if fixture_doc is not None else f.fixture(steps=steps if steps is not None else f.steady(3))
    p = f.write(inputs_dir / "params.json", params_doc if params_doc is not None else f.params())
    c = f.write(inputs_dir / "policy.json", policy_doc if policy_doc is not None else policy_v2())
    x = f.write(inputs_dir / "fixture.json", doc)
    inputs = runner.load_inputs(p, c, x)
    out = tmp_path / f"out_{name}"
    manifest = runner.run_fixture_shadow(inputs, out, _frame_hook=hook, _frame_transform=transform, predictive_mode=predictive_mode)
    return manifest, f.rows_of(out), out
