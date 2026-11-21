"""Shared fixtures for the A0m tests (2026-11-21). Synthetic values only; nothing here is a recommended or approved setting, and nothing here ran Caliper,
Fabric, Docker or a network. Node runs only the offline driver ``tests/e1/js/a0m_generated_run.js``."""

from __future__ import annotations

import copy
import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
for _entry in (str(REPO), str(REPO / "scripts" / "e1"), str(REPO / "src")):
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

from tests import a0e_support as e   # noqa: E402

import e1_a0m_dry_render as dr   # noqa: E402

CHANNEL = "e1-offline.a0m"
RUN_ID = "a0m-test-001"
DRIVER = REPO / "tests" / "e1" / "js" / "a0m_generated_run.js"

GOOD_A0M = {
    "schema": "a0m_render_params/1",
    "selected_workload": "e1_create_asset_issue_logged/1",
    "selected_recorder": "e1_per_tx_recorder_v3",
    "worker_count": 1,
    "issue_log": {"clock_domain": "caliper-worker-host:node_date_now", "tick_interval_ms": 1000, "fsync_policy": "tick"},
    "recorder_health": {"source_id": "a0m-test-recorder", "collector_id": "a0m-test-collector", "evidence_origin": "synthetic_fixture",
                        "heartbeat_interval_ms": 1000, "checkpoint_every_heartbeats": 60},
}


def a0e_params(**overrides) -> dict:
    doc = e.params(**{"run_id": RUN_ID, "channel": CHANNEL, **overrides})
    if "cell" not in overrides:
        doc["cell"]["warmup_s"] = 30                       # two rounds: a warm-up round and the measured round
        doc["cell"]["duration_s"] = 60
    if "ledger_collector" not in overrides:
        doc["ledger_collector"]["max_polls"] = 130         # the A0e contract needs the collector to span the planned round
    return doc


def a0m_params(**overrides) -> dict:
    doc = copy.deepcopy(GOOD_A0M)
    for key, value in overrides.items():
        if value is e.DROP:
            doc.pop(key, None)
        else:
            doc[key] = value
    return doc


def write(path: Path, doc: dict) -> Path:
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return path


def render(tmp: Path, *, a0e=None, a0m=None, render_id="r1", channel=CHANNEL, out_name="out"):
    """One real dry render into ``tmp/out_name/render_id``. Returns (root, manifest)."""
    p1 = write(tmp / f"{out_name}_a0e.json", a0e if a0e is not None else a0e_params())
    p2 = write(tmp / f"{out_name}_a0m.json", a0m if a0m is not None else a0m_params())
    manifest = dr.dry_render(p1, p2, tmp / out_name, render_id, channel=channel)
    return tmp / out_name / render_id, manifest


def clone(root: Path, dest: Path) -> Path:
    """A throw-away copy of a rendered directory, for tests that damage it."""
    shutil.copytree(root, dest)
    return dest


def wired(root: Path, manifest: dict) -> Path:
    return Path(root) / manifest["wired_v3_dir"]


def read_json(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def run_driver(wired_dir: Path, out_dir: Path, *, round_index: int | None = None, submits: int = 5) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = ["node", str(DRIVER), str(wired_dir), str(out_dir)] + ([] if round_index is None else [str(round_index), str(submits)])
    if round_index is None and submits != 5:
        cmd = ["node", str(DRIVER), str(wired_dir), str(out_dir), str(len(read_json(wired_dir / "benchmark.yaml")["test"]["rounds"]) - 1), str(submits)]
    done = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=120)
    if done.returncode != 0:
        raise AssertionError(f"driver failed: {done.stderr[-800:]}")
    return json.loads(done.stdout.strip().splitlines()[-1])
