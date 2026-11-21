"""Shared helpers for the A0f tests (2026-11-21). Synthetic values only; nothing here is a recommended or approved setting."""

from __future__ import annotations

import copy
import hashlib
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
E1 = REPO / "scripts" / "e1"
for entry in (str(E1), str(REPO / "src")):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from fabric_perf.controllers import stage3_shadow_runner as runner  # noqa: E402

FIXTURE_DIR = REPO / "Integration" / "system_integration" / "a0f_2026-11-21" / "fixtures"
PARAMS_FILE = FIXTURE_DIR / "params.json"
POLICY_FILE = FIXTURE_DIR / "controller_policy.json"
NOMINAL_FILE = FIXTURE_DIR / "fixture_nominal.json"
DEGRADED_FILE = FIXTURE_DIR / "fixture_degraded.json"
CANDIDATE_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21.json"


def load(path: Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def params(**sections) -> dict:
    """A fresh copy of the example parameters; keyword arguments replace top-level keys."""
    doc = copy.deepcopy(load(PARAMS_FILE))
    doc.update(sections)
    return doc


def policy(**sections) -> dict:
    doc = copy.deepcopy(load(POLICY_FILE))
    doc.update(sections)
    return doc


def fixture(**sections) -> dict:
    doc = copy.deepcopy(load(NOMINAL_FILE))
    doc.update(sections)
    return doc


def step(advance_s: float = 1.0, **kwargs) -> dict:
    return {"advance_s": advance_s, **kwargs}


def write(path: Path, doc) -> Path:
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8", newline="\n")
    return path


def sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tree(directory: Path) -> dict:
    return {p.relative_to(directory).as_posix(): sha(p) for p in sorted(Path(directory).rglob("*")) if p.is_file()}


def run(tmp_path: Path, *, fixture_doc=None, params_doc=None, policy_doc=None, name="run", hook=None, transform=None):
    """Write the three input files into ``tmp_path`` and run Mode A. Returns (manifest, rows, run_dir)."""
    inputs_dir = tmp_path / f"in_{name}"
    inputs_dir.mkdir(exist_ok=True)
    p = write(inputs_dir / "params.json", params_doc if params_doc is not None else params())
    c = write(inputs_dir / "policy.json", policy_doc if policy_doc is not None else policy())
    f = write(inputs_dir / "fixture.json", fixture_doc if fixture_doc is not None else fixture())
    inputs = runner.load_inputs(p, c, f)
    out = tmp_path / f"out_{name}"
    manifest = runner.run_fixture_shadow(inputs, out, _frame_hook=hook, _frame_transform=transform)
    return manifest, rows_of(out), out


def rows_of(out: Path) -> list:
    path = Path(out) / "shadow_decisions.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def by_frame(rows: list) -> dict:
    result: dict = {}
    for row in rows:
        result.setdefault(row["frame"]["index"], {})[row["controller"]] = row
    return result


def steady(n: int, *, latency_ms: float = 100, tx: int = 5) -> list:
    """An initialization step followed by ``n`` ordinary accepted steps."""
    first = step(client={"count": 5, "latency_ms": latency_ms})
    return [first] + [step(client={"count": 5, "latency_ms": latency_ms}, ledger_valid_tx=tx) for _ in range(n)]
