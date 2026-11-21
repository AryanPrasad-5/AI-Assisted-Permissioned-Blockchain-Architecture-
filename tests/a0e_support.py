"""Shared fixtures for the A0e tests (2026-11-21). Synthetic values only; nothing here is a recommended or approved setting."""

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

ENV_FILE = REPO / "fabric-testbed" / "network" / "env-e1-caliper-benchmark.json"
NETWORK_FILE = REPO / "fabric-testbed" / "network" / "network-e1-recovery.json"
CHANNEL = "e1-offline.a0e"                  # synthetic; the owner has not approved any validation channel
RUN_ID = "a0e-test-001"

# Test values only. They are placeholders chosen so the checks have something to compare; none is a recommended value.
GOOD_PARAMS = {
    "schema": "prelive_params/1",
    "run_id": RUN_ID,
    "channel": CHANNEL,
    "cell": {"max_message_count": 40, "batch_timeout": "2s", "preferred_max_bytes": 10485760, "rate_tps": 4.0,
             "workload": "create", "warmup_s": 0, "duration_s": 5},
    "recorder": {"live_flush_rows": 20, "live_flush_interval_ms": 250},
    "ledger_collector": {"poll_interval_s": 1.0, "max_polls": 20},
    "telemetry_adapter": {"window_s": 4.0, "recorder_latency": "derive_from_timestamps"},
    "stage3": {"max_consecutive_abstentions": 3, "max_initialization_frames": 2},
    "telemetry_sources": {"prometheus_endpoints": ["orderer1=http://localhost:9443/metrics"]},
    "environment": {"renderer_env_path": str(ENV_FILE), "network_json_path": str(NETWORK_FILE)},
}


def params(**overrides) -> dict:
    """A fresh deep copy of the good parameters; ``overrides`` replace top-level keys; a value of KEY_DROPPED removes one."""
    doc = copy.deepcopy(GOOD_PARAMS)
    for key, value in overrides.items():
        if value is DROP:
            doc.pop(key, None)
        else:
            doc[key] = value
    return doc


class _Drop:
    def __repr__(self) -> str:
        return "DROP"


DROP = _Drop()


def sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_params(path: Path, doc: dict) -> Path:
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return path


def tree_hashes(directory: Path) -> dict:
    return {p.relative_to(directory).as_posix(): sha(p) for p in sorted(Path(directory).rglob("*")) if p.is_file()}


# ---------------------------------------------------------------- synthetic clock samples
MS = 1_000_000


def clock_sample(*, offset_ns: int = 0, rtt_ns: int = 2 * MS, resolution_ns: int = 1_000, base_ns: int = 1_800_000_000_000_000_000,
                 mono_rtt_ns: int | None = None, mono_base_ns: int = 5_000_000_000) -> dict:
    """One synthetic round trip. The remote clock is ``offset_ns`` ahead of the reference; the remote reading is taken at the
    middle of the round trip and truncated to the resolution, as an ideal probe would."""
    before = base_ns
    after = base_ns + rtt_ns
    remote = (before + after) // 2 + offset_ns
    remote -= remote % resolution_ns
    mono = rtt_ns if mono_rtt_ns is None else mono_rtt_ns
    return {"ref_wall_before_ns": before, "remote_wall_ns": remote, "ref_wall_after_ns": after,
            "ref_mono_before_ns": mono_base_ns, "ref_mono_after_ns": mono_base_ns + mono, "remote_resolution_ns": resolution_ns}


CLOCK_PARAMS = {"tolerance_ms": 10.0, "min_valid_samples": 3, "max_wall_monotonic_disagreement_ms": 1.0}
