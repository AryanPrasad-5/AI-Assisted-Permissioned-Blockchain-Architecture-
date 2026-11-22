"""Shared helpers for the A0p tests (2026-11-22). EVERYTHING here is SYNTHETIC: a real Node writer run under a fake Caliper driver with scripted clocks, forged sidecars, scripted spools
and scripted bridge clocks. Nothing is a measurement of Caliper, Fabric or a real clock, and nothing starts a network connection, Docker, WSL or a model.

The Node driver (``tests/e1/js/a0p_timed_round.js``) is a local interpreter running the REAL timed workload and writer; tests that use it need ``node`` and skip without it."""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import pytest

from tests import a0j_support as j
from tests.a0j_support import FAST, SOURCE_ID, SyntheticRecorder, at, bridge, short_recorder   # noqa: F401
from tests.a0l_support import DOMAIN, RUN, WORKER, Scripted, SyntheticIssueLog, ilr, wc          # noqa: F401
from tests.a0o_support import NL, REPO, lines_of, write_lines                                       # noqa: F401
from tests.a0o_support import scripted_log as _scripted_log

from fabric_perf.controllers import issue_log_timing as ilt                # noqa: E402,F401
from fabric_perf.controllers import recorder_health_incremental as rhi     # noqa: E402,F401
from fabric_perf.controllers import timing_durable as td                   # noqa: E402,F401
from fabric_perf.controllers import sourcecount_reader as sr                # noqa: E402,F401
from fabric_perf.controllers import timing_sampler as ts                   # noqa: E402,F401

DRIVER = REPO / "tests" / "e1" / "js" / "a0p_timed_round.js"
NODE = shutil.which("node")
DAY_MS_A0P = 1_795_000_000_000          # an arbitrary epoch-ms day start for the node driver (scripted)
BRIDGE_DAY = datetime(2026, 11, 22, 9, 0, 0, tzinfo=timezone.utc)


def need_node() -> None:
    if NODE is None:
        pytest.skip("node is not available")


def run_timed_node(tmp: Path, **cfg) -> dict:
    """Run the real timed workload/writer under the fake Caliper driver. Returns the driver's JSON result (paths, status, error)."""
    need_node()
    tmp.mkdir(parents=True, exist_ok=True)
    config = {"runId": "a0p-fx-001", "dayMs": DAY_MS_A0P, "until": 6, "txEvery": 1, "tickEveryS": 2, "tickIntervalMs": 2000, **cfg}
    path = tmp / "driver_config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    done = subprocess.run([NODE, str(DRIVER), str(tmp), str(path)], capture_output=True, text=True, timeout=120, check=False)
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def verify(node: dict, **kw) -> "ilt.TimingEvidence":
    return ilt.verify_timing(Path(node["log"]), Path(node["timing"]), **kw)


def rows_of(path: Path) -> list[dict]:
    return [json.loads(line) for line in lines_of(Path(path))]


def chain(rows: list[dict]) -> list[bytes]:
    """Recompute prev/sha256 for edited rows (kept canonical) so a sidecar edit can be VALID as a chain and wrong only in its content."""
    prev, out = "0" * 64, []
    for row in rows:
        row = {k: v for k, v in row.items() if k not in ("prev", "sha256")}
        row["prev"] = prev
        row["sha256"] = wc.event_digest(row, prev)
        prev = row["sha256"]
        out.append(wc.canonical(row).encode("ascii"))
    return out


def forge_sidecar(log_path: Path, out: Path, *, dur_us: Callable[[int], int] = lambda rseq: 100, sync_after: Callable[[int, str], bool] = lambda rseq, kind: kind in ("tick", "stop"),
                  sync_dur_us: int = 500, stop: bool = True, mutate: Callable[[list[dict]], None] | None = None, drop_last_write: bool = False) -> Path:
    """A well-formed sidecar for a log, written in Python with scripted microsecond stamps: ``dur_us(rseq)`` is each record's write duration. ``mutate`` may edit the (unchained) rows."""
    records = rows_of(log_path)
    head = records[0]
    rows: list[dict] = [{"schema": ilt.TIMING_SCHEMA, "tseq": 1, "kind": "tstart", "run_id": head["run_id"], "worker_id": head["worker_id"], "log_name": Path(log_path).name,
                         "log_schema": ilt.LOG_SCHEMA, "round_index": head["round_index"], "clock_domain": head["clock_domain"], "timing_writer_version": "forged",
                         "wall_clock_source": "Date.now()", "wall_precision": "ms", "mono_clock_source": "forged", "mono_unit": "us", "mono_origin": "forged",
                         "fsync_policy": "tick", "write_semantics": "forged", "sync_semantics": "forged"}]
    mono, synced, writes, syncs = 10_000_000, 0, 0, 0
    for rec in records:
        start = mono + 20
        end = start + dur_us(rec["rseq"])
        wall = rec["t_ms"] + (start - 10_000_000) // 1000
        rows.append({"schema": ilt.TIMING_SCHEMA, "tseq": len(rows) + 1, "kind": "write", "run_id": rec["run_id"], "worker_id": rec["worker_id"], "target_rseq": rec["rseq"],
                     "target_kind": rec["kind"], "target_sha256": rec["sha256"], "target_t_ms": rec["t_ms"], "target_mono_ms": rec["mono_ms"], "write_start_wall_ms": wall,
                     "write_end_wall_ms": wall + (end - start) // 1000, "write_start_mono_us": start, "write_end_mono_us": end, "write_status": "returned"})
        writes += 1
        mono = end
        if sync_after(rec["rseq"], rec["kind"]):
            s_start = mono + 10
            s_end = s_start + sync_dur_us
            rows.append({"schema": ilt.TIMING_SCHEMA, "tseq": len(rows) + 1, "kind": "sync", "run_id": rec["run_id"], "worker_id": rec["worker_id"], "call": "fsyncSync",
                         "covered_from_rseq": synced + 1, "covered_through_rseq": rec["rseq"], "sync_start_wall_ms": wall + (s_start - start) // 1000,
                         "sync_end_wall_ms": wall + (s_end - start) // 1000, "sync_start_mono_us": s_start, "sync_end_mono_us": s_end, "sync_status": "returned"})
            synced, syncs, mono = rec["rseq"], syncs + 1, s_end
    if drop_last_write:
        last = max(i for i, r in enumerate(rows) if r["kind"] == "write")
        del rows[last:]
        stop = False
        writes = syncs = 0
    if stop:
        rows.append({"schema": ilt.TIMING_SCHEMA, "tseq": len(rows) + 1, "kind": "tstop", "run_id": head["run_id"], "worker_id": head["worker_id"], "log_records": len(records),
                     "writes": writes, "syncs": syncs, "failures": 0, "synced_through_rseq": synced})
    if mutate is not None:
        mutate(rows)
    for number, row in enumerate(rows, 1):
        row["tseq"] = number
    write_lines(out, chain(rows))
    return Path(out)


class BridgeClock:
    """A scripted bridge host: aware wall time and monotonic microseconds, each call advancing by a fixed step. ``set`` moves the wall clock (the monotonic clock keeps counting)."""

    def __init__(self, start: datetime = BRIDGE_DAY, step_ms: float = 1.0, mono_step_us: int = 100):
        self.now, self.step, self.mono_us, self.mono_step = start, timedelta(milliseconds=step_ms), 1_000_000, mono_step_us

    def wall(self) -> datetime:
        self.now += self.step
        return self.now

    def mono(self) -> int:
        self.mono_us += self.mono_step
        return self.mono_us

    def set(self, moment: datetime) -> None:
        self.now = moment


def bridge_config(rec: SyntheticRecorder, tmp: Path, *, name: str = "b", policy=FAST, spool_domain: str = "recorder-host", bridge_domain: str = "bridge-host", **kw) -> "rhi.BridgeConfig":
    return rhi.BridgeConfig(rec.spool_path, tmp / f"{name}.journal.jsonl", tmp / f"{name}.ledger.jsonl", tmp / f"{name}.checkpoint.json", source_id=rec.source_id, run_id=j.RUN,
                            policy=policy, spool_clock_domain=spool_domain, bridge_clock_domain=bridge_domain, **kw)


def create_bridge(cfg, clock: BridgeClock | None = None, **kw):
    clock = clock or BridgeClock()
    return rhi.IncrementalRecorderBridge.create(cfg, wall_clock=clock.wall, mono_us=clock.mono, **kw), clock


def resume_bridge(cfg, clock: BridgeClock | None = None, **kw):
    clock = clock or BridgeClock(start=BRIDGE_DAY + timedelta(hours=1))
    return rhi.IncrementalRecorderBridge.resume(cfg, wall_clock=clock.wall, mono_us=clock.mono, **kw), clock


class SimulatedCrash(BaseException):
    """A process death at a named boundary (a BaseException, so no bridge handler can swallow it)."""


def crash_at(point: str, occurrence: int = 1, torn: int | None = None):
    """A fault hook that kills the bridge the ``occurrence``-th time it reaches ``point``. ``torn`` leaves that many bytes of a journal row behind (point journal_mid_write)."""
    seen = {"n": 0}

    def hook(name: str) -> None:
        if name == point:
            seen["n"] += 1
            if seen["n"] == occurrence:
                if torn is not None:
                    raise rhi._TornWrite(torn)
                raise SimulatedCrash(point)
    return hook


def drain_after_crash(cfg, hook=None) -> tuple[bool, "rhi.IncrementalRecorderBridge"]:
    """Run a bridge to the crash (if any) and return ``(crashed, bridge)``; the caller then resumes with a fresh instance."""
    bridge_obj, _ = create_bridge(cfg, fault=hook)
    try:
        bridge_obj.poll_until_drained()
    except (SimulatedCrash, rhi._TornWrite):
        return True, bridge_obj
    return False, bridge_obj


def whole_file_journal(rec: SyntheticRecorder, tmp: Path, name: str = "whole", policy=FAST) -> bytes:
    bridge.ingest_spool(rec.spool_path, tmp / f"{name}.journal.jsonl", source_id=rec.source_id, policy=policy)
    return (tmp / f"{name}.journal.jsonl").read_bytes()


def scripted_log(tmp: Path, **kw):
    tmp.mkdir(parents=True, exist_ok=True)
    return _scripted_log(tmp, **kw)


def spool_recorder(tmp: Path, **kw) -> SyntheticRecorder:
    """The 20-second FAST fixture, written to disk (start + 20 beats + stop)."""
    tmp.mkdir(parents=True, exist_ok=True)
    rec = short_recorder(tmp, **kw)
    rec.write()
    return rec
