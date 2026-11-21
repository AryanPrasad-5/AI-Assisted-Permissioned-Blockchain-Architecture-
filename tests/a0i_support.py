"""Shared helpers for the A0i tests (2026-11-21). Synthetic values only; nothing here is a recommended or approved setting."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tests import a0h_support as h  # noqa: F401  (re-exports the A0f/A0g/A0h helpers as h.f / h.g)
from fabric_perf.controllers import abstention_accounting as acc  # noqa: E402,F401
from fabric_perf.controllers import captured_history as ch  # noqa: E402,F401
from fabric_perf.controllers import collector_health as chj  # noqa: E402,F401
from fabric_perf.controllers import real_inference_smoke as smoke  # noqa: E402,F401
from fabric_perf.controllers import workload_coverage as wc  # noqa: E402,F401

REPO = h.REPO
SOURCE_ID = h.source().source_id                      # "synthetic-test-day"
RUN_ID = "run-a0i"
POLICY = wc.CoveragePolicy(max_heartbeat_gap_s=7200.0)     # a synthetic placeholder: hourly heartbeats keep the journals short
DAY_START = h.DAY_START
DAY_MS = int(DAY_START.timestamp() * 1000)
A0I_DIR = REPO / "Integration" / "system_integration" / "a0i_2026-11-21"
SUCCESSOR_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0i.json"


class ScriptClock:
    """A scripted collector clock, in seconds relative to the synthetic UTC day's midnight."""

    def __init__(self, second: float = -0.5):
        self.second = second

    def __call__(self) -> datetime:
        return DAY_START + timedelta(seconds=self.second)

    def at(self, second: float) -> "ScriptClock":
        self.second = second
        return self


def at(second: float) -> datetime:
    return DAY_START + timedelta(seconds=second)


def new_journal(tmp_path: Path, *, name: str = "journal.jsonl", source_id: str = SOURCE_ID, run_id: str = RUN_ID, start: int = 0,
                clock_second: float = -0.5, instance: str = "inst-1", first_event_seq=None, channel_id=None, started: bool = True):
    """A started journal on a scripted clock. Returns ``(journal, clock, path)``."""
    clock = ScriptClock(clock_second)
    path = tmp_path / name
    journal = chj.CollectorHealthJournal(path, source_id=source_id, collector_id="col", run_id=run_id, channel_id=channel_id, clock=clock,
                                         collector_instance=instance)
    if started:
        journal.start(observation_start_utc=at(start), first_event_seq=first_event_seq, coverage_scope=wc.SCOPE_SOURCE_STREAM)   # A0k: heartbeats certify zeros only under a declared scope
    return journal, clock, path


def beat(journal, clock: ScriptClock, second: float, watermark: float | None = None, *, events: int = 0, **kwargs):
    clock.at(second)
    return journal.heartbeat(at(second if watermark is None else watermark), events_received=events, **kwargs)


def covered_journal(tmp_path: Path, *, until: int, name: str = "journal.jsonl", start: int = 0, **kwargs):
    """Hourly heartbeats, then one per second from 12:00:00Z: capture is continuous from ``start`` to ``until`` (day seconds)."""
    journal, clock, path = new_journal(tmp_path, name=name, start=start, **kwargs)
    seconds = [3600 * k for k in range(1, 13) if 3600 * k <= until]
    seconds += [s for s in range(43_201, until + 1)]
    if until < 3600 and until not in seconds:
        seconds.append(until)
    for second in sorted(set(seconds)):
        beat(journal, clock, second)
    return journal, clock, path


def coverage_for(path: Path, *, source_id: str = SOURCE_ID, **kwargs) -> "wc.VerifiedCoverage":
    kwargs.setdefault("policy", POLICY)
    kwargs.setdefault("run_id", RUN_ID)
    return wc.VerifiedCoverage.from_journal_file(path, source_id=source_id, **kwargs)


def forge(rows: list[dict], *, source_id: str = SOURCE_ID, run_id: str = RUN_ID) -> list[dict]:
    """A hash-VALID chain of rows with arbitrary content: what a buggy or dishonest collector could write. ``t_utc`` etc. are used as given."""
    events, previous = [], wc.GENESIS_SHA256
    for index, row in enumerate(rows, 1):
        body = {"schema": wc.JOURNAL_EVENT_SCHEMA, "seq": index, "source_id": source_id, "collector_id": "col", "run_id": run_id,
                "channel_id": None, "dataset_id": None, "collector_instance": "inst-1", **row}
        body["prev"] = previous
        body["sha256"] = wc.event_digest(body, previous)
        previous = body["sha256"]
        events.append(body)
    return events


def forged_coverage(rows: list[dict], **kwargs) -> "wc.VerifiedCoverage":
    return wc.VerifiedCoverage(source_id=SOURCE_ID, mode=wc.MODE_ATTESTED, events=forge(rows), policy=POLICY, run_id=RUN_ID, **kwargs)


def iso(second: float) -> str:
    return wc.iso(at(second))


def start_row(second: float = -0.5, observation: int = 0, **extra) -> dict:
    return {"kind": "start", "t_utc": iso(second), "observation_start_utc": iso(observation), "timestamp_provenance": "forged",
            "coverage_scope": wc.SCOPE_SOURCE_STREAM, **extra}              # A0k: a forged journal declares the scope the A0i tests assume unless a test says otherwise


def heartbeat_row(second: float, watermark: float | None = None, *, events: int = 0, health: str = "healthy", errors: int = 0, **extra) -> dict:
    return {"kind": "heartbeat", "t_utc": iso(second), "observed_through_utc": iso(second if watermark is None else watermark),
            "events_received": events, "last_event_seq": None, "health": health, "errors_since_last": errors, **extra}


def captured(coverage, counts=None, **kwargs) -> "ch.CapturedHistory":
    return ch.CapturedHistory(h.binned(counts), coverage=coverage, **kwargs)


def sha(path) -> str:
    import hashlib
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 22), b""):
            digest.update(chunk)
    return digest.hexdigest()
