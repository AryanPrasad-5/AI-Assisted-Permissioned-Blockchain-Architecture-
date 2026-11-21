"""Shared helpers for the A0j tests (2026-11-21). EVERYTHING here is SYNTHETIC: a scripted recorder lifecycle on a scripted clock.

``SyntheticRecorder`` writes what ``e1_per_tx_recorder_v3.js`` writes (a ``recorder_health_spool/1`` file and a ``live.submits.csv``) so the
Python side can be tested without Node, Caliper or Fabric. A separate test runs the real v3 recorder under a fake Caliper dispatch and checks
that its spool ingests identically. Nothing here is a measurement.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from itertools import accumulate
from pathlib import Path

from tests import a0i_support as s
from tests.a0i_support import SOURCE_ID, at, ch, wc  # noqa: F401

from fabric_perf.controllers import recorder_coverage as rc  # noqa: E402,F401
from fabric_perf.controllers import recorder_health_bridge as bridge  # noqa: E402,F401

REPO = s.REPO
DAY = s.h.DAY
DAY_START = s.DAY_START
DAY_MS = s.DAY_MS
INSTANCE = "rec:run-a0j:w0:r0"
RUN = "run-a0j"
# SYNTHETIC test policy: hourly declared heartbeats keep the day-long fixtures short. NOT a recommendation.
POLICY = bridge.RecorderHealthPolicy(heartbeat_interval_s=3600, max_heartbeat_gap_s=7200.0, freshness_limit_s=7200.0, clock_tolerance_ms=250.0)
SOURCE_BASIS = "synthetic_independent_source_counter"
# A0k: a source count is believed only with a descriptor that classifies as SOURCE-INDEPENDENT AND INTERVAL-SUITABLE. SYNTHETIC.
SOURCE_DESCRIPTOR = {"counter_id": "synthetic-generator-issue-counter", "kind": "generator_issue_counter", "independent_of_recorder": True,
                     "derived_from": "synthetic_workload_generator_counter", "timestamp_semantics": "generator_issue_time", "resolution": "interval"}


def base_units(second: int) -> int:
    return 20 + (second % 7)


class SyntheticRecorder:
    """A scripted recorder: ``units[sec]`` is what its callback observed (what reaches the live submits file); ``truth[sec]`` is what the
    synthetic SOURCE issued (differs from ``units`` when callbacks are missed)."""

    def __init__(self, tmp: Path, *, name: str = "r0", instance: str = INSTANCE, origin: str = bridge.ORIGIN_SYNTHETIC, source_count: bool = True,
                 declared_interval_ms: int = 3_600_000, scope: str = "recorder_callback", source_id: str = SOURCE_ID, worker: int = 0,
                 round_: int = 0, mono_skew_ms: float = 0.0):
        self.dir = tmp
        self.spool_path = tmp / f"{name}.health.jsonl"
        self.submits_path = tmp / f"{name}.live.submits.csv"
        self.instance, self.origin, self.source_count = instance, origin, source_count
        self.declared_interval_ms, self.scope, self.source_id = declared_interval_ms, scope, source_id
        self.worker, self.round = worker, round_
        self.n = 0
        self.rows: list[dict] = []
        self.units: dict[int, int] = {}
        self.truth: dict[int, int] = {}
        self.obs_start = 0
        self.mono_skew_ms = mono_skew_ms
        self.stopped = False

    # --- the observed world ------------------------------------------------------------------------------------------------
    def traffic(self, seconds, *, zero=(), missed=(), units_fn=base_units) -> "SyntheticRecorder":
        """Source traffic for ``seconds``: ``zero`` seconds have NO arrival at all; ``missed`` seconds had arrivals the callback never saw."""
        for sec in seconds:
            if sec in zero:
                continue
            u = units_fn(sec)
            self.truth[sec] = u
            if sec not in missed:
                self.units[sec] = u
        return self

    def received_through(self, watermark: int) -> int:
        return sum(u for sec, u in self.units.items() if self.obs_start <= sec < watermark)

    def source_through(self, watermark: int) -> int:
        return sum(u for sec, u in self.truth.items() if self.obs_start <= sec < watermark)

    # --- the spool ---------------------------------------------------------------------------------------------------------
    def _row(self, kind: str, t: float, **fields) -> dict:
        self.n += 1
        row = {"schema": bridge.SPOOL_SCHEMA, "n": self.n, "kind": kind, "t_utc": wc.iso(at(t)), "mono_ms": t * 1000.0 + self.mono_skew_ms,
               "source_id": self.source_id, "collector_id": "rec", "collector_instance": self.instance, "run_id": RUN, "worker": self.worker,
               "round": self.round, "label": "synthetic", **fields}
        self.rows.append(row)
        return row

    def start(self, t: float = -0.5, observation: int = 0) -> "SyntheticRecorder":
        self.obs_start = observation
        self._row("start", t, observation_start_utc=wc.iso(at(observation)), timestamp_provenance="synthetic_scripted_clock",
                  coverage_scope=self.scope, evidence_origin=self.origin, recorder_version="synthetic", options_sha256="0" * 64,
                  lifecycle={"initialized_utc": wc.iso(at(t - 0.1)), "subscription_active_utc": wc.iso(at(t)), "window_open_utc": wc.iso(at(observation))},
                  declared_policy={"heartbeat_interval_ms": self.declared_interval_ms, "checkpoint_every_heartbeats": 60},
                  source_count_declared=self.source_count, **({"source_counter": dict(SOURCE_DESCRIPTOR)} if self.source_count else {}))
        return self

    def beat(self, t: float, watermark: float | None = None, *, health: str = "healthy", errors: int = 0, phase: str = "heartbeat",
             received: int | None = None, source: int | None = None, with_source: bool | None = None) -> "SyntheticRecorder":
        wm = int(t if watermark is None else watermark)
        fields = {"observed_through_utc": wc.iso(at(wm)), "events_received": self.received_through(wm) if received is None else received,
                  "last_event_seq": None, "health": health, "errors_since_last": errors, "phase": phase}
        if self.source_count if with_source is None else with_source:
            fields.update(source_events_expected=self.source_through(wm) if source is None else source, source_basis=SOURCE_BASIS,
                          source_independence_claim=True)
        self._row("heartbeat", t, **fields)
        return self

    def error(self, t: float, code: str = "synthetic_fault", affects_from: float | None = None) -> "SyntheticRecorder":
        self._row("error", t, error_code=code, message="synthetic fault", **({} if affects_from is None else {"affects_from_utc": wc.iso(at(affects_from))}))
        return self

    def recovered(self, t: float, resumes: float) -> "SyntheticRecorder":
        self._row("recovered", t, coverage_resumes_at_utc=wc.iso(at(resumes)))
        return self

    def stop(self, t: float, watermark: float | None = None, *, reason: str = "clean") -> "SyntheticRecorder":
        wm = int(t if watermark is None else watermark)
        self._row("stop", t, observed_through_utc=wc.iso(at(wm)), events_received=self.received_through(wm), last_event_seq=None, reason=reason,
                  phase="window_close", final_files_present=True, live_stream_ok=True)
        self.stopped = True
        return self

    def covered(self, until: int, *, step_after: int = 43_200) -> "SyntheticRecorder":
        """Hourly beats up to ``step_after`` then one per second: the same cadence as the A0i fixtures."""
        seconds = [3600 * k for k in range(1, 13) if 3600 * k <= until] + list(range(step_after + 1, until + 1))
        if until < 3600:
            seconds.append(until)
        for sec in sorted(set(seconds)):
            self.beat(sec)
        return self

    def write(self, *, torn_tail: str | None = None, rows: list | None = None) -> "SyntheticRecorder":
        lines = [json.dumps(r) + "\n" for r in (self.rows if rows is None else rows)]
        text = "".join(lines) + (torn_tail or "")
        self.spool_path.write_text(text, encoding="utf-8", newline="")
        events = sorted(self.units.items())
        body = "".join(f"{seq},{DAY_MS + sec * 1000 + 250},{u}\n" for seq, (sec, u) in enumerate(events))
        self.submits_path.write_text("seq_submit,t_submit_evt_ms,count\n" + body, encoding="utf-8", newline="")
        return self


def new_recorder(tmp: Path, *, until: int = 43_204, zero=(), missed=(), traffic_until: int | None = None, **kwargs) -> SyntheticRecorder:
    """A started, scripted recorder with traffic every second from 0 to ``traffic_until`` (default ``until``), healthy beats to ``until``, no stop."""
    rec = SyntheticRecorder(tmp, **kwargs).start()
    rec.traffic(range(0, (until if traffic_until is None else traffic_until) + 1), zero=zero, missed=missed)
    rec.covered(until)
    return rec


def ingest(rec: SyntheticRecorder, *, journal_name: str = "journal.jsonl", policy=POLICY, resume: bool = False, channel_id=None):
    if not rec.spool_path.exists():
        rec.write()
    return bridge.ingest_spool(rec.spool_path, rec.dir / journal_name, source_id=rec.source_id, policy=policy, resume=resume, channel_id=channel_id)


def recorder_source(origin: str = bridge.ORIGIN_SYNTHETIC, day: str = DAY, source_id: str = SOURCE_ID) -> "rc.RecorderArrivalSource":
    return rc.RecorderArrivalSource(source_id=source_id, utc_day=day, dataset_path="synthetic", evidence_origin=origin)


def coverage_and_history(rec: SyntheticRecorder, *, journal_name: str = "journal.jsonl", policy=POLICY, instance: str | None = None, origin=None):
    """Ingest ``rec`` and build the strict recorder history. Returns ``(report, coverage, history)``."""
    report = ingest(rec, journal_name=journal_name, policy=policy)
    events, read_report = rc.read_submit_events(rec.submits_path, instance=instance or rec.instance)
    coverage = rc.RecorderVerifiedCoverage.from_recorder_journal(rec.dir / journal_name, source_id=rec.source_id, submit_events=events, policy=policy,
                                                                 run_id=RUN)
    binned = rc.bin_submit_events(events, recorder_source(origin or rec.origin, source_id=rec.source_id))
    return report, coverage, rc.RecorderCapturedHistory(binned, coverage=coverage, policy=policy)


def by_controller(rows):
    out: dict = {}
    for row in rows:
        out.setdefault(row["controller"], []).append(row)
    return out


# ---------------------------------------------------------------------------------------------------- short fixtures (1 s cadence)
FAST = bridge.RecorderHealthPolicy(heartbeat_interval_s=1, max_heartbeat_gap_s=5.0, freshness_limit_s=10.0, clock_tolerance_ms=250.0)
SHORT = 20


def short_recorder(tmp: Path, *, zero=(), missed=(), source_count: bool = True, stop: bool = True, **kwargs) -> SyntheticRecorder:
    """Twenty seconds of traffic and one heartbeat per second under the FAST policy (1 s interval, 5 s gap)."""
    rec = SyntheticRecorder(tmp, declared_interval_ms=1000, source_count=source_count, **kwargs).start()
    rec.traffic(range(0, SHORT + 1), zero=zero, missed=missed)
    for sec in range(1, SHORT + 1):
        rec.beat(sec)
    if stop:
        rec.stop(SHORT)
    return rec


def snapshot_for(rec: SyntheticRecorder, as_of: float, *, policy=FAST, journal_name: str = "journal.jsonl", rows_hook=None):
    """Ingest ``rec`` and return the recorder coverage snapshot at ``as_of`` seconds after midnight."""
    if not rec.spool_path.exists():
        rec.write()
    bridge.ingest_spool(rec.spool_path, rec.dir / journal_name, source_id=rec.source_id, policy=policy)
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    coverage = rc.RecorderVerifiedCoverage.from_recorder_journal(rec.dir / journal_name, source_id=rec.source_id, submit_events=events, policy=policy)
    return coverage.snapshot(at(as_of))
