"""Shared helpers for the A0l tests (2026-11-21). EVERYTHING here is SYNTHETIC: a scripted generator writing the issue-log format, scripted read times,
scripted recorders. Nothing is a measurement of Caliper, Fabric or a real clock.

``SyntheticIssueLog`` writes byte-for-byte what ``fabric-testbed/caliper/workloads/e1_issue_log.js`` writes (same canonical JSON, same hash chain), so the
Python reader can be tested at day scale without Node. ``test_a0l_workload_demo`` runs the REAL JavaScript writer and reads its output with the same
reader, which is what keeps this double honest.
"""

from __future__ import annotations

import json
from pathlib import Path

from tests import a0j_support as j
from tests.a0j_support import DAY, DAY_MS, FAST, SHORT, SOURCE_ID, at, bridge, rc, wc  # noqa: F401
from tests.a0k_support import DOMAIN, LAG_MS  # noqa: F401

from fabric_perf.controllers import captured_history as ch  # noqa: E402,F401
from fabric_perf.controllers import issue_log_coverage as gc  # noqa: E402,F401
from fabric_perf.controllers import issue_log_reader as ilr  # noqa: E402,F401
from fabric_perf.controllers import source_completeness as sc  # noqa: E402,F401

C = wc.CoverageClass
RUN = j.RUN                       # "run-a0j": the recorder fixtures use it, so the two routes name the same run
WORKER = "w0"
DAY_GAP_MS = 7_200_000            # SYNTHETIC: hourly ticks keep the day-long fixtures short; NOT a recommendation
SHORT_GAP_MS = 5_000


def ms(second: float) -> int:
    """Epoch ms of ``second`` seconds into the synthetic day."""
    return DAY_MS + int(round(second * 1000))


def policy(**overrides) -> "ilr.IssueLogPolicy":
    base = {"run_id": RUN, "worker_id": WORKER, "clock_domain": DOMAIN, "max_tick_gap_ms": DAY_GAP_MS}
    base.update(overrides)
    return ilr.IssueLogPolicy(**base)


class SyntheticIssueLog:
    """A scripted generator log writer. ``t`` arguments are seconds into the synthetic day (floats allowed)."""

    def __init__(self, path: Path, *, run_id: str = RUN, worker_id: str = WORKER, round_index: int = 1, clock_domain: str = DOMAIN, roster=None,
                 total_workers: int = 1, tick_interval_ms: int = 1000, workload_id: str = "e1_create_asset_issue_logged/1", worker_index: int | None = None):
        self.path = Path(path)
        self.run_id, self.worker_id, self.round_index = run_id, worker_id, round_index
        self.clock_domain, self.roster, self.total_workers = clock_domain, list(roster or [worker_id]), total_workers
        self.tick_interval_ms, self.workload_id = tick_interval_ms, workload_id
        self.worker_index = int(worker_id[1:]) if worker_index is None else worker_index
        self.rseq, self.prev = 0, wc.GENESIS_SHA256
        self.n = 0
        self.tx_seq = 0
        self.t0_ms: int | None = None
        self.path.write_bytes(b"")

    # ------------------------------------------------------------------------------------------------------------ rows
    def body(self, kind: str, t: float, *, rseq: int | None = None, mono_ms: int | None = None, **fields) -> dict:
        t_ms = ms(t)
        if self.t0_ms is None:
            self.t0_ms = t_ms
        row = {"schema": ilr.ISSUE_LOG_SCHEMA, "rseq": self.rseq + 1 if rseq is None else rseq, "kind": kind, "run_id": self.run_id, "worker_id": self.worker_id,
               "t_ms": t_ms, "mono_ms": t_ms - self.t0_ms if mono_ms is None else mono_ms, **fields}
        row["prev"] = self.prev
        row["sha256"] = wc.event_digest(row, self.prev)
        return row

    def append(self, row: dict, *, advance: bool = True) -> dict:
        with self.path.open("ab") as handle:
            handle.write((wc.canonical(row) + "\n").encode("ascii"))
        if advance:
            self.rseq, self.prev = row["rseq"], row["sha256"]
        return row

    def raw(self, kind: str, t: float, **fields) -> dict:
        return self.append(self.body(kind, t, **fields))

    def start(self, t: float = -0.5, **over) -> dict:
        fields = {"writer_version": "synthetic", "workload_id": self.workload_id, "worker_index": self.worker_index, "total_workers": self.total_workers,
                  "worker_roster": self.roster, "round_index": self.round_index, "clock_domain": self.clock_domain, "wall_clock_source": "Date.now()",
                  "timestamp_precision": "ms", "tick_interval_ms": self.tick_interval_ms, "fsync_policy": "tick", "config": {"mode": "create"},
                  "event_time_semantics": "generator_issue_time"}
        fields.update(over)
        return self.raw("start", t, **fields)

    def issue_id(self, n: int) -> str:
        return f"{self.run_id}:{self.round_index}:{n}"

    def intent(self, t: float, *, n: int | None = None, tx_seq: int | None = None, op: str = "CreateAsset", key: str | None = "auto", **over) -> int:
        n = self.n if n is None else n
        tx_seq = self.tx_seq if tx_seq is None else tx_seq
        fields = {"n": n, "tx_seq": tx_seq, "issue_id": self.issue_id(n), "op": op,
                  "asset_key": f"{self.run_id}_{self.round_index}_{tx_seq:07d}" if key == "auto" else key}
        fields.update(over)
        self.raw("intent", t, **fields)
        self.n, self.tx_seq = n + 1, tx_seq + 1
        return n

    def attempt(self, t: float, n: int, **over) -> None:
        self.raw("attempt", t, **{"n": n, "issue_id": self.issue_id(n), **over})

    def outcome(self, t: float, n: int, result: str = "returned", **over) -> None:
        fields = {"n": n, "issue_id": self.issue_id(n), "result": result, "error": None if result == "returned" else "send rejected",
                  "client_tx_id": None, "client_status": "success" if result == "returned" else None}
        fields.update(over)
        self.raw("outcome", t, **fields)

    def issue(self, t: float, *, outcome_after: float | None = 0.01, result: str = "returned", **over) -> int:
        """intent + attempt (both at ``t``) and, unless ``outcome_after`` is None, an outcome ``outcome_after`` seconds later."""
        n = self.intent(t, **over)
        self.attempt(t, n)
        if outcome_after is not None:
            self.outcome(t + outcome_after, n, result)
        return n

    def counts(self, resolved: int, attempted: int, intents: int) -> dict:
        return {"attempted": attempted, "intents": intents, "outcomes": resolved}

    def tick(self, t: float, *, attempted: int, intents: int, outcomes: int, **over) -> dict:
        return self.raw("tick", t, **{"attempted": attempted, "intents": intents, "outcomes": outcomes, **over})

    def stop(self, t: float, *, attempted: int, intents: int, outcomes: int, in_flight: int = 0, reason: str = "cleanup", **over) -> dict:
        return self.raw("stop", t, **{"reason": reason, "intents": intents, "attempted": attempted, "outcomes": outcomes, "in_flight": in_flight, **over})

    def text(self) -> str:
        return self.path.read_text(encoding="ascii")

    def lines(self) -> list[str]:
        return self.text().split("\n")[:-1]


class Scripted:
    """A scripted generator: it keeps its own counts so ticks and stops are always consistent with the records written before them."""

    def __init__(self, log: SyntheticIssueLog):
        self.log = log
        self.attempted = self.intents = self.outcomes = 0

    def start(self, t: float = -0.5, **over) -> "Scripted":
        self.log.start(t, **over)
        return self

    def tx(self, t: float, *, outcome_after: float | None = 0.01, result: str = "returned") -> int:
        n = self.log.issue(t, outcome_after=outcome_after, result=result)
        self.intents += 1
        self.attempted += 1
        self.outcomes += 0 if outcome_after is None else 1
        return n

    def tick(self, t: float, **over) -> "Scripted":
        self.log.tick(t, attempted=self.attempted, intents=self.intents, outcomes=self.outcomes, **over)
        return self

    def stop(self, t: float, **over) -> "Scripted":
        self.log.stop(t, attempted=self.attempted, intents=self.intents, outcomes=self.outcomes, in_flight=self.attempted - self.outcomes, **over)
        return self


def short_log(tmp: Path, *, truth: dict | None = None, until: int = SHORT, name: str = "g", stop: bool = True, tick_every: int = 1, **kw) -> "Scripted":
    """Twenty seconds, one tick per second, ``truth[sec]`` transactions per second (default 2) issued at sec+0.1, sec+0.15, ..."""
    log = SyntheticIssueLog(tmp / f"{name}.issue.jsonl", tick_interval_ms=1000, **kw)
    gen = Scripted(log).start(-0.5)
    truth = {sec: 2 for sec in range(until)} if truth is None else truth
    for sec in range(1, until + 1):
        for k in range(truth.get(sec - 1, 0)):
            gen.tx(sec - 1 + 0.1 + 0.05 * k)
        if sec % tick_every == 0:
            gen.tick(sec)
    if stop:
        gen.stop(until + 0.0)
    return gen


class Reader:
    """A tail (and optional bridge) driven by scripted read times: ``poll(second)`` reads at that second of the synthetic day."""

    def __init__(self, path: Path, *, pol: "ilr.IssueLogPolicy | None" = None, checkpoint: Path | None = None):
        self.tail = ilr.IssueLogTail(path, pol or policy(max_tick_gap_ms=SHORT_GAP_MS), checkpoint_path=checkpoint)
        self.bridge = None

    def with_bridge(self, journal: Path, *, origin: str = gc.ORIGIN_SYNTHETIC, channel_id=None) -> "Reader":
        self.bridge = gc.IssueLogJournalBridge(self.tail, journal, source_id=SOURCE_ID, origin=origin, channel_id=channel_id)
        return self

    def poll(self, second: float):
        result = self.tail.poll(at(second))
        report = self.bridge.sync(at(second)) if self.bridge is not None else None
        return result, report


def drain(reader: Reader, seconds) -> list:
    """Poll at each second in order (the log must already hold what a poll at that second should see)."""
    return [reader.poll(s) for s in seconds]


def short_history(tmp: Path, **kw):
    """A 20-second generator history on FAST-like settings: returns (reader, coverage)."""
    gen = short_log(tmp, **kw)
    reader = Reader(gen.log.path).with_bridge(tmp / "gj.jsonl")
    reader.poll(-0.4)
    for sec in range(1, SHORT + 1):
        reader.poll(sec)
    coverage = gc.GeneratorIssueCoverage.from_bridge(reader.bridge, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0))
    return gen, reader, coverage


# -------------------------------------------------------------------------------------------------------------------------- day-long fixtures
def day_generator(tmp: Path, *, every: int = 10, until: int = 43_204, name: str = "day", unresolved_at: set | None = None, skip: set | None = None,
                  stop: bool = False, pol: "ilr.IssueLogPolicy | None" = None, journal: bool = True, latency: float = 0.0, ticks: set | None = None,
                  late_ticks: bool = True, after_poll=None):
    """A day of generator traffic: one transaction at second ``s + 0.1`` for every ``s`` that is a multiple of ``every`` (not in ``skip``), ticks hourly
    and then every second from 43,201 to ``until``, each read at ``tick time + latency``. The log is written and polled INCREMENTALLY, so every record's
    read time is its poll time. Returns ``(scripted_generator, reader)``."""
    log = SyntheticIssueLog(tmp / f"{name}.issue.jsonl", tick_interval_ms=3_600_000)
    gen = Scripted(log).start(-0.5)
    reader = Reader(log.path, pol=pol or policy())
    if journal:
        reader.with_bridge(tmp / f"{name}.journal.jsonl")
    reader.poll(-0.4)
    tick_seconds = sorted((ticks if ticks is not None else {3600 * k for k in range(1, 13) if 3600 * k <= until})
                          | (set(range(43_201, until + 1)) if late_ticks else set()))
    done = -1
    for tick_at in tick_seconds:
        for sec in range(done + 1, tick_at):
            if sec % every == 0 and sec not in (skip or set()):
                unresolved = bool(unresolved_at and sec in unresolved_at)
                gen.tx(sec + 0.1, outcome_after=None if unresolved else 0.01)
        done = tick_at - 1
        gen.tick(tick_at)
        reader.poll(tick_at + latency)
        if after_poll is not None:
            after_poll(tick_at, log)                                                     # a hook for scripted faults (e.g. editing a record that was already read)
    if stop:
        gen.stop(until + 0.0)
        reader.poll(until + latency)
    return gen, reader


def day_history(reader: "Reader", *, gap_s: float = 7200.0, freshness_s: float = 7200.0, origin: str = gc.ORIGIN_SYNTHETIC):
    coverage = gc.GeneratorIssueCoverage.from_bridge(reader.bridge, policy=wc.CoveragePolicy(max_heartbeat_gap_s=gap_s))
    source = gc.GeneratorIssueSource(source_id=SOURCE_ID, utc_day=DAY, dataset_path="synthetic", evidence_origin=origin)
    binned = gc.bin_issue_events(reader.tail.attempt_events(reader.bridge.instance), source)
    return gc.GeneratorIssueCapturedHistory(binned, coverage=coverage, freshness_limit_s=freshness_s)


def recorder_for(tmp: Path, *, every: int = 10, until: int = 43_204, missed=(), extra_zero=(), name: str = "w0", source_count: bool = False):
    """A scripted recorder day (callback at +0.25 s) whose scripted source issues one transaction at ``s`` for every multiple of ``every``."""
    rec = j.SyntheticRecorder(tmp, name=name, source_count=source_count)
    rec.start()
    skip = {s for s in range(until + 1) if s % every != 0} | set(extra_zero)
    rec.traffic(range(0, until + 1), zero=skip, missed=missed, units_fn=lambda s: 1)
    rec.covered(until)
    return rec


def read_json(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))
