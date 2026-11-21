"""Shared helpers for the A0o tests (2026-11-21). EVERYTHING here is SYNTHETIC: scripted generators writing the issue-log format, scripted read times, scripted spools.
Nothing is a measurement of Caliper, Fabric or a real clock, and nothing starts a process, a network connection or a model."""

from __future__ import annotations

import json
from pathlib import Path

from tests import a0l_support as a                                            # noqa: F401  (SyntheticIssueLog, Scripted, short_log, policy, DOMAIN, RUN, ...)
from tests.a0l_support import DAY_GAP_MS, DAY_MS, DOMAIN, RUN, SHORT_GAP_MS, WORKER, Scripted, SyntheticIssueLog, at, gc, ilr, wc   # noqa: F401

from fabric_perf.controllers import sourcecount_reader as sr                    # noqa: E402,F401
from fabric_perf.controllers import timing_sampler as ts                        # noqa: E402,F401

REPO = Path(__file__).resolve().parents[1]
STORED = REPO / "docs" / "research" / "e1" / "a0m_dry_render_evidence_2026-11-21" / "node_driver_run"
LIMITS = {"max_tick_gap_ms": SHORT_GAP_MS}
NL = bytes([10])


def ident(log: Path, *, round_index: int = 1, checkpoint: Path | None = None, run_id: str = RUN, worker: str = WORKER, domain: str = DOMAIN) -> "sr.ReaderIdentity":
    return sr.ReaderIdentity(run_id, round_index, worker, log, domain, checkpoint)


def scripted_log(tmp: Path, *, name: str = "g", round_index: int = 1, seconds: int = 20, **kw) -> "Scripted":
    """Twenty synthetic seconds, two transactions per second, ticks every second, a clean stop."""
    return a.short_log(tmp, name=name, until=seconds, round_index=round_index, **kw)


def day_with_reader(tmp: Path, *, name: str = "day", every: int = 10, until: int = 43_204, ticks: set | None = None, late_ticks: bool = True, latency: float = 0.0,
                    after_poll=None, round_index: int = 1):
    """A day of generator traffic read by the A0o driver INCREMENTALLY (the same script as ``a0l_support.day_generator``, but the reader is a ``SourceCountReader`` with a
    round-carrying journal bridge). Returns ``(scripted_generator, reader)``."""
    log = SyntheticIssueLog(tmp / f"{name}.issue.jsonl", tick_interval_ms=3_600_000, round_index=round_index)
    gen = Scripted(log).start(-0.5)
    reader = sr.SourceCountReader(ident(log.path, round_index=round_index), policy_limits={"max_tick_gap_ms": DAY_GAP_MS}, journal_path=tmp / f"{name}.journal.jsonl",
                                  source_id=a.SOURCE_ID, origin=gc.ORIGIN_SYNTHETIC)
    reader.poll(at(-0.4))
    tick_seconds = sorted((ticks if ticks is not None else {3600 * k for k in range(1, 13) if 3600 * k <= until}) | (set(range(43_201, until + 1)) if late_ticks else set()))
    done = -1
    for tick_at in tick_seconds:
        for sec in range(done + 1, tick_at):
            if sec % every == 0:
                gen.tx(sec + 0.1)
        done = tick_at - 1
        gen.tick(tick_at)
        reader.poll(at(tick_at + latency))
        if after_poll is not None:
            after_poll(tick_at, log)
    return gen, reader


def history_of(reader: "sr.SourceCountReader", *, gap_s: float = 7200.0, freshness_s: float = 7200.0):
    return sr.build_captured_history(reader, utc_day=a.DAY, source_id=a.SOURCE_ID, coverage_policy=wc.CoveragePolicy(max_heartbeat_gap_s=gap_s), freshness_limit_s=freshness_s)


def lines_of(path: Path) -> list[bytes]:
    return Path(path).read_bytes().split(NL)[:-1]


def write_lines(path: Path, lines: list[bytes]) -> None:
    Path(path).write_bytes(NL.join(lines) + NL if lines else b"")


def tamper(line: bytes, field: str, value) -> bytes:
    row = json.loads(line)
    row[field] = value
    return wc.canonical(row).encode("ascii")
