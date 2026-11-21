"""Shared helpers for the A0k tests (2026-11-21). EVERYTHING here is SYNTHETIC: scripted generator logs, scripted recorders, scripted clocks.

Nothing here is a measurement of Caliper, Fabric or a real clock, and no synthetic source counter proves anything about a real source counter.
"""

from __future__ import annotations

import copy
from pathlib import Path

from tests import a0j_support as j
from tests.a0j_support import DAY_MS, FAST, SHORT, SOURCE_DESCRIPTOR, SOURCE_ID, at, bridge, rc, wc  # noqa: F401

from fabric_perf.controllers import captured_history as ch  # noqa: E402,F401
from fabric_perf.controllers import collector_health as chj  # noqa: E402,F401
from fabric_perf.controllers import source_completeness as sc  # noqa: E402,F401
from fabric_perf.controllers import worker_coverage as wk  # noqa: E402,F401

C = wc.CoverageClass
LAG_MS = 500                       # SYNTHETIC declared callback-lag allowance for the dispatch-log reconciliation
DOMAIN = "synthetic-host:node_date_now"


def dispatch_text(truth: dict[int, int], *, offset_ms: int = 10, step_ms: int = 5, extra_rows: list | None = None, header: bool = True) -> str:
    """A dispatch log (``seq,intended_offset_ms,intended_ms,dispatch_ms,lag_ms``) for a scripted source: ``truth[sec]`` transactions issued in day second
    ``sec``, at ``offset_ms + k * step_ms`` after the second starts (generator ISSUE times, earlier than the recorder's callback at +250 ms)."""
    rows, seq = [], 0
    for sec in sorted(truth):
        for k in range(truth[sec]):
            dispatched = DAY_MS + sec * 1000 + offset_ms + k * step_ms
            intended = dispatched                       # lag 0: a log row needs lag_ms == dispatch_ms - intended_ms
            rows.append(f"{seq},{sec * 1000 + offset_ms + k * step_ms},{intended},{dispatched},0")
            seq += 1
    body = "".join(r + "\n" for r in rows + list(extra_rows or []))
    return (",".join(sc.DISPATCH_HEADER) + "\n" if header else "") + body


def dispatch_evidence(rec: "j.SyntheticRecorder", *, text: str | None = None, lag_ms: int = LAG_MS, **kwargs) -> "sc.DispatchLogEvidence":
    """The generator-side evidence for ``rec``'s scripted source (``rec.truth``), as the trace-replay dispatch log would hold it."""
    return sc.DispatchLogEvidence.from_text(dispatch_text(rec.truth) if text is None else text, worker=str(kwargs.pop("worker", rec.worker)),
                                            clock_domain=kwargs.pop("clock_domain", DOMAIN), run_id=j.RUN, instance=kwargs.pop("instance", rec.instance),
                                            max_callback_lag_ms=lag_ms, **kwargs)


def descriptor(**overrides) -> dict:
    """A fresh copy of the synthetic interval-suitable descriptor with ``overrides`` applied (``None`` removes a key)."""
    doc = copy.deepcopy(SOURCE_DESCRIPTOR)
    for key, value in overrides.items():
        if value is None:
            doc.pop(key, None)
        else:
            doc[key] = value
    return doc


def recorder_coverage(rec: "j.SyntheticRecorder", tmp: Path, *, policy=FAST, journal_name: str = "journal.jsonl", source_evidence=None, rows_hook=None):
    """Ingest ``rec`` and build the strict recorder coverage (optionally with external source evidence). Returns ``(report, coverage, events)``."""
    if rows_hook is not None:
        rows_hook(rec)
    rec.write()
    report = bridge.ingest_spool(rec.spool_path, tmp / journal_name, source_id=rec.source_id, policy=policy)
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    coverage = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp / journal_name, source_id=rec.source_id, submit_events=events, policy=policy,
                                                                 run_id=j.RUN, source_evidence=source_evidence)
    return report, coverage, events


def short_worker(tmp: Path, index: int, *, zero=(), missed=(), source_count: bool = True, stop: bool = True, name: str | None = None) -> "j.SyntheticRecorder":
    """One short scripted worker whose instance, submits file and spool are its own."""
    instance = f"rec:{j.RUN}:w{index}:r0"
    rec = j.SyntheticRecorder(tmp, name=name or f"w{index}", instance=instance, worker=index, declared_interval_ms=1000, source_count=source_count).start()
    rec.traffic(range(0, SHORT + 1), zero=zero, missed=missed)
    for sec in range(1, SHORT + 1):
        rec.beat(sec)
    if stop:
        rec.stop(SHORT)
    return rec


def worker_coverage(tmp: Path, recs: dict, *, roster=None, declared_total=None, drop=(), duplicate=None):
    """Combine scripted workers. ``recs`` maps worker index -> SyntheticRecorder. Returns ``(coverage, per_worker_coverages)``."""
    per = {}
    for index, rec in recs.items():
        _report, cov, _events = recorder_coverage(rec, tmp, journal_name=f"journal_w{index}.jsonl")
        per[index] = cov
    pairs = [(i, c) for i, c in per.items() if i not in drop]
    if duplicate is not None:
        pairs.append((duplicate, per[duplicate]))
    roster = roster or wk.WorkerRoster.from_benchmark_config(len(recs), DOMAIN)
    return wk.MultiWorkerCoverage(source_id=SOURCE_ID, roster=roster, workers=pairs, declared_source_total=declared_total, run_id=j.RUN), per
