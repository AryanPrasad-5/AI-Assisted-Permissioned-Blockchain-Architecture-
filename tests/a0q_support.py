"""Shared helpers for the A0q tests (2026-11-22). EVERYTHING here is SYNTHETIC: a scripted generator writing the issue-log format, scripted read times, scripted process deaths.
Nothing is a measurement of Caliper, Fabric or a real clock, and nothing starts a process, a network connection or a model.

``Scenario`` replays a fixed script of generator writes and reader polls against a ``RecoverableSourceCountReader``. A FAULT kills the reader (or makes a write fail) at the n-th
occurrence of a named boundary; the harness then rebuilds the reader from the files and re-issues the interrupted poll at its SCHEDULED read time (the synthetic schedule is the same
as in the uninterrupted run, which is what lets the final journal be compared byte for byte). Downtime with a LATER restart time is tested separately, where the final journal is
expected to differ and only the eligibility rules are compared."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from tests.a0o_support import LIMITS, NL, a, at, gc, ident, ilr, sr, wc          # noqa: F401

from fabric_perf.controllers import collector_health as ch_mod              # noqa: E402,F401
from fabric_perf.controllers import issue_log_bridge_recovery as rec         # noqa: E402,F401

ROUND = 1
SOURCE_ID = a.SOURCE_ID


class Death(BaseException):
    """A scripted process death: nothing after it runs, and nothing catches it."""


# ---------------------------------------------------------------------------------------------------------------------------------- the script
@dataclass
class Step:
    poll: float                                    # synthetic second at which the reader polls
    ops: Callable[[Any], None] | None = None       # generator writes that happen just before the poll
    label: str = ""


def _txs(gen, sec: int, count: int = 2) -> None:
    for k in range(count):
        gen.tx(sec - 1 + 0.1 + 0.05 * k)


def script() -> list[Step]:
    """About 24 synthetic seconds: ticks every second to 8, silence (idle polls) to 15, a liveness gap, ticks to 19, a tick the reader reaches 0.2 s BEFORE its own second (the
    generator clock is ahead: held back, written by the next, idle poll), more traffic, a clean stop."""
    steps = [Step(-0.4, None, "start only")]
    for sec in range(1, 9):
        steps.append(Step(sec + 0.2, (lambda g, s=sec: (_txs(g, s), g.tick(s))), f"tick {sec}"))
    steps.append(Step(10.0, None, "idle"))
    steps.append(Step(12.0, None, "idle"))
    steps.append(Step(15.2, lambda g: (_txs(g, 15), g.tick(15.0)), "after a 7 s liveness gap"))
    for sec in range(16, 20):
        steps.append(Step(sec + 0.2, (lambda g, s=sec: (_txs(g, s), g.tick(s))), f"tick {sec}"))
    steps.append(Step(19.8, lambda g: (_txs(g, 20), g.tick(20.0)), "tick 20 read before its own second (held back)"))
    steps.append(Step(21.0, None, "idle poll that releases the held-back segment"))
    steps.append(Step(22.3, lambda g: (_txs(g, 22), g.tick(22.0)), "tick 22"))
    steps.append(Step(23.2, lambda g: g.stop(23.0), "stop"))
    return steps


# ---------------------------------------------------------------------------------------------------------------------------------- world
class World:
    def __init__(self, tmp: Path, name: str = "w"):
        self.tmp = Path(tmp)
        self.log_path = self.tmp / f"{name}.issue.jsonl"
        self.ckpt = self.tmp / f"{name}.ckpt.json"
        self.journal = self.tmp / f"{name}.journal.jsonl"
        self.recovery_log = self.tmp / f"{name}.recovery.jsonl"
        log = a.SyntheticIssueLog(self.log_path, tick_interval_ms=1000, round_index=ROUND)
        self.gen = a.Scripted(log).start(-0.5)
        self.steps = script()
        self.applied: set[int] = set()

    def identity(self):
        return ident(self.log_path, round_index=ROUND, checkpoint=self.ckpt)

    def new_reader(self, **kw) -> "rec.RecoverableSourceCountReader":
        return rec.RecoverableSourceCountReader(self.identity(), policy_limits=dict(LIMITS), journal_path=self.journal, source_id=SOURCE_ID, origin=gc.ORIGIN_SYNTHETIC,
                                                recovery_log_path=self.recovery_log, **kw)

    def reopened(self, **kw) -> "rec.RecoverableSourceCountReader":
        """A reader in a NEW process: built fresh, then restarted from the files."""
        reader = self.new_reader(**kw)
        if not self.ckpt.exists() and not self.journal.exists():
            return reader                                            # nothing was ever persisted: this is a first start, not a restart
        reader.restart()
        return reader

    def apply(self, index: int) -> None:
        if index not in self.applied:
            self.applied.add(index)
            if self.steps[index].ops is not None:
                self.steps[index].ops(self.gen)


# ---------------------------------------------------------------------------------------------------------------------------------- faults
def _row_line(journal, kind: str, fields: dict) -> bytes:
    now = journal._clock()
    body = {"schema": wc.JOURNAL_EVENT_SCHEMA, "seq": journal._seq + 1, "kind": kind, "t_utc": wc.iso(now), "source_id": journal.source_id, "collector_id": journal.collector_id,
            "collector_instance": journal.instance, "run_id": journal.run_id, "channel_id": journal.channel_id, "dataset_id": journal.dataset_id, **fields}
    body["prev"] = journal._prev
    body["sha256"] = wc.event_digest(body, journal._prev)
    return (wc.canonical(body) + "\n").encode("utf-8")


BOUNDARIES = {
    "death_before_poll": "process dies after the generator wrote, before the reader reads",
    "death_after_read_before_checkpoint": "records consumed in memory, the process dies before the checkpoint is replaced (a stale .tmp is left)",
    "death_after_checkpoint_before_sync": "checkpoint replaced, the process dies before the bridge derives anything",
    "death_after_sync": "a whole poll and sync finished; the process dies before the next poll",
    "death_after_journal_row": "the process dies after the n-th journal row was appended, inside a sync",
    "journal_torn_write": "a journal row is written only partly, then the process dies",
    "journal_write_failure": "the n-th journal append raises before any byte is written",
    "journal_sync_failure": "the n-th journal row is written in full but its fsync raises",
}


def install(mp, boundary: str, nth: int) -> dict:
    """Patch the n-th occurrence of ``boundary`` (counted from 1 over the FAULTED phase only; the caller removes the patch before the restart)."""
    state = {"n": 0, "fired": False}
    orig_append = ch_mod.CollectorHealthJournal._append
    orig_save = ilr.IssueLogTail.save_checkpoint
    orig_sync = gc.IssueLogJournalBridge.sync

    if boundary == "death_after_read_before_checkpoint":
        def save(self):
            state["n"] += 1
            if state["n"] == nth:
                state["fired"] = True
                tmp = self.checkpoint_path.with_name(self.checkpoint_path.name + ".tmp")
                tmp.write_bytes(b'{"schema":"e1_issue_log_checkpoint/1","offs')                 # a torn temporary file; the real checkpoint is untouched
                raise Death()
            return orig_save(self)
        mp.setattr(ilr.IssueLogTail, "save_checkpoint", save)
    elif boundary == "death_after_checkpoint_before_sync":
        def sync(self, read_time):
            state["n"] += 1
            if state["n"] == nth:
                state["fired"] = True
                raise Death()
            return orig_sync(self, read_time)
        mp.setattr(gc.IssueLogJournalBridge, "sync", sync)
    elif boundary in ("death_after_journal_row", "journal_torn_write", "journal_write_failure", "journal_sync_failure"):
        def append(self, kind, **fields):
            state["n"] += 1
            if state["n"] == nth and boundary != "death_after_journal_row":
                state["fired"] = True
                if boundary == "journal_write_failure":
                    raise OSError("simulated write failure")
                line = _row_line(self, kind, fields)
                keep = len(line) if boundary == "journal_sync_failure" else max(1, len(line) // 2)
                with self.path.open("ab") as handle:
                    handle.write(line[:keep])
                    handle.flush()
                raise OSError("simulated fsync failure") if boundary == "journal_sync_failure" else Death()
            row = orig_append(self, kind, **fields)
            if boundary == "death_after_journal_row" and state["n"] == nth:
                state["fired"] = True
                raise Death()
            return row
        mp.setattr(ch_mod.CollectorHealthJournal, "_append", append)
    return state


# ---------------------------------------------------------------------------------------------------------------------------------- runner
def run(tmp: Path, *, boundary: str | None = None, nth: int = 1, name: str = "w", in_process_restart: bool = False, until: int | None = None):
    """Run the script. Returns ``(world, reader, outcome)`` where outcome is ``{"fired": bool, "exception": str|None}``.

    A fault fires at most once. After it, a new reader (or the same one for an ``OSError`` when ``in_process_restart``) is rebuilt from the files and the interrupted step is polled
    again at its scheduled read time."""
    import pytest
    world = World(tmp, name)
    reader = world.new_reader()
    outcome: dict[str, Any] = {"fired": False, "exception": None, "boundary": boundary, "nth": nth}
    mp = pytest.MonkeyPatch()
    state = install(mp, boundary, nth) if boundary and boundary not in ("death_before_poll", "death_after_sync") else {"n": 0, "fired": False}
    active = True
    index = 0
    steps = world.steps if until is None else world.steps[:until]
    try:
        while index < len(steps):
            world.apply(index)
            try:
                if boundary == "death_before_poll" and active and index == nth:
                    state["fired"] = True
                    raise Death()
                reader.poll(at(steps[index].poll))
                if boundary == "death_after_sync" and active and index == nth:
                    state["fired"] = True
                    raise Death()
            except (Death, OSError) as exc:
                outcome.update({"fired": True, "exception": type(exc).__name__})
                mp.undo()
                active = False
                if isinstance(exc, OSError) and in_process_restart:
                    reader.restart()
                else:
                    reader = world.reopened()
                if boundary in ("death_after_sync",):
                    index += 1                                       # that poll had completed
                continue
            index += 1
    finally:
        mp.undo()
    outcome["fired"] = outcome["fired"] or state["fired"]
    return world, reader, outcome


def sha(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def journal_rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in Path(path).read_bytes().split(NL)[:-1]]


def signature(world: World, reader, *, seconds=range(-1, 26)) -> dict[str, Any]:
    """Everything a decision could depend on: the journal bytes, every attempt with its ORIGINAL read time, the observed segments and the evidence/coverage seen at each cutoff."""
    tail = reader.tail
    per_cutoff: dict[str, Any] = {}
    cov = gc.GeneratorIssueCoverage.from_bridge(reader.bridge, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0)) if world.journal.exists() else None
    for s in seconds:
        ev = reader.evidence(as_of=at(s))
        for key in ("recovery",):
            ev.pop(key, None)
        ev["time"] = {k: v for k, v in ev["time"].items() if k not in ("reads", "restarts", "read_watermark_utc")}
        ev.pop("checkpoint", None)
        snap = None
        if cov is not None:
            sn = cov.snapshot(at(s))
            snap = {"valid": sn.valid, "failure": sn.failure_code, "scope": sn.scope, "derivation": repr(sn.derivation)}
        per_cutoff[str(s)] = {"evidence": ev, "snapshot": snap}
    return {"journal_sha256": sha(world.journal) if world.journal.exists() else None,
            "attempts": [(e["id"], e["ts_ms"], e["read_ms"]) for e in tail.attempt_events(reader.bridge.instance)],
            "segments": [list(x) for x in tail.observed_segments()], "marks": [list(m) for m in tail._marks], "per_cutoff": per_cutoff}


def baseline(tmp: Path, name: str = "w"):
    world, reader, outcome = run(tmp, name=name)
    return world, reader, signature(world, reader)


# ---------------------------------------------------------------------------------------------------------------------------------- matrix
def matrix_cases(steps: int) -> list[tuple[str, int]]:
    """Every (boundary, n) the scenario can reach, plus a margin that must come back NOT_REACHED (so a changed scenario is noticed)."""
    cases = [("death_before_poll", n) for n in range(0, steps)] + [("death_after_sync", n) for n in range(0, steps)]
    for boundary in ("death_after_read_before_checkpoint", "death_after_checkpoint_before_sync", "death_after_journal_row", "journal_torn_write", "journal_write_failure",
                     "journal_sync_failure"):
        cases += [(boundary, n) for n in range(1, steps + 1)]
    return cases


def run_matrix(tmp: Path) -> dict[str, Any]:
    """Run every case against the uninterrupted baseline. Outcomes: IDENTICAL (journal, evidence and coverage at every cutoff equal), REFUSED (recovery raised a coded refusal; the
    journal on disk is left as the crash left it), DIFFERENT (a defect), NOT_REACHED (the n-th boundary never occurs in this script)."""
    tmp = Path(tmp)
    (tmp / "baseline").mkdir()
    world, reader, base_sig = baseline(tmp / "baseline")
    results = []
    for index, (boundary, nth) in enumerate(matrix_cases(len(world.steps))):
        case_dir = tmp / f"c{index:03d}"
        case_dir.mkdir()
        entry: dict[str, Any] = {"boundary": boundary, "n": nth}
        try:
            w, r, out = run(case_dir, boundary=boundary, nth=nth)
        except rec.BridgeRecoveryError as exc:
            entry.update({"outcome": "REFUSED", "code": exc.code})
            results.append(entry)
            continue
        if not out["fired"]:
            entry["outcome"] = "NOT_REACHED"
        else:
            entry["outcome"] = "IDENTICAL" if signature(w, r) == base_sig else "DIFFERENT"
            entry["exception"] = out["exception"]
            if w.recovery_log.exists():
                entry["recovery_records"] = len(w.recovery_log.read_bytes().split(NL)) - 1
        results.append(entry)
    counts: dict[str, int] = {}
    for entry in results:
        counts[entry["outcome"]] = counts.get(entry["outcome"], 0) + 1
    return {"baseline_journal_rows": len(journal_rows(world.journal)), "baseline_journal_sha256": base_sig["journal_sha256"], "script_steps": len(world.steps),
            "counts": counts, "results": results}
