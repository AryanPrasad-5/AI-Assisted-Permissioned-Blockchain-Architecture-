"""Restart recovery for the GENERATOR-route journal bridge (task A0q, 2026-11-22). OFFLINE ONLY.

The problem (reproduced before any change; ``tests/test_a0q_generator_bridge_recovery_2026_11_22.py::test_01``): ``SourceCountReader.restart`` refuses every restart that has a journal bridge
(``bridge_restart_unsupported``), because ``issue_log_coverage.IssueLogJournalBridge`` keeps state that nothing persists (``_seg_done``, ``_last_end_ms``, ``_wm``, ``_origin_dt``,
``_pending_recovery``, ``_error_written``, ``_stopped`` and the journal's ``_open_error``), and because each journal row is stamped with the READ time of the sync that wrote it.

What this module does (``sourcecount_reader``, ``issue_log_coverage``, ``issue_log_reader`` and ``collector_health`` are NOT edited; they are candidate files with recorded hashes):

* RE-SIMULATES the earlier syncs with the unmodified ``IssueLogJournalBridge.sync`` into a scratch journal. Each simulated sync runs at an ORIGINAL read time (the read marks the
  checkpoint keeps for every consuming poll, plus the times stamped on the existing journal rows) against a TIME-LIMITED VIEW of the resumed tail that shows only what had been read by
  that time (start, segments, attempts and stop with their original read times). Nothing is simulated at the restart time and nothing is read from the future.
* Requires the existing journal to be an EXACT BYTE PREFIX of the simulated journal. Anything else (a changed row, a row the checkpoint cannot explain, a different identity, scope or
  origin, a broken hash chain) is refused with a code and nothing is written.
* Completes the journal ONLY with rows the simulation proves the original sync would have written (a crash between the checkpoint and the journal append, or between the rows of one
  sync). Those rows keep the ORIGINAL read time as ``t_utc`` (the evidence was read then); the recovery record lists them as ``completed_after_restart`` and states that they were not
  in the journal before the restart. A torn final journal line is removed only if it is provably a strict prefix of the next simulated row.
* Transplants the simulated bridge state into a real bridge so later syncs continue exactly as an uninterrupted bridge would.

What it does NOT do. It does not recover from an integrity failure (a sticky ``issue_log_invalid`` error row: the original detection time is not recoverable, so restart is refused).
It cannot tell two polls at one read time from one (the checkpoint merges them), so a journal written by several same-time polls that differs from the one-sync simulation is refused, not
guessed. Idle polls that wrote nothing leave no trace and are not needed. A real power loss, a storage device that misreports a flush and a second concurrent bridge are not covered.
No process, network, container, model or Caliper API is imported.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import issue_log_coverage as gc
from . import issue_log_reader as ilr
from . import sourcecount_reader as sr
from . import source_completeness as sc
from .workload_coverage import (GENESIS_SHA256, JOURNAL_EVENT_SCHEMA, KIND_ERROR, KIND_START, SCOPE_SOURCE_STREAM, canonical, event_digest, parse_utc)

RECOVERY_VERSION = "issue_log_bridge_recovery 0.1 (A0q, 2026-11-22)"
RECOVERY_SCHEMA = "generator_bridge_recovery/1"
TORN_REFUSE, TORN_TRUNCATE = "refuse", "truncate_if_provable_prefix"
TORN_POLICIES = (TORN_REFUSE, TORN_TRUNCATE)
ST_RECOVERED, ST_NOTHING = "RECOVERED", "NOTHING_TO_RECOVER"
BRIDGE_STATE_FIELDS = ("_seg_done", "_last_end_ms", "_wm", "_origin_dt", "_pending_recovery", "_error_written", "_stopped", "_now")
JOURNAL_FLAGS = ("_started", "_stopped", "_open_error", "_last_watermark")


class BridgeRecoveryError(sc.SourceEvidenceError):
    """A generator-route bridge cannot be recovered from the evidence on disk. ``code`` is machine-readable. Nothing is written when this is raised."""


def _fail(code: str, message: str) -> BridgeRecoveryError:
    return BridgeRecoveryError(code, message)


# ------------------------------------------------------------------------------------------------------------------ time-limited view of a tail
class TimeLimitedTailView:
    """What ``IssueLogJournalBridge`` reads from a tail, restricted to what had been READ by ``as_of_ms``. Only the attributes the bridge uses are provided, so a bridge that started
    reading something else would fail loudly instead of seeing the future."""

    def __init__(self, tail: ilr.IssueLogTail, as_of_ms: int, *, start_read_ms: int | None, stop_read_ms: int | None):
        self._tail = tail
        self._as_of = as_of_ms
        self.policy = tail.policy
        self.error = None
        self.error_read_ms = None
        self._visible_segments = bisect.bisect_right(tail._seg_read, as_of_ms)
        self._start_visible = tail.start_record is not None and start_read_ms is not None and start_read_ms <= as_of_ms
        self._stop_visible = tail.stop_record is not None and stop_read_ms is not None and stop_read_ms <= as_of_ms
        self._head = tail._head if self._start_visible else None
        self._attempt_ms = tail._attempt_ms[:bisect.bisect_right(tail._attempt_read, as_of_ms)]

    @property
    def start_record(self) -> dict[str, Any] | None:
        return self._tail.start_record if self._start_visible else None

    @property
    def stop_record(self) -> dict[str, Any] | None:
        return self._tail.stop_record if self._stop_visible else None

    @property
    def segment_count(self) -> int:
        return self._visible_segments

    def liveness_points_since(self, count: int) -> list[tuple[int, int, int]]:
        tail = self._tail
        return [(int(tail._seg_start[i]), int(tail._seg_end[i]), int(tail._seg_read[i])) for i in range(count, self._visible_segments)]

    def writer_state(self) -> dict[str, Any]:
        """Only the ``state`` the bridge consults. A stop record is the last record a valid log can hold, so its state does not change after it was read."""
        if not self._stop_visible:
            return {"state": ilr.W_ACTIVE}
        return {"state": self._tail.writer_state()["state"]}


def _read_time_of_record(marks: Sequence[Sequence[int]], rseq: int) -> int | None:
    """The read time of record ``rseq``: the first mark whose end is at or after it."""
    ends = [m[0] for m in marks]
    index = bisect.bisect_left(ends, rseq)
    return None if index >= len(marks) else int(marks[index][1])


# ------------------------------------------------------------------------------------------------------------------ journal bytes
def _split_journal(raw: bytes) -> tuple[list[bytes], bytes]:
    parts = raw.split(b"\n")
    return parts[:-1], parts[-1]


def _verify_chain(lines: Sequence[bytes]) -> tuple[list[dict[str, Any]], str]:
    rows: list[dict[str, Any]] = []
    previous = GENESIS_SHA256
    last_ms: int | None = None
    for number, line in enumerate(lines, 1):
        try:
            text = line.decode("ascii")
            row = json.loads(text)
        except (UnicodeDecodeError, ValueError) as exc:
            raise _fail("journal_row_malformed", f"journal row {number} is not ASCII JSON") from exc
        if not isinstance(row, dict) or canonical(row) != text:
            raise _fail("journal_row_not_canonical", f"journal row {number} is not an object in canonical form")
        if row.get("schema") != JOURNAL_EVENT_SCHEMA or row.get("seq") != number:
            raise _fail("journal_chain_broken", f"journal row {number}: schema or sequence is wrong (seq {row.get('seq')!r})")
        if row.get("prev") != previous or row.get("sha256") != event_digest(row, previous):
            raise _fail("journal_chain_broken", f"journal row {number} does not chain to the row before it (altered, dropped or reordered)")
        moment = parse_utc(row.get("t_utc"), f"journal row {number} t_utc")
        stamp = ilr.utc_ms(moment, "t_utc")
        if last_ms is not None and stamp < last_ms:
            raise _fail("journal_time_regressed", f"journal row {number} is stamped earlier than the row before it")
        last_ms = stamp
        previous = row["sha256"]
        rows.append(row)
    return rows, previous


def _append_line(path: Path, line: bytes) -> None:
    """The same write the journal makes: append, flush, fsync (a crash leaves the row or a torn tail)."""
    with Path(path).open("ab") as handle:
        handle.write(line)
        handle.flush()
        os.fsync(handle.fileno())


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest() if Path(path).exists() else hashlib.sha256(b"").hexdigest()


# ------------------------------------------------------------------------------------------------------------------ simulation
def _simulate(tail: ilr.IssueLogTail, *, times_ms: Sequence[int], source_id: str, origin: str, channel_id: str | None, instance: str, scratch_dir: Path):
    """Run the UNMODIFIED bridge at each original sync time against a time-limited view. Returns ``(scratch_bridge, expected_lines)``."""
    marks = tail._marks
    start_read = _read_time_of_record(marks, 1) if tail._rseq >= 1 else None
    stop_read = _read_time_of_record(marks, tail._rseq) if tail.stop_record is not None else None
    path = Path(scratch_dir) / "simulated.journal.jsonl"
    first = TimeLimitedTailView(tail, times_ms[0] if times_ms else 0, start_read_ms=start_read, stop_read_ms=stop_read)
    bridge = gc.IssueLogJournalBridge(first, path, source_id=source_id, origin=origin, channel_id=channel_id)   # type: ignore[arg-type]
    bridge.instance = instance
    for stamp in times_ms:
        bridge.tail = TimeLimitedTailView(tail, stamp, start_read_ms=start_read, stop_read_ms=stop_read)         # type: ignore[assignment]
        bridge.sync(ilr.ms_to_utc(stamp))
    lines, torn = _split_journal(path.read_bytes()) if path.exists() else ([], b"")
    if torn:
        raise _fail("simulation_inconsistent", "the simulated journal ended with a fragment")
    return bridge, lines


def _check_start_row(row: Mapping[str, Any], *, tail: ilr.IssueLogTail, source_id: str, origin: str, channel_id: str | None, instance: str) -> None:
    """Explicit, coded checks of the identity, scope and origin a journal declares (the byte comparison would also catch them, but with a less precise reason)."""
    for key, want in (("run_id", tail.policy.run_id), ("source_id", source_id), ("collector_id", "issue-log-reader"), ("collector_instance", instance), ("channel_id", channel_id)):
        if row.get(key) != want:
            raise _fail("journal_identity_mismatch", f"journal {key} is {row.get(key)!r}, this reader is {want!r}")
    if row.get("kind") != KIND_START:
        raise _fail("journal_not_started", "the first journal row is not a start record")
    if row.get("coverage_scope") != SCOPE_SOURCE_STREAM or row.get("coverage_subject") != gc.COVERAGE_SUBJECT:
        raise _fail("coverage_scope_mismatch", f"the journal declares scope {row.get('coverage_scope')!r} / subject {row.get('coverage_subject')!r}; a generator journal declares "
                                               f"{SCOPE_SOURCE_STREAM!r} / {gc.COVERAGE_SUBJECT!r}: no scope is escalated or changed by a restart")
    if row.get("issue_log_origin") != origin:
        raise _fail("origin_mismatch", f"the journal's evidence origin is {row.get('issue_log_origin')!r}, this reader declares {origin!r}")
    log = row.get("issue_log") or {}
    for key, want in (("run_id", tail.policy.run_id), ("worker_id", tail.policy.worker_id), ("clock_domain", tail.policy.clock_domain)):
        if log.get(key) != want:
            raise _fail("journal_identity_mismatch", f"the journal's issue_log.{key} is {log.get(key)!r}, this reader is {want!r}")
    if log.get("head_record_sha256") != tail._head:
        raise _fail("source_log_replaced", "the journal was started over a log whose first record differs from this one")


def _validate_marks(tail: ilr.IssueLogTail) -> None:
    marks = tail._marks
    previous_end, previous_ms = 0, None
    for end, stamp in marks:
        if end <= previous_end or (previous_ms is not None and stamp < previous_ms):
            raise _fail("marks_invalid", "the checkpoint's read marks are not increasing: the original poll schedule cannot be trusted")
        previous_end, previous_ms = end, stamp
    if (marks[-1][0] if marks else 0) != tail._rseq:
        raise _fail("marks_incomplete", "the checkpoint's read marks do not cover every record read: the original read times are not all known")


# ------------------------------------------------------------------------------------------------------------------ recovery
def recover_bridge(tail: sr.RoundBoundTail, journal_path: Path, *, source_id: str, origin: str, channel_id: str | None = None,
                   torn_tail_policy: str = TORN_TRUNCATE, recovery_log_path: Path | None = None,
                   wall_clock: Callable[[], datetime] | None = None) -> tuple[sr.RoundIsolatedBridge, dict[str, Any]]:
    """Rebuild a ``RoundIsolatedBridge`` over ``tail`` (a tail resumed from its checkpoint) and the journal file at ``journal_path``.

    Order: (1) read and verify everything, write nothing; (2) simulate; (3) compare; (4) only then settle a provable torn tail and append the rows the simulation proves are missing;
    (5) transplant the simulated state. Every refusal happens before step 4."""
    if torn_tail_policy not in TORN_POLICIES:
        raise _fail("torn_tail_policy_invalid", f"torn_tail_policy must be one of {TORN_POLICIES}")
    if not isinstance(tail, sr.RoundBoundTail) or tail.round_index is None:
        raise _fail("round_unbound", "recovery needs a RoundBoundTail bound to a round")
    if tail.error is not None:
        raise _fail("tail_in_error", "the tail is in a sticky error state; an integrity failure is never recovered from")
    journal_path = Path(journal_path)
    real = sr.RoundIsolatedBridge(tail, journal_path, source_id=source_id, origin=origin, channel_id=channel_id)
    instance = real.instance
    _validate_marks(tail)

    raw = journal_path.read_bytes() if journal_path.exists() else b""
    lines, torn = _split_journal(raw)
    existing, last_hash = _verify_chain(lines)
    if existing:
        _check_start_row(existing[0], tail=tail, source_id=source_id, origin=origin, channel_id=channel_id, instance=instance)
    for number, row in enumerate(existing, 1):
        for key, want in (("run_id", tail.policy.run_id), ("source_id", source_id), ("collector_instance", instance), ("channel_id", channel_id)):
            if row.get(key) != want:
                raise _fail("journal_identity_mismatch", f"journal row {number}: {key} is {row.get(key)!r}, this reader is {want!r}")
        if row.get("kind") == KIND_ERROR and row.get("error_code") == "issue_log_invalid":
            raise _fail("restart_after_integrity_failure",
                        "the journal records an integrity failure of the issue log; the detection time of a sticky failure cannot be reconstructed, so no restart is supported")

    times = sorted({int(m[1]) for m in tail._marks} | {ilr.utc_ms(parse_utc(r["t_utc"], "t_utc")) for r in existing})
    with tempfile.TemporaryDirectory(prefix="a0q_recovery_") as scratch:
        simulated, expected = _simulate(tail, times_ms=times, source_id=source_id, origin=origin, channel_id=channel_id, instance=instance, scratch_dir=Path(scratch))
        if len(existing) > len(expected):
            raise _fail("journal_ahead_of_checkpoint", f"the journal holds {len(existing)} rows, the checkpoint's read marks explain only {len(expected)}: the checkpoint is older "
                                                       f"than the journal (or the journal belongs to a longer read)")
        for index, line in enumerate(lines):
            if line != expected[index]:
                raise _fail("journal_diverges_from_replay", f"journal row {index + 1} is not the row a deterministic replay of the recorded reads produces: the journal and the "
                                                            f"checkpoint/source disagree (or several polls shared one read time); nothing was changed")
        pending = expected[len(lines):]
        if torn:
            if torn_tail_policy == TORN_REFUSE:
                raise _fail("journal_torn_tail_unreviewed", "the journal ends with an unterminated fragment and torn_tail_policy is 'refuse'")
            if not pending or not (pending[0] + b"\n").startswith(torn) or len(torn) >= len(pending[0]) + 1:
                raise _fail("journal_torn_tail_not_provable", "the journal ends with an unterminated fragment that is not a strict prefix of the next row the replay produces")
        before_sha = _file_sha256(journal_path)
        torn_removed = 0
        if torn:
            size = journal_path.stat().st_size
            with journal_path.open("r+b") as handle:
                handle.truncate(size - len(torn))
                handle.flush()
                os.fsync(handle.fileno())
            torn_removed = len(torn)
        if not journal_path.exists() and expected:
            with journal_path.open("xb"):
                pass
        completed = []
        for line in pending:
            _append_line(journal_path, line + b"\n")
            row = json.loads(line.decode("ascii"))
            completed.append({"seq": row["seq"], "kind": row["kind"], "t_utc": row["t_utc"], "sha256": row["sha256"]})
        # the real bridge continues exactly where the simulated one stopped
        for name in BRIDGE_STATE_FIELDS:
            setattr(real, name, getattr(simulated, name))
        if simulated._journal is not None:
            journal = gc.GeneratorIssueJournal(journal_path, source_id=source_id, collector_id="issue-log-reader", run_id=tail.policy.run_id, channel_id=real.channel_id,
                                               clock=real._clock, resume=True, collector_instance=instance)
            for flag in JOURNAL_FLAGS:
                setattr(journal, flag, getattr(simulated._journal, flag))
            real._journal = journal
    last_row_ms = max([ilr.utc_ms(parse_utc(r["t_utc"], "t_utc")) for r in existing] + [ilr.utc_ms(parse_utc(c["t_utc"], "t_utc")) for c in completed] + [0])
    last_mark_ms = int(tail._marks[-1][1]) if tail._marks else 0
    tail._last_read_ms = max(last_mark_ms, last_row_ms) if (tail._marks or existing or completed) else None     # a later poll may not be stamped earlier than evidence already written
    after_sha = _file_sha256(journal_path) if journal_path.exists() else None
    wall = (wall_clock or (lambda: datetime.now(timezone.utc)))()
    report = {
        "schema": RECOVERY_SCHEMA, "version": RECOVERY_VERSION, "status": ST_RECOVERED if (completed or torn_removed) else ST_NOTHING,
        "reader_instance": instance, "run_id": tail.policy.run_id, "round_index": tail.round_index, "worker_id": tail.policy.worker_id, "source_id": source_id,
        "channel_id": real.channel_id, "evidence_origin": origin,
        "recovery_wall_utc": wall.astimezone(timezone.utc).isoformat(timespec="milliseconds"),
        "recovery_wall_utc_note": "when the restart happened; it is NEVER used as an observation time",
        "tail": {"rseq": tail._rseq, "offset": tail.offset, "chain_head_sha256": tail.chain_head if tail._rseq else None, "read_marks": len(tail._marks),
                 "first_read_ms": int(tail._marks[0][1]) if tail._marks else None, "last_read_ms": last_mark_ms or None},
        "journal": {"rows_before": len(existing), "rows_after": len(existing) + len(completed), "sha256_before": before_sha, "sha256_after": after_sha,
                    "torn_tail_bytes_removed": torn_removed,
                    "torn_tail_fragment_sha256": hashlib.sha256(torn).hexdigest() if torn else None},
        "simulation": {"sync_times": len(times), "basis": "the checkpoint's read marks of every consuming poll plus the times stamped on existing journal rows; the unmodified "
                                                          "IssueLogJournalBridge.sync run against a time-limited view of the tail",
                       "same_time_polls_distinguishable": False},
        "completed_after_restart": completed,
        "completed_rows_were_in_the_journal_before_restart": False,
        "eligibility": ("a completed row keeps the ORIGINAL read time as t_utc, so a decision cutoff at or after that time sees it exactly as it would in an uninterrupted run; "
                        "a decision that was actually made before this restart used the journal prefix whose sha256 is journal.sha256_before and is not changed by it"),
        "not_claimed": ["absolute storage durability", "protection against a storage device that misreports a flush", "power-loss immunity", "a second concurrent bridge"],
    }
    if recovery_log_path is not None:
        body = dict(report)
        line = canonical({**body, "self_sha256": hashlib.sha256(canonical(body).encode("utf-8")).hexdigest()}) + "\n"
        _append_line(Path(recovery_log_path), line.encode("utf-8"))
    return real, report


# ------------------------------------------------------------------------------------------------------------------ reader
class _NeedsRecovery(sc.SourceEvidenceError):
    pass


class RecoverableSourceCountReader(sr.SourceCountReader):
    """``SourceCountReader`` whose ``restart`` also recovers the journal bridge. The legacy class is unchanged and keeps refusing; this one is selected explicitly.

    After ANY exception from a poll (a failed write, a failed fsync, a failed checkpoint) the reader refuses further polls until ``restart()`` is called: the in-memory bridge state is
    then unknown, and only a rebuild from the files is trusted."""

    def __init__(self, *args: Any, torn_tail_policy: str = TORN_TRUNCATE, recovery_log_path: Path | None = None, wall_clock: Callable[[], datetime] | None = None, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.torn_tail_policy, self.recovery_log_path, self.wall_clock = torn_tail_policy, recovery_log_path, wall_clock
        self._needs_restart = False
        self.recovery_report: dict[str, Any] | None = None
        self._bridge_args = (kwargs.get("source_id"), kwargs.get("origin"), kwargs.get("channel_id"))

    def poll(self, read_time: Any) -> dict[str, Any]:
        if self._needs_restart:
            raise _fail("restart_required", "an earlier poll raised: the in-memory bridge state is unknown, call restart() to rebuild it from the checkpoint, the log and the journal")
        try:
            return super().poll(read_time)
        except Exception:
            self._needs_restart = True
            raise

    def restart(self) -> None:
        ident = self.identity
        if ident.checkpoint_path is None:
            raise _fail("checkpoint_path_missing", "a restart needs a checkpoint path")
        if self.bridge is None:
            super().restart()
            self._needs_restart = False
            return
        if not Path(ident.checkpoint_path).is_file():
            if self.journal_path is not None and Path(self.journal_path).exists() and Path(self.journal_path).stat().st_size:
                raise _fail("journal_without_checkpoint", "a journal exists but its checkpoint does not: the original read times cannot be recovered")
            raise _fail("checkpoint_missing", f"{Path(ident.checkpoint_path).name} does not exist: there is nothing to restart from")
        tail = sr.RoundBoundTail.resume(ident.log_path, self.policy, ident.checkpoint_path, round_index=ident.round_index)
        source_id, origin, channel_id = self._bridge_args
        bridge, report = recover_bridge(tail, self.journal_path, source_id=source_id, origin=origin, channel_id=channel_id, torn_tail_policy=self.torn_tail_policy,
                                        recovery_log_path=self.recovery_log_path, wall_clock=self.wall_clock)
        self.tail, self.bridge = tail, bridge
        self.recovery_report = report
        self.first_read_ms = report["tail"]["first_read_ms"]
        self.last_read_ms = tail._last_read_ms
        self.restarts += 1
        self._needs_restart = False

    def evidence(self, **kwargs: Any) -> dict[str, Any]:
        doc = super().evidence(**kwargs)
        doc["recovery"] = None if self.recovery_report is None else {
            "restarts": self.restarts, "status": self.recovery_report["status"], "rows_completed_after_restart": len(self.recovery_report["completed_after_restart"]),
            "recovery_wall_utc_note": self.recovery_report["recovery_wall_utc_note"],
            "poll_history": "per-poll audit entries before the restart are not persisted; the checkpoint keeps one read mark per consuming poll, and those marks are the original read times"}
        return doc
