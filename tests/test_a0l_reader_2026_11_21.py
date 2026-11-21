"""A0l workstream B (checklist items 12-22): the incremental issue-log tail reader.

Every log here is a SYNTHETIC scripted generator (``tests/a0l_support.py``); read times are scripted. Nothing is a measurement of Caliper, Fabric or a clock.
"""

from __future__ import annotations

import ast
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tests import a0l_support as a
from tests.a0l_support import DAY_MS, DOMAIN, RUN, SHORT_GAP_MS, WORKER, Reader, Scripted, SyntheticIssueLog, at, gc, ilr, policy, wc

REPO = Path(__file__).resolve().parents[1]
SHORT_POLICY = policy(max_tick_gap_ms=SHORT_GAP_MS)


def fresh(tmp_path, *, name="g", **kw):
    log = SyntheticIssueLog(tmp_path / f"{name}.jsonl", **kw)
    return log, Scripted(log)


def tail_for(log, **over):
    return ilr.IssueLogTail(log.path, policy(max_tick_gap_ms=SHORT_GAP_MS, **over))


def code(tail) -> str | None:
    return None if tail.error is None else tail.error.code


# ------------------------------------------------------------------------------------------------------------------ 12 valid appended records
def test_12_records_are_consumed_incrementally_and_a_quiet_poll_reports_no_new_data(tmp_path):
    log, gen = fresh(tmp_path)
    tail = tail_for(log)
    assert tail.poll(at(-1)).status == ilr.S_NO_NEW                                     # the empty file exists: nothing to read, nothing proven
    gen.start(-0.5)
    first = tail.poll(at(0))
    assert (first.status, first.records) == (ilr.S_PROGRESS, 1) and tail.offset == len(log.path.read_bytes())
    assert tail.poll(at(0.5)).status == ilr.S_NO_NEW
    gen.tx(1.1)
    gen.tick(2.0)
    second = tail.poll(at(2))
    assert second.records == 4 and second.error_code is None                            # intent, attempt, outcome, tick
    assert tail.counts() == {"intents": 1, "attempted": 1, "returned": 1, "rejected": 0, "ticks": 1, "unresolved": 0, "intent_without_attempt": 0, "records": 5}


def test_12b_event_time_and_read_time_are_kept_apart_and_evidence_never_precedes_its_read(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    gen.tx(1.1)
    tail = tail_for(log)
    tail.poll(at(1.2))                                                                   # read AFTER the event time
    gen.tx(2.1)
    tail.poll(at(30))                                                                    # read 28 s after its event time
    events = tail.attempt_events("i")
    assert [e["ts_ms"] for e in events] == [DAY_MS + 1100, DAY_MS + 2100] and [e["read_ms"] for e in events] == [DAY_MS + 1200, DAY_MS + 30_000]
    assert [e["seq"] for e in events] == [0, 1] and events[0]["id"] == f"{RUN}:1:0"
    assert [e["seq"] for e in tail.attempt_events("i", as_of_ms=DAY_MS + 2500)] == [0]  # the second record had not been read yet
    assert tail.attempt_events("i", as_of_ms=DAY_MS + 1100) == []                       # nothing is visible at its own event time


# ------------------------------------------------------------------------------------------------------------------ 13 partial trailing record
def test_13_a_partial_trailing_record_is_not_consumed_is_reported_and_completes_later(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    gen.tx(1.1)
    full = log.path.read_bytes()
    row = log.body("tick", 2.0, attempted=1, intents=1, outcomes=1)
    line = (wc.canonical(row) + "\n").encode("ascii")
    log.path.write_bytes(full + line[:40])
    tail = tail_for(log)
    result = tail.poll(at(2))
    assert result.status == ilr.S_PROGRESS and tail.offset == len(full) and tail.partial_tail_bytes == 40 and tail.error is None
    assert tail.writer_state()["partial_tail_bytes"] == 40
    assert tail.evidence(instance="i", max_callback_lag_ms=500).torn_tail is True       # nothing after the last complete row will be certified
    log.path.write_bytes(full + line)
    done = tail.poll(at(3))
    assert done.records == 1 and tail.partial_tail_bytes == 0 and tail.counts()["ticks"] == 1


def test_13b_a_file_that_holds_only_a_partial_record_reports_partial_tail_and_consumes_nothing(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    data = log.path.read_bytes()
    log.path.write_bytes(data[:30])
    tail = tail_for(log)
    result = tail.poll(at(0))
    assert (result.status, result.records, result.offset, result.partial_tail_bytes) == (ilr.S_PARTIAL, 0, 0, 30) and tail.error is None
    assert tail.writer_state()["state"] == ilr.W_NOT_STARTED
    with pytest.raises(ilr.IssueLogError) as info:
        tail.evidence(instance="i", max_callback_lag_ms=500)
    assert info.value.code == "issue_log_not_started"


# ------------------------------------------------------------------------------------------------------------------ 14 corrupt record
def _lines_of(log):
    return log.path.read_bytes().split(b"\n")[:-1]


def _write_lines(log, lines):
    log.path.write_bytes(b"".join(line + b"\n" for line in lines))


def _tamper(line: bytes, field: str, value) -> bytes:
    doc = json.loads(line)
    doc[field] = value
    return wc.canonical(doc).encode("ascii")


@pytest.mark.parametrize("how,expected", [
    ("garbage", "record_malformed"), ("not_ascii", "record_malformed"), ("blank", "record_blank"), ("array", "record_malformed"),
    ("field_changed", "hash_mismatch"), ("respaced", "record_not_canonical"), ("wrong_schema", "schema_mismatch"), ("bad_kind", "hash_mismatch"),
    ("chain_cut", "hash_chain_broken"),
])
def test_14_a_corrupt_record_is_a_sticky_error_and_the_evidence_is_refused(tmp_path, how, expected):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    gen.tx(1.1)
    gen.tick(2.0)
    lines = _lines_of(log)
    target = 2
    lines[target] = {"garbage": b"{not json", "not_ascii": "café".encode("utf-8"), "blank": b"", "array": b"[1,2]",
                     "field_changed": _tamper(lines[target], "t_ms", json.loads(lines[target])["t_ms"] + 1),
                     "respaced": json.dumps(json.loads(lines[target]), sort_keys=True).encode("ascii"),
                     "wrong_schema": _tamper(lines[target], "schema", "e1_issue_log/9"),
                     "bad_kind": _tamper(lines[target], "kind", "teleport"),
                     "chain_cut": _tamper(lines[target], "prev", "f" * 64)}[how]
    _write_lines(log, lines)
    tail = tail_for(log)
    result = tail.poll(at(3))
    assert result.status == ilr.S_ERROR and result.error_code == expected, (how, result.error_code)
    assert tail.counts()["records"] == 2                                                # nothing at or after the bad record was accepted
    _write_lines(log, _lines_of(log)[:target])                                          # even a repaired file does not clear a sticky error
    assert tail.poll(at(4)).status == ilr.S_ERROR and code(tail) == expected
    assert tail.error_read_ms == DAY_MS + 3000 and tail.writer_state()["state"] == ilr.W_ERROR
    with pytest.raises(ilr.IssueLogError):
        tail.evidence(instance="i", max_callback_lag_ms=500)


# ------------------------------------------------------------------------------------------------------------------ 15 sequence gaps
def test_15_a_missing_line_is_a_record_sequence_gap(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    gen.tx(1.1)
    gen.tx(2.1)
    lines = _lines_of(log)
    del lines[3]
    _write_lines(log, lines)
    tail = tail_for(log)
    assert tail.poll(at(3)).error_code == "record_sequence_gap"


def test_15b_a_skipped_issue_number_with_a_valid_chain_is_an_issue_sequence_gap(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    gen.tx(1.1)
    log.n = 5                                                                           # the generator "lost" transactions 1..4 but kept chaining
    log.issue(2.1)
    tail = tail_for(log)
    assert tail.poll(at(3)).error_code == "issue_sequence_gap"
    assert tail.counts()["intents"] == 1


# ------------------------------------------------------------------------------------------------------------------ 16 duplicates
def test_16_a_repeated_line_is_a_duplicate_record(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    gen.tx(1.1)
    lines = _lines_of(log)
    lines.insert(3, lines[2])
    _write_lines(log, lines)
    assert tail_for(log).poll(at(3)).error_code == "record_duplicate"


def test_16b_a_repeated_issue_number_or_transaction_sequence_is_refused(tmp_path):
    log, gen = fresh(tmp_path, name="a")
    gen.start(-0.5)
    gen.tx(1.1)
    log.n = 0                                                                           # the same n issued twice
    log.issue(2.1)
    assert tail_for(log).poll(at(3)).error_code == "issue_duplicate"
    log2, gen2 = fresh(tmp_path, name="b")
    gen2.start(-0.5)
    gen2.tx(1.1)
    log2.tx_seq = 0                                                                     # a retry: the same transaction sequence issued again under a new n
    log2.issue(2.1)
    assert tail_for(log2).poll(at(3)).error_code == "tx_seq_regressed"
    log3, gen3 = fresh(tmp_path, name="c")
    gen3.start(-0.5)
    n = log3.intent(1.1, issue_id="run-a0j:1:77")
    assert tail_for(log3).poll(at(3)).error_code == "issue_id_mismatch" and n == 0


# ------------------------------------------------------------------------------------------------------------------ 17 replacement, 18 truncation
def test_17_a_replaced_file_is_detected_whether_or_not_its_length_changed(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    gen.tx(1.1)
    tail = tail_for(log)
    tail.poll(at(2))
    original = log.path.read_bytes()
    other, other_gen = fresh(tmp_path, name="other", run_id=RUN)
    other_gen.start(-0.4)                                                               # a different start time: different bytes, same shape
    other_gen.tx(1.2)
    replacement = other.path.read_bytes()
    log.path.write_bytes(replacement + b"x" * max(0, len(original) - len(replacement)))
    assert tail.poll(at(3)).error_code == "log_replaced"


def test_17b_a_removed_file_after_reading_is_an_error_but_a_not_yet_created_file_is_not(tmp_path):
    path = tmp_path / "later.jsonl"
    tail = ilr.IssueLogTail(path, policy(max_tick_gap_ms=SHORT_GAP_MS))
    result = tail.poll(at(0))
    assert result.status == ilr.S_MISSING and tail.error is None                          # a writer that has not started is not an error and not evidence
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    tail2 = tail_for(log)
    tail2.poll(at(0))
    log.path.unlink()
    assert tail2.poll(at(1)).error_code == "log_missing_after_start"


def test_18_a_truncated_file_is_detected(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    gen.tx(1.1)
    gen.tick(2.0)
    tail = tail_for(log)
    tail.poll(at(2))
    data = log.path.read_bytes()
    log.path.write_bytes(data[:len(data) - 50])
    assert tail.poll(at(3)).error_code == "log_truncated"


# ------------------------------------------------------------------------------------------------------------------ 19 restart checkpoint
def _consume_in_steps(tmp_path, *, checkpoint=True):
    log, gen = fresh(tmp_path)
    ckpt = tmp_path / "g.ckpt.json"
    tail = ilr.IssueLogTail(log.path, policy(max_tick_gap_ms=SHORT_GAP_MS), checkpoint_path=ckpt if checkpoint else None)
    gen.start(-0.5)
    tail.poll(at(-0.4))
    for sec in range(1, 6):
        gen.tx(sec - 0.9)
        gen.tick(sec)
        tail.poll(at(sec + 0.25))
    return log, gen, tail, ckpt


def test_19_a_restart_resumes_from_the_checkpoint_with_the_original_read_times(tmp_path):
    log, gen, tail, ckpt = _consume_in_steps(tmp_path)
    resumed = ilr.IssueLogTail.resume(log.path, policy(max_tick_gap_ms=SHORT_GAP_MS), ckpt)
    assert resumed.offset == tail.offset and resumed.chain_head == tail.chain_head and resumed.counts() == tail.counts()
    assert resumed.attempt_events("i") == tail.attempt_events("i")                       # each record keeps the read time of the poll that first saw it
    assert resumed.observed_segments() == tail.observed_segments()
    gen.tx(5.2)
    gen.tick(6.0)
    for t in (tail, resumed):
        t.poll(at(6.5))
    assert resumed.counts() == tail.counts() and resumed.attempt_events("i") == tail.attempt_events("i")
    assert resumed.attempt_events("i")[-1]["read_ms"] == DAY_MS + 6500 and resumed.attempt_events("i")[0]["read_ms"] == DAY_MS + 1250
    assert not (tmp_path / "g.ckpt.json.tmp").exists()                                   # atomic replace left nothing behind


def test_19b_a_resume_after_more_records_were_written_continues_where_the_checkpoint_stopped(tmp_path):
    log, gen, tail, ckpt = _consume_in_steps(tmp_path)
    gen.tx(5.2)
    gen.tick(6.0)                                                                       # written while the reader was down
    resumed = ilr.IssueLogTail.resume(log.path, policy(max_tick_gap_ms=SHORT_GAP_MS), ckpt)
    assert resumed.counts()["attempted"] == 5
    resumed.poll(at(9))
    assert resumed.counts()["attempted"] == 6 and resumed.attempt_events("i")[-1]["read_ms"] == DAY_MS + 9000


@pytest.mark.parametrize("how,expected", [
    ("prefix_edited", {"hash_mismatch", "record_not_canonical", "log_replaced"}), ("truncated", {"log_truncated"}),
    ("other_policy", {"checkpoint_mismatch"}), ("checkpoint_edited", {"checkpoint_corrupt"}), ("checkpoint_missing", {"checkpoint_unreadable"}),
    ("other_run", {"checkpoint_mismatch"}), ("log_missing", {"log_missing_after_start"}), ("log_swapped", {"log_replaced", "hash_mismatch", "hash_chain_broken"}),
])
def test_19c_a_resume_is_refused_for_an_edited_truncated_swapped_or_mismatched_log_or_checkpoint(tmp_path, how, expected):
    log, gen, tail, ckpt = _consume_in_steps(tmp_path)
    pol = policy(max_tick_gap_ms=SHORT_GAP_MS)
    if how == "prefix_edited":
        lines = _lines_of(log)
        lines[1] = _tamper(lines[1], "t_ms", json.loads(lines[1])["t_ms"] + 1)
        _write_lines(log, lines)
    elif how == "truncated":
        log.path.write_bytes(log.path.read_bytes()[:-30])
    elif how == "other_policy":
        pol = policy(max_tick_gap_ms=SHORT_GAP_MS + 1)
    elif how == "checkpoint_edited":
        ckpt.write_text(ckpt.read_text(encoding="ascii").replace('"offset":', '"offset": 1,"x":'), encoding="ascii")
    elif how == "checkpoint_missing":
        ckpt.unlink()
    elif how == "other_run":
        pol = policy(max_tick_gap_ms=SHORT_GAP_MS, run_id="another-run")
    elif how == "log_missing":
        log.path.unlink()
    elif how == "log_swapped":
        other, other_gen = fresh(tmp_path, name="other")
        other_gen.start(-0.4)
        for sec in range(1, 6):
            other_gen.tx(sec - 0.8)
            other_gen.tick(sec)
        data = other.path.read_bytes()
        mine = len(log.path.read_bytes())
        log.path.write_bytes((data + b" " * mine)[:mine] if len(data) >= mine else data)
    with pytest.raises(ilr.IssueLogError) as info:
        ilr.IssueLogTail.resume(log.path, pol, ckpt)
    assert info.value.code in expected, (how, info.value.code)


# ------------------------------------------------------------------------------------------------------------------ 20 timestamps
def test_20_read_times_are_timezone_aware_and_normalised_to_utc_and_a_naive_time_is_refused(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    tail = tail_for(log)
    with pytest.raises(ilr.IssueLogError) as info:
        tail.poll(datetime(2026, 9, 21, 0, 0, 1))
    assert info.value.code == "timestamp_naive"
    local = at(2).astimezone(timezone(timedelta(hours=5, minutes=30)))
    assert ilr.utc_ms(local) == ilr.utc_ms(at(2)) == DAY_MS + 2000
    tail.poll(local)
    assert tail._marks[-1][1] == DAY_MS + 2000
    assert ilr.ms_to_utc(DAY_MS) == a.j.DAY_START and ilr.ms_to_utc(DAY_MS + 1500).microsecond == 500_000
    with pytest.raises(ilr.IssueLogError):
        ilr.utc_ms("2026-09-21T00:00:00Z")                                                # a string is not a time


def test_20b_clock_regressions_and_steps_are_errors_not_adjustments(tmp_path):
    for case in ("wall_back", "mono_back", "wall_step", "read_back"):
        log, gen = fresh(tmp_path, name=case)
        gen.start(-0.5)
        gen.tx(1.1)
        if case == "wall_back":
            log.raw("tick", 0.5, attempted=1, intents=1, outcomes=1, mono_ms=2000)
        elif case == "mono_back":
            log.raw("tick", 2.0, attempted=1, intents=1, outcomes=1, mono_ms=10)
        elif case == "wall_step":
            log.raw("tick", 3600.0, attempted=1, intents=1, outcomes=1, mono_ms=2500)       # one hour of wall clock in 0.9 s of monotonic time
        tail = tail_for(log)
        if case == "read_back":
            tail.poll(at(5))
            result = tail.poll(at(4))
        else:
            result = tail.poll(at(5))
        expected = {"wall_back": "clock_regression", "mono_back": "monotonic_regression", "wall_step": "wall_clock_step", "read_back": "read_clock_regressed"}[case]
        assert result.error_code == expected, (case, result.error_code)


# ------------------------------------------------------------------------------------------------------------------ 21 identity
@pytest.mark.parametrize("how,expected", [
    ("run", "run_identity_mismatch"), ("worker", "worker_identity_mismatch"), ("roster", "worker_roster_mismatch"), ("domain", "clock_domain_mismatch"),
    ("two_workers", "worker_count_unsupported"), ("semantics", "timestamp_semantics_mismatch"), ("index", "worker_identity_mismatch"),
])
def test_21_a_log_of_another_run_worker_roster_or_clock_domain_is_refused(tmp_path, how, expected):
    kw, start_over = {}, {}
    if how == "run":
        kw["run_id"] = "another-run"
    elif how == "worker":
        kw.update(worker_id="w1", roster=["w1"])
    elif how == "roster":
        kw["roster"] = ["w0", "w1"]
    elif how == "domain":
        kw["clock_domain"] = "another-host:node_date_now"
    elif how == "two_workers":
        kw["total_workers"] = 2
    elif how == "semantics":
        start_over["event_time_semantics"] = "recorder_callback_time"
    elif how == "index":
        start_over["worker_index"] = 3
    log, gen = fresh(tmp_path, **kw)
    gen.start(-0.5, **start_over)
    tail = tail_for(log)
    assert tail.poll(at(0)).error_code == expected
    assert tail.counts()["records"] == 0


def test_21b_records_of_a_second_worker_inside_one_log_are_refused_not_merged(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    gen.tx(1.1)
    log.worker_id = "w1"                                                                # a different writer appends to the same file
    log.tick(2.0, attempted=1, intents=1, outcomes=1)
    tail = tail_for(log)
    assert tail.poll(at(3)).error_code == "worker_identity_mismatch"


def test_21c_the_policy_itself_must_name_a_consistent_identity():
    for kw in ({"run_id": ""}, {"worker_id": " "}, {"clock_domain": ""}, {"expected_roster": ("w5",)}, {"max_tick_gap_ms": 0}, {"max_clock_skew_ms": True},
               {"max_bytes_per_poll": 10, "max_line_bytes": 100}):
        with pytest.raises(ilr.IssueLogError):
            policy(**kw)
    assert policy().expected_roster == (WORKER,) and "no scientific justification" in policy().as_dict()["basis"]


# ------------------------------------------------------------------------------------------------------------------ 22 bounded reads
def test_22_each_poll_is_bounded_by_the_record_and_byte_limits_and_reports_more_available(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    for k in range(60):
        gen.tx(1.0 + k * 0.01)
    gen.tick(2.0)
    total = len(_lines_of(log))
    tail = ilr.IssueLogTail(log.path, policy(max_tick_gap_ms=SHORT_GAP_MS, max_records_per_poll=7))
    seen, polls, last = 0, 0, None
    while True:
        last = tail.poll(at(3))
        polls += 1
        assert last.records <= 7
        seen += last.records
        if not last.more_available:
            break
    assert seen == total == tail.counts()["records"] and polls == -(-total // 7) and last.error_code is None
    small = ilr.IssueLogTail(log.path, policy(max_tick_gap_ms=SHORT_GAP_MS, max_bytes_per_poll=1200, max_line_bytes=1024))
    polls = 0
    while True:
        result = small.poll(at(3))
        polls += 1
        assert result.bytes_consumed <= 1200 and result.error_code is None
        if small.counts()["records"] == total:
            break
        assert polls < 500
    assert small.counts() == tail.counts() and small.chain_head == tail.chain_head and polls > 10


def test_22b_a_line_longer_than_the_limit_is_an_error_not_an_unbounded_read(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    with log.path.open("ab") as handle:
        handle.write(b"x" * 3000)
    tail = ilr.IssueLogTail(log.path, policy(max_tick_gap_ms=SHORT_GAP_MS, max_bytes_per_poll=4096, max_line_bytes=1024))
    assert tail.poll(at(1)).error_code == "line_too_long"


def test_22b2_a_file_that_holds_only_an_unterminated_over_long_line_is_refused_without_reading_it_all(tmp_path):
    path = tmp_path / "long.jsonl"
    path.write_bytes(b"x" * 3000)                                                       # no newline at all: not even one complete record
    tail = ilr.IssueLogTail(path, policy(max_tick_gap_ms=SHORT_GAP_MS, max_bytes_per_poll=4096, max_line_bytes=1024))
    result = tail.poll(at(1))
    assert result.error_code == "line_too_long" and result.records == 0 and tail.offset == 0


def test_22c_memory_is_kept_per_event_not_per_file(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    for k in range(200):
        gen.tx(1.0 + k * 0.02)
    tail = tail_for(log)
    tail.poll(at(3))
    assert len(tail._attempt_ms) == 200 and tail._attempt_ms.itemsize == 8 and not hasattr(tail, "_lines") and len(tail._tail_buf) <= ilr.TAIL_BYTES


# ------------------------------------------------------------------------------------------------------------------ the state machine
def _bad_sequence(log, gen, how):
    if how == "first_not_start":
        log.raw("tick", 1.0, attempted=0, intents=0, outcomes=0)
        return "journal_not_started"
    gen.start(-0.5)
    if how == "start_repeated":
        log.start(0.5)
        return "start_repeated"
    if how == "attempt_without_intent":
        log.attempt(1.0, 0)
        return "attempt_without_intent"
    if how == "outcome_without_attempt":
        log.outcome(1.0, 0)
        return "outcome_without_attempt"
    if how == "outcome_twice":
        n = log.issue(1.0)
        log.outcome(1.2, n)
        return "outcome_duplicate"
    if how == "intent_then_tick":
        log.intent(1.0)
        log.raw("tick", 1.1, attempted=0, intents=1, outcomes=0)
        return "attempt_ordering_violation"
    if how == "intent_twice":
        log.intent(1.0)
        log.intent(1.1)
        return "attempt_ordering_violation"
    if how == "tick_counts":
        gen.tx(1.0)
        log.raw("tick", 2.0, attempted=5, intents=1, outcomes=1)
        return "tick_count_mismatch"
    if how == "stop_counts":
        gen.tx(1.0)
        log.raw("stop", 2.0, reason="cleanup", attempted=1, intents=1, outcomes=0, in_flight=0)
        return "stop_count_mismatch"
    if how == "record_after_stop":
        gen.stop(2.0)
        gen.tick(3.0)
        return "record_after_stop"
    if how == "bad_result":
        n = log.issue(1.0, outcome_after=None)
        log.outcome(1.1, n, "teleported")
        return "record_field_invalid"
    if how == "attempt_wrong_n":
        n = log.intent(1.0)
        log.attempt(1.0, n + 3)
        return "attempt_without_intent"
    raise AssertionError(how)


@pytest.mark.parametrize("how", ["first_not_start", "start_repeated", "attempt_without_intent", "outcome_without_attempt", "outcome_twice", "intent_then_tick",
                                 "intent_twice", "tick_counts", "stop_counts", "record_after_stop", "bad_result", "attempt_wrong_n"])
def test_22d_every_state_machine_violation_is_a_named_sticky_error(tmp_path, how):
    log, gen = fresh(tmp_path)
    expected = _bad_sequence(log, gen, how)
    tail = tail_for(log)
    assert tail.poll(at(5)).error_code == expected, (how, code(tail))


def test_22e_unresolved_attempts_and_an_intent_without_attempt_are_reported_not_resolved(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    gen.tx(1.0, outcome_after=None)                                                     # attempt, no outcome: the call's result is unknown
    gen.tx(1.5)
    log.intent(1.8)                                                                     # intent, no attempt: the call was NOT made
    tail = tail_for(log)
    tail.poll(at(2))
    assert tail.error is None
    counts = tail.counts()
    assert (counts["intents"], counts["attempted"], counts["returned"], counts["unresolved"], counts["intent_without_attempt"]) == (3, 2, 1, 1, 1)
    assert tail.unresolved_ns() == [0]
    state = tail.writer_state()
    assert state["unresolved_attempts"] == 1 and state["intent_without_attempt"] is True
    assert len(tail.attempt_events("i")) == 2                                           # the unsent intent is not an issuance


def test_22f_a_unresolved_attempt_limit_is_enforced(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    for k in range(5):
        gen.tx(1.0 + k * 0.01, outcome_after=None)
    tail = ilr.IssueLogTail(log.path, policy(max_tick_gap_ms=SHORT_GAP_MS, max_unresolved=3))
    assert tail.poll(at(2)).error_code == "unresolved_limit_exceeded"


# ------------------------------------------------------------------------------------------------------------------ what a quiet log does and does not mean
def test_22g_a_quiet_log_with_ticks_is_alive_a_log_whose_ticks_stop_is_unobserved_and_a_stop_is_a_stop(tmp_path):
    log, gen = fresh(tmp_path, name="alive")
    gen.start(-0.5)
    for sec in range(1, 11):
        gen.tick(sec)                                                                   # no transactions at all, but the writer is demonstrably alive
    tail = tail_for(log)
    tail.poll(at(10))
    assert tail.observed_segments() == [(DAY_MS - 500, DAY_MS + 10_000)] and tail.counts()["attempted"] == 0
    assert tail.writer_state()["state"] == ilr.W_ACTIVE

    log2, gen2 = fresh(tmp_path, name="silent")
    gen2.start(-0.5)
    for sec in range(1, 6):
        gen2.tick(sec)
    tail2 = tail_for(log2)
    tail2.poll(at(5))
    before = tail2.observed_segments()
    tail2.poll(at(500))                                                                 # 495 s of silence: still "active per last liveness point", not covered
    assert tail2.observed_segments() == before == [(DAY_MS - 500, DAY_MS + 5000)] and tail2.writer_state()["last_liveness_point_ms"] == DAY_MS + 5000

    log3, gen3 = fresh(tmp_path, name="stopped")
    gen3.start(-0.5)
    gen3.tick(1.0)
    gen3.stop(2.0)
    tail3 = tail_for(log3)
    tail3.poll(at(3))
    assert tail3.writer_state()["state"] == ilr.W_CLOSED_CLEAN and tail3.stop_record["reason"] == "cleanup"
    assert tail3.observed_segments() == [(DAY_MS - 500, DAY_MS + 2000)]


def test_22h_a_gap_between_liveness_points_is_unobserved_even_though_records_continue(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    gen.tick(1.0)
    gen.tick(2.0)
    gen.tx(10.0)                                                                        # activity during the silence of the ticks
    gen.tick(20.0)                                                                      # 18 s after the last tick: over the 5 s allowance
    gen.tick(21.0)
    tail = tail_for(log)
    tail.poll(at(21))
    assert tail.observed_segments() == [(DAY_MS - 500, DAY_MS + 2000), (DAY_MS + 20_000, DAY_MS + 21_000)]
    assert tail.gaps == ((DAY_MS + 2000, DAY_MS + 20_000),) and tail.writer_state()["unobserved_gaps"] == 1


def test_22i_segments_become_visible_only_when_their_closing_point_has_been_read(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    gen.tick(1.0)
    tail = tail_for(log)
    tail.poll(at(1.5))
    gen.tick(2.0)
    tail.poll(at(9.0))
    assert tail.observed_segments(as_of_ms=DAY_MS + 5000) == [(DAY_MS - 500, DAY_MS + 1000)]
    assert tail.observed_segments(as_of_ms=DAY_MS + 9000) == [(DAY_MS - 500, DAY_MS + 2000)]


# ------------------------------------------------------------------------------------------------------------------ the reader is not fed by the recorder
def _code_strings(tree: ast.AST) -> list[str]:
    doc_owners = [n for n in ast.walk(tree) if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))]
    docs = {id(n.body[0].value) for n in doc_owners if n.body and isinstance(n.body[0], ast.Expr) and isinstance(getattr(n.body[0], "value", None), ast.Constant)}
    return [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docs]


def test_22j_the_reader_imports_no_recorder_module_and_names_no_recorder_file():
    tree = ast.parse((REPO / "src/fabric_perf/controllers/issue_log_reader.py").read_text(encoding="utf-8"))
    imported = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
               {f"{'.' * n.level}{n.module or ''}:{a.name}" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}
    assert not [i for i in imported if "recorder" in i.lower()], imported
    for text in _code_strings(tree):
        assert not any(bad in text.lower() for bad in ("submits.csv", "live.submits", "recorder.csv", "health_spool", "e1_per_tx")), text
    coverage_tree = ast.parse((REPO / "src/fabric_perf/controllers/issue_log_coverage.py").read_text(encoding="utf-8"))
    names = {n.module or "" for n in ast.walk(coverage_tree) if isinstance(n, ast.ImportFrom)}
    assert not [n for n in names if "recorder" in n]


def test_22k_the_evidence_does_not_depend_on_any_recorder_file(tmp_path):
    log, gen = fresh(tmp_path)
    gen.start(-0.5)
    for sec in range(1, 6):
        gen.tx(sec - 0.9)
        gen.tick(sec)
    tail = tail_for(log)
    tail.poll(at(5))
    first = tail.evidence(instance="rec:x", max_callback_lag_ms=500)
    junk = tmp_path / "x.live.submits.csv"
    junk.write_text("seq_submit,t_submit_evt_ms,count\n0,1,999\n", encoding="utf-8")        # a recorder file that disagrees completely
    tail2 = tail_for(log)
    tail2.poll(at(5))
    second = tail2.evidence(instance="rec:x", max_callback_lag_ms=500)
    assert first == second and first.dispatch_ms == tuple((n, DAY_MS + 100 + 1000 * n) for n in range(5))
