"""A0j recorder instrumentation (checklist items 1-12): the recorder health spool is replayed through the one collector-health journal.

SYNTHETIC: a scripted recorder (``a0j_support.SyntheticRecorder``) on a scripted clock. Nothing is a Caliper or Fabric measurement.
"""

from __future__ import annotations

import json
import os

import pytest

from tests import a0j_support as j
from tests.a0j_support import at, bridge, rc, wc

SHORT = 20            # seconds of synthetic traffic for the cheap tests


def short(tmp_path, **kwargs):
    """A recorder with traffic and one beat per second for ``SHORT`` seconds (policy: 1 s interval, 5 s gap)."""
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, **{k: v for k, v in kwargs.items() if k in ("name", "instance", "source_count", "scope", "origin")})
    rec.start()
    rec.traffic(range(0, SHORT + 1), zero=kwargs.get("zero", ()), missed=kwargs.get("missed", ()))
    for sec in range(1, SHORT + 1):
        rec.beat(sec)
    return rec


FAST = bridge.RecorderHealthPolicy(heartbeat_interval_s=1, max_heartbeat_gap_s=5.0, freshness_limit_s=10.0, clock_tolerance_ms=250.0)


def journal_rows(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


# ---------------------------------------------------------------------------------------------------------- 1 initialization, 2 start
def test_01_the_start_row_records_initialization_scope_origin_and_the_declared_policy(tmp_path):
    rec = short(tmp_path)
    rec.stop(SHORT)
    report = j.ingest(rec, policy=FAST)
    first = journal_rows(tmp_path / "journal.jsonl")[0]
    assert first["kind"] == "start" and first["coverage_scope"] == "recorder_callback" and first["evidence_origin"] == "synthetic_fixture"
    assert first["lifecycle"]["initialized_utc"] < first["lifecycle"]["subscription_active_utc"] <= first["lifecycle"]["window_open_utc"]
    assert first["declared_policy"]["heartbeat_interval_ms"] == 1000 and first["recorder_version"] and first["options_sha256"]
    assert report["complete"] is True and report["coverage_scope"] == "recorder_callback"


def test_02_observation_never_starts_before_the_start_row_is_written(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000).start(t=5.0, observation=2)       # claims observation from 2 s, written at 5 s
    rec.traffic(range(2, 10))
    rec.beat(6)
    report = j.ingest(rec, policy=FAST)
    assert [p["code"] for p in report["problems"] if p["code"] == "retroactive_coverage_claim"] == ["retroactive_coverage_claim"]
    assert report["spool_rows_ingested"] == 0 and report["complete"] is False


# --------------------------------------------------------------------------------- 3 observation, 4 heartbeat, 5 checkpoint, 6 end
def test_03_every_heartbeat_count_equals_the_units_in_the_live_submits_file(tmp_path):
    rec = short(tmp_path)
    rec.stop(SHORT)
    j.ingest(rec, policy=FAST)
    events, report = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    assert report["units"] == sum(e["units"] for e in events) and report["torn_tail_ignored"] is False
    coverage = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=FAST)
    snap = coverage.snapshot(at(SHORT + 1))
    assert snap.valid and snap.submit_units_visible == report["units"]


def test_04_heartbeat_rows_keep_phase_spool_position_and_source_count(tmp_path):
    rec = short(tmp_path)
    rec.stop(SHORT)
    j.ingest(rec, policy=FAST)
    beats = [r for r in journal_rows(tmp_path / "journal.jsonl") if r["kind"] == "heartbeat"]
    assert len(beats) == SHORT and all(b["phase"] == "heartbeat" and b["health"] == "healthy" for b in beats)
    assert [b["spool_n"] for b in beats] == list(range(2, SHORT + 2))
    assert all(b["source_basis"] == j.SOURCE_BASIS and b["source_independence_claim"] is True for b in beats)


def test_05_a_checkpoint_keeps_its_progress_counters(tmp_path):
    rec = short(tmp_path)
    rec.rows[10]["phase"] = "checkpoint"
    rec.rows[10]["progress"] = {"submitted_total": 99, "finished_total": 90, "pending_total": 9, "observer_errors": 0}
    rec.stop(SHORT)
    j.ingest(rec, policy=FAST)
    row = [r for r in journal_rows(tmp_path / "journal.jsonl") if r.get("phase") == "checkpoint"][0]
    assert row["progress"]["pending_total"] == 9


def test_06_a_clean_stop_ends_collection_and_the_report_says_so(tmp_path):
    rec = short(tmp_path)
    rec.stop(SHORT)
    report = j.ingest(rec, policy=FAST)
    assert report["has_stop"] is True and report["stop_reason"] == "clean" and report["complete"] is True and report["problems"] == []
    assert journal_rows(tmp_path / "journal.jsonl")[-1]["kind"] == "stop"


def test_06b_an_unknown_phase_is_refused(tmp_path):
    rec = short(tmp_path)
    rec.rows[3]["phase"] = "subscription_active"                  # lifecycle markers that precede observation ride on the start row only
    with pytest.raises(bridge.BridgeError) as excinfo:
        j.ingest(rec, policy=FAST)
    assert excinfo.value.code == "spool_field_invalid"


# --------------------------------------------------------------------------------------------------- 7 interruption, 8 restart
def test_07_a_killed_recorder_leaves_no_stop_and_none_is_invented(tmp_path):
    rec = short(tmp_path)                                          # never stopped: the process died
    report = j.ingest(rec, policy=FAST)
    assert report["has_stop"] is False and report["complete"] is False
    assert [p["code"] for p in report["problems"]] == ["no_stop_row"]
    assert all(r["kind"] != "stop" for r in journal_rows(tmp_path / "journal.jsonl"))
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    snap = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=FAST) \
        .snapshot(at(SHORT + 30))
    cls, _ = snap.classify(at(SHORT + 10), 0)
    assert cls in (wc.CoverageClass.STALE_COLLECTOR_EVIDENCE, wc.CoverageClass.UNKNOWN_COVERAGE_ZERO)       # newer than the last watermark: never certified


def test_08_a_restart_opens_a_new_instance_and_never_certifies_the_gap(tmp_path):
    first = short(tmp_path, name="a", instance="rec:run-a0j:w0:r0")
    first.stop(10)                                                 # first round covers 0..10 and stops cleanly
    first.rows = first.rows[:11] + [first.rows[-1]]
    j.ingest(first, policy=FAST)
    second = j.SyntheticRecorder(tmp_path, name="b", instance="rec:run-a0j:w0:r1", declared_interval_ms=1000, round_=1)
    second.start(t=14.5, observation=15)
    second.traffic(range(0, 11))                                   # nothing between 10 and 15 was observed by anyone
    second.traffic(range(15, 25))
    for sec in range(16, 25):
        second.beat(sec)
    second.stop(24)
    report = j.ingest(second, policy=FAST, resume=True)
    assert report["complete"] is True
    events = rc.read_submit_events(first.submits_path, instance=first.instance)[0] + rc.read_submit_events(second.submits_path, instance=second.instance)[0]
    snap = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=FAST) \
        .snapshot(at(30))
    assert snap.valid
    for second_in_gap in (11, 12, 14):
        cls, _ = snap.classify(at(second_in_gap), 0)
        assert cls != wc.CoverageClass.VERIFIED_ZERO, second_in_gap                          # nothing in the gap is a genuine zero
    assert snap.classify(at(20), 5)[0] == wc.CoverageClass.OBSERVED_POSITIVE


# --------------------------------------------------------------------------------------------------------------- 9 write failure
def test_09_a_refused_journal_write_raises_and_certifies_nothing(tmp_path, monkeypatch):
    rec = short(tmp_path)
    rec.stop(SHORT)
    rec.write()
    calls = {"n": 0}
    real_fsync = os.fsync

    def failing(fd):
        calls["n"] += 1
        if calls["n"] > 3:
            raise OSError("disk full (synthetic)")
        return real_fsync(fd)
    monkeypatch.setattr(os, "fsync", failing)
    with pytest.raises(bridge.BridgeError) as excinfo:
        j.ingest(rec, policy=FAST)
    assert excinfo.value.code == "journal_write_failed"
    monkeypatch.undo()
    events, torn, _ = wc.read_journal_file(tmp_path / "journal.jsonl")
    assert len(events) < len(rec.rows) and not any(e["kind"] == "stop" for e in events)       # the failed row may be written but was never confirmed durable


def test_09b_a_torn_spool_tail_is_reported_and_dropped_not_repaired(tmp_path):
    rec = short(tmp_path)
    rec.write(torn_tail='{"schema":"recorder_health_spool/1","n":99,"kind":"heart')
    report = j.ingest(rec, policy=FAST)
    codes = [p["code"] for p in report["problems"]]
    assert "spool_torn_tail" in codes and report["complete"] is False
    assert report["spool_rows_read"] == len(rec.rows)


def test_09c_a_spool_sequence_gap_ends_ingestion_at_the_gap(tmp_path):
    rec = short(tmp_path)
    del rec.rows[8]                                                # row n=9 is missing
    rec.write()
    report = j.ingest(rec, policy=FAST)
    assert report["spool_rows_ingested"] == 8
    gap = [p for p in report["problems"] if p["code"] == "spool_sequence_gap"][0]
    assert gap["next_expected_n"] == 9 and report["complete"] is False


def test_09d_an_existing_journal_is_never_overwritten(tmp_path):
    rec = short(tmp_path)
    rec.stop(SHORT)
    j.ingest(rec, policy=FAST)
    before = (tmp_path / "journal.jsonl").read_bytes()
    with pytest.raises(bridge.BridgeError) as excinfo:
        j.ingest(rec, policy=FAST)
    assert excinfo.value.code == "journal_exists" and (tmp_path / "journal.jsonl").read_bytes() == before


# ----------------------------------------------------------------------------------------------------------------- 10 duplicates
def test_10_an_identical_duplicate_spool_row_is_dropped_and_a_conflicting_one_is_refused(tmp_path):
    rec = short(tmp_path)
    rec.stop(SHORT)
    rows = list(rec.rows)
    rows.insert(6, dict(rows[5]))                                  # the same row written twice
    rec.write(rows=rows)
    report = j.ingest(rec, policy=FAST)
    assert report["spool_duplicates_dropped"] == 1 and report["complete"] is True
    conflicting = list(rec.rows)
    conflicting.insert(6, {**conflicting[5], "events_received": conflicting[5]["events_received"] + 1})
    rec.write(rows=conflicting)
    with pytest.raises(bridge.BridgeError) as excinfo:
        j.ingest(rec, journal_name="other.jsonl", policy=FAST)
    assert excinfo.value.code == "spool_row_conflict" and not (tmp_path / "other.jsonl").exists()


def test_10b_duplicate_submit_rows_are_deduplicated_and_conflicting_ones_refused(tmp_path):
    rec = short(tmp_path)
    rec.write()
    lines = rec.submits_path.read_text(encoding="utf-8").splitlines()
    rec.submits_path.write_text("\n".join(lines[:4] + [lines[3]] + lines[4:]) + "\n", encoding="utf-8")
    events, report = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    assert report["duplicates_dropped"] == 1 and len(events) == SHORT + 1
    seq, ts, count = lines[3].split(",")
    rec.submits_path.write_text("\n".join(lines[:4] + [f"{seq},{ts},{int(count) + 1}"] + lines[4:]) + "\n", encoding="utf-8")
    with pytest.raises(bridge.BridgeError) as excinfo:
        rc.read_submit_events(rec.submits_path, instance=rec.instance)
    assert excinfo.value.code == "submits_row_conflict"


# ------------------------------------------------------------------------------------------------------------ 11 missing callbacks
def test_11_missed_callbacks_show_up_as_a_source_count_mismatch_and_the_span_is_uncovered(tmp_path):
    rec = short(tmp_path, missed={7})                              # the source issued traffic in second 7 that the callback never saw
    rec.stop(SHORT)
    j.ingest(rec, policy=FAST)
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    snap = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=FAST) \
        .snapshot(at(SHORT + 1))
    assert snap.valid and rc.RC_MISSING_CALLBACKS in snap.cause_codes()
    mismatch = [m for m in snap.extra_uncovered if m[3].startswith("source_count_mismatch")]
    assert len(mismatch) == 1 and mismatch[0][2] == wc.CoverageClass.INTERRUPTED_COLLECTION
    assert snap.classify(at(7), 0)[0] == wc.CoverageClass.INTERRUPTED_COLLECTION
    assert snap.classify(at(2), 0)[0] == wc.CoverageClass.VERIFIED_ZERO and snap.classify(at(12), 0)[0] == wc.CoverageClass.VERIFIED_ZERO


# ------------------------------------------------------------------------------------------------------- 12 source identity mismatch
def test_12_a_spool_for_another_source_is_refused_before_any_file_exists(tmp_path):
    rec = short(tmp_path)
    rec.stop(SHORT)
    rec.write()
    with pytest.raises(bridge.BridgeError) as excinfo:
        bridge.ingest_spool(rec.spool_path, tmp_path / "journal.jsonl", source_id="some-other-source", policy=FAST)
    assert excinfo.value.code == "source_identity_mismatch" and not (tmp_path / "journal.jsonl").exists()


def test_12b_a_row_from_a_different_instance_or_run_is_refused(tmp_path):
    rec = short(tmp_path)
    rec.rows[4]["collector_instance"] = "rec:run-a0j:w0:r9"
    with pytest.raises(bridge.BridgeError) as excinfo:
        j.ingest(rec, policy=FAST)
    assert excinfo.value.code == "spool_identity_mismatch" and not (tmp_path / "journal.jsonl").exists()


def test_12c_a_journal_with_the_wrong_declared_policy_or_scope_is_refused(tmp_path):
    rec = short(tmp_path)
    with pytest.raises(bridge.BridgeError) as excinfo:
        j.ingest(rec, policy=bridge.RecorderHealthPolicy(heartbeat_interval_s=2, max_heartbeat_gap_s=5.0, freshness_limit_s=10.0, clock_tolerance_ms=250.0))
    assert excinfo.value.code == "policy_declared_mismatch"
    wrong = j.SyntheticRecorder(tmp_path, name="w", declared_interval_ms=1000, scope="source_attested").start()
    wrong.beat(1)
    with pytest.raises(bridge.BridgeError) as excinfo2:
        j.ingest(wrong, journal_name="w.jsonl", policy=FAST)
    assert excinfo2.value.code == "coverage_scope_invalid"


def test_12d_a_wall_clock_step_opens_a_journal_error_that_nothing_recovers(tmp_path):
    rec = short(tmp_path)
    for row in rec.rows[8:]:
        row["mono_ms"] += 3000.0                                   # the wall clock stepped 3 s relative to the monotonic clock
    rec.stop(SHORT)
    rec.rows[-1]["mono_ms"] += 3000.0
    report = j.ingest(rec, policy=FAST)
    assert any(p["code"] == "clock_discontinuity" for p in report["problems"])
    rows = journal_rows(tmp_path / "journal.jsonl")
    assert any(r["kind"] == "error" and r["error_code"] == "clock_discontinuity" for r in rows) and not any(r["kind"] == "recovered" for r in rows)


# --------------------------------------------------------------------------------------------------------------- timing policy
@pytest.mark.parametrize("kwargs,code", [
    ({"heartbeat_interval_s": 1, "max_heartbeat_gap_s": 1.5, "freshness_limit_s": 10.0, "clock_tolerance_ms": 250.0}, "policy_inconsistent"),
    ({"heartbeat_interval_s": 1, "max_heartbeat_gap_s": 5.0, "freshness_limit_s": 4.0, "clock_tolerance_ms": 250.0}, "policy_inconsistent"),
    ({"heartbeat_interval_s": 1, "max_heartbeat_gap_s": 5.0, "freshness_limit_s": 10.0, "clock_tolerance_ms": 1500.0}, "policy_inconsistent"),
    ({"heartbeat_interval_s": 0, "max_heartbeat_gap_s": 5.0, "freshness_limit_s": 10.0, "clock_tolerance_ms": 250.0}, "policy_invalid"),
    ({"heartbeat_interval_s": 1.5, "max_heartbeat_gap_s": 5.0, "freshness_limit_s": 10.0, "clock_tolerance_ms": 250.0}, "policy_invalid"),
    ({"heartbeat_interval_s": 1, "max_heartbeat_gap_s": float("nan"), "freshness_limit_s": 10.0, "clock_tolerance_ms": 250.0}, "policy_invalid"),
    ({"heartbeat_interval_s": True, "max_heartbeat_gap_s": 5.0, "freshness_limit_s": 10.0, "clock_tolerance_ms": 250.0}, "policy_invalid"),
])
def test_13_policy_relationships_are_validated(kwargs, code):
    with pytest.raises(bridge.BridgeError) as excinfo:
        bridge.RecorderHealthPolicy(**kwargs)
    assert excinfo.value.code == code


def test_13b_the_candidate_proposal_is_labelled_and_internally_consistent_but_applied_nowhere():
    proposal = dict(bridge.CANDIDATE_PROPOSAL)
    assert "not scientifically validated" in proposal["label"] and "measured" in proposal["basis"]
    policy = bridge.RecorderHealthPolicy(**{k: proposal[k] for k in ("heartbeat_interval_s", "max_heartbeat_gap_s", "freshness_limit_s", "clock_tolerance_ms")})
    assert policy.to_coverage_policy().max_heartbeat_gap_s == 5.0
    with pytest.raises(TypeError):
        bridge.RecorderHealthPolicy()                              # there are no defaults: the operator must state every value
