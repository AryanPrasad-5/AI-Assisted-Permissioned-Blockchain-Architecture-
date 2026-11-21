"""A0j coverage correctness (checklist items 13-24): when a recorder-derived zero second may be believed.

The rule under test: recorder heartbeats prove the RECORDER ran; a zero second is genuine only where an INDEPENDENT source count agrees with
the recorder's count over that span. SYNTHETIC evidence throughout; none of it is a Caliper or Fabric measurement.
"""

from __future__ import annotations

import json

import pytest

from tests import a0j_support as j
from tests.a0j_support import FAST, SHORT, at, rc, wc

C = wc.CoverageClass


def classes(snap, seconds, count):
    return [snap.classify(at(sec), count)[0] for sec in seconds]


def rewrite_journal_row(path, index, change):
    lines = path.read_text(encoding="utf-8").splitlines()
    row = json.loads(lines[index])
    change(row)
    lines[index] = json.dumps(row, sort_keys=True, separators=(",", ":"))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# --------------------------------------------------------------------------------------------------------- 13 verified positive
def test_13_a_positive_second_inside_recorder_coverage_is_observed_positive(tmp_path):
    snap = j.snapshot_for(j.short_recorder(tmp_path), SHORT + 1)
    assert snap.valid and set(classes(snap, range(0, SHORT), 25)) == {C.OBSERVED_POSITIVE}
    assert snap.summary()["recorder_evidence"]["positive_second_caveat"]               # the residual under-count risk is stated, not hidden


# --------------------------------------------------------------------------------- 14 genuine zero with source-completeness evidence
def test_14_a_zero_is_genuine_only_where_an_independent_source_count_agrees(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5, 12})
    snap = j.snapshot_for(rec, SHORT + 1)
    assert snap.valid and classes(snap, (5, 12), 0) == [C.VERIFIED_ZERO, C.VERIFIED_ZERO]
    evidence = snap.summary()["recorder_evidence"]
    assert evidence["source_completeness"]["verified_seconds"] == SHORT and evidence["source_completeness"]["basis_claimed_by_collector"] == [j.SOURCE_BASIS]
    assert evidence["synthetic"] is True and evidence["live_source_coverage_verified"] is False         # synthetic evidence is never live coverage


def test_14b_the_independence_of_the_source_count_is_declared_not_verified(tmp_path):
    summary = j.snapshot_for(j.short_recorder(tmp_path, zero={5}), SHORT + 1).summary()["recorder_evidence"]["source_completeness"]
    assert "DECLARED" in summary["independence"] and "cannot verify" in summary["independence"]


# ------------------------------------------------------------------------------------------------- 15 zero with heartbeat only
def test_15_heartbeats_alone_never_certify_a_zero_second(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5, 12}, source_count=False)                  # healthy recorder, no source evidence at all
    snap = j.snapshot_for(rec, SHORT + 1)
    assert snap.valid                                                                      # the recorder's operation IS verified ...
    cls, why = snap.classify(at(5), 0)
    assert cls == C.UNKNOWN_COVERAGE_ZERO and rc.RECORDER_ONLY_CAUSE in why                # ... and still says nothing about the source
    assert snap.classify(at(6), 30)[0] == C.OBSERVED_POSITIVE                              # positives are unaffected
    assert snap.summary()["recorder_evidence"]["source_completeness"]["verified_seconds"] == 0


def test_15b_the_same_journal_through_the_generic_contract_no_longer_certifies_the_zero(tmp_path):
    """A0k: this test ASSERTED THE DEFECT in A0j (the generic contract certified a heartbeat-only zero, ``== C.VERIFIED_ZERO``). The generic
    ``VerifiedCoverage`` now reads the journal's ``coverage_scope``, so the same journal fails closed on every route. The inversion is deliberate;
    the original input (a recorder journal with no source count) is unchanged."""
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    j.snapshot_for(rec, SHORT + 1)
    generic = wc.VerifiedCoverage.from_journal_file(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=FAST.to_coverage_policy())
    cls, why = generic.snapshot(at(SHORT + 1)).classify(at(5), 0)
    assert cls == C.UNKNOWN_COVERAGE_ZERO and rc.RECORDER_ONLY_CAUSE in why                  # the generic route now obeys the declared scope too
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    strict = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=FAST)
    assert strict.snapshot(at(SHORT + 1)).classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO


def test_15c_a_source_count_the_start_did_not_declare_is_contradictory(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5})
    rec.rows[0]["source_count_declared"] = False
    snap = j.snapshot_for(rec, SHORT + 1)
    assert not snap.valid and snap.failure_code == "source_count_undeclared"
    assert snap.classify(at(5), 0)[0] == C.INVALID_ATTESTATION


# ------------------------------------------------------------------------------------------------------------ 16 unknown coverage
def test_16_seconds_outside_the_observation_window_are_unknown(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000).start(t=4.5, observation=5)
    rec.traffic(range(0, 25), zero={2, 9})
    for sec in range(6, 21):
        rec.beat(sec)
    rec.stop(20)
    snap = j.snapshot_for(rec, 30)
    assert snap.classify(at(2), 0)[0] == C.UNKNOWN_COVERAGE_ZERO                            # before the collector started: unknown, never 0
    assert snap.classify(at(2), 25)[0] == C.COVERAGE_GAP                                    # a positive from before the window is not accepted either
    assert snap.classify(at(9), 0)[0] == C.VERIFIED_ZERO
    assert snap.classify(at(25), 0)[0] == C.UNKNOWN_COVERAGE_ZERO                           # after the clean stop: unknown


# ---------------------------------------------------------------------------------------------------------- 17 stale, 18 gap
def test_17_a_watermark_newer_than_the_evidence_is_stale_not_covered(tmp_path):
    rec = j.short_recorder(tmp_path, stop=False)
    snap = j.snapshot_for(rec, SHORT + 8)
    assert snap.classify(at(SHORT + 3), 0)[0] == C.STALE_COLLECTOR_EVIDENCE
    assert snap.classify(at(SHORT - 3), 0)[0] == C.VERIFIED_ZERO


def test_17b_evidence_written_after_the_decision_is_not_used_for_that_decision(tmp_path):
    rec = j.short_recorder(tmp_path, zero={15})
    snap_early = j.snapshot_for(rec, 10)
    assert snap_early.classify(at(9), 5)[0] == C.OBSERVED_POSITIVE
    assert snap_early.classify(at(15), 0)[0] == C.STALE_COLLECTOR_EVIDENCE                  # certified by a later heartbeat, invisible at t=10


def test_18_an_excessive_heartbeat_gap_is_a_coverage_gap_and_certifies_nothing(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000).start()
    rec.traffic(range(0, 31), zero={8, 20})
    for sec in (1, 2, 3, 4, 5, 6):
        rec.beat(sec)
    rec.beat(14)                                               # 8 s of silence > the 5 s limit
    for sec in range(15, 31):
        rec.beat(sec)
    rec.stop(30)
    snap = j.snapshot_for(rec, 40)
    assert snap.classify(at(8), 0)[0] == C.COVERAGE_GAP and rc.RC_HEARTBEAT_GAP in snap.cause_codes()
    assert snap.classify(at(20), 0)[0] == C.VERIFIED_ZERO
    # the source counts at 6 and 14 agree, yet the span between them is a coverage gap and must not count as source-verified: [0,6) + [14,30)
    assert snap.summary()["recorder_evidence"]["source_completeness"]["verified_seconds"] == 22


# ------------------------------------------------------------------------------------------ 19 missing segment, 20 restart gap
def test_19_an_error_that_is_never_recovered_leaves_the_rest_uncovered(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000).start()
    rec.traffic(range(0, 21), zero={4, 15})
    for sec in range(1, 9):
        rec.beat(sec)
    rec.error(9, "live_submit_stream_failed", affects_from=8)
    for sec in range(10, 21):
        rec.beat(sec, health="degraded", errors=1)
    rec.stop(20, reason="abort")
    snap = j.snapshot_for(rec, 30)
    assert snap.classify(at(4), 0)[0] == C.VERIFIED_ZERO
    assert snap.classify(at(15), 0)[0] == C.INTERRUPTED_COLLECTION and rc.RC_HEALTH_FAILURE in snap.cause_codes()


def test_20_a_restart_cannot_retroactively_certify_the_unobserved_gap(tmp_path):
    first = j.SyntheticRecorder(tmp_path, name="a", declared_interval_ms=1000).start()
    first.traffic(range(0, 11))
    for sec in range(1, 11):
        first.beat(sec)
    j.ingest(first, policy=FAST)                                                             # killed at 10 s: no stop
    second = j.SyntheticRecorder(tmp_path, name="b", instance="rec:run-a0j:w0:r1", declared_interval_ms=1000, round_=1).start(t=19.5, observation=20)
    second.traffic(range(20, 31), zero={25})
    for sec in range(21, 31):
        second.beat(sec)
    second.stop(30)
    j.ingest(second, policy=FAST, resume=True)
    events = rc.read_submit_events(first.submits_path, instance=first.instance)[0] + rc.read_submit_events(second.submits_path, instance=second.instance)[0]
    snap = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=FAST).snapshot(at(40))
    assert snap.valid
    assert classes(snap, (11, 14, 18), 0) == [C.INTERRUPTED_COLLECTION] * 3 and rc.RC_RESTART_GAP in snap.cause_codes()
    assert snap.classify(at(25), 0)[0] == C.VERIFIED_ZERO                                    # the new instance certifies only what it observed


# -------------------------------------------------------------------------------------------------------- 21 partial source coverage
def test_21_a_source_count_that_covers_only_part_of_the_window_certifies_only_that_part(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000).start()
    rec.traffic(range(0, 21), zero={3, 12})
    for sec in range(1, 9):
        rec.beat(sec)
    for sec in range(9, 21):
        rec.beat(sec, with_source=False)                       # the source counter became unavailable and never came back
    rec.stop(20)
    snap = j.snapshot_for(rec, 30)
    assert snap.valid
    assert snap.classify(at(3), 0)[0] == C.VERIFIED_ZERO
    cls, why = snap.classify(at(12), 0)
    assert cls == C.UNKNOWN_COVERAGE_ZERO and rc.RECORDER_ONLY_CAUSE in why                  # recorder coverage continues, source verification ended at 8
    assert snap.summary()["recorder_evidence"]["source_completeness"]["verified_seconds"] == 8


def test_21a_a_source_gap_bracketed_by_two_agreeing_source_counts_is_verified_as_one_interval(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000).start()
    rec.traffic(range(0, 21), zero={3, 12})
    for sec in range(1, 9):
        rec.beat(sec)
    for sec in range(9, 16):
        rec.beat(sec, with_source=False)
    for sec in range(16, 21):
        rec.beat(sec)
    rec.stop(20)
    snap = j.snapshot_for(rec, 30)
    # equal cumulative deltas over [8, 16) mean the recorder missed nothing in that span, so the zero at 12 is genuine even though no
    # source count was reported at 12 itself
    assert snap.classify(at(12), 0)[0] == C.VERIFIED_ZERO and snap.summary()["recorder_evidence"]["source_completeness"]["verified_seconds"] == 20


def test_21c_a_bracketing_mismatch_uncovers_the_whole_bracket(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000).start()
    rec.traffic(range(0, 21), zero={3}, missed={12})
    for sec in range(1, 9):
        rec.beat(sec)
    for sec in range(9, 16):
        rec.beat(sec, with_source=False)
    for sec in range(16, 21):
        rec.beat(sec)
    rec.stop(20)
    snap = j.snapshot_for(rec, 30)
    assert classes(snap, (9, 12, 15), 0) == [C.INTERRUPTED_COLLECTION] * 3 and rc.RC_MISSING_CALLBACKS in snap.cause_codes()


def test_21b_after_an_error_the_next_source_beat_only_re_establishes_the_baseline(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000).start()
    rec.traffic(range(0, 21), zero={6, 14, 18})
    for sec in range(1, 6):
        rec.beat(sec)
    rec.error(6, "synthetic_fault", affects_from=5)
    rec.recovered(8, resumes=9)
    for sec in range(10, 21):
        rec.beat(sec)
    rec.stop(20)
    snap = j.snapshot_for(rec, 30)
    assert snap.classify(at(6), 0)[0] == C.INTERRUPTED_COLLECTION
    assert snap.classify(at(9), 0)[0] in (C.UNKNOWN_COVERAGE_ZERO, C.INTERRUPTED_COLLECTION)        # the baseline beat itself carries no verification
    assert snap.classify(at(14), 0)[0] == C.VERIFIED_ZERO


# ------------------------------------------------------------------------------------------------------- 22 hash-chain corruption
@pytest.mark.parametrize("change,code", [
    (lambda r: r.update(events_received=r["events_received"] + 1), "hash_mismatch"),
    (lambda r: r.update(observed_through_utc=r["observed_through_utc"].replace("T00:00:0", "T00:00:1")), "hash_mismatch"),
])
def test_22_an_altered_journal_row_invalidates_the_whole_attestation(tmp_path, change, code):
    rec = j.short_recorder(tmp_path, zero={5})
    j.ingest(rec, policy=FAST)
    rewrite_journal_row(tmp_path / "journal.jsonl", 6, change)
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    snap = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=FAST).snapshot(at(30))
    assert not snap.valid and snap.failure_code == code and snap.cause_codes() == [rc.RC_HASH_CHAIN]
    assert snap.classify(at(5), 0)[0] == C.INVALID_ATTESTATION and snap.classify(at(2), 25)[0] == C.INVALID_ATTESTATION


def test_22b_a_dropped_journal_row_breaks_the_chain(tmp_path):
    rec = j.short_recorder(tmp_path)
    j.ingest(rec, policy=FAST)
    path = tmp_path / "journal.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()
    path.write_text("\n".join(lines[:4] + lines[5:]) + "\n", encoding="utf-8")
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    snap = rc.RecorderVerifiedCoverage.from_recorder_journal(path, source_id=j.SOURCE_ID, submit_events=events, policy=FAST).snapshot(at(30))
    assert not snap.valid and snap.failure_code in ("event_sequence_broken", "hash_chain_broken")


# ------------------------------------------------------------------------------------------------------ 23 timestamp inconsistency
def test_23_a_journal_whose_timestamps_run_backwards_is_invalid(tmp_path):
    rec = j.short_recorder(tmp_path)
    rows = [dict(r) for r in rec.rows]
    rows[7]["t_utc"] = wc.iso(at(2.0))                                                       # an earlier time than the row before it
    forged = j.s.forge([{k: v for k, v in r.items() if k not in ("schema", "n", "source_id", "collector_id", "collector_instance", "run_id", "worker", "round", "label", "mono_ms")}
                        | {"collector_instance": rec.instance} for r in rows])
    coverage = rc.RecorderVerifiedCoverage(source_id=j.SOURCE_ID, events=forged, submit_events=rc.read_submit_events(
        (rec.write() or rec).submits_path, instance=rec.instance)[0], policy=FAST.to_coverage_policy())
    snap = coverage.snapshot(at(30))
    assert not snap.valid and snap.failure_code == "timestamp_regressed"


def test_23b_a_watermark_claimed_after_the_row_was_written_is_refused(tmp_path):
    rec = j.short_recorder(tmp_path)
    rec.rows[4]["observed_through_utc"] = wc.iso(at(9))                                      # row n=5 is written at t=4 but claims capture through 9
    j.ingest(rec, policy=FAST)
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    snap = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=FAST).snapshot(at(30))
    assert not snap.valid


# ---------------------------------------------------------------------------------------------------------- 24 contradictory evidence
def test_24_a_heartbeat_count_that_disagrees_with_the_submits_file_is_contradictory(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5})
    rec.rows[9]["events_received"] += 4                                                      # the heartbeat claims 4 more transactions than the file holds
    snap = j.snapshot_for(rec, SHORT + 1)
    assert not snap.valid and snap.failure_code == "heartbeat_event_count_mismatch" and snap.cause_codes() == [rc.RC_CONTRADICTORY]
    assert snap.classify(at(5), 0)[0] == C.INVALID_ATTESTATION


def test_24b_a_submits_file_missing_events_the_heartbeat_counted_is_contradictory(tmp_path):
    rec = j.short_recorder(tmp_path)
    rec.write()
    lines = rec.submits_path.read_text(encoding="utf-8").splitlines()
    rec.submits_path.write_text("\n".join(lines[:6] + lines[7:]) + "\n", encoding="utf-8")        # a row never reached the file
    j.ingest(rec, policy=FAST)
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    snap = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=FAST).snapshot(at(30))
    assert not snap.valid and snap.failure_code == "heartbeat_event_count_mismatch"


@pytest.mark.parametrize("mutation,code", [
    (lambda rec: rec.rows[0].update(evidence_origin="live"), "evidence_origin_invalid"),
    (lambda rec: rec.rows[6].update(source_events_expected=rec.rows[6]["source_events_expected"] - 40), "source_count_regressed"),
    (lambda rec: rec.rows[6].update(source_basis=""), "event_field_missing"),
    (lambda rec: rec.rows[6].update(source_events_expected=-3), "event_field_invalid"),
])
def test_24c_contradictory_or_malformed_recorder_fields_invalidate_the_attestation(tmp_path, mutation, code):
    rec = j.short_recorder(tmp_path, zero={5})
    mutation(rec)
    rec.write()
    forged_rows = [{k: v for k, v in r.items() if k not in ("schema", "n", "mono_ms", "worker", "round", "label")} for r in rec.rows]
    forged = j.s.forge(forged_rows)
    coverage = rc.RecorderVerifiedCoverage(source_id=j.SOURCE_ID, events=forged, submit_events=rc.read_submit_events(rec.submits_path, instance=rec.instance)[0],
                                           policy=FAST.to_coverage_policy())
    snap = coverage.snapshot(at(30))
    assert not snap.valid and snap.failure_code == code, (snap.failure_code, snap.failure_reason)


def test_24d_a_forged_journal_with_a_non_recorder_scope_is_refused_by_the_recorder_route(tmp_path):
    rec = j.short_recorder(tmp_path)
    rec.rows[0]["coverage_scope"] = "source_attested"
    rec.write()
    forged_rows = [{k: v for k, v in r.items() if k not in ("schema", "n", "mono_ms", "worker", "round", "label")} for r in rec.rows]
    coverage = rc.RecorderVerifiedCoverage(source_id=j.SOURCE_ID, events=j.s.forge(forged_rows),
                                           submit_events=rc.read_submit_events(rec.submits_path, instance=rec.instance)[0], policy=FAST.to_coverage_policy())
    snap = coverage.snapshot(at(30))
    assert not snap.valid and snap.failure_code == "coverage_scope_invalid"


def test_24e_the_generic_contract_still_rejects_a_healthy_report_with_errors_since_the_last_heartbeat(tmp_path):
    rec = j.short_recorder(tmp_path)
    rec.rows[6]["errors_since_last"] = 2                                                     # healthy AND 2 errors: contradictory
    snap = j.snapshot_for(rec, SHORT + 1)
    assert not snap.valid and snap.failure_code == "contradictory_health_evidence"


def test_24f_submit_events_naming_an_unknown_instance_are_contradictory(tmp_path):
    rec = j.short_recorder(tmp_path)
    rec.write()
    events, _ = rc.read_submit_events(rec.submits_path, instance="rec:run-a0j:w0:r7")
    j.ingest(rec, policy=FAST)
    coverage = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=FAST)
    snap = coverage.snapshot(at(30))
    assert not snap.valid and snap.failure_code == "event_instance_unknown"


def test_24g_submit_time_not_completion_time_is_the_arrival_time():
    """The loader's only time column is t_submit_evt_ms; a finished-transaction time cannot be passed in as an arrival."""
    assert rc.SUBMIT_COLUMNS == ("seq_submit", "t_submit_evt_ms", "count")
    assert "NOT ledger commits" in rc.REPRESENTS and "completion times" in rc.REPRESENTS


def test_24h_unhealthy_heartbeats_with_a_frozen_watermark_never_enter_the_source_check(tmp_path):
    """What a recorder whose live file stopped growing writes: degraded heartbeats at a frozen watermark while the source keeps counting."""
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000).start()
    rec.traffic(range(0, 21), zero={3})
    for sec in range(1, 6):
        rec.beat(sec)
    rec.error(6, "live_submit_stream_failed", affects_from=5)
    for t in (7, 8, 9):
        rec.beat(t, watermark=5, health="degraded", errors=1, source=rec.source_through(t))      # the source count grows, the frozen watermark and recorder count do not
    rec.stop(9, watermark=5, reason="abort")
    rec.rows[-1].update(source_events_expected=rec.source_through(9), source_basis=j.SOURCE_BASIS)
    snap = j.snapshot_for(rec, 30)
    assert snap.valid, (snap.failure_code, snap.failure_reason)
    assert snap.classify(at(3), 0)[0] == C.VERIFIED_ZERO and snap.classify(at(8), 5)[0] == C.INTERRUPTED_COLLECTION
