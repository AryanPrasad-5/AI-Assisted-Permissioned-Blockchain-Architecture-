"""A0k workstream A (checklist items 1-11): the generic coverage contract obeys the journal's declared scope, so a recorder heartbeat never certifies
a zero-arrival second on ANY route. Synthetic scripted recorders on scripted clocks; nothing here touches Caliper or Fabric.
"""

from __future__ import annotations

import json

import pytest

from tests import a0j_support as j
from tests import a0k_support as k
from tests.a0k_support import C, FAST, SHORT, at, ch, chj, rc, sc, wc


def generic_snapshot(tmp_path, rec, as_of=SHORT + 1):
    """The ORIGINAL BYPASS route: the generic ``VerifiedCoverage`` over a recorder's journal, no recorder subclass."""
    k.recorder_coverage(rec, tmp_path)
    generic = wc.VerifiedCoverage.from_journal_file(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=FAST.to_coverage_policy())
    return generic.snapshot(at(as_of))


# ------------------------------------------------------------------------------------------------------------ 1 heartbeat-only zero
def test_01_the_original_bypass_heartbeat_only_zero_fails_closed_on_the_generic_route(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    snap = generic_snapshot(tmp_path, rec)
    cls, why = snap.classify(at(5), 0)
    assert snap.scope == wc.SCOPE_RECORDER_CALLBACK
    assert cls == C.UNKNOWN_COVERAGE_ZERO and wc.RECORDER_ONLY_CAUSE in why
    assert snap.classify(at(4), 4)[0] == C.OBSERVED_POSITIVE                      # a genuine positive is preserved ...
    assert snap.classify(at(5), 0)[0] != C.VERIFIED_ZERO                          # ... and does not certify the adjacent empty second


def test_01b_every_zero_second_of_a_heartbeat_only_recorder_is_unknown_not_just_one(tmp_path):
    rec = j.short_recorder(tmp_path, zero={3, 4, 5, 9, 10, 11, 12}, source_count=False)
    snap = generic_snapshot(tmp_path, rec)
    for second in (3, 4, 5, 9, 10, 11, 12):
        assert snap.classify(at(second), 0)[0] == C.UNKNOWN_COVERAGE_ZERO, second


def test_01c_the_scope_is_part_of_the_hash_chained_row_so_it_cannot_be_edited_away(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    k.recorder_coverage(rec, tmp_path)
    lines = (tmp_path / "journal.jsonl").read_text(encoding="utf-8").splitlines()
    first = json.loads(lines[0])
    assert first["coverage_scope"] == "recorder_callback"
    first.pop("coverage_scope")
    lines[0] = json.dumps(first, sort_keys=True, separators=(",", ":"))
    (tmp_path / "edited.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    edited = wc.VerifiedCoverage.from_journal_file(tmp_path / "edited.jsonl", source_id=j.SOURCE_ID, policy=FAST.to_coverage_policy())
    snap = edited.snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code in ("hash_mismatch", "hash_chain_broken")
    assert snap.classify(at(5), 0)[0] == C.INVALID_ATTESTATION


# -------------------------------------------------------------------------------------------------------- 2 callback-only zero rejection
def test_02_recorder_callback_counts_without_independent_source_evidence_never_certify_a_zero(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    _report, cov, events = k.recorder_coverage(rec, tmp_path)
    snap = cov.snapshot(at(SHORT + 1))
    assert snap.valid and sum(e["units"] for e in events) > 0                      # callbacks fed the recorder and were counted
    assert snap.classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO and snap.source_verified == ()


def test_02b_a_source_count_that_is_the_recorders_own_callback_stream_certifies_nothing(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=True)
    rec.start().rows[0]["source_counter"] = k.descriptor(kind=sc.KIND_RECORDER_DERIVED, derived_from="recorder_submits_file")
    rec.traffic(range(0, SHORT + 1), zero={5})
    for second in range(1, SHORT + 1):
        rec.beat(second)
    snap = k.recorder_coverage(rec, tmp_path)[1].snapshot(at(SHORT + 1))
    assert snap.valid and snap.classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO
    assert "source_counter_recorder_derived" in snap.cause_codes()
    assert snap.source_info["classifications"][rec.instance]["class"] == sc.CLASS_RECORDER_DEPENDENT


@pytest.mark.parametrize("derived_from", ["recorder_submits_file", "txSubmitted callback", "live.submits.csv", "observer stream", "TxStatus completions"])
def test_02c_every_recorder_marker_in_derived_from_is_recognised(derived_from):
    verdict = sc.classify_descriptor(k.descriptor(derived_from=derived_from))
    assert verdict.label == sc.CLASS_RECORDER_DEPENDENT and not verdict.certifies_intervals and verdict.reason_code == sc.RC_RECORDER_DERIVED


# -------------------------------------------------------------------------------------------------- 3 independent source evidence accepted
def test_03_journal_reported_independent_counts_certify_a_zero_on_the_generic_route(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5})                                      # source counter declared WITH a descriptor
    snap = generic_snapshot(tmp_path, rec)
    cls, why = snap.classify(at(5), 0)
    assert cls == C.VERIFIED_ZERO and "independent source count agrees" in why
    assert snap.source_info["classifications"][rec.instance]["class"] == sc.CLASS_INTERVAL
    assert snap.summary()["source_evidence"]["independence"].startswith("DECLARED")      # never presented as proven


def test_03b_a_generator_dispatch_log_certifies_the_same_zero_and_is_the_only_evidence_used(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)                    # the journal itself reports NO source count
    evidence = k.dispatch_evidence(rec)
    _report, cov, _events = k.recorder_coverage(rec, tmp_path, source_evidence=evidence)
    snap = cov.snapshot(at(SHORT + 1))
    assert snap.classify(at(5), 0)[0] == C.VERIFIED_ZERO and snap.source_info["kind"] == "dispatch_log"
    assert snap.summary()["recorder_evidence"]["live_source_coverage_verified"] is False
    assert snap.summary()["recorder_evidence"]["synthetic"] is True


# ------------------------------------------------------------------------------------------------------------------ 4 independence proof
@pytest.mark.parametrize("override,code", [
    ({"independent_of_recorder": None}, sc.RC_INDEPENDENCE_UNPROVEN),
    ({"independent_of_recorder": False}, sc.RC_RECORDER_DERIVED),
    ({"derived_from": None}, sc.RC_INDEPENDENCE_UNPROVEN),
    ({"counter_id": None}, sc.RC_INDEPENDENCE_UNPROVEN),
    ({"timestamp_semantics": "recorder_callback_time"}, sc.RC_TIMESTAMP_SEMANTICS),
    ({"timestamp_semantics": "completion_time"}, sc.RC_TIMESTAMP_SEMANTICS),
    ({"kind": "round_total_configured"}, sc.RC_ROUND_ONLY),
    ({"kind": "round_total_observed"}, sc.RC_ROUND_ONLY),
    ({"kind": "caliper_aggregate"}, sc.RC_UNVERIFIABLE),
    ({"kind": "made_up"}, sc.RC_DESCRIPTOR_INVALID),
    ({"resolution": "round_total"}, sc.RC_ROUND_ONLY),
])
def test_04_a_descriptor_without_an_independence_proof_certifies_no_zero(tmp_path, override, code):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=True)
    rec.start().rows[0]["source_counter"] = k.descriptor(**override)
    rec.traffic(range(0, SHORT + 1), zero={5})
    for second in range(1, SHORT + 1):
        rec.beat(second)
    snap = k.recorder_coverage(rec, tmp_path)[1].snapshot(at(SHORT + 1))
    assert snap.valid and snap.classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO and code in snap.cause_codes()


def test_04b_a_missing_descriptor_and_a_heartbeat_that_does_not_claim_independence_both_fail_closed(tmp_path):
    bare = j.SyntheticRecorder(tmp_path, name="a", instance="rec:run-a0j:w0:ra", declared_interval_ms=1000, source_count=True)
    bare.start().rows[0].pop("source_counter")                                       # declared a count but no descriptor
    bare.traffic(range(0, SHORT + 1), zero={5})
    for second in range(1, SHORT + 1):
        bare.beat(second)
    snap = k.recorder_coverage(bare, tmp_path, journal_name="a.jsonl")[1].snapshot(at(SHORT + 1))
    assert snap.classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO and sc.RC_DESCRIPTOR_MISSING in snap.cause_codes()
    unclaimed = j.SyntheticRecorder(tmp_path, name="b", instance="rec:run-a0j:w0:rb", declared_interval_ms=1000, source_count=True).start()
    unclaimed.traffic(range(0, SHORT + 1), zero={5})
    for second in range(1, SHORT + 1):
        unclaimed.beat(second)
    for row in unclaimed.rows:
        if row["kind"] == "heartbeat":
            row["source_independence_claim"] = False
    snap = k.recorder_coverage(unclaimed, tmp_path, journal_name="b.jsonl")[1].snapshot(at(SHORT + 1))
    assert snap.classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO and sc.RC_INDEPENDENCE_UNPROVEN in snap.cause_codes()


# ------------------------------------------------------------------------------------------------------------------- 5 mismatched counts
def test_05_a_source_that_issued_more_than_the_recorder_saw_uncovers_the_span(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, missed={8})
    snap = generic_snapshot(tmp_path, rec)
    assert snap.classify(at(5), 0)[0] == C.VERIFIED_ZERO                                # before the loss: still verified
    assert snap.classify(at(8), 0)[0] == C.INTERRUPTED_COLLECTION and "source_count_mismatch" in snap.classify(at(8), 0)[1]


def test_05b_a_recorder_that_counted_more_than_the_dispatch_log_issued_invalidates_the_whole_attestation(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    thin = {sec: n for sec, n in rec.truth.items()}
    thin[7] = thin[7] - 1                                                             # the generator log shows one transaction fewer than the recorder saw
    evidence = k.dispatch_evidence(rec, text=k.dispatch_text(thin))
    snap = k.recorder_coverage(rec, tmp_path, source_evidence=evidence)[1].snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == "recorder_exceeds_source"
    assert snap.classify(at(5), 0)[0] == C.INVALID_ATTESTATION


# ----------------------------------------------------------------------------------------------------------------------- 6 invalid scope
def test_06_a_scope_outside_the_vocabulary_is_invalid(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5})
    rec.rows[0]["coverage_scope"] = "source_attested"
    rec.write()
    forged = j.s.forge([{kk: v for kk, v in r.items() if kk not in ("schema", "n", "mono_ms", "worker", "round", "label")} for r in rec.rows])
    snap = wc.VerifiedCoverage(source_id=j.SOURCE_ID, mode=wc.MODE_ATTESTED, events=forged, policy=FAST.to_coverage_policy()).snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == "coverage_scope_invalid"


def test_06b_recorder_fields_cannot_borrow_the_source_stream_scope(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    rec.rows[0]["coverage_scope"] = wc.SCOPE_SOURCE_STREAM                              # a recorder claiming it observes the source stream itself
    forged = j.s.forge([{kk: v for kk, v in r.items() if kk not in ("schema", "n", "mono_ms", "worker", "round", "label")} for r in rec.rows])
    snap = wc.VerifiedCoverage(source_id=j.SOURCE_ID, mode=wc.MODE_ATTESTED, events=forged, policy=FAST.to_coverage_policy()).snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == "coverage_scope_contradiction"
    assert snap.classify(at(5), 0)[0] == C.INVALID_ATTESTATION


def test_06c_recorder_fields_with_no_scope_at_all_are_invalid_not_legacy(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    rec.rows[0].pop("coverage_scope")
    forged = j.s.forge([{kk: v for kk, v in r.items() if kk not in ("schema", "n", "mono_ms", "worker", "round", "label")} for r in rec.rows])
    snap = wc.VerifiedCoverage(source_id=j.SOURCE_ID, mode=wc.MODE_ATTESTED, events=forged, policy=FAST.to_coverage_policy()).snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == "coverage_scope_invalid"


def test_06d_a_restart_that_changes_the_declared_scope_is_inconsistent(tmp_path):
    rows = [j.s.start_row(-0.5, 0, coverage_scope=wc.SCOPE_SOURCE_STREAM), j.s.heartbeat_row(5),
            j.s.start_row(6.0, 10, collector_instance="inst-2", coverage_scope=None), j.s.heartbeat_row(12, collector_instance="inst-2")]
    rows[1].setdefault("collector_instance", "inst-1")
    snap = j.s.forged_coverage([{**r, "collector_instance": r.get("collector_instance", "inst-1")} for r in rows]).snapshot(at(20))
    assert not snap.valid and snap.failure_code == "coverage_scope_inconsistent"


def test_06e_an_undeclared_scope_observes_positives_but_certifies_no_zero(tmp_path):
    journal, clock, path = j.s.new_journal(tmp_path, started=False)                   # a journal written without any declared scope
    journal.start(observation_start_utc=at(0))
    for second in range(1, 11):
        j.s.beat(journal, clock, second)
    snap = j.s.coverage_for(path, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0)).snapshot(at(12))
    assert snap.scope is None
    assert snap.classify(at(3), 3)[0] == C.OBSERVED_POSITIVE
    cls, why = snap.classify(at(3), 0)
    assert cls == C.UNKNOWN_COVERAGE_ZERO and wc.UNDECLARED_SCOPE_CAUSE in why


def test_06f_the_journal_writer_refuses_a_scope_outside_the_vocabulary(tmp_path):
    journal = chj.CollectorHealthJournal(tmp_path / "x.jsonl", source_id="s", collector_id="c", run_id="r", clock=j.s.ScriptClock())
    with pytest.raises(wc.CoverageEvidenceError) as excinfo:
        journal.start(observation_start_utc=at(0), coverage_scope="source_attested")
    assert excinfo.value.code == "coverage_scope_invalid"


# ------------------------------------------------------------------------------------------------------------------- 7 partial interval
def test_07_a_source_count_that_first_appears_late_is_cumulative_so_a_loss_anywhere_in_the_span_uncovers_all_of_it(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=True).start()
    rec.traffic(range(0, SHORT + 1), zero={3, 15}, missed={5})                         # a callback was missed at second 5
    for second in range(1, SHORT + 1):
        rec.beat(second, with_source=second >= 10)                                     # the provider only reports from second 10
    snap = k.recorder_coverage(rec, tmp_path)[1].snapshot(at(SHORT + 1))
    cls, why = snap.classify(at(3), 0)                                                 # the loss cannot be placed inside [0, 10): the whole span is uncovered
    assert cls == C.INTERRUPTED_COLLECTION and "source_count_mismatch" in why
    assert snap.classify(at(15), 0)[0] == C.VERIFIED_ZERO                              # [10, 20) agrees


def test_07a_a_late_first_source_report_that_agrees_verifies_the_whole_cumulative_span(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=True).start()
    rec.traffic(range(0, SHORT + 1), zero={3, 15})
    for second in range(1, SHORT + 1):
        rec.beat(second, with_source=second >= 10)
    snap = k.recorder_coverage(rec, tmp_path)[1].snapshot(at(SHORT + 1))
    assert snap.classify(at(3), 0)[0] == C.VERIFIED_ZERO and snap.classify(at(15), 0)[0] == C.VERIFIED_ZERO     # cumulative since the observation start


def test_07b_an_error_resets_the_source_baseline_so_the_first_second_after_recovery_is_recorder_only(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=True).start()
    rec.traffic(range(0, SHORT + 1), zero={3, 9, 17})
    for second in range(1, 7):
        rec.beat(second)
    rec.error(7.5, affects_from=7)
    rec.recovered(8.5, 9)
    for second in range(10, SHORT + 1):
        rec.beat(second)
    snap = k.recorder_coverage(rec, tmp_path)[1].snapshot(at(SHORT + 1))
    assert snap.classify(at(3), 0)[0] == C.VERIFIED_ZERO                               # before the error
    assert snap.classify(at(7), 0)[0] == C.INTERRUPTED_COLLECTION                      # inside the error
    cls, why = snap.classify(at(9), 0)
    assert cls == C.UNKNOWN_COVERAGE_ZERO and wc.RECORDER_ONLY_CAUSE in why            # recovered and covered, but no source baseline yet
    assert snap.classify(at(17), 0)[0] == C.VERIFIED_ZERO                              # a new baseline from second 10


# ----------------------------------------------------------------------------------------------------------------- 8 stale source evidence
def test_08_evidence_after_the_decision_time_certifies_nothing_at_that_decision(tmp_path):
    rec = j.short_recorder(tmp_path, zero={15})
    cov = k.recorder_coverage(rec, tmp_path)[1]
    early = cov.snapshot(at(10))
    cls, why = early.classify(at(15), 0)
    assert cls == C.STALE_COLLECTOR_EVIDENCE and "newer than the last collector evidence" in why
    assert cov.snapshot(at(SHORT + 1)).classify(at(15), 0)[0] == C.VERIFIED_ZERO


def test_08b_dispatch_rows_written_after_the_decision_time_do_not_exist_yet(tmp_path):
    rec = j.short_recorder(tmp_path, zero={15}, source_count=False)
    evidence = k.dispatch_evidence(rec)
    cov = k.recorder_coverage(rec, tmp_path, source_evidence=evidence)[1]
    assert evidence.issued_through(j.DAY_MS + 20_000, as_of_ms=j.DAY_MS + 5_000) < evidence.issued_through(j.DAY_MS + 20_000)
    assert cov.snapshot(at(10)).classify(at(15), 0)[0] == C.STALE_COLLECTOR_EVIDENCE


# ------------------------------------------------------------------------------------------------------- 9 benchmark zero-fill preserved
def test_09_benchmark_zero_fill_is_unchanged_labelled_and_never_satisfies_the_strict_contract():
    from tests import a0h_support as h
    history = ch.CapturedHistory(h.binned({5: 0}), zero_policy=ch.ZERO_POLICY_BENCHMARK_ZERO_FILL)
    history.advance_to(at(30))
    evidence = history.evidence(at(30))
    assert evidence["coverage_mode"] == wc.MODE_BENCHMARK and evidence["live_semantics"] is False
    block = evidence["coverage_provenance"]
    assert block["contract_satisfied"] is False and block["live_quality"] is False
    assert history.counts_fed["zero_assumed_benchmark"] > 0
    with pytest.raises(ch.CapturedHistoryError) as excinfo:                          # and it cannot be mixed with verified coverage
        ch.CapturedHistory(h.binned(), zero_policy=ch.ZERO_POLICY_BENCHMARK_ZERO_FILL, coverage=wc.VerifiedCoverage.no_evidence(h.source().source_id, "x"))
    assert excinfo.value.code == "zero_policy_conflict"


def test_09b_the_stored_benchmark_inputs_are_untouched():
    for relative in ("data/processed/flashbots/series_2026-09-21_1s.csv.gz",):
        assert (j.REPO / relative).exists() and j.s.sha(j.REPO / relative) == j.s.h.SERIES_09_21_SHA


# ------------------------------------------------------------------------------------------------------------- 10 generic API bypass
def test_10_a_journal_written_without_the_bridge_and_without_a_scope_certifies_nothing(tmp_path):
    """Someone writes a recorder-style journal straight with the generic writer and leaves the scope out."""
    journal, clock, path = j.s.new_journal(tmp_path, started=False)
    journal.start(observation_start_utc=at(0))
    for second in range(1, 21):
        j.s.beat(journal, clock, second, events=0)
    snap = j.s.coverage_for(path, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0)).snapshot(at(21))
    assert snap.classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO


def test_10b_the_generic_class_the_recorder_class_and_the_multi_worker_class_agree_on_the_original_input(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    _report, recorder_cov, _events = k.recorder_coverage(rec, tmp_path)
    generic = wc.VerifiedCoverage.from_journal_file(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=FAST.to_coverage_policy())
    multi = k.wk.MultiWorkerCoverage(source_id=j.SOURCE_ID, roster=k.wk.WorkerRoster.from_benchmark_config(1, k.DOMAIN), workers=[(0, recorder_cov)])
    verdicts = {name: c.snapshot(at(SHORT + 1)).classify(at(5), 0)[0] for name, c in (("generic", generic), ("recorder", recorder_cov), ("multi", multi))}
    assert set(verdicts.values()) == {C.UNKNOWN_COVERAGE_ZERO}, verdicts


def test_10c_the_only_route_to_verified_zero_in_the_source_is_the_scope_gate():
    import re
    from pathlib import Path
    src = j.REPO / "src" / "fabric_perf"
    producers = []
    for path in src.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(r"CoverageClass\.VERIFIED_ZERO", text):
            line = text[:match.start()].count("\n") + 1
            producers.append((path.name, line, text.splitlines()[line - 1].strip()))
    returning = [(n, line_no, text) for n, line_no, text in producers if "return" in text]
    allowed = {"workload_coverage.py", "worker_coverage.py"}
    assert {n for n, _l, _t in returning} <= allowed, returning
    assert not any("count > 0 else" in t and "VERIFIED_ZERO" in t and "OBSERVED_POSITIVE" in t and n == "recorder_coverage.py" for n, _l, t in producers)


# ------------------------------------------------------------------------------------------------------- 11 Stage 3 wrapper bypass
def test_11_the_plain_captured_history_wrapper_over_a_recorder_journal_stores_no_verified_zero(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    _report, _cov, events = k.recorder_coverage(rec, tmp_path)
    generic = wc.VerifiedCoverage.from_journal_file(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=FAST.to_coverage_policy())
    history = ch.CapturedHistory(rc.bin_submit_events(events, j.recorder_source()), coverage=generic)         # NOT RecorderCapturedHistory
    history.advance_to(at(SHORT + 1))
    assert history.coverage_counts[C.VERIFIED_ZERO.value] == 0 and history.coverage_counts[C.UNKNOWN_COVERAGE_ZERO.value] >= 1
    assert history.evidence(at(SHORT + 1))["validation_outcome"] == ch.VALID_INCOMPLETE


def test_11b_a_source_stream_scope_journal_cannot_be_attached_to_recorder_derived_arrivals(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5})
    _report, _cov, events = k.recorder_coverage(rec, tmp_path)
    journal, clock, path = j.s.new_journal(tmp_path, name="stream.jsonl", source_id=j.SOURCE_ID)
    for second in range(1, 21):
        j.s.beat(journal, clock, second)
    stream = j.s.coverage_for(path, source_id=j.SOURCE_ID, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0))
    with pytest.raises(ch.CapturedHistoryError) as excinfo:
        ch.CapturedHistory(rc.bin_submit_events(events, j.recorder_source()), coverage=stream)
    assert excinfo.value.code == "coverage_scope_source_mismatch"


def test_11c_an_undeclared_scope_journal_cannot_be_attached_to_recorder_derived_arrivals(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5})
    _report, _cov, events = k.recorder_coverage(rec, tmp_path)
    journal, clock, path = j.s.new_journal(tmp_path, name="undeclared.jsonl", started=False)
    journal.start(observation_start_utc=at(0))
    for second in range(1, 21):
        j.s.beat(journal, clock, second)
    undeclared = j.s.coverage_for(path, source_id=j.SOURCE_ID, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0))
    with pytest.raises(ch.CapturedHistoryError) as excinfo:
        ch.CapturedHistory(rc.bin_submit_events(events, j.recorder_source()), coverage=undeclared)
    assert excinfo.value.code == "coverage_scope_source_mismatch"


def test_11d_the_recorder_history_still_refuses_a_non_recorder_coverage_object(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5})
    _report, _cov, events = k.recorder_coverage(rec, tmp_path)
    generic = wc.VerifiedCoverage.from_journal_file(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=FAST.to_coverage_policy())
    with pytest.raises(ch.CapturedHistoryError) as excinfo:
        rc.RecorderCapturedHistory(rc.bin_submit_events(events, j.recorder_source()), coverage=generic, policy=FAST)
    assert excinfo.value.code == "coverage_type_invalid"


# ------------------------------------------------------------------------------------------------------------------ evidence-object bypass
def test_12_a_duck_typed_source_evidence_object_cannot_supply_its_own_verified_spans(tmp_path):
    class Forger:
        def spans(self, derivation, as_of, events):
            return sc.SourceSpans(verified=((derivation.first_start, as_of),), evidence_kind="forged")
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    rec.write()
    j.bridge.ingest_spool(rec.spool_path, tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=FAST)
    with pytest.raises(wc.CoverageEvidenceError) as excinfo:
        wc.VerifiedCoverage.from_journal_file(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=FAST.to_coverage_policy(), source_evidence=Forger())
    assert excinfo.value.code == "source_evidence_type_invalid"


# ----------------------------------------------------------------------------------------------- closed after the first mutation pass (A0k)
def test_13_a_recorder_derived_kind_is_recorder_dependent_even_with_an_innocent_derived_from_and_an_independence_flag():
    verdict = sc.classify_descriptor(k.descriptor(kind=sc.KIND_RECORDER_DERIVED, derived_from="workload_generator_counter", independent_of_recorder=True))
    assert verdict.label == sc.CLASS_RECORDER_DEPENDENT and not verdict.certifies_intervals and verdict.reason_code == sc.RC_RECORDER_DERIVED


def test_13a_an_explicit_independent_of_recorder_false_is_recorder_dependent_whatever_else_the_descriptor_says():
    verdict = sc.classify_descriptor(k.descriptor(independent_of_recorder=False, derived_from="workload_generator_counter"))
    assert verdict.label == sc.CLASS_RECORDER_DEPENDENT and verdict.reason_code == sc.RC_RECORDER_DERIVED


def test_13b_a_source_count_that_agrees_across_a_heartbeat_gap_does_not_verify_the_gap(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=True).start()
    rec.traffic(range(0, SHORT + 1), zero={7, 15})
    for second in list(range(1, 6)) + list(range(11, SHORT + 1)):                      # no heartbeat from second 6 to 10: longer than the 5 s limit
        rec.beat(second)
    snap = k.recorder_coverage(rec, tmp_path)[1].snapshot(at(SHORT + 1))
    assert snap.classify(at(7), 0)[0] == C.COVERAGE_GAP                                # a coverage gap, never a certified zero
    assert snap.classify(at(15), 0)[0] == C.VERIFIED_ZERO
    assert all(not (start < at(10) and end > at(6)) for start, end in snap.source_verified)        # no verified span reaches into the gap
    assert snap.summary()["source_evidence"]["verified_seconds"] == sum(int((e - s).total_seconds()) for s, e in snap.source_verified)


def test_13c_a_cumulative_source_count_that_moves_backwards_invalidates_the_attestation(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=True).start()
    rec.traffic(range(0, SHORT + 1), zero={5})
    for second in range(1, SHORT + 1):
        rec.beat(second)
    beats = [r for r in rec.rows if r["kind"] == "heartbeat"]
    beats[9]["source_events_expected"] = beats[8]["source_events_expected"] - 1          # the cumulative count falls
    snap = k.recorder_coverage(rec, tmp_path)[1].snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == "source_count_regressed"
    assert snap.classify(at(5), 0)[0] == C.INVALID_ATTESTATION


def test_13d_a_recorder_count_that_moves_backwards_is_refused_before_it_can_be_compared(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=True).start()
    rec.traffic(range(0, SHORT + 1))
    for second in range(1, SHORT + 1):
        rec.beat(second)
    beats = [r for r in rec.rows if r["kind"] == "heartbeat"]
    beats[9]["events_received"] = beats[8]["events_received"] - 1
    rec.write()
    j.bridge.ingest_spool(rec.spool_path, tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=FAST)
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    snap = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=FAST).snapshot(at(SHORT + 1))
    assert not snap.valid                                                                # the recorder route also checks the count against its own submits file


# ------------------------------------------------------------------------------------------ closed after the first mutation pass (A0k)
def test_15f_a_transaction_in_flight_at_the_observation_start_is_not_trusted_as_a_baseline(tmp_path):
    """Second 5's transactions were dispatched at 4.95 s (before the observation start at 5 s) and called back at 5.25 s: the recorder counts them, the
    generator log's count from the start does not. A start inside the callback-lag allowance is unsettled, so the first settled heartbeat becomes the baseline."""
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=False).start(t=4.5, observation=5)
    rec.traffic(range(5, SHORT + 1), zero={15})
    for second in range(6, SHORT + 1):
        rec.beat(second)
    early = [k.DAY_MS + 4950 + i for i in range(rec.truth[5])]
    later = [k.DAY_MS + sec * 1000 + 10 + i * 5 for sec in sorted(rec.truth) if sec != 5 for i in range(rec.truth[sec])]
    stamps = sorted(early + later)
    text = ",".join(sc.DISPATCH_HEADER) + "\n" + "".join(f"{seq},0,{ms},{ms},0\n" for seq, ms in enumerate(stamps))
    ev = k.dispatch_evidence(rec, text=text)
    snap = k.recorder_coverage(rec, tmp_path, source_evidence=ev)[1].snapshot(at(SHORT + 1))
    assert snap.valid and snap.classify(at(15), 0)[0] == C.VERIFIED_ZERO


def test_15g_dispatch_evidence_verifies_only_what_exists_at_an_earlier_decision_time(tmp_path):
    rec = j.short_recorder(tmp_path, zero={15}, source_count=False)
    cov = k.recorder_coverage(rec, tmp_path, source_evidence=k.dispatch_evidence(rec))[1]
    early = cov.snapshot(at(8))
    assert early.valid and early.classify(at(3), 3)[0] == C.OBSERVED_POSITIVE and early.source_verified and early.source_verified[-1][1] <= at(8)


def test_15h_a_source_count_the_start_row_never_declared_is_a_contradiction_not_evidence(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=False).start()
    rec.traffic(range(0, SHORT + 1), zero={5})
    for second in range(1, SHORT + 1):
        rec.beat(second, with_source=second >= 3)                                       # source fields appear although the start declared none
    snap = k.recorder_coverage(rec, tmp_path)[1].snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == "source_count_undeclared"
    assert snap.classify(at(5), 0)[0] == C.INVALID_ATTESTATION
