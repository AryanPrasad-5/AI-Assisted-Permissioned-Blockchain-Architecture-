"""A0l workstream C (checklist items 23-31): generator-side issuance evidence in the source-count and coverage contracts.

Two routes, kept apart:
  R1  the recorder's callbacks are the arrivals and the issue log is the INDEPENDENT source count (``IssueLogEvidence`` through ``source_evidence``);
  R2  the generator's own attempt records are the arrivals and the journal is written from the log's liveness points (``GeneratorIssueCoverage``).
Everything is SYNTHETIC. A verified generator interval supports a claim about the experimental generator's issuance only.
"""

from __future__ import annotations

import json

import pytest

from tests import a0j_support as j
from tests import a0k_support as k
from tests import a0l_support as a
from tests.a0l_support import C, DAY_MS, LAG_MS, SHORT, SOURCE_ID, Reader, Scripted, SyntheticIssueLog, at, bridge, gc, ilr, rc, sc, wc

FAST = a.FAST


# ------------------------------------------------------------------------------------------------------------------------------ helpers
def r1_evidence(tmp, rec, *, skip_ticks=(), stop=True, unresolved=(), skip_tx=(), lag=LAG_MS, extra_tx=None, last_tick=SHORT, run_id=a.RUN, worker_id=a.WORKER,
                latency=0.0):
    """The issue log a generator would write for ``rec``'s scripted source (``rec.truth`` transactions in second s, issued at s + 0.01 + 0.005 i)."""
    log = SyntheticIssueLog(tmp / "ig.issue.jsonl", run_id=run_id, worker_id=worker_id)
    gen = Scripted(log).start(-0.5)
    pol = a.policy(run_id=run_id, worker_id=worker_id, max_tick_gap_ms=a.SHORT_GAP_MS)
    reader = Reader(log.path, pol=pol)
    reader.poll(-0.4)
    for sec in range(1, last_tick + 1):
        for i in range((extra_tx or {}).get(sec - 1, rec.truth.get(sec - 1, 0) if sec - 1 not in skip_tx else 0)):
            gen.tx(sec - 1 + 0.01 + 0.005 * i, outcome_after=None if (sec - 1) in unresolved and i == 0 else 0.0)
        if sec not in skip_ticks:
            gen.tick(sec)
        reader.poll(sec + latency)
    if stop:
        gen.stop(last_tick + 0.0)
        reader.poll(last_tick + latency)
    return reader.tail.evidence(instance=rec.instance, max_callback_lag_ms=lag), reader, gen


def r1_coverage(tmp, rec, **kw):
    evidence, reader, gen = r1_evidence(tmp, rec, **kw)
    _report, coverage, _events = k.recorder_coverage(rec, tmp, source_evidence=evidence)
    return coverage, evidence, reader, gen


def classify(coverage, second, count, *, as_of=SHORT + 1):
    return coverage.snapshot(at(as_of)).classify(at(second), count)[0]


# ------------------------------------------------------------------------------------------------------------------------------ 23 positives
def test_23_a_generator_interval_with_attempts_is_observed_positive_and_carries_issue_time_provenance(tmp_path):
    gen, reader, coverage = a.short_history(tmp_path)
    snap = coverage.snapshot(at(SHORT + 1))
    assert snap.valid and snap.scope == wc.SCOPE_SOURCE_STREAM
    assert {snap.classify(at(s), 2)[0] for s in range(0, SHORT)} == {C.OBSERVED_POSITIVE}
    events = reader.tail.attempt_events(reader.bridge.instance)
    assert len(events) == 2 * SHORT and events[0]["ts_ms"] == DAY_MS + 100 and events[1]["ts_ms"] == DAY_MS + 150          # the generator's own issue times
    source = gc.GeneratorIssueSource(source_id=SOURCE_ID, utc_day=j.DAY, dataset_path="synthetic", evidence_origin=gc.ORIGIN_SYNTHETIC)
    binned = gc.bin_issue_events(events, source)
    assert binned.counts[0] == 2 and binned.counts[SHORT - 1] == 2 and binned.counts[SHORT] == 0
    doc = source.as_dict()
    assert doc["event_time"] == "generator_issue_time" and doc["external_mempool_capture"] is False and doc["is_fabric_observation"] is False and doc["synthetic"] is True
    assert "NOT Ethereum mempool arrivals" in doc["represents"]


def test_23b_the_recorder_route_with_issue_log_evidence_observes_positives_and_reports_the_source_kind(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    coverage, evidence, reader, gen = r1_coverage(tmp_path, rec)
    snap = coverage.snapshot(at(SHORT + 1))
    assert snap.valid and snap.source_info["kind"] == "issue_log"
    assert snap.source_info["classifications"][rec.instance]["class"] == sc.CLASS_INTERVAL and snap.source_info["rejection_codes"] == []
    assert snap.classify(at(3), rec.truth[3])[0] == C.OBSERVED_POSITIVE
    assert evidence.timestamp_provenance()["event_time"] == sc.SEMANTICS_ISSUE and evidence.timestamp_provenance()["not_a_callback_time"] is True
    assert evidence.descriptor["derived_from"] == ilr.ISSUE_DERIVED_FROM and evidence.descriptor["independent_of_recorder"] is True


# ------------------------------------------------------------------------------------------------------------------------------ 24 verified zero
def test_24_a_zero_second_is_verified_only_inside_a_span_in_which_the_generator_was_observed(tmp_path):
    truth = {s: 2 for s in range(SHORT) if s != 5}
    gen, reader, coverage = a.short_history(tmp_path, truth=truth)
    snap = coverage.snapshot(at(SHORT + 1))
    assert snap.classify(at(5), 0)[0] == C.VERIFIED_ZERO
    assert snap.classify(at(2), 2)[0] == C.OBSERVED_POSITIVE
    assert snap.classify(at(SHORT + 5), 0)[0] != C.VERIFIED_ZERO                         # after the generator's stop: not covered
    assert snap.classify(at(-3), 0)[0] != C.VERIFIED_ZERO                                # before the observation start: not covered
    assert snap.scope == wc.SCOPE_SOURCE_STREAM and "generator" in " ".join(e.get("coverage_subject", "") for e in coverage.events if e["kind"] == "start")


def test_24b_the_recorder_route_verifies_a_zero_only_where_recorder_and_generator_agree_and_the_generator_was_observed(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    coverage, evidence, reader, gen = r1_coverage(tmp_path, rec)
    snap = coverage.snapshot(at(SHORT + 1))
    assert snap.classify(at(5), 0)[0] == C.VERIFIED_ZERO
    assert snap.source_info["verified_seconds"] >= SHORT - 1


def test_24c_a_generator_that_is_alive_but_idle_certifies_zeros_while_a_missing_log_certifies_nothing(tmp_path):
    gen, reader, coverage = a.short_history(tmp_path, truth={})
    snap = coverage.snapshot(at(SHORT + 1))
    assert {snap.classify(at(s), 0)[0] for s in range(0, SHORT)} == {C.VERIFIED_ZERO}   # ticks prove it was alive; no transaction in 20 s is a genuine zero
    nothing = Reader(tmp_path / "absent.jsonl").with_bridge(tmp_path / "none.jsonl")
    result, report = nothing.poll(0)
    assert result.status == ilr.S_MISSING and report["state"] == "not_started" and not (tmp_path / "none.jsonl").exists()
    with pytest.raises(wc.CoverageEvidenceError) as info:                                # no journal -> no coverage object -> no zero
        gc.GeneratorIssueCoverage.from_bridge(nothing.bridge, policy=wc.CoveragePolicy())
    assert info.value.code == "journal_missing"


# ------------------------------------------------------------------------------------------------------------------------------ 25 empty without health
def test_25_an_empty_log_with_no_liveness_points_certifies_no_zero_on_either_route(tmp_path):
    log = SyntheticIssueLog(tmp_path / "e.jsonl")
    Scripted(log).start(-0.5)                                                            # a start and nothing else: no tick, no stop
    reader = Reader(log.path).with_bridge(tmp_path / "ej.jsonl")
    reader.poll(-0.4)
    for sec in range(1, SHORT + 1):
        reader.poll(sec)                                                                 # the file is quiet for 20 s
    coverage = gc.GeneratorIssueCoverage.from_bridge(reader.bridge, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0))
    snap = coverage.snapshot(at(SHORT + 1))
    assert snap.valid and snap.derivation.covered == [] and snap.derivation.heartbeats == []
    classes = {snap.classify(at(s), 0)[0] for s in range(0, SHORT)}
    assert classes and not classes & wc.ACCEPTED_CLASSES and classes <= {C.UNKNOWN_COVERAGE_ZERO, C.STALE_COLLECTOR_EVIDENCE}
    assert reader.tail.writer_state()["state"] == ilr.W_ACTIVE and reader.tail.observed_segments() == []
    quiet = j.SyntheticRecorder(tmp_path, name="quiet", declared_interval_ms=1000, source_count=False).start()
    quiet.traffic(range(0, SHORT + 1), zero=set(range(SHORT + 1)))                       # a recorder that saw nothing at all
    for sec in range(1, SHORT + 1):
        quiet.beat(sec)
    evidence = reader.tail.evidence(instance=quiet.instance, max_callback_lag_ms=LAG_MS)
    _r, cov, _e = k.recorder_coverage(quiet, tmp_path, journal_name="quiet.jsonl", source_evidence=evidence)
    snap1 = cov.snapshot(at(SHORT + 1))
    assert snap1.valid and snap1.classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO       # 0 = 0 agrees, but nothing shows the generator was alive
    assert ilr.RC_GENERATOR_UNOBSERVED in snap1.source_info["rejection_codes"] and snap1.source_info["verified_seconds"] == 0
    (tmp_path / "busy").mkdir()
    busy = j.short_recorder(tmp_path / "busy", zero={5}, source_count=False)
    evidence = reader.tail.evidence(instance=busy.instance, max_callback_lag_ms=LAG_MS)
    _r, cov2, _e = k.recorder_coverage(busy, tmp_path / "busy", source_evidence=evidence)
    assert cov2.snapshot(at(SHORT + 1)).failure_code == "recorder_exceeds_source"        # traffic the recorder counted but an empty generator log denies: contradiction


# ------------------------------------------------------------------------------------------------------------------------------ 26 interruption
def test_26_a_generator_whose_ticks_stop_is_uncovered_from_the_last_liveness_point_on(tmp_path):
    truth = {s: 2 for s in range(0, 10)}
    log = SyntheticIssueLog(tmp_path / "i.jsonl")
    gen = Scripted(log).start(-0.5)
    reader = Reader(log.path).with_bridge(tmp_path / "ij.jsonl")
    reader.poll(-0.4)
    for sec in range(1, 11):
        for i in range(truth.get(sec - 1, 0)):
            gen.tx(sec - 1 + 0.1 + 0.05 * i)
        gen.tick(sec)
        reader.poll(sec)
    for sec in range(11, SHORT + 1):
        reader.poll(sec)                                                                 # the generator was killed at second 10; no stop record, ever
    coverage = gc.GeneratorIssueCoverage.from_bridge(reader.bridge, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0))
    snap = coverage.snapshot(at(SHORT + 1))
    assert snap.classify(at(5), 2)[0] == C.OBSERVED_POSITIVE
    assert snap.classify(at(10), 0)[0] == C.STALE_COLLECTOR_EVIDENCE                      # the last evidence is the watermark at 10; nothing newer exists
    for second in (12, 15, 19):
        assert snap.classify(at(second), 0)[0] not in wc.ACCEPTED_CLASSES, second
        assert snap.classify(at(second), 3)[0] not in wc.ACCEPTED_CLASSES, second       # not even a positive: the generator is no longer shown to be alive
    state = reader.tail.writer_state()
    assert state["state"] == ilr.W_ACTIVE and state["last_liveness_point_ms"] == DAY_MS + 10_000 and reader.tail.stop_record is None
    assert snap.derivation.terminated_cleanly is False


def test_26b_on_the_recorder_route_a_dead_generator_leaves_later_zeros_unverified_even_if_the_recorder_agrees(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=False).start()
    rec.traffic(range(0, 11), zero=set(), units_fn=lambda s: 1)
    for sec in range(1, SHORT + 1):
        rec.beat(sec)                                                                    # the recorder keeps beating; the source went quiet after second 10
    rec.stop(SHORT)
    evidence, reader, gen = r1_evidence(tmp_path, rec, skip_ticks=set(range(11, SHORT + 1)), stop=False, last_tick=SHORT)
    _r, coverage, _e = k.recorder_coverage(rec, tmp_path, source_evidence=evidence)
    snap = coverage.snapshot(at(SHORT + 1))
    assert snap.classify(at(5), 1)[0] == C.OBSERVED_POSITIVE
    for second in (11, 15, 19):
        assert snap.classify(at(second), 0)[0] == C.UNKNOWN_COVERAGE_ZERO, second        # the recorder agrees (0 = 0) but the generator is not shown to be alive
    assert snap.classify(at(5), 0)[0] != C.UNKNOWN_COVERAGE_ZERO


# ------------------------------------------------------------------------------------------------------------------------------ 27 mismatch
def test_27_missed_recorder_callbacks_leave_the_span_unverified_never_certified(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, missed={8}, source_count=False)
    coverage, evidence, reader, gen = r1_coverage(tmp_path, rec)
    snap = coverage.snapshot(at(SHORT + 1))
    assert snap.valid and snap.classify(at(5), 0)[0] == C.VERIFIED_ZERO
    assert snap.classify(at(8), 0)[0] == C.INTERRUPTED_COLLECTION                       # the generator issued; the recorder saw nothing: a missed callback, not a zero
    assert any(c.startswith("source_count_mismatch") for _s, _e, _cls, c in snap.extra_uncovered)


def test_27b_a_recorder_that_counts_more_than_the_generator_issued_invalidates_the_whole_attestation(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    coverage, evidence, reader, gen = r1_coverage(tmp_path, rec, skip_tx={7})            # the generator log under-reports second 7 (a dropped record)
    snap = coverage.snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == "recorder_exceeds_source"
    assert snap.classify(at(5), 0)[0] == C.INVALID_ATTESTATION


def test_27c_counts_are_reported_side_by_side_and_a_difference_is_never_repaired(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, missed={8}, source_count=False)
    evidence, reader, gen = r1_evidence(tmp_path, rec)
    rec.write()
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    units = sum(e["units"] for e in events)
    report = ilr.reconcile_counts(reader.tail, recorder_submit_units=units)
    issued = sum(v for s_, v in rec.truth.items() if s_ < SHORT)                       # the generator is ticked and read through second 20, so seconds 0..19
    assert report["generator"]["submission_attempts"] == issued
    assert report["recorder_submit_callbacks"]["units"] == units and report["recorder_submit_callbacks"]["minus_generator_attempts"] == units - issued
    assert units - issued == rec.truth[SHORT] - rec.truth[8]                           # the missed second and the not-yet-issued last second, side by side, unrepaired
    assert "counts only" in report["recorder_submit_callbacks"]["join"] and "note" in report


def test_27d_identifiers_are_joined_only_on_a_stable_key_and_never_by_timestamp(tmp_path):
    log = SyntheticIssueLog(tmp_path / "ids.jsonl")
    gen = Scripted(log).start(-0.5)
    for i in range(4):
        n = log.intent(1.0 + i * 0.1)
        log.attempt(1.0 + i * 0.1, n)
        log.outcome(1.05 + i * 0.1, n, "returned", client_tx_id=f"tx{i}" if i < 3 else None)
        gen.intents += 1
        gen.attempted += 1
        gen.outcomes += 1
    reader = Reader(log.path)
    reader.poll(5)
    report = ilr.reconcile_counts(reader.tail, recorder_completed_ids=["tx0", "tx1", "txZ"], ledger_committed_keys=[f"{a.RUN}_1_000000{i}" for i in (0, 2, 3, 9)])
    assert report["recorder_completed"]["matched_by_client_tx_id"] == 2 and report["recorder_completed"]["generator_ids_without_recorder_row"] == 1
    assert report["recorder_completed"]["generator_ids_known"] == 3 and "stable identifier" in report["recorder_completed"]["join"]
    assert report["ledger_committed"]["matched_by_asset_key"] == 3 and report["ledger_committed"]["committed_keys_not_issued_here"] == 1
    assert report["ledger_committed"]["generator_keys_not_committed"] == 1
    assert "timestamp" not in json.dumps({k_: v for k_, v in report.items() if k_ != "note"})


# ------------------------------------------------------------------------------------------------------------------------------ 28 missing confirmation
def test_28_attempts_without_an_outcome_are_counted_as_attempts_and_never_reported_as_completed(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    coverage, evidence, reader, gen = r1_coverage(tmp_path, rec, unresolved={8}, stop=False)
    counts = reader.tail.counts()
    assert counts["unresolved"] == 1 and counts["returned"] == counts["attempted"] - 1 and evidence.counts["unresolved"] == 1
    snap = coverage.snapshot(at(SHORT + 1))
    assert snap.classify(at(8), rec.truth[8])[0] == C.OBSERVED_POSITIVE                 # it was issued: positive
    report = ilr.reconcile_counts(reader.tail)
    assert report["generator"]["unresolved_attempts"] == 1 and report["generator"]["send_returned"] == counts["returned"]


def test_28b_a_stop_that_leaves_unresolved_attempts_is_an_aborted_stop_and_its_last_span_is_not_certified(tmp_path):
    log = SyntheticIssueLog(tmp_path / "u.jsonl")
    gen = Scripted(log).start(-0.5)
    reader = Reader(log.path).with_bridge(tmp_path / "uj.jsonl")
    reader.poll(-0.4)
    for sec in range(1, 11):
        if sec == 5:
            gen.tx(4.1, outcome_after=None)                                              # the call never reported back
        gen.tick(sec)
        reader.poll(sec)
    gen.stop(11.0)
    reader.poll(11)
    assert reader.tail.writer_state()["state"] == ilr.W_CLOSED_OTHER and reader.tail.stop_record["in_flight"] == 1
    rows = [json.loads(line) for line in (tmp_path / "uj.jsonl").read_text(encoding="utf-8").splitlines()]
    assert rows[-1]["kind"] == "stop" and rows[-1]["reason"] == "abort"
    coverage = gc.GeneratorIssueCoverage.from_bridge(reader.bridge, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0))
    snap = coverage.snapshot(at(12))
    assert snap.classify(at(10), 0)[0] == C.INTERRUPTED_COLLECTION                       # the span closed by the aborted stop
    assert snap.classify(at(9), 0)[0] == C.VERIFIED_ZERO and snap.classify(at(2), 0)[0] == C.VERIFIED_ZERO     # earlier healthy spans stay certified


# ------------------------------------------------------------------------------------------------------------------------------ 29 independence
def test_29_the_generator_evidence_decides_the_span_whatever_the_recorder_claims_about_its_own_source_count(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, missed={8}, source_count=True)           # the recorder's own journal-reported counts say "source agrees"
    _r, plain, _e = k.recorder_coverage(rec, tmp_path, journal_name="plain.jsonl")
    assert plain.snapshot(at(SHORT + 1)).source_info["kind"] == "journal_reported_counts"
    evidence, reader, gen = r1_evidence(tmp_path, rec)
    _r, coverage, _e = k.recorder_coverage(rec, tmp_path, journal_name="issue.jsonl", source_evidence=evidence)
    snap = coverage.snapshot(at(SHORT + 1))
    assert snap.source_info["kind"] == "issue_log"
    assert snap.classify(at(8), 0)[0] == C.INTERRUPTED_COLLECTION                       # the log is not asked what the recorder thinks


def test_29b_evidence_for_another_run_or_worker_than_the_recorder_instance_invalidates_the_attestation(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    for kw, code in (({"worker_id": "w1"}, "worker_identity_mismatch"), ({"run_id": "another-run"}, "run_identity_mismatch")):
        sub = tmp_path / code
        sub.mkdir()
        ev, rdr, _g = r1_evidence(sub, rec, **kw)
        _r, cov, _e = k.recorder_coverage(rec, sub, source_evidence=ev)
        snap = cov.snapshot(at(SHORT + 1))
        assert not snap.valid and snap.failure_code == code, (kw, snap.failure_code)


def test_29c_the_source_evidence_slot_accepts_the_issue_log_class_and_still_refuses_a_duck_typed_object(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)

    class Fake:
        def spans(self, derivation, as_of, events=None):
            return sc.SourceSpans(((at(0), at(20)),), (), ("x",), ("True",), {}, (), "fake")
    rec.write()
    bridge.ingest_spool(rec.spool_path, tmp_path / "journal.jsonl", source_id=rec.source_id, policy=FAST)
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    with pytest.raises(wc.CoverageEvidenceError) as info:
        rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=rec.source_id, submit_events=events, policy=FAST,
                                                          source_evidence=Fake())
    assert info.value.code == "source_evidence_type_invalid"
    (tmp_path / "x").mkdir()
    evidence, _r, _g = r1_evidence(tmp_path / "x", rec)
    assert isinstance(evidence, sc.DispatchLogEvidence) and isinstance(evidence, ilr.IssueLogEvidence)


def test_29d_the_issue_log_evidence_refuses_a_descriptor_that_is_not_interval_suitable_or_not_derived_from_the_issue_log():
    good = ilr.issue_log_descriptor(a.RUN, "w0")
    base = dict(worker="w0", clock_domain=a.DOMAIN, run_id=a.RUN, instance="i", max_callback_lag_ms=500, dispatch_ms=(), read_ms=())
    assert ilr.IssueLogEvidence(descriptor=good, **base).descriptor == good
    for over in ({"derived_from": "recorder_submits_file"}, {"derived_from": "workload_module_dispatch_log"}, {"independent_of_recorder": False},
                 {"independent_of_recorder": None}, {"kind": "round_total_configured"}, {"timestamp_semantics": "recorder_callback_time"}, {"resolution": "round_total"},
                 {"counter_id": None}):
        doc = {kk: v for kk, v in {**good, **over}.items() if v is not None}
        with pytest.raises(sc.SourceEvidenceError) as info:
            ilr.IssueLogEvidence(descriptor=doc, **base)
        assert info.value.code in {"source_descriptor_not_interval_suitable"}, over
    for over, code in (({"worker": ""}, "source_evidence_identity_missing"), ({"max_callback_lag_ms": -1}, "source_evidence_policy_invalid"),
                       ({"dispatch_ms": ((0, 5),), "read_ms": ()}, "source_evidence_inconsistent"), ({"dispatch_ms": ((1, 5),), "read_ms": (9,)}, "dispatch_seq_gap"),
                       ({"dispatch_ms": ((0, 9), (1, 5)), "read_ms": (9, 9)}, "dispatch_clock_regression"), ({"dispatch_ms": ((0, 5), (1, 6)), "read_ms": (9, 8)}, "read_clock_regressed")):
        with pytest.raises(sc.SourceEvidenceError) as info:
            ilr.IssueLogEvidence(descriptor=good, **{**base, **over})
        assert info.value.code == code, over


def test_29e_evidence_is_cut_at_the_decision_time_by_read_time_not_by_event_time(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    evidence, reader, gen = r1_evidence(tmp_path, rec, stop=False)
    _r, coverage, _e = k.recorder_coverage(rec, tmp_path, source_evidence=evidence)
    early = coverage.snapshot(at(8))                                                      # the generator's records for seconds 8..19 were READ later than this
    assert early.valid and early.source_info["verified_seconds"] <= 8
    assert early.classify(at(5), 0)[0] == C.VERIFIED_ZERO and early.classify(at(12), rec.truth.get(12, 0))[0] not in wc.ACCEPTED_CLASSES
    late = coverage.snapshot(at(SHORT + 1))
    assert late.source_info["verified_seconds"] > early.source_info["verified_seconds"]


def test_29f_generator_observation_counts_only_from_the_moment_its_closing_tick_was_read(tmp_path):
    quiet = j.SyntheticRecorder(tmp_path, name="quiet", declared_interval_ms=1000, source_count=False).start()
    quiet.traffic(range(0, SHORT + 1), zero=set(range(SHORT + 1)))
    for sec in range(1, SHORT + 1):
        quiet.beat(sec)
    log = SyntheticIssueLog(tmp_path / "lat.jsonl")
    gen = Scripted(log).start(-0.5)
    reader = Reader(log.path, pol=a.policy(max_tick_gap_ms=a.SHORT_GAP_MS))
    reader.poll(-0.4)
    for sec in range(1, SHORT + 1):
        gen.tick(sec)
        reader.poll(sec + 3.0)                                                           # every tick is read 3 s after it was written
    evidence = reader.tail.evidence(instance=quiet.instance, max_callback_lag_ms=LAG_MS)
    _r, cov, _e = k.recorder_coverage(quiet, tmp_path, source_evidence=evidence)
    early = cov.snapshot(at(8))                                                          # ticks 1..5 have been read by second 8, tick 6 has not
    assert early.valid and early.classify(at(3), 0)[0] == C.VERIFIED_ZERO
    assert early.classify(at(7), 0)[0] != C.VERIFIED_ZERO and early.source_info["verified_seconds"] <= 6
    assert cov.snapshot(at(25)).classify(at(7), 0)[0] == C.VERIFIED_ZERO


def test_29g_an_issuance_the_reader_had_not_yet_read_makes_the_recorder_look_ahead_of_the_source_until_it_is_read(tmp_path):
    """Fail closed by design: with read latency the recorder can count a transaction whose generator record has not been read at the decision time. That reads as
    'recorder exceeds source' (the whole attestation is invalid AT THAT DECISION TIME) and resolves once the record is read. A limit of the contract, stated in the report."""
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    evidence, reader, gen = r1_evidence(tmp_path, rec, latency=3.0)
    _r, cov, _e = k.recorder_coverage(rec, tmp_path, source_evidence=evidence)
    early = cov.snapshot(at(8))
    assert not early.valid and early.failure_code == "recorder_exceeds_source"
    assert cov.snapshot(at(30)).valid and cov.snapshot(at(30)).classify(at(5), 0)[0] == C.VERIFIED_ZERO


# ------------------------------------------------------------------------------------------------------------------------------ 30 scope
def test_30_generator_arrivals_need_generator_coverage_and_recorder_coverage_is_refused(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=True)
    _r, rcov, events = k.recorder_coverage(rec, tmp_path)
    (tmp_path / "g").mkdir()
    gen, reader, gcov = a.short_history(tmp_path / "g")
    source = gc.GeneratorIssueSource(source_id=rec.source_id, utc_day=j.DAY, dataset_path="synthetic", evidence_origin=gc.ORIGIN_SYNTHETIC)
    binned = gc.bin_issue_events(reader.tail.attempt_events(reader.bridge.instance), source)
    with pytest.raises(ch_error()) as info:
        gc.GeneratorIssueCapturedHistory(binned, coverage=rcov)
    assert info.value.code == "coverage_type_invalid"
    plain = wc.VerifiedCoverage.from_journal_file(tmp_path / "journal.jsonl", source_id=rec.source_id, policy=FAST.to_coverage_policy(), run_id=a.RUN)
    with pytest.raises(ch_error()) as info:
        gc.GeneratorIssueCapturedHistory(binned, coverage=plain)
    assert info.value.code == "coverage_type_invalid"
    with pytest.raises(ch_error()) as info:                                              # recorder-derived arrivals with generator coverage
        a.ch.CapturedHistory(rc.bin_submit_events(events, j.recorder_source(source_id=rec.source_id)), coverage=gcov)
    assert info.value.code in {"coverage_scope_source_mismatch", "coverage_source_mismatch"}
    with pytest.raises(ch_error()) as info:                                              # generator coverage with recorder arrivals' source kind
        gc.GeneratorIssueCapturedHistory(rc.bin_submit_events(events, j.recorder_source(source_id=SOURCE_ID)), coverage=gcov)
    assert info.value.code == "source_type_invalid"


def ch_error():
    return a.ch.CapturedHistoryError


def _forged_generator_coverage(tail, **start_over):
    rows = [j.s.start_row(-0.5, 0, coverage_subject=gc.COVERAGE_SUBJECT, issue_log_origin=gc.ORIGIN_SYNTHETIC, **start_over),
            j.s.heartbeat_row(5, 5), j.s.heartbeat_row(10, 10)]
    return gc.GeneratorIssueCoverage(tail=tail, events=j.s.forge(rows, source_id=SOURCE_ID, run_id=a.RUN), policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0),
                                     source_id=SOURCE_ID, run_id=a.RUN)


def test_30b_a_generator_history_needs_the_source_stream_scope_and_the_generator_subject(tmp_path):
    gen, reader, _cov = a.short_history(tmp_path)
    source = gc.GeneratorIssueSource(source_id=SOURCE_ID, utc_day=j.DAY, dataset_path="synthetic", evidence_origin=gc.ORIGIN_SYNTHETIC)
    binned = gc.bin_issue_events(reader.tail.attempt_events(reader.bridge.instance), source)
    good = _forged_generator_coverage(reader.tail)
    assert gc.GeneratorIssueCapturedHistory(binned, coverage=good)                       # the forged-but-consistent shape is accepted: scope and subject are right
    for over, code in (({"coverage_scope": wc.SCOPE_RECORDER_CALLBACK}, "coverage_scope_source_mismatch"), ({"coverage_scope": None}, "coverage_scope_source_mismatch"),
                       ({"coverage_subject": "ethereum_mempool"}, "coverage_subject_mismatch"), ({"coverage_subject": None}, "coverage_subject_mismatch")):
        rows = [{**j.s.start_row(-0.5, 0, coverage_subject=gc.COVERAGE_SUBJECT, issue_log_origin=gc.ORIGIN_SYNTHETIC), **over}, j.s.heartbeat_row(5, 5)]
        rows[0] = {kk: v for kk, v in rows[0].items() if v is not None}
        bad = gc.GeneratorIssueCoverage(tail=reader.tail, events=j.s.forge(rows, source_id=SOURCE_ID, run_id=a.RUN), policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0),
                                        source_id=SOURCE_ID, run_id=a.RUN)
        with pytest.raises(ch_error()) as info:
            gc.GeneratorIssueCapturedHistory(binned, coverage=bad)
        assert info.value.code == code, (over, info.value.code)


def test_30c_a_generator_journal_never_carries_recorder_fields_and_the_zero_rule_is_the_declared_scope_rule(tmp_path):
    gen, reader, coverage = a.short_history(tmp_path)
    start = next(e for e in coverage.events if e["kind"] == "start")
    assert not set(wc.RECORDER_FINGERPRINT) & set(start)                                  # nothing here can be mistaken for a recorder journal
    assert start["coverage_scope"] == wc.SCOPE_SOURCE_STREAM and start["coverage_subject"] == "generator_issuance" and start["issue_log_origin"] == gc.ORIGIN_SYNTHETIC
    assert start["issue_log"]["descriptor"]["timestamp_semantics"] == sc.SEMANTICS_ISSUE and start["timestamp_provenance"] == "reader_read_time_utc"
    forged = j.s.forge([j.s.start_row(-0.5, 0, coverage_scope=wc.SCOPE_SOURCE_STREAM, recorder_version="x")])            # a forged fingerprint under this scope ...
    with pytest.raises(wc.CoverageEvidenceError) as info:
        wc.derive(forged, wc.CoveragePolicy(), source_id=SOURCE_ID)                                                       # ... is refused by the derivation
    assert info.value.code == "coverage_scope_contradiction"


# ------------------------------------------------------------------------------------------------------------------------------ 31 unknown
def test_31_without_any_coverage_evidence_every_generator_zero_is_unknown(tmp_path):
    gen, reader, _cov = a.short_history(tmp_path, truth={s: 2 for s in range(SHORT) if s != 5})
    source = gc.GeneratorIssueSource(source_id=SOURCE_ID, utc_day=j.DAY, dataset_path="synthetic", evidence_origin=gc.ORIGIN_SYNTHETIC)
    binned = gc.bin_issue_events(reader.tail.attempt_events(reader.bridge.instance), source)
    history = a.ch.CapturedHistory(binned)                                               # no coverage evidence of any kind
    history.advance_to(at(30))
    assert history.counts_fed["zero_unverified_unavailable"] >= 1 and history.counts_fed["zero_attested"] == 0
    assert history.evidence(at(30))["validation_outcome"] != a.ch.VALID_COMPLETE
    none = wc.VerifiedCoverage.no_evidence(SOURCE_ID, "no collector evidence")
    snap = none.snapshot(at(30))
    assert snap.classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO and snap.classify(at(5), 2)[0] == C.OBSERVED_POSITIVE


# ------------------------------------------------------------------------------------------------------------------------------ the bridge
def test_31b_a_gap_between_ticks_is_an_error_and_a_recovery_and_the_seconds_between_are_never_covered(tmp_path):
    log = SyntheticIssueLog(tmp_path / "gap.jsonl")
    gen = Scripted(log).start(-0.5)
    reader = Reader(log.path).with_bridge(tmp_path / "gapj.jsonl")
    reader.poll(-0.4)
    for sec in (1, 2, 3):
        gen.tick(sec)
        reader.poll(sec)
    for sec in range(4, 13):
        reader.poll(sec)                                                                 # silence for 9 s: over the 5 s allowance
    for sec in (13, 14, 15, 16):
        gen.tick(sec)
        reader.poll(sec)
    kinds = [json.loads(line)["kind"] for line in (tmp_path / "gapj.jsonl").read_text(encoding="utf-8").splitlines()]
    assert "error" in kinds and "recovered" in kinds and kinds.index("error") < kinds.index("recovered")
    coverage = gc.GeneratorIssueCoverage.from_bridge(reader.bridge, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0))
    snap = coverage.snapshot(at(17))
    assert snap.valid
    assert snap.classify(at(1), 0)[0] == C.VERIFIED_ZERO
    assert snap.classify(at(8), 0)[0] == C.INTERRUPTED_COLLECTION and snap.classify(at(12), 0)[0] == C.INTERRUPTED_COLLECTION
    assert snap.classify(at(15), 0)[0] == C.VERIFIED_ZERO


def test_31c_a_late_first_read_loses_the_early_seconds_it_never_certifies_them_retroactively(tmp_path):
    log = SyntheticIssueLog(tmp_path / "late.jsonl")
    gen = Scripted(log).start(-0.5)
    for sec in range(1, 11):
        gen.tx(sec - 0.9)
        gen.tick(sec)
    reader = Reader(log.path).with_bridge(tmp_path / "latej.jsonl")
    reader.poll(7.2)                                                                     # the reader only begins at second 7
    reader.poll(10)
    coverage = gc.GeneratorIssueCoverage.from_bridge(reader.bridge, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0))
    snap = coverage.snapshot(at(11))
    start = next(e for e in coverage.events if e["kind"] == "start")
    assert start["observation_start_utc"] == wc.iso(at(8))                              # the first whole second after the first read
    assert snap.classify(at(3), 1)[0] not in {C.OBSERVED_POSITIVE, C.VERIFIED_ZERO}
    assert snap.classify(at(8), 1)[0] == C.OBSERVED_POSITIVE


def test_31d_a_generator_clock_ahead_of_the_reader_is_held_back_never_claimed(tmp_path):
    log = SyntheticIssueLog(tmp_path / "ahead.jsonl")
    gen = Scripted(log).start(-0.5)
    gen.tick(1.0)
    gen.tick(2.0)
    reader = Reader(log.path).with_bridge(tmp_path / "aheadj.jsonl")
    reader.poll(-0.4)
    result, report = reader.poll(0.5)                                                    # the reader's clock is 1.5 s behind the generator's second tick
    assert report["held_back"] == "generator_clock_ahead_of_reader"
    kinds = [json.loads(line)["kind"] for line in (tmp_path / "aheadj.jsonl").read_text(encoding="utf-8").splitlines()]
    assert kinds == ["start"]
    reader.poll(3.0)
    kinds = [json.loads(line)["kind"] for line in (tmp_path / "aheadj.jsonl").read_text(encoding="utf-8").splitlines()]
    assert kinds[-1] == "heartbeat"


def test_31e_an_integrity_failure_invalidates_every_snapshot_from_the_time_it_was_read_and_none_before(tmp_path):
    gen, reader, coverage = a.short_history(tmp_path)
    log = gen.log
    data = log.path.read_bytes().split(b"\n")
    row = json.loads(data[-3])
    row["t_ms"] += 1
    data[-3] = wc.canonical(row).encode("ascii")
    log.path.write_bytes(b"\n".join(data))
    reader.poll(25)                                                                      # the tampered tail is noticed at read time 25
    assert reader.tail.error is not None and reader.tail.error_read_ms == DAY_MS + 25_000
    reader.bridge.sync(at(25))
    fresh = gc.GeneratorIssueCoverage.from_bridge(reader.bridge, policy=wc.CoveragePolicy(max_heartbeat_gap_s=60.0))
    assert fresh.snapshot(at(24)).valid                                                  # at 24 the reader did not yet know
    late = fresh.snapshot(at(26))
    assert not late.valid and late.failure_code == reader.tail.error.code
    assert late.classify(at(2), 0)[0] == C.INVALID_ATTESTATION


def test_31f_the_journal_cross_check_rejects_a_heartbeat_count_that_disagrees_with_the_events(tmp_path):
    gen, reader, coverage = a.short_history(tmp_path)
    events = reader.tail.attempt_events(reader.bridge.instance)
    del events[5]                                                                        # one issuance event missing from what the history is built from
    broken = gc.GeneratorIssueCoverage(tail=reader.tail, events=coverage.events, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0), source_id=SOURCE_ID,
                                       run_id=a.RUN, observed_events=events)
    snap = broken.snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == "heartbeat_event_count_mismatch"


def test_31g_the_bridge_refuses_protected_and_earlier_channels_before_any_file_exists(tmp_path):
    log = SyntheticIssueLog(tmp_path / "c.jsonl")
    Scripted(log).start(-0.5)
    tail = ilr.IssueLogTail(log.path, a.policy(max_tick_gap_ms=a.SHORT_GAP_MS))
    for channel in ("e1-r002", "e1-cfg001", "e1-b001", "not-an-e1-channel", ""):
        with pytest.raises(Exception) as info:
            gc.IssueLogJournalBridge(tail, tmp_path / "cj.jsonl", source_id=SOURCE_ID, origin=gc.ORIGIN_SYNTHETIC, channel_id=channel)
        assert info.type.__name__ in {"ChannelSelectionError", "CoverageEvidenceError"}
        assert not (tmp_path / "cj.jsonl").exists()
    ok = gc.IssueLogJournalBridge(tail, tmp_path / "cj.jsonl", source_id=SOURCE_ID, origin=gc.ORIGIN_SYNTHETIC, channel_id="e1-val-a")
    tail.poll(at(-0.4))
    ok.sync(at(-0.4))
    assert json.loads((tmp_path / "cj.jsonl").read_text(encoding="utf-8").splitlines()[0])["channel_id"] == "e1-val-a"


def test_31h_the_origin_is_declared_and_a_scripted_log_is_never_live_quality(tmp_path):
    with pytest.raises(wc.CoverageEvidenceError):
        gc.IssueLogJournalBridge(ilr.IssueLogTail(tmp_path / "x.jsonl", a.policy()), tmp_path / "xj.jsonl", source_id=SOURCE_ID, origin="live_recorder")
    gen, reader, coverage = a.short_history(tmp_path)
    source = gc.GeneratorIssueSource(source_id=SOURCE_ID, utc_day=j.DAY, dataset_path="synthetic", evidence_origin=gc.ORIGIN_SYNTHETIC)
    history = gc.GeneratorIssueCapturedHistory(gc.bin_issue_events(reader.tail.attempt_events(reader.bridge.instance), source), coverage=coverage, freshness_limit_s=10.0,
                                               min_history_bins=5)
    history.advance_to(at(SHORT))
    block = history.coverage_provenance(at(SHORT))
    assert block["live_quality"] is False and block["synthetic_evidence"] is True and block["live_source_coverage_verified"] is False
    assert block["coverage_subject"] == "generator_issuance" and block["external_mempool_coverage"] is False
    assert block["issue_log"]["independent_of_recorder"].startswith("DECLARED") and "nothing about an external Ethereum mempool" in block["claim"]
