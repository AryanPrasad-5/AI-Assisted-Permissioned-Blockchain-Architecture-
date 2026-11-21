"""A0k workstreams C and E (checklist items 12-19 and the timestamp requirements): what may count as an independent source count, the one adapter the
repository's actual workload generator can justify (the trace-replay dispatch log), and the time semantics that must not be conflated.

Everything is SYNTHETIC: scripted generator logs and recorders. No synthetic counter here proves anything about a real Caliper or Fabric run.
"""

from __future__ import annotations

import pytest

from tests import a0j_support as j
from tests import a0k_support as k
from tests.a0k_support import C, DAY_MS, FAST, SHORT, at, rc, sc, wc


def evidence_from(rec, **overrides):
    return k.dispatch_evidence(rec, **overrides)


# ------------------------------------------------------------------------------------------------- 12 per-transaction issuance evidence
def test_12_a_dispatch_log_gives_per_transaction_issue_time_evidence_that_is_not_the_recorders_callback_stream(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    ev = evidence_from(rec)
    assert len(ev.dispatch_ms) == sum(rec.truth.values()) and ev.descriptor["kind"] == sc.KIND_ISSUE_LOG
    assert ev.timestamp_provenance()["event_time"] == sc.SEMANTICS_ISSUE and ev.timestamp_provenance()["not_a_callback_time"] is True
    per_second = ev.per_second_counts()
    assert per_second.get(DAY_MS // 1000 + 5) is None                                  # an empty source second has no rows
    assert per_second[DAY_MS // 1000 + 4] == rec.truth[4]
    callback_second_of_4 = {(e["ts_ms"] // 1000) for e in rc.read_submit_events(_write(rec), instance=rec.instance)[0]}
    assert DAY_MS // 1000 + 5 not in callback_second_of_4                              # the two streams agree here, but are different files
    assert sc.classify_descriptor(ev.descriptor).label == sc.CLASS_INTERVAL


def _write(rec):
    rec.write()
    return rec.submits_path


def test_12b_the_log_is_read_from_a_file_with_its_overflow_rows_counted_but_not_issued(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    log = tmp_path / "dispatch.csv"
    log.write_text(k.dispatch_text(rec.truth), encoding="utf-8", newline="")
    (tmp_path / "dispatch.csv.overflow.csv").write_text("requested_seq,observed_ms\n9999,1\n10000,2\n", encoding="utf-8", newline="")
    ev = sc.DispatchLogEvidence.from_file(log, worker="0", clock_domain=k.DOMAIN, run_id=j.RUN, instance=rec.instance, max_callback_lag_ms=k.LAG_MS)
    assert ev.overflow_rows == 2 and len(ev.dispatch_ms) == sum(rec.truth.values())       # the trailing Caliper callbacks were never issued transactions
    assert ev.log_sha256 and len(ev.log_sha256) == 64


def test_12c_evidence_after_the_decision_time_is_not_counted(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    ev = evidence_from(rec)
    full = ev.issued_through(DAY_MS + 30_000)
    assert ev.issued_through(DAY_MS + 30_000, as_of_ms=DAY_MS + 3_000) < full and full == sum(rec.truth.values())


# ------------------------------------------------------------------------------------------------------------ 13 round-only limitations
@pytest.mark.parametrize("kind,why", [(sc.KIND_ROUND_CONFIGURED, "intent"), (sc.KIND_ROUND_OBSERVED, "interval resolution")])
def test_13_a_round_total_never_certifies_a_one_second_zero_even_when_it_agrees(tmp_path, kind, why):
    verdict = sc.classify_descriptor(k.descriptor(kind=kind))
    assert verdict.label == sc.CLASS_ROUND_ONLY and not verdict.certifies_intervals and why in verdict.detail
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=True).start()
    rec.rows[0]["source_counter"] = k.descriptor(kind=kind)
    rec.traffic(range(0, SHORT + 1), zero={5})
    for second in range(1, SHORT + 1):
        rec.beat(second)
    rec.stop(SHORT)                                                                    # the totals agree at the end of the round, and still nothing is certified
    snap = k.recorder_coverage(rec, tmp_path)[1].snapshot(at(SHORT + 1))
    assert snap.classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO and sc.RC_ROUND_ONLY in snap.cause_codes()


def test_13b_a_configured_round_count_is_not_an_issued_count_and_caliper_aggregates_are_unverifiable():
    assert sc.classify_descriptor(k.descriptor(kind=sc.KIND_ROUND_CONFIGURED)).reason_code == sc.RC_ROUND_ONLY
    verdict = sc.classify_descriptor(k.descriptor(kind=sc.KIND_CALIPER_AGGREGATE))
    assert verdict.label == sc.CLASS_UNVERIFIABLE and not verdict.certifies_intervals


def test_13c_the_feasibility_table_classifies_every_actual_candidate_and_names_its_evidence():
    table = {c["id"]: c for c in sc.SOURCE_CANDIDATES}
    assert table["trace_replay_dispatch_log"]["class"] == sc.CLASS_INTERVAL
    assert table["create_asset_in_memory_seq"]["class"] == sc.CLASS_NOT_AVAILABLE
    assert table["configured_round_total"]["class"] == sc.CLASS_ROUND_ONLY
    assert table["recorder_submit_callbacks"]["class"] == table["recorder_progress_counters"]["class"] == sc.CLASS_RECORDER_DEPENDENT
    assert table["caliper_transaction_statistics"]["class"] == table["tx_ids_at_completion"]["class"] == sc.CLASS_UNVERIFIABLE
    assert all(c["class"] in sc.FEASIBILITY_CLASSES and c["evidence"] and c["limits"] for c in table.values())


def test_13d_the_cited_evidence_files_and_lines_really_say_what_the_table_claims():
    repo = j.REPO
    replay = (repo / "fabric-testbed/caliper/workloads/e1_trace_replay.js").read_text(encoding="utf-8")
    create = (repo / "fabric-testbed/caliper/workloads/e1_create_asset.js").read_text(encoding="utf-8")
    render = (repo / "scripts/e1/e1_la2_render.py").read_text(encoding="utf-8")
    assert replay.index("appendFileSync(this.dispatchLog") < replay.index("this.sutAdapter.sendRequests(request)")          # logged BEFORE it is sent
    assert "totalWorkers !== 1" in replay and "totalWorkers !== 1" in create                                               # one worker only
    assert "dispatchLog" not in create and "appendFileSync" not in create                                                  # the pilot workload logs nothing
    assert "e1_create_asset.js" in render and "e1_trace_replay" not in render                                              # and the pilot renderer uses it
    assert '"workers": {"number": 1}' in render and '"type": "fixed-rate"' in render


# --------------------------------------------------------------------------------------------------------------------------- 14 retries
def test_14_an_identical_repeated_row_is_dropped_and_counted_but_a_retry_with_a_different_time_is_refused():
    base = k.dispatch_text({0: 3})
    row = base.splitlines()[1]
    ok = sc.DispatchLogEvidence.from_text(base + row + "\n", worker="0", clock_domain=k.DOMAIN, run_id="r", instance="i", max_callback_lag_ms=k.LAG_MS)
    assert ok.duplicates_dropped == 1 and len(ok.dispatch_ms) == 3
    retry = row.split(",")
    retry[3], retry[4] = str(int(retry[3]) + 40), str(int(retry[4]) + 40)             # the same seq dispatched again 40 ms later
    with pytest.raises(sc.SourceEvidenceError) as excinfo:
        sc.DispatchLogEvidence.from_text(base + ",".join(retry) + "\n", worker="0", clock_domain=k.DOMAIN, run_id="r", instance="i", max_callback_lag_ms=k.LAG_MS)
    assert excinfo.value.code == "dispatch_seq_conflict"


# ------------------------------------------------------------------------------------------------------------------ 15 submission failure
def test_15_transactions_dispatched_but_never_reported_submitted_uncover_the_span(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False, missed={9})           # the generator issued second 9's transactions; no callback fired
    ev = evidence_from(rec)                                                              # the generator's truth still holds them (rec.truth has second 9)
    snap = k.recorder_coverage(rec, tmp_path, source_evidence=ev)[1].snapshot(at(SHORT + 1))
    cls, why = snap.classify(at(9), 0)
    assert cls == C.INTERRUPTED_COLLECTION and "source_count_mismatch" in why and "missed callbacks" in why
    assert snap.classify(at(5), 0)[0] == C.VERIFIED_ZERO
    assert "missing_callbacks_detected" in snap.cause_codes()


def test_15b_a_torn_dispatch_log_certifies_nothing_after_its_last_complete_row(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    text = k.dispatch_text(rec.truth) + "99999,1,2"                                       # an interrupted final write: no newline
    ev = sc.DispatchLogEvidence.from_text(text, worker="0", clock_domain=k.DOMAIN, run_id=j.RUN, instance=rec.instance, max_callback_lag_ms=k.LAG_MS)
    assert ev.torn_tail is True
    snap = k.recorder_coverage(rec, tmp_path, source_evidence=ev)[1].snapshot(at(SHORT + 1))
    assert snap.classify(at(5), 0)[0] == C.VERIFIED_ZERO                                  # earlier seconds are still verified
    last = ev.last_complete_dispatch_ms()
    late_second = (last - DAY_MS) // 1000 + 1
    assert snap.classify(at(late_second), 0)[0] == C.INTERRUPTED_COLLECTION


def test_15c_when_no_boundary_is_settled_nothing_is_verified_and_no_mismatch_is_invented(tmp_path):
    rec = j.short_recorder(tmp_path, source_count=False)                                  # traffic in every second, no quiet second
    late = k.dispatch_text(rec.truth, offset_ms=700, step_ms=1)                           # every issue lands within the callback-lag allowance of a boundary
    ev = sc.DispatchLogEvidence.from_text(late, worker="0", clock_domain=k.DOMAIN, run_id=j.RUN, instance=rec.instance, max_callback_lag_ms=k.LAG_MS)
    snap = k.recorder_coverage(rec, tmp_path, source_evidence=ev)[1].snapshot(at(SHORT + 1))
    assert snap.valid and snap.extra_uncovered == () and snap.source_verified == ()      # no false 'missed callbacks', and no certification either
    assert snap.classify(at(3), 3)[0] == C.OBSERVED_POSITIVE


def test_15e_a_quiet_boundary_after_a_zero_second_settles_and_verifies_the_cumulative_span_behind_it(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    late = k.dispatch_text(rec.truth, offset_ms=700, step_ms=1)
    ev = sc.DispatchLogEvidence.from_text(late, worker="0", clock_domain=k.DOMAIN, run_id=j.RUN, instance=rec.instance, max_callback_lag_ms=k.LAG_MS)
    snap = k.recorder_coverage(rec, tmp_path, source_evidence=ev)[1].snapshot(at(SHORT + 1))
    assert snap.classify(at(5), 0)[0] == C.VERIFIED_ZERO                                  # the boundary at second 6 saw no dispatch in the last 500 ms
    assert snap.source_verified and snap.source_verified[0][1] == at(6)                   # only up to the settled boundary


def test_15d_with_no_lag_allowance_the_same_log_verifies_because_every_boundary_is_settled(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    late = k.dispatch_text(rec.truth, offset_ms=700, step_ms=1)
    ev = sc.DispatchLogEvidence.from_text(late, worker="0", clock_domain=k.DOMAIN, run_id=j.RUN, instance=rec.instance, max_callback_lag_ms=0)
    snap = k.recorder_coverage(rec, tmp_path, source_evidence=ev)[1].snapshot(at(SHORT + 1))
    assert snap.classify(at(5), 0)[0] == C.VERIFIED_ZERO                                  # the assumption max_callback_lag_ms drives the result, so it is declared


# ------------------------------------------------------------------------------------------------------------- 16 duplicate transaction id
def test_16_a_duplicate_seq_two_rows_for_one_transaction_is_never_double_counted():
    base = k.dispatch_text({0: 2})
    rows = base.splitlines()
    dup = rows + [rows[1]]
    ev = sc.DispatchLogEvidence.from_text("\n".join(dup) + "\n", worker="0", clock_domain=k.DOMAIN, run_id="r", instance="i", max_callback_lag_ms=k.LAG_MS)
    assert len(ev.dispatch_ms) == 2 and ev.duplicates_dropped == 1


def test_16b_a_gap_in_the_seq_values_means_a_row_is_missing_and_the_log_is_refused():
    rows = k.dispatch_text({0: 4}).splitlines()
    del rows[2]                                                                           # seq 1 is missing
    with pytest.raises(sc.SourceEvidenceError) as excinfo:
        sc.DispatchLogEvidence.from_text("\n".join(rows) + "\n", worker="0", clock_domain=k.DOMAIN, run_id="r", instance="i", max_callback_lag_ms=k.LAG_MS)
    assert excinfo.value.code == "dispatch_seq_gap"


def test_16c_duplicate_recorder_event_ids_are_still_dropped_by_the_recorder_side_reader(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    rec.write()
    text = rec.submits_path.read_text(encoding="utf-8")
    rec.submits_path.write_text(text + text.splitlines()[1] + "\n", encoding="utf-8", newline="")
    events, report = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    assert report["duplicates_dropped"] == 1 and len(events) == len(text.splitlines()) - 1


# ------------------------------------------------------------------------------------------------------------------ 17 source-count provenance
def test_17_the_snapshot_carries_the_classification_and_never_calls_independence_proven(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    snap = k.recorder_coverage(rec, tmp_path, source_evidence=evidence_from(rec))[1].snapshot(at(SHORT + 1))
    info = snap.summary()["source_evidence"]
    assert info["kind"] == "dispatch_log" and info["classifications"][rec.instance]["class"] == sc.CLASS_INTERVAL
    assert info["independence"].startswith("DECLARED") and "never proven" in info["independence"]
    assert info["classifications"][rec.instance]["independence"].startswith("DECLARED")
    evidence = snap.summary()["recorder_evidence"]
    assert evidence["live_source_coverage_verified"] is False and evidence["synthetic"] is True


def test_17b_the_provenance_never_describes_a_callback_time_as_an_issue_time():
    for entry in sc.TIMESTAMP_INVENTORY:
        if "callback" in entry["name"]:
            assert entry["semantics"] == sc.SEMANTICS_CALLBACK and "never the issue time" in entry["where"]
        if "completion" in entry["name"]:
            assert entry["semantics"] == sc.SEMANTICS_COMPLETION and "never converted" in entry["where"]
    assert {e["status"] for e in sc.TIMESTAMP_INVENTORY} <= {"AVAILABLE", "INFERRED", "NOT AVAILABLE"}
    issue = [e for e in sc.TIMESTAMP_INVENTORY if e["semantics"] == sc.SEMANTICS_ISSUE]
    assert len(issue) == 1 and "e1_create_asset.js records none" in issue[0]["where"]


@pytest.mark.parametrize("semantics", [sc.SEMANTICS_CALLBACK, sc.SEMANTICS_COMPLETION, sc.SEMANTICS_UNKNOWN])
def test_17c_a_non_issue_timestamp_cannot_back_an_issue_time_claim(semantics):
    with pytest.raises(sc.SourceEvidenceError) as excinfo:
        sc.require_issue_time(semantics, claim="a zero-arrival second")
    assert excinfo.value.code == "timestamp_semantics_conflated"
    sc.require_issue_time(sc.SEMANTICS_ISSUE, claim="a zero-arrival second")


# ------------------------------------------------------------------------------------------------------------ 18 recorder-derived rejection
@pytest.mark.parametrize("derived_from", ["live.submits.csv", "recorder_submits_file", "txSubmitted callback", "observer stream"])
def test_18_a_dispatch_evidence_object_cannot_be_built_from_a_recorder_derived_source(derived_from):
    bad = k.descriptor(kind=sc.KIND_ISSUE_LOG, derived_from=derived_from)
    with pytest.raises(sc.SourceEvidenceError) as excinfo:
        sc.DispatchLogEvidence.from_text(k.dispatch_text({0: 2}), worker="0", clock_domain=k.DOMAIN, run_id="r", instance="i", max_callback_lag_ms=k.LAG_MS,
                                         descriptor=bad)
    assert excinfo.value.code == "source_descriptor_not_interval_suitable"


def test_18b_only_the_trace_replay_derivation_is_accepted_for_the_dispatch_adapter():
    odd = k.descriptor(kind=sc.KIND_ISSUE_LOG, derived_from="some_other_generator_log")
    with pytest.raises(sc.SourceEvidenceError) as excinfo:
        sc.DispatchLogEvidence.from_text(k.dispatch_text({0: 2}), worker="0", clock_domain=k.DOMAIN, run_id="r", instance="i", max_callback_lag_ms=k.LAG_MS,
                                         descriptor=odd)
    assert excinfo.value.code == "source_descriptor_not_interval_suitable"


# ----------------------------------------------------------------------------------------------------------- 19 missing source-counter fields
@pytest.mark.parametrize("missing", ["counter_id", "derived_from", "independent_of_recorder", "timestamp_semantics", "resolution"])
def test_19_each_descriptor_field_is_required_for_an_interval_claim(missing):
    verdict = sc.classify_descriptor(k.descriptor(**{missing: None}))
    assert not verdict.certifies_intervals and verdict.reason_code in (sc.RC_INDEPENDENCE_UNPROVEN, sc.RC_TIMESTAMP_SEMANTICS, sc.RC_ROUND_ONLY)


def test_19b_no_descriptor_at_all_is_not_available_and_a_non_object_is_invalid():
    assert sc.classify_descriptor(None).label == sc.CLASS_NOT_AVAILABLE and sc.classify_descriptor(None).reason_code == sc.RC_DESCRIPTOR_MISSING
    assert sc.classify_descriptor("generator").reason_code == sc.RC_DESCRIPTOR_INVALID


@pytest.mark.parametrize("text,code", [
    ("seq,dispatch_ms\n0,1\n", "dispatch_log_schema_mismatch"),
    (",".join(sc.DISPATCH_HEADER) + "\n0,0,1,2\n", "dispatch_row_malformed"),
    (",".join(sc.DISPATCH_HEADER) + "\n0,0,1000,1000,5\n", "dispatch_row_inconsistent"),
    (",".join(sc.DISPATCH_HEADER) + "\n-1,0,1000,1000,0\n", "dispatch_row_invalid"),
    (",".join(sc.DISPATCH_HEADER) + "\n0,0,2000,2000,0\n1,0,1000,1000,0\n", "dispatch_clock_regression"),
    ("", "dispatch_log_schema_mismatch"),
])
def test_19c_a_malformed_dispatch_log_is_refused_with_a_machine_readable_code(text, code):
    with pytest.raises(sc.SourceEvidenceError) as excinfo:
        sc.DispatchLogEvidence.from_text(text, worker="0", clock_domain=k.DOMAIN, run_id="r", instance="i", max_callback_lag_ms=k.LAG_MS)
    assert excinfo.value.code == code


@pytest.mark.parametrize("field", ["worker", "clock_domain", "run_id", "instance"])
def test_19d_identity_fields_are_required(field):
    kwargs = {"worker": "0", "clock_domain": k.DOMAIN, "run_id": "r", "instance": "i", "max_callback_lag_ms": k.LAG_MS, field: " "}
    with pytest.raises(sc.SourceEvidenceError) as excinfo:
        sc.DispatchLogEvidence.from_text(k.dispatch_text({0: 2}), **kwargs)
    assert excinfo.value.code == "source_evidence_identity_missing"


@pytest.mark.parametrize("lag", [-1, 1.5, True, None, "500"])
def test_19e_the_callback_lag_allowance_must_be_stated_as_a_whole_non_negative_number(lag):
    with pytest.raises(sc.SourceEvidenceError) as excinfo:
        sc.DispatchLogEvidence.from_text(k.dispatch_text({0: 2}), worker="0", clock_domain=k.DOMAIN, run_id="r", instance="i", max_callback_lag_ms=lag)
    assert excinfo.value.code == "source_evidence_policy_invalid"


def test_19f_dispatch_evidence_for_an_instance_the_journal_never_started_is_invalid(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    ev = evidence_from(rec, instance="rec:run-a0j:w9:r9")
    snap = k.recorder_coverage(rec, tmp_path, source_evidence=ev)[1].snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == "source_evidence_instance_unknown"


# ------------------------------------------------------------------------------------------------------ timestamp provenance (workstream E)
def test_20_epoch_milliseconds_become_timezone_aware_utc_and_malformed_values_are_refused():
    moment = sc.epoch_ms_to_utc(DAY_MS + 1234, "t")
    assert moment.tzinfo is not None and moment.utcoffset().total_seconds() == 0 and moment.microsecond == 234_000
    for bad in (True, 1.5, 0, -1, "1700000000000", None):
        with pytest.raises(sc.SourceEvidenceError) as excinfo:
            sc.epoch_ms_to_utc(bad, "t")
        assert excinfo.value.code == "timestamp_malformed"


def test_20b_a_wall_clock_step_inside_the_generator_log_is_detected():
    rows = k.dispatch_text({0: 1, 1: 1}).splitlines()
    first, second = rows[1].split(","), rows[2].split(",")
    second[3], second[2], second[4] = str(int(first[3]) - 1), str(int(first[3]) - 1), "0"
    with pytest.raises(sc.SourceEvidenceError) as excinfo:
        sc.DispatchLogEvidence.from_text("\n".join([rows[0], rows[1], ",".join(second)]) + "\n", worker="0", clock_domain=k.DOMAIN, run_id="r", instance="i",
                                         max_callback_lag_ms=k.LAG_MS)
    assert excinfo.value.code == "dispatch_clock_regression"


def test_20c_the_dispatch_log_keeps_its_raw_hash_and_clock_domain_for_audit():
    ev = sc.DispatchLogEvidence.from_text(k.dispatch_text({0: 2}), worker="0", clock_domain=k.DOMAIN, run_id="r", instance="i", max_callback_lag_ms=k.LAG_MS)
    prov = ev.timestamp_provenance()
    assert prov["clock_domain"] == k.DOMAIN and prov["log_sha256"] == ev.log_sha256 and "Date.now" in prov["raw_unit"] and "declared, not measured" in prov["assumption"]


def test_20d_the_collector_clock_step_detector_of_the_bridge_still_applies_to_the_health_spool(tmp_path):
    rec = j.SyntheticRecorder(tmp_path, declared_interval_ms=1000, source_count=True, mono_skew_ms=0.0).start()
    rec.traffic(range(0, 12))
    for second in range(1, 6):
        rec.beat(second)
    stepped = rec.beat(6)
    rec.rows[-1]["mono_ms"] = rec.rows[-1]["mono_ms"] + 40_000                           # the wall delta and the monotonic delta now disagree
    rec.write()
    report = j.bridge.ingest_spool(rec.spool_path, tmp_path / "journal.jsonl", source_id=rec.source_id, policy=FAST)
    assert any(p["code"] == "clock_discontinuity" for p in report["problems"])
    assert stepped is rec
