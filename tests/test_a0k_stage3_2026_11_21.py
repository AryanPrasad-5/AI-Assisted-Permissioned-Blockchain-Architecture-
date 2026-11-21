"""A0k workstream F (checklist items 30-37): the repaired coverage contract carried through
workload source -> source-count contract -> recorder-health evidence -> coverage validation -> captured history -> Stage 3 predictive path.

Every run is the A0f/A0h offline fixture pipeline fed by a SYNTHETIC recorder day. Forecast and performance values are scripted fixtures. Nothing is
loaded, forecast or sent anywhere; none of it is a Caliper or Fabric measurement, and no controller is judged.
"""

from __future__ import annotations

import json
import pickle
import socket
import subprocess

import pytest

from tests import a0j_support as j
from tests import a0k_support as k
from tests.a0k_support import C, DOMAIN, FAST, at, bridge, ch, rc, sc, wc, wk
from tests.a0j_support import by_controller

from fabric_perf.controllers import abstention_accounting as abstention  # noqa: E402
from fabric_perf.controllers import live_foundation as lf  # noqa: E402

F = j.s.h.f


@pytest.fixture(autouse=True)
def nothing_loaded_nothing_sent(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a model load, process or network use is forbidden")
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(pickle, "load", refuse)
    monkeypatch.setattr(pickle, "loads", refuse)
    monkeypatch.setattr(lf.StreamingEWMA, "__init__", refuse)


def day(tmp_path, mutate=None, *, zero=(100, 5000), name="w0", **kwargs):
    """A day-long scripted recorder (hourly heartbeats, then per second). ``mutate(rec)`` may edit the start row before traffic is scripted."""
    rec = j.SyntheticRecorder(tmp_path, name=name, **kwargs).start()
    if mutate is not None:
        mutate(rec)
    rec.traffic(range(0, 43_205), zero=zero)
    rec.covered(43_204)
    return rec


def run_recorder(tmp_path, rec, *, steps=None, name="cap"):
    report, coverage, history = j.coverage_and_history(rec)
    manifest, rows, out = j.s.h.run_captured(tmp_path, history, steps=steps, name=name)
    return report, history, manifest, rows, out


def run_generic_wrapper(tmp_path, rec, *, steps=None, name="gen"):
    """THE ORIGINAL BYPASS shape: the plain ``CapturedHistory`` over the generic ``VerifiedCoverage``, no recorder subclass."""
    j.ingest(rec)
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    generic = wc.VerifiedCoverage.from_journal_file(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=j.POLICY.to_coverage_policy(), run_id=j.RUN)
    history = ch.CapturedHistory(rc.bin_submit_events(events, j.recorder_source()), coverage=generic, max_age_s=j.POLICY.freshness_limit_s)
    manifest, rows, out = j.s.h.run_captured(tmp_path, history, steps=steps, name=name)
    return history, manifest, rows, out


def codes(row) -> list:
    return row["predictive_evidence"]["workload_observation"]["coverage"].get("abstention_reason_codes")


# ------------------------------------------------------------------------------------------------------------------------- 30 valid shared frame
def test_30_valid_shared_telemetry_is_not_rejected_and_the_predictive_controller_decides_when_source_evidence_is_declared_and_classified(tmp_path):
    rec = day(tmp_path, source_count=True)
    report, history, manifest, rows, out = run_recorder(tmp_path, rec)
    assert manifest["status"] == "COMPLETED" and j.s.h.runner.verify_run_directory(out) == []
    groups = by_controller(rows)
    assert [r["outcome"] for r in groups["predictive"]][1:] == ["propose"] * 3
    block = groups["predictive"][1]["predictive_evidence"]["workload_observation"]["coverage"]
    assert block["class_counts"]["verified_zero"] == 2 and block["coverage_scope"] == "recorder_callback" and block["abstention_reason_codes"] == []
    assert block["source_evidence"]["classifications"][rec.instance]["class"] == sc.CLASS_INTERVAL
    assert block["live_quality"] is False and block["recorder_evidence"]["live_source_coverage_verified"] is False
    assert {r["frame"]["status"] for r in rows if r["frame"]["index"] > 0} == {"accepted"}


# ----------------------------------------------------------------------------------------------------------- 31 predictive-only abstention
@pytest.mark.parametrize("variant", ["heartbeat_only", "recorder_derived", "round_total", "unproven_independence", "no_descriptor"])
def test_31_a_coverage_failure_that_only_concerns_workload_history_abstains_only_the_predictive_controller(tmp_path, variant):
    def mutate(rec):
        if variant == "recorder_derived":
            rec.rows[0]["source_counter"] = k.descriptor(kind=sc.KIND_RECORDER_DERIVED, derived_from="recorder_submits_file")
        elif variant == "round_total":
            rec.rows[0]["source_counter"] = k.descriptor(kind=sc.KIND_ROUND_CONFIGURED)
        elif variant == "unproven_independence":
            rec.rows[0]["source_counter"] = k.descriptor(independent_of_recorder=None)
        elif variant == "no_descriptor":
            rec.rows[0].pop("source_counter")
    rec = day(tmp_path, mutate, zero={100}, source_count=variant != "heartbeat_only")
    _report, history, manifest, rows, out = run_recorder(tmp_path, rec)
    groups = by_controller(rows)
    for row in groups["predictive"][1:]:
        assert row["outcome"] == "abstain" and row["abstention_scope"] == "controller" and row["frame"]["status"] == "accepted"
        assert row["reason"] == "workload_prefix_incomplete_fail_safe" and row["forecast"] is None
    for name in ("static", "reactive"):
        assert [r["outcome"] for r in groups[name]][1:] == ["hold"] * 3                       # the shared frame is valid, so they still decide
    assert {f["validation_outcome"] for f in manifest["captured_history"]["frames"]} == {ch.VALID_INCOMPLETE}
    assert abstention.classify_row(groups["predictive"][1])[0] == abstention.PREDICTIVE_HISTORY_ABSTENTION


def test_31b_the_original_bypass_shape_abstains_the_predictive_controller_and_nobody_else(tmp_path):
    rec = j.new_recorder(tmp_path, zero={100}, source_count=False)
    history, manifest, rows, out = run_generic_wrapper(tmp_path, rec)
    groups = by_controller(rows)
    assert [r["outcome"] for r in groups["predictive"]][1:] == ["abstain"] * 3
    assert {r["outcome"] for name in ("static", "reactive") for r in groups[name][1:]} == {"hold"}
    assert history.coverage_counts[C.VERIFIED_ZERO.value] == 0                                # the zero never entered the history
    assert history.evidence(at(43_204))["validation_outcome"] == ch.VALID_INCOMPLETE


# ---------------------------------------------------------------------------------------------------------- 32 shared invalid-frame abstention
def test_32_an_invalid_shared_frame_gives_all_three_controllers_the_same_disposition_whatever_the_coverage_says(tmp_path):
    steps = F.steady(3)
    steps[2] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5, config_reader_ok=False)
    steps[3] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5)
    rec = day(tmp_path, zero={100}, source_count=False)                                        # the workload history is ALSO incomplete here
    _r, _h, manifest, rows, _o = run_recorder(tmp_path, rec, steps=steps)
    frames: dict = {}
    for row in rows:
        frames.setdefault(row["frame"]["index"], []).append(row)
    bad = [i for i, group in frames.items() if {r["frame"]["status"] for r in group} == {"rejected"}]
    assert bad
    for index in bad:
        group = frames[index]
        assert {r["outcome"] for r in group} == {"abstain"} and {r["abstention_scope"] for r in group} == {"frame"}
        assert {abstention.classify_row(r)[0] for r in group} == {abstention.SHARED_INVALID_FRAME}      # the frame, not the coverage, explains it
    assert manifest["status"] == "COMPLETED"


# ------------------------------------------------------------------------------------------------------------------------- 33 coverage reasons
@pytest.mark.parametrize("variant,expected", [
    ("heartbeat_only", rc.RC_NO_SOURCE_EVIDENCE),
    ("recorder_derived", sc.RC_RECORDER_DERIVED),
    ("round_total", sc.RC_ROUND_ONLY),
    ("unproven_independence", sc.RC_INDEPENDENCE_UNPROVEN),
    ("no_descriptor", sc.RC_DESCRIPTOR_MISSING),
])
def test_33_every_predictive_abstention_records_the_specific_coverage_reason(tmp_path, variant, expected):
    def mutate(rec):
        if variant == "recorder_derived":
            rec.rows[0]["source_counter"] = k.descriptor(kind=sc.KIND_RECORDER_DERIVED, derived_from="recorder_submits_file")
        elif variant == "round_total":
            rec.rows[0]["source_counter"] = k.descriptor(kind=sc.KIND_ROUND_CONFIGURED)
        elif variant == "unproven_independence":
            rec.rows[0]["source_counter"] = k.descriptor(independent_of_recorder=None)
        elif variant == "no_descriptor":
            rec.rows[0].pop("source_counter")
    rec = day(tmp_path, mutate, zero={100}, source_count=variant != "heartbeat_only")
    _r, _h, _m, rows, _o = run_recorder(tmp_path, rec)
    for row in by_controller(rows)["predictive"][1:]:
        assert expected in codes(row), (variant, codes(row))


def test_33b_a_missing_worker_is_named_in_the_decision_record_and_only_the_predictive_controller_abstains(tmp_path):
    w0 = day(tmp_path, zero={100}, name="w0", instance="rec:run-a0j:w0:r0", worker=0)
    w0.write()
    bridge.ingest_spool(w0.spool_path, tmp_path / "jw0.jsonl", source_id=j.SOURCE_ID, policy=j.POLICY)
    events, _ = rc.read_submit_events(w0.submits_path, instance=w0.instance)
    cov0 = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "jw0.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=j.POLICY, run_id=j.RUN)
    multi = wk.MultiWorkerCoverage(source_id=j.SOURCE_ID, roster=wk.WorkerRoster.from_benchmark_config(2, DOMAIN), workers=[(0, cov0)])     # worker 1 never reported
    history = rc.RecorderCapturedHistory(rc.bin_submit_events(events, j.recorder_source()), coverage=multi, policy=j.POLICY)
    manifest, rows, out = j.s.h.run_captured(tmp_path, history, name="miss")
    groups = by_controller(rows)
    for row in groups["predictive"][1:]:
        assert row["outcome"] == "abstain" and wk.RC_WORKER_MISSING in codes(row)
    assert {r["outcome"] for name in ("static", "reactive") for r in groups[name][1:]} == {"hold"}
    assert j.s.h.runner.verify_run_directory(out) == []


def test_33c_a_complete_two_worker_day_reaches_the_predictive_controller(tmp_path):
    recs = {0: day(tmp_path, zero={100}, name="w0", instance="rec:run-a0j:w0:r0", worker=0), 1: day(tmp_path, zero={100}, name="w1", instance="rec:run-a0j:w1:r0", worker=1)}
    covs, event_lists = [], {}
    for index, rec in recs.items():
        rec.write()
        bridge.ingest_spool(rec.spool_path, tmp_path / f"jw{index}.jsonl", source_id=j.SOURCE_ID, policy=j.POLICY)
        event_lists[index] = rc.read_submit_events(rec.submits_path, instance=rec.instance)[0]
        covs.append((index, rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / f"jw{index}.jsonl", source_id=j.SOURCE_ID, submit_events=event_lists[index],
                                                                              policy=j.POLICY, run_id=j.RUN)))
    merged, _report = wk.merge_worker_submit_events(event_lists)
    multi = wk.MultiWorkerCoverage(source_id=j.SOURCE_ID, roster=wk.WorkerRoster.from_benchmark_config(2, DOMAIN), workers=covs)
    history = rc.RecorderCapturedHistory(rc.bin_submit_events(merged, j.recorder_source()), coverage=multi, policy=j.POLICY)
    manifest, rows, out = j.s.h.run_captured(tmp_path, history, name="two")
    assert [r["outcome"] for r in by_controller(rows)["predictive"]][1:] == ["propose"] * 3
    block = by_controller(rows)["predictive"][1]["predictive_evidence"]["workload_observation"]["coverage"]
    assert block["worker_coverage"]["workers"].keys() == {"0", "1"} and block["abstention_reason_codes"] == [] and block["live_quality"] is False


# ----------------------------------------------------------------------------------------------------------- 34 consistent frame identity
@pytest.mark.parametrize("route", ["recorder_ok", "recorder_heartbeat_only", "generic_wrapper_heartbeat_only"])
def test_34_all_three_controllers_see_the_same_frame_whatever_the_coverage_route_says(tmp_path, route):
    if route == "recorder_ok":
        _r, _h, manifest, rows, _o = run_recorder(tmp_path, day(tmp_path, zero={100}, source_count=True))
    elif route == "recorder_heartbeat_only":
        _r, _h, manifest, rows, _o = run_recorder(tmp_path, day(tmp_path, zero={100}, source_count=False))
    else:
        _h, manifest, rows, _o = run_generic_wrapper(tmp_path, j.new_recorder(tmp_path, zero={100}, source_count=False))
    assert j.s.h.runner.check_comparability(rows) == {"frames": 4, "comparable": True, "problems": []}
    for index in range(1, 4):
        group = [r for r in rows if r["frame"]["index"] == index]
        assert len(group) == 3 and len({wc.canonical(r["frame"]) for r in group}) == 1


# ------------------------------------------------------------------------------------------------------------- 35 denominator accounting
def test_35_every_scheduled_opportunity_is_counted_and_the_categories_sum_to_the_denominator(tmp_path):
    _r, _h, _m, rows, _o = run_recorder(tmp_path, day(tmp_path, zero={100}, source_count=False))
    result = abstention.account(rows, scheduled_frames=4, segment="a0k-synthetic")
    assert result["consistent"] is True
    predictive, static = result["controllers"]["predictive"], result["controllers"]["static"]
    counts = predictive["category_counts"]
    assert predictive["total_scheduled_decision_opportunities"] == 4 and sum(counts.values()) == 4
    assert counts[abstention.PREDICTIVE_HISTORY_ABSTENTION] == 3 and counts[abstention.INITIALIZATION] == 1 and counts[abstention.VALID_RECOMMENDATION] == 0
    assert static["category_counts"][abstention.VALID_RECOMMENDATION] == 3 and predictive["decision_coverage_rate"]["rate"] == 0.0


def test_35b_the_reason_codes_in_the_records_never_change_the_accounting_categories(tmp_path):
    def mutate(rec):
        rec.rows[0]["source_counter"] = k.descriptor(kind=sc.KIND_RECORDER_DERIVED, derived_from="recorder_submits_file")
    _r, _h, _m, rows, _o = run_recorder(tmp_path, day(tmp_path, mutate, zero={100}))
    result = abstention.account(rows, scheduled_frames=4, segment="a0k-synthetic")
    counts = result["controllers"]["predictive"]["category_counts"]
    assert result["consistent"] is True and counts[abstention.PREDICTIVE_HISTORY_ABSTENTION] == 3 and sum(counts.values()) == 4


# ---------------------------------------------------------------------------------------------------------------------- 36 no actuation
def test_36_no_actuator_is_invoked_on_any_route(tmp_path):
    for label, route in (("ok", "recorder"), ("bypass", "generic")):
        sub = tmp_path / f"r_{label}"
        sub.mkdir()
        if route == "recorder":
            _r, _h, manifest, rows, out = run_recorder(sub, day(sub, zero={100}))
        else:
            _h, manifest, rows, out = run_generic_wrapper(sub, j.new_recorder(sub, zero={100}, source_count=False))
        assert manifest["actuation"]["configuration_changes_applied"] == 0 and manifest["actuation"]["live_actuator_available"] is False
        assert all(r["actuator"]["submitted_to_fabric"] is False and r["actuator"]["live_actuator_available"] is False for r in rows)


# --------------------------------------------------------------------------------------------------------- 37 real inference stays disabled
def test_37_real_inference_stays_disabled_and_no_model_or_ewma_is_loaded(tmp_path):
    _r, _h, manifest, rows, out = run_recorder(tmp_path, day(tmp_path, zero={100}))
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert manifest["models"]["frozen_hfbtp_model_loaded"] is False and manifest["models"]["frozen_ewma_loaded"] is False
    assert summary["real_inference_performed"] is False and j.s.h.runner.sp.REAL_INFERENCE_AUTHORIZED is False


def test_37b_historical_fixture_forecasting_stays_labelled(tmp_path):
    _r, _h, manifest, rows, _o = run_recorder(tmp_path, day(tmp_path, zero={100}))
    text = json.dumps(manifest, sort_keys=True)
    assert "fixture" in text.lower() and manifest["captured_history"]["source"]["synthetic"] is True
