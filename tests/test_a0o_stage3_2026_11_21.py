"""A0o workstream B (checklist items 38-44): SourceCount reader output carried through the existing offline Stage 3 pipeline.

SourceCountReader (round-isolated) -> coverage validation (``GeneratorIssueCoverage``) -> captured workload history -> Stage 3 history guard -> predictive recommendation or
abstention. Forecast and performance values are scripted fixtures; nothing is loaded, forecast or sent anywhere; none of it is a Caliper or Fabric measurement."""

from __future__ import annotations

import json
import pickle
import socket
import subprocess

import pytest

from tests import a0j_support as j
from tests.a0j_support import by_controller
from tests.a0o_support import DAY_MS, NL, a, at, day_with_reader, gc, history_of, ilr, sr, wc

from fabric_perf.controllers import abstention_accounting as abstention  # noqa: E402
from fabric_perf.controllers import live_foundation as lf  # noqa: E402

F = j.s.h.f
HISTORY_REASONS = {"workload_prefix_incomplete_fail_safe", "prediction_failure_fail_safe", "arrival_history_missing"}


@pytest.fixture(autouse=True)
def nothing_loaded_nothing_sent(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a model load, process or network use is forbidden")
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(pickle, "load", refuse)
    monkeypatch.setattr(pickle, "loads", refuse)
    monkeypatch.setattr(lf.StreamingEWMA, "__init__", refuse)


def run(tmp_path, *, name="a0o", steps=None, tamper=False, **kw):
    def corrupt(tick_at, log):
        if tamper and tick_at == 18_000:                                                    # an already-read record is edited; the next poll (6 h) notices
            data = log.path.read_bytes().split(NL)
            row = json.loads(data[-2])
            row["t_ms"] += 7
            data[-2] = wc.canonical(row).encode("ascii")
            log.path.write_bytes(NL.join(data))
    gen, reader = day_with_reader(tmp_path, name=name, after_poll=corrupt, **kw)
    history = history_of(reader)
    manifest, rows, out = j.s.h.run_captured(tmp_path, history, steps=steps, name=name)
    return reader, history, manifest, rows, out


def block(rows):
    return by_controller(rows)["predictive"][1]["predictive_evidence"]["workload_observation"]["coverage"]


def codes(row):
    return row["predictive_evidence"]["workload_observation"]["coverage"].get("abstention_reason_codes")


# ----------------------------------------------------------------------------------------------------------------------------- 38-39 ingestion and validation
def test_38_valid_reader_evidence_is_consumed_by_the_existing_history_adapter(tmp_path):
    reader, history, manifest, rows, out = run(tmp_path)
    assert manifest["status"] == "COMPLETED" and j.s.h.runner.verify_run_directory(out) == []
    groups = by_controller(rows)
    assert [r["outcome"] for r in groups["predictive"]][1:] == ["propose"] * 3
    assert {r["frame"]["status"] for r in rows if r["frame"]["index"] > 0} == {"accepted"}
    source = manifest["captured_history"]["source"]
    assert source["kind"] == gc.SOURCE_KIND_GENERATOR_ISSUE and source["event_time"] == "generator_issue_time" and source["synthetic"] is True
    assert "NOT Ethereum mempool arrivals" in source["represents"]
    assert reader.evidence()["status"] == sr.ST_VERIFIED


def test_39_coverage_validation_carries_the_round_identity_scope_and_limits(tmp_path):
    reader, history, manifest, rows, out = run(tmp_path)
    b = block(rows)
    assert b["class_counts"]["verified_zero"] > 0 and b["class_counts"]["observed_positive"] > 0 and b["abstention_reason_codes"] == []
    assert b["coverage_scope"] == "source_stream_observer" and b["coverage_subject"] == "generator_issuance" and b["external_mempool_coverage"] is False
    assert b["live_quality"] is False and b["live_source_coverage_verified"] is False and b["synthetic_evidence"] is True
    assert b["issue_log"]["run_id"] == a.RUN and b["issue_log"]["worker_id"] == "w0" and b["issue_log"]["worker_roster"] == ["w0"]
    instances = {ev["collector_instance"] for ev in history.coverage.events if ev.get("collector_instance")}
    assert instances == {reader.bridge.instance} and reader.bridge.instance.endswith(":w0:r1")        # the round is in the journal, not the base bridge's literal r0
    assert "EXPERIMENTAL GENERATOR" in b["claim"] and "nothing about an external Ethereum mempool" in b["claim"]


def test_39b_arrivals_are_binned_by_generator_event_time_not_by_the_readers_observation_time(tmp_path):
    reader, history, *_ = run(tmp_path, name="late", latency=3.0)                             # every tick is READ three seconds after it was written
    counts = history.binned.counts
    assert counts[10] == 1 and counts[11] == 0 and counts[13] == 0 and sum(counts) == len(reader.tail.attempt_events("x"))
    issued = reader.tail.attempt_events("x")[1]
    assert issued["ts_ms"] - DAY_MS == 10_100 and issued["read_ms"] - DAY_MS >= 13_000            # read time is later and is kept as a separate field


def test_39c_generator_and_recorder_counts_are_reported_side_by_side_and_a_mismatch_is_visible(tmp_path):
    reader, *_ = run(tmp_path, name="recon")
    matched = ilr.reconcile_counts(reader.tail, recorder_submit_units=reader.tail.counts()["attempted"])
    short = ilr.reconcile_counts(reader.tail, recorder_submit_units=reader.tail.counts()["attempted"] - 2)
    over = ilr.reconcile_counts(reader.tail, recorder_submit_units=reader.tail.counts()["attempted"] + 3)
    assert matched["recorder_submit_callbacks"]["minus_generator_attempts"] == 0
    assert short["recorder_submit_callbacks"]["minus_generator_attempts"] == -2 and over["recorder_submit_callbacks"]["minus_generator_attempts"] == 3
    assert "counts only" in over["recorder_submit_callbacks"]["join"]                         # a timestamp is never used to match records


# ----------------------------------------------------------------------------------------------------------------------------- 40 predictive-only abstention
@pytest.mark.parametrize("variant", ["tick_gap", "never_ticked"])
def test_40_missing_source_evidence_stays_unknown_and_abstains_only_the_predictive_controller(tmp_path, variant):
    hourly = {3600 * k for k in range(1, 13)}
    kwargs = {"tick_gap": {"ticks": hourly - {7200, 10_800}}, "never_ticked": {"ticks": set(), "late_ticks": False}}[variant]
    reader, history, manifest, rows, out = run(tmp_path, **kwargs)
    groups = by_controller(rows)
    for row in groups["predictive"][1:]:
        assert row["outcome"] == "abstain" and row["abstention_scope"] == "controller" and row["frame"]["status"] == "accepted"
        assert row["reason"] in HISTORY_REASONS and row["forecast"] is None and row["predicted_performance"] is None
        assert abstention.classify_row(row)[0] != abstention.VALID_RECOMMENDATION
    for name in ("static", "reactive"):
        assert [r["outcome"] for r in groups[name]][1:] == ["hold"] * 3                      # the shared frame is valid, so they still decide
    assert reader.evidence()["status"] != sr.ST_VERIFIED and reader.evidence()["failure_states"]


def test_40b_a_log_edited_after_it_was_read_invalidates_the_history_and_abstains_only_the_predictive_controller(tmp_path):
    reader, history, manifest, rows, out = run(tmp_path, tamper=True)
    assert reader.tail.error is not None and reader.tail.error.code == "log_replaced"
    assert reader.evidence()["status"] == sr.ST_INVALID and reader.evidence()["counts"]["attempts"] is None
    groups = by_controller(rows)
    assert {r["outcome"] for r in groups["predictive"][1:]} == {"abstain"} and {r["outcome"] for n in ("static", "reactive") for r in groups[n][1:]} == {"hold"}
    assert gc.RC_INTEGRITY in codes(groups["predictive"][1])


# ----------------------------------------------------------------------------------------------------------------------------- source evidence after the decision
def test_41_a_failure_found_after_a_decision_does_not_reach_back_into_it(tmp_path):
    gen, reader = day_with_reader(tmp_path, name="after")
    data = gen.log.path.read_bytes().split(NL)
    row = json.loads(data[-2])
    row["t_ms"] += 7
    data[-2] = wc.canonical(row).encode("ascii")
    gen.log.path.write_bytes(NL.join(data))
    reader.poll(at(43_210))                                                                  # noticed at 43,210, after the decisions at 43,201..43,204
    history = history_of(reader)
    manifest, rows, out = j.s.h.run_captured(tmp_path, history, name="after")
    assert reader.tail.error is not None and [r["outcome"] for r in by_controller(rows)["predictive"]][1:] == ["propose"] * 3
    assert not history.coverage.snapshot(at(43_211)).valid and history.coverage.snapshot(at(43_204)).valid


# ----------------------------------------------------------------------------------------------------------------------------- 42 shared-frame failure
def test_42_an_invalid_shared_frame_abstains_all_three_controllers_with_one_disposition(tmp_path):
    steps = F.steady(3)
    steps[2] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5, config_reader_ok=False)
    steps[3] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5)
    reader, history, manifest, rows, out = run(tmp_path, steps=steps, name="bad")
    frames: dict = {}
    for row in rows:
        frames.setdefault(row["frame"]["index"], []).append(row)
    bad = [i for i, group in frames.items() if {r["frame"]["status"] for r in group} == {"rejected"}]
    assert bad
    for index in bad:
        group = frames[index]
        assert {r["outcome"] for r in group} == {"abstain"} and {r["abstention_scope"] for r in group} == {"frame"}
        assert {abstention.classify_row(r)[0] for r in group} == {abstention.SHARED_INVALID_FRAME}
    assert manifest["status"] == "COMPLETED"


# ----------------------------------------------------------------------------------------------------------------------------- 43 frame identity and 44 no actuation
@pytest.mark.parametrize("route", ["valid", "tick_gap"])
def test_43_all_three_controllers_share_one_frame_identity_on_every_reader_route(tmp_path, route):
    kwargs = {} if route == "valid" else {"ticks": {3600 * k for k in range(1, 13)} - {7200, 10_800}}
    *_r, manifest, rows, _o = run(tmp_path, name=route, **kwargs)
    assert j.s.h.runner.check_comparability(rows) == {"frames": 4, "comparable": True, "problems": []}
    for index in range(1, 4):
        group = [r for r in rows if r["frame"]["index"] == index]
        assert len(group) == 3 and len({wc.canonical(r["frame"]) for r in group}) == 1


@pytest.mark.parametrize("route", ["valid", "tick_gap"])
def test_44_no_actuator_is_invoked_and_real_inference_stays_disabled(tmp_path, route):
    kwargs = {} if route == "valid" else {"ticks": {3600}}
    reader, history, manifest, rows, out = run(tmp_path, name=route, **kwargs)
    assert manifest["actuation"]["configuration_changes_applied"] == 0 and manifest["actuation"]["live_actuator_available"] is False
    assert all(r["actuator"]["submitted_to_fabric"] is False and r["actuator"]["live_actuator_available"] is False for r in rows)
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert manifest["models"]["frozen_hfbtp_model_loaded"] is False and manifest["models"]["frozen_ewma_loaded"] is False
    assert summary["real_inference_performed"] is False and j.s.h.runner.sp.REAL_INFERENCE_AUTHORIZED is False
    assert "fixture" in json.dumps(manifest, sort_keys=True).lower()
