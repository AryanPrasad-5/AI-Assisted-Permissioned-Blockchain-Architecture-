"""A0i Stage 3 integration (checklist items 16-25): coverage flows through history adapter -> history guard -> predictive controller -> records.

Every run is the A0f/A0h offline fixture pipeline with a SYNTHETIC captured day. Forecast and performance values are scripted fixtures;
nothing is loaded, forecast or sent anywhere.
"""

from __future__ import annotations

import json
import pickle
import socket
import subprocess
from datetime import datetime, timedelta

import pytest

from tests import a0i_support as s
from tests.a0i_support import at, ch, wc

from fabric_perf.controllers import abstention_accounting as abstention  # noqa: E402
from fabric_perf.controllers import live_foundation as lf  # noqa: E402

F = s.h.f
FIRST_FRAME_S = 43_201                                   # the fixture starts at 12:00:00Z and every step advances one second


@pytest.fixture(autouse=True)
def nothing_loaded_nothing_sent(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a model load, process or network use is forbidden")
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(pickle, "load", refuse)
    monkeypatch.setattr(pickle, "loads", refuse)
    monkeypatch.setattr(lf.StreamingEWMA, "__init__", refuse)          # the frozen EWMA is never constructed


def attested(tmp_path, *, until=43_204, start=0, counts=None, name="journal.jsonl", **kwargs):
    _, _, path = s.covered_journal(tmp_path, until=until, start=start, name=name)
    return s.captured(s.coverage_for(path), counts, **kwargs), path


def by_controller(rows):
    out: dict = {}
    for row in rows:
        out.setdefault(row["controller"], []).append(row)
    return out


# ------------------------------------------------------------------------------------------------- 16 valid covered history
def test_16_verified_coverage_lets_genuine_zeros_through_and_the_predictive_controller_decides(tmp_path):
    history, _ = attested(tmp_path, counts={100: 0, 5000: 0})
    manifest, rows, out = s.h.run_captured(tmp_path, history)
    assert manifest["status"] == "COMPLETED" and s.h.runner.verify_run_directory(out) == []
    predictive = by_controller(rows)["predictive"]
    assert [r["outcome"] for r in predictive][1:] == ["propose"] * 3                                   # forecast 55 is inside the grid
    block = predictive[1]["predictive_evidence"]["workload_observation"]["coverage"]
    assert block["mode"] == "strict_attested" and block["contract_satisfied"] is True and block["integrity"]["status"] == "VERIFIED"
    assert block["class_counts"]["verified_zero"] == 2 and "unavailable" not in json.dumps(block["class_counts"])
    assert all(f["validation_outcome"] == ch.VALID_COMPLETE for f in manifest["captured_history"]["frames"])


# ------------------------------------------------------------------------------------------------- 17 unknown-coverage rejection
@pytest.mark.parametrize("how", ["no_evidence", "zero_before_collector_started"])
def test_17_unknown_coverage_is_rejected_and_only_the_predictive_controller_abstains(tmp_path, how):
    if how == "no_evidence":
        history = s.captured(wc.VerifiedCoverage.no_evidence(s.SOURCE_ID, "no collector evidence"), {100: 0})
    else:
        history, _ = attested(tmp_path, start=200, counts={100: 0})                  # the collector started at 200 s; the zero at 100 s is unknown
    manifest, rows, _ = s.h.run_captured(tmp_path, history)
    groups = by_controller(rows)
    for row in groups["predictive"][1:]:
        assert row["outcome"] == "abstain" and row["abstention_scope"] == "controller" and row["frame"]["status"] == "accepted"
        assert row["reason"] == "workload_prefix_incomplete_fail_safe" and row["forecast"] is None and row["predicted_performance"] is None
    for name in ("static", "reactive"):
        assert [r["outcome"] for r in groups[name]][1:] == ["hold"] * 3                      # the shared frame is valid: they still decide
    assert {f["validation_outcome"] for f in manifest["captured_history"]["frames"]} == {ch.VALID_INCOMPLETE}
    assert abstention.classify_row(groups["predictive"][1])[0] == abstention.PREDICTIVE_HISTORY_ABSTENTION


# -------------------------------------------------------------------------------------------------------------- 18 missing history
def test_18_a_missing_second_is_not_zero_and_blocks_the_forecast(tmp_path):
    history, _ = attested(tmp_path, counts={50: None})
    manifest, rows, _ = s.h.run_captured(tmp_path, history)
    for row in by_controller(rows)["predictive"][1:]:
        assert row["outcome"] == "abstain" and row["reason"] == "workload_prefix_incomplete_fail_safe"
    frame = manifest["captured_history"]["frames"][1]
    assert frame["validation_outcome"] == ch.VALID_INCOMPLETE and frame["missing_seconds"] >= 1
    assert frame["coverage_provenance"]["class_counts"]["missing_observation"] == 1


def test_18b_no_history_at_all_at_the_start_of_the_day_is_empty_not_valid(tmp_path):
    _, _, path = s.covered_journal(tmp_path, until=10)
    history = s.captured(s.coverage_for(path))
    history.advance_to(at(0))
    assert history.evidence(at(0))["validation_outcome"] == ch.VALID_EMPTY


# ----------------------------------------------------------------------------------------------- 19 contiguous history validation
def test_19_the_history_handed_to_the_forecaster_is_the_contiguous_utc_day_prefix(tmp_path):
    history, _ = attested(tmp_path)
    manifest, _, _ = s.h.run_captured(tmp_path, history)
    for index, frame in enumerate(manifest["captured_history"]["frames"]):
        assert frame["valid_bin_count"] == FIRST_FRAME_S + index and frame["history_start_utc"].startswith("2026-09-21T00:00:00")
        assert frame["missing_seconds"] == 0 and frame["unavailable_seconds"] == 0 and frame["no_future_data"] is True
    times, counts, reference = history.store.snapshot(now_utc=at(FIRST_FRAME_S + 3).isoformat())
    parsed = [datetime.fromisoformat(t) for t in times]
    assert reference.status == "complete" and len(parsed) == FIRST_FRAME_S + 3 and all(b - a == timedelta(seconds=1) for a, b in zip(parsed, parsed[1:]))


# ------------------------------------------------------------------------------------------------------------- 20 history cutoff
def test_20_evidence_recorded_after_a_decision_is_never_used_for_that_decision(tmp_path):
    journal, clock, path = s.new_journal(tmp_path)
    for second in [3600 * k for k in range(1, 13)]:
        s.beat(journal, clock, second)                                  # certified through 12:00:00 only ...
    s.beat(journal, clock, 43_210)                                      # ... and a LATER heartbeat that would certify the decision seconds
    history = s.captured(s.coverage_for(path))
    manifest, rows, _ = s.h.run_captured(tmp_path, history)
    for frame in manifest["captured_history"]["frames"]:
        assert frame["valid_bin_count"] == 43_200 and frame["history_end_utc"].startswith("2026-09-21T11:59:59")
        assert frame["validation_outcome"] == ch.VALID_STALE_COVERAGE and frame["no_future_data"] is True
        assert frame["coverage_provenance"]["frontier_utc"].startswith("2026-09-21T12:00:00")
    assert history.coverage_counts["stale_collector_evidence"] == 0


def test_20b_a_decision_whose_history_has_gone_stale_abstains_with_a_stated_reason(tmp_path):
    journal, clock, path = s.new_journal(tmp_path)
    for second in [3600 * k for k in range(1, 13)]:
        s.beat(journal, clock, second)
    history = s.captured(s.coverage_for(path))
    steps = [s.h.f.step(advance_s=4.0, client={"count": 5, "latency_ms": 100})] + \
            [s.h.f.step(advance_s=4.0, client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5) for _ in range(3)]
    manifest, rows, _ = s.h.run_captured(tmp_path, history, steps=steps)
    late = by_controller(rows)["predictive"][-1]
    assert late["outcome"] == "abstain" and late["abstention_scope"] in ("controller", "frame")
    assert manifest["captured_history"]["frames"][-1]["validation_outcome"] == ch.VALID_STALE_COVERAGE
    assert late["forecast"] is None and late["predicted_performance"] is None


# ------------------------------------------------------------------------------------------------ 21 invalid shared frame, 22 and 23
def test_21_an_invalid_shared_frame_gives_all_three_controllers_the_same_disposition(tmp_path):
    steps = F.steady(3)
    steps[2] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5, config_reader_ok=False)
    steps[3] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5)
    history, _ = attested(tmp_path)
    manifest, rows, _ = s.h.run_captured(tmp_path, history, steps=steps)
    frames = {}
    for row in rows:
        frames.setdefault(row["frame"]["index"], []).append(row)
    statuses = {i: {r["frame"]["status"] for r in group} for i, group in frames.items()}
    assert all(len(v) == 1 for v in statuses.values())                                  # one status per frame, for every controller
    bad = [i for i, v in statuses.items() if v == {"rejected"}]
    assert bad, statuses
    for index in bad:
        group = frames[index]
        assert {r["outcome"] for r in group} == {"abstain"} and {r["abstention_scope"] for r in group} == {"frame"}
        assert len({r["frame"]["reason_code"] for r in group}) == 1 and len({s.wc.canonical(r["frame"]) for r in group}) == 1
        assert {abstention.classify_row(r)[0] for r in group} == {abstention.SHARED_INVALID_FRAME}
        predictive = [r for r in group if r["controller"] == "predictive"][0]
        assert predictive["predictive_evidence"]["frame_status"] == "rejected"
        assert predictive["predictive_evidence"]["workload_observation"]["coverage"]["mode"] == "strict_attested"      # coverage still recorded
    assert manifest["status"] == "COMPLETED"


def test_22_23_when_only_predictive_history_is_missing_the_shared_frame_stays_valid_and_identical(tmp_path):
    history = s.captured(wc.VerifiedCoverage.no_evidence(s.SOURCE_ID, "none"), {100: 0})
    manifest, rows, out = s.h.run_captured(tmp_path, history)
    assert s.h.runner.check_comparability(rows) == {"frames": 4, "comparable": True, "problems": []}
    for index in range(1, 4):
        group = [r for r in rows if r["frame"]["index"] == index]
        assert len({s.wc.canonical(r["frame"]) for r in group}) == 1 and group[0]["frame"]["status"] == "accepted"
        assert sorted((r["controller"], r["outcome"], r["abstention_scope"]) for r in group) == [
            ("predictive", "abstain", "controller"), ("reactive", "hold", None), ("static", "hold", None)]
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert summary["comparability"]["comparable"] is True and summary["frame_status_counts"] == {"accepted": 3, "initialization": 1, "rejected": 0}


# ------------------------------------------------------------------------------------------------------------- 24 provenance
def test_24_decision_records_preserve_the_coverage_result_and_its_identity(tmp_path):
    history, path = attested(tmp_path, counts={100: 0})
    manifest, rows, _ = s.h.run_captured(tmp_path, history)
    groups = by_controller(rows)
    assert all(r["predictive_evidence"] is None for name in ("static", "reactive") for r in groups[name])
    cutoffs = [f["decision_cutoff_utc"] for f in manifest["captured_history"]["frames"]]
    for row, cutoff in zip(groups["predictive"], cutoffs):
        coverage = row["predictive_evidence"]["workload_observation"]["coverage"]
        assert coverage["schema"] == wc.PROVENANCE_SCHEMA and coverage["source_id"] == s.SOURCE_ID
        assert coverage["as_of_utc"].startswith(cutoff[:19]) and coverage["journal_sha256"] == s.sha(path)
        assert coverage["chain_head_sha256"] and coverage["records_verified"] >= 13 and coverage["collector_active"] is True
        assert coverage["integrity"] == {"status": "VERIFIED", "failure_code": None, "failure_reason": None}
        assert "no running recorder writes collector-health journals yet" in coverage["note"]
    assert manifest["captured_history"]["coverage_mode"] == "strict_attested"
    assert manifest["captured_history"]["final_frame_evidence"]["coverage_provenance"]["contract_satisfied"] is True
    # the fixture forecaster stays labelled; the coverage result does not turn a fixture value into a real one
    pe = groups["predictive"][1]["predictive_evidence"]
    assert pe["fixture_data"] is True and pe["real_inference_performed"] is False and pe["forecast"]["is_fixture_value"] is True


def test_24b_a_failed_integrity_check_is_recorded_in_the_decision_rows_and_blocks_the_forecast(tmp_path):
    _, _, path = s.covered_journal(tmp_path, until=43_204)
    path.write_text(path.read_text(encoding="utf-8").replace('"events_received":0', '"events_received":9', 1), encoding="utf-8")
    manifest, rows, _ = s.h.run_captured(tmp_path, s.captured(s.coverage_for(path)))
    row = by_controller(rows)["predictive"][1]
    coverage = row["predictive_evidence"]["workload_observation"]["coverage"]
    assert coverage["integrity"]["status"] == "FAILED" and coverage["integrity"]["failure_code"] == "hash_mismatch"
    assert coverage["contract_satisfied"] is False and row["outcome"] == "abstain" and row["forecast"] is None


def test_24c_legacy_modes_are_labelled_in_the_rows_and_never_claim_the_contract(tmp_path):
    for name, history in (("bench", ch.CapturedHistory(s.h.binned({100: 0}), zero_policy=ch.ZERO_POLICY_BENCHMARK_ZERO_FILL)),
                          ("strict", ch.CapturedHistory(s.h.binned({100: 0})))):
        manifest, rows, _ = s.h.run_captured(tmp_path, history, name=name)
        block = by_controller(rows)["predictive"][1]["predictive_evidence"]["workload_observation"]["coverage"]
        assert block["contract_satisfied"] is False and block["mode"] == ("benchmark_zero_fill" if name == "bench" else "strict_no_evidence")
        assert manifest["captured_history"]["live_semantics"] is (name == "strict")


# --------------------------------------------------------------------------------------------------------- 25 no actuation
def test_25_no_actuator_is_invoked_and_real_inference_stays_off(tmp_path):
    history, _ = attested(tmp_path)
    manifest, rows, out = s.h.run_captured(tmp_path, history)
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert manifest["actuation"]["configuration_changes_applied"] == 0 and manifest["actuation"]["live_actuator_available"] is False
    assert summary["configuration_changes_applied"] == 0 and summary["shadow_actuator_proposals_accepted"] >= 0
    assert all(r["actuator"] == {"kind": "ShadowActuator", "live_actuator_available": False, "configuration_changes_applied": 0,
                                 "proposal_recorded_by_shadow_actuator": r["actuator"]["proposal_recorded_by_shadow_actuator"],
                                 "submitted_to_fabric": False} for r in rows)
    assert manifest["models"]["frozen_hfbtp_model_loaded"] is False and manifest["models"]["frozen_ewma_loaded"] is False
    assert summary["real_inference_performed"] is False and s.h.runner.sp.REAL_INFERENCE_AUTHORIZED is False


def test_25b_a_protected_channel_is_still_refused(tmp_path):
    protected = F.params()
    protected["channel"] = "e1-r002"
    inputs_dir = tmp_path / "in"
    inputs_dir.mkdir()
    history, _ = attested(tmp_path)
    p = F.write(inputs_dir / "params.json", protected)
    c = F.write(inputs_dir / "policy.json", s.h.policy_h60())
    x = F.write(inputs_dir / "fixture.json", s.h.captured_fixture())
    with pytest.raises(Exception) as excinfo:
        s.h.runner.run_fixture_shadow(s.h.runner.load_inputs(p, c, x), tmp_path / "out", captured_history=history)
    assert "e1-r002" in str(excinfo.value) or "channel" in str(excinfo.value).lower()
    assert not (tmp_path / "out").exists()


# ----------------------------------------------------------------------------------------------- authentic data, strict mode
def test_26_the_real_stored_day_without_collector_evidence_still_abstains_strictly_and_is_labelled(tmp_path):
    source = ch.CapturedSource(source_id="flashbots-mempool-2026-09-21", utc_day="2026-09-21")
    binned = ch.load_binned_series(s.h.SERIES_09_21, source, expected_sha256=s.h.SERIES_09_21_SHA)
    history = ch.CapturedHistory(binned, coverage=wc.VerifiedCoverage.no_evidence(
        source.source_id, "the archived Flashbots dataset carries no collector lifecycle, heartbeat or sequence evidence (sources column is empty)"))
    fixture = s.h.captured_fixture()
    manifest, rows, _ = s.h.run_captured(tmp_path, history, fixture_doc=fixture)
    predictive = by_controller(rows)["predictive"][1:]
    assert {r["outcome"] for r in predictive} == {"abstain"} and {r["reason"] for r in predictive} == {"workload_prefix_incomplete_fail_safe"}
    block = predictive[0]["predictive_evidence"]["workload_observation"]["coverage"]
    assert block["mode"] == "strict_no_evidence" and block["contract_satisfied"] is False
    assert manifest["captured_history"]["frames"][1]["unavailable_seconds"] > 0
    assert block["integrity"]["status"] == "NO_EVIDENCE" and "sources column is empty" in block["integrity"]["failure_reason"]
