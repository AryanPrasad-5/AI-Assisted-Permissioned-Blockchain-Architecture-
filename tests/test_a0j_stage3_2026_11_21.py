"""A0j Stage 3 integration (checklist items 25-32): recorder observations + health journal -> coverage validation -> ArrivalHistoryStore ->
forecasting history guard -> Stage 3 recommendation or abstention.

Every run is the A0f/A0h offline fixture pipeline fed by a SYNTHETIC recorder day. Forecast and performance values are scripted fixtures.
Nothing is loaded, forecast or sent anywhere; none of it is a Caliper or Fabric measurement.
"""

from __future__ import annotations

import json
import pickle
import socket
import subprocess

import pytest

from tests import a0j_support as j
from tests.a0j_support import at, bridge, by_controller, ch, rc, wc

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


def run(tmp_path, rec, *, steps=None, name="cap"):
    report, coverage, history = j.coverage_and_history(rec)
    manifest, rows, out = j.s.h.run_captured(tmp_path, history, steps=steps, name=name)
    return report, history, manifest, rows, out


def day(tmp_path, *, zero=(), source_count=True, **kwargs):
    return j.new_recorder(tmp_path, zero=zero, source_count=source_count, **kwargs)


# ------------------------------------------------------------------------------------------------------------ 25 valid history
def test_25_verified_recorder_coverage_lets_genuine_zeros_through_and_the_predictive_controller_decides(tmp_path):
    rec = day(tmp_path, zero={100, 5000})
    report, history, manifest, rows, out = run(tmp_path, rec)
    assert manifest["status"] == "COMPLETED" and j.s.h.runner.verify_run_directory(out) == []
    predictive = by_controller(rows)["predictive"]
    assert [r["outcome"] for r in predictive][1:] == ["propose"] * 3
    block = predictive[1]["predictive_evidence"]["workload_observation"]["coverage"]
    assert block["mode"] == "strict_attested" and block["integrity"]["status"] == "VERIFIED"
    assert block["class_counts"]["verified_zero"] == 2 and block["recorder_only_zero_seconds"] == 0 and block["abstention_reason_codes"] == []
    assert all(f["validation_outcome"] == ch.VALID_COMPLETE for f in manifest["captured_history"]["frames"])
    assert report["problems"][0]["code"] == "no_stop_row"                             # the fixture recorder was not stopped; stated, not hidden


# ------------------------------------------------------------------------------------------------------------ 26 invalid history
def test_26_a_tampered_journal_blocks_the_forecast_and_is_recorded_with_its_reason(tmp_path):
    rec = day(tmp_path, zero={100})
    j.ingest(rec)
    path = tmp_path / "journal.jsonl"
    original = path.read_text(encoding="utf-8")
    tampered = original.replace('"health":"healthy"', '"health":"degraded"', 1)
    assert tampered != original
    path.write_text(tampered, encoding="utf-8")
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    coverage = rc.RecorderVerifiedCoverage.from_recorder_journal(path, source_id=j.SOURCE_ID, submit_events=events, policy=j.POLICY, run_id=j.RUN)
    history = rc.RecorderCapturedHistory(rc.bin_submit_events(events, j.recorder_source()), coverage=coverage, policy=j.POLICY)
    manifest, rows, _ = j.s.h.run_captured(tmp_path, history)
    for row in by_controller(rows)["predictive"][1:]:
        assert row["outcome"] == "abstain" and row["forecast"] is None and row["frame"]["status"] == "accepted"
        block = row["predictive_evidence"]["workload_observation"]["coverage"]
        assert block["integrity"]["status"] == "FAILED" and block["abstention_reason_codes"] == [rc.RC_HASH_CHAIN]
    assert {r["outcome"] for name in ("static", "reactive") for r in by_controller(rows)[name][1:]} == {"hold"}


# ------------------------------------------------------------------------------------------------- 27 predictive-only abstention
@pytest.mark.parametrize("scenario", ["heartbeat_only", "missed_callbacks", "interrupted"])
def test_27_incomplete_recorder_evidence_abstains_only_the_predictive_controller(tmp_path, scenario):
    if scenario == "heartbeat_only":
        rec, expected = day(tmp_path, zero={100}, source_count=False), rc.RC_NO_SOURCE_EVIDENCE
    elif scenario == "missed_callbacks":
        rec, expected = day(tmp_path, zero={100}, missed={2000}), rc.RC_MISSING_CALLBACKS
    else:
        rec = j.SyntheticRecorder(tmp_path).start().traffic(range(0, 43_205), zero={100})
        for sec in [3600 * k for k in range(1, 6)]:
            rec.beat(sec)
        rec.error(18_001, "live_submit_stream_failed", affects_from=18_000)               # never recovered
        for sec in [21_600, 25_200, 28_800, 32_400, 36_000, 39_600, 43_200, 43_201, 43_202, 43_203, 43_204]:
            rec.beat(sec, health="degraded", errors=1)
        expected = rc.RC_HEALTH_FAILURE
    manifest, rows, out = None, None, None
    _, history, manifest, rows, out = run(tmp_path, rec)
    groups = by_controller(rows)
    for row in groups["predictive"][1:]:
        assert row["outcome"] == "abstain" and row["abstention_scope"] == "controller" and row["frame"]["status"] == "accepted"
        assert row["reason"] == "workload_prefix_incomplete_fail_safe" and row["forecast"] is None and row["predicted_performance"] is None
        assert expected in row["predictive_evidence"]["workload_observation"]["coverage"]["abstention_reason_codes"]
    for name in ("static", "reactive"):
        assert [r["outcome"] for r in groups[name]][1:] == ["hold"] * 3                      # the shared frame is valid: they still decide
    assert abstention.classify_row(groups["predictive"][1])[0] == abstention.PREDICTIVE_HISTORY_ABSTENTION
    assert {f["validation_outcome"] for f in manifest["captured_history"]["frames"]} == {ch.VALID_INCOMPLETE}


# -------------------------------------------------------------------------------------------------------- 28 shared-frame abstention
def test_28_an_invalid_shared_frame_gives_all_three_controllers_the_same_disposition(tmp_path):
    steps = F.steady(3)
    steps[2] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5, config_reader_ok=False)
    steps[3] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5)
    _, _, manifest, rows, _ = run(tmp_path, day(tmp_path, zero={100}), steps=steps)
    frames: dict = {}
    for row in rows:
        frames.setdefault(row["frame"]["index"], []).append(row)
    bad = [i for i, group in frames.items() if {r["frame"]["status"] for r in group} == {"rejected"}]
    assert bad
    for index in bad:
        group = frames[index]
        assert {r["outcome"] for r in group} == {"abstain"} and {r["abstention_scope"] for r in group} == {"frame"}
        assert {abstention.classify_row(r)[0] for r in group} == {abstention.SHARED_INVALID_FRAME}
        predictive = [r for r in group if r["controller"] == "predictive"][0]
        assert predictive["predictive_evidence"]["workload_observation"]["coverage"]["mode"] == "strict_attested"       # coverage still recorded
    assert manifest["status"] == "COMPLETED"


# ----------------------------------------------------------------------------------------------------- 29 consistent frame identity
def test_29_all_three_controllers_see_the_same_frame_whatever_the_coverage_says(tmp_path):
    for name, rec in (("ok", day(tmp_path / "ok", zero={100}) if (tmp_path / "ok").mkdir() is None else None),
                      ("bad", day(tmp_path / "bad", zero={100}, source_count=False) if (tmp_path / "bad").mkdir() is None else None)):
        _, _, manifest, rows, out = run(tmp_path / name, rec)
        assert j.s.h.runner.check_comparability(rows) == {"frames": 4, "comparable": True, "problems": []}
        for index in range(1, 4):
            group = [r for r in rows if r["frame"]["index"] == index]
            assert len(group) == 3 and len({wc.canonical(r["frame"]) for r in group}) == 1


# ------------------------------------------------------------------------------------------------------------------ 30 provenance
def test_30_decision_records_carry_the_recorder_coverage_provenance_and_label_it_synthetic(tmp_path):
    rec = day(tmp_path, zero={100})
    _, history, manifest, rows, _ = run(tmp_path, rec)
    for row in by_controller(rows)["predictive"][1:]:
        block = row["predictive_evidence"]["workload_observation"]["coverage"]
        evidence = block["recorder_evidence"]
        assert block["schema"] == wc.PROVENANCE_SCHEMA and block["source_id"] == j.SOURCE_ID and block["journal_sha256"] == j.s.sha(tmp_path / "journal.jsonl")
        assert block["chain_head_sha256"] and block["records_verified"] >= 15
        assert evidence["coverage_scope"] == ["recorder_callback"] and evidence["evidence_origins"] == ["synthetic_fixture"]
        assert evidence["synthetic"] is True and evidence["live_source_coverage_verified"] is False
        assert block["live_quality"] is False and block["synthetic_evidence"] is True
        assert "no running recorder writes" not in block["note"] and "NOT verified" in block["note"]
    source = manifest["captured_history"]["source"]
    assert source["kind"] == "caliper_client_submit_events" and source["synthetic"] is True and source["is_fabric_observation"] is False
    assert "NOT ledger commits" in source["represents"]


def test_30b_live_origin_evidence_still_never_reports_live_quality_from_this_module(tmp_path):
    rec = day(tmp_path, zero={100}, origin=bridge.ORIGIN_LIVE_RECORDER)
    _, history, _, rows, _ = run(tmp_path, rec)
    block = by_controller(rows)["predictive"][1]["predictive_evidence"]["workload_observation"]["coverage"]
    assert block["recorder_evidence"]["evidence_origins"] == ["live_recorder"] and block["recorder_evidence"]["live_source_coverage_verified"] is False
    assert block["live_quality"] is False                                               # a label on a file is not a verification of a live run


def test_30c_the_decision_cutoff_never_sees_later_evidence(tmp_path):
    rec = j.SyntheticRecorder(tmp_path).start().traffic(range(0, 43_300))
    for sec in [3600 * k for k in range(1, 13)]:
        rec.beat(sec)
    rec.beat(43_250)                                                                      # a LATER heartbeat that would certify the decision seconds
    _, history, manifest, rows, _ = run(tmp_path, rec)
    for frame in manifest["captured_history"]["frames"]:
        assert frame["valid_bin_count"] == 43_200 and frame["validation_outcome"] == ch.VALID_STALE_COVERAGE and frame["no_future_data"] is True
        assert rc.RC_STALE in frame["coverage_provenance"]["abstention_reason_codes"]


# ------------------------------------------------------------------------------------------------------------ 31 abstention accounting
def test_31_coverage_abstentions_stay_in_the_denominator_and_the_categories_sum(tmp_path):
    _, _, manifest, rows, _ = run(tmp_path, day(tmp_path, zero={100}, source_count=False))
    result = abstention.account(rows, scheduled_frames=4, segment="a0j-synthetic")
    predictive, static = result["controllers"]["predictive"], result["controllers"]["static"]
    assert predictive["total_scheduled_decision_opportunities"] == 4 and result["consistent"] is True
    counts = predictive["category_counts"]
    assert counts[abstention.PREDICTIVE_HISTORY_ABSTENTION] == 3 and counts[abstention.INITIALIZATION] == 1
    assert counts[abstention.VALID_RECOMMENDATION] == 0 and sum(counts.values()) == 4
    assert predictive["reason_breakdown"][abstention.PREDICTIVE_HISTORY_ABSTENTION] == {"workload_prefix_incomplete_fail_safe": 3}
    assert static["category_counts"][abstention.VALID_RECOMMENDATION] == 3 and predictive["decision_coverage_rate"]["rate"] == 0.0     # a count of recommendations, not a performance result


# -------------------------------------------------------------------------------------------------------------- 32 no actuation
def test_32_no_actuator_is_invoked_and_real_inference_stays_off(tmp_path):
    _, _, manifest, rows, out = run(tmp_path, day(tmp_path, zero={100}))
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert manifest["actuation"]["configuration_changes_applied"] == 0 and manifest["actuation"]["live_actuator_available"] is False
    assert all(r["actuator"]["submitted_to_fabric"] is False and r["actuator"]["live_actuator_available"] is False for r in rows)
    assert manifest["models"]["frozen_hfbtp_model_loaded"] is False and manifest["models"]["frozen_ewma_loaded"] is False
    assert summary["real_inference_performed"] is False and j.s.h.runner.sp.REAL_INFERENCE_AUTHORIZED is False


def test_32b_a_recorder_history_cannot_be_combined_with_zero_fill_or_a_different_source(tmp_path):
    rec = day(tmp_path, zero={100})
    j.ingest(rec)
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    coverage = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=j.POLICY)
    plain = ch.BinnedDay(ch.CapturedSource(source_id=j.SOURCE_ID, utc_day=j.DAY), tuple([5] * ch.SECONDS_PER_DAY), {})
    with pytest.raises(ch.CapturedHistoryError) as excinfo:
        rc.RecorderCapturedHistory(plain, coverage=coverage, policy=j.POLICY)
    assert excinfo.value.code == "source_type_invalid"
    with pytest.raises(ch.CapturedHistoryError) as excinfo2:
        rc.RecorderCapturedHistory(rc.bin_submit_events(events, j.recorder_source(source_id="another-source")), coverage=coverage, policy=j.POLICY)
    assert excinfo2.value.code == "coverage_source_mismatch"
    with pytest.raises(ch.CapturedHistoryError) as excinfo3:
        rc.RecorderCapturedHistory(rc.bin_submit_events(events, j.recorder_source()), coverage=wc.VerifiedCoverage.no_evidence(j.SOURCE_ID, "x"), policy=j.POLICY)
    assert excinfo3.value.code == "coverage_type_invalid"
