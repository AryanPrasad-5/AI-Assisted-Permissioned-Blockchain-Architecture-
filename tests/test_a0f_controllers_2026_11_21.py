"""A0f: static, reactive and predictive controllers driven by the runner over identical frames (2026-11-21).

Covers checklist items 20-28. The predictive controller runs against a labelled FIXTURE performance model and forecaster; the
frozen HFBTP model and EWMA are never loaded, so nothing here says anything about the real predictor.
"""

from __future__ import annotations

import ast
import json
import socket
import subprocess
import urllib.request

import pytest

from tests import a0f_support as s
from fabric_perf.controllers import harness
from fabric_perf.controllers import stage3_shadow_runner as runner
from fabric_perf.controllers.live_integration import IntegratedControllerRunner, IntegrationSafetyError


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("a process or network call was attempted")
    for obj, names in ((subprocess, ("run", "Popen", "call", "check_output")), (socket, ("create_connection", "getaddrinfo")),
                       (urllib.request, ("urlopen",))):
        for name in names:
            monkeypatch.setattr(obj, name, boom)


def c(n=5, latency=100):
    return {"count": n, "latency_ms": latency}


def go(tmp_path, steps, **kw):
    manifest, rows, out = s.run(tmp_path, fixture_doc=s.fixture(steps=steps), **kw)
    return s.by_frame(rows), rows, out, manifest


def high_latency(n):
    return [s.step(client=c(latency=900), ledger_valid_tx=4) for _ in range(n)]


# ---------------------------------------------------------------------------------------------- 20 static
def test_static_always_holds_the_cell_configuration_even_when_latency_is_terrible(tmp_path):
    frames, _, _, manifest = go(tmp_path, s.steady(1) + high_latency(6))
    accepted = [f["static"] for f in frames.values() if f["static"]["frame"]["status"] == "accepted"]
    assert len(accepted) == 7
    for row in accepted:
        assert row["outcome"] == "hold" and row["reason"] == "fixed_configuration" and row["config_change_proposed"] is False
        assert row["recommended_config"] == manifest["static_configuration"]
    assert manifest["static_configuration"]["max_message_count"] == s.load(s.PARAMS_FILE)["cell"]["max_message_count"]


def test_static_has_no_forecast_no_model_and_no_predicted_performance(tmp_path):
    frames, _, _, _ = go(tmp_path, s.steady(2))
    row = frames[2]["static"]
    assert row["forecast"] is None and row["model"] is None and row["predicted_performance"] is None


# ---------------------------------------------------------------------------------------------- 21 reactive
def test_reactive_holds_until_latency_is_confirmed_over_consecutive_frames_then_proposes_the_congested_configuration(tmp_path):
    frames, _, _, _ = go(tmp_path, s.steady(2) + high_latency(5))
    reactive = [frames[i]["reactive"] for i in range(len(frames))]
    assert [r["outcome"] for r in reactive[:3]] == ["abstain", "hold", "hold"]
    proposals = [r for r in reactive if r["outcome"] == "propose"]
    assert proposals, "the sustained high latency never produced a reactive proposal"
    first = proposals[0]
    assert first["reason"] == "congestion_threshold_crossed" and first["recommended_config"]["max_message_count"] == 45
    assert first["observed"]["client_latency_mean_ms"] >= 500.0          # the operator's latency_high_ms
    assert first["frame"]["index"] > next(i for i, r in enumerate(reactive) if r["observed"]["client_latency_mean_ms"] >= 500.0)


def test_reactive_never_acts_on_a_rejected_frame_and_its_streak_is_broken(tmp_path):
    steps = s.steady(1) + high_latency(1) + [s.step(client=c(latency=900), ledger_valid_tx=4, config_reader_ok=False)] + high_latency(1)
    frames, _, _, _ = go(tmp_path, steps)
    assert frames[3]["reactive"]["outcome"] == "abstain"
    assert all(r["outcome"] != "propose" for r in (frames[i]["reactive"] for i in range(len(frames))))   # the broken streak never confirmed


# ---------------------------------------------------------------------------------------------- 22 predictive on fixtures
def test_predictive_proposes_the_fixture_preferred_candidate_and_records_its_inputs(tmp_path):
    frames, _, _, _ = go(tmp_path, s.steady(2))
    row = frames[2]["predictive"]
    assert row["outcome"] == "propose" and row["recommended_config"]["max_message_count"] == 45
    assert row["forecast"]["value_tps"] == 55.0 and row["forecast"]["horizon_seconds"] == 10
    assert row["model"] == {"identity": runner.FIXTURE_PERFORMANCE_IDENTITY, "frozen_model_loaded": False}
    assert row["predicted_performance"] == {"throughput_tps": 60.0, "latency_ms": 300.0}


def test_predictive_holds_when_the_fixture_table_prefers_the_current_configuration(tmp_path):
    doc = s.fixture(steps=s.steady(2), fixture_performance_model={"40": {"throughput_tps": 60.0, "latency_ms": 300.0},
                                                                  "45": {"throughput_tps": 50.0, "latency_ms": 400.0}})
    manifest, rows, out = s.run(tmp_path, fixture_doc=doc)
    row = s.by_frame(rows)[2]["predictive"]
    assert row["outcome"] == "hold" and row["reason"] == "prediction_hold" and row["recommended_config"]["max_message_count"] == 40


def test_the_fixture_model_is_labelled_everywhere_and_the_frozen_artifact_is_never_loaded(tmp_path):
    _, rows, out, manifest = go(tmp_path, s.steady(2))
    assert manifest["models"]["frozen_hfbtp_model_loaded"] is False and manifest["models"]["frozen_ewma_loaded"] is False
    assert "NOT the frozen HFBTP model" in manifest["models"]["performance_model"]
    assert "NOT the frozen EWMA" in manifest["models"]["forecaster"]
    native = json.loads((out / "native" / "integration_manifest.json").read_text(encoding="utf-8"))
    assert native["artifact_provenance"]["frozen_model_loaded"] is False
    assert all(r["model"] is None or r["model"]["frozen_model_loaded"] is False for r in rows)


# ---------------------------------------------------------------------------------------------- 23 identical frame identity
def test_all_three_controllers_receive_the_identical_frame(tmp_path):
    frames, rows, out, _ = go(tmp_path, s.steady(3))
    for index, group in frames.items():
        assert set(group) == {"static", "reactive", "predictive"}
        assert len({r["frame"]["id"] for r in group.values()}) == 1
        for key in ("observed", "measurement", "evidence", "known_actual_config"):
            assert len({json.dumps(r[key], sort_keys=True) for r in group.values()}) == 1, key
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert summary["comparability"] == {"frames": 4, "comparable": True, "problems": []}
    native = json.loads((out / "native" / "integration_manifest.json").read_text(encoding="utf-8"))
    assert native["identical_input_digests"] is True


def test_the_recorded_frame_id_is_the_runner_digest_of_the_frame_the_controllers_saw(tmp_path):
    _, rows, out, _ = go(tmp_path, s.steady(2))
    native = [json.loads(line) for line in (out / "native" / "controller_decisions.jsonl").read_text(encoding="utf-8").splitlines()]
    digests = {(r["integration"]["decision_index"], r["integration"]["strategy"]): r["integration"]["input_digest"] for r in native}
    for row in rows:
        assert row["frame"]["id"] == digests[(row["frame"]["index"], row["controller"])]


def test_a_tampered_frame_digest_is_detected_when_records_are_joined(tmp_path):
    _, rows, out, _ = go(tmp_path, s.steady(1))
    native = [json.loads(line) for line in (out / "native" / "controller_decisions.jsonl").read_text(encoding="utf-8").splitlines()]
    frames = {0: {"input_digest": "0" * 64, "observation_timestamp_utc": "x", "observed": {}, "measurement": {}, "evidence": {},
                  "reported_config": None, "reported_config_unknown": False, "reported_config_source": None, "critical_missing": [],
                  "inconsistencies": []}}
    with pytest.raises(runner.Stage3RunnerError, match="frame identity mismatch"):
        runner.build_decision_rows(native[:1], frames, run_id="r", channel="c", cell={}, mode=runner.MODE_FIXTURE, evidence_class="x")


# ---------------------------------------------------------------------------------------------- 24 consistent abstention
@pytest.mark.parametrize("breaker", [
    {"config_reader_ok": False}, {"prometheus_ok": False}, {"poll": False, "advance_s": 6.0},      # 6 s: the unpolled ledger evidence is stale
])
def test_an_unusable_frame_gives_every_controller_the_same_abstention(tmp_path, breaker):
    steps = s.steady(2) + [s.step(**{"client": c(), "ledger_valid_tx": 5, **breaker})]
    frames, _, _, _ = go(tmp_path, steps)
    group = frames[3]
    assert {r["outcome"] for r in group.values()} == {"abstain"}
    assert len({(r["reason"], r["frame"]["reason_code"], r["frame"]["abstention_reason"]) for r in group.values()}) == 1
    for row in group.values():
        assert row["abstention_scope"] == "frame" and row["recommended_config"] is None
        assert row["config_change_proposed"] is False and row["frame"]["status"] == "rejected"


def test_an_initialization_frame_is_the_same_abstention_for_every_controller(tmp_path):
    frames, _, _, _ = go(tmp_path, s.steady(1))
    assert {r["reason"] for r in frames[0].values()} == {"telemetry_initializing_fail_safe"}
    assert {r["frame"]["status"] for r in frames[0].values()} == {"initialization"}


# ---------------------------------------------------------------------------------------------- 25 missing forecast
def test_a_missing_forecast_makes_only_the_predictive_controller_abstain(tmp_path):
    steps = s.steady(1) + [s.step(client=c(), ledger_valid_tx=5, forecast="missing")] + [s.step(client=c(), ledger_valid_tx=5)]
    frames, _, _, _ = go(tmp_path, steps)
    group = frames[2]
    assert group["predictive"]["outcome"] == "abstain" and group["predictive"]["reason"] == "prediction_failure_fail_safe"
    assert group["predictive"]["abstention_scope"] == "controller" and group["predictive"]["recommended_config"] is None
    assert group["static"]["outcome"] == "hold" and group["reactive"]["outcome"] == "hold"
    assert {r["frame"]["status"] for r in group.values()} == {"accepted"}            # the frame itself was valid
    assert len({r["frame"]["id"] for r in group.values()}) == 1
    assert frames[3]["predictive"]["outcome"] == "propose"                           # and it recovers on the next valid forecast


# ---------------------------------------------------------------------------------------------- 26 invalid model output
@pytest.mark.parametrize("fault", [{"model": "nan"}, {"model": "negative"}, {"model": "missing_candidate"},
                                   {"forecast": "nan"}, {"forecast": "negative"}])
def test_an_invalid_model_or_forecast_output_is_an_abstention_never_a_recommendation(tmp_path, fault):
    steps = s.steady(1) + [s.step(client=c(), ledger_valid_tx=5, **fault)]       # frame 0 initialization, 1 accepted, 2 faulted
    frames, _, _, _ = go(tmp_path, steps)
    assert frames[1]["predictive"]["outcome"] == "propose"                       # the same run without the fault proposes
    row = frames[2]["predictive"]
    assert row["outcome"] == "abstain" and row["reason"] == "prediction_failure_fail_safe"
    assert row["recommended_config"] is None and row["config_change_proposed"] is False and row["error"]
    assert row["abstention_scope"] == "controller" and frames[2]["static"]["outcome"] == "hold"


# ---------------------------------------------------------------------------------------------- 27 proposed vs applied
def test_a_proposal_is_never_applied_and_the_known_configuration_does_not_change(tmp_path):
    frames, rows, _, _ = go(tmp_path, s.steady(5))
    proposals = [r for r in rows if r["outcome"] == "propose"]
    assert proposals
    for row in proposals:
        assert row["config_change_proposed"] is True and row["proposal_applied"] is False
        assert row["recommended_config"]["max_message_count"] == 45
        assert row["known_actual_config"]["max_message_count"] == 40                  # the evidence is unchanged by the proposal
        assert row["recommendation_matches_known_actual_config"] is False
        assert row["actuator"]["submitted_to_fabric"] is False and row["actuator"]["configuration_changes_applied"] == 0
    # because nothing was applied, the predictive controller keeps proposing the same change frame after frame
    assert [frames[i]["predictive"]["outcome"] for i in range(1, 6)] == ["propose"] * 5


def test_a_hold_is_not_a_proposal_and_matches_the_known_configuration(tmp_path):
    frames, _, _, _ = go(tmp_path, s.steady(2))
    row = frames[1]["static"]
    assert row["config_change_proposed"] is False and row["recommendation_matches_known_actual_config"] is True


# ---------------------------------------------------------------------------------------------- 28 no actuator invocation
def test_no_live_actuator_can_be_invoked_by_a_run(tmp_path, monkeypatch):
    def forbidden(*a, **k):
        raise AssertionError("a live actuator was invoked")
    for cls in (harness.CallableActuator, harness.VerifiedCallableActuator):
        monkeypatch.setattr(cls, "apply", forbidden)
    _, rows, out, manifest = go(tmp_path, s.steady(4))
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "COMPLETED" and summary["configuration_changes_applied"] == 0
    assert summary["shadow_actuator_proposals_recorded"] >= 1 and summary["shadow_actuator_proposals_accepted"] == 0
    assert manifest["actuation"]["live_actuator_available"] is False
    assert all(r["actuator"]["live_actuator_available"] is False and r["proposal_applied"] is False for r in rows)
    native = json.loads((out / "native" / "integration_manifest.json").read_text(encoding="utf-8"))
    assert native["mode"] == "shadow" and native["live_submission_enabled"] is False


def test_the_library_refuses_a_non_shadow_runner_without_explicit_live_enablement(tmp_path):
    from fabric_perf.controllers.base import FabricConfig, StaticController
    with pytest.raises(IntegrationSafetyError):
        IntegratedControllerRunner({"static": StaticController(FabricConfig())}, FabricConfig(), run_id="r", shadow=False)


@pytest.mark.parametrize("path", [s.REPO / "src/fabric_perf/controllers/stage3_shadow_runner.py", s.E1 / "e1_stage3_shadow_runner.py"])
def test_the_runner_never_asks_for_a_live_runner_or_a_live_actuator(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    assert not names & {"enable_live", "VerifiedCallableActuator", "CallableActuator", "IndependentConfigurationVerifier", "RecordingActuator"}
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "shadow":
            assert isinstance(node.value, ast.Constant) and node.value.value is True


def test_a_non_shadow_actuator_is_refused_by_the_runner_assembly(tmp_path, monkeypatch):
    class Live:
        current_config = None
        def apply(self, config):
            raise AssertionError("live")
    monkeypatch.setattr(runner, "ShadowActuator", Live)
    with pytest.raises(Exception):
        s.run(tmp_path, fixture_doc=s.fixture(steps=s.steady(1)))


# ---------------------------------------------------------------------------------------------- comparability checker itself
def test_the_comparability_check_detects_a_frame_a_controller_did_not_receive_and_differing_inputs(tmp_path):
    _, rows, _, _ = go(tmp_path, s.steady(2))
    assert runner.check_comparability(rows)["comparable"] is True
    missing = [r for r in rows if not (r["frame"]["index"] == 1 and r["controller"] == "reactive")]
    result = runner.check_comparability(missing)
    assert result["comparable"] is False and any("frame 1" in p for p in result["problems"])
    altered = json.loads(json.dumps(rows))
    next(r for r in altered if r["frame"]["index"] == 2 and r["controller"] == "static")["frame"]["id"] = "0" * 64
    result = runner.check_comparability(altered)
    assert result["comparable"] is False and any("different frame" in p for p in result["problems"])
    altered = json.loads(json.dumps(rows))
    next(r for r in altered if r["frame"]["index"] == 2 and r["controller"] == "predictive")["observed"]["client_latency_mean_ms"] = 1.0
    assert runner.check_comparability(altered)["comparable"] is False
