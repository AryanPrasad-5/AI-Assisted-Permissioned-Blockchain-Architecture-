"""Common runner contracts with explicit fixtures and separate genuine-data tests.

No fixture latency or table-model output is research data. No training, random
workload, live command or Fabric configuration action is executed.
"""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from fabric_perf.controllers import (
    ApplyResult, ApprovedMaxMessageCountSpace, CallableActuator, ControllerDecision,
    FabricConfig, IntegratedControllerRunner, PerformancePrediction, PredictiveController,
    ReactiveController, RecordingActuator, StaticController, StopPolicy,
)
from fabric_perf.controllers.live_integration import IntegrationSafetyError, load_jsonl_frames
from fabric_perf.controllers.offline_replay import run_offline_replay, workload_frames

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "Integration/system_integration/offline_replay_inputs.json"
BASE = FabricConfig(max_message_count=40)
TARGET = replace(BASE, max_message_count=10)
SPACE = ApprovedMaxMessageCountSpace(frozenset({10, 40}), {10: 10, 40: 40}, BASE)


def stamp(index):
    return (datetime(2026, 9, 21, tzinfo=timezone.utc) + timedelta(seconds=index)).isoformat()


class Forecast:
    def predict(self, history, horizon):
        return history[-1]


class Model:
    def predict(self, arrival, config):
        return PerformancePrediction(120 if config == TARGET else 100, 100)


def controllers():
    return {"static": StaticController(BASE),
            "reactive": ReactiveController(BASE, congested_config=TARGET, latency_high_ms=200, latency_low_ms=100,
                                            consecutive_samples=1, cooldown_samples=0),
            "predictive": PredictiveController(Model(), BASE, arrival_forecaster=Forecast(),
                                               candidate_configs=[BASE, TARGET], horizon_seconds=60, cooldown_samples=0)}


def runner(arms=None, stop_policy=None):
    return IntegratedControllerRunner(controllers() if arms is None else arms, BASE, run_id="common-unit",
                                      configuration_policy=SPACE, stop_policy=stop_policy)


def record(index, latency=100, config=BASE, *, fabric=True):
    result = {"timestamp_utc": stamp(index), "current_config": config.as_dict(), "telemetry_age_s": 0,
              "arrival_history_tps": [100]}
    if fabric:
        result.update(observed_latency_ms=latency, observed_throughput_tps=90, failed_transactions=0)
    return result


def test_three_modes_consume_identical_inputs_start_fair_and_keep_shadow_state(tmp_path):
    inputs = [record(0), record(1, 250)]
    original = deepcopy(inputs)
    result = runner().run_offline(inputs, source_kind="fabric", output_dir=tmp_path / "result")
    assert inputs == original
    assert result["frame_count"] == 2 and result["record_count"] == 6
    assert result["identical_input_digests"] and result["fairness"]["initial_configurations_identical"]
    assert result["status"] == "completed" and result["live_submission_enabled"] is False
    for index in range(2):
        rows = [row for row in result["records"] if row["integration"]["decision_index"] == index]
        assert len({row["integration"]["controller_input_digest"] for row in rows}) == 1
        assert {tuple(row["state"]["arrival_history_tps"]) for row in rows} == {(100.0,)}
        assert all(row["configuration_state"]["after"] == BASE.as_dict() for row in rows)
    assert sorted(path.name for path in (tmp_path / "result").iterdir()) == [
        "controller_decisions.jsonl", "integration_manifest.json", "observations.jsonl", "summary.json"]
    stored = [json.loads(line) for line in (tmp_path / "result/controller_decisions.jsonl").read_text().splitlines()]
    assert len(stored) == 6
    assert result["summary"]["strategies"]["predictive"]["proposed_changes"] == 2
    assert result["summary"]["strategies"]["predictive"]["actuator_errors"] == 0


@pytest.mark.parametrize("mode", ["static", "reactive", "predictive"])
def test_each_controller_mode_is_supported(mode):
    result = runner({mode: controllers()[mode]}).run_offline([record(0)], source_kind="fabric")
    assert result["strategies"] == [mode] and result["record_count"] == 1


def test_unequal_starting_configuration_is_refused_before_replay():
    with pytest.raises(ValueError, match="identical"):
        runner({"static": StaticController(TARGET)}).run_offline([record(0)], source_kind="fabric")
    with pytest.raises(ValueError, match="identical"):
        runner().run_offline([record(0)], source_kind="fabric",
                             recording_actuators={name: RecordingActuator(TARGET) for name in controllers()})


def test_recording_changes_memory_then_uses_verified_state_with_exogenous_workload():
    arms = controllers()
    actuators = {name: RecordingActuator(BASE) for name in arms}
    result = runner(arms).run_offline([record(i, fabric=False) for i in range(3)], source_kind="flashbots",
                                     recording_actuators=actuators)
    rows = [row for row in result["records"] if row["integration"]["strategy"] == "predictive"]
    assert [row["decision"]["kind"] for row in rows] == ["select", "hold", "hold"]
    assert rows[0]["application"]["accepted"]
    assert rows[1]["state"]["current_config"] == TARGET.as_dict()
    assert result["observations"][1]["observation"]["current_config"] == BASE.as_dict()
    assert all(row["state"]["observed_latency_ms"] is None for row in rows)
    summary = result["summary"]["strategies"]["predictive"]
    assert summary["verified_configuration_changes"] == 1 and summary["live_configuration_changes"] == 0
    assert not result["summary"]["recorded_fabric"]["available"]
    assert result["summary"]["recorded_fabric"]["unweighted_window_throughput_tps"]["mean"] is None


def test_recording_cannot_relabel_historical_fabric_outcomes_after_change():
    arms = controllers()
    result = runner(arms).run_offline([record(0, 250), record(1, 250)], source_kind="fabric",
        recording_actuators={name: RecordingActuator(BASE) for name in arms})
    for name in ("reactive", "predictive"):
        rows = [row for row in result["records"] if row["integration"]["strategy"] == name]
        assert rows[0]["application"]["accepted"]
        assert rows[1]["decision"]["reason"] == "configuration_mismatch_fail_safe"
        assert rows[1]["application"] is None
    assert result["observations"][1]["observation"]["current_config"] == BASE.as_dict()
    assert result["summary"]["performance_comparison_supported"] is False


@pytest.mark.parametrize("unknown", [False, True])
def test_actuator_failure_does_not_promote_or_retry_silently(monkeypatch, unknown):
    arms = controllers()
    actuators = {name: RecordingActuator(BASE) for name in arms}
    calls = []
    def fail(config):
        calls.append(config)
        return ApplyResult(False, BASE, 0, "unit rejection", remote_state_unknown=unknown)
    monkeypatch.setattr(actuators["predictive"], "apply", fail)
    result = runner(arms).run_offline([record(i, fabric=False) for i in range(3)], source_kind="flashbots",
                                     recording_actuators=actuators)
    assert arms["predictive"]._verified_config == BASE and calls == [TARGET]
    assert result["halted_strategies"] == {"predictive": "actuation_failed_or_unverified"}
    assert result["status"] == "completed_with_errors"
    assert result["summary"]["strategies"]["predictive"]["actuator_errors"] == 1
    later = [row for row in result["records"] if row["integration"]["strategy"] == "predictive"][1]
    assert later["decision"]["kind"] == "abstain"
    assert later["configuration_state"]["after"] == (None if unknown else BASE.as_dict())


def test_unknown_outcome_stops_even_when_known_failure_retry_is_allowed(monkeypatch):
    arms = controllers()
    actuators = {name: RecordingActuator(BASE) for name in arms}
    calls = []
    def fail(config):
        calls.append(config)
        return ApplyResult(False, BASE, 0, "unknown", remote_state_unknown=True)
    monkeypatch.setattr(actuators["predictive"], "apply", fail)
    result = runner(arms, StopPolicy(stop_on_actuation_failure=False)).run_offline(
        [record(i, fabric=False) for i in range(2)], source_kind="flashbots", recording_actuators=actuators)
    assert calls == [TARGET] and result["halted_strategies"]


def test_inconsistent_success_is_not_verified(monkeypatch):
    arms = controllers()
    actuators = {name: RecordingActuator(BASE) for name in arms}
    monkeypatch.setattr(actuators["predictive"], "apply", lambda _: ApplyResult(True, TARGET, 0, verified_config=TARGET))
    result = runner(arms).run_offline([record(0, fabric=False)], source_kind="flashbots", recording_actuators=actuators)
    row = next(row for row in result["records"] if row["integration"]["strategy"] == "predictive")
    assert not row["application"]["accepted"] and row["application"]["remote_state_unknown"]
    assert arms["predictive"]._verified_config == BASE


@pytest.mark.parametrize("changes", [{"observed_latency_ms": -1}, {"telemetry_age_s": 6},
    {"timestamp_utc": "bad"}, {"current_config": {}}, {"units": {"observed_latency_ms": "seconds"}},
    {"freshness_seconds": {"unit": True}}, {"future_arrivals": [100]}, {"telemetry_age_s": None}])
def test_invalid_observation_is_logged_for_every_arm_without_filling_or_dropping(changes):
    result = runner().run_offline([{**record(0), **changes}, record(1)], source_kind="fabric")
    assert result["record_count"] == 6 and result["frame_count"] == 2
    assert result["summary"]["input_validation_errors"] == 1
    assert all(row["decision"]["kind"] == "abstain" for row in result["records"][:3])
    assert all(row["application"] is None for row in result["records"][:3])


def test_duplicate_capture_is_not_another_valid_observation():
    result = runner().run_offline([record(0), record(0), record(1)], source_kind="fabric")
    assert result["record_count"] == 9
    assert result["summary"]["input_validation_errors"] == 1


def test_reactive_source_progress_checks_are_used_by_common_runner():
    arms = {"reactive": ReactiveController(BASE, congested_config=TARGET, latency_high_ms=200,
                                            latency_low_ms=100, consecutive_samples=2)}
    inputs = [record(0, 250), {**record(1, 250), "source_timestamps_utc": {"latency_mean_ms": stamp(0)}}, record(2, 250)]
    result = runner(arms).run_offline(inputs, source_kind="fabric")
    assert [row["decision"]["kind"] for row in result["records"]] == ["hold", "abstain", "hold"]
    assert not result["records"][1]["phases"]["observation_validation"]["accepted"]
    assert result["records"][1]["phases"]["observation_validation"]["common_frame_accepted"]


def test_global_stop_policy_is_preserved_and_following_inputs_remain_audited():
    inputs = [{**record(0), "queue_depth": 20}, record(1)]
    result = runner(stop_policy=StopPolicy(max_queue_depth=10)).run_offline(inputs, source_kind="fabric")
    assert result["stop_reasons"] == ["backlog_limit_exceeded"]
    assert result["record_count"] == 6 and all(row["application"] is None for row in result["records"])


def test_flashbots_input_cannot_claim_fabric_measurements():
    result = runner().run_offline([record(0)], source_kind="flashbots")
    assert result["summary"]["input_validation_errors"] == 1
    assert result["summary"]["recorded_fabric"]["unweighted_window_mean_latency_ms"]["sample_count"] == 0


def test_summary_describes_supported_windows_not_arm_performance_or_pooled_percentiles():
    result = runner().run_offline([record(0, 100), record(2, 300)], source_kind="fabric")
    summary = result["summary"]
    assert summary["recorded_fabric"]["unweighted_window_mean_latency_ms"]["mean"] == 200
    assert summary["recorded_fabric"]["unweighted_window_throughput_tps"]["mean"] == 90
    assert summary["recorded_fabric"]["transaction_latency_p95_ms"] is None
    assert summary["recorded_fabric"]["duration_weighted_throughput_tps"] is None
    assert summary["strategies"]["static"]["decision_opportunity_hz"] == 0.5
    assert not summary["performance_comparison_supported"]


def test_decision_validation_records_rejected_request_before_any_actuation():
    class BadStatic(StaticController):
        def decide(self, state):
            return ControllerDecision("static", TARGET, False, "unit-invalid-changed-flag")
    result = runner({"static": BadStatic(BASE)}).run_offline([record(0)], source_kind="fabric")
    row = result["records"][0]
    assert row["decision"]["reason"] == "decision_validation_fail_safe" and row["application"] is None
    assert row["decision"]["metadata"]["rejected_decision"]["config"] == TARGET.as_dict()
    assert result["summary"]["strategies"]["static"]["decision_validation_errors"] == 1


def test_controller_exception_is_recorded_and_cannot_apply():
    class Throws(StaticController):
        def decide(self, state):
            raise RuntimeError("unit controller failure")
    result = runner({"static": Throws(BASE)}).run_offline([record(0)], source_kind="fabric")
    assert result["summary"]["strategies"]["static"]["controller_errors"] == 1
    assert result["records"][0]["application"] is None


def test_partial_predictions_and_failure_provenance_are_retained():
    arms = controllers()
    def predict(arrival, config):
        if config == TARGET:
            raise ValueError("unit candidate unavailable")
        return PerformancePrediction(100, 100)
    arms["predictive"].performance_predictor.predict = predict
    result = runner(arms).run_offline([record(0)], source_kind="fabric")
    row = next(row for row in result["records"] if row["integration"]["strategy"] == "predictive")
    assert row["decision"]["kind"] == "abstain"
    assert len(row["phases"]["predictions"]) == 1
    assert row["decision"]["metadata"]["failed_candidate"] == TARGET.as_dict()


def test_each_arm_isolated_from_other_controllers_state_mutation():
    class Mutates(StaticController):
        def decide(self, state):
            state.arrival_history_tps = (999,)
            return super().decide(state)
    arms = controllers()
    arms["a-static"] = Mutates(BASE)
    arms.pop("static")
    inputs = [record(0)]
    result = runner(arms).run_offline(inputs, source_kind="fabric")
    predictive = next(row for row in result["records"] if row["integration"]["strategy"] == "predictive")
    assert predictive["decision"]["predicted_arrival_rate_tps"] == 100
    assert inputs[0]["arrival_history_tps"] == [100]


def test_no_live_actuator_or_reused_runner_or_output_overwrite(tmp_path):
    arms = controllers()
    with pytest.raises(ValueError, match="built-in"):
        runner(arms).run_offline([record(0)], source_kind="fabric", recording_actuators={
            name: CallableActuator(BASE, lambda _: None) for name in arms})
    current = runner()
    current.run_offline([record(0)], source_kind="fabric", output_dir=tmp_path / "out")
    with pytest.raises(ValueError, match="fresh"):
        current.run_offline([record(0)], source_kind="fabric")
    with pytest.raises(FileExistsError):
        runner().run_offline([record(0)], source_kind="fabric", output_dir=tmp_path / "out")
    with pytest.raises(IntegrationSafetyError, match="live-enabled"):
        IntegratedControllerRunner(controllers(), BASE, run_id="unit", enable_live=True).run_offline([record(0)], source_kind="fabric")


def test_shared_controller_or_actuator_instances_are_refused():
    shared = StaticController(BASE)
    with pytest.raises(ValueError, match="isolated"):
        runner({"one": shared, "two": shared}).run_offline([record(0)], source_kind="fabric")
    same_actuator = RecordingActuator(BASE)
    with pytest.raises(ValueError, match="isolated"):
        runner().run_offline([record(0)], source_kind="fabric", recording_actuators={name: same_actuator for name in controllers()})


@pytest.mark.parametrize("mutation", ["reason", "metadata", "nonfinite_metadata", "kind", "prediction"])
def test_malformed_decision_is_rejected_before_recording_actuation(mutation):
    class BadReactive(ReactiveController):
        def decide(self, state):
            result = ControllerDecision("reactive", TARGET, True, "unit request")
            if mutation == "reason":
                result.reason = None
            elif mutation == "metadata":
                result.metadata = None
            elif mutation == "nonfinite_metadata":
                result.metadata = {"unit_invalid": float("nan")}
            elif mutation == "kind":
                result.metadata = {"decision_kind": "hold"}
            else:
                from types import SimpleNamespace
                result.predicted_performance = SimpleNamespace(throughput_tps=-1, latency_ms=100)
            return result
    policy = BadReactive(BASE, congested_config=TARGET, latency_high_ms=200)
    actuator = RecordingActuator(BASE)
    result = runner({"reactive": policy}).run_offline([record(0, 250)], source_kind="fabric",
                                                     recording_actuators={"reactive": actuator})
    row = result["records"][0]
    assert row["decision"]["reason"] == "decision_validation_fail_safe"
    assert row["application"] is None and actuator.current_config == BASE
    json.dumps(result, allow_nan=False)


def test_complete_decision_config_mapping_is_normalized_before_actuation():
    class MappingReactive(ReactiveController):
        def decide(self, state):
            return ControllerDecision("reactive", TARGET.as_dict(), True, "unit complete mapping")
    policy = MappingReactive(BASE, congested_config=TARGET, latency_high_ms=200)
    result = runner({"reactive": policy}).run_offline([record(0, 250)], source_kind="fabric",
        recording_actuators={"reactive": RecordingActuator(BASE)})
    assert result["records"][0]["application"]["accepted"]
    assert policy._verified_config == TARGET


@pytest.mark.parametrize("stop_policy", [StopPolicy(max_queue_depth=10), StopPolicy(max_failed_transactions=0)])
def test_missing_requested_guardrail_telemetry_cannot_default_to_safety(stop_policy):
    result = runner(stop_policy=stop_policy).run_offline([record(0, fabric=False)], source_kind="flashbots")
    assert result["summary"]["input_validation_errors"] == 1
    assert all(row["application"] is None for row in result["records"])


@pytest.fixture(scope="module")
def frozen_parts():
    from fabric_perf.controllers.predictive_pipeline import frozen_predictive_pipeline
    entries = json.loads(MANIFEST.read_text())["files"]
    missing = [key for key, value in entries.items() if not (ROOT / value["path"]).is_file()]
    if missing:
        pytest.skip(f"genuine inputs unavailable: {missing}; no substitute generated")
    return frozen_predictive_pipeline(ROOT, MANIFEST, initial_batch_size=40)


def real_runner(parts):
    return IntegratedControllerRunner({"static": StaticController(BASE),
        "reactive": ReactiveController(BASE, congested_config=TARGET, latency_high_ms=1500, latency_low_ms=1200,
                                        consecutive_samples=2, cooldown_samples=3),
        "predictive": PredictiveController(parts.controller.performance_predictor, BASE,
            arrival_forecaster=parts.controller.arrival_forecaster, candidate_configs=[BASE, TARGET], horizon_seconds=60)},
        BASE, run_id="genuine-common", configuration_policy=SPACE)


def test_genuine_flashbots_replay_has_repeatable_signature_and_no_fabric_metrics(frozen_parts):
    def execute():
        current = real_runner(frozen_parts)
        frames = workload_frames(frozen_parts.controller.arrival_forecaster, BASE, run_id="genuine-common", start=43200, samples=3)
        return current.run_offline(frames, source_kind="flashbots")
    first, second = execute(), execute()
    assert first["deterministic_signature"] == second["deterministic_signature"]
    assert first["identical_input_digests"] and first["record_count"] == 9
    assert first["summary"]["strategies"]["predictive"]["outcomes"] == {"select": 3}
    assert first["summary"]["strategies"]["reactive"]["outcomes"] == {"abstain": 3}
    assert all(row["observation"]["state"].get("failed_transactions") is None for row in first["observations"])
    assert not first["summary"]["recorded_fabric"]["available"]


def test_genuine_fabric_replay_preserves_measurements_and_uses_all_three_modes(frozen_parts):
    entry = json.loads(MANIFEST.read_text())["files"]["snapshots"]
    frames = load_jsonl_frames(ROOT / entry["path"], run_id="genuine-fabric-common")
    initial = frames[0].state.current_config
    from fabric_perf.controllers.adapters import HFBTPArtifactPerformancePredictor
    space = ApprovedMaxMessageCountSpace(frozenset({40, 45}), {40: 40, 45: 45}, initial)
    # Reuse frozen estimators, with the genuine recording's preserved byte-cap provenance.
    model = object.__new__(HFBTPArtifactPerformancePredictor)
    model.__dict__.update(frozen_parts.controller.performance_predictor.__dict__)
    model.candidate_space = space
    current = IntegratedControllerRunner({"static": StaticController(initial),
        "reactive": ReactiveController(initial, congested_config=initial, latency_high_ms=1500, latency_low_ms=1200,
                                         consecutive_samples=2, cooldown_samples=3),
        "predictive": PredictiveController(model, initial, arrival_forecaster=frozen_parts.controller.arrival_forecaster,
            candidate_configs=[initial, replace(initial, max_message_count=45)], horizon_seconds=60)},
        initial, run_id="genuine-fabric-common", configuration_policy=space)
    result = current.run_offline(frames, source_kind="fabric")
    assert result["record_count"] == 60 and result["summary"]["input_validation_errors"] == 0
    assert result["summary"]["strategies"]["predictive"]["outcomes"] == {"abstain": 20}
    assert result["summary"]["recorded_fabric"]["unweighted_window_mean_latency_ms"]["sample_count"] == 11
    assert result["summary"]["recorded_fabric"]["unweighted_window_throughput_tps"]["sample_count"] == 20
    for index, entry in enumerate(result["observations"]):
        assert entry["observation"]["state"]["observed_latency_ms"] == frames[index].state.observed_latency_ms


def test_static_and_reactive_factory_need_no_model_or_forecast_artifacts(tmp_path, monkeypatch):
    entries = json.loads(MANIFEST.read_text())
    if not (ROOT / entries["files"]["snapshots"]["path"]).is_file():
        pytest.skip("genuine Fabric recording unavailable")
    entries["files"] = {"snapshots": entries["files"]["snapshots"]}
    path = tmp_path / "fabric-only-pins.json"
    path.write_text(json.dumps(entries))
    import fabric_perf.controllers.offline_replay as module
    monkeypatch.setattr(module, "HFBTPArtifactPerformancePredictor", lambda *_args, **_kwargs: pytest.fail("loaded model"))
    result = run_offline_replay(ROOT, path, modes=("static", "reactive"))
    assert result["record_count"] == 40
    assert result["replay"]["frozen_alpha"] is None


def test_factory_passes_its_candidate_policy_into_common_decision_guard(monkeypatch):
    entries = json.loads(MANIFEST.read_text())
    if not (ROOT / entries["files"]["snapshots"]["path"]).is_file():
        pytest.skip("genuine Fabric recording unavailable")
    import fabric_perf.controllers.offline_replay as module
    original = module.ReactiveController
    def outside_menu(*args, **kwargs):
        policy = original(*args, **kwargs)
        policy.decide = lambda state: ControllerDecision("reactive", replace(state.current_config, max_message_count=30), True, "unit outside menu")
        return policy
    monkeypatch.setattr(module, "ReactiveController", outside_menu)
    result = module.run_offline_replay(ROOT, MANIFEST, modes=("reactive",))
    assert result["summary"]["strategies"]["reactive"]["decision_validation_errors"] > 0
    assert all(row["application"] is None for row in result["records"])
