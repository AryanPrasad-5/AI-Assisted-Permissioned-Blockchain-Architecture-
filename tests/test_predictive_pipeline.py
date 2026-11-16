"""Predictive unit contracts and genuine frozen-input integration, without training.

Small table/forecaster doubles below are explicit deterministic test fixtures,
not research predictions or Fabric measurements. Real inference tests use the
existing pinned HFBTP forests and recorded Flashbots counts only.
"""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from fabric_perf.controllers.adapters import ForecastResult, HFBTPArtifactPerformancePredictor
from fabric_perf.controllers.base import ControllerState, FabricConfig, PerformancePrediction
from fabric_perf.controllers.harness import ApplyResult, CallableActuator, RecordingActuator
from fabric_perf.controllers.predictive_pipeline import PredictiveDecisionPipeline, frozen_predictive_pipeline, replay_frozen_predictive
from fabric_perf.controllers.strategies import PredictiveController

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "Integration/system_integration/offline_replay_inputs.json"
BASE = FabricConfig(max_message_count=40)
TARGET = replace(BASE, max_message_count=10)


def stamp(index):
    return (datetime(2026, 9, 21, tzinfo=timezone.utc) + timedelta(seconds=index)).isoformat()


class ScalarForecast:
    def predict(self, history, horizon):
        return history[-1]


class TableModel:
    def predict(self, arrival, config):
        self.last_prediction_metadata = {"fixture_only": True, "config": config.as_dict()}
        return PerformancePrediction(120 if config.max_message_count == 10 else 100,
                                     100 if config.max_message_count == 10 else 200)


def controller(**kwargs):
    return PredictiveController(TableModel(), BASE, arrival_forecaster=ScalarForecast(),
                                candidate_configs=[TARGET, BASE], horizon_seconds=60, **kwargs)


def row(index=0, config=BASE, rate=100):
    return {"timestamp_utc": stamp(index), "current_config": config.as_dict(), "telemetry_age_s": 0,
            "arrival_history_tps": [rate]}


def state(index=0, config=BASE, rate=100):
    return ControllerState.from_mapping(row(index, config, rate))


def test_utility_includes_units_and_backlog_and_selected_provenance():
    result = controller(cooldown_samples=0).decide(state(rate=130))
    assert result.metadata["decision_kind"] == "select" and result.outcome == "propose"
    assert result.config == TARGET
    assert result.metadata["current_utility"] == 68  # 100 - .01*200 - (130-100)
    assert result.metadata["selected_utility"] == 109  # 120 - .01*100 - (130-120)
    assert result.metadata["utility_improvement"] == 41
    assert result.metadata["performance_predictor"]["config"] == TARGET.as_dict()


@pytest.mark.parametrize("margin,kind", [(20.999, "select"), (21, "hold"), (21.001, "hold")])
def test_strict_margin_is_preserved(margin, kind):
    result = controller(minimum_utility_improvement=margin).decide(state())
    assert result.metadata["decision_kind"] == kind
    assert result.metadata["utility_improvement"] == 21


def test_exact_utility_tie_holds_current_even_if_smaller_batch_sorts_first():
    model = TableModel()
    model.predict = lambda *_: PerformancePrediction(100, 100)
    policy = controller()
    policy.performance_predictor = model
    result = policy.decide(state())
    assert result.metadata["decision_kind"] == "hold" and result.config == BASE
    assert result.metadata["utility_improvement"] == 0


def test_telemetry_does_not_confirm_proposed_configuration():
    policy = controller()
    assert policy.decide(state()).changed
    since = policy._since_change
    result = policy.decide(state(1, TARGET))
    assert result.reason == "configuration_mismatch_fail_safe"
    assert policy._verified_config == BASE and policy._since_change == since


def test_shadow_repeated_selection_never_changes_verified_state_or_starts_cooldown():
    policy = controller(cooldown_samples=3)
    pipeline = PredictiveDecisionPipeline(policy, run_id="unit")
    for index in range(3):
        result = pipeline.process_record(row(index))
        assert result["decision"]["kind"] == "select"
        assert result["application"]["accepted"] is False
        assert result["configuration_state"]["after"] == BASE.as_dict()
        assert policy._verified_config == BASE
        assert result["decision"]["metadata"]["cooldown_remaining_samples"] == 0


def test_confirmation_starts_cooldown_and_subsequent_decisions_use_applied_state():
    policy = controller(cooldown_samples=3)
    actuator = RecordingActuator(BASE)
    pipeline = PredictiveDecisionPipeline(policy, run_id="unit", actuator=actuator)
    assert pipeline.process_record(row())["application"]["accepted"]
    assert policy._verified_config == TARGET
    policy.performance_predictor.predict = lambda _, config: PerformancePrediction(100 if config == TARGET else 120, 100)
    assert not pipeline.process_record(row(1, TARGET))["decision"]["changed"]
    assert not pipeline.process_record(row(2, TARGET))["decision"]["changed"]
    assert pipeline.process_record(row(3, TARGET))["application"]["accepted"]
    assert policy._verified_config == BASE


@pytest.mark.parametrize("value", [None, -1, float("nan"), float("inf"), True, "100"])
def test_invalid_forecast_safely_abstains_without_consuming_cooldown(value):
    policy = controller()
    policy.arrival_forecaster.predict = lambda *_: value
    before = policy._since_change
    result = policy.decide(state())
    assert result.metadata["decision_kind"] == "abstain" and result.config == BASE
    assert result.predicted_performance is None and policy._since_change == before


@pytest.mark.parametrize("changes", [{"telemetry_age_s": None}, {"telemetry_age_s": -1},
    {"telemetry_age_s": float("nan")}, {"timestamp_utc": "bad"}, {"timestamp_utc": "2026-09-21T00:00:00"},
    {"current_config": {}}, {"arrival_history_tps": [-1]}, {"units": {"arrival_history_tps": "transactions/bin"}}])
def test_invalid_recorded_schema_abstains(changes):
    result = PredictiveDecisionPipeline(controller(), run_id="unit").process_record({**row(), **changes})
    assert result["decision"]["kind"] == "abstain" and result["application"] is None


def test_duplicate_observation_and_reused_history_do_not_advance_cooldown():
    policy = controller()
    sample = state(1)
    policy.decide(sample)
    before = policy._since_change
    assert policy.decide(sample).reason == "nonadvancing_telemetry_fail_safe"
    assert policy._since_change == before
    policy = controller()
    first = state(1)
    first.arrival_history_timestamps_utc = (stamp(0),)
    policy.decide(first)
    before = policy._since_change
    second = state(2)
    second.arrival_history_timestamps_utc = (stamp(0),)
    result = policy.decide(second)
    assert result.metadata["decision_kind"] == "abstain" and "did not advance" in result.metadata["error"]
    assert policy._since_change == before


@pytest.mark.parametrize("change", [{"horizon_seconds": 10}, {"origin_timestamp_utc": stamp(20)},
    {"origin_timestamp_utc": stamp(-10)}, {"value_unit": "transactions/min"}, {"value_tps": True}])
def test_timestamped_forecast_schema_units_and_freshness(change):
    class BadForecast:
        def predict_timestamped(self, *args, **kwargs):
            return SimpleNamespace(**{"value_tps": 100, "horizon_seconds": 60, "value_unit": "transactions/s",
                                     "origin_timestamp_utc": stamp(1), **change})
    policy = controller()
    policy.arrival_forecaster = BadForecast()
    sample = state(1)
    sample.arrival_history_timestamps_utc = (stamp(0),)
    result = policy.decide(sample)
    assert result.metadata["decision_kind"] == "abstain"


def test_reused_cached_forecast_origin_is_rejected_even_with_new_input_history():
    class Cached:
        def predict_timestamped(self, *args, **kwargs):
            return ForecastResult(100, 60, 1, "unit-fixture", 0, "unit-fixture", origin_timestamp_utc=stamp(1))
    policy = controller()
    policy.arrival_forecaster = Cached()
    first = state(1)
    first.arrival_history_timestamps_utc = (stamp(0),)
    assert policy.decide(first).changed
    since = policy._since_change
    second = state(2)
    second.arrival_history_timestamps_utc = (stamp(1),)
    result = policy.decide(second)
    assert result.metadata["decision_kind"] == "abstain" and "forecast origin" in result.metadata["error"]
    assert policy._since_change == since


def test_forecast_origin_cannot_precede_newest_supplied_history():
    class OlderOrigin:
        def predict_timestamped(self, *args, **kwargs):
            return ForecastResult(100, 60, 1, "unit-fixture", 0, "unit-fixture", origin_timestamp_utc=stamp(1))
    policy = controller()
    policy.arrival_forecaster = OlderOrigin()
    sample = state(3)
    sample.arrival_history_timestamps_utc = (stamp(2),)
    result = policy.decide(sample)
    assert result.metadata["decision_kind"] == "abstain"
    assert "precedes" in result.metadata["error"]


def test_finite_predictions_with_overflowing_utility_difference_abstain():
    policy = controller(latency_penalty_per_ms=1)
    policy.performance_predictor.predict = lambda _, config: PerformancePrediction(1e308, 0) if config == TARGET else PerformancePrediction(0, 1e308)
    before = policy._since_change
    result = policy.decide(state())
    assert result.metadata["decision_kind"] == "abstain" and policy._since_change == before


@pytest.mark.parametrize("extra", [{"freshness_seconds": {"workload": float("nan")}},
    {"freshness_seconds": {"workload": 6}}, {"source_timestamps_utc": {"workload": stamp(1)}},
    {"source_timestamps_utc": {"workload": stamp(-6)}}])
def test_source_freshness_metadata_is_not_ignored(extra):
    result = PredictiveDecisionPipeline(controller(), run_id="unit").process_record({**row(), **extra})
    assert result["decision"]["kind"] == "abstain" and result["application"] is None


@pytest.mark.parametrize("value", [{"Throughput": 100, "Avg Latency": True},
    {"throughput_tps": "100", "latency_ms": 1}, {"throughput_tps": 100, "latency_ms": -1},
    {"throughput_tps": 100, "latency_ms": 1, "Avg Latency": 0.001}])
def test_performance_prediction_mapping_has_strict_numeric_and_unit_schema(value):
    policy = controller()
    policy.performance_predictor.predict = lambda *_: value
    assert policy.decide(state()).metadata["decision_kind"] == "abstain"


def test_hfbtp_seconds_mapping_converts_once():
    policy = controller()
    prediction = policy._coerce_prediction({"Throughput": 100, "Avg Latency": 0.5})
    assert prediction.latency_ms == 500 and prediction.throughput_tps == 100


def test_one_failed_candidate_abstains_instead_of_ranking_incomplete_menu():
    policy = controller()
    def predict(arrival, config):
        if config == TARGET:
            raise ValueError("unsupported test candidate")
        return PerformancePrediction(100, 100)
    policy.performance_predictor.predict = predict
    result = policy.decide(state())
    assert result.metadata["decision_kind"] == "abstain"
    assert result.metadata["abstention_stage"] == "performance_model"
    assert not result.changed and result.predicted_performance is None


def test_unknown_actuator_outcome_latches_stop_and_cannot_promote(monkeypatch):
    actuator = RecordingActuator(BASE)
    policy = controller()
    calls = []
    def fail(config):
        calls.append(config)
        return ApplyResult(False, BASE, 0, "ambiguous outcome", remote_state_unknown=True)
    monkeypatch.setattr(actuator, "apply", fail)
    pipeline = PredictiveDecisionPipeline(policy, run_id="unit", actuator=actuator)
    assert pipeline.process_record(row())["configuration_state"]["after"] is None
    assert pipeline.process_record(row(1))["decision"]["kind"] == "abstain"
    assert policy._verified_config == BASE and calls == [TARGET]


def test_offline_pipeline_refuses_live_callback_wrapper():
    with pytest.raises(ValueError, match="only shadow"):
        PredictiveDecisionPipeline(controller(), run_id="unit", actuator=CallableActuator(BASE, lambda _: None))


@pytest.fixture(scope="module")
def frozen_parts():
    entries = json.loads(MANIFEST.read_text())["files"]
    missing = [key for key in ("series", "selection", "model_metadata", "throughput_model", "latency_model")
               if not (ROOT / entries[key]["path"]).is_file()]
    if missing:
        pytest.skip(f"genuine frozen inputs unavailable: {missing}; no substitute generated")
    return frozen_predictive_pipeline(ROOT, MANIFEST)


def fresh_frozen(parts, initial_size=10):
    initial = replace(parts.controller.default_config, max_message_count=initial_size)
    policy = PredictiveController(parts.controller.performance_predictor, initial,
        arrival_forecaster=parts.controller.arrival_forecaster,
        candidate_configs=[replace(initial, max_message_count=size) for size in (10, 40)], horizon_seconds=60)
    return PredictiveDecisionPipeline(policy, run_id="genuine-frozen-replay", input_pins=parts.input_pins)


def test_genuine_frozen_forecast_and_model_pipeline_hold(tmp_path, frozen_parts):
    pipeline = fresh_frozen(frozen_parts)
    out = tmp_path / "decisions.jsonl"
    result = replay_frozen_predictive(pipeline, start=43200, samples=3, output_path=out)
    assert result["outcomes"] == {"hold": 3}
    assert not result["fabric_performance_measured"]
    first = result["records"][0]
    assert first["decision"]["predicted_arrival_rate_tps"] == pytest.approx(15.754735501056683, abs=1e-12)
    forecast = first["decision"]["metadata"]["forecast"]
    assert forecast["value_unit"] == "transactions/s"
    assert forecast["origin_timestamp_utc"] == "2026-09-21T12:00:00+00:00"
    assert forecast["target_end_timestamp_utc"] == "2026-09-21T12:01:00+00:00"
    for record in result["records"]:
        assert record["state"]["observed_latency_ms"] is None
        assert record["state"]["observed_throughput_tps"] is None
        assert record["state"]["failed_transactions"] is None
        assert not record["audit"]["predictions_are_fabric_measurements"]
        assert len(record["decision"]["metadata"]["candidate_utilities"]) == 2
    assert len(out.read_text().splitlines()) == 3
    with pytest.raises(FileExistsError):
        replay_frozen_predictive(pipeline, output_path=out)


def test_genuine_frozen_forecast_can_select_without_implying_application(frozen_parts):
    pipeline = fresh_frozen(frozen_parts, initial_size=40)
    result = replay_frozen_predictive(pipeline, start=43200, samples=3)
    assert result["outcomes"] == {"select": 3}
    for record in result["records"]:
        assert record["decision"]["config"]["max_message_count"] == 10
        assert record["configuration_state"]["after"]["max_message_count"] == 40
        assert record["application"]["accepted"] is False


def test_genuine_below_domain_forecasts_abstain_and_keep_forecast_provenance(frozen_parts):
    pipeline = fresh_frozen(frozen_parts)
    result = replay_frozen_predictive(pipeline, start=600, samples=3)
    assert result["outcomes"] == {"abstain": 3}
    for record in result["records"]:
        decision = record["decision"]
        assert decision["predicted_arrival_rate_tps"] < 10
        assert decision["predicted_performance"] is None
        assert decision["metadata"]["forecast"]["horizon_seconds"] == 60
        assert decision["metadata"]["abstention_stage"] == "performance_model"


@pytest.mark.parametrize("rate", [10, 200])
def test_real_model_domain_boundaries_and_feature_units(frozen_parts, rate):
    model = frozen_parts.controller.performance_predictor
    prediction = model.predict(rate, frozen_parts.controller.default_config)
    assert prediction.throughput_tps >= 0 and prediction.latency_ms >= 0
    metadata = model.last_prediction_metadata
    assert list(metadata["feature_units"]) == metadata["feature_order"]
    assert metadata["target_units"]["Avg Latency"] == "s"
    assert metadata["target_units"]["controller_latency"] == "ms"


def test_missing_fabric_recording_is_not_a_predictive_dependency(tmp_path, monkeypatch):
    import fabric_perf.controllers.predictive_pipeline as module
    from fabric_perf.controllers.offline_replay import verified_inputs
    entries = json.loads(MANIFEST.read_text())
    entries["files"].pop("snapshots")
    path = tmp_path / "predictive-only-manifest.json"
    path.write_text(json.dumps(entries))
    # Only test pin validation here; no additional model load is needed.
    paths = verified_inputs(ROOT, path, required_keys=module.PREDICTIVE_INPUT_KEYS)
    assert "snapshots" not in paths and len(paths) == 5


def test_loader_refuses_wrong_estimator_family_without_fitting(tmp_path, monkeypatch):
    from fabric_perf.models import artifacts
    metadata = json.loads((ROOT / "experiments/artifacts/hfbtp/metadata.json").read_text())
    (tmp_path / "metadata.json").write_text(json.dumps(metadata))
    fake = SimpleNamespace(feature_names_in_=metadata["features"], named_steps={"model": object()})
    monkeypatch.setattr(artifacts.joblib, "load", lambda *_: fake)
    with pytest.raises(ValueError, match="random forest"):
        artifacts.load_hfbtp_artifact(tmp_path, "Throughput")
