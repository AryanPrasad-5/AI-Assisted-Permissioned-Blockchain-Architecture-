import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from fabric_perf.controllers import (  # noqa: E402
    ControllerExperimentHarness,
    ControllerState,
    CallableActuator,
    ApprovedMaxMessageCountSpace,
    FabricConfig,
    FlashbotsSeriesForecaster,
    PerformancePrediction,
    PredictiveController,
    ReactiveController,
    RecordingActuator,
    ShadowActuator,
    RollingMeanArrivalForecaster,
    StaticController,
)


class FakePerformanceModel:
    def predict(self, arrival_rate_tps, config):
        if config.max_message_count == 60:
            return PerformancePrediction(throughput_tps=arrival_rate_tps + 20, latency_ms=100)
        return PerformancePrediction(throughput_tps=arrival_rate_tps - 20, latency_ms=500)


def state(config, *, latency=100, queue=0, history=()):
    return ControllerState(
        current_config=config,
        observed_latency_ms=latency,
        queue_depth=queue,
        arrival_history_tps=history,
    )


def test_static_controller_preserves_legacy_mapping_and_does_not_change():
    fixed = {"batch_size": 50, "batch_timeout": 500}
    controller = StaticController(fixed)
    assert controller.select_configuration({"current_latency": 1.2, "tps": 200}) == fixed
    decision = controller.decide(state(FabricConfig(max_message_count=50, batch_timeout_ms=500)))
    assert decision.changed is False
    assert decision.config.max_message_count == 50


def test_reactive_controller_requires_consecutive_congestion_and_has_hysteresis():
    default = FabricConfig(max_message_count=45)
    relief = FabricConfig(max_message_count=30)
    controller = ReactiveController(
        default,
        congested_config=relief,
        latency_high_ms=200,
        latency_low_ms=120,
        queue_high=10,
        queue_low=3,
        consecutive_samples=2,
        cooldown_samples=2,
    )
    assert controller.decide(state(default, latency=210, queue=11)).changed is False
    assert controller.decide(state(default, latency=220, queue=12)).config == relief
    assert controller.decide(state(relief, latency=150, queue=5)).config == relief
    assert controller.decide(state(relief, latency=100, queue=2)).config == relief
    assert controller.decide(state(relief, latency=100, queue=2)).config == default


def test_reactive_controller_enforces_safety_validation():
    with pytest.raises(ValueError):
        ReactiveController(FabricConfig(), latency_high_ms=100, latency_low_ms=100)
    with pytest.raises(ValueError):
        ReactiveController(FabricConfig(), latency_high_ms=100, queue_high=5, queue_low=5)


def test_reactive_controller_holds_on_stale_telemetry():
    current = FabricConfig()
    controller = ReactiveController(current, congested_config=FabricConfig(max_message_count=40),
                                     latency_high_ms=100, cooldown_samples=0)
    decision = controller.decide(ControllerState(current_config=current, observed_latency_ms=1000,
                                                  telemetry_age_s=6))
    assert decision.reason == "stale_telemetry_fail_safe"
    assert decision.changed is False


def test_predictive_controller_uses_forecast_and_model_and_logs_candidates():
    current = FabricConfig(max_message_count=45)
    better = FabricConfig(max_message_count=60)
    controller = PredictiveController(
        FakePerformanceModel(),
        current,
        arrival_forecaster=RollingMeanArrivalForecaster(window=2),
        candidate_configs=[current, better],
        cooldown_samples=0,
    )
    decision = controller.decide(state(current, history=(100, 120)))
    assert decision.changed is True
    assert decision.config == better
    assert decision.predicted_arrival_rate_tps == 110
    assert len(decision.metadata["candidate_utilities"]) == 2


def test_predictive_controller_fails_safe_on_missing_or_bad_predictions():
    current = FabricConfig()
    controller = PredictiveController(
        None,
        current,
        arrival_forecaster=RollingMeanArrivalForecaster(),
        candidate_configs=[current],
    )
    missing = controller.decide(state(current))
    assert missing.reason == "performance_predictor_missing"

    class BadForecaster:
        def predict(self, history, horizon):
            return float("nan")

    controller = PredictiveController(
        FakePerformanceModel(),
        current,
        arrival_forecaster=BadForecaster(),
        candidate_configs=[current],
    )
    failed = controller.decide(state(current, history=(1,)))
    assert failed.reason == "prediction_failure_fail_safe"
    assert failed.changed is False


def test_predictive_controller_holds_on_stale_telemetry():
    current = FabricConfig()
    controller = PredictiveController(FakePerformanceModel(), current,
        arrival_forecaster=RollingMeanArrivalForecaster(), candidate_configs=[current],
        max_telemetry_age_s=5)
    decision = controller.decide(ControllerState(current_config=current, arrival_history_tps=(10,), telemetry_age_s=6))
    assert decision.reason == "stale_telemetry_fail_safe"


def test_common_harness_records_decisions_and_application_latency(tmp_path):
    current = FabricConfig()
    target = FabricConfig(max_message_count=30)
    controller = ReactiveController(current, congested_config=target, latency_high_ms=200, cooldown_samples=0)
    actuator = RecordingActuator(current)
    harness = ControllerExperimentHarness(controller, actuator, run_id="unit-reactive")
    records = harness.run([state(current, latency=250)], tmp_path / "decisions.json")
    assert records[0]["decision"]["changed"] is True
    assert records[0]["application"]["accepted"] is True
    assert actuator.current_config == target
    assert (tmp_path / "decisions.json").exists()


def test_fabric_config_rejects_unsafe_values():
    with pytest.raises(ValueError):
        FabricConfig(max_message_count=0)
    with pytest.raises(ValueError):
        FabricConfig(batch_timeout_ms=60_001)
    with pytest.raises(ValueError):
        FabricConfig(preferred_max_bytes=100, absolute_max_bytes=99)


def test_callable_actuator_records_update_failure_and_preserves_previous_config():
    current = FabricConfig()
    target = FabricConfig(max_message_count=30)

    def reject(_config):
        raise RuntimeError("configuration transaction rejected")

    result = CallableActuator(current, reject).apply(target)
    assert result.accepted is False
    assert result.applied_config == current
    assert result.elapsed_ms >= 0
    assert "configuration transaction rejected" in result.error


def test_callable_actuator_requires_verified_record_when_requested():
    current = FabricConfig(max_message_count=45)
    target = FabricConfig(max_message_count=40)
    result = CallableActuator(current, lambda _config: None, require_verified_result=True).apply(target)
    assert result.accepted is False
    assert result.applied_config == current
    assert "verified actuator" in result.error


def test_callable_actuator_preserves_verified_update_timing_record():
    current = FabricConfig(max_message_count=45)
    target = FabricConfig(max_message_count=40)
    record = {
        "success": True,
        "after_config": target.as_dict(),
        "submission_duration_ms": 12.5,
        "commit_latency_ms": 125.0,
        "commit_to_independent_verification_ms": 8.0,
        "total_adaptation_latency_ms": 140.0,
        "submission_start_timestamp_utc": "2026-11-11T00:00:00.000Z",
        "submission_completion_timestamp_utc": "2026-11-11T00:00:00.012Z",
        "config_block_commit_timestamp_utc": "2026-11-11T00:00:00Z",
        "independent_verification_timestamp_utc": "2026-11-11T00:00:00.140Z",
        "after_block_number": 8,
        "verification_checks": {"target_max_message_count": True},
    }
    result = CallableActuator(current, lambda _config: record, require_verified_result=True).apply(target)
    assert result.accepted is True
    assert result.verified_config == target
    assert result.submission_to_commit_ms == 125.0
    assert result.commit_to_independent_verification_ms == 8.0
    assert result.after_block_number == 8


def test_callable_actuator_stops_on_independent_verification_failure():
    current = FabricConfig(max_message_count=45)
    target = FabricConfig(max_message_count=40)
    record = {"success": True, "after_config": target.as_dict()}
    actuator = CallableActuator(
        current, lambda _config: record, require_verified_result=True,
        verify_fn=lambda _record, _config: {"success": False, "error": "wrong active block"},
    )
    result = actuator.apply(target)
    assert result.accepted is False
    assert result.applied_config == current
    assert result.error == "wrong active block"
    assert actuator.current_config == current


def test_approved_live_space_rejects_unapproved_fields_and_requires_explicit_mapping():
    baseline = FabricConfig(max_message_count=45)
    space = ApprovedMaxMessageCountSpace(frozenset({40, 45}), {40: 40, 45: 45}, baseline)
    space.validate(FabricConfig(max_message_count=40))
    assert space.hfbtp_block_size(FabricConfig(max_message_count=45)) == 45
    with pytest.raises(ValueError):
        space.validate(FabricConfig(max_message_count=40, batch_timeout_ms=1000))
    with pytest.raises(ValueError):
        ApprovedMaxMessageCountSpace(frozenset({40, 45}), {40: 40}, baseline)


def test_shadow_actuator_never_applies_and_harness_records_safety_gate():
    current = FabricConfig(max_message_count=45)
    target = FabricConfig(max_message_count=40)
    controller = ReactiveController(current, congested_config=target, latency_high_ms=200, cooldown_samples=0)
    harness = ControllerExperimentHarness(
        controller, ShadowActuator(current), run_id="shadow",
        configuration_policy=ApprovedMaxMessageCountSpace(frozenset({40, 45}), {40: 40, 45: 45}, current),
    )
    record = harness.process(state(current, latency=250))
    assert record["application"]["accepted"] is False
    assert record["application"]["error"] == "shadow_mode_no_submission"
