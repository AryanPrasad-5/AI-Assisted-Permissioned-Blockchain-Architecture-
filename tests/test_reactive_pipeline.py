"""Explicit deterministic unit fixtures plus a separate genuine-recording test.

Fixture latencies test policy boundaries; they are not Fabric measurements and
are never used as evidence of performance or workload realism.
"""

from dataclasses import replace
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from fabric_perf.controllers.base import ControllerState, FabricConfig
from fabric_perf.controllers.harness import ApplyResult, CallableActuator, RecordingActuator
from fabric_perf.controllers.reactive_pipeline import ReactiveTelemetryPipeline, replay_reactive_jsonl
from fabric_perf.controllers.strategies import ReactiveController
from fabric_perf.controllers.telemetry import FabricTelemetrySnapshot, MetricObservation, TelemetryDataError

ROOT = Path(__file__).resolve().parents[1]
BASE = FabricConfig(max_message_count=45)
TARGET = replace(BASE, max_message_count=40)


def stamp(index):
    return f"2026-11-16T00:00:{index:02d}Z"


def controller(**kwargs):
    return ReactiveController(BASE, congested_config=TARGET, latency_high_ms=200, latency_low_ms=100, **kwargs)


def record(index, latency=100, config=BASE, **kwargs):
    return {"timestamp_utc": stamp(index), "current_config": config.as_dict(), "telemetry_age_s": 0,
            "observed_latency_ms": latency, **kwargs}


def snapshot(index=0, latency=100, *, metric_time=None, unit="ms", config=BASE):
    metric = MetricObservation("latency_mean_ms", latency, unit, metric_time or stamp(index), "explicit unit fixture", 1,
                                missing_reason="no completed transactions" if latency is None else None)
    return FabricTelemetrySnapshot("unit-channel", stamp(index), {metric.name: metric}, config, {})


@pytest.mark.parametrize("value,changed", [(199.99, False), (200, True), (200.01, True), (1e12, True)])
def test_latency_boundary_and_no_invented_upper_ceiling(value, changed):
    pipeline = ReactiveTelemetryPipeline(controller(cooldown_samples=0), run_id="unit")
    result = pipeline.process_snapshot(snapshot(latency=value), now_utc=stamp(0))
    assert result["decision"]["changed"] is changed
    assert not result["configuration_state"]["proposal_applied"]
    assert result["configuration_state"]["after"] == BASE.as_dict()


def test_confirmed_success_recovery_and_exact_cooldown():
    policy = controller(cooldown_samples=3)
    actuator = RecordingActuator(BASE)
    pipeline = ReactiveTelemetryPipeline(policy, run_id="contract-unit", actuator=actuator)
    high = pipeline.process_record(record(0, 200))
    assert high["application"]["accepted"] and policy._active
    assert high["audit"]["policy_and_state_before"]["verified_config"] == BASE.as_dict()
    assert high["audit"]["policy_and_state_after"]["verified_config"] == TARGET.as_dict()
    assert not pipeline.process_record(record(1, 100, TARGET))["decision"]["changed"]
    assert not pipeline.process_record(record(2, 100, TARGET))["decision"]["changed"]
    low = pipeline.process_record(record(3, 100, TARGET))
    assert low["application"]["accepted"] and low["decision"]["reason"] == "recovery_threshold_crossed"
    assert actuator.current_config == BASE and not policy._active


def test_unconfirmed_configuration_never_promotes_controller_state():
    policy = controller(cooldown_samples=0)
    assert policy.decide(ControllerState.from_mapping(record(0, 200))).changed
    decision = policy.decide(ControllerState.from_mapping(record(1, 100, TARGET)))
    assert decision.reason == "configuration_mismatch_fail_safe"
    assert decision.config == BASE and policy._verified_config == BASE and not policy._active


def test_recorded_configuration_mismatch_rejects_metrics_before_harness_overwrite():
    pipeline = ReactiveTelemetryPipeline(controller(), run_id="unit")
    result = pipeline.process_record(record(0, 300, TARGET))
    assert not result["audit"]["validation"]["accepted"]
    assert result["application"] is None
    assert pipeline.controller._verified_config == BASE


def test_shadow_proposals_do_not_activate_or_start_cooldown():
    policy = controller(consecutive_samples=2, cooldown_samples=3)
    pipeline = ReactiveTelemetryPipeline(policy, run_id="unit")
    assert not pipeline.process_record(record(0, 200))["decision"]["changed"]
    for i in (1, 2, 3):
        result = pipeline.process_record(record(i, 200))
        assert result["decision"]["outcome"] == "propose"
        assert not result["application"]["accepted"]
        assert not policy._active and policy._verified_config == BASE
        assert result["decision"]["metadata"]["cooldown_remaining_samples"] == 0


def test_hysteresis_deadband_clears_consecutive_confirmation():
    pipeline = ReactiveTelemetryPipeline(controller(consecutive_samples=2), run_id="unit")
    assert not pipeline.process_record(record(0, 201))["decision"]["changed"]
    assert pipeline.process_record(record(1, 150))["decision"]["reason"] == "within_hysteresis"
    assert not pipeline.process_record(record(2, 201))["decision"]["changed"]
    assert pipeline.process_record(record(3, 201))["decision"]["changed"]


@pytest.mark.parametrize("bad", [
    {"observed_latency_ms": None}, {"observed_latency_ms": -1}, {"observed_latency_ms": float("nan")},
    {"observed_latency_ms": float("inf")}, {"observed_latency_ms": True}, {"observed_latency_ms": "200"},
    {"telemetry_age_s": 6}, {"telemetry_age_s": None}, {"telemetry_age_s": -1},
    {"timestamp_utc": "bad"}, {"timestamp_utc": "2026-11-16T00:00:01"},
    {"units": {"observed_latency_ms": "s"}}, {"current_config": {}},
])
def test_invalid_or_missing_observation_abstains_and_breaks_streak(bad):
    policy = controller(consecutive_samples=2)
    pipeline = ReactiveTelemetryPipeline(policy, run_id="unit")
    pipeline.process_record(record(0, 200))
    since = policy._since_change
    result = pipeline.process_record({**record(1, 200), **bad})
    assert result["decision"]["outcome"] == "abstain"
    assert policy._since_change == since and policy._verified_config == BASE
    assert not pipeline.process_record(record(2, 200))["decision"]["changed"]
    assert pipeline.process_record(record(3, 200))["decision"]["changed"]
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("field", ["timestamp_utc", "current_config", "telemetry_age_s"])
def test_record_schema_never_substitutes_required_fields(field):
    row = record(0, 200)
    row.pop(field)
    result = ReactiveTelemetryPipeline(controller(), run_id="unit").process_record(row)
    assert result["decision"]["outcome"] == "abstain" and result["state"] is None


@pytest.mark.parametrize("index", [0, -1])
def test_duplicate_or_older_observation_cannot_count_as_confirmation(index):
    policy = controller(consecutive_samples=2)
    pipeline = ReactiveTelemetryPipeline(policy, run_id="unit")
    pipeline.process_record(record(1, 200))
    result = pipeline.process_record(record(index + 1, 200))
    assert result["decision"]["outcome"] == "abstain"
    assert "duplicate or out of order" in result["audit"]["validation"]["error"]
    assert not pipeline.process_record(record(2, 200))["decision"]["changed"]


def test_new_capture_of_same_source_measurement_does_not_confirm_congestion():
    policy = controller(consecutive_samples=2)
    pipeline = ReactiveTelemetryPipeline(policy, run_id="unit")
    assert not pipeline.process_snapshot(snapshot(0, 200), now_utc=stamp(0))["decision"]["changed"]
    since = policy._since_change
    repeated = pipeline.process_snapshot(snapshot(1, 200, metric_time=stamp(0)), now_utc=stamp(1))
    assert repeated["decision"]["outcome"] == "abstain"
    assert policy._since_change == since
    assert not pipeline.process_snapshot(snapshot(2, 200), now_utc=stamp(2))["decision"]["changed"]
    assert pipeline.process_snapshot(snapshot(3, 200), now_utc=stamp(3))["decision"]["changed"]


def test_repeated_recovery_source_does_not_advance_cooldown():
    policy = controller(cooldown_samples=3)
    policy.configuration_applied(TARGET)
    pipeline = ReactiveTelemetryPipeline(policy, run_id="unit", actuator=RecordingActuator(TARGET))
    pipeline.process_snapshot(snapshot(0, 100, config=TARGET), now_utc=stamp(0))
    since = policy._since_change
    row = pipeline.process_snapshot(snapshot(1, 100, metric_time=stamp(0), config=TARGET), now_utc=stamp(1))
    assert row["decision"]["outcome"] == "abstain" and policy._since_change == since


@pytest.mark.parametrize("age", [None, -1, float("nan"), float("inf"), True, 6])
def test_recorded_source_age_is_validated(age):
    row = {**record(0, 200), "freshness_seconds": {"latency_mean_ms": age}}
    result = ReactiveTelemetryPipeline(controller(), run_id="unit").process_record(row)
    assert result["decision"]["outcome"] == "abstain"


def test_legacy_latency_without_units_is_not_interpreted_as_ms():
    row = record(0)
    row.pop("observed_latency_ms")
    row["current_latency"] = 2
    result = ReactiveTelemetryPipeline(controller(), run_id="unit").process_record(row)
    assert result["decision"]["outcome"] == "abstain"


@pytest.mark.parametrize("value", [-1, float("inf"), True, "200"])
def test_mutated_controller_state_is_validated_at_use(value):
    state = ControllerState.from_mapping(record(0))
    state.observed_latency_ms = value
    assert controller().decide(state).reason == "invalid_telemetry_fail_safe"


def test_core_missing_freshness_abstains():
    state = ControllerState(timestamp_utc=stamp(0), current_config=BASE, observed_latency_ms=500)
    assert controller().decide(state).reason == "telemetry_freshness_unknown_fail_safe"


def test_missing_failure_count_is_not_logged_as_observed_zero():
    pipeline = ReactiveTelemetryPipeline(controller(), run_id="unit")
    row = pipeline.process_record(record(0))
    assert row["state"]["failed_transactions"] is None
    assert row["audit"]["unavailable_fields"] == ["failed_transactions"]


@pytest.mark.parametrize("case", ["stale", "future", "wrong_units", "missing_config", "incomplete", "read_error"])
def test_snapshot_adapter_rejects_unsafe_evidence(case):
    snap = snapshot(6, 200)
    now = stamp(6)
    if case == "stale":
        snap = snapshot(6, 200, metric_time=stamp(0))
    elif case == "future":
        snap = snapshot(7, 200)
    elif case == "wrong_units":
        snap = snapshot(6, 200, unit="s")
    elif case == "missing_config":
        snap = snapshot(6, 200, config=None)
    elif case == "incomplete":
        snap = replace(snap, metrics={"latency_mean_ms": replace(snap.metric("latency_mean_ms"), incomplete_window=True)})
    else:
        snap = replace(snap, health={"read_errors": ["recorder unavailable"]})
    result = ReactiveTelemetryPipeline(controller(), run_id="unit").process_snapshot(snap, now_utc=now)
    assert result["decision"]["outcome"] == "abstain"
    assert result["application"] is None


@pytest.mark.parametrize("kwargs", [
    {"timestamp_utc": "2026-11-16T00:00:00"}, {"timestamp_utc": "invalid"},
    {"value": -1}, {"value": True}, {"staleness_threshold_s": float("nan")},
    {"sampling_interval_s": -1}, {"window_start_utc": stamp(2), "window_end_utc": stamp(1)},
])
def test_metric_schema_rejects_invalid_fields(kwargs):
    args = {"name": "latency_mean_ms", "value": 100, "unit": "ms", "timestamp_utc": stamp(0),
            "source": "unit fixture", "sampling_interval_s": 1, **kwargs}
    with pytest.raises((TelemetryDataError, ValueError)):
        MetricObservation(**args)


def test_failure_preserves_state_and_unknown_outcome_latches_stop(monkeypatch):
    policy = controller(cooldown_samples=0)
    actuator = RecordingActuator(BASE)
    calls = []
    def fail(config):
        calls.append(config)
        return ApplyResult(False, BASE, 0, "timeout", remote_state_unknown=True)
    monkeypatch.setattr(actuator, "apply", fail)
    pipeline = ReactiveTelemetryPipeline(policy, run_id="unit", actuator=actuator)
    first = pipeline.process_record(record(0, 200))
    assert first["configuration_state"]["after"] is None
    assert not policy._active and policy._verified_config == BASE
    second = pipeline.process_record(record(1, 200))
    assert second["decision"]["reason"] == "remote_state_unknown_fail_safe"
    assert calls == [TARGET]


def test_pipeline_rejects_callback_live_actuator():
    with pytest.raises(ValueError, match="only shadow"):
        ReactiveTelemetryPipeline(controller(), run_id="unit", actuator=CallableActuator(BASE, lambda _: None))


def test_jsonl_malformed_row_is_audited_and_interrupts_streak(tmp_path):
    source, out = tmp_path / "input.jsonl", tmp_path / "audit.jsonl"
    source.write_text(json.dumps(record(0, 200)) + "\n{broken\n" + json.dumps(record(2, 200)) + "\n")
    pipeline = ReactiveTelemetryPipeline(controller(consecutive_samples=2), run_id="unit")
    rows = replay_reactive_jsonl(source, pipeline, output_path=out)
    assert [row["decision"]["outcome"] for row in rows] == ["hold", "abstain", "hold"]
    assert len(out.read_text().splitlines()) == 3
    for i, row in enumerate(rows):
        assert row["audit"]["decision_index"] == i
        assert len(row["audit"]["source_file_sha256"]) == 64
        assert row["audit"]["policy_and_state_before"]["latency_high_ms"] == 200
    with pytest.raises(FileExistsError):
        replay_reactive_jsonl(source, pipeline, output_path=out)
    with pytest.raises(TelemetryDataError, match="SHA-256"):
        replay_reactive_jsonl(source, pipeline, expected_sha256="0" * 64)


def test_genuine_recorded_fabric_pipeline_has_no_model_or_live_dependency():
    entry = json.loads((ROOT / "Integration/system_integration/offline_replay_inputs.json").read_text())["files"]["snapshots"]
    source = ROOT / entry["path"]
    if not source.is_file():
        pytest.skip("genuine Fabric recording unavailable; no substitute generated")
    data = source.read_bytes()
    assert hashlib.sha256(data).hexdigest().upper() == entry["sha256"]
    original = [json.loads(line) for line in data.decode().splitlines() if line.strip()]
    initial = FabricConfig(**{key: value for key, value in original[0]["current_config"].items() if key != "batch_timeout"})
    policy = ReactiveController(initial, congested_config=initial, latency_high_ms=1500, latency_low_ms=1200,
                                consecutive_samples=2, cooldown_samples=3)
    rows = replay_reactive_jsonl(source, ReactiveTelemetryPipeline(policy, run_id="genuine"), expected_sha256=entry["sha256"])
    assert len(rows) == 20
    assert sum(row["decision"]["outcome"] == "abstain" for row in rows) == 9
    for row, recorded in zip(rows, original):
        assert row["audit"]["observation"] == recorded
        assert row["audit"]["live_submission_enabled"] is False
        assert row["configuration_state"]["after"] == initial.as_dict()
    assert hashlib.sha256(source.read_bytes()).hexdigest().upper() == entry["sha256"]


def test_reactive_cli_uses_only_standard_library_and_remains_shadow():
    entry = json.loads((ROOT / "Integration/system_integration/offline_replay_inputs.json").read_text())["files"]["snapshots"]
    if not (ROOT / entry["path"]).is_file():
        pytest.skip("genuine Fabric recording unavailable")
    result = subprocess.run([sys.executable, "-I", "-B", "-S", str(ROOT / "Integration/system_integration/run_reactive_replay.py")],
                            capture_output=True, text=True, timeout=30, check=True)
    summary = json.loads(result.stdout)
    assert summary["mode"] == "shadow" and summary["live_submission_enabled"] is False
    assert summary["records"] == 20 and summary["no_action_profile"] is True


def test_existing_continuous_csv_adapter_to_reactive_pipeline(tmp_path):
    from fabric_perf.controllers.telemetry import ContinuousCaliperLedgerAdapter
    epoch = int(datetime.fromisoformat(stamp(0).replace("Z", "+00:00")).timestamp() * 1000)
    recorder, submits, ledger = [tmp_path / name for name in ("recorder.csv", "submits.csv", "ledger.csv")]
    recorder.write_text(f"tx_id,final_ms,latency_ms,status\na,{epoch + 4000},200,success\n")
    submits.write_text(f"t_submit_evt_ms,count\n{epoch},1\n{epoch + 1000},1\n{epoch + 5000},1\n")
    ledger.write_text("tx_id,validation_code\na,0\n")
    source = ContinuousCaliperLedgerAdapter(recorder_csv=recorder, submits_csv=submits, ledger_csv=ledger,
                                             channel_id="unit-channel", run_id="csv-contract-unit", window_s=5)
    pipeline = ReactiveTelemetryPipeline(controller(consecutive_samples=2), run_id="csv-contract-unit")
    # Adapter has no configuration reader; explicitly provide the test's known
    # baseline configuration. This is a unit fixture, not a Fabric retrieval.
    first = replace(source.collect(observation_timestamp_utc=stamp(5)), current_config=BASE)
    row = pipeline.process_snapshot(first, now_utc=stamp(5))
    assert row["decision"]["outcome"] == "hold"
    assert row["state"]["observed_latency_ms"] == 200
    assert row["state"]["observed_throughput_tps"] is None
    with recorder.open("a") as stream:
        stream.write(f"b,{epoch + 6000},200,success\n")
    with submits.open("a") as stream:
        stream.write(f"{epoch + 6000},1\n")
    with ledger.open("a") as stream:
        stream.write("b,0\n")
    second = replace(source.collect(observation_timestamp_utc=stamp(6)), current_config=BASE)
    row = pipeline.process_snapshot(second, now_utc=stamp(6))
    assert row["decision"]["outcome"] == "propose"
    assert row["state"]["observed_throughput_tps"] == 1
    assert row["application"]["accepted"] is False
    assert pipeline.actuator.current_config == BASE
