import json
from pathlib import Path

import pytest

from fabric_perf.controllers import (
    FabricConfig,
    IndependentConfigurationVerifier,
    IntegrationSafetyError,
    IntegratedControllerRunner,
    LatencyEvidence,
    load_direct_cut_events,
    ProductionTelemetryProvider,
    PrometheusTelemetryAdapter,
    ReactiveController,
    StaticController,
    StopPolicy,
    TelemetryFrame,
    TimestampContract,
    VerifiedCallableActuator,
    load_jsonl_frames,
)
from fabric_perf.controllers.base import ControllerState


def frame(run_id="run-1", index=0, *, latency=100.0, queue=0.0, age=None):
    config = FabricConfig(max_message_count=45)
    state = ControllerState(
        timestamp_utc=f"2026-11-11T00:00:0{index}Z",
        current_config=config,
        observed_latency_ms=latency,
        observed_throughput_tps=10.0,
        observed_arrival_rate_tps=10.0,
        queue_depth=queue,
        telemetry_age_s=age,
        arrival_history_tps=(10.0,),
        arrival_history_timestamps_utc=(f"2026-11-11T00:00:0{index}Z",),
    )
    return TelemetryFrame(
        run_id=run_id,
        decision_index=index,
        observation_timestamp_utc=f"2026-11-11T00:00:0{index}Z",
        observation_monotonic_ns=1000 + index,
        state=state,
        source_timestamps_utc={"caliper": f"2026-11-11T00:00:0{index}Z"},
        freshness_seconds={"caliper": 0.1},
    )


def test_timestamp_alignment_and_input_equality_in_shadow_mode(tmp_path):
    config = FabricConfig(max_message_count=45)
    controllers = {
        "static": StaticController(config),
        "reactive": ReactiveController(config, congested_config=FabricConfig(max_message_count=40),
                                        latency_high_ms=200, cooldown_samples=0),
    }
    runner = IntegratedControllerRunner(
        controllers, config, run_id="run-1", required_fields=("observed_latency_ms", "queue_depth"),
        stop_policy=StopPolicy(max_queue_depth=100),
    )
    result = runner.run([frame(index=0), frame(index=1)], output_dir=tmp_path / "out")
    assert result["mode"] == "shadow"
    assert result["live_submission_enabled"] is False
    assert result["identical_input_digests"] is True
    assert result["frame_count"] == 2
    assert all(row["application"]["error"] == "shadow_mode_no_submission" for row in result["records"] if row["application"])
    assert all(row["latency"]["timestamps"]["telemetry_observation"] for row in result["records"])
    assert (tmp_path / "out" / "adaptation_latency.jsonl").exists()


def test_stale_and_missing_telemetry_are_rejected():
    with pytest.raises(IntegrationSafetyError, match="stale"):
        frame(age=6).validate(required_fields=("observed_latency_ms",), max_age_s=5)
    missing = frame()
    missing = TelemetryFrame(
        **{**missing.__dict__, "critical_missing": ("latency_mean_ms",)}
    )
    with pytest.raises(IntegrationSafetyError, match="critical telemetry"):
        missing.validate()
    state = ControllerState(timestamp_utc="2026-11-11T00:00:00Z", current_config=FabricConfig(),
                            arrival_history_tps=(10,), arrival_history_timestamps_utc=("2026-11-11T00:00:01Z",))
    future = TelemetryFrame("run-1", 0, "2026-11-11T00:00:00Z", 1, state)
    with pytest.raises(IntegrationSafetyError, match="future data"):
        future.validate()


def test_future_fields_are_rejected_from_replay(tmp_path):
    path = tmp_path / "snapshots.jsonl"
    path.write_text(json.dumps({"timestamp_utc": "2026-11-11T00:00:00Z", "future_arrivals": [20]}) + "\n", encoding="utf-8")
    with pytest.raises(IntegrationSafetyError, match="future-data"):
        load_jsonl_frames(path, run_id="run-1")


def test_jsonl_loader_round_trips_nested_telemetry_frame_state(tmp_path):
    path = tmp_path / "nested.jsonl"
    row = frame().as_dict()
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")
    loaded = load_jsonl_frames(path, run_id="run-1")
    assert loaded[0].state.observed_arrival_rate_tps == 10.0
    loaded[0].validate(required_fields=("observed_arrival_rate_tps", "observed_latency_ms"))


def test_stop_condition_prevents_controller_decisions():
    config = FabricConfig()
    runner = IntegratedControllerRunner(
        {"static": StaticController(config)}, config, run_id="run-1",
        stop_policy=StopPolicy(max_queue_depth=1),
    )
    result = runner.run([frame(queue=2)])
    assert result["record_count"] == 0
    assert "backlog_limit_exceeded" in result["stop_reasons"]


def test_live_mode_requires_explicit_enable_and_verified_actuator():
    config = FabricConfig()
    with pytest.raises(IntegrationSafetyError, match="enable_live"):
        IntegratedControllerRunner({"static": StaticController(config)}, config, run_id="run-1", shadow=False)
    with pytest.raises(ValueError, match="verify_fn"):
        VerifiedCallableActuator(config, lambda _config: {}, verify_fn=None)  # type: ignore[arg-type]


def test_verified_actuator_success_failure_and_ambiguous_commit():
    before = FabricConfig(max_message_count=45)
    target = FabricConfig(max_message_count=40)
    record = {"success": True, "after_config": target.as_dict(), "after_block_number": 8,
              "verification_checks": {"target_max_message_count": True}}
    success = VerifiedCallableActuator(before, lambda _config: record,
                                       verify_fn=lambda _record, _config: {"success": True, "timestamp_utc": "2026-11-11T00:00:01Z"})
    assert success.apply(target).accepted is True
    failed = VerifiedCallableActuator(before, lambda _config: record,
                                      verify_fn=lambda _record, _config: {"success": False, "error": "stale block"})
    failed_result = failed.apply(target)
    assert failed_result.accepted is False
    assert failed_result.remote_state_unknown is True
    ambiguous = VerifiedCallableActuator(before, lambda _config: (_ for _ in ()).throw(RuntimeError("timeout")),
                                         verify_fn=lambda _record, _config: {"success": True})
    ambiguous_result = ambiguous.apply(target)
    assert ambiguous_result.accepted is False
    assert ambiguous_result.remote_state_unknown is True


def test_latency_evidence_distinguishes_adaptation_stages():
    target = FabricConfig(max_message_count=40)
    evidence = LatencyEvidence.from_record(
        run_id="run-1", decision_index=0,
        decision_start="2026-11-11T00:00:00.000Z", decision_end="2026-11-11T00:00:00.010Z",
        decision={"metadata": {"forecast": {"inference_duration_ms": 2.0}}},
        application={
            "submission_start_timestamp_utc": "2026-11-11T00:00:00.020Z",
            "submission_completion_timestamp_utc": "2026-11-11T00:00:00.030Z",
            "config_block_commit_timestamp_utc": "2026-11-11T00:00:00.100Z",
            "independent_verification_timestamp_utc": "2026-11-11T00:00:00.120Z",
            "submission_to_commit_ms": 80.0,
            "commit_to_independent_verification_ms": 20.0,
        },
        clock=TimestampContract(),
    )
    assert evidence.durations_ms["controller_computation_ms"] == 10.0
    assert evidence.durations_ms["decision_to_submission_ms"] == 10.0
    assert evidence.durations_ms["submission_to_commit_observation_ms"] == 80.0
    assert evidence.durations_ms["commit_observation_to_independent_verification_ms"] == 20.0


def test_production_provider_preserves_missing_prometheus_client_metrics():
    prom = """# TYPE consensus_etcdraft_is_leader gauge
consensus_etcdraft_is_leader{channel=\"e1-cfg001\"} 1
# TYPE blockcutter_block_fill_duration_sum counter
blockcutter_block_fill_duration_sum{channel=\"e1-cfg001\"} 2
# TYPE blockcutter_block_fill_duration_count counter
blockcutter_block_fill_duration_count{channel=\"e1-cfg001\"} 1
"""
    def fetch(url, _timeout):
        leader = "1" if url.endswith("orderer1/metrics") else "0"
        return prom.replace(" 1\n# TYPE blockcutter", f" {leader}\n# TYPE blockcutter")

    adapter = PrometheusTelemetryAdapter(
        {"orderer1": "http://orderer1/metrics", "orderer2": "http://orderer2/metrics", "orderer3": "http://orderer3/metrics"},
        fetch=fetch,
    )
    provider = ProductionTelemetryProvider(
        adapter, "e1-cfg001", config_reader=lambda: FabricConfig(max_message_count=40),
        required_fields=(),
    )
    observed = "2026-11-11T00:00:00Z"
    frame = provider.collect(run_id="run-1", decision_index=0, observation_timestamp_utc=observed)
    frame.validate()
    assert frame.state.observed_latency_ms is None
    assert frame.provenance["prometheus_endpoints"] == {
        "orderer1": "http://orderer1/metrics", "orderer2": "http://orderer2/metrics", "orderer3": "http://orderer3/metrics"
    }
    assert frame.freshness_seconds["observation"] == 0.0


def test_independent_configuration_verifier_requires_fresh_later_matching_state():
    requested = FabricConfig(max_message_count=40)
    record = {"success": True, "after_config": requested.as_dict(), "before_block_number": 7,
              "verification_checks": {"target_max_message_count": True}}
    verifier = IndependentConfigurationVerifier(
        lambda: {"block_number": 8, "active_config": requested.as_dict()},
        transition_validator=lambda *_: {"exact_transition": True},
    )
    result = verifier(record, requested)
    assert result["success"] is True
    assert result["verification_checks"]["active_config_matches_requested"] is True
    stale = IndependentConfigurationVerifier(
        lambda: {"block_number": 7, "active_config": FabricConfig(max_message_count=45).as_dict()},
        transition_validator=lambda *_: {"exact_transition": True},
    )
    failed = stale(record, requested)
    assert failed["success"] is False
    assert failed["verification_checks"]["later_configuration_block"] is False


def test_live_runner_rejects_verified_wrapper_with_nonconcrete_verifier():
    config = FabricConfig(max_message_count=45)
    actuator = VerifiedCallableActuator(config, lambda _target: {"success": True},
                                        verify_fn=lambda *_: {"success": True})
    with pytest.raises(IntegrationSafetyError, match="IndependentConfigurationVerifier"):
        IntegratedControllerRunner(
            {"static": StaticController(config)}, config, run_id="run-live-test",
            shadow=False, enable_live=True, actuators={"static": actuator},
        )


def test_direct_cut_loader_keeps_only_confirmed_explicit_reasons(tmp_path):
    path = tmp_path / "cuts.csv"
    path.write_text(
        "channel,block_number,reason,event_ms,join_confirmed,source\n"
        "e1-cfg001,10,MESSAGE_COUNT,1000,true,orderer.log\n"
        "e1-cfg001,11,TIMEOUT,2000,true,orderer.log\n"
        "e1-r002,12,TIMEOUT,3000,true,other.log\n",
        encoding="utf-8",
    )
    events = load_direct_cut_events(path, channel_id="e1-cfg001")
    assert [row["reason"] for row in events] == ["MESSAGE_COUNT", "TIMEOUT"]

