"""Review (task A0b, 2026-11-20) of the opt-in invalid-frame policy ``IntegratedControllerRunner(invalid_frame_policy="abstain")``.

Nothing in ``src/`` was changed. These tests CHARACTERIZE what the runner does today, so that the review's findings are
reproducible and any later change to them is noticed. Tests named ``test_limitation_*`` pin behaviour that is NOT a
guarantee: they document a gap that a future Stage 3 shadow run has to close or accept explicitly.

Offline and in-process: fake performance model and forecaster, no Fabric, no files outside tmp_path.
"""

import ast
import inspect
import json
from dataclasses import replace
from pathlib import Path

import pytest

from fabric_perf.controllers import (
    FabricConfig,
    IntegratedControllerRunner,
    PredictiveController,
    ReactiveController,
    StaticController,
    TelemetryFrame,
)
from fabric_perf.controllers.base import ControllerState, PerformancePrediction
from fabric_perf.controllers.harness import ShadowActuator
from fabric_perf.controllers.live_integration import IntegrationSafetyError, StopPolicy

REPO = Path(__file__).resolve().parents[1]
CONFIG = FabricConfig(max_message_count=45)
THREE = {"static", "reactive", "predictive"}


class FakeModel:
    def predict(self, arrival, config):
        return PerformancePrediction(10.0, 100.0)


class FakeForecaster:
    def predict(self, history, horizon):
        return 10.0


def controllers(consecutive=1):
    return {"static": StaticController(CONFIG),
            "reactive": ReactiveController(CONFIG, congested_config=replace(CONFIG, max_message_count=40), latency_high_ms=50,
                                           latency_low_ms=10, consecutive_samples=consecutive, cooldown_samples=0),
            "predictive": PredictiveController(FakeModel(), CONFIG, arrival_forecaster=FakeForecaster(),
                                               candidate_configs=[CONFIG, replace(CONFIG, max_message_count=40)])}


def frame(index, *, latency=100.0, queue=0.0, age=0.5, freshness=None, critical=(), inconsistencies=(), observed=None,
          state_stamp=None, history=None):
    stamp = observed or f"2026-11-20T12:00:{index:02d}Z"
    history_stamps = (stamp,) if history is None else tuple(history)
    state = ControllerState(timestamp_utc=state_stamp or stamp, current_config=CONFIG, observed_latency_ms=latency,
                            observed_throughput_tps=10.0, observed_arrival_rate_tps=10.0, queue_depth=queue, telemetry_age_s=age,
                            arrival_history_tps=tuple(10.0 for _ in history_stamps), arrival_history_timestamps_utc=history_stamps)
    return TelemetryFrame("r", index, stamp, 1000 + index, state, source_timestamps_utc={"caliper": stamp},
                          freshness_seconds={"caliper": 0.1} if freshness is None else freshness,
                          critical_missing=critical, inconsistencies=inconsistencies)


def run(frames, *, policy="abstain", consecutive=1, **kwargs):
    runner = IntegratedControllerRunner(controllers(consecutive), CONFIG, run_id="r", invalid_frame_policy=policy, **kwargs)
    return runner.run(frames)


def records_at(result, index):
    return [r for r in result["records"] if r["integration"]["decision_index"] == index]


# (name, bad frame, expected reason_code, expected abstention reason, extra runner keywords)
INVALID = [
    ("stale source", frame(1, freshness={"caliper": 9.0}), "stale_observation", "stale_telemetry_fail_safe", {}),
    ("stale state age", frame(1, age=9.0), "stale_observation", "stale_telemetry_fail_safe", {}),
    ("source age unknown", frame(1, freshness={"caliper": None}), "observation_timestamp_missing",
     "telemetry_freshness_unknown_fail_safe", {}),
    ("future-dated source", frame(1, freshness={"caliper": -10.0}), "future_dated_observation",
     "future_dated_telemetry_fail_safe", {}),
    ("critical field missing", frame(1, critical=("observed_latency_ms",)), "critical_telemetry_missing",
     "telemetry_missing_fail_safe", {}),
    ("read failure", frame(1, critical=("endpoint_read_error:orderer1",)), "critical_telemetry_missing",
     "telemetry_read_failed_fail_safe", {}),
    ("inconsistent timestamps", frame(1, inconsistencies=("wall_clock_step_detected",)), "telemetry_inconsistent",
     "telemetry_timestamp_inconsistent_fail_safe", {}),
    ("state after observation", frame(1, state_stamp="2026-11-20T12:30:00Z"), "state_timestamp_after_observation",
     "telemetry_timestamp_inconsistent_fail_safe", {}),
    ("history not chronological", frame(1, history=("2026-11-20T12:00:01Z", "2026-11-20T12:00:00Z")),
     "history_not_chronological", "telemetry_timestamp_inconsistent_fail_safe", {}),
    ("required field absent", frame(1, latency=None), "required_field_missing", "telemetry_missing_fail_safe",
     {"required_fields": ("observed_latency_ms",)}),
    ("frame does not advance", frame(1, observed="2026-11-20T12:00:00Z"), "frames_not_advancing",
     "nonadvancing_telemetry_fail_safe", {}),
]
IDS = [case[0] for case in INVALID]


# --------------------------------------------------- 1. every controller receives an explicit abstention
@pytest.mark.parametrize("name, bad, code, abstention, extra", INVALID, ids=IDS)
def test_every_controller_receives_an_explicit_abstention_for_an_invalid_frame(name, bad, code, abstention, extra):
    result = run([frame(0), bad, frame(2)], **extra)
    at_bad = records_at(result, 1)
    assert {r["decision"]["controller"] for r in at_bad} == THREE and len(at_bad) == 3         # none left out
    for record in at_bad:
        assert record["decision"]["outcome"] == "abstain" and record["decision"]["changed"] is False
        assert record["decision"]["reason"] == abstention and abstention.endswith("fail_safe")
    assert {r["decision"]["outcome"] for r in at_bad} == {"abstain"}                           # no controller diverges
    assert result["abstained_frame_count"] == 1 and result["stop_reasons"] == []
    assert {r["decision"]["outcome"] for r in records_at(result, 0)} != {"abstain"}            # the good frames were not affected


# ---------------------------- 2. an invalid frame cannot be read as healthy telemetry
@pytest.mark.parametrize("name, bad, code, abstention, extra", INVALID, ids=IDS)
def test_a_rejected_frame_is_never_offered_to_a_controller_or_an_actuator(monkeypatch, name, bad, code, abstention, extra):
    offered = []
    original = ShadowActuator.apply

    def spy(self, config):
        offered.append(config)
        return original(self, config)
    monkeypatch.setattr(ShadowActuator, "apply", spy)
    run([frame(0)], **extra)
    baseline = len(offered)                                                  # the good frame alone: the reactive controller proposes
    assert baseline >= 1                                                     # so the comparison below is not vacuous
    offered.clear()
    result = run([frame(0), bad], **extra)
    assert len(offered) == baseline                                          # the invalid frame caused no actuator call at all
    for record in records_at(result, 1):
        assert record["state"] is None                                       # no observation reached the decision logic
        assert record["application"] is None and record["configuration_state"]["proposal_applied"] is False
        assert record["configuration_state"]["remote_state_unknown"] is False
    assert all(r["application"] is None or r["application"]["accepted"] is False for r in result["records"])
    assert result["live_submission_enabled"] is False and result["mode"] == "shadow"


def with_recorded_config(source: TelemetryFrame, max_message_count: int) -> TelemetryFrame:
    state = replace(source.state, current_config=replace(CONFIG, max_message_count=max_message_count))
    return replace(source, state=state)


def test_a_rejected_frame_cannot_change_the_configuration_the_shadow_actuator_believes_is_in_force():
    """observe_recorded_config makes the actuator adopt a frame's recorded configuration, so it must not see rejected frames."""
    runner = IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain")
    runner.run([frame(0), with_recorded_config(frame(1, freshness={"caliper": 9.0}), 40)])        # invalid frame says B=40
    assert {a.current_config.max_message_count for a in runner.actuators.values()} == {45}
    runner = IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain")
    runner.run([with_recorded_config(frame(0), 40)])                                              # the same claim in a VALID frame
    assert {a.current_config.max_message_count for a in runner.actuators.values()} == {40}        # is adopted: the check is not vacuous


def test_an_abstention_breaks_the_reactive_confirmation_streak_instead_of_counting_as_a_healthy_or_congested_sample():
    two_good = run([frame(0), frame(1)], consecutive=2)
    assert [r["decision"]["outcome"] for r in two_good["records"] if r["decision"]["controller"] == "reactive"] == ["hold", "propose"]
    interrupted = run([frame(0), frame(1, freshness={"caliper": 9.0}), frame(2)], consecutive=2)
    assert [r["decision"]["outcome"] for r in interrupted["records"] if r["decision"]["controller"] == "reactive"] == ["hold", "abstain", "hold"]


# ------------------------------------------------------ 3. the invalidity reason is preserved
@pytest.mark.parametrize("name, bad, code, abstention, extra", INVALID, ids=IDS)
def test_the_decision_records_preserve_the_invalidity_reason(name, bad, code, abstention, extra):
    result = run([frame(0), bad], **extra)
    entry = next(v for v in result["validation_records"] if v["decision_index"] == 1)
    assert entry["status"] == "rejected" and entry["handled"] == "abstained"
    assert entry["reason_code"] == code and entry["abstention_reason"] == abstention and entry["error"]
    for record in records_at(result, 1):
        rejection = record["decision"]["metadata"]["telemetry_rejection"]
        assert rejection["reason_code"] == code and rejection["abstention_reason"] == abstention and rejection["message"]
        assert record["integration"]["observation_rejected"]["reason_code"] == code
        assert record["decision"]["reason"] == abstention


def test_the_reason_reaches_the_files_that_a_run_writes(tmp_path):
    out = tmp_path / "run"
    result = run([frame(0), frame(1, critical=("endpoint_read_error:orderer1",)), frame(2)], consecutive=1)
    runner = IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain")
    runner.run([frame(0), frame(1, critical=("endpoint_read_error:orderer1",)), frame(2)], output_dir=out)
    evidence = json.loads((out / "validation_evidence.json").read_text(encoding="utf-8"))
    rejected = [v for v in evidence["validation_records"] if v["status"] == "rejected"]
    assert [(v["decision_index"], v["handled"], v["abstention_reason"]) for v in rejected] == [(1, "abstained", "telemetry_read_failed_fail_safe")]
    rows = [json.loads(line) for line in (out / "controller_decisions.jsonl").read_text(encoding="utf-8").splitlines()]
    assert sum(1 for r in rows if r["decision"]["outcome"] == "abstain") == 3
    manifest = json.loads((out / "integration_manifest.json").read_text(encoding="utf-8"))
    assert manifest["invalid_frame_policy"] == "abstain" and manifest["abstained_frame_count"] == 1
    assert result["abstained_frame_count"] == 1


# ------------------------------------- 4. what the policy deliberately does not turn into an abstention
def test_structural_errors_and_the_stop_policy_still_stop_the_run():
    with pytest.raises(IntegrationSafetyError, match="run_id does not match"):
        run([TelemetryFrame("another-run", 0, "2026-11-20T12:00:00Z", 1, frame(0).state)])
    negative = TelemetryFrame("r", -1, "2026-11-20T12:00:05Z", 1, frame(5).state, source_timestamps_utc={"caliper": "2026-11-20T12:00:05Z"},
                              freshness_seconds={"caliper": 0.1})
    result = run([frame(0), negative])
    entry = next(v for v in result["validation_records"] if v["decision_index"] == -1)
    assert entry["handled"] == "stopped" and result["stop_reasons"] == ["telemetry_validation_failed"]
    stopped = run([frame(0, queue=500.0)], stop_policy=StopPolicy(max_queue_depth=100))
    assert stopped["stop_reasons"] == ["backlog_limit_exceeded"] and stopped["record_count"] == 0


# ---------------------------------------------------- 5. the global default is unchanged
def test_abstain_is_opt_in_and_the_default_is_still_stop():
    assert IntegratedControllerRunner(controllers(), CONFIG, run_id="r").invalid_frame_policy == "stop"
    stop = run([frame(0), frame(1, freshness={"caliper": 9.0}), frame(2)], policy="stop")
    assert stop["stop_reasons"] == ["telemetry_validation_failed"] and stop["abstained_frame_count"] == 0
    assert len(records_at(stop, 2)) == 0                                       # the run ended at the invalid frame
    with pytest.raises(ValueError, match="'stop' or 'abstain'"):
        IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="ignore")
    from fabric_perf.controllers.live_foundation import make_shadow_foundation
    assert inspect.signature(make_shadow_foundation).parameters["invalid_frame_policy"].default == "stop"


# ======================================================= 6. limitations that remain (NOT guarantees)
def test_limitation_a_frame_that_passes_validation_but_lacks_client_metrics_is_not_abstained_by_every_controller():
    """With no required fields (what make_shadow_foundation configures) the frame is valid, and the controllers diverge."""
    result = run([frame(0, latency=None, queue=None)])
    outcome = {r["decision"]["controller"]: (r["decision"]["outcome"], r["decision"]["reason"]) for r in result["records"]}
    assert outcome == {"static": ("hold", "fixed_configuration"), "reactive": ("abstain", "telemetry_missing_fail_safe"),
                       "predictive": ("hold", "prediction_hold")}
    assert result["abstained_frame_count"] == 0                                # the policy was never involved


def test_limitation_a_frame_with_an_unknown_state_age_passes_validation_and_each_controller_decides_for_itself():
    """Two layers: the runner rejects frames (and then every controller abstains); the controllers also fail safe on their own.

    The state age being unknown is not a runner-level rejection, so the policy is not involved: the reactive and predictive
    controllers abstain by their own rule, while the static controller, which never reads an observation, holds.
    """
    result = run([frame(0, age=None)])
    outcome = {r["decision"]["controller"]: (r["decision"]["outcome"], r["decision"]["reason"]) for r in result["records"]}
    assert outcome == {"static": ("hold", "fixed_configuration"), "reactive": ("abstain", "telemetry_freshness_unknown_fail_safe"),
                       "predictive": ("abstain", "telemetry_freshness_unknown_fail_safe")}
    assert result["abstained_frame_count"] == 0


def test_declaring_the_client_fields_required_makes_all_three_controllers_abstain_on_the_same_frame():
    result = run([frame(0, latency=None, queue=None)], required_fields=("observed_latency_ms", "queue_depth"))
    assert {r["decision"]["controller"]: r["decision"]["outcome"] for r in result["records"]} == {c: "abstain" for c in THREE}
    assert result["abstained_frame_count"] == 1


def keyword_of(call: ast.Call, name: str):
    return next((k.value for k in call.keywords if k.arg == name), None)


def test_limitation_the_shadow_foundation_declares_no_required_client_fields():
    tree = ast.parse((REPO / "src" / "fabric_perf" / "controllers" / "live_foundation.py").read_text(encoding="utf-8"))
    function = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "make_shadow_foundation")
    calls = {getattr(n.func, "id", ""): n for n in ast.walk(function) if isinstance(n, ast.Call)}
    provider_fields = keyword_of(calls["ProductionTelemetryProvider"], "required_fields")
    assert isinstance(provider_fields, ast.Tuple) and provider_fields.elts == []         # required_fields=()
    assert keyword_of(calls["IntegratedControllerRunner"], "required_fields") is None      # the runner default () applies
    prometheus = keyword_of(calls["ProductionTelemetryProvider"], "required_prometheus_metrics")
    assert prometheus is not None                                                        # only a Raft metric is required by default


def test_the_production_shadow_validation_cli_requires_the_client_fields_unless_told_otherwise():
    text = (REPO / "Fabric_Live_Testbed" / "controllers" / "run_production_shadow_validation.py").read_text(encoding="utf-8")
    assert "required_fields=() if args.allow_missing_client_metrics else (" in text
    assert '"observed_arrival_rate_tps", "observed_throughput_tps", "observed_latency_ms", "queue_depth"' in text


def test_limitation_nothing_bounds_the_number_of_consecutive_abstentions():
    """A run in which every frame is invalid completes with no stop reason; only the manifest counts the abstentions."""
    result = run([frame(i, freshness={"caliper": 9.0}) for i in range(40)])
    assert result["stop_reasons"] == [] and result["abstained_frame_count"] == 40 and result["record_count"] == 120
    assert {r["decision"]["outcome"] for r in result["records"]} == {"abstain"}
    parameters = inspect.signature(IntegratedControllerRunner.__init__).parameters
    assert not any("abstain" in name or "consecutive_invalid" in name for name in parameters)       # no limit can be configured
