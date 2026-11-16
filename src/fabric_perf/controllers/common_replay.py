"""Shared observational replay and in-memory actuator contracts, never live Fabric."""

from __future__ import annotations

from collections import Counter
import hashlib
import json
import math
from pathlib import Path
from typing import Mapping

from .base import BaseController, ControllerDecision, ControllerState, StaticController, confirmed_config, _finite_number
from .harness import ControllerExperimentHarness, RecordingActuator, ShadowActuator
from .live_integration import IntegrationSafetyError, TelemetryFrame
from .reactive_pipeline import ReactiveTelemetryPipeline, STATE_UNITS, _audit, _digest
from .strategies import PredictiveController, ReactiveController
from .telemetry import parse_observation_time


class _DecisionGuard(BaseController):
    """Validate a decision before the existing harness can submit any request."""

    def __init__(self, controller, actuator, policy):
        self.controller, self.actuator, self.policy = controller, actuator, policy

    def decide(self, state):
        try:
            decision = self.controller.decide(state)
        except Exception as exc:
            return ControllerDecision(type(self.controller).__name__, self.actuator.current_config, False,
                "controller_error_fail_safe", metadata={"error": f"{type(exc).__name__}: {exc}"})
        try:
            if not isinstance(decision, ControllerDecision):
                raise ValueError("controller returned no ControllerDecision")
            config = confirmed_config(decision.config)
            decision.config = config
            if not isinstance(decision.reason, str) or not decision.reason or not isinstance(decision.controller, str) or not decision.controller:
                raise ValueError("decision controller and reason must be non-empty strings")
            if not isinstance(decision.metadata, dict):
                raise ValueError("decision metadata must be a dictionary")
            if type(decision.changed) is not bool or decision.changed != (config != self.actuator.current_config):
                raise ValueError("decision changed flag disagrees with requested/verified configuration")
            if decision.changed and decision.outcome != "propose":
                raise ValueError("an abstention cannot request a configuration change")
            kind = "select" if decision.changed else decision.outcome
            if decision.metadata.get("decision_kind", kind) != kind:
                raise ValueError("decision kind disagrees with the proposal/hold/abstention")
            if decision.predicted_arrival_rate_tps is not None:
                _finite_number("predicted arrival TPS", decision.predicted_arrival_rate_tps, minimum=0)
            if decision.predicted_performance is not None:
                _finite_number("predicted throughput TPS", decision.predicted_performance.throughput_tps, minimum=0)
                _finite_number("predicted latency ms", decision.predicted_performance.latency_ms, minimum=0)
            if self.policy is not None:
                self.policy.validate(config)
            # Prove the full audit payload can be recorded BEFORE any actuation.
            json.dumps(decision.as_dict(), allow_nan=False)
            decision.metadata["decision_validation"] = {"accepted": True}
            return decision
        except (TypeError, ValueError, AttributeError, OverflowError) as exc:
            try:
                rejected = _audit(decision.as_dict())
            except Exception:
                rejected = repr(decision)
            return ControllerDecision(type(self.controller).__name__, self.actuator.current_config, False,
                "decision_validation_fail_safe", metadata={"decision_validation": {"accepted": False, "error": str(exc)},
                                                          "rejected_decision": rejected})

    def configuration_applied(self, config):
        self.controller.configuration_applied(config)


def _validated(row, *, run_id, index, max_age, source_kind):
    if not isinstance(row, Mapping):
        raise IntegrationSafetyError("observation must be a mapping")
    payload = row.get("state", row)
    if not isinstance(payload, Mapping):
        raise IntegrationSafetyError("state must be a mapping")
    forbidden = {"future", "future_arrivals", "future_observations", "actual_future"}
    if forbidden & (set(row) | set(payload)):
        raise IntegrationSafetyError("future observations cannot enter replay decisions")
    for name in ("timestamp_utc", "current_config", "telemetry_age_s"):
        if name not in payload or payload[name] is None:
            raise IntegrationSafetyError(f"record requires {name}; no default is substituted")
    confirmed_config(payload["current_config"])
    if source_kind == "flashbots" and any(payload.get(field) is not None for field in (
            "observed_latency_ms", "observed_latency_p95_ms", "observed_throughput_tps", "queue_depth", "failed_transactions")):
        raise IntegrationSafetyError("Flashbots workload alone cannot supply Fabric performance telemetry")
    if {"current_latency", "current_latency_ms", "arrival_rate", "arrival_history", "tps", "backlog"} & set(payload):
        raise IntegrationSafetyError("canonical recorded fields and units are required")
    units = {**STATE_UNITS, "arrival_history_tps": "transactions/s"}
    for name, unit in row.get("units", {}).items():
        if name in units and units[name] != unit:
            raise IntegrationSafetyError(f"{name} must use {units[name]}")
    for name, age in row.get("freshness_seconds", {}).items():
        if isinstance(age, bool) or not isinstance(age, (int, float)) or not math.isfinite(age) or not 0 <= age <= max_age:
            raise IntegrationSafetyError(f"source {name} has invalid or stale age")
    frame = TelemetryFrame.from_mapping(row, run_id=run_id, decision_index=index, observation_monotonic_ns=index)
    frame.validate(max_age_s=max_age)
    return frame


def _rejection(controller, actuator, reason, error):
    if hasattr(controller, "reject_observation"):
        decision = controller.reject_observation(reason, error=error)
    else:
        decision = ControllerDecision("static", actuator.current_config, False, reason, metadata={"error": error})
    return {"state": None, "decision": decision.as_dict(), "application": None,
            "configuration_state": {"after": actuator.current_config.as_dict(), "proposal_applied": False,
                                    "remote_state_unknown": False}}


def _semantic(value):
    """Replay signature ignores execution timing; raw inference output stays intact."""
    volatile = {"decision_overhead_ms", "inference_duration_ms", "elapsed_ms", "forecast_start_timestamp_utc",
                "forecast_completion_timestamp_utc"}
    if isinstance(value, dict):
        return {key: _semantic(item) for key, item in value.items() if key not in volatile}
    if isinstance(value, (tuple, list)):
        return [_semantic(item) for item in value]
    if isinstance(value, float):
        # Frozen forests aggregate in parallel; their sub-picosecond roundoff
        # is excluded from the signature, never from utility/decision arithmetic.
        return round(value, 10)
    return value


def _describe(values):
    return {"sample_count": len(values), "mean": sum(values) / len(values) if values else None,
            "min": min(values) if values else None, "max": max(values) if values else None}


def _summary(records, observations, strategies, source_kind):
    by_strategy = {}
    for name in strategies:
        rows = [row for row in records if row["integration"]["strategy"] == name]
        times = [parse_observation_time(row["integration"]["decision_timestamp_utc"]) for row in rows
                 if row["integration"]["decision_timestamp_utc"] is not None]
        gaps = [(right - left).total_seconds() for left, right in zip(times, times[1:]) if right > left]
        applications = [row["application"] for row in rows if row.get("application") is not None]
        errors = [app for app in applications if not app["accepted"] and app.get("error") != "shadow_mode_no_submission"]
        by_strategy[name] = {
            "decision_count": len(rows), "outcomes": dict(Counter(row["decision"]["kind"] for row in rows)),
            "reasons": dict(Counter(row["decision"]["reason"] for row in rows)),
            "proposed_changes": sum(row["decision"]["changed"] for row in rows),
            "verified_configuration_changes": sum(bool(row["configuration_state"]["proposal_applied"]) for row in rows),
            "configuration_change_scope": "in_memory_only", "live_configuration_changes": 0,
            "actuator_responses": len(applications), "actuator_errors": len(errors),
            "decision_validation_errors": sum(row["decision"]["reason"] == "decision_validation_fail_safe" for row in rows),
            "controller_errors": sum(row["decision"]["reason"] == "controller_error_fail_safe" for row in rows),
            "forecast_abstentions": sum(row["decision"]["metadata"].get("abstention_stage") == "forecast" for row in rows),
            "performance_model_abstentions": sum(row["decision"]["metadata"].get("abstention_stage") == "performance_model" for row in rows),
            "inter_decision_interval_s": _describe(gaps),
            "decision_opportunity_hz": len(gaps) / sum(gaps) if gaps and sum(gaps) > 0 else None,
        }
    metrics = {"latency_mean_ms": [], "throughput_tps": []}
    for entry in observations:
        if source_kind != "fabric" or not entry["validation"]["accepted"]:
            continue
        state = entry["observation"].get("state", entry["observation"])
        for key, field in (("latency_mean_ms", "observed_latency_ms"), ("throughput_tps", "observed_throughput_tps")):
            if state.get(field) is not None:
                metrics[key].append(state[field])
    return {"strategies": by_strategy,
            "input_validation_errors": sum(not entry["validation"]["accepted"] for entry in observations),
            "recorded_fabric": {
                "available": bool(metrics["latency_mean_ms"] or metrics["throughput_tps"]),
                "unweighted_window_mean_latency_ms": _describe(metrics["latency_mean_ms"]),
                "unweighted_window_throughput_tps": _describe(metrics["throughput_tps"]),
                "transaction_latency_p95_ms": None, "duration_weighted_throughput_tps": None,
                "limitation": "Source window summaries only; no pooled percentiles or controller-specific performance effects.",
            }, "performance_comparison_supported": False}


def run_common_replay(runner, observations, *, source_kind, output_dir=None, recording_actuators=None, input_pins=None):
    if not runner.shadow or runner.enable_live:
        raise IntegrationSafetyError("offline replay cannot use a live-enabled runner")
    if source_kind not in {"fabric", "flashbots"}:
        raise ValueError("source_kind must explicitly be fabric or flashbots")
    if getattr(runner, "_offline_run_started", False):
        raise ValueError("deterministic replay requires fresh runner/controller instances for each run")
    if output_dir is not None and Path(output_dir).exists():
        raise FileExistsError(f"refusing to overwrite {output_dir}")
    names = sorted(runner.controllers)
    actuators = runner.actuators if recording_actuators is None else dict(recording_actuators)
    expected_type = ShadowActuator if recording_actuators is None else RecordingActuator
    if set(actuators) != set(names) or any(type(value) is not expected_type for value in actuators.values()):
        raise ValueError("offline replay requires one built-in shadow/recording actuator per controller")
    if len({id(controller) for controller in runner.controllers.values()}) != len(names) or len({id(value) for value in actuators.values()}) != len(names):
        raise ValueError("fair replay requires isolated controller and actuator instances for every arm")
    initial = confirmed_config(runner.initial_config)
    if runner.configuration_policy is not None:
        runner.configuration_policy.validate(initial)
    pipelines, harnesses = {}, {}
    for name, controller in runner.controllers.items():
        current = controller.config if isinstance(controller, StaticController) else getattr(controller, "_verified_config", None)
        if current != initial or actuators[name].current_config != initial:
            raise ValueError("fair replay requires identical verified starting configurations")
        if getattr(controller, "_last_observation_time", None) is not None:
            raise ValueError("replay controllers must have no previous observation history")
        if isinstance(controller, ReactiveController):
            pipeline = ReactiveTelemetryPipeline(controller, run_id=runner.run_id, actuator=actuators[name])
            pipelines[name], harness = pipeline, pipeline.harness
        elif isinstance(controller, PredictiveController):
            from .predictive_pipeline import PredictiveDecisionPipeline
            pipeline = PredictiveDecisionPipeline(controller, run_id=runner.run_id, actuator=actuators[name],
                                                   configuration_policy=runner.configuration_policy, input_pins=input_pins)
            pipelines[name], harness = pipeline, pipeline.harness
        elif isinstance(controller, StaticController):
            harness = ControllerExperimentHarness(controller, actuators[name], run_id=runner.run_id,
                                                   configuration_policy=runner.configuration_policy)
        else:
            raise TypeError("offline replay supports StaticController, ReactiveController and PredictiveController")
        harness.controller = _DecisionGuard(controller, actuators[name], runner.configuration_policy)
        harnesses[name] = harness
    runner._offline_run_started = True
    runner.actuators = actuators
    records, inputs, halted, stop_reasons = [], [], {}, []
    previous_time = None
    stop_all = None
    for index, raw in enumerate(observations):
        row = raw.as_dict() if isinstance(raw, TelemetryFrame) else raw
        if isinstance(raw, TelemetryFrame):
            # This field may be assigned while loading, not by the historical
            # source. Replay ordering/freshness uses the recorded UTC clocks.
            row.pop("observation_monotonic_ns", None)
            if source_kind == "flashbots" and row["state"].get("failed_transactions") == 0:
                # workload_frames uses the legacy ControllerState default;
                # Flashbots supplies no observed Fabric failure count.
                row["state"].pop("failed_transactions", None)
        # Freeze one input before any strategy receives it. JSON is also a deep copy.
        row = _audit(row)
        digest = _digest(row)
        error, frame = None, None
        try:
            frame = _validated(row, run_id=runner.run_id, index=index, max_age=runner.max_telemetry_age_s, source_kind=source_kind)
            now = parse_observation_time(frame.observation_timestamp_utc)
            if previous_time is not None and now <= previous_time:
                raise IntegrationSafetyError("observations must advance in time")
            previous_time = now
            raw_state = row.get("state", row)
            if stop_all is None and ((runner.stop_policy.max_failed_transactions is not None and raw_state.get("failed_transactions") is None)
                                    or (runner.stop_policy.max_queue_depth is not None and raw_state.get("queue_depth") is None)):
                raise IntegrationSafetyError("configured stop guard requires unavailable telemetry")
            reasons = runner.stop_policy.reasons(frame.state)
            if reasons:
                stop_all = ",".join(reasons)
                stop_reasons.extend(reasons)
        except (ValueError, TypeError, AttributeError, KeyError) as exc:
            error = f"{type(exc).__name__}: {exc}"
        inputs.append({"decision_index": index, "input_sha256": digest, "observation": row,
                       "validation": {"accepted": error is None, "error": error}})
        payload = row.get("state", row) if isinstance(row, Mapping) else {}
        if isinstance(payload, Mapping):
            timestamp = payload.get("timestamp_utc")
        else:
            timestamp = None
        for name in names:
            controller, actuator, harness = runner.controllers[name], actuators[name], harnesses[name]
            before = None if harness.remote_state_unknown else actuator.current_config.as_dict()
            arm_row = json.loads(json.dumps(row))
            reason, arm_error = "invalid_telemetry_fail_safe", error
            if stop_all or name in halted:
                reason, arm_error = "replay_stopped_fail_safe", stop_all or halted[name]
            if arm_error is None and frame.state.current_config != actuator.current_config:
                if source_kind == "flashbots" and not any(getattr(frame.state, key) is not None for key in (
                        "observed_latency_ms", "observed_throughput_tps", "observed_latency_p95_ms", "queue_depth")):
                    # Only the offline reference state changes; workload history
                    # stays identical and no Fabric measurements are relabelled.
                    target_payload = arm_row.get("state", arm_row)
                    target_payload["current_config"] = actuator.current_config.as_dict()
                else:
                    reason, arm_error = "configuration_mismatch_fail_safe", "recorded performance belongs to another configuration"
            if arm_error is not None:
                record = _rejection(controller, actuator, reason, arm_error)
                if harness.remote_state_unknown:
                    record["configuration_state"]["after"] = None
                    record["configuration_state"]["remote_state_unknown"] = True
            elif name in pipelines:
                record = pipelines[name].process_record(arm_row, source=f"common observation {index}")
            else:
                record = harness.process(ControllerState.from_mapping(arm_row.get("state", arm_row)))
            if record.get("state") is not None and (not isinstance(payload, Mapping) or payload.get("failed_transactions") is None):
                record["state"]["failed_transactions"] = None
            record["decision"]["kind"] = (record["decision"]["metadata"].get("decision_kind") or
                                             ("select" if record["decision"]["changed"] else record["decision"]["outcome"]))
            if "audit" in record:
                record["audit"].pop("observation", None)  # retained once in observations.jsonl
            record["integration"] = {
                "strategy": name, "decision_index": index, "input_digest": digest,
                "controller_input_digest": _digest(arm_row), "decision_timestamp_utc": timestamp if error is None else None,
                "verified_config_before": before, "initial_config": initial.as_dict(),
                "source_kind": source_kind, "shadow_mode": recording_actuators is None, "live_submission_enabled": False,
            }
            metadata = record["decision"]["metadata"]
            pipeline_audit = record.get("audit", {})
            observation_error = arm_error or pipeline_audit.get("schema_error") or pipeline_audit.get("validation", {}).get("error")
            if metadata.get("abstention_stage") == "observation" and record["decision"]["outcome"] == "abstain":
                observation_error = observation_error or record["decision"]["reason"]
            record["phases"] = {
                "observation_validation": {"common_frame_accepted": error is None,
                                           "accepted": observation_error is None, "error": observation_error},
                "forecast": metadata.get("forecast") or None,
                "forecast_status": ("not_required" if not isinstance(controller, PredictiveController) else
                                    "produced" if metadata.get("forecast") else "not_produced_or_failed"),
                "predictions": metadata.get("candidate_utilities", metadata.get("candidate_evaluations", [])),
                "decision_validation": metadata.get("decision_validation"),
                "actuator_status": "responded" if record.get("application") is not None else "not_requested",
            }
            application = record.get("application")
            if application and not application["accepted"] and application.get("error") != "shadow_mode_no_submission":
                if runner.stop_policy.stop_on_actuation_failure or application.get("remote_state_unknown"):
                    halted[name] = "actuation_failed_or_unverified"
            records.append(record)
    if not inputs:
        raise ValueError("recorded observation stream is empty")
    comparison = {"initial_configurations_identical": True, "common_observation_sequence": True,
                  "comparison_scope": "decision behavior only; observations are not counterfactual arm outcomes"}
    signature_payload = {"observations": inputs, "records": records, "initial_config": initial.as_dict()}
    result = {"tool": "IntegratedControllerRunner offline replay 2.0", "run_id": runner.run_id,
              "mode": "shadow" if recording_actuators is None else "recording_dry_run", "source_kind": source_kind,
              "live_submission_enabled": False, "strategies": names, "frame_count": len(inputs), "record_count": len(records),
              "identical_input_digests": all(len({row["integration"]["input_digest"] for row in records
                                                  if row["integration"]["decision_index"] == index}) == 1 for index in range(len(inputs))),
              "fairness": comparison, "stop_reasons": list(dict.fromkeys(stop_reasons)),
              "halted_strategies": halted, "observations": inputs, "records": records,
              "input_pins": dict(input_pins or {}), "summary": _summary(records, inputs, names, source_kind),
              "deterministic_signature": _digest(_semantic(signature_payload)),
              "signature_note": "Execution timings excluded; floats rounded to 10 decimals only for the signature, never for decisions."}
    result["status"] = ("completed_with_errors" if result["summary"]["input_validation_errors"] or halted or stop_reasons or
        any(item["decision_validation_errors"] or item["controller_errors"] or item["actuator_errors"]
            for item in result["summary"]["strategies"].values()) else "completed")
    folder = Path(__file__).parent
    result["implementation_sha256"] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest().upper()
        for path in [*(folder / name for name in ("common_replay.py", "live_integration.py", "offline_replay.py",
            "base.py", "strategies.py", "harness.py", "reactive_pipeline.py", "predictive_pipeline.py", "adapters.py", "telemetry.py")),
            folder.parent / "models/artifacts.py", folder.parent / "models/baseline.py", folder.parent / "forecasting/horizon_benchmark.py"]}
    if output_dir is not None:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=False)
        for filename, rows in (("observations.jsonl", inputs), ("controller_decisions.jsonl", records)):
            with (out / filename).open("x", encoding="utf-8", newline="\n") as stream:
                for row in rows:
                    stream.write(json.dumps(row, sort_keys=True, allow_nan=False) + "\n")
        for filename, value in (("summary.json", result["summary"]),
                                ("integration_manifest.json", {key: value for key, value in result.items() if key not in {"records", "observations"}})):
            with (out / filename).open("x", encoding="utf-8") as stream:
                json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
                stream.write("\n")
    return result
