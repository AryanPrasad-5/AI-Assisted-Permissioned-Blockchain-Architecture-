"""Auditable offline predictive decisions using frozen recorded inputs.

Forecasts and HFBTP outputs are model predictions. No Fabric performance is
generated, inferred as a measurement, or conditioned on an unexecuted proposal.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import replace
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping

from .adapters import ApprovedMaxMessageCountSpace, FlashbotsSeriesForecaster, HFBTPArtifactPerformancePredictor
from .base import ControllerState, FabricConfig, confirmed_config
from .harness import ControllerExperimentHarness, RecordingActuator, ShadowActuator
from .offline_replay import verified_inputs, workload_frames
from .reactive_pipeline import _audit, _digest, STATE_UNITS
from .strategies import PredictiveController
from .telemetry import parse_observation_time


PREDICTIVE_INPUT_KEYS = ("series", "selection", "model_metadata", "throughput_model", "latency_model")
PREDICTIVE_UNITS = {**STATE_UNITS, "arrival_history_tps": "transactions/s"}


class PredictiveDecisionPipeline:
    """Strict recorded-state ingestion, shadow by default, no live actuator API."""

    def __init__(self, controller: PredictiveController, *, run_id: str,
                 actuator: ShadowActuator | RecordingActuator | None = None,
                 configuration_policy: ApprovedMaxMessageCountSpace | None = None,
                 input_pins: Mapping[str, Any] | None = None):
        if not run_id:
            raise ValueError("run_id is required")
        self.controller = controller
        self.actuator = actuator if actuator is not None else ShadowActuator(controller.default_config)
        if type(self.actuator) not in {ShadowActuator, RecordingActuator}:
            raise ValueError("predictive offline pipeline permits only shadow or in-memory recording actuators")
        if self.actuator.current_config != controller._verified_config:
            raise ValueError("initial actuator and controller configurations disagree")
        if configuration_policy is not None:
            configuration_policy.validate(self.actuator.current_config)
            for config in controller.candidate_configs:
                configuration_policy.validate(config)
        self.harness = ControllerExperimentHarness(controller, self.actuator, run_id=run_id,
                                                   configuration_policy=configuration_policy)
        self.run_id, self.index = run_id, 0
        self.input_pins = dict(input_pins or {})
        folder = Path(__file__).parent
        self.code_sha256 = {str(path.relative_to(folder.parent)): hashlib.sha256(path.read_bytes()).hexdigest().upper()
                           for path in [*(folder / name for name in ("predictive_pipeline.py", "strategies.py", "adapters.py",
                               "base.py", "harness.py", "offline_replay.py", "reactive_pipeline.py")),
                               folder.parent / "models/artifacts.py", folder.parent / "models/baseline.py",
                               folder.parent / "forecasting/horizon_benchmark.py"]}

    def process_record(self, observation: Mapping[str, Any], *, source: str = "recorded workload") -> dict:
        before = self.controller.policy_metadata()
        error = None
        state = None
        payload = {}
        try:
            if not isinstance(observation, Mapping):
                raise ValueError("observation must be a mapping")
            if {"future", "future_arrivals", "future_observations", "actual_future"} & set(observation):
                raise ValueError("future observations cannot be forecast inputs")
            payload = observation.get("state", observation)
            if not isinstance(payload, Mapping):
                raise ValueError("state must be a mapping")
            if {"future", "future_arrivals", "future_observations", "actual_future"} & set(payload):
                raise ValueError("future observations cannot be forecast inputs")
            for name in ("timestamp_utc", "current_config", "telemetry_age_s"):
                if name not in payload or payload[name] is None:
                    raise ValueError(f"recorded state requires {name}; no default is substituted")
            observed = parse_observation_time(payload["timestamp_utc"])
            config = confirmed_config(payload["current_config"])
            if config != self.actuator.current_config or config != self.controller._verified_config:
                raise ValueError("recorded configuration differs from actuator-confirmed state")
            if {"arrival_rate", "arrival_history", "tps", "current_latency", "backlog"} & set(payload):
                raise ValueError("recorded inputs must use canonical unit-labelled state fields")
            if observation.get("critical_missing") or observation.get("inconsistencies"):
                raise ValueError("record declares missing critical fields or inconsistencies")
            for field, unit in observation.get("units", {}).items():
                if field in PREDICTIVE_UNITS and unit != PREDICTIVE_UNITS[field]:
                    raise ValueError(f"{field} must use {PREDICTIVE_UNITS[field]}")
            for name, timestamp in observation.get("source_timestamps_utc", {}).items():
                age = (observed - parse_observation_time(timestamp)).total_seconds()
                if age < 0 or age > self.controller.max_telemetry_age_s:
                    raise ValueError(f"source {name} is stale or in the future")
            for name, age in observation.get("freshness_seconds", {}).items():
                if isinstance(age, bool) or not isinstance(age, (int, float)) or not math.isfinite(age) or not 0 <= age <= self.controller.max_telemetry_age_s:
                    raise ValueError(f"source {name} has invalid or stale age")
            state = ControllerState.from_mapping(payload)
        except (ValueError, TypeError, AttributeError, KeyError) as exc:
            error = f"{type(exc).__name__}: {exc}"
        if error is None:
            record = self.harness.process(state)
        else:
            decision = self.controller.reject_observation("invalid_telemetry_fail_safe", error=error)
            record = {"run_id": self.run_id, "state": None, "decision": decision.as_dict(), "application": None,
                      "configuration_state": {"after": None if self.harness.remote_state_unknown else self.actuator.current_config.as_dict(),
                                              "proposal_applied": False, "remote_state_unknown": self.harness.remote_state_unknown}}
        # Legacy ControllerState defaults do not establish observed Fabric failures.
        if record["state"] is not None and payload.get("failed_transactions") is None:
            record["state"]["failed_transactions"] = None
        record["decision"]["kind"] = record["decision"]["metadata"].get("decision_kind", record["decision"]["outcome"])
        record["audit"] = {
            "decision_index": self.index, "source": source, "observation": _audit(observation),
            "input_sha256": _digest(observation), "implementation_sha256": self.code_sha256,
            "input_pins": self.input_pins, "policy_and_state_before": before,
            "units": dict(PREDICTIVE_UNITS),
            "policy_and_state_after": self.controller.policy_metadata(),
            "schema_error": error, "mode": "shadow" if type(self.actuator) is ShadowActuator else "in_memory_contract_test",
            "live_submission_enabled": False,
            "predictions_are_fabric_measurements": False, "counterfactual_performance_evaluated": False,
        }
        self.index += 1
        return record


def frozen_predictive_pipeline(root: Path, manifest_path: Path, *, initial_batch_size: int = 10,
                               run_id: str = "frozen-predictive-replay") -> PredictiveDecisionPipeline:
    """Verify trusted local inputs before deserialization; require no Fabric recording."""
    paths = verified_inputs(root, manifest_path, required_keys=PREDICTIVE_INPUT_KEYS)
    entries = json.loads(manifest_path.read_text(encoding="utf-8"))["files"]
    initial = FabricConfig(max_message_count=initial_batch_size)
    space = ApprovedMaxMessageCountSpace(frozenset({10, 40}), {10: 10, 40: 40}, initial)
    forecaster = FlashbotsSeriesForecaster.from_frozen_selection(paths["series"], paths["selection"],
        selection_sha256=entries["selection"]["sha256"], series_sha256=entries["series"]["sha256"])
    predictor = HFBTPArtifactPerformancePredictor(paths["model_metadata"].parent, candidate_space=space)
    candidates = [replace(initial, max_message_count=size) for size in (10, 40)]
    for config in candidates:
        predictor.validate_config(config)
    controller = PredictiveController(predictor, initial, arrival_forecaster=forecaster,
                                      candidate_configs=candidates, horizon_seconds=60)
    return PredictiveDecisionPipeline(controller, run_id=run_id, configuration_policy=space,
                                      input_pins={key: entries[key] for key in PREDICTIVE_INPUT_KEYS})


def replay_frozen_predictive(pipeline: PredictiveDecisionPipeline, *, start: int = 43200,
                             samples: int = 10, output_path: Path | None = None) -> dict:
    """Forecast genuine daily prefixes; performance measurements remain absent."""
    if type(pipeline.actuator) is not ShadowActuator:
        raise ValueError("frozen observational replay is shadow only; in-memory transitions use explicit contract-test states")
    if output_path is not None and Path(output_path).exists():
        raise FileExistsError(f"refusing to overwrite {output_path}")
    if getattr(pipeline.controller.arrival_forecaster, "baseline", None) != "ewma_alpha_0.005" or pipeline.controller.horizon_seconds != 60:
        raise ValueError("this replay requires the frozen EWMA alpha=0.005, H=60 profile")
    records = []
    frames = workload_frames(pipeline.controller.arrival_forecaster, pipeline.actuator.current_config,
                             run_id=pipeline.run_id, start=start, samples=samples)
    for frame in frames:
        row = frame.state.as_dict()
        row.pop("failed_transactions")
        records.append(pipeline.process_record({"state": row, "provenance": frame.provenance},
                                               source=str(pipeline.controller.arrival_forecaster.series_path)))
    if output_path is not None:
        with Path(output_path).open("x", encoding="utf-8", newline="\n") as stream:
            for record in records:
                stream.write(json.dumps(record, sort_keys=True, allow_nan=False) + "\n")
    return {"mode": "shadow" if type(pipeline.actuator) is ShadowActuator else "in_memory_contract_test",
            "live_submission_enabled": False, "alpha": 0.005, "horizon_seconds": 60,
            "record_count": len(records), "outcomes": dict(Counter(row["decision"]["kind"] for row in records)),
            "reasons": dict(Counter(row["decision"]["reason"] for row in records)),
            "fabric_performance_measured": False, "records": records}
