"""Strict reactive telemetry ingestion and offline/shadow audit records.

No Fabric client, model, workload generator or live actuator is constructed.
Recorded state fields encode units in their canonical names. Snapshot ingestion
uses the existing MetricObservation unit and timestamp contract.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping

from .base import ControllerState, confirmed_config
from .harness import ControllerExperimentHarness, RecordingActuator, ShadowActuator
from .strategies import ReactiveController
from .telemetry import FabricTelemetrySnapshot, TelemetryDataError, parse_observation_time


STATE_UNITS = {"observed_latency_ms": "ms", "observed_latency_p95_ms": "ms", "queue_depth": "transactions",
               "observed_arrival_rate_tps": "transactions/s", "observed_throughput_tps": "VALID transactions/s",
               "telemetry_age_s": "s", "failed_transactions": "transactions"}


def _audit(value: Any) -> Any:
    """Retain invalid input as explicit text instead of nonstandard JSON NaN."""
    if isinstance(value, float) and not math.isfinite(value):
        return {"invalid_numeric_value": repr(value)}
    if isinstance(value, Mapping):
        return {str(key): _audit(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_audit(item) for item in value]
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    return str(value)


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(_audit(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


class ReactiveTelemetryPipeline:
    """Use recorded observations or existing snapshots; shadow is the default.

    RecordingActuator is permitted only for explicit in-memory contract tests.
    It does not generate subsequent performance observations.
    """

    def __init__(self, controller: ReactiveController, *, run_id: str,
                 actuator: ShadowActuator | RecordingActuator | None = None):
        if not run_id:
            raise ValueError("run_id is required")
        self.controller = controller
        self.actuator = actuator if actuator is not None else ShadowActuator(controller.default_config)
        if type(self.actuator) not in {ShadowActuator, RecordingActuator}:
            raise ValueError("reactive replay permits only shadow or in-memory recording actuators")
        if self.actuator.current_config != controller._verified_config:
            raise ValueError("initial actuator and controller configurations disagree")
        self.harness = ControllerExperimentHarness(controller, self.actuator, run_id=run_id)
        self.run_id = run_id
        self.index = 0
        self._source_times: dict[str, Any] = {}
        self._implementation_sha256 = {
            name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest().upper()
            for name in ("reactive_pipeline.py", "strategies.py", "telemetry.py", "harness.py", "base.py")
        }

    def _check_source_progress(self, state: ControllerState, timestamps: Mapping[str, str]) -> None:
        """A fresh capture of an unchanged metric is not another measurement."""
        values = {"observed_latency_ms": state.observed_latency_ms}
        if self.controller.queue_high is not None:
            values["queue_depth"] = state.queue_depth
        times = {field: parse_observation_time(timestamps.get(field, state.timestamp_utc))
                 for field, value in values.items() if value is not None}
        hits = [field for field, value in values.items() if value is not None and value >= (
            self.controller.latency_high_ms if field == "observed_latency_ms" else self.controller.queue_high)]
        new = {field: field not in self._source_times or timestamp > self._source_times[field]
               for field, timestamp in times.items()}
        # Congestion is OR: one newly measured high signal is sufficient.
        # Recovery is AND: every supplied recovery input must advance.
        if hits and not any(new[field] for field in hits):
            raise TelemetryDataError("congestion evidence is duplicate or out of order")
        if not hits and new and not all(new.values()):
            raise TelemetryDataError("recovery/normal evidence is duplicate or out of order")
        for field, timestamp in times.items():
            if new[field]:
                self._source_times[field] = timestamp

    def _record(self, observation: Any, state: ControllerState | None, error: str | None,
                *, source: str) -> dict:
        before = self.controller.policy_metadata()
        if error is not None:
            decision = self.controller.reject_observation("invalid_telemetry_fail_safe", error=error)
            record = {"run_id": self.run_id, "state": None, "decision": decision.as_dict(), "application": None,
                      "configuration_state": {"after": None if self.harness.remote_state_unknown else self.actuator.current_config.as_dict(),
                                              "remote_state_unknown": self.harness.remote_state_unknown, "proposal_applied": False}}
        else:
            record = self.harness.process(state)
        # ControllerState retains legacy defaults for shared callers. Do not
        # present its absent failure-count default as an observed zero here.
        if isinstance(observation, Mapping) and "snapshot" in observation:
            failure_metric = observation["snapshot"].get("metrics", {}).get("failed_transactions", {})
            failure_count_missing = failure_metric.get("value") is None
        else:
            payload = observation.get("state", observation) if isinstance(observation, Mapping) else {}
            failure_count_missing = not isinstance(payload, Mapping) or payload.get("failed_transactions") is None
        if record["state"] is not None and failure_count_missing:
            record["state"]["failed_transactions"] = None
        record["audit"] = {
            "decision_index": self.index, "source": source, "input_sha256": _digest(observation),
            "observation": _audit(observation), "units": dict(STATE_UNITS),
            "unavailable_fields": ["failed_transactions"] if failure_count_missing else [],
            "validation": {"schema_accepted": error is None,
                           "accepted": error is None and record["decision"]["outcome"] != "abstain",
                           "error": error or (record["decision"]["reason"] if record["decision"]["outcome"] == "abstain" else None)},
            "policy_and_state_before": before, "policy_and_state_after": self.controller.policy_metadata(),
            "mode": "shadow" if type(self.actuator) is ShadowActuator else "in_memory_contract_test",
            "live_submission_enabled": False,
            "implementation_sha256": dict(self._implementation_sha256),
        }
        self.index += 1
        return record

    def process_record(self, observation: Mapping[str, Any], *, source: str = "recorded state") -> dict:
        state = None
        error = None
        try:
            if not isinstance(observation, Mapping):
                raise TelemetryDataError("record must be a mapping")
            if {"future", "future_arrivals", "future_observations", "actual_future"} & set(observation):
                raise TelemetryDataError("future observations cannot be controller inputs")
            payload = observation.get("state", observation)
            if not isinstance(payload, Mapping):
                raise TelemetryDataError("recorded state must be a mapping")
            for field in ("timestamp_utc", "current_config", "telemetry_age_s"):
                if field not in payload or payload[field] is None:
                    raise TelemetryDataError(f"recorded state requires {field}; no default is substituted")
            parse_observation_time(payload["timestamp_utc"])
            if {"current_latency", "current_latency_ms", "arrival_rate", "tps", "backlog"} & set(payload):
                raise TelemetryDataError("recorded telemetry must use canonical fields with explicit unit suffixes")
            config = confirmed_config(payload["current_config"])
            if config != self.actuator.current_config or config != self.controller._verified_config:
                raise TelemetryDataError("recorded configuration differs from actuator-confirmed state")
            units = observation.get("units", {})
            if not isinstance(units, Mapping):
                raise TelemetryDataError("units must be a mapping")
            for field, unit in units.items():
                if field in STATE_UNITS and unit != STATE_UNITS[field]:
                    raise TelemetryDataError(f"{field} must use {STATE_UNITS[field]}")
            if observation.get("critical_missing") or observation.get("inconsistencies"):
                raise TelemetryDataError("record contains missing critical telemetry or inconsistencies")
            for name, timestamp in observation.get("source_timestamps_utc", {}).items():
                age = (parse_observation_time(payload["timestamp_utc"]) - parse_observation_time(timestamp)).total_seconds()
                if age < 0 or age > self.controller.max_telemetry_age_s:
                    raise TelemetryDataError(f"source {name} is stale or in the future")
            for name, age in observation.get("freshness_seconds", {}).items():
                if isinstance(age, bool) or not isinstance(age, (int, float)) or not math.isfinite(age) or not 0 <= age <= self.controller.max_telemetry_age_s:
                    raise TelemetryDataError(f"source {name} has invalid or stale age")
            state = ControllerState.from_mapping(payload)
            if state.telemetry_age_s <= self.controller.max_telemetry_age_s:
                source_times = observation.get("source_timestamps_utc", {})
                self._check_source_progress(state, {
                    "observed_latency_ms": source_times.get("latency_mean_ms", source_times.get("observed_latency_ms", state.timestamp_utc)),
                    "queue_depth": source_times.get("inflight_backlog", source_times.get("queue_depth", state.timestamp_utc)),
                })
        except (ValueError, TypeError, AttributeError, KeyError) as exc:
            error = f"{type(exc).__name__}: {exc}"
        return self._record(observation, state, error, source=source)

    def process_snapshot(self, snapshot: FabricTelemetrySnapshot, *, now_utc: str | None = None) -> dict:
        state = None
        error = None
        try:
            if snapshot.health.get("read_errors") or snapshot.health.get("cross_source", {}).get("status_validation_contradictions"):
                raise TelemetryDataError("telemetry source reports errors or contradictory transaction status")
            for name in ("latency_mean_ms", "inflight_backlog"):
                metric = snapshot.metrics.get(name)
                if metric is not None and metric.value is not None and metric.incomplete_window:
                    raise TelemetryDataError(f"{name} measurement window is incomplete")
            state = snapshot.to_controller_state(now_utc=now_utc)
            if state.current_config != self.actuator.current_config or state.current_config != self.controller._verified_config:
                raise TelemetryDataError("observed configuration differs from actuator-confirmed state")
            self._check_source_progress(state, {
                field: snapshot.metrics[name].timestamp_utc
                for field, name in (("observed_latency_ms", "latency_mean_ms"), ("queue_depth", "inflight_backlog"))
                if name in snapshot.metrics
            })
        except (ValueError, TypeError, AttributeError) as exc:
            error = f"{type(exc).__name__}: {exc}"
        return self._record({"snapshot": snapshot.as_dict(), "evaluation_timestamp_utc": now_utc}, state, error,
                            source="FabricTelemetrySnapshot")


def replay_reactive_jsonl(path: Path, pipeline: ReactiveTelemetryPipeline, *, output_path: Path | None = None,
                          expected_sha256: str | None = None) -> list[dict]:
    """Read complete recorded rows; malformed rows abstain instead of disappearing."""
    path = Path(path)
    if output_path is not None and Path(output_path).exists():
        raise FileExistsError(f"refusing to overwrite {output_path}")
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest().upper()
    if expected_sha256 is not None and digest != expected_sha256.upper():
        raise TelemetryDataError("recorded telemetry SHA-256 mismatch")
    lines = data.decode("utf-8").splitlines()
    if not any(line.strip() for line in lines):
        raise TelemetryDataError("recorded telemetry file is empty")
    records = []
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError as exc:
            row = {"invalid_json_line": line, "parse_error": str(exc)}
        record = pipeline.process_record(row, source=f"{path}:{line_number}")
        record["audit"]["source_file_sha256"] = digest
        records.append(record)
    if output_path is not None:
        with Path(output_path).open("x", encoding="utf-8", newline="\n") as stream:
            for record in records:
                stream.write(json.dumps(record, sort_keys=True, allow_nan=False) + "\n")
    return records
