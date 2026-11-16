"""Safe integration foundation for live controller experiments.

The module deliberately keeps controller decisions side-effect free until a
caller explicitly supplies verified live actuators and opts out of shadow
mode. It provides one timestamped frame contract for deterministic replay,
strict freshness/leakage checks, identical controller inputs, stop conditions,
and machine-readable timing evidence.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol

from .base import BaseController, ControllerState, FabricConfig, coerce_config
from .harness import (
    ApplyResult,
    ConfigurationActuator,
    ControllerExperimentHarness,
    ShadowActuator,
    VerifiedCallableActuator,
)
from .telemetry import (
    ContinuousCaliperLedgerAdapter,
    FabricTelemetrySnapshot,
    PrometheusTelemetryAdapter,
    TelemetryDataError,
    caliper_snapshot_from_summary,
)


class IntegrationSafetyError(ValueError):
    """Raised when the integration contract cannot safely continue."""


def _parse_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise IntegrationSafetyError(f"invalid UTC timestamp {value!r}") from exc
    if parsed.tzinfo is None:
        raise IntegrationSafetyError(f"timestamp must include timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class Clock(Protocol):
    def monotonic_ns(self) -> int: ...

    def utc_now(self) -> str: ...


class SystemClock:
    """UTC wall time plus monotonic duration source for one runner."""

    def monotonic_ns(self) -> int:
        return time.monotonic_ns()

    def utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass(frozen=True)
class TimestampContract:
    clock_source: str = "UTC wall clock + process monotonic_ns"
    timezone: str = "UTC"
    utc_precision: str = "milliseconds"
    monotonic_precision: str = "nanoseconds"

    def as_dict(self) -> dict[str, str]:
        return {
            "clock_source": self.clock_source,
            "timezone": self.timezone,
            "utc_precision": self.utc_precision,
            "monotonic_precision": self.monotonic_precision,
        }


@dataclass(frozen=True)
class TelemetryFrame:
    """One decision opportunity shared by every controller strategy."""

    run_id: str
    decision_index: int
    observation_timestamp_utc: str
    observation_monotonic_ns: int
    state: ControllerState
    source_timestamps_utc: Mapping[str, str] = field(default_factory=dict)
    freshness_seconds: Mapping[str, float | None] = field(default_factory=dict)
    provenance: Mapping[str, Any] = field(default_factory=dict)
    critical_missing: tuple[str, ...] = ()
    inconsistencies: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any], *, run_id: str, decision_index: int,
                     observation_monotonic_ns: int | None = None) -> "TelemetryFrame":
        if not isinstance(row, Mapping):
            raise IntegrationSafetyError("telemetry row must be a mapping")
        state_payload = row.get("state", row)
        if not isinstance(state_payload, Mapping):
            raise IntegrationSafetyError("telemetry frame state must be a mapping")
        state = ControllerState.from_mapping(state_payload)
        observation = str(row.get("observation_timestamp_utc", row.get("capture_timestamp_utc", state.timestamp_utc)))
        sources = row.get("source_timestamps_utc", {})
        freshness = row.get("freshness_seconds", {})
        if not isinstance(sources, Mapping) or not isinstance(freshness, Mapping):
            raise IntegrationSafetyError("source_timestamps_utc and freshness_seconds must be mappings")
        return cls(
            run_id=run_id,
            decision_index=int(decision_index),
            observation_timestamp_utc=observation,
            observation_monotonic_ns=int(observation_monotonic_ns if observation_monotonic_ns is not None else time.monotonic_ns()),
            state=state,
            source_timestamps_utc={str(k): str(v) for k, v in sources.items()},
            freshness_seconds={str(k): (None if v is None else float(v)) for k, v in freshness.items()},
            provenance=dict(row.get("provenance", {})) if isinstance(row.get("provenance", {}), Mapping) else {},
            critical_missing=tuple(str(v) for v in row.get("critical_missing", ())),
            inconsistencies=tuple(str(v) for v in row.get("inconsistencies", ())),
        )

    def input_payload(self) -> dict[str, Any]:
        return {
            "observation_timestamp_utc": self.observation_timestamp_utc,
            "state": self.state.as_dict(),
            "source_timestamps_utc": dict(self.source_timestamps_utc),
            "freshness_seconds": dict(self.freshness_seconds),
        }

    @property
    def input_digest(self) -> str:
        return _canonical_digest(self.input_payload())

    def validate(self, *, required_fields: Iterable[str] = (), max_age_s: float = 5.0) -> None:
        if not self.run_id:
            raise IntegrationSafetyError("telemetry frame has no run_id")
        if self.decision_index < 0:
            raise IntegrationSafetyError("decision_index must be non-negative")
        observation = _parse_utc(self.observation_timestamp_utc)
        if self.critical_missing:
            raise IntegrationSafetyError(f"critical telemetry missing: {sorted(self.critical_missing)}")
        if self.inconsistencies:
            raise IntegrationSafetyError(f"inconsistent telemetry: {sorted(self.inconsistencies)}")
        if self.state.telemetry_age_s is not None and self.state.telemetry_age_s > max_age_s:
            raise IntegrationSafetyError(f"telemetry is stale by {self.state.telemetry_age_s:.3f}s")
        for name, age in self.freshness_seconds.items():
            if age is None or age > max_age_s:
                raise IntegrationSafetyError(f"telemetry source {name} is stale or unavailable: {age!r}")
        for name, value in self.source_timestamps_utc.items():
            if _parse_utc(value) > observation:
                raise IntegrationSafetyError(f"source timestamp {name} is after observation timestamp")
        for field_name in required_fields:
            if field_name not in {"current_config", "timestamp_utc"} and getattr(self.state, field_name, None) is None:
                raise IntegrationSafetyError(f"required telemetry field is missing: {field_name}")
        history_timestamps = self.state.arrival_history_timestamps_utc
        if history_timestamps:
            parsed = [_parse_utc(value) for value in history_timestamps]
            if parsed != sorted(parsed):
                raise IntegrationSafetyError("arrival history timestamps are not chronological")
            origin = _parse_utc(self.state.timestamp_utc)
            if parsed[-1] > origin:
                raise IntegrationSafetyError("arrival history contains future data")

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "decision_index": self.decision_index,
            "observation_timestamp_utc": self.observation_timestamp_utc,
            "observation_monotonic_ns": self.observation_monotonic_ns,
            "state": self.state.as_dict(),
            "source_timestamps_utc": dict(self.source_timestamps_utc),
            "freshness_seconds": dict(self.freshness_seconds),
            "provenance": dict(self.provenance),
            "critical_missing": list(self.critical_missing),
            "inconsistencies": list(self.inconsistencies),
            "input_digest": self.input_digest,
        }


@dataclass(frozen=True)
class StopPolicy:
    max_queue_depth: float | None = None
    max_failed_transactions: int | None = None
    stop_on_actuation_failure: bool = True

    def reasons(self, state: ControllerState) -> list[str]:
        reasons: list[str] = []
        if self.max_queue_depth is not None and state.queue_depth is not None and state.queue_depth > self.max_queue_depth:
            reasons.append("backlog_limit_exceeded")
        if self.max_failed_transactions is not None and state.failed_transactions > self.max_failed_transactions:
            reasons.append("failure_limit_exceeded")
        return reasons


class TelemetryFrameProvider(Protocol):
    def collect(self, *, run_id: str, decision_index: int) -> TelemetryFrame: ...


def _metric_to_state_field(name: str) -> str | None:
    return {
        "offered_tps": "observed_arrival_rate_tps",
        "delivered_valid_tps": "observed_throughput_tps",
        "latency_mean_ms": "observed_latency_ms",
        "inflight_backlog": "queue_depth",
    }.get(name)


def ledger_observation_from_blocks_csv(path: Path, *, timestamp_utc: str | None = None) -> dict[str, Any]:
    """Read the latest verified ledger block count without inventing metrics."""
    import csv

    rows = []
    with Path(path).open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise TelemetryDataError(f"ledger block file is empty: {path}")
    row = rows[-1]
    try:
        number = int(row["number"])
        count = int(row["tx_count"])
    except (KeyError, TypeError, ValueError) as exc:
        raise TelemetryDataError(f"ledger block file lacks numeric number/tx_count: {path}") from exc
    return {
        "block_number": number,
        "block_transaction_count": count,
        "timestamp_utc": timestamp_utc,
        "source": str(path),
    }


def load_direct_cut_events(path: Path, *, channel_id: str) -> list[dict[str, Any]]:
    """Load only explicitly logged MESSAGE_COUNT/TIMEOUT events.

    Rows with inferred causes, missing joins, wrong channels or unsupported
    reasons are rejected rather than converted into a guessed cut reason.
    """
    import csv

    required = {"channel", "block_number", "reason", "event_ms", "join_confirmed", "source"}
    events: list[dict[str, Any]] = []
    with Path(path).open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise TelemetryDataError(f"direct cut evidence missing columns: {sorted(missing)}")
        for line_no, row in enumerate(reader, 2):
            if row["channel"] != channel_id:
                continue
            reason = row["reason"]
            if reason not in {"MESSAGE_COUNT", "TIMEOUT"}:
                raise TelemetryDataError(f"unsupported direct cut reason at line {line_no}: {reason!r}")
            if row["join_confirmed"].lower() != "true":
                raise TelemetryDataError(f"unconfirmed direct cut join at line {line_no}")
            try:
                block_number = int(row["block_number"])
                event_ms = float(row["event_ms"])
            except (TypeError, ValueError) as exc:
                raise TelemetryDataError(f"invalid direct cut event coordinates at line {line_no}") from exc
            events.append({
                "channel": channel_id,
                "block_number": block_number,
                "reason": reason,
                "event_ms": event_ms,
                "source": row["source"],
                "line": int(row.get("line") or line_no),
            })
    return events


class ProductionTelemetryProvider:
    """Read-only provider joining actual Prometheus and recorded Caliper/ledger sources.

    Caliper summaries are accepted only with their own provenance timestamp;
    absent live Caliper/ledger data remains explicitly missing. The provider
    never derives client latency or backlog from Prometheus counters.
    """

    def __init__(self, prometheus: PrometheusTelemetryAdapter, channel_id: str,
                 *, config_reader: Callable[[], FabricConfig | None] | None = None,
                 caliper_summary_path: Path | None = None,
                 continuous_source: ContinuousCaliperLedgerAdapter | None = None,
                 ledger_reader: Callable[[], Mapping[str, Any] | None] | None = None,
                 arrival_history_reader: Callable[[], tuple[Iterable[str], Iterable[float]]] | None = None,
                 max_source_skew_s: float = 5.0,
                 required_fields: Iterable[str] = ("observed_arrival_rate_tps", "observed_throughput_tps",
                                                    "observed_latency_ms", "queue_depth")):
        if max_source_skew_s <= 0:
            raise ValueError("max_source_skew_s must be positive")
        self.prometheus = prometheus
        self.channel_id = channel_id
        self.config_reader = config_reader
        self.caliper_summary_path = Path(caliper_summary_path) if caliper_summary_path else None
        if self.caliper_summary_path is not None and continuous_source is not None:
            raise ValueError("choose caliper_summary_path or continuous_source, not both")
        self.continuous_source = continuous_source
        self.ledger_reader = ledger_reader
        self.arrival_history_reader = arrival_history_reader
        self.max_source_skew_s = float(max_source_skew_s)
        self.required_fields = tuple(required_fields)

    def collect(self, *, run_id: str, decision_index: int, observation_timestamp_utc: str | None = None) -> TelemetryFrame:
        observed = observation_timestamp_utc or datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        errors: list[str] = []
        config = None
        if self.config_reader is not None:
            try:
                config = self.config_reader()
            except Exception as exc:
                errors.append(f"configuration_read_error:{type(exc).__name__}:{exc}")

        caliper: FabricTelemetrySnapshot | None = None
        if self.continuous_source is not None:
            try:
                caliper = self.continuous_source.collect(observation_timestamp_utc=observed)
            except Exception as exc:
                errors.append(f"continuous_caliper_read_error:{type(exc).__name__}:{exc}")
        elif self.caliper_summary_path is not None:
            if self.caliper_summary_path.is_file():
                try:
                    caliper = caliper_snapshot_from_summary(self.caliper_summary_path, channel_id=self.channel_id)
                except Exception as exc:
                    errors.append(f"caliper_read_error:{type(exc).__name__}:{exc}")
            else:
                errors.append(f"caliper_summary_missing:{self.caliper_summary_path}")

        ledger: Mapping[str, Any] | None = None
        if self.ledger_reader is not None:
            try:
                ledger = self.ledger_reader()
            except Exception as exc:
                errors.append(f"ledger_read_error:{type(exc).__name__}:{exc}")

        snapshot = self.prometheus.collect(
            self.channel_id, timestamp_utc=observed, current_config=config,
            caliper=caliper, ledger=ledger,
            health={"provider": "ProductionTelemetryProvider", "read_errors": errors},
        )
        history_timestamps: Iterable[str] = ()
        history_values: Iterable[float] = ()
        if self.arrival_history_reader is not None:
            history_timestamps, history_values = self.arrival_history_reader()
        state = snapshot.to_controller_state(
            arrival_history_tps=history_values,
            arrival_history_timestamps_utc=history_timestamps,
            now_utc=observed,
            require_fresh=False,
        )

        source_timestamps: dict[str, str] = {"observation": observed}
        source_values: list[datetime] = [_parse_utc(observed)]
        for name, metric in snapshot.metrics.items():
            if metric.value is not None:
                source_timestamps[name] = metric.timestamp_utc
                source_values.append(_parse_utc(metric.timestamp_utc))
        if caliper is not None:
            source_timestamps["caliper"] = caliper.timestamp_utc
        if ledger and ledger.get("timestamp_utc"):
            source_timestamps["ledger"] = str(ledger["timestamp_utc"])
            source_values.append(_parse_utc(str(ledger["timestamp_utc"])))
        freshness = {
            name: max(0.0, (_parse_utc(observed) - _parse_utc(value)).total_seconds())
            for name, value in source_timestamps.items()
        }
        missing = list(errors)
        for field_name in self.required_fields:
            if field_name == "current_config":
                if config is None:
                    missing.append("current_config")
            elif getattr(state, field_name, None) is None:
                missing.append(field_name)
        inconsistencies: list[str] = []
        if source_values and (max(source_values) - min(source_values)).total_seconds() > self.max_source_skew_s:
            inconsistencies.append("source_timestamp_skew_exceeds_limit")
        leaders = snapshot.metrics.get("raft_leader_count")
        if leaders is not None and leaders.value is not None and leaders.value != 1:
            inconsistencies.append("expected_exactly_one_raft_leader")
        health_metric = snapshot.metrics.get("orderer_peer_health")
        if health_metric is not None and health_metric.value == 0:
            missing.append("orderer_peer_health")
        provenance = {
            "channel_id": self.channel_id,
            "prometheus_endpoints": dict(sorted(self.prometheus.endpoints.items())),
            "caliper_summary": None if self.caliper_summary_path is None else str(self.caliper_summary_path),
            "continuous_caliper_source": None if self.continuous_source is None else {
                "recorder": str(self.continuous_source.recorder.path),
                "submits": str(self.continuous_source.submits.path),
                "ledger": str(self.continuous_source.ledger.path),
            },
            "ledger_source": None if not ledger else ledger.get("source"),
            "missing_sources": list(errors),
            "telemetry_health": dict(snapshot.health),
            "metrics": {name: metric.as_dict() for name, metric in snapshot.metrics.items()},
        }
        return TelemetryFrame(
            run_id=run_id, decision_index=decision_index,
            observation_timestamp_utc=observed,
            observation_monotonic_ns=time.monotonic_ns(), state=state,
            source_timestamps_utc=source_timestamps,
            freshness_seconds=freshness, provenance=provenance,
            critical_missing=tuple(sorted(set(missing))),
            inconsistencies=tuple(sorted(set(inconsistencies))),
        )


class IndependentConfigurationVerifier:
    """Adapt a read-only active-config inspector to ``VerifiedCallableActuator``.

    ``inspect_current`` must perform a fresh Fabric channel-config retrieval;
    a cached controller state is not accepted. An optional transition validator
    can call the existing `validate_batch_size_transition` implementation with
    the recorded before block and freshly fetched after block.
    """

    def __init__(self, inspect_current: Callable[[], Mapping[str, Any]], *,
                 transition_validator: Callable[[Any, Any, FabricConfig], Mapping[str, Any]]):
        if transition_validator is None:
            raise ValueError("independent configuration verifier requires a transition_validator")
        self.inspect_current = inspect_current
        self.transition_validator = transition_validator

    def __call__(self, update_record: Any, requested: FabricConfig) -> dict[str, Any]:
        started = time.monotonic_ns()
        try:
            current = self.inspect_current()
            if not isinstance(current, Mapping):
                raise IntegrationSafetyError("independent inspector returned a non-mapping result")
            active_raw = current.get("active_config", current)
            active = coerce_config(active_raw)
            after_block = int(current.get("block_number", -1))
            before_block = int(update_record.get("before_block_number", -1) if isinstance(update_record, Mapping) else getattr(update_record, "before_block_number", -1))
            checks: dict[str, Any] = {
                "active_config_matches_requested": active == requested,
                "later_configuration_block": after_block > before_block,
            }
            recorded_checks = update_record.get("verification_checks", {}) if isinstance(update_record, Mapping) else getattr(update_record, "verification_checks", {})
            if recorded_checks:
                checks["recorded_transition_checks"] = all(bool(value) for value in recorded_checks.values())
            if self.transition_validator is not None:
                checks["independent_transition_checks"] = dict(self.transition_validator(update_record, current, requested))
                checks["independent_transition_valid"] = all(bool(value) for value in checks["independent_transition_checks"].values())
            success = all(bool(value) for value in checks.values())
            return {
                "success": success,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "after_config": active.as_dict(),
                "after_block_number": after_block,
                "verification_checks": checks,
                "commit_to_independent_verification_ms": (time.monotonic_ns() - started) / 1_000_000,
                "error": None if success else "independent active-configuration verification failed",
            }
        except Exception as exc:
            return {
                "success": False,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "verification_checks": {"verifier_exception": False},
                "commit_to_independent_verification_ms": (time.monotonic_ns() - started) / 1_000_000,
                "error": f"{type(exc).__name__}: {exc}",
            }


@dataclass(frozen=True)
class LatencyEvidence:
    run_id: str
    decision_index: int
    timestamps: Mapping[str, str | None]
    durations_ms: Mapping[str, float | None]
    clock: TimestampContract
    notes: tuple[str, ...] = ()

    @staticmethod
    def _delta(start: str | None, end: str | None) -> float | None:
        if not start or not end:
            return None
        return (_parse_utc(end) - _parse_utc(start)).total_seconds() * 1000.0

    @classmethod
    def from_record(cls, *, run_id: str, decision_index: int, decision_start: str,
                    decision_end: str, application: Mapping[str, Any] | None,
                    decision: Mapping[str, Any], clock: TimestampContract) -> "LatencyEvidence":
        app = application or {}
        metadata = decision.get("metadata", {}) if isinstance(decision, Mapping) else {}
        forecast = metadata.get("forecast", {}) if isinstance(metadata, Mapping) else {}
        forecast_start = forecast.get("forecast_start_timestamp_utc")
        forecast_end = forecast.get("forecast_completion_timestamp_utc")
        timestamps = {
            "telemetry_observation": None,
            "forecast_start": forecast_start,
            "forecast_completion": forecast_end,
            "decision_start": decision_start,
            "decision_completion": decision_end,
            "submission_start": app.get("submission_start_timestamp_utc"),
            "submission_completion": app.get("submission_completion_timestamp_utc"),
            "config_block_commit_observation": app.get("config_block_commit_timestamp_utc"),
            "independent_verification_completion": app.get("independent_verification_timestamp_utc"),
        }
        durations = {
            "forecast_inference_ms": forecast.get("inference_duration_ms"),
            "controller_computation_ms": LatencyEvidence._delta(decision_start, decision_end),
            "decision_to_submission_ms": LatencyEvidence._delta(decision_end, timestamps["submission_start"]),
            "submission_to_commit_observation_ms": app.get("submission_to_commit_ms") or LatencyEvidence._delta(timestamps["submission_start"], timestamps["config_block_commit_observation"]),
            "commit_observation_to_independent_verification_ms": app.get("commit_to_independent_verification_ms") or LatencyEvidence._delta(timestamps["config_block_commit_observation"], timestamps["independent_verification_completion"]),
            "decision_to_independent_verification_ms": LatencyEvidence._delta(decision_start, timestamps["independent_verification_completion"]),
        }
        notes = ("shadow mode has no actuator submission timestamps",) if application is None else ()
        return cls(run_id, decision_index, timestamps, durations, clock, notes)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "decision_index": self.decision_index,
            "timestamps": dict(self.timestamps),
            "durations_ms": dict(self.durations_ms),
            "clock": self.clock.as_dict(),
            "notes": list(self.notes),
        }


# Public name used by the experiment manifests and protocol.
AdaptationLatencyEvidence = LatencyEvidence


class IntegratedControllerRunner:
    """Run all supplied strategies on one validated frame stream.

    Shadow mode is the default and always replaces supplied actuators with
    ``ShadowActuator`` instances. Live mode requires both ``shadow=False`` and
    ``enable_live=True``; it is intentionally not exposed by the default CLI.
    """

    def __init__(self, controllers: Mapping[str, BaseController], initial_config: FabricConfig,
                 *, run_id: str, shadow: bool = True, enable_live: bool = False,
                 actuators: Mapping[str, ConfigurationActuator] | None = None,
                 configuration_policy: Any | None = None,
                 required_fields: Iterable[str] = (), max_telemetry_age_s: float = 5.0,
                 stop_policy: StopPolicy | None = None, clock: Clock | None = None):
        if not controllers:
            raise ValueError("at least one controller is required")
        if not shadow and not enable_live:
            raise IntegrationSafetyError("live actuation requires explicit enable_live=True")
        self.controllers = dict(controllers)
        self.initial_config = initial_config
        self.run_id = run_id
        self.shadow = shadow
        self.enable_live = enable_live
        self.required_fields = tuple(required_fields)
        self.max_telemetry_age_s = max_telemetry_age_s
        self.stop_policy = stop_policy or StopPolicy()
        self.clock = clock or SystemClock()
        self.clock_contract = TimestampContract()
        self.configuration_policy = configuration_policy
        if shadow:
            self.actuators = {name: ShadowActuator(initial_config) for name in self.controllers}
        else:
            if actuators is None or set(actuators) != set(self.controllers):
                raise IntegrationSafetyError("live mode requires one actuator per controller")
            if any(not isinstance(actuator, VerifiedCallableActuator) or actuator.verify_fn is None
                   or not isinstance(actuator.verify_fn, IndependentConfigurationVerifier)
                   or actuator.verify_fn.transition_validator is None
                   or not actuator.require_verified_result for actuator in actuators.values()):
                raise IntegrationSafetyError("live mode requires VerifiedCallableActuator with the mandatory IndependentConfigurationVerifier and transition validator for every strategy")
            self.actuators = dict(actuators)
        self.harnesses = {
            name: ControllerExperimentHarness(
                controller, self.actuators[name], run_id=f"{run_id}-{name}",
                configuration_policy=configuration_policy,
            )
            for name, controller in self.controllers.items()
        }

    def run(self, frames: Iterable[TelemetryFrame], *, output_dir: Path | None = None) -> dict[str, Any]:
        frames = list(frames)
        if not frames:
            raise ValueError("telemetry frame stream is empty")
        all_records: list[dict[str, Any]] = []
        latency_records: list[dict[str, Any]] = []
        validation_records: list[dict[str, Any]] = []
        stop_reasons: list[str] = []
        input_digests_by_frame: dict[int, set[str]] = {}
        for frame in frames:
            if frame.run_id != self.run_id:
                raise IntegrationSafetyError("frame run_id does not match runner run_id")
            try:
                frame.validate(required_fields=self.required_fields, max_age_s=self.max_telemetry_age_s)
                reasons = self.stop_policy.reasons(frame.state)
                if reasons:
                    stop_reasons.extend(reasons)
                    validation_records.append({"decision_index": frame.decision_index, "status": "stopped", "reasons": reasons})
                    break
            except IntegrationSafetyError as exc:
                validation_records.append({"decision_index": frame.decision_index, "status": "rejected", "error": str(exc)})
                stop_reasons.append("telemetry_validation_failed")
                break

            for name in sorted(self.controllers):
                # Each strategy gets a fresh copy of precisely the same frame.
                state = ControllerState.from_mapping(frame.state.as_dict())
                decision_start = self.clock.utc_now()
                decision_start_ns = self.clock.monotonic_ns()
                record = self.harnesses[name].process(state)
                decision_end = self.clock.utc_now()
                decision_end_ns = self.clock.monotonic_ns()
                record["integration"] = {
                    "run_id": self.run_id,
                    "strategy": name,
                    "decision_index": frame.decision_index,
                    "input_digest": frame.input_digest,
                    "decision_start_timestamp_utc": decision_start,
                    "decision_completion_timestamp_utc": decision_end,
                    "decision_monotonic_duration_ms": (decision_end_ns - decision_start_ns) / 1_000_000,
                    "shadow_mode": self.shadow,
                    "live_submission_enabled": bool(not self.shadow and self.enable_live),
                    "timestamp_contract": self.clock_contract.as_dict(),
                }
                record["latency"] = LatencyEvidence.from_record(
                    run_id=self.run_id, decision_index=frame.decision_index,
                    decision_start=decision_start, decision_end=decision_end,
                    application=record.get("application"), decision=record["decision"],
                    clock=self.clock_contract,
                ).as_dict()
                record["latency"]["timestamps"]["telemetry_observation"] = frame.observation_timestamp_utc
                input_digests_by_frame.setdefault(frame.decision_index, set()).add(frame.input_digest)
                all_records.append(record)
                latency_records.append(record["latency"])
                application = record.get("application")
                if application and not application.get("accepted", False) and self.stop_policy.stop_on_actuation_failure and not self.shadow:
                    stop_reasons.append("actuation_failed_or_unverified")
                    break
            if stop_reasons and not self.shadow:
                break

        result = {
            "tool": "IntegratedControllerRunner 1.0",
            "run_id": self.run_id,
            "mode": "shadow" if self.shadow else "live",
            "live_submission_enabled": bool(not self.shadow and self.enable_live),
            "strategies": sorted(self.controllers),
            "frame_count": len({r["integration"]["decision_index"] for r in all_records}),
            "record_count": len(all_records),
            "identical_input_digests": all(len(values) == 1 for values in input_digests_by_frame.values()),
            "stop_reasons": stop_reasons,
            "clock": self.clock_contract.as_dict(),
            "records": all_records,
            "latency_records": latency_records,
            "validation_records": validation_records,
        }
        if output_dir is not None:
            self._write_outputs(Path(output_dir), result)
        return result

    def run_provider(self, provider: TelemetryFrameProvider, *, samples: int,
                     interval_s: float = 0.0, output_dir: Path | None = None) -> dict[str, Any]:
        """Collect a bounded live-source stream, then execute it in shadow mode.

        The provider is sampled before any controller decision is made for the
        corresponding index. A finite sample count is required so a production
        validation cannot become an unbounded workload or actuation process.
        """
        if samples < 1:
            raise ValueError("samples must be positive")
        if interval_s < 0:
            raise ValueError("interval_s must be non-negative")
        frames: list[TelemetryFrame] = []
        for index in range(samples):
            if index and interval_s:
                time.sleep(interval_s)
            frames.append(provider.collect(run_id=self.run_id, decision_index=index))
        return self.run(frames, output_dir=output_dir)

    def _write_outputs(self, output_dir: Path, result: Mapping[str, Any]) -> None:
        if output_dir.exists():
            raise FileExistsError(f"refusing to overwrite {output_dir}")
        output_dir.mkdir(parents=True)
        (output_dir / "integration_manifest.json").write_text(json.dumps({k: v for k, v in result.items() if k not in {"records", "latency_records"}}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        with (output_dir / "controller_decisions.jsonl").open("x", encoding="utf-8", newline="\n") as fh:
            for row in result["records"]:
                fh.write(json.dumps(row, sort_keys=True) + "\n")
        with (output_dir / "adaptation_latency.jsonl").open("x", encoding="utf-8", newline="\n") as fh:
            for row in result["latency_records"]:
                fh.write(json.dumps(row, sort_keys=True) + "\n")
        (output_dir / "validation_evidence.json").write_text(json.dumps({"validation_records": result["validation_records"], "stop_reasons": result["stop_reasons"]}, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_jsonl_frames(path: Path, *, run_id: str) -> list[TelemetryFrame]:
    """Load a deterministic replay stream and reject future-data fields."""
    frames: list[TelemetryFrame] = []
    with Path(path).open(encoding="utf-8") as fh:
        for index, line in enumerate(fh):
            if not line.strip():
                continue
            row = json.loads(line)
            forbidden = {"future", "future_arrivals", "future_observations", "actual_future"} & set(row)
            if forbidden:
                raise IntegrationSafetyError(f"future-data fields present at line {index + 1}: {sorted(forbidden)}")
            frames.append(TelemetryFrame.from_mapping(row, run_id=run_id, decision_index=len(frames)))
    if not frames:
        raise ValueError(f"telemetry snapshot file is empty: {path}")
    return frames

