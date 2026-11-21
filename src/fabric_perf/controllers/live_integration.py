"""Safe integration foundation for live controller experiments.

The module deliberately keeps controller decisions side-effect free until a
caller explicitly supplies verified live actuators and opts out of shadow
mode. It provides one timestamped frame contract for deterministic replay,
strict freshness/leakage checks, identical controller inputs, stop conditions,
and machine-readable timing evidence.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import time
from dataclasses import dataclass, field, replace as _dataclass_replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol

from .base import (
    BaseController,
    ControllerDecision,
    ControllerState,
    FabricConfig,
    StaticController,
    coerce_config,
    confirmed_config,
)
from .harness import (
    ApplyResult,
    ConfigurationActuator,
    ControllerExperimentHarness,
    ShadowActuator,
    VerifiedCallableActuator,
)
from .stage3_frame_quality import (
    INITIALIZATION_LIMIT_REASON_CODE,
    INITIALIZATION_REASON_CODE,
    THROUGHPUT_FIELD,
    Stage3FrameQualityContract,
)
from .telemetry import (
    ContinuousCaliperLedgerAdapter,
    FabricTelemetrySnapshot,
    PrometheusTelemetryAdapter,
    TelemetryDataError,
    caliper_snapshot_from_summary,
)
from .telemetry_errors import (
    TelemetryReadFailure,
    aggregate_read_status,
    bounded_details,
    failure_from_category,
    failure_from_exception,
    sanitize_text,
    sanitize_url,
)
from .telemetry_freshness import (
    ABSTENTION_REASON,
    ClockConsistencyMonitor,
    FreshnessPolicy,
    FreshnessStatus,
    assess_sources,
    classify_observation,
    parse_utc,
)


class IntegrationSafetyError(ValueError):
    """Raised when the integration contract cannot safely continue."""


# Controller abstention reasons for observation-quality problems. Each ends in ``fail_safe`` so that
# ``ControllerDecision.outcome`` reports "abstain". Freshness reasons live in ``ABSTENTION_REASON``.
ABSTAIN_MISSING = "telemetry_missing_fail_safe"
ABSTAIN_READ_FAILED = "telemetry_read_failed_fail_safe"
ABSTAIN_INCONSISTENT = "telemetry_inconsistent_fail_safe"
ABSTAIN_NONADVANCING = "nonadvancing_telemetry_fail_safe"
# Stage 3 warm-up (task A0d): the ledger journal has only its baseline, so no committed-throughput interval exists yet.
ABSTAIN_INITIALIZING = "telemetry_initializing_fail_safe"
# Frame ``critical_missing`` codes that report a failed or invalid read rather than an absent field.
READ_FAILURE_PREFIXES = (
    "endpoint_read_error:", "continuous_source_read_error:", "continuous_source_integrity_compromised:",
    "configuration_read_error", "continuous_caliper_read_error", "caliper_read_error", "caliper_summary_missing",
    "ledger_read_error", "arrival_history_read_error",
)
# Sources stamped by their producer, whose timestamps must not go backwards between frames.
ORDERED_SOURCES = ("caliper", "ledger")
TIMESTAMP_INCONSISTENCY_PREFIXES = (
    "source_timestamp_skew_exceeds_limit", "source_timestamp_out_of_order", "wall_clock_step_detected",
    "observation_timestamp_inconsistent",
)


class TelemetryQualityError(IntegrationSafetyError):
    """A frame is well formed but its observation cannot safely drive a decision.

    It carries the controller abstention reason, a machine-readable ``reason_code`` and bounded
    structured ``details``, so a rejection is never just a message string.
    """

    def __init__(self, message: str, *, reason_code: str, abstention_reason: str,
                 details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.reason_code = reason_code
        self.abstention_reason = abstention_reason
        self.details = dict(details or {})


def missing_abstention_reason(codes: Iterable[str]) -> str:
    """A failed read outranks an absent field: say the read failed when any code reports that."""
    return ABSTAIN_READ_FAILED if any(str(code).startswith(READ_FAILURE_PREFIXES) for code in codes) else ABSTAIN_MISSING


def inconsistency_abstention_reason(codes: Iterable[str]) -> str:
    stamped = any(str(code).startswith(TIMESTAMP_INCONSISTENCY_PREFIXES) for code in codes)
    return ABSTENTION_REASON[FreshnessStatus.INCONSISTENT] if stamped else ABSTAIN_INCONSISTENT


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
                     observation_monotonic_ns: int | None = None, strict_config: bool = False) -> "TelemetryFrame":
        """Build a frame from a recorded row.

        ``ControllerState.from_mapping`` fills a missing ``current_config`` with the dataclass defaults
        (B=45). With ``strict_config=True`` the row must carry a complete canonical configuration and
        nothing is defaulted, so a block size can never come from a constant.
        """
        if not isinstance(row, Mapping):
            raise IntegrationSafetyError("telemetry row must be a mapping")
        state_payload = row.get("state", row)
        if not isinstance(state_payload, Mapping):
            raise IntegrationSafetyError("telemetry frame state must be a mapping")
        if strict_config:
            try:
                confirmed_config(state_payload.get("current_config"))
            except (TypeError, ValueError) as exc:
                raise IntegrationSafetyError(
                    f"frame carries no complete current_config ({exc}); no default is substituted") from exc
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
        # A state carrying a shared-history reference is digested in reference form: the
        # reference binds the exact prefix by SHA-256, so the digest still covers its content.
        return {
            "observation_timestamp_utc": self.observation_timestamp_utc,
            "state": self.state.as_dict(history="auto"),
            "source_timestamps_utc": dict(self.source_timestamps_utc),
            "freshness_seconds": dict(self.freshness_seconds),
        }

    @property
    def input_digest(self) -> str:
        return _canonical_digest(self.input_payload())

    def validate(self, *, required_fields: Iterable[str] = (), max_age_s: float = 5.0,
                 policy: FreshnessPolicy | None = None) -> None:
        """Reject a frame that cannot safely drive a decision.

        Structural problems raise ``IntegrationSafetyError``. Observation-quality problems raise
        ``TelemetryQualityError``, which names the controller abstention reason: stale, missing,
        future-dated or inconsistent timestamps, absent required fields, read failures. The age
        limit comes from ``policy`` (validated), or from ``max_age_s`` for existing callers; an age
        exactly equal to the limit is fresh and anything greater is stale.
        """
        if not self.run_id:
            raise IntegrationSafetyError("telemetry frame has no run_id")
        if self.decision_index < 0:
            raise IntegrationSafetyError("decision_index must be non-negative")
        if policy is None:
            try:
                policy = FreshnessPolicy(max_age_s=max_age_s)
            except ValueError as exc:
                raise IntegrationSafetyError(f"invalid freshness limit: {exc}") from exc
        limit = policy.max_age_s
        stamp_inconsistent = ABSTENTION_REASON[FreshnessStatus.INCONSISTENT]
        observation = _parse_utc(self.observation_timestamp_utc)
        if _parse_utc(self.state.timestamp_utc) > observation:
            raise TelemetryQualityError("controller state timestamp is after observation",
                                        reason_code="state_timestamp_after_observation",
                                        abstention_reason=stamp_inconsistent)
        if self.critical_missing:
            raise TelemetryQualityError(f"critical telemetry missing: {sorted(self.critical_missing)}",
                                        reason_code="critical_telemetry_missing",
                                        abstention_reason=missing_abstention_reason(self.critical_missing),
                                        details={"codes": sorted(self.critical_missing)})
        if self.inconsistencies:
            raise TelemetryQualityError(f"inconsistent telemetry: {sorted(self.inconsistencies)}",
                                        reason_code="telemetry_inconsistent",
                                        abstention_reason=inconsistency_abstention_reason(self.inconsistencies),
                                        details={"codes": sorted(self.inconsistencies)})
        if self.state.telemetry_age_s is not None and self.state.telemetry_age_s > limit:
            raise TelemetryQualityError(f"telemetry is stale by {self.state.telemetry_age_s:.3f}s",
                                        reason_code="stale_observation",
                                        abstention_reason=ABSTENTION_REASON[FreshnessStatus.STALE],
                                        details={"age_s": self.state.telemetry_age_s, "max_age_s": limit})
        for name, age in self.freshness_seconds.items():
            if age is None or not math.isfinite(age):
                raise TelemetryQualityError(f"telemetry source {name} is stale or unavailable: {age!r}",
                                            reason_code="observation_timestamp_missing",
                                            abstention_reason=ABSTENTION_REASON[FreshnessStatus.MISSING],
                                            details={"source": name})
            if age < -policy.max_future_skew_s:
                raise TelemetryQualityError(f"telemetry source {name} is future-dated by {-age:.3f}s",
                                            reason_code="future_dated_observation",
                                            abstention_reason=ABSTENTION_REASON[FreshnessStatus.FUTURE_DATED],
                                            details={"source": name, "age_s": age})
            if age > limit:
                raise TelemetryQualityError(f"telemetry source {name} is stale or unavailable: {age!r}",
                                            reason_code="stale_observation",
                                            abstention_reason=ABSTENTION_REASON[FreshnessStatus.STALE],
                                            details={"source": name, "age_s": age, "max_age_s": limit})
        for name, value in self.source_timestamps_utc.items():
            verdict = classify_observation(name, value, observation, policy)
            if verdict.status is FreshnessStatus.FRESH:
                continue
            message = {
                FreshnessStatus.FUTURE_DATED: f"source timestamp {name} is after observation timestamp",
                FreshnessStatus.STALE: f"telemetry source {name} is stale by {verdict.age_s:.3f}s"
                                       if verdict.age_s is not None else f"telemetry source {name} is stale",
                FreshnessStatus.MISSING: f"telemetry source {name} has no observation timestamp",
                FreshnessStatus.INCONSISTENT: f"invalid UTC timestamp {value!r} for source {name}",
            }[verdict.status]
            raise TelemetryQualityError(message, reason_code=f"{verdict.status.value.lower()}_observation",
                                        abstention_reason=ABSTENTION_REASON[verdict.status],
                                        details=verdict.as_dict())
        for field_name in required_fields:
            if field_name not in {"current_config", "timestamp_utc"} and getattr(self.state, field_name, None) is None:
                raise TelemetryQualityError(f"required telemetry field is missing: {field_name}",
                                            reason_code="required_field_missing", abstention_reason=ABSTAIN_MISSING,
                                            details={"field": field_name})
        history_timestamps = self.state.arrival_history_timestamps_utc
        if history_timestamps:
            parsed = [_parse_utc(value) for value in history_timestamps]
            if parsed != sorted(parsed):
                raise TelemetryQualityError("arrival history timestamps are not chronological",
                                            reason_code="history_not_chronological", abstention_reason=stamp_inconsistent)
            origin = _parse_utc(self.state.timestamp_utc)
            if parsed[-1] > origin:
                raise TelemetryQualityError("arrival history contains future data",
                                            reason_code="future_dated_observation",
                                            abstention_reason=ABSTENTION_REASON[FreshnessStatus.FUTURE_DATED])

    def as_dict(self, *, history: str = "full") -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "decision_index": self.decision_index,
            "observation_timestamp_utc": self.observation_timestamp_utc,
            "observation_monotonic_ns": self.observation_monotonic_ns,
            "state": self.state.as_dict(history=history),
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
        if self.max_failed_transactions is not None:
            if state.failed_transactions is None:
                reasons.append("failure_count_unavailable")
            elif state.failed_transactions > self.max_failed_transactions:
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
                 workload: Any | None = None,
                 max_source_skew_s: float | None = None,
                 required_fields: Iterable[str] = ("observed_arrival_rate_tps", "observed_throughput_tps",
                                                    "observed_latency_ms", "queue_depth"),
                 freshness_policy: FreshnessPolicy | None = None,
                 required_prometheus_metrics: Iterable[str] = (),
                 clock: Clock | None = None):
        if max_source_skew_s is not None and max_source_skew_s <= 0:
            raise ValueError("max_source_skew_s must be positive")
        if freshness_policy is not None and not isinstance(freshness_policy, FreshnessPolicy):
            raise ValueError("freshness_policy must be a FreshnessPolicy; build it with FreshnessPolicy.from_mapping")
        if freshness_policy is None:
            freshness_policy = (FreshnessPolicy() if max_source_skew_s is None
                                else FreshnessPolicy(max_source_skew_s=float(max_source_skew_s)))
        elif max_source_skew_s is not None and float(max_source_skew_s) != freshness_policy.max_source_skew_s:
            raise ValueError("max_source_skew_s conflicts with freshness_policy; supply only one")
        self.freshness_policy = freshness_policy
        self.required_prometheus_metrics = tuple(required_prometheus_metrics)
        self._clock: Clock = clock or SystemClock()
        # Latest timestamp already accepted per source: a source moving backwards is inconsistent, a
        # source repeating a timestamp (the same summary read again) is simply older.
        self._last_source_time: dict[str, datetime] = {}
        self._clock_monitor = ClockConsistencyMonitor(freshness_policy.max_clock_discrepancy_s)
        self.prometheus = prometheus
        self.channel_id = channel_id
        self.config_reader = config_reader
        self.caliper_summary_path = Path(caliper_summary_path) if caliper_summary_path else None
        if self.caliper_summary_path is not None and continuous_source is not None:
            raise ValueError("choose caliper_summary_path or continuous_source, not both")
        self.continuous_source = continuous_source
        self.ledger_reader = ledger_reader
        self.arrival_history_reader = arrival_history_reader
        # A shared history store (``workload_history.ArrivalHistoryStore``) is read through one
        # atomic ``snapshot`` that also returns the content-addressed prefix reference.
        self.workload = workload if callable(getattr(workload, "snapshot", None)) else None
        self.max_source_skew_s = freshness_policy.max_source_skew_s
        self.required_fields = tuple(required_fields)

    def collect(self, *, run_id: str, decision_index: int, observation_timestamp_utc: str | None = None) -> TelemetryFrame:
        live_timestamp = observation_timestamp_utc is None
        observed = observation_timestamp_utc or self._clock.utc_now()
        errors: list[str] = []
        failures: list[TelemetryReadFailure] = []
        # The legacy ``errors`` strings keep their historical form (consumers and tests read them);
        # ``failures`` carries the same events as structured, sanitized records.
        config = None
        if self.config_reader is not None:
            try:
                observed_config = self.config_reader()
                config = None if observed_config is None else confirmed_config(observed_config)
            except Exception as exc:
                errors.append(f"configuration_read_error:{type(exc).__name__}:{sanitize_text(exc)}")
                failures.append(failure_from_exception("configuration", exc, kind="file"))

        caliper: FabricTelemetrySnapshot | None = None
        if self.continuous_source is not None:
            try:
                caliper = self.continuous_source.collect(observation_timestamp_utc=observed)
            except Exception as exc:
                errors.append(f"continuous_caliper_read_error:{type(exc).__name__}:{sanitize_text(exc)}")
                failures.append(failure_from_exception("continuous_caliper", exc, kind="file"))
        elif self.caliper_summary_path is not None:
            if self.caliper_summary_path.is_file():
                try:
                    caliper = caliper_snapshot_from_summary(self.caliper_summary_path, channel_id=self.channel_id)
                except Exception as exc:
                    errors.append(f"caliper_read_error:{type(exc).__name__}:{sanitize_text(exc)}")
                    failures.append(failure_from_exception("caliper_summary", exc, kind="file"))
            else:
                errors.append(f"caliper_summary_missing:{self.caliper_summary_path}")
                failures.append(failure_from_category("caliper_summary", "file_missing",
                                                      f"summary file not found: {self.caliper_summary_path}",
                                                      error_type="FileNotFoundError"))

        ledger: Mapping[str, Any] | None = None
        if self.ledger_reader is not None:
            try:
                ledger = self.ledger_reader()
            except Exception as exc:
                errors.append(f"ledger_read_error:{type(exc).__name__}:{sanitize_text(exc)}")
                failures.append(failure_from_exception("ledger", exc, kind="file"))
        # A ledger observation with no usable timestamp cannot be shown to be fresh. It is excluded,
        # explicitly reported, and never presented as current (fix 2: missing timestamps).
        ledger_excluded = False
        if ledger:
            try:
                ledger_stamp = parse_utc(str(ledger["timestamp_utc"])) if ledger.get("timestamp_utc") else None
            except ValueError:
                ledger_stamp = None
            if ledger_stamp is None:
                failures.append(failure_from_category(
                    "ledger", "missing_timestamp",
                    "ledger observation has no usable timestamp; excluded from the frame, not treated as fresh"))
                ledger, ledger_excluded = None, True

        snapshot = self.prometheus.collect(
            self.channel_id, timestamp_utc=observed, current_config=config,
            caliper=caliper, ledger=ledger,
            health={"provider": "ProductionTelemetryProvider", "read_errors": errors},
        )
        history_timestamps: Iterable[str] = ()
        history_values: Iterable[float] = ()
        history_ref = None
        workload_provenance = None
        if self.workload is not None:
            try:
                history_timestamps, history_values, history_ref = self.workload.snapshot(now_utc=observed)
                workload_provenance = self.workload.provenance(history_ref)
            except Exception as exc:
                errors.append(f"arrival_history_read_error:{type(exc).__name__}:{sanitize_text(exc)}")
                failures.append(failure_from_exception("arrival_history", exc, kind="file"))
                history_timestamps, history_values, history_ref, workload_provenance = (), (), None, None
        elif self.arrival_history_reader is not None:
            try:
                history_timestamps, history_values = self.arrival_history_reader()
                history_timestamps, history_values = tuple(history_timestamps), tuple(history_values)
            except Exception as exc:
                errors.append(f"arrival_history_read_error:{type(exc).__name__}:{sanitize_text(exc)}")
                failures.append(failure_from_exception("arrival_history", exc, kind="file"))
                history_timestamps, history_values = (), ()
        state = snapshot.to_controller_state(
            arrival_history_tps=history_values,
            arrival_history_timestamps_utc=history_timestamps,
            now_utc=observed,
            require_fresh=False,
            allow_unknown_config=True,
            arrival_history_ref=history_ref,
            allow_future=True,
        )

        # Failures reported by the adapters themselves. The provider used to merge only the metrics of
        # these snapshots and never their health, so an unreachable orderer or a rejected recorder file
        # reached the controller without its cause (live-shadow fix 3).
        adapter_missing: list[str] = []
        failure_fields = ("source", "category", "error_type", "message", "endpoint", "http_status")
        for target, detail in sorted((snapshot.health.get("endpoint_error_details") or {}).items()):
            adapter_missing.append(f"endpoint_read_error:{target}:{detail['category']}")
            failures.append(TelemetryReadFailure(**{key: detail.get(key) for key in failure_fields}))
        if caliper is not None:
            caliper_items = (caliper.health.get("read_error_details") or {}).get("items") or []
            for item in caliper_items:
                stream = str(item["source"]).split(":", 1)[-1]
                adapter_missing.append(f"continuous_source_read_error:{stream}:{item['category']}")
                failures.append(TelemetryReadFailure(**{key: item.get(key) for key in failure_fields}))
            if caliper.health.get("read_errors") and not caliper_items:
                adapter_missing.append("continuous_source_read_error:unclassified")
            integrity = caliper.health.get("integrity") or {}
            if integrity.get("status") == "COMPROMISED":
                streams = sorted({name for name, count in (integrity.get("rows_rejected") or {}).items() if count}
                                 | set(integrity.get("streams_lost_to_read_errors") or ()))
                adapter_missing.append("continuous_source_integrity_compromised:" + (",".join(streams) or "unknown"))
        required_metrics_missing: list[str] = []
        for name in self.required_prometheus_metrics:
            metric = snapshot.metrics.get(name)
            if metric is None or metric.value is None:
                reason = "metric not produced by the adapter" if metric is None else (metric.missing_reason or "no value")
                required_metrics_missing.append(name)
                adapter_missing.append(f"required_metric_missing:{name}")
                failures.append(failure_from_category("prometheus", "missing_required_metric", f"{name}: {reason}"))

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
        # Ages are reported as they are. A negative age (a future-dated source) used to be clamped to
        # zero here and so looked perfectly fresh; it is now left negative for the validator to reject.
        reference = parse_utc(observed)
        freshness = {name: (reference - parse_utc(value)).total_seconds() for name, value in source_timestamps.items()}
        dated_sources = {name: value for name, value in source_timestamps.items() if name != "observation"}
        # Ordering is checked only for producer-stamped sources. A source stamped with this frame's own
        # time (the Prometheus metrics) moves backwards only when the wall clock does, and that is
        # reported once, as ``wall_clock_step_detected``, instead of once per metric.
        ordered = {name: self._last_source_time[name] for name in ORDERED_SOURCES if name in self._last_source_time}
        assessment = assess_sources(dated_sources, observed, self.freshness_policy, previous=ordered)
        for name in ORDERED_SOURCES:
            if name in dated_sources:
                stamp = parse_utc(dated_sources[name])
                if name not in self._last_source_time or stamp > self._last_source_time[name]:
                    self._last_source_time[name] = stamp
        missing = list(errors) + adapter_missing
        if config is None:
            missing.append("current_config")
        for field_name in self.required_fields:
            if field_name == "current_config":
                if config is None:
                    missing.append("current_config")
            elif getattr(state, field_name, None) is None:
                missing.append(field_name)
        inconsistencies: list[str] = []
        # Skew is judged among fresh sources only. A stale source already carries its own, clearer
        # reason ("stale"); letting it also trip the skew check made a stale summary frame read as
        # merely "inconsistent", because that check runs before the freshness checks.
        skew_values = [parse_utc(value) for name, value in source_timestamps.items()
                       if name == "observation" or assessment.verdicts[name].fresh]
        if skew_values and (max(skew_values) - min(skew_values)).total_seconds() > self.freshness_policy.max_source_skew_s:
            inconsistencies.append("source_timestamp_skew_exceeds_limit")
        for name, verdict in assessment.verdicts.items():
            if verdict.detail == "out_of_order_source":
                inconsistencies.append(f"source_timestamp_out_of_order:{name}")
        clock_step_s = (self._clock_monitor.check(reference, self._clock.monotonic_ns()) if live_timestamp else None)
        if clock_step_s is not None:
            inconsistencies.append("wall_clock_step_detected")
        leaders = snapshot.metrics.get("raft_leader_count")
        if leaders is not None and leaders.value is not None and leaders.value != 1:
            inconsistencies.append("expected_exactly_one_raft_leader")
        health_metric = snapshot.metrics.get("orderer_peer_health")
        if health_metric is not None and health_metric.value == 0:
            missing.append("orderer_peer_health")

        # One observation status for the frame: SUCCESSFUL, PARTIAL, FAILED or INVALID.
        source_outcomes: dict[str, str] = {}
        endpoint_status = snapshot.health.get("read_status") or {}
        for key, outcome in (("sources_ok", "ok"), ("sources_failed", "failed"), ("sources_invalid", "invalid")):
            for target in endpoint_status.get(key, ()):
                source_outcomes[f"prometheus:{target}"] = outcome

        def source_outcome(*prefixes: str) -> str:
            mine = [item for item in failures
                    if any(item.source == prefix or item.source.startswith(prefix + ":") for prefix in prefixes)]
            if any(item.outcome == "failed" for item in mine):
                return "failed"
            return "invalid" if mine else "ok"

        # Observation sources only. The configuration is required context rather than an observation:
        # it is reported beside the status, and a missing one already makes the frame incomplete
        # through ``current_config``. Counting it here would turn "every telemetry read failed" into
        # PARTIAL merely because the configuration file was readable.
        for name, prefixes, configured in (
                ("continuous_caliper", ("continuous_caliper", "continuous"), self.continuous_source is not None),
                ("caliper_summary", ("caliper_summary",), self.caliper_summary_path is not None),
                ("ledger", ("ledger",), self.ledger_reader is not None),
                ("arrival_history", ("arrival_history",),
                 self.workload is not None or self.arrival_history_reader is not None)):
            if configured:
                source_outcomes[name] = source_outcome(*prefixes)
        absent_required = sorted({code for code in missing
                                  if not str(code).startswith(READ_FAILURE_PREFIXES) and code != "orderer_peer_health"})
        read_status = aggregate_read_status(source_outcomes, absent_required)
        read_status["configuration"] = source_outcome("configuration") if self.config_reader is not None else "not_configured"
        provenance = {
            "channel_id": self.channel_id,
            # Endpoint URLs are recorded without user information or query values.
            "prometheus_endpoints": {name: sanitize_url(url) for name, url in sorted(self.prometheus.endpoints.items())},
            "caliper_summary": None if self.caliper_summary_path is None else str(self.caliper_summary_path),
            "continuous_caliper_source": None if self.continuous_source is None else {
                "recorder": str(self.continuous_source.recorder.path),
                "submits": str(self.continuous_source.submits.path),
                "ledger": str(self.continuous_source.ledger.path),
                **({"ledger_observations": str(self.continuous_source.ledger_observations.path)}
                   if getattr(self.continuous_source, "ledger_observations", None) is not None else {}),
            },
            "ledger_source": None if not ledger else ledger.get("source"),
            "missing_sources": list(errors),
            "telemetry_health": dict(snapshot.health),
            "metrics": {name: metric.as_dict() for name, metric in snapshot.metrics.items()},
            # Structured evidence that survives into every controller record (fixes 1-3, 2026-11-17).
            "read_status": read_status,
            "read_failures": bounded_details(failures),
            "freshness_assessment": assessment.compact_dict(),
            "clock_consistency": {"checked_against_monotonic_clock": live_timestamp,
                                  "discrepancy_s": clock_step_s,
                                  "limit_s": self.freshness_policy.max_clock_discrepancy_s},
            "ledger_observation_excluded": ledger_excluded,
            # The client-latency definition, sample counts and stream integrity behind this frame's client metrics
            # (task A0c). None when no continuous source is configured or the snapshot came from a summary.
            "continuous_caliper_health": None if caliper is None or "latency" not in caliper.health else {
                "latency": caliper.health.get("latency"),
                "integrity": caliper.health.get("integrity"),
                "totals": caliper.health.get("totals"),
                # Committed-throughput evidence behind this frame (task A0d). Present only when the continuous source
                # reads a ledger observation journal; its ``state`` is what the Stage 3 warm-up rule looks at.
                **({"ledger": caliper.health.get("ledger_observations")} if "ledger_observations" in caliper.health else {}),
            },
        }
        config_provenance = getattr(self.config_reader, "provenance", None)
        if callable(config_provenance):
            config_provenance = config_provenance()
        if self.config_reader is None:
            provenance["configuration_source"] = {"status": "unavailable", "note": "no configuration reader"}
        elif isinstance(config_provenance, Mapping):
            provenance["configuration_source"] = dict(config_provenance)
        else:
            provenance["configuration_source"] = {
                "status": "unverified_reader",
                "note": "the configuration reader supplies no provenance; use config_evidence to build one"}
        workload_source = getattr(self.arrival_history_reader, "__self__", None)
        if workload_provenance is not None:
            provenance["workload"] = workload_provenance
        elif workload_source is not None and callable(getattr(workload_source, "provenance", None)):
            provenance["workload"] = workload_source.provenance()
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
            active = confirmed_config(active_raw)
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
                 stop_policy: StopPolicy | None = None, clock: Clock | None = None,
                 artifact_provenance: Mapping[str, Any] | None = None,
                 max_record_bytes: int | None = None,
                 invalid_frame_policy: str = "stop",
                 frame_quality: Stage3FrameQualityContract | None = None):
        if not controllers:
            raise ValueError("at least one controller is required")
        # Opt-in Stage 3 contract (task A0c). Without it nothing below changes. With it: the client fields are
        # required of every frame, a frame must come from a continuous client source, and the run stops after the
        # configured number of CONSECUTIVE abstained frames. It needs the abstain policy (a bound on abstentions is
        # meaningless if the first invalid frame already stops the run) and shadow mode (it is a shadow-run contract).
        if frame_quality is not None:
            if not isinstance(frame_quality, Stage3FrameQualityContract):
                raise ValueError("frame_quality must be a Stage3FrameQualityContract")
            if invalid_frame_policy != "abstain":
                raise ValueError("a Stage 3 frame-quality contract requires invalid_frame_policy='abstain'")
            if not shadow:
                raise IntegrationSafetyError("the Stage 3 frame-quality contract applies to shadow runs only")
        self.frame_quality = frame_quality
        # What happens to a frame whose observation cannot safely drive a decision (stale, missing,
        # future-dated or inconsistent timestamps, read failures, absent required fields):
        # "stop"    - record the rejection and end the run (the established, fail-closed default);
        # "abstain" - record the rejection, give every strategy an abstention carrying the structured
        #             cause, apply nothing, and continue with the next frame (live shadow, stage 3).
        # Structural errors (wrong run, bad decision index, unparseable observation time) always stop.
        if invalid_frame_policy not in {"stop", "abstain"}:
            raise ValueError("invalid_frame_policy must be 'stop' or 'abstain'")
        self.invalid_frame_policy = invalid_frame_policy
        try:
            self.freshness_policy = FreshnessPolicy(max_age_s=max_telemetry_age_s)
        except ValueError as exc:
            raise ValueError(f"max_telemetry_age_s is not an accepted freshness limit: {exc}") from exc
        if max_record_bytes is not None and (isinstance(max_record_bytes, bool) or not isinstance(max_record_bytes, int) or max_record_bytes < 1):
            raise ValueError("max_record_bytes must be a positive integer or None")
        if not shadow and not enable_live:
            raise IntegrationSafetyError("live actuation requires explicit enable_live=True")
        self.controllers = dict(controllers)
        self.initial_config = initial_config
        self.run_id = run_id
        self.shadow = shadow
        self.enable_live = enable_live
        self.required_fields = tuple(required_fields)
        # The fields a frame is validated against: the runner's own plus, under the Stage 3 contract, the client fields.
        self._validated_fields = tuple(dict.fromkeys(
            self.required_fields + (frame_quality.required_client_fields if frame_quality is not None else ())))
        self.max_telemetry_age_s = max_telemetry_age_s
        self.stop_policy = stop_policy or StopPolicy()
        self.clock = clock or SystemClock()
        self.clock_contract = TimestampContract()
        self.configuration_policy = configuration_policy
        self.artifact_provenance = None if artifact_provenance is None else json.loads(json.dumps(dict(artifact_provenance)))
        # Fail-closed guard against per-record growth (independent audit D3): a record whose
        # serialized size exceeds the limit is not kept and the run stops.
        self.max_record_bytes = max_record_bytes
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
                configuration_policy=configuration_policy, history_serialization="auto",
            )
            for name, controller in self.controllers.items()
        }

    def run(self, frames: Iterable[TelemetryFrame], *, output_dir: Path | None = None,
            retain_records: bool = True) -> dict[str, Any]:
        """Run every strategy on each validated frame.

        Records are compact: a state carrying a shared-history reference is recorded by that
        reference, never by a copy of the history (independent audit D3). With
        ``retain_records=False`` records are streamed to ``output_dir`` as they are produced and
        not kept in memory, so memory does not grow with run length.
        """
        if output_dir is not None and Path(output_dir).exists():
            raise FileExistsError(f"refusing to overwrite {output_dir}")
        if not retain_records and output_dir is None:
            raise ValueError("retain_records=False requires output_dir to stream records")
        streams = None
        if not retain_records:
            Path(output_dir).mkdir(parents=True)
            streams = {name: (Path(output_dir) / filename).open("x", encoding="utf-8", newline="\n")
                       for name, filename in (("records", "controller_decisions.jsonl"),
                                              ("latency", "adaptation_latency.jsonl"))}
        all_records: list[dict[str, Any]] = []
        latency_records: list[dict[str, Any]] = []
        validation_records: list[dict[str, Any]] = []
        stop_reasons: list[str] = []
        identical_digests = True
        frames_with_records = 0
        record_count = 0
        abstained_frames = 0
        consecutive_abstained = 0
        longest_abstention_run = 0
        abstention_limit_reached = False
        abstention_limit = None if self.frame_quality is None else self.frame_quality.max_consecutive_abstentions
        # Warm-up bookkeeping (task A0d). Frames are accepted at most once warm-up is over, and warm-up is bounded.
        accepted_frames = 0
        initialization_frames = 0
        initialization_limit_hit = False
        halt = False
        previous_observation = None
        previous_index = -1
        seen = False
        try:
            for frame in frames:
                seen = True
                if frame.run_id != self.run_id:
                    raise IntegrationSafetyError("frame run_id does not match runner run_id")
                rejection: IntegrationSafetyError | None = None
                is_initialization = False
                try:
                    checked, checked_fields, waived = frame, self._validated_fields, ()
                    init_details = (self.frame_quality.initialization_candidate(frame)
                                    if self.frame_quality is not None and accepted_frames == 0 else None)
                    if init_details is not None:
                        if initialization_frames >= self.frame_quality.max_initialization_frames:
                            initialization_limit_hit = True
                            raise TelemetryQualityError(
                                "the operator-supplied initialization bound is exhausted and committed throughput is still "
                                "unavailable", reason_code=INITIALIZATION_LIMIT_REASON_CODE, abstention_reason=ABSTAIN_MISSING,
                                details={**init_details, "initialization_frames_used": initialization_frames})
                        # Only the one waived shortfall is set aside; every other check below still runs on the frame.
                        checked = _dataclass_replace(frame, critical_missing=tuple(
                            code for code in frame.critical_missing if code != THROUGHPUT_FIELD))
                        checked_fields = tuple(name for name in self._validated_fields if name != THROUGHPUT_FIELD)
                        waived = (THROUGHPUT_FIELD,)
                    checked.validate(required_fields=checked_fields, policy=self.freshness_policy)
                    if self.frame_quality is not None:
                        issue = self.frame_quality.check(checked, waived_fields=waived)
                        if issue is not None:
                            raise TelemetryQualityError(
                                issue.message, reason_code=issue.reason_code,
                                abstention_reason=(ABSTENTION_REASON[FreshnessStatus.MISSING]
                                                   if issue.kind == "freshness_unknown" else ABSTAIN_MISSING),
                                details=issue.details)
                    if init_details is not None:
                        is_initialization = True
                        raise TelemetryQualityError(
                            "initialization: the ledger journal holds only its baseline observation, so committed throughput "
                            "has no interval yet; no decision is made", reason_code=INITIALIZATION_REASON_CODE,
                            abstention_reason=ABSTAIN_INITIALIZING,
                            details={**init_details, "initialization_frame": initialization_frames + 1})
                    observed = _parse_utc(frame.observation_timestamp_utc)
                    if previous_observation is not None and (observed <= previous_observation or frame.decision_index <= previous_index):
                        raise TelemetryQualityError(
                            "telemetry frames must advance in time and decision index",
                            reason_code="frames_not_advancing", abstention_reason=ABSTAIN_NONADVANCING,
                            details={"observation_timestamp_utc": frame.observation_timestamp_utc,
                                     "previous_observation_timestamp_utc": previous_observation.isoformat(timespec="milliseconds"),
                                     "decision_index": frame.decision_index, "previous_decision_index": previous_index})
                    previous_observation, previous_index = observed, frame.decision_index
                    reasons = self.stop_policy.reasons(frame.state)
                    if reasons:
                        stop_reasons.extend(reasons)
                        validation_records.append({"decision_index": frame.decision_index, "status": "stopped", "reasons": reasons})
                        break
                except IntegrationSafetyError as exc:
                    rejection = exc
                if rejection is None:
                    consecutive_abstained = 0
                    accepted_frames += 1
                if rejection is not None:
                    abstain = self.invalid_frame_policy == "abstain" and isinstance(rejection, TelemetryQualityError)
                    validation_records.append({"decision_index": frame.decision_index, "status": "rejected",
                                               "error": str(rejection), "observation": frame.as_dict(history="auto"),
                                               "handled": ("initializing" if is_initialization else "abstained") if abstain else "stopped",
                                               **self._rejection_summary(rejection)})
                    if not abstain:
                        stop_reasons.append("telemetry_validation_failed")
                        break
                    if rejection.reason_code != "frames_not_advancing":
                        # Keep the stream strictly advancing across abstentions; an ordering failure is
                        # the one rejection that must not move the reference forward.
                        rejected_at = _parse_utc(frame.observation_timestamp_utc)
                        if previous_observation is None or rejected_at > previous_observation:
                            previous_observation, previous_index = rejected_at, frame.decision_index
                    abstained_frames += 1
                    if is_initialization:
                        # A warm-up frame is neither a valid frame (it must not reset the count) nor an invalid one
                        # (it must not spend the abstention budget); it spends its own bounded budget instead.
                        initialization_frames += 1
                    else:
                        consecutive_abstained += 1
                        longest_abstention_run = max(longest_abstention_run, consecutive_abstained)

                frame_digests: set[str] = set()
                for name in sorted(self.controllers):
                    # Each strategy gets its own state object for precisely the same frame. A
                    # shallow copy suffices: the history arrays, reference and configuration are
                    # immutable, and the harness only rebinds fields on its own copy.
                    state = copy.copy(frame.state)
                    if self.shadow and rejection is None:
                        self.actuators[name].observe_recorded_config(state.current_config)
                    decision_start = self.clock.utc_now()
                    decision_start_ns = self.clock.monotonic_ns()
                    # A rejected frame never reaches a controller's decision logic: each strategy
                    # receives an explicit abstention and no state is offered to the actuator.
                    record = (self._rejection_record(name, frame, rejection) if rejection is not None
                              else self.harnesses[name].process(state))
                    decision_end = self.clock.utc_now()
                    decision_end_ns = self.clock.monotonic_ns()
                    digest = frame.input_digest  # recomputed per arm: detects any mutation of the shared frame
                    frame_digests.add(digest)
                    record["integration"] = {
                        "run_id": self.run_id,
                        "strategy": name,
                        "decision_index": frame.decision_index,
                        "input_digest": digest,
                        "decision_start_timestamp_utc": decision_start,
                        "decision_completion_timestamp_utc": decision_end,
                        "decision_monotonic_duration_ms": (decision_end_ns - decision_start_ns) / 1_000_000,
                        "shadow_mode": self.shadow,
                        "live_submission_enabled": bool(not self.shadow and self.enable_live),
                        "timestamp_contract": self.clock_contract.as_dict(),
                        "observation_provenance": dict(frame.provenance),
                        "source_timestamps_utc": dict(frame.source_timestamps_utc),
                        "freshness_seconds": dict(frame.freshness_seconds),
                    }
                    if rejection is not None:
                        record["integration"]["observation_rejected"] = self._rejection_summary(rejection)
                    record["latency"] = LatencyEvidence.from_record(
                        run_id=self.run_id, decision_index=frame.decision_index,
                        decision_start=decision_start, decision_end=decision_end,
                        application=record.get("application"), decision=record["decision"],
                        clock=self.clock_contract,
                    ).as_dict()
                    record["latency"]["timestamps"]["telemetry_observation"] = frame.observation_timestamp_utc
                    record["decision_trace"] = _decision_trace(
                        name, record, frame, state, digest,
                        self.artifact_provenance if _is_predictive(self.controllers[name]) else None)
                    encoded = json.dumps(record, sort_keys=True) if (streams is not None or self.max_record_bytes is not None) else None
                    if self.max_record_bytes is not None and len(encoded) > self.max_record_bytes:  # ASCII JSON: chars == bytes
                        stop_reasons.append("record_size_limit_exceeded")
                        validation_records.append({"decision_index": frame.decision_index, "status": "stopped",
                                                   "reasons": ["record_size_limit_exceeded"], "strategy": name,
                                                   "record_bytes": len(encoded), "max_record_bytes": self.max_record_bytes,
                                                   "decision_trace": record["decision_trace"]})
                        halt = True
                        break
                    record_count += 1
                    if streams is not None:
                        streams["records"].write(encoded + "\n")
                        streams["latency"].write(json.dumps(record["latency"], sort_keys=True) + "\n")
                    else:
                        all_records.append(record)
                        latency_records.append(record["latency"])
                    application = record.get("application")
                    if application and not application.get("accepted", False) and self.stop_policy.stop_on_actuation_failure and not self.shadow:
                        stop_reasons.append("actuation_failed_or_unverified")
                        break
                if frame_digests:
                    frames_with_records += 1
                    identical_digests = identical_digests and len(frame_digests) == 1
                if streams is not None:
                    for stream in streams.values():
                        stream.flush()
                # Stage 3 safety stop. The abstentions of this frame are already recorded for every controller; the
                # run then ends, so a stream of unusable frames cannot be mistaken for a completed comparison.
                if initialization_limit_hit and not halt:
                    stop_reasons.append("initialization_limit_exceeded")
                    validation_records.append({
                        "decision_index": frame.decision_index, "status": "stopped",
                        "reasons": ["initialization_limit_exceeded"],
                        "initialization_frames": initialization_frames,
                        "max_initialization_frames": self.frame_quality.max_initialization_frames,
                        "input_digest": frame.input_digest})
                if (abstention_limit is not None and rejection is not None and not halt and not is_initialization
                        and consecutive_abstained >= abstention_limit):
                    abstention_limit_reached = True
                    stop_reasons.append("consecutive_abstention_limit_reached")
                    validation_records.append({
                        "decision_index": frame.decision_index, "status": "stopped",
                        "reasons": ["consecutive_abstention_limit_reached"],
                        "consecutive_abstained_frames": consecutive_abstained,
                        "max_consecutive_abstentions": abstention_limit,
                        "last_rejection": self._rejection_summary(rejection),
                        "input_digest": frame.input_digest})
                    break
                if initialization_limit_hit and not halt:
                    break
                if halt or (stop_reasons and not self.shadow):
                    break
        finally:
            if streams is not None:
                for stream in streams.values():
                    stream.close()

        if not seen:
            raise ValueError("telemetry frame stream is empty")

        result = {
            "tool": "IntegratedControllerRunner 1.1",
            "run_id": self.run_id,
            "mode": "shadow" if self.shadow else "live",
            "live_submission_enabled": bool(not self.shadow and self.enable_live),
            "strategies": sorted(self.controllers),
            "frame_count": frames_with_records,
            "record_count": record_count,
            "records_retained_in_memory": retain_records,
            "max_record_bytes": self.max_record_bytes,
            "record_format": "compact: shared-history reference instead of per-record history copies",
            "identical_input_digests": identical_digests,
            "invalid_frame_policy": self.invalid_frame_policy,
            "abstained_frame_count": abstained_frames,
            "frame_quality_contract": None if self.frame_quality is None else self.frame_quality.as_dict(),
            "longest_consecutive_abstention_run": longest_abstention_run,
            "consecutive_abstention_limit_reached": abstention_limit_reached,
            "initialization_frame_count": initialization_frames,
            "initialization_limit_exceeded": initialization_limit_hit,
            "freshness_policy": self.freshness_policy.as_dict(),
            "stop_reasons": stop_reasons,
            "clock": self.clock_contract.as_dict(),
            "artifact_provenance": self.artifact_provenance,
            "records": all_records,
            "latency_records": latency_records,
            "validation_records": validation_records,
        }
        if output_dir is not None:
            self._write_outputs(Path(output_dir), result, records_already_streamed=streams is not None)
        return result

    def run_offline(self, observations, *, source_kind: str, output_dir: Path | None = None,
                    recording_actuators=None, input_pins=None) -> dict[str, Any]:
        """Common validated replay; live APIs and callbacks are never permitted.

        Source observations are identical across arms. Explicit recording
        actuators confirm only in-memory changes and generate no performance.
        """
        from .common_replay import run_common_replay
        return run_common_replay(self, observations, source_kind=source_kind, output_dir=output_dir,
                                 recording_actuators=recording_actuators, input_pins=input_pins)

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
        if not self.shadow and len(self.controllers) != 1:
            raise IntegrationSafetyError("a live provider loop requires one controller for one observed network")
        provider_policy = getattr(provider, "freshness_policy", None)
        if provider_policy is not None and provider_policy.max_age_s != self.freshness_policy.max_age_s:
            # Two different limits would make a frame fresh for one side and stale for the other.
            raise IntegrationSafetyError(
                f"provider freshness limit {provider_policy.max_age_s}s differs from the runner's "
                f"{self.freshness_policy.max_age_s}s; configure one validated limit")

        def frames():
            for index in range(samples):
                if index and interval_s:
                    time.sleep(interval_s)
                yield provider.collect(run_id=self.run_id, decision_index=index)
        return self.run(frames(), output_dir=output_dir)

    @staticmethod
    def _rejection_summary(rejection: IntegrationSafetyError) -> dict[str, Any]:
        """Structured description of why a frame was rejected (JSON-safe; untyped errors have no code)."""
        return {"reason_code": getattr(rejection, "reason_code", None),
                "abstention_reason": getattr(rejection, "abstention_reason", None),
                "details": json.loads(json.dumps(getattr(rejection, "details", {}), default=str))}

    def _rejection_record(self, name: str, frame: "TelemetryFrame", rejection: IntegrationSafetyError) -> dict[str, Any]:
        """One strategy's abstention for a rejected frame: no state offered, nothing proposed or applied.

        Reactive and predictive controllers abstain through their own ``reject_observation`` (which also
        breaks their confirmation streaks); a controller without one records a fixed-configuration
        abstention, exactly as the common offline replay does. The structured cause stays in the record.
        """
        controller, actuator = self.controllers[name], self.actuators[name]
        reason = getattr(rejection, "abstention_reason", None) or "invalid_telemetry_fail_safe"
        error = sanitize_text(rejection)
        if hasattr(controller, "reject_observation"):
            decision = controller.reject_observation(reason, error=error)
        else:
            decision = ControllerDecision("static" if isinstance(controller, StaticController) else type(controller).__name__.lower(),
                                          actuator.current_config, False, reason, metadata={"error": error})
        decision.metadata["telemetry_rejection"] = {
            **self._rejection_summary(rejection), "message": error,
            "read_status": frame.provenance.get("read_status"),
            "read_failures": frame.provenance.get("read_failures"),
            "freshness_assessment": frame.provenance.get("freshness_assessment"),
            "critical_missing": list(frame.critical_missing),
            "inconsistencies": list(frame.inconsistencies),
        }
        return {"run_id": f"{self.run_id}-{name}", "state": None,
                "configuration_state": {"after": actuator.current_config.as_dict(), "remote_state_unknown": False,
                                        "proposal_applied": False},
                "decision": decision.as_dict(), "application": None}

    def _write_outputs(self, output_dir: Path, result: Mapping[str, Any], *, records_already_streamed: bool = False) -> None:
        manifest = {k: v for k, v in result.items() if k not in {"records", "latency_records"}}
        evidence = {"validation_records": result["validation_records"], "stop_reasons": result["stop_reasons"]}
        if records_already_streamed:
            for filename, value in (("integration_manifest.json", manifest), ("validation_evidence.json", evidence)):
                with (output_dir / filename).open("x", encoding="utf-8") as stream:
                    stream.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
            return
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


def _is_predictive(controller: BaseController) -> bool:
    return type(controller).__name__ == "PredictiveController"


def _decision_trace(strategy: str, record: Mapping[str, Any], frame: "TelemetryFrame", state: ControllerState,
                    input_digest: str, artifact: Mapping[str, Any] | None) -> dict[str, Any]:
    """Bounded, self-contained audit summary of one decision (size independent of history length)."""
    decision = record["decision"]
    meta = decision.get("metadata") or {}
    forecast = meta.get("forecast") or None
    trace_forecast = None
    if forecast:
        init = forecast.get("initialization_provenance") or {}
        trace_forecast = {key: forecast.get(key) for key in (
            "value_tps", "value_unit", "origin_timestamp_utc", "target_end_timestamp_utc",
            "history_last_timestamp_utc", "horizon_seconds", "frequency_seconds", "model_identity", "source")}
        trace_forecast["ewma_parameters"] = init.get("ewma_parameters")
    rows = meta.get("candidate_utilities", meta.get("candidate_evaluations", [])) or []
    candidates = [{"config": row.get("config"),
                   "model_input_features": (row.get("prediction_metadata") or {}).get("feature_values"),
                   "model_identity": (row.get("prediction_metadata") or {}).get("model_identity"),
                   "predicted_throughput_tps": row.get("throughput_tps"),
                   "predicted_latency_ms": row.get("latency_ms"),
                   "utility": row.get("utility")} for row in rows]
    return {
        "strategy": strategy,
        "observation_timestamp_utc": frame.observation_timestamp_utc,
        # Taken from the shared frame (what every arm was given), not from an arm's own copy.
        "state_timestamp_utc": frame.state.timestamp_utc,
        "input_digest": input_digest,
        "history_reference": None if frame.state.arrival_history_ref is None else frame.state.arrival_history_ref.as_dict(),
        "forecast": trace_forecast,
        "hfbtp_artifact": None if artifact is None else dict(artifact),
        "candidates": candidates,
        "decision": {"controller": decision.get("controller"), "outcome": decision.get("outcome"),
                     "reason": decision.get("reason"), "changed": decision.get("changed"),
                     "config": decision.get("config"),
                     "predicted_arrival_rate_tps": decision.get("predicted_arrival_rate_tps"),
                     "predicted_performance": decision.get("predicted_performance"),
                     "error": meta.get("error"), "abstention_stage": meta.get("abstention_stage"),
                     "hold_cause": meta.get("hold_cause")},
    }

def load_jsonl_frames(path: Path, *, run_id: str, strict_config: bool = False) -> list[TelemetryFrame]:
    """Load a deterministic replay stream and reject future-data fields.

    ``strict_config=True`` additionally refuses any frame without a complete canonical
    ``current_config`` instead of defaulting it (see ``TelemetryFrame.from_mapping``).
    """
    frames: list[TelemetryFrame] = []
    with Path(path).open(encoding="utf-8") as fh:
        for index, line in enumerate(fh):
            if not line.strip():
                continue
            row = json.loads(line)
            forbidden = {"future", "future_arrivals", "future_observations", "actual_future"} & set(row)
            if forbidden:
                raise IntegrationSafetyError(f"future-data fields present at line {index + 1}: {sorted(forbidden)}")
            frames.append(TelemetryFrame.from_mapping(row, run_id=run_id, decision_index=len(frames),
                                                      strict_config=strict_config))
    if not frames:
        raise ValueError(f"telemetry snapshot file is empty: {path}")
    return frames

