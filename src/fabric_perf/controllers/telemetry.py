"""Read-only Fabric telemetry adapters with explicit provenance.

Prometheus does not expose the client-side VALID transaction latency used by
the E1 benchmark. This module therefore keeps Prometheus orderer/peer metrics
and Caliper/ledger observations as separate sources and represents unavailable
values explicitly instead of substituting a related metric.
"""

from __future__ import annotations

import csv
import json
import math
import time
import urllib.request
from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Deque, Iterable, Mapping

from .base import ControllerState, FabricConfig, confirmed_config
from .client_latency import (
    EXPLICIT_COLUMN_TOLERANCE_MS,
    LATENCY_DEFINITION,
    LATENCY_SCHEMA_VERSION,
    LATENCY_UNIT,
    PERCENTILE_METHOD,
    LatencyState,
    derive_latency_ms,
    nearest_rank,
)
from .ledger_evidence import (
    LedgerEvidenceError,
    LedgerRate,
    ObservationRow,
    parse_observation_row,
    windowed_committed_rate,
)
from .metric_baseline import (
    BaselineState,
    CounterDelta,
    DeltaStatus,
    HistogramObservation,
    assess_delta,
    observe_histogram,
)
from .telemetry_errors import (
    TelemetryReadFailure,
    aggregate_read_status,
    bounded_details,
    failure_from_category,
    failure_from_exception,
    sanitize_text,
)


class TelemetryDataError(ValueError):
    """Telemetry is missing, invalid, or has inconsistent units."""


class TelemetryTimestampError(TelemetryDataError):
    """An observation has no usable timestamp (the failure category is ``missing_timestamp``)."""

    telemetry_category = "missing_timestamp"


class TelemetryTimestampConflict(TelemetryTimestampError):
    """A supplied timestamp contradicts the source's own timestamp (``timestamp_conflict``)."""

    telemetry_category = "timestamp_conflict"


def parse_observation_time(value: str, *, field: str = "timestamp_utc") -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("timezone is required")
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, AttributeError) as exc:
        raise TelemetryDataError(f"invalid {field}: {value!r}; timezone-aware ISO timestamp required") from exc


@dataclass(frozen=True)
class MetricObservation:
    name: str
    value: float | None
    unit: str
    timestamp_utc: str
    source: str
    sampling_interval_s: float | None
    missing_reason: str | None = None
    staleness_threshold_s: float = 5.0
    observation_timestamp_utc: str | None = None
    window_start_utc: str | None = None
    window_end_utc: str | None = None
    run_id: str | None = None
    incomplete_window: bool = False

    def __post_init__(self) -> None:
        if self.value is not None:
            if isinstance(self.value, bool) or not isinstance(self.value, (int, float)) or not math.isfinite(self.value):
                raise TelemetryDataError(f"metric {self.name} must be finite numeric data")
            nonnegative = {"offered_tps", "delivered_valid_tps", "latency_mean_ms", "latency_p95_ms",
                           "inflight_backlog", "failed_transactions", "submitted_transactions",
                           "valid_transactions", "invalid_transactions", "failure_rate"}
            if self.name in nonnegative and self.value < 0:
                raise TelemetryDataError(f"metric {self.name} cannot be negative")
            if self.name == "failure_rate" and self.value > 1:
                raise TelemetryDataError("failure_rate must be a fraction in [0,1]")
            if self.name.endswith("transactions") and not float(self.value).is_integer():
                raise TelemetryDataError(f"metric {self.name} must be a whole transaction count")
        if self.value is None and not self.missing_reason:
            raise TelemetryDataError(f"metric {self.name} is missing without a reason")
        if isinstance(self.staleness_threshold_s, bool) or not math.isfinite(self.staleness_threshold_s) or self.staleness_threshold_s <= 0:
            raise TelemetryDataError(f"metric {self.name} has a non-positive staleness threshold")
        parse_observation_time(self.timestamp_utc)
        if self.sampling_interval_s is not None and (isinstance(self.sampling_interval_s, bool)
                or not math.isfinite(self.sampling_interval_s) or self.sampling_interval_s < 0):
            raise TelemetryDataError("sampling interval must be finite and non-negative")
        for field_name in ("observation_timestamp_utc", "window_start_utc", "window_end_utc"):
            value = getattr(self, field_name)
            if value is not None:
                parse_observation_time(value, field=field_name)
        if self.window_start_utc and self.window_end_utc and parse_observation_time(self.window_start_utc) > parse_observation_time(self.window_end_utc):
            raise TelemetryDataError("metric measurement window runs backwards")

    def is_stale(self, *, now_utc: str | None = None) -> bool:
        if self.value is None:
            return False
        now = datetime.now(timezone.utc) if now_utc is None else parse_observation_time(now_utc)
        observed = parse_observation_time(self.timestamp_utc)
        age = (now - observed).total_seconds()
        if age < 0:
            raise TelemetryDataError(f"metric {self.name} is timestamped in the future")
        return age > self.staleness_threshold_s

    def as_dict(self) -> dict:
        return self.__dict__.copy()


@dataclass(frozen=True)
class FabricTelemetrySnapshot:
    channel_id: str
    timestamp_utc: str
    metrics: Mapping[str, MetricObservation]
    current_config: FabricConfig | None
    health: Mapping[str, object]

    def metric(self, name: str) -> MetricObservation:
        if name not in self.metrics:
            raise TelemetryDataError(f"required metric {name!r} is not present")
        return self.metrics[name]

    def to_controller_state(self, *, arrival_history_tps: Iterable[float] = (),
                            arrival_history_timestamps_utc: Iterable[str] = (),
                            now_utc: str | None = None, require_fresh: bool = True,
                            allow_unknown_config: bool = False,
                            arrival_history_ref: Any = None,
                            allow_future: bool = False) -> ControllerState:
        """Build a controller state.

        ``allow_future`` is for the frame provider only. By default a future-dated source raises. With it,
        the state's own ``telemetry_age_s`` is clamped to zero so a state can be built at all, while the
        provider keeps the honest negative age in the frame's ``freshness_seconds``, where
        ``TelemetryFrame.validate`` rejects it as future-dated. The clamped age is never the only record.
        """
        if require_fresh:
            stale = [name for name, observation in self.metrics.items() if observation.is_stale(now_utc=now_utc)]
            if stale:
                raise TelemetryDataError(f"stale telemetry metrics: {sorted(stale)}")
        def value(name: str) -> float | None:
            obs = self.metrics.get(name)
            return None if obs is None else obs.value

        expected_units = {"offered_tps": "transactions/s", "delivered_valid_tps": "VALID transactions/s",
                          "latency_mean_ms": "ms", "latency_p95_ms": "ms",
                          "inflight_backlog": "transactions", "failed_transactions": "transactions"}
        for name, unit in expected_units.items():
            observation = self.metrics.get(name)
            if observation is not None and observation.value is not None and observation.unit != unit:
                raise TelemetryDataError(f"metric {name} must use {unit}, got {observation.unit}")

        if self.current_config is None and not allow_unknown_config:
            raise TelemetryDataError("current configuration is unavailable")
        # Provider-only placeholder: the frame MUST list current_config as
        # critical_missing and validation rejects it before a decision.
        config = FabricConfig() if self.current_config is None else confirmed_config(self.current_config)
        reference = datetime.now(timezone.utc) if now_utc is None else parse_observation_time(now_utc)
        observed_at = parse_observation_time(self.timestamp_utc)
        ages = [(reference - observed_at).total_seconds()]
        ages.extend((reference - parse_observation_time(obs.timestamp_utc)).total_seconds()
                    for name, obs in self.metrics.items() if name in expected_units and obs.value is not None)
        if any(age < 0 for age in ages):
            if not allow_future:
                raise TelemetryDataError("controller telemetry is timestamped in the future")
            ages = [max(0.0, age) for age in ages]
        telemetry_age_s = max(ages)
        return ControllerState(
            timestamp_utc=self.timestamp_utc,
            current_config=config,
            observed_arrival_rate_tps=value("offered_tps"),
            observed_throughput_tps=value("delivered_valid_tps"),
            observed_latency_ms=value("latency_mean_ms"),
            observed_latency_p95_ms=value("latency_p95_ms"),
            queue_depth=value("inflight_backlog"),
            failed_transactions=None if value("failed_transactions") is None else int(value("failed_transactions")),
            arrival_history_tps=tuple(arrival_history_tps),
            arrival_history_timestamps_utc=tuple(arrival_history_timestamps_utc),
            telemetry_age_s=telemetry_age_s,
            arrival_history_ref=arrival_history_ref,
        )

    def as_dict(self) -> dict:
        return {
            "channel_id": self.channel_id,
            "timestamp_utc": self.timestamp_utc,
            "metrics": {k: v.as_dict() for k, v in self.metrics.items()},
            "current_config": None if self.current_config is None else self.current_config.as_dict(),
            "health": dict(self.health),
        }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _metric(
    name: str,
    value: float | None,
    *,
    unit: str,
    timestamp_utc: str,
    source: str,
    sampling_interval_s: float | None,
    missing_reason: str | None = None,
    staleness_threshold_s: float = 5.0,
    observation_timestamp_utc: str | None = None,
    window_start_utc: str | None = None,
    window_end_utc: str | None = None,
    run_id: str | None = None,
    incomplete_window: bool = False,
) -> MetricObservation:
    if value is not None:
        value = float(value)
    return MetricObservation(name, value, unit, timestamp_utc, source, sampling_interval_s, missing_reason,
                             staleness_threshold_s, observation_timestamp_utc, window_start_utc, window_end_utc,
                             run_id, incomplete_window)


def _labels_text(labels: tuple[tuple[str, str], ...]) -> str:
    return ",".join(f'{k}="{v}"' for k, v in labels)


def parse_prometheus_text(text: str) -> list[tuple[str, tuple[tuple[str, str], ...], float]]:
    """Parse the sample subset needed by the E1 operations endpoints."""
    rows: list[tuple[str, tuple[tuple[str, str], ...], float]] = []
    for line_no, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        head, *tail = line.split()
        if len(tail) not in (1, 2):
            raise TelemetryDataError(f"invalid Prometheus sample at line {line_no}")
        if len(tail) == 2:
            raise TelemetryDataError(f"explicit Prometheus sample timestamp is unsupported at line {line_no}")
        try:
            value = float(tail[0])
        except ValueError as exc:
            raise TelemetryDataError(f"invalid Prometheus value at line {line_no}") from exc
        if not math.isfinite(value):
            raise TelemetryDataError(f"non-finite Prometheus value at line {line_no}")
        labels: tuple[tuple[str, str], ...] = ()
        if "{" in head:
            if not head.endswith("}"):
                raise TelemetryDataError(f"invalid Prometheus labels at line {line_no}")
            metric, label_text = head.split("{", 1)
            label_text = label_text[:-1]
            parsed = []
            if label_text:
                for part in label_text.split(","):
                    key, sep, val = part.partition("=")
                    if sep != "=" or len(val) < 2 or not val.startswith('"') or not val.endswith('"'):
                        raise TelemetryDataError(f"invalid Prometheus label at line {line_no}")
                    parsed.append((key, val[1:-1]))
            labels = tuple(sorted(parsed))
        else:
            metric = head
        rows.append((metric, labels, value))
    return rows


FetchMetrics = Callable[[str, float], str]


def fetch_prometheus(url: str, timeout_s: float = 2.0) -> str:
    request = urllib.request.Request(url, headers={"Accept": "text/plain;version=0.0.4"})
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        return response.read().decode("utf-8")


class PrometheusTelemetryAdapter:
    """Collect read-only orderer/peer observations from E1 operations URLs."""

    def __init__(self, endpoints: Mapping[str, str], *, fetch: FetchMetrics = fetch_prometheus, timeout_s: float = 2.0):
        self.endpoints = dict(endpoints)
        if not self.endpoints:
            raise ValueError("at least one Prometheus endpoint is required")
        self.fetch = fetch
        self.timeout_s = timeout_s
        # Per orderer: the previous block-fill reading (valid or not) and when it was taken. The
        # previous reading is kept even when it is not valid, so a delta after a gap is reported
        # as unavailable with its real reason instead of being computed across the gap.
        self._fill_previous: dict[str, HistogramObservation] = {}
        self._fill_previous_mono: dict[str, float] = {}

    @staticmethod
    def _on_channel(labels: tuple[tuple[str, str], ...], channel_id: str) -> bool:
        return dict(labels).get("channel") == channel_id

    def collect(
        self,
        channel_id: str,
        *,
        timestamp_utc: str | None = None,
        current_config: FabricConfig | None = None,
        caliper: FabricTelemetrySnapshot | None = None,
        ledger: Mapping[str, object] | None = None,
        health: Mapping[str, object] | None = None,
    ) -> FabricTelemetrySnapshot:
        timestamp = timestamp_utc or _utc_now()
        now = time.monotonic()
        rows: list[tuple[str, tuple[tuple[str, str], ...], float, str]] = []
        endpoint_errors: dict[str, str] = {}
        failures: dict[str, TelemetryReadFailure] = {}
        outcomes: dict[str, str] = {}
        parsed_by_target: dict[str, list[tuple[str, tuple[tuple[str, str], ...], float]]] = {}
        for target, url in self.endpoints.items():
            try:
                parsed = parse_prometheus_text(self.fetch(url, self.timeout_s))
                if not parsed:
                    raise TelemetryDataError("endpoint returned no metric samples")
            except Exception as exc:
                failure = failure_from_exception(f"prometheus:{target}", exc, kind="network", endpoint=url)
                if isinstance(exc, TelemetryDataError) and "no metric samples" in str(exc):
                    failure = failure_from_category(f"prometheus:{target}", "empty_response", str(exc), endpoint=url)
                failures[target] = failure
                outcomes[target] = failure.outcome
                # Legacy string form, kept for existing consumers; now sanitized and bounded.
                endpoint_errors[target] = sanitize_text(f"{type(exc).__name__}: {exc}")
                continue
            outcomes[target] = "ok"
            parsed_by_target[target] = parsed
            rows.extend((name, labels, value, target) for name, labels, value in parsed)
        unreadable = sorted(target for target, outcome in outcomes.items() if outcome != "ok")

        def channel_rows(metric_name: str) -> list[tuple[float, str, tuple[tuple[str, str], ...]]]:
            return [(value, target, labels) for name, labels, value, target in rows
                    if name == metric_name and self._on_channel(labels, channel_id)]

        observations: dict[str, MetricObservation] = {}
        leader_rows = channel_rows("consensus_etcdraft_is_leader")
        leaders = sum(1 for value, _, _ in leader_rows if value == 1.0)
        if unreadable:
            # A leader count is a cross-endpoint aggregate. Counting over the endpoints that answered
            # would report zero leaders whenever the leader's own endpoint is the one that failed.
            leader_value, leader_reason = None, (f"partial endpoint observation: {unreadable} unreadable, so the "
                                                 "leader count over the remaining endpoints is not a complete view")
        elif not leader_rows:
            leader_value, leader_reason = None, "leader metric unavailable for channel"
        else:
            leader_value, leader_reason = leaders, None
        observations["raft_leader_count"] = _metric("raft_leader_count", leader_value, unit="leaders", timestamp_utc=timestamp,
                                                     source="Fabric Prometheus consensus_etcdraft_is_leader", sampling_interval_s=None,
                                                     missing_reason=leader_reason)

        # Block fill is a per-orderer counter delta between two valid readings (owner decision D3,
        # 2026-11-17). An absent series, a failed or malformed scrape, or a counter reset gives no
        # delta, and an absent baseline is never read as zero. See metric_baseline for the six
        # reading states and for why this differs from a ledger block-height delta.
        fill_readings: dict[str, HistogramObservation] = {}
        fill_deltas: dict[str, CounterDelta] = {}
        fill_intervals: list[float] = []
        for target in self.endpoints:
            if outcomes[target] == "failed":
                reading = HistogramObservation(BaselineState.SCRAPE_FAILED, detail=failures[target].message)
            elif outcomes[target] == "invalid":
                reading = HistogramObservation(BaselineState.MALFORMED, detail=failures[target].message)
            else:
                reading = observe_histogram(parsed_by_target[target], channel=channel_id)
            delta = assess_delta(self._fill_previous.get(target), reading)
            if delta.status is DeltaStatus.AVAILABLE and target in self._fill_previous_mono:
                fill_intervals.append(max(0.0, now - self._fill_previous_mono[target]))
            fill_readings[target], fill_deltas[target] = reading, delta
            self._fill_previous[target] = reading
            self._fill_previous_mono[target] = now
        fill_unresolved = sorted(t for t, d in fill_deltas.items()
                                 if d.status is DeltaStatus.UNAVAILABLE and not d.never_observed)
        fill_available = [d for d in fill_deltas.values() if d.status is DeltaStatus.AVAILABLE]
        fill_value: float | None = None
        if fill_unresolved:
            fill_reason: str | None = "block-fill delta unavailable: " + "; ".join(
                f"{t} ({fill_deltas[t].reason})" for t in fill_unresolved)
        elif not fill_available:
            fill_reason = "no orderer reports a block-fill series for this channel, so there is nothing to difference"
        else:
            fill_count_delta = sum(d.count_delta for d in fill_available)
            if fill_count_delta > 0:
                fill_value, fill_reason = sum(d.sum_delta for d in fill_available) / fill_count_delta, None
            else:
                fill_reason = "no block was cut in this interval (a valid zero delta)"
        observations["block_fill_duration_s"] = _metric(
            "block_fill_duration_s", fill_value, unit="s", timestamp_utc=timestamp,
            source="Fabric Prometheus blockcutter_block_fill_duration sum/count delta",
            sampling_interval_s=min(fill_intervals) if fill_intervals else None,
            missing_reason=fill_reason,
        )
        observations["block_transaction_count"] = _metric(
            "block_transaction_count", (ledger or {}).get("block_transaction_count"), unit="transactions/block", timestamp_utc=timestamp,
            source="ledger block reader", sampling_interval_s=None,
            missing_reason=None if ledger and (ledger or {}).get("block_transaction_count") is not None else "not exposed by Prometheus adapter; provide verified ledger observation",
        )
        observations["orderer_peer_health"] = _metric(
            "orderer_peer_health", 1.0 if not endpoint_errors else 0.0, unit="boolean", timestamp_utc=timestamp,
            source="Prometheus endpoint availability", sampling_interval_s=None,
            missing_reason=None,
        )
        observations["raft_quorum"] = _metric(
            "raft_quorum", None, unit="boolean", timestamp_utc=timestamp,
            source="Fabric health/quorum verifier", sampling_interval_s=None,
            missing_reason="provide independent three-node quorum observation",
        )

        if caliper is not None:
            observations.update(caliper.metrics)
        else:
            for name, unit, reason in (
                ("offered_tps", "transactions/s", "Caliper submission telemetry not supplied"),
                ("delivered_valid_tps", "VALID transactions/s", "Caliper/ledger telemetry not supplied"),
                ("latency_mean_ms", "ms", "client transaction telemetry not supplied"),
                ("latency_p95_ms", "ms", "client transaction telemetry not supplied"),
                ("inflight_backlog", "transactions", "Caliper transaction timestamps not supplied"),
                ("failed_transactions", "transactions", "Caliper transaction statuses not supplied"),
                ("failure_rate", "fraction", "Caliper transaction statuses not supplied"),
            ):
                observations[name] = _metric(name, None, unit=unit, timestamp_utc=timestamp, source="Caliper/ledger", sampling_interval_s=None, missing_reason=reason)
        snapshot_health = {
            "endpoint_errors": endpoint_errors,
            "endpoint_error_details": {target: failure.as_dict() for target, failure in failures.items()},
            "read_status": aggregate_read_status(outcomes),
            "block_fill": {target: {"reading": fill_readings[target].as_dict(), "delta": fill_deltas[target].as_dict()}
                           for target in fill_deltas},
            **dict(health or {}),
        }
        return FabricTelemetrySnapshot(channel_id, timestamp, observations, current_config, snapshot_health)


def caliper_snapshot_from_summary(path: Path, *, channel_id: str | None = None, timestamp_utc: str | None = None) -> FabricTelemetrySnapshot:
    """Convert an existing E1 benchmark summary into a provenance-rich snapshot.

    The observation timestamp is the round's own end time from the recorder manifest
    (``clock.wall1_ms``). It is the time the data was observed, so re-reading the same summary
    later yields the same timestamp and a larger age; it is never refreshed to "now". An explicit
    ``timestamp_utc`` is accepted only if it agrees with the manifest (1 ms); otherwise it would be
    a refreshed timestamp and is refused. Without a manifest an explicit value cannot be verified
    and is recorded as ``explicit_unverified``.
    """
    summary_path = Path(path)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    metrics = summary.get("metrics", {})
    counts = summary.get("counts", {})
    inputs = summary.get("inputs", {})
    manifest_path = inputs.get("manifest", {}).get("path")
    manifest_time: datetime | None = None
    if manifest_path and Path(manifest_path).is_file():
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        wall_ms = manifest.get("clock", {}).get("wall1_ms")
        if wall_ms is not None:
            try:
                manifest_time = datetime.fromtimestamp(float(wall_ms) / 1000, tz=timezone.utc)
            except (TypeError, ValueError, OverflowError, OSError) as exc:
                raise TelemetryTimestampError(f"recorder manifest clock.wall1_ms is not a usable time: {wall_ms!r}") from exc
    explicit_time = None if timestamp_utc is None else parse_observation_time(timestamp_utc, field="timestamp_utc")
    if explicit_time is not None and manifest_time is not None \
            and abs((explicit_time - manifest_time).total_seconds()) > 0.001:
        raise TelemetryTimestampConflict(
            "explicit observation timestamp contradicts the recorder manifest round-end time; a re-read summary "
            "keeps its original timestamp and is not refreshed")
    if explicit_time is not None:
        observed_timestamp = timestamp_utc
        timestamp_source = "recorder_manifest_wall1_ms" if manifest_time is not None else "explicit_unverified"
    elif manifest_time is not None:
        observed_timestamp = manifest_time.isoformat(timespec="milliseconds")
        timestamp_source = "recorder_manifest_wall1_ms"
    else:
        raise TelemetryTimestampError("Caliper summary needs an explicit observation timestamp or a readable recorder manifest")
    wall_elapsed = float(metrics.get("round_wall_elapsed_s") or 0.0)
    source = f"Caliper/ledger summary:{summary_path}"
    values = {
        "offered_tps": (metrics.get("offered_tps_from_submit_events"), "transactions/s"),
        "delivered_valid_tps": ((float(counts["valid_workload_ledger_transactions"]) / wall_elapsed) if wall_elapsed > 0 and "valid_workload_ledger_transactions" in counts else None, "VALID transactions/s"),
        "latency_mean_ms": (metrics.get("latency_ms_mean"), "ms"),
        "latency_p95_ms": (metrics.get("latency_ms_p95_nearest_rank"), "ms"),
        "inflight_backlog": (metrics.get("max_inflight_backlog"), "transactions"),
        "failed_transactions": (counts.get("failed"), "transactions"),
        "failure_rate": ((float(counts["failed"]) / float(counts["submitted"])) if counts.get("submitted") else None, "fraction"),
    }
    observations = {
        name: _metric(name, value, unit=unit, timestamp_utc=observed_timestamp, source=source,
                      sampling_interval_s=wall_elapsed or None,
                      missing_reason="field absent from E1 summary" if value is None else None)
        for name, (value, unit) in values.items()
    }
    return FabricTelemetrySnapshot(
        channel_id=str(channel_id or summary.get("configuration", {}).get("channel_id", summary.get("run_id", "unknown"))),
        timestamp_utc=observed_timestamp,
        metrics=observations,
        current_config=None,
        health={"summary_analysis_verdict": summary.get("analysis_verdict"),
                "observation_timestamp_source": timestamp_source,
                "observation_timestamp_policy": "original round-end time; never refreshed when the summary is re-read"},
    )


def _epoch_ms(value: object, *, field: str) -> datetime:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise TelemetryDataError(f"{field} must be epoch milliseconds") from exc
    if not math.isfinite(number):
        raise TelemetryDataError(f"{field} must be finite")
    return datetime.fromtimestamp(number / 1000.0, tz=timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds")


class _AppendOnlyCsvTail:
    """Read only complete, newly appended CSV rows without loading the file."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.offset = 0
        self.header: str | None = None
        self.partial = ""

    def read_rows(self) -> list[dict[str, str]]:
        if not self.path.is_file():
            return []
        size = self.path.stat().st_size
        if size < self.offset:
            raise TelemetryDataError(f"append-only source was truncated or replaced: {self.path}")
        with self.path.open("rb") as fh:
            fh.seek(self.offset)
            chunk = fh.read()
            self.offset = fh.tell()
        text = self.partial + chunk.decode("utf-8")
        lines = text.splitlines(keepends=True)
        if lines and not lines[-1].endswith(("\n", "\r")):
            self.partial = lines.pop()
        else:
            self.partial = ""
        if self.header is None:
            if not lines:
                return []
            self.header = lines.pop(0).strip("\r\n")
        if not lines:
            return []
        import io
        reader = csv.DictReader(io.StringIO(self.header + "\n" + "".join(lines)))
        return [dict(row) for row in reader]


class ContinuousCaliperLedgerAdapter:
    """Join append-only Caliper recorder/submission and ledger CSV streams.

    This adapter treats Caliper ``create_ms``/``final_ms`` as client-side
    timestamps. Ledger ``ch_ts_ms`` is retained as a channel-header timestamp;
    it is never treated as a commit timestamp. Ledger delivery rates use the
    local observation time at which newly appended ledger rows were discovered.

    ``recorder_latency`` says where client latency comes from (task A0c, G1):

    * ``"column"`` (default, unchanged): the recorder CSV must carry a ``latency_ms`` column. A file without it is
      reported as a schema error and its rows are rejected, never guessed.
    * ``"derive_from_timestamps"`` (explicit opt-in): latency is ``final_ms - create_ms`` of each successful row, the
      project's verified Caliper definition (see ``client_latency``). This reads the v1 recorder schema, which has no
      ``latency_ms`` column. A file that also carries the column must agree with the timestamps. A successful row
      whose interval cannot be computed is rejected; a failed row without a valid interval is counted as failed and
      contributes no latency. A missing latency is never zero.

    ``ledger_observations_csv`` (task A0d, explicit opt-in) names the append-only observation journal written by
    ``ledger_evidence.LedgerCollector``. With it, committed throughput (``delivered_valid_tps``) and the
    ``valid_transactions``/``invalid_transactions`` window counts come from LEDGER OBSERVATION TIME: whole observed
    intervals between channel-height reads, rate = VALID endorser transactions / elapsed observed time. The metric is
    stamped with the end of the newest interval used, so a collector that stops makes it stale instead of leaving it
    fresh by construction, and a first observation (no interval yet) is a missing value, never zero. Without it, the
    behaviour is unchanged: the ledger rate is taken over the time between this adapter's own ``collect`` calls.
    """

    RECORDER_LATENCY_MODES = ("column", "derive_from_timestamps")

    def __init__(self, *, recorder_csv: Path, submits_csv: Path, ledger_csv: Path,
                 channel_id: str, run_id: str, window_s: float = 5.0,
                 stale_after_s: float = 5.0, recorder_latency: str = "column",
                 ledger_observations_csv: Path | None = None):
        if window_s <= 0 or stale_after_s <= 0:
            raise ValueError("window_s and stale_after_s must be positive")
        if recorder_latency not in self.RECORDER_LATENCY_MODES:
            raise ValueError(f"recorder_latency must be one of {self.RECORDER_LATENCY_MODES}")
        self.recorder_latency = recorder_latency
        # Rows seen without a final timestamp (not completed, or no id): never counted, never zero-filled.
        self._incomplete_rows = 0
        self.recorder = _AppendOnlyCsvTail(Path(recorder_csv))
        self.submits = _AppendOnlyCsvTail(Path(submits_csv))
        self.ledger = _AppendOnlyCsvTail(Path(ledger_csv))
        self.ledger_observations = None if ledger_observations_csv is None else _AppendOnlyCsvTail(Path(ledger_observations_csv))
        self._ledger_obs: list[ObservationRow] = []
        self.channel_id = str(channel_id)
        self.run_id = str(run_id)
        self.window_s = float(window_s)
        self.stale_after_s = float(stale_after_s)
        self._submit_events: Deque[tuple[datetime, int]] = deque()
        self._record_events: Deque[tuple[datetime, float | None, str, str]] = deque()
        self._ledger_events: Deque[tuple[datetime, str, int, datetime | None]] = deque()
        self._record_ids: set[str] = set()
        self._ledger_ids: set[str] = set()
        self._record_status: dict[str, str] = {}
        self._submitted_total = 0
        self._completed_total = 0
        self._failed_total = 0
        self._valid_total = 0
        self._invalid_total = 0
        self._duplicate_ids = 0
        self._last_observation: datetime | None = None
        # Cumulative integrity record (live-shadow fix 3). The streams are append-only and are
        # read once, so a row that is rejected, or a chunk lost to an exception, is gone for good.
        # Totals derived from that stream (backlog, valid counts, rates) stay untrustworthy for the
        # life of this adapter instead of looking healthy again once the error scrolls out of view.
        self._rejected = {"recorder": 0, "submits": 0, "ledger": 0}
        self._stream_lost = {"recorder": False, "submits": False, "ledger": False}

    @staticmethod
    def _status_success(status: str) -> bool:
        return status.lower() in {"success", "succeeded", "valid"}

    def _metric(self, name: str, value: float | None, *, unit: str, source: str,
                timestamp: datetime, observation: datetime, window_start: datetime,
                interval_s: float | None, missing: str | None = None,
                incomplete: bool = False) -> MetricObservation:
        return _metric(name, value, unit=unit, timestamp_utc=_iso(timestamp), source=source,
                       sampling_interval_s=interval_s, missing_reason=missing,
                       staleness_threshold_s=self.stale_after_s,
                       observation_timestamp_utc=_iso(observation),
                       window_start_utc=_iso(window_start), window_end_utc=_iso(observation),
                       run_id=self.run_id, incomplete_window=incomplete)

    @staticmethod
    def _window(rows: Iterable[tuple], index: int, start: datetime) -> list[tuple]:
        return [row for row in rows if row[index] >= start]

    def collect(self, *, observation_timestamp_utc: str | None = None) -> FabricTelemetrySnapshot:
        observation = datetime.now(timezone.utc) if observation_timestamp_utc is None else datetime.fromisoformat(
            observation_timestamp_utc.replace("Z", "+00:00"))
        observation = observation.astimezone(timezone.utc)
        window_start = observation.timestamp() - self.window_s
        start = datetime.fromtimestamp(window_start, tz=timezone.utc)
        errors: list[str] = []
        failures: list[TelemetryReadFailure] = []
        row_failures: dict[str, tuple[int, TelemetryReadFailure]] = {}

        def reject_row(stream: str, failure: TelemetryReadFailure) -> None:
            self._rejected[stream] += 1
            count, first = row_failures.get(stream, (0, failure))
            row_failures[stream] = (count + 1, first)

        def lose_stream(stream: str, exc: BaseException) -> None:
            # An exception can discard rows already consumed from an append-only stream.
            self._stream_lost[stream] = True
            failures.append(failure_from_exception(f"continuous:{stream}", exc, kind="file"))

        for stream, source in (("recorder", self.recorder), ("submits", self.submits), ("ledger", self.ledger)):
            if not source.path.is_file():
                errors.append(f"{stream}_missing:{source.path}")
                failures.append(failure_from_category(f"continuous:{stream}", "file_missing",
                                                      f"{stream} file not found: {source.path}",
                                                      error_type="FileNotFoundError"))
        try:
            for row in self.submits.read_rows():
                event_time = _epoch_ms(row.get("t_submit_evt_ms"), field="t_submit_evt_ms")
                count = int(row.get("count", "1"))
                if count < 1:
                    raise TelemetryDataError("submit count must be positive")
                self._submit_events.append((event_time, count))
                self._submitted_total += count
        except (OSError, UnicodeError, TelemetryDataError, ValueError) as exc:
            errors.append(f"submits_read_error:{type(exc).__name__}:{sanitize_text(exc)}")
            lose_stream("submits", exc)
        try:
            for row in self.recorder.read_rows():
                tx_id = str(row.get("tx_id", ""))
                final_value = row.get("final_ms")
                if not tx_id or final_value in (None, ""):
                    self._incomplete_rows += 1
                    continue
                if tx_id in self._record_ids:
                    self._duplicate_ids += 1
                    continue
                final_time = _epoch_ms(final_value, field="final_ms")
                status = str(row.get("status", ""))
                if self.recorder_latency == "column":
                    try:
                        latency_ms = float(row["latency_ms"])
                    except (KeyError, TypeError, ValueError) as exc:
                        errors.append(f"recorder_latency_error:{type(exc).__name__}:{sanitize_text(exc)}")
                        reject_row("recorder", failure_from_exception("continuous:recorder", exc, kind="file"))
                        continue
                    if latency_ms < 0 or not math.isfinite(latency_ms):
                        errors.append(f"negative_or_nonfinite_latency:{tx_id}")
                        reject_row("recorder", failure_from_category(
                            "continuous:recorder", "invalid_value", f"negative or non-finite latency for transaction {tx_id}"))
                        continue
                else:
                    sample = derive_latency_ms(row.get("create_ms"), final_value)
                    problem: tuple[str, str] | None = None
                    explicit = row.get("latency_ms")
                    if explicit not in (None, ""):
                        try:
                            explicit_value = float(explicit)
                        except (TypeError, ValueError):
                            problem = ("invalid_value", f"latency_ms column is not numeric for transaction {tx_id}")
                        else:
                            if not math.isfinite(explicit_value) or explicit_value < 0:
                                problem = ("invalid_value", f"negative or non-finite latency for transaction {tx_id}")
                            elif sample.ok and abs(explicit_value - sample.latency_ms) > EXPLICIT_COLUMN_TOLERANCE_MS:
                                problem = ("invalid_value", f"latency_ms column disagrees with final_ms - create_ms "
                                                            f"for transaction {tx_id}")
                    if problem is None and self._status_success(status) and not sample.ok:
                        category = "schema_error" if sample.state in (LatencyState.MISSING_CREATE,
                                                                       LatencyState.MISSING_FINAL) else "invalid_value"
                        problem = (category, f"successful transaction {tx_id} has no valid latency ({sample.state.value})")
                    if problem is not None:
                        errors.append(f"recorder_latency_error:{problem[0]}:{tx_id}")
                        reject_row("recorder", failure_from_category("continuous:recorder", problem[0], problem[1]))
                        continue
                    # None only for a failed transaction without a valid interval: it counts as failed and
                    # contributes no latency. It is never replaced by zero.
                    latency_ms = sample.latency_ms
                self._record_ids.add(tx_id)
                self._record_status[tx_id] = status
                self._record_events.append((final_time, latency_ms, status, tx_id))
                self._completed_total += 1
                if not self._status_success(status):
                    self._failed_total += 1
        except (OSError, UnicodeError, TelemetryDataError, ValueError) as exc:
            errors.append(f"recorder_read_error:{type(exc).__name__}:{sanitize_text(exc)}")
            lose_stream("recorder", exc)
        ledger_new: list[tuple[datetime, str, int, datetime | None]] = []
        try:
            for row in self.ledger.read_rows():
                tx_id = str(row.get("tx_id", ""))
                if not tx_id or tx_id in self._ledger_ids:
                    if tx_id:
                        self._duplicate_ids += 1
                    continue
                try:
                    validation_code = int(row.get("validation_code", ""))
                except (TypeError, ValueError) as exc:
                    errors.append(f"ledger_validation_code_error:{type(exc).__name__}:{sanitize_text(exc)}")
                    reject_row("ledger", failure_from_exception("continuous:ledger", exc, kind="file"))
                    continue
                header_time = None
                if row.get("ch_ts_ms") not in (None, ""):
                    header_time = _epoch_ms(row["ch_ts_ms"], field="ch_ts_ms")
                item = (observation, tx_id, validation_code, header_time)
                self._ledger_events.append(item)
                ledger_new.append(item)
                self._ledger_ids.add(tx_id)
                if validation_code == 0:
                    self._valid_total += 1
                else:
                    self._invalid_total += 1
        except (OSError, UnicodeError, TelemetryDataError, ValueError) as exc:
            errors.append(f"ledger_read_error:{type(exc).__name__}:{sanitize_text(exc)}")
            lose_stream("ledger", exc)
        ledger_rate: LedgerRate | None = None
        if self.ledger_observations is not None:
            if not self.ledger_observations.path.is_file():
                errors.append(f"ledger_observations_missing:{self.ledger_observations.path}")
                failures.append(failure_from_category("continuous:ledger", "file_missing",
                                                      f"ledger observation journal not found: {self.ledger_observations.path}",
                                                      error_type="FileNotFoundError"))
            try:
                for row in self.ledger_observations.read_rows():
                    try:
                        parsed_obs = parse_observation_row(row)
                    except LedgerEvidenceError as exc:
                        errors.append(f"ledger_observation_row_error:{sanitize_text(exc)}")
                        reject_row("ledger", failure_from_category("continuous:ledger", "invalid_value", str(exc)))
                        continue
                    expected_seq = (self._ledger_obs[-1].seq + 1) if self._ledger_obs else 1
                    if parsed_obs.channel_id != self.channel_id:
                        reject_row("ledger", failure_from_category(
                            "continuous:ledger", "invalid_value",
                            f"ledger observation {parsed_obs.seq} is for channel {parsed_obs.channel_id!r}, not {self.channel_id!r}"))
                        continue
                    if parsed_obs.seq != expected_seq:
                        reject_row("ledger", failure_from_category(
                            "continuous:ledger", "invalid_value",
                            f"ledger observation sequence {parsed_obs.seq} follows {expected_seq - 1} (duplicate, rollback or gap)"))
                        continue
                    self._ledger_obs.append(parsed_obs)
            except (OSError, UnicodeError, ValueError) as exc:
                errors.append(f"ledger_observations_read_error:{type(exc).__name__}:{sanitize_text(exc)}")
                lose_stream("ledger", exc)
            ledger_rate = windowed_committed_rate(self._ledger_obs, now=observation, window_s=self.window_s)
        for stream, (count, first) in row_failures.items():
            failures.append(TelemetryReadFailure(
                source=first.source, category=first.category, error_type=first.error_type,
                message=sanitize_text(f"{count} row(s) rejected in this call; first: {first.message}")))

        # Outcome per stream: ``failed`` if it could not be read, ``invalid`` if rows were rejected
        # or lost (now or earlier in this adapter's life), else ``ok``.
        stream_outcome: dict[str, str] = {}
        for stream in ("recorder", "submits", "ledger"):
            these = [item for item in failures if item.source == f"continuous:{stream}"]
            if any(item.outcome == "failed" for item in these):
                stream_outcome[stream] = "failed"
            elif these or self._rejected[stream] or self._stream_lost[stream]:
                stream_outcome[stream] = "invalid"
            else:
                stream_outcome[stream] = "ok"

        submits = self._window(self._submit_events, 0, start)
        records = self._window(self._record_events, 0, start)
        ledger_events = self._window(self._ledger_events, 0, start)
        interval_s = None if self._last_observation is None else max(0.0, (observation - self._last_observation).total_seconds())
        self._last_observation = observation
        incomplete = bool(submits and min(row[0] for row in submits) > start)

        def source_time(values: Iterable[datetime], fallback: datetime = observation) -> datetime:
            values = list(values)
            return max(values) if values else fallback

        submit_times = [row[0] for row in submits]
        submit_span = (max(submit_times) - min(submit_times)).total_seconds() if len(submit_times) > 1 else 0.0
        submit_count = sum(row[1] for row in submits)
        offered = submit_count / submit_span if submit_count and submit_span > 0 else None
        offered_missing = None if offered is not None else ("insufficient timestamp span for offered-rate calculation" if submit_count else "no complete submit events in measurement window")

        completed = len(records)
        failed = sum(1 for row in records if not self._status_success(row[2]))
        # Latency-eligible samples: successful transactions with a validated interval. One window of raw
        # samples gives one p95 (client_latency.nearest_rank); p95 values of different windows are never averaged.
        latencies = [row[1] for row in records if self._status_success(row[2]) and row[1] is not None]
        p95 = nearest_rank(latencies, 0.95)
        mean = sum(latencies) / len(latencies) if latencies else None
        # A window with records but no successful one has no latency; say so rather than leave the reason blank.
        latency_missing = ("no completed Caliper records in measurement window" if not records
                           else "no successful transaction in measurement window" if not latencies else None)
        ledger_valid = sum(1 for row in ledger_events if row[2] == 0)
        ledger_invalid = sum(1 for row in ledger_events if row[2] != 0)
        delivered = None
        delivered_missing = "no newly observed ledger interval"
        if interval_s and interval_s > 0:
            delivered = sum(1 for row in ledger_new if row[2] == 0) / interval_s
            delivered_missing = None
        pending = self._submitted_total - self._completed_total if self._submitted_total >= self._completed_total else None
        pending_missing = None if pending is not None else "completed recorder count exceeds submitted count"
        source = f"Caliper recorder/submits + Fabric ledger:{self.channel_id}"
        metrics = {
            "submitted_transactions": self._metric("submitted_transactions", float(submit_count), unit="transactions", source=source,
                                                   timestamp=source_time(submit_times), observation=observation, window_start=start,
                                                   interval_s=submit_span or interval_s, missing="no complete submit events in measurement window" if not submits else None,
                                                   incomplete=incomplete),
            "valid_transactions": self._metric("valid_transactions", float(ledger_valid), unit="transactions", source="Fabric ledger ledger_tx.csv",
                                                timestamp=source_time([row[0] for row in ledger_events]), observation=observation, window_start=start,
                                                interval_s=interval_s, missing="no ledger rows in measurement window" if not ledger_events else None,
                                                incomplete=incomplete),
            "invalid_transactions": self._metric("invalid_transactions", float(ledger_invalid), unit="transactions", source="Fabric ledger ledger_tx.csv",
                                                  timestamp=source_time([row[0] for row in ledger_events]), observation=observation, window_start=start,
                                                  interval_s=interval_s, missing="no ledger rows in measurement window" if not ledger_events else None,
                                                  incomplete=incomplete),
            "offered_tps": self._metric("offered_tps", offered, unit="transactions/s", source="Caliper submits CSV",
                                         timestamp=source_time(submit_times), observation=observation, window_start=start,
                                         interval_s=submit_span or interval_s, missing=offered_missing, incomplete=incomplete),
            "delivered_valid_tps": self._metric("delivered_valid_tps", delivered, unit="VALID transactions/s", source="Fabric ledger append observation",
                                                 timestamp=observation, observation=observation, window_start=start,
                                                 interval_s=interval_s, missing=delivered_missing, incomplete=interval_s is None or interval_s > self.window_s),
            "latency_mean_ms": self._metric("latency_mean_ms", mean, unit="ms", source="Caliper recorder CSV",
                                             timestamp=source_time([row[0] for row in records]), observation=observation, window_start=start,
                                             interval_s=interval_s, missing=latency_missing,
                                             incomplete=incomplete),
            "latency_p95_ms": self._metric("latency_p95_ms", p95, unit="ms", source="Caliper recorder CSV nearest-rank p95",
                                            timestamp=source_time([row[0] for row in records]), observation=observation, window_start=start,
                                            interval_s=interval_s, missing=latency_missing,
                                            incomplete=incomplete),
            "inflight_backlog": self._metric("inflight_backlog", float(pending) if pending is not None else None, unit="transactions",
                                              source="Caliper submits minus completed recorder rows", timestamp=observation, observation=observation,
                                              window_start=start, interval_s=interval_s, missing=pending_missing or ("submit stream unavailable" if not submits else None)),
            "failed_transactions": self._metric("failed_transactions", float(failed), unit="transactions", source="Caliper recorder CSV status",
                                                timestamp=source_time([row[0] for row in records]), observation=observation, window_start=start,
                                                interval_s=interval_s, missing="no completed Caliper records in measurement window" if not records else None,
                                                incomplete=incomplete),
        }
        if ledger_rate is not None:
            # Committed throughput from LEDGER OBSERVATION time (task A0d). Values are stamped with the end of the newest
            # observed interval, not with the moment this adapter looked, so they can go stale.
            measuring = ledger_rate.value is not None
            ledger_source = "Fabric ledger observation journal (ledger_observations.csv)"
            stamp_end = ledger_rate.interval_end if measuring else observation
            stamp_start = ledger_rate.interval_start if measuring else start

            def ledger_metric(name: str, value: float | None, unit: str) -> MetricObservation:
                return _metric(name, value, unit=unit, timestamp_utc=_iso(stamp_end), source=ledger_source,
                               sampling_interval_s=ledger_rate.interval_s,
                               missing_reason=None if value is not None else ledger_rate.reason,
                               staleness_threshold_s=self.stale_after_s, observation_timestamp_utc=_iso(observation),
                               window_start_utc=_iso(stamp_start), window_end_utc=_iso(stamp_end), run_id=self.run_id,
                               incomplete_window=bool(measuring and not ledger_rate.window_covered))

            metrics["delivered_valid_tps"] = ledger_metric("delivered_valid_tps", ledger_rate.value, "VALID transactions/s")
            metrics["valid_transactions"] = ledger_metric(
                "valid_transactions", None if not measuring else float(ledger_rate.valid), "transactions")
            metrics["invalid_transactions"] = ledger_metric(
                "invalid_transactions", None if not measuring else float(ledger_rate.invalid), "transactions")
        # A metric derived from a stream that was not fully read is withheld, never zero-filled or
        # computed from the rows that happened to survive.
        derived_from = {
            "submitted_transactions": ("submits",), "offered_tps": ("submits",),
            "valid_transactions": ("ledger",), "invalid_transactions": ("ledger",), "delivered_valid_tps": ("ledger",),
            "latency_mean_ms": ("recorder",), "latency_p95_ms": ("recorder",), "failed_transactions": ("recorder",),
            "inflight_backlog": ("submits", "recorder"),
        }
        def stream_reason(stream: str) -> str:
            categories = sorted({item.category for item in failures if item.source == f"continuous:{stream}"})
            if categories:
                return f"{stream}: {', '.join(categories)}"
            if self._stream_lost[stream]:
                return f"{stream}: rows lost to a read error earlier"
            return f"{stream}: {self._rejected[stream]} row(s) rejected earlier"

        for name, streams in derived_from.items():
            bad = [stream for stream in streams if stream_outcome[stream] != "ok"]
            if bad:
                why = "; ".join(stream_reason(stream) for stream in bad)
                metrics[name] = replace(metrics[name], value=None,
                                        missing_reason=f"stream not fully read ({why}); value withheld, not substituted")
        contradictions: list[str] = []
        for tx_id, status in self._record_status.items():
            if tx_id not in self._ledger_ids:
                continue
            ledger_row = next(row for row in self._ledger_events if row[1] == tx_id)
            ledger_valid_state = ledger_row[2] == 0
            if self._status_success(status) != ledger_valid_state:
                contradictions.append(tx_id)
        health = {
            "source_files": {"recorder": str(self.recorder.path), "submits": str(self.submits.path), "ledger": str(self.ledger.path)},
            "read_errors": errors,
            "read_error_details": bounded_details(failures),
            "read_status": aggregate_read_status(stream_outcome),
            "integrity": {
                # COMPROMISED: rows were rejected or lost, which cannot be undone. DEGRADED: a stream
                # cannot be read right now (for example the file does not exist yet), nothing is lost.
                "status": ("COMPROMISED" if any(self._rejected.values()) or any(self._stream_lost.values())
                           else "OK" if all(value == "ok" for value in stream_outcome.values()) else "DEGRADED"),
                "rows_rejected": dict(self._rejected),
                "streams_lost_to_read_errors": sorted(name for name, lost in self._stream_lost.items() if lost),
                "note": "rejected or lost rows are never recovered; derived metrics stay withheld for this adapter's life",
            },
            "run_id": self.run_id,
            "window_s": self.window_s,
            # What the latency metrics mean and which samples they were computed from (task A0c, G1).
            "latency": {
                "schema_version": LATENCY_SCHEMA_VERSION,
                "definition": LATENCY_DEFINITION,
                "unit": LATENCY_UNIT,
                "source_mode": self.recorder_latency,
                "source": ("recorder column latency_ms, as written" if self.recorder_latency == "column"
                           else "derived per row: final_ms - create_ms"),
                "eligible_statuses": "success",
                "percentile_method": PERCENTILE_METHOD,
                "window_start_utc": _iso(start), "window_end_utc": _iso(observation),
                "event_time_basis": "final_ms of each row (event time), not the time the row was read",
                "eligible_samples_in_window": len(latencies),
                "failed_rows_in_window": failed,
                "rows_without_final_timestamp_cumulative": self._incomplete_rows,
                "rows_rejected_cumulative": self._rejected["recorder"],
            },
            "cross_source": {"recorder_unique_ids": len(self._record_ids), "ledger_unique_ids": len(self._ledger_ids),
                             "duplicate_ids": self._duplicate_ids, "status_validation_contradictions": contradictions,
                             "ledger_header_timestamp_is_not_commit_timestamp": True},
            "totals": {"submitted": self._submitted_total, "completed": self._completed_total,
                       "failed": self._failed_total, "valid": self._valid_total, "invalid": self._invalid_total},
        }
        if ledger_rate is not None:
            health["ledger_observations"] = {**ledger_rate.as_health(), "source_file": str(self.ledger_observations.path),
                                             "rows_read": len(self._ledger_obs)}
        return FabricTelemetrySnapshot(self.channel_id, _iso(observation), metrics, None, health)
