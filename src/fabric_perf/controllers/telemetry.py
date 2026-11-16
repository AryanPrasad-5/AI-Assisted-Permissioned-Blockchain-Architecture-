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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Deque, Iterable, Mapping

from .base import ControllerState, FabricConfig


class TelemetryDataError(ValueError):
    """Telemetry is missing, invalid, or has inconsistent units."""


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
        if self.value is not None and (not math.isfinite(float(self.value))):
            raise TelemetryDataError(f"metric {self.name} is not finite")
        if self.value is None and not self.missing_reason:
            raise TelemetryDataError(f"metric {self.name} is missing without a reason")
        if self.staleness_threshold_s <= 0:
            raise TelemetryDataError(f"metric {self.name} has a non-positive staleness threshold")
        for field_name in ("observation_timestamp_utc", "window_start_utc", "window_end_utc"):
            value = getattr(self, field_name)
            if value is not None:
                try:
                    datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                except ValueError as exc:
                    raise TelemetryDataError(f"metric {self.name} has invalid {field_name}") from exc

    def is_stale(self, *, now_utc: str | None = None) -> bool:
        if self.value is None:
            return False
        now = datetime.now(timezone.utc) if now_utc is None else datetime.fromisoformat(now_utc.replace("Z", "+00:00"))
        observed = datetime.fromisoformat(self.timestamp_utc.replace("Z", "+00:00"))
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
                            now_utc: str | None = None, require_fresh: bool = True) -> ControllerState:
        if require_fresh:
            stale = [name for name, observation in self.metrics.items() if observation.is_stale(now_utc=now_utc)]
            if stale:
                raise TelemetryDataError(f"stale telemetry metrics: {sorted(stale)}")
        def value(name: str) -> float | None:
            obs = self.metrics.get(name)
            return None if obs is None else obs.value

        config = self.current_config or FabricConfig()
        reference = datetime.now(timezone.utc) if now_utc is None else datetime.fromisoformat(now_utc.replace("Z", "+00:00"))
        observed_at = datetime.fromisoformat(self.timestamp_utc.replace("Z", "+00:00"))
        telemetry_age_s = max(0.0, (reference - observed_at).total_seconds())
        return ControllerState(
            timestamp_utc=self.timestamp_utc,
            current_config=config,
            observed_arrival_rate_tps=value("offered_tps"),
            observed_throughput_tps=value("delivered_valid_tps"),
            observed_latency_ms=value("latency_mean_ms"),
            observed_latency_p95_ms=value("latency_p95_ms"),
            queue_depth=value("inflight_backlog"),
            failed_transactions=int(value("failed_transactions") or 0),
            arrival_history_tps=tuple(arrival_history_tps),
            arrival_history_timestamps_utc=tuple(arrival_history_timestamps_utc),
            telemetry_age_s=telemetry_age_s,
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
        try:
            value = float(tail[0])
        except ValueError as exc:
            raise TelemetryDataError(f"invalid Prometheus value at line {line_no}") from exc
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
        self._previous: dict[tuple[str, str, tuple[tuple[str, str], ...]], tuple[float, float]] = {}

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
        for target, url in self.endpoints.items():
            try:
                rows.extend((name, labels, value, target) for name, labels, value in parse_prometheus_text(self.fetch(url, self.timeout_s)))
            except Exception as exc:
                endpoint_errors[target] = f"{type(exc).__name__}: {exc}"

        def channel_rows(metric_name: str) -> list[tuple[float, str, tuple[tuple[str, str], ...]]]:
            return [(value, target, labels) for name, labels, value, target in rows
                    if name == metric_name and self._on_channel(labels, channel_id)]

        observations: dict[str, MetricObservation] = {}
        leaders = sum(1 for value, _, _ in channel_rows("consensus_etcdraft_is_leader") if value == 1.0)
        observations["raft_leader_count"] = _metric("raft_leader_count", leaders if rows else None, unit="leaders", timestamp_utc=timestamp,
                                                     source="Fabric Prometheus consensus_etcdraft_is_leader", sampling_interval_s=None,
                                                     missing_reason="all Prometheus endpoints failed" if not rows else None)

        fill_sum = channel_rows("blockcutter_block_fill_duration_sum")
        fill_count = channel_rows("blockcutter_block_fill_duration_count")
        fill_sum_delta = 0.0
        fill_count_delta = 0.0
        intervals: list[float] = []
        for metric_name, metric_rows in (("blockcutter_block_fill_duration_sum", fill_sum),
                                         ("blockcutter_block_fill_duration_count", fill_count)):
            for value, target, labels in metric_rows:
                key = (target, metric_name, labels)
                previous = self._previous.get(key)
                if previous is not None:
                    fill_delta = value - previous[0]
                    interval = max(0.0, now - previous[1])
                    intervals.append(interval)
                    if metric_name.endswith("_sum"):
                        fill_sum_delta += fill_delta
                    else:
                        fill_count_delta += fill_delta
                self._previous[key] = (value, now)
        interval = min(intervals) if intervals else None
        observations["block_fill_duration_s"] = _metric(
            "block_fill_duration_s",
            fill_sum_delta / fill_count_delta if fill_count_delta > 0 else None,
            unit="s", timestamp_utc=timestamp,
            source="Fabric Prometheus blockcutter_block_fill_duration sum/count delta",
            sampling_interval_s=interval,
            missing_reason="no complete positive histogram delta in this sample" if fill_count_delta <= 0 else None,
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
        return FabricTelemetrySnapshot(channel_id, timestamp, observations, current_config, {"endpoint_errors": endpoint_errors, **dict(health or {})})


def caliper_snapshot_from_summary(path: Path, *, channel_id: str | None = None, timestamp_utc: str | None = None) -> FabricTelemetrySnapshot:
    """Convert an existing E1 benchmark summary into a provenance-rich snapshot."""
    summary_path = Path(path)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    metrics = summary.get("metrics", {})
    counts = summary.get("counts", {})
    inputs = summary.get("inputs", {})
    manifest_path = inputs.get("manifest", {}).get("path")
    observed_timestamp = timestamp_utc
    if observed_timestamp is None and manifest_path and Path(manifest_path).is_file():
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        wall_ms = manifest.get("clock", {}).get("wall1_ms")
        if wall_ms is not None:
            observed_timestamp = datetime.fromtimestamp(float(wall_ms) / 1000, tz=timezone.utc).isoformat(timespec="milliseconds")
    if observed_timestamp is None:
        raise TelemetryDataError("Caliper summary needs an explicit observation timestamp or a readable recorder manifest")
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
        health={"summary_analysis_verdict": summary.get("analysis_verdict")},
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
    """

    def __init__(self, *, recorder_csv: Path, submits_csv: Path, ledger_csv: Path,
                 channel_id: str, run_id: str, window_s: float = 5.0,
                 stale_after_s: float = 5.0):
        if window_s <= 0 or stale_after_s <= 0:
            raise ValueError("window_s and stale_after_s must be positive")
        self.recorder = _AppendOnlyCsvTail(Path(recorder_csv))
        self.submits = _AppendOnlyCsvTail(Path(submits_csv))
        self.ledger = _AppendOnlyCsvTail(Path(ledger_csv))
        self.channel_id = str(channel_id)
        self.run_id = str(run_id)
        self.window_s = float(window_s)
        self.stale_after_s = float(stale_after_s)
        self._submit_events: Deque[tuple[datetime, int]] = deque()
        self._record_events: Deque[tuple[datetime, float, str, str]] = deque()
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
        if not self.recorder.path.is_file():
            errors.append(f"recorder_missing:{self.recorder.path}")
        if not self.submits.path.is_file():
            errors.append(f"submits_missing:{self.submits.path}")
        if not self.ledger.path.is_file():
            errors.append(f"ledger_missing:{self.ledger.path}")
        try:
            for row in self.submits.read_rows():
                event_time = _epoch_ms(row.get("t_submit_evt_ms"), field="t_submit_evt_ms")
                count = int(row.get("count", "1"))
                if count < 1:
                    raise TelemetryDataError("submit count must be positive")
                self._submit_events.append((event_time, count))
                self._submitted_total += count
        except (OSError, UnicodeError, TelemetryDataError, ValueError) as exc:
            errors.append(f"submits_read_error:{type(exc).__name__}:{exc}")
        try:
            for row in self.recorder.read_rows():
                tx_id = str(row.get("tx_id", ""))
                final_value = row.get("final_ms")
                if not tx_id or final_value in (None, ""):
                    continue
                if tx_id in self._record_ids:
                    self._duplicate_ids += 1
                    continue
                final_time = _epoch_ms(final_value, field="final_ms")
                status = str(row.get("status", ""))
                try:
                    latency_ms = float(row["latency_ms"])
                except (KeyError, TypeError, ValueError) as exc:
                    errors.append(f"recorder_latency_error:{type(exc).__name__}:{exc}")
                    continue
                if latency_ms < 0 or not math.isfinite(latency_ms):
                    errors.append(f"negative_or_nonfinite_latency:{tx_id}")
                    continue
                self._record_ids.add(tx_id)
                self._record_status[tx_id] = status
                self._record_events.append((final_time, latency_ms, status, tx_id))
                self._completed_total += 1
                if not self._status_success(status):
                    self._failed_total += 1
        except (OSError, UnicodeError, TelemetryDataError, ValueError) as exc:
            errors.append(f"recorder_read_error:{type(exc).__name__}:{exc}")
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
                    errors.append(f"ledger_validation_code_error:{type(exc).__name__}:{exc}")
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
            errors.append(f"ledger_read_error:{type(exc).__name__}:{exc}")

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
        latencies = [row[1] for row in records]
        ordered = sorted(latencies)
        p95 = ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)] if ordered else None
        mean = sum(latencies) / len(latencies) if latencies else None
        ledger_valid = sum(1 for row in ledger_events if row[2] == 0)
        ledger_invalid = sum(1 for row in ledger_events if row[2] != 0)
        delivered = None
        delivered_missing = "no newly observed ledger interval"
        if interval_s and interval_s > 0:
            delivered = len(ledger_new) / interval_s
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
                                             interval_s=interval_s, missing="no completed Caliper records in measurement window" if not records else None,
                                             incomplete=incomplete),
            "latency_p95_ms": self._metric("latency_p95_ms", p95, unit="ms", source="Caliper recorder CSV nearest-rank p95",
                                            timestamp=source_time([row[0] for row in records]), observation=observation, window_start=start,
                                            interval_s=interval_s, missing="no completed Caliper records in measurement window" if not records else None,
                                            incomplete=incomplete),
            "inflight_backlog": self._metric("inflight_backlog", float(pending) if pending is not None else None, unit="transactions",
                                              source="Caliper submits minus completed recorder rows", timestamp=observation, observation=observation,
                                              window_start=start, interval_s=interval_s, missing=pending_missing or ("submit stream unavailable" if not submits else None)),
            "failed_transactions": self._metric("failed_transactions", float(failed), unit="transactions", source="Caliper recorder CSV status",
                                                timestamp=source_time([row[0] for row in records]), observation=observation, window_start=start,
                                                interval_s=interval_s, missing="no completed Caliper records in measurement window" if not records else None,
                                                incomplete=incomplete),
        }
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
            "run_id": self.run_id,
            "window_s": self.window_s,
            "cross_source": {"recorder_unique_ids": len(self._record_ids), "ledger_unique_ids": len(self._ledger_ids),
                             "duplicate_ids": self._duplicate_ids, "status_validation_contradictions": contradictions,
                             "ledger_header_timestamp_is_not_commit_timestamp": True},
            "totals": {"submitted": self._submitted_total, "completed": self._completed_total,
                       "failed": self._failed_total, "valid": self._valid_total, "invalid": self._invalid_total},
        }
        return FabricTelemetrySnapshot(self.channel_id, _iso(observation), metrics, None, health)
