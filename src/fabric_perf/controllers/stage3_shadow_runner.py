"""Operational Stage 3 shadow runner, offline fixture mode only (task A0f, 2026-11-21).

This module connects the existing A0b-A0e pieces into one reproducible path:

    recorder-v2-format client rows + append-only ledger journal  ->  ContinuousCaliperLedgerAdapter
    -> ProductionTelemetryProvider (freshness, provenance)        ->  TelemetryFrame (one per decision opportunity)
    -> IntegratedControllerRunner + Stage3FrameQualityContract    ->  static, reactive, predictive controllers
    -> flat decision records (``stage3_shadow_decision/1``) + run summary + run manifest

It adds no controller algorithm and no second runner. It composes the library classes unchanged.

What this module is
-------------------
* Mode A, ``run_fixture_shadow``: a complete Stage 3 pass over a synthetic or recorded FIXTURE. The ledger source, the client
  rows, the Prometheus endpoint, the clock, the performance model and the arrival forecaster are all fixtures created
  inside a new output directory. No network, process, Docker, WSL, Caliper or Fabric operation exists on this path.
* Mode B, ``evaluate_live_gates``: a READ-ONLY report of why a live shadow run is not permitted. It opens no connection and
  always says ``live_permitted: False``. There is no authorization record format in the repository, so no gate can be met.

What this module is not
-----------------------
* The frozen HFBTP model and the frozen EWMA forecaster are NOT loaded (computation boundary, ``PROJECT_RULES.md``). The predictive
  controller runs against ``FixturePerformanceModel`` / ``FixtureForecaster``, whose outputs are scripted by the fixture file.
  Predictive results from this module say nothing about the real model.
* No configuration is ever applied. Every actuator is a ``ShadowActuator``; a "proposal" is a record, never a change.
* A fixture run does not show that any controller improves Fabric performance, that recorder v2 works inside a real Caliper
  worker, that a real peer or orderer behaves like the fixture, or that the Windows and WSL clocks agree.

Nothing here has a research default: every threshold, window, limit and candidate comes from the operator's parameter files
(``prelive_params/1`` plus ``stage3_controller_policy/1``). A missing value is an error, never a default.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from . import channel_selection as cs
from . import prelive_contract as pc
from . import stage3_predictive as sp
from .captured_history import CapturedHistory, CapturedHistoryError
from .adapters import ApprovedMaxMessageCountSpace, ForecastResult
from .base import FabricConfig, PerformancePrediction, StaticController, coerce_config
from .harness import ShadowActuator
from .ledger_evidence import LedgerBlock, LedgerCollector, LedgerReadError, LedgerTx
from .live_integration import IntegratedControllerRunner, ProductionTelemetryProvider, TelemetryFrame
from .research_stages import ResearchStage, assert_no_live_actuation
from .stage3_frame_quality import (
    INITIALIZATION_LIMIT_REASON_CODE,
    INITIALIZATION_REASON_CODE,
    Stage3FrameQualityContract,
)
from .strategies import PredictiveController, ReactiveController
from .telemetry import ContinuousCaliperLedgerAdapter, PrometheusTelemetryAdapter
from .telemetry_freshness import FreshnessPolicy

TOOL_VERSION = "stage3_shadow_runner 0.3 (A0f 2026-11-21; A0g predictive contract and freshness limits; A0h captured history)"
POLICY_SCHEMA = "stage3_controller_policy/1"
# /2 (A0g) adds the three freshness limits the library only had as defaults. /1 stays readable so the A0f inputs and tests are
# unchanged; a /1 policy runs with the library defaults, says so in its manifest, and is refused wherever operator limits are required.
POLICY_SCHEMA_V2 = "stage3_controller_policy/2"
POLICY_SCHEMAS = (POLICY_SCHEMA, POLICY_SCHEMA_V2)
FRESHNESS_OPERATOR, FRESHNESS_LEGACY = "operator_supplied", "legacy_library_defaults_not_operator_supplied"
FIXTURE_SCHEMA = "stage3_fixture/1"
DECISION_SCHEMA = "stage3_shadow_decision/1"
DECISION_SCHEMA_V2 = "stage3_shadow_decision/2"      # /1 plus ``predictive_evidence`` (written only for a /2 policy)
SUMMARY_SCHEMA = "stage3_shadow_summary/1"
MANIFEST_SCHEMA = "stage3_shadow_run/1"
MODE_FIXTURE = "offline_fixture"
CONTROLLERS = ("static", "reactive", "predictive")
FIXTURE_PERFORMANCE_IDENTITY = "FIXTURE_MOCK_PERFORMANCE_MODEL (scripted by the fixture; NOT the frozen HFBTP model)"
FIXTURE_FORECAST_IDENTITY = "FIXTURE_MOCK_FORECAST (scripted by the fixture; NOT the frozen EWMA forecaster)"
RECORD_BYTES_LIMIT = 65_536     # the per-record size guard the live foundation also uses (LIVE_SHADOW_MAX_RECORD_BYTES)
EXIT_LIVE_REFUSED = 5

# Recorder-v2 CSV layouts the continuous adapter reads (kept identical to the A0d synthetic rig; a test pins the equality).
RECORDER_HEADER = ("tx_id", "seq_finish", "create_ms", "final_ms", "latency_ms", "latency_state", "status", "verified",
                   "err", "obs_wall_ms", "obs_hr_ns", "worker", "round", "label")
SUBMIT_HEADER = ("seq_submit", "t_submit_evt_ms", "count")

LIMITATIONS = (
    "Offline fixture run: no Fabric, peer, orderer, Caliper worker, Docker, WSL or Prometheus server was contacted.",
    "The performance model and the arrival forecaster are fixtures; the frozen HFBTP model and EWMA were not loaded.",
    "Recommendations are shadow records. No configuration was applied and no actuator that can change Fabric exists here.",
    "Fixture evidence cannot show that recorder v2 works inside a real Caliper worker or that a real ledger matches the fixture.",
    "Windows-WSL clock agreement was not measured; fixture time is a scripted clock.",
    "No controller is shown to outperform another. A shadow recommendation is not evidence of a performance effect.",
)


class Stage3RunnerError(ValueError):
    """The request cannot be run. Raised before any output directory is created unless the message says otherwise."""


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(Path(path).read_bytes())


def digest(value: Any) -> str:
    return sha256_bytes(_canonical(value).encode("utf-8"))


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _epoch_ms(moment: datetime) -> int:
    return int(round(moment.timestamp() * 1000))


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _finite(value: Any) -> bool:
    return _is_number(value) and math.isfinite(value)


# ------------------------------------------------------------------------------------------------ controller policy
@dataclass(frozen=True)
class ControllerPolicy:
    """The operator-supplied controller parameters. Nothing here has a default."""

    block_size_map: Mapping[int, int]          # approved MaxMessageCount -> HFBTP block-size feature (operator mapping)
    freshness_max_age_s: float
    client_stale_after_s: float
    reactive: Mapping[str, Any]
    predictive: Mapping[str, Any]
    schema: str = POLICY_SCHEMA
    freshness: FreshnessPolicy | None = None     # the one validated policy shared by provider, runner and controllers

    @property
    def freshness_basis(self) -> str:
        return FRESHNESS_OPERATOR if self.schema == POLICY_SCHEMA_V2 else FRESHNESS_LEGACY

    def freshness_policy(self) -> FreshnessPolicy:
        """The policy every component receives. For /1 it is exactly what A0f built: library defaults except ``max_age_s``."""
        return self.freshness if self.freshness is not None else FreshnessPolicy(max_age_s=self.freshness_max_age_s)

    def as_dict(self) -> dict[str, Any]:
        telemetry = {"freshness_max_age_s": self.freshness_max_age_s, "client_stale_after_s": self.client_stale_after_s}
        if self.schema == POLICY_SCHEMA_V2:
            telemetry.update({k: v for k, v in self.freshness_policy().as_dict().items() if k != "max_age_s"})
        return {"schema": self.schema,
                "candidate_hfbtp_block_size_by_max_message_count": {str(k): v for k, v in sorted(self.block_size_map.items())},
                "telemetry": telemetry, "reactive": dict(self.reactive), "predictive": dict(self.predictive)}


_REACTIVE_KEYS = ("congested_max_message_count", "latency_high_ms", "latency_low_ms", "consecutive_samples", "cooldown_samples")
_PREDICTIVE_KEYS = ("horizon_seconds", "latency_penalty_per_ms", "backlog_penalty_per_tps", "minimum_utility_improvement",
                    "cooldown_samples")
_POLICY_TOP = ("schema", "candidate_hfbtp_block_size_by_max_message_count", "telemetry", "reactive", "predictive")
_TELEMETRY_KEYS = {POLICY_SCHEMA: ("freshness_max_age_s", "client_stale_after_s"),
                   POLICY_SCHEMA_V2: ("freshness_max_age_s", "client_stale_after_s", "max_source_skew_s", "max_future_skew_s",
                                      "max_clock_discrepancy_s")}


def validate_controller_policy(raw: Any, *, cell_max_message_count: int) -> ControllerPolicy:
    """Validate ``stage3_controller_policy/1``. Every problem is listed; nothing is defaulted, repaired or converted."""
    problems: list[str] = []
    if not isinstance(raw, Mapping):
        raise Stage3RunnerError("controller policy must be a JSON object")
    schema = raw.get("schema")
    if schema not in POLICY_SCHEMAS:
        problems.append(f"schema: must be one of {POLICY_SCHEMAS}")
    for key in sorted(set(raw) - set(_POLICY_TOP)):
        problems.append(f"{key}: unknown top-level key")
    for key in _POLICY_TOP:
        if key not in raw:
            problems.append(f"{key}: missing (no default exists)")

    def section(name: str, keys: tuple[str, ...]) -> Mapping[str, Any]:
        value = raw.get(name)
        if not isinstance(value, Mapping):
            if name in raw:
                problems.append(f"{name}: must be an object")
            return {}
        for key in sorted(set(value) - set(keys)):
            problems.append(f"{name}.{key}: unknown key")
        for key in keys:
            if key not in value or value[key] is None:
                problems.append(f"{name}.{key}: missing or null (no default exists)")
        return value

    def number(sec: Mapping[str, Any], name: str, key: str, *, positive: bool, integer: bool = False) -> float | None:
        value = sec.get(key)
        if value is None:
            return None
        if not _finite(value) or (integer and (isinstance(value, float) or int(value) != value)):
            problems.append(f"{name}.{key}: must be a finite {'integer' if integer else 'number'}, got {value!r}")
            return None
        if (value <= 0) if positive else (value < 0):
            problems.append(f"{name}.{key}: must be {'positive' if positive else 'non-negative'}, got {value!r}")
            return None
        return value

    mapping_raw = raw.get("candidate_hfbtp_block_size_by_max_message_count")
    block_map: dict[int, int] = {}
    if isinstance(mapping_raw, Mapping) and mapping_raw:
        for key, value in mapping_raw.items():
            try:
                counts = int(key)
            except (TypeError, ValueError):
                problems.append(f"candidate_hfbtp_block_size_by_max_message_count: key {key!r} is not an integer")
                continue
            if isinstance(value, bool) or not isinstance(value, int) or value < 1 or counts < 1:
                problems.append(f"candidate_hfbtp_block_size_by_max_message_count[{key}]: must map a positive integer to a positive integer")
                continue
            block_map[counts] = value
    elif "candidate_hfbtp_block_size_by_max_message_count" in raw:
        problems.append("candidate_hfbtp_block_size_by_max_message_count: must be a non-empty object")
    if block_map and cell_max_message_count not in block_map:
        problems.append(f"candidate_hfbtp_block_size_by_max_message_count: must include the cell's max_message_count {cell_max_message_count}")

    telemetry = section("telemetry", _TELEMETRY_KEYS.get(schema, _TELEMETRY_KEYS[POLICY_SCHEMA]))
    fresh = number(telemetry, "telemetry", "freshness_max_age_s", positive=True)
    stale = number(telemetry, "telemetry", "client_stale_after_s", positive=True)
    limits: dict[str, float] = {}
    if schema == POLICY_SCHEMA_V2:
        # The three library limits, now operator-supplied. Each must be a finite number; none is derived from another and none
        # has a default here. ``FreshnessPolicy`` then applies its own sanity ceilings (a unit slip is an error, not a clip).
        for key, strictly_positive in (("max_source_skew_s", True), ("max_future_skew_s", False), ("max_clock_discrepancy_s", True)):
            value = number(telemetry, "telemetry", key, positive=strictly_positive)
            if value is not None:
                limits[key] = float(value)
    if fresh is not None:
        try:
            FreshnessPolicy(max_age_s=fresh, **limits)
        except ValueError as exc:
            problems.append(f"telemetry: not an accepted freshness policy: {exc}")

    reactive = section("reactive", _REACTIVE_KEYS)
    congested = number(reactive, "reactive", "congested_max_message_count", positive=True, integer=True)
    high = number(reactive, "reactive", "latency_high_ms", positive=True)
    low = number(reactive, "reactive", "latency_low_ms", positive=False)
    number(reactive, "reactive", "consecutive_samples", positive=True, integer=True)
    number(reactive, "reactive", "cooldown_samples", positive=False, integer=True)
    if high is not None and low is not None and low >= high:
        problems.append("reactive.latency_low_ms: must be below latency_high_ms")
    if congested is not None and block_map and congested not in block_map:
        problems.append("reactive.congested_max_message_count: must be one of the approved candidates")
    if congested is not None and congested == cell_max_message_count:
        problems.append("reactive.congested_max_message_count: equals the cell's max_message_count, so the reactive controller could never change anything")

    predictive = section("predictive", _PREDICTIVE_KEYS)
    number(predictive, "predictive", "horizon_seconds", positive=True, integer=True)
    number(predictive, "predictive", "latency_penalty_per_ms", positive=False)
    number(predictive, "predictive", "backlog_penalty_per_tps", positive=False)
    number(predictive, "predictive", "minimum_utility_improvement", positive=False)
    number(predictive, "predictive", "cooldown_samples", positive=False, integer=True)
    if problems:
        raise Stage3RunnerError("controller policy rejected: " + "; ".join(problems))
    freshness = FreshnessPolicy(max_age_s=float(fresh), **limits) if schema == POLICY_SCHEMA_V2 else None
    return ControllerPolicy(block_map, float(fresh), float(stale), dict(reactive), dict(predictive), schema, freshness)


# ------------------------------------------------------------------------------------------------ fixture
_FIXTURE_TOP = ("schema", "provenance", "description", "start_utc", "channel_configuration", "fixture_performance_model",
                "fixture_forecast_tps", "steps")
_STEP_KEYS = ("advance_s", "client", "ledger_valid_tx", "poll", "ledger_read_ok", "prometheus_ok", "config_reader_ok", "reported_max_message_count",
              "forecast", "model", "note")
_CLIENT_KEYS = ("count", "latency_ms", "status")
FIXTURE_PROVENANCE = ("SYNTHETIC", "RECORDED")
FORECAST_MODES = ("ok", "missing", "nan", "negative")
MODEL_MODES = ("ok", "nan", "negative", "missing_candidate")


@dataclass(frozen=True)
class Fixture:
    provenance: str
    description: str
    start: datetime
    channel_configuration: Mapping[str, Any]
    performance_table: Mapping[int, tuple[float, float]]
    forecast_tps: float | None
    steps: tuple[Mapping[str, Any], ...]
    raw: Mapping[str, Any]


def validate_fixture(raw: Any) -> Fixture:
    problems: list[str] = []
    if not isinstance(raw, Mapping):
        raise Stage3RunnerError("fixture must be a JSON object")
    if raw.get("schema") != FIXTURE_SCHEMA:
        problems.append(f"schema: must be {FIXTURE_SCHEMA!r}")
    for key in sorted(set(raw) - set(_FIXTURE_TOP)):
        problems.append(f"{key}: unknown top-level key")
    if raw.get("provenance") not in FIXTURE_PROVENANCE:
        problems.append(f"provenance: must be one of {FIXTURE_PROVENANCE} (a fixture says what it is)")
    description = raw.get("description")
    if not isinstance(description, str) or not description.strip():
        problems.append("description: required non-empty text")
    start = None
    try:
        start = datetime.fromisoformat(str(raw.get("start_utc")).replace("Z", "+00:00"))
        if start.tzinfo is None:
            raise ValueError("naive timestamp")
        start = start.astimezone(timezone.utc)
    except ValueError:
        problems.append("start_utc: must be a timezone-aware ISO-8601 timestamp")
    config = None
    try:
        cfg = raw.get("channel_configuration")
        required = {"max_message_count", "batch_timeout_ms", "preferred_max_bytes", "absolute_max_bytes", "orderers"}
        if not isinstance(cfg, Mapping) or set(cfg) != required:
            raise ValueError(f"must contain exactly {sorted(required)}")
        config = coerce_config(cfg)
    except (ValueError, TypeError) as exc:
        problems.append(f"channel_configuration: {exc}")
    table: dict[int, tuple[float, float]] = {}
    perf = raw.get("fixture_performance_model")
    if not isinstance(perf, Mapping) or not perf:
        problems.append("fixture_performance_model: required non-empty object {max_message_count: {throughput_tps, latency_ms}}")
    else:
        for key, row in perf.items():
            try:
                table[int(key)] = (float(row["throughput_tps"]), float(row["latency_ms"]))
            except (KeyError, TypeError, ValueError):
                problems.append(f"fixture_performance_model[{key}]: needs numeric throughput_tps and latency_ms")
    forecast = raw.get("fixture_forecast_tps")
    if forecast is not None and (not _finite(forecast) or forecast < 0):
        problems.append("fixture_forecast_tps: must be a finite non-negative number or null")
    steps = raw.get("steps")
    if not isinstance(steps, list) or not steps:
        problems.append("steps: required non-empty list")
        steps = []
    for index, step in enumerate(steps):
        where = f"steps[{index}]"
        if not isinstance(step, Mapping):
            problems.append(f"{where}: must be an object")
            continue
        for key in sorted(set(step) - set(_STEP_KEYS)):
            problems.append(f"{where}.{key}: unknown key")
        if not _finite(step.get("advance_s")) or step.get("advance_s") <= 0:
            problems.append(f"{where}.advance_s: required positive number")
        client = step.get("client")
        if client is not None:
            if not isinstance(client, Mapping) or set(client) - set(_CLIENT_KEYS):
                problems.append(f"{where}.client: object with keys {_CLIENT_KEYS}")
            else:
                if isinstance(client.get("count"), bool) or not isinstance(client.get("count"), int) or client["count"] < 1:
                    problems.append(f"{where}.client.count: positive integer")
                if not _finite(client.get("latency_ms")) or client["latency_ms"] < 0:
                    problems.append(f"{where}.client.latency_ms: non-negative number")
                if client.get("status", "success") not in {"success", "failed"}:
                    problems.append(f"{where}.client.status: success or failed")
        ledger = step.get("ledger_valid_tx")
        if ledger is not None and (isinstance(ledger, bool) or not isinstance(ledger, int) or ledger < 1):
            problems.append(f"{where}.ledger_valid_tx: positive integer (a block with no transactions is not a fixture event)")
        for flag in ("poll", "ledger_read_ok", "prometheus_ok", "config_reader_ok"):
            if flag in step and not isinstance(step[flag], bool):
                problems.append(f"{where}.{flag}: must be a boolean")
        if "reported_max_message_count" in step and (isinstance(step["reported_max_message_count"], bool)
                                                      or not isinstance(step["reported_max_message_count"], int)):
            problems.append(f"{where}.reported_max_message_count: integer")
        if step.get("forecast", "ok") not in FORECAST_MODES and not (_finite(step.get("forecast")) and step["forecast"] >= 0):
            problems.append(f"{where}.forecast: one of {FORECAST_MODES} or a non-negative number")
        if step.get("model", "ok") not in MODEL_MODES:
            problems.append(f"{where}.model: one of {MODEL_MODES}")
    if problems:
        raise Stage3RunnerError("fixture rejected: " + "; ".join(problems))
    return Fixture(raw["provenance"], raw["description"], start, config, table,
                   None if forecast is None else float(forecast), tuple(dict(s) for s in steps), raw)


class FixtureClock:
    def __init__(self, start: datetime):
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


class FixtureRunnerClock:
    """Runner clock driven by the fixture clock, so every recorded timestamp is reproducible."""

    def __init__(self, clock: FixtureClock):
        self._clock = clock
        self.wall_offset_s = 0.0          # A0h: a scripted wall-clock step; the monotonic clock below never sees it

    def utc_now(self) -> str:
        return _iso(self._clock.now + timedelta(seconds=self.wall_offset_s))

    def monotonic_ns(self) -> int:
        return int(self._clock.now.timestamp() * 1_000_000_000)


class FixtureLedgerSource:
    """A scripted ``LedgerSource``: height and blocks are set by the fixture, never read from a peer."""

    def __init__(self, channel: str):
        self.channel = channel
        self.current_height = 0
        self.blocks: dict[int, LedgerBlock] = {}
        self.readable = True

    def height(self) -> int:
        if not self.readable:
            raise LedgerReadError("fixture: scripted ledger read failure")
        return self.current_height

    def block(self, number: int) -> LedgerBlock:
        if number not in self.blocks:
            raise LedgerReadError(f"cannot read block {number}")
        return self.blocks[number]

    def commit_valid(self, count: int) -> LedgerBlock:
        number = self.current_height
        txs = tuple(LedgerTx(tx_id=f"fx{number}-{i}", index=i, header_type=3, validation_code=0, env_bytes=100,
                             channel_id=self.channel, ch_ts_ms=1_790_000_000_000 + i) for i in range(count))
        block = LedgerBlock(number=number, txs=txs, header_hash_hex=f"{number:064x}",
                            previous_hash_hex=f"{number - 1:064x}" if number else "", data_hash_ok=True, flags_len=len(txs))
        self.blocks[number] = block
        self.current_height = number + 1
        return block


class FixturePerformanceModel:
    """Scripted stand-in for the frozen HFBTP performance model. It reads a table; it learns nothing."""

    def __init__(self, table: Mapping[int, tuple[float, float]]):
        self.table = dict(table)
        self.mode = "ok"
        self.last_prediction_metadata: dict[str, Any] = {}

    def predict(self, arrival_rate_tps: float, config: FabricConfig):
        self.last_prediction_metadata = {"model_identity": FIXTURE_PERFORMANCE_IDENTITY,
                                         "feature_values": {"max_message_count": config.max_message_count,
                                                            "arrival_rate_tps": arrival_rate_tps}}
        if self.mode == "nan":
            return {"throughput_tps": float("nan"), "latency_ms": 1.0}
        if self.mode == "negative":
            return {"throughput_tps": -1.0, "latency_ms": 1.0}
        if self.mode == "missing_candidate" or config.max_message_count not in self.table:
            raise KeyError(f"fixture performance model has no row for max_message_count={config.max_message_count}")
        throughput, latency = self.table[config.max_message_count]
        return PerformancePrediction(throughput, latency)


class FixtureForecaster:
    """Scripted stand-in for the frozen EWMA forecaster. ``value`` is set per step by the fixture."""

    def __init__(self, clock: FixtureClock, horizon_seconds: int, default_tps: float | None):
        self.clock, self.horizon_seconds = clock, horizon_seconds
        self.value: Any = default_tps
        self.last_result: ForecastResult | None = None

    def predict(self, history_tps: Iterable[float], horizon_seconds: int):
        self.last_result = None
        value = self.value
        if value is None:
            return None                              # a missing forecast: the controller's own check abstains
        if value == "nan":
            value = float("nan")
        elif value == "negative":
            value = -1.0
        now = self.clock.now
        self.last_result = ForecastResult(
            value_tps=value, horizon_seconds=horizon_seconds, frequency_seconds=1, model_identity=FIXTURE_FORECAST_IDENTITY,
            inference_duration_ms=0.0, source="fixture", origin_timestamp_utc=_iso(now),
            history_last_timestamp_utc=_iso(now - timedelta(seconds=1)))
        return value


class FixtureRig:
    """All fixture sources for one run, written under ``work_dir`` (which must not exist yet).

    ``client`` / ``commit_ledger`` / ``poll`` / ``advance`` are public so a test can build the exact situation it needs.
    """

    def __init__(self, work_dir: Path, *, channel: str, run_id: str, fixture: Fixture, window_s: float,
                 stale_after_s: float, recorder_latency: str, endpoints: Mapping[str, str], reported: FabricConfig,
                 freshness: FreshnessPolicy, horizon_seconds: int, history: CapturedHistory | None = None,
                 wall_clock_steps: Mapping[int, float] | None = None):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=False)
        self.channel, self.run_id = channel, run_id
        self.clock = FixtureClock(fixture.start)
        self.source = FixtureLedgerSource(channel)
        self.collector = LedgerCollector(self.source, channel_id=channel, out_dir=self.work_dir / "ledger",
                                         collector_run_id=f"{run_id}-fixture", clock=self.clock)
        self.recorder_csv, self.submits_csv = self.work_dir / "recorder.csv", self.work_dir / "submits.csv"
        for path, header in ((self.recorder_csv, RECORDER_HEADER), (self.submits_csv, SUBMIT_HEADER)):
            with path.open("x", newline="", encoding="utf-8") as stream:
                csv.writer(stream, lineterminator="\n").writerow(header)
        self.seq = 0
        self.prometheus_ok = True
        self.config_reader_ok = True
        self.reported = reported
        self.adapter = ContinuousCaliperLedgerAdapter(
            recorder_csv=self.recorder_csv, submits_csv=self.submits_csv, ledger_csv=self.collector.paths["tx"],
            channel_id=channel, run_id=run_id, window_s=window_s, stale_after_s=stale_after_s,
            recorder_latency=recorder_latency, ledger_observations_csv=self.collector.paths["observations"])
        self.model = FixturePerformanceModel(fixture.performance_table)
        # A0h: with a captured history the forecaster has the real timestamped signature and the provider reads the history store.
        self.history = history
        self.forecaster = (FixtureForecaster(self.clock, horizon_seconds, fixture.forecast_tps) if history is None
                           else sp.TimestampedFixtureForecaster(FIXTURE_FORECAST_IDENTITY, fixture.forecast_tps))
        self.default_forecast = fixture.forecast_tps
        # A0h: with scripted wall-clock steps every frame is collected WITHOUT a scripted observation time, which is the only way the
        # provider's clock-discrepancy check runs. Without them the A0f/A0g behaviour (scripted observation times) is unchanged.
        self.wall_clock_steps = None if wall_clock_steps is None else dict(wall_clock_steps)
        self.runner_clock = FixtureRunnerClock(self.clock)

        def fetch(url: str, timeout: float) -> str:              # a scripted body; the URL is never contacted
            if not self.prometheus_ok:
                raise ConnectionRefusedError("fixture: scripted Prometheus endpoint failure")
            return f'consensus_etcdraft_is_leader{{channel="{channel}"}} 1\n'

        def config_reader() -> FabricConfig:
            if not self.config_reader_ok:
                raise RuntimeError("fixture: scripted configuration read failure")
            return self.reported

        self.provider = ProductionTelemetryProvider(
            PrometheusTelemetryAdapter(dict(endpoints), fetch=fetch), channel, config_reader=config_reader,
            continuous_source=self.adapter, arrival_history_reader=self._history if history is None else None,
            workload=None if history is None else history.store, required_fields=(),
            freshness_policy=freshness, required_prometheus_metrics=("raft_leader_count",),
            clock=self.runner_clock)     # used only when a frame is collected without a scripted observation time

    def _history(self):
        # Fixture arrival history (the real EWMA history store is not used). Values are placeholders; the fixture forecaster
        # ignores them. They exist so the predictive controller sees a timestamped, fresh history exactly as in A0d tests.
        return (_iso(self.clock.now - timedelta(seconds=2)), _iso(self.clock.now - timedelta(seconds=1))), (10.0, 12.0)

    def client(self, count: int, *, latency_ms: float = 100, status: str = "success") -> None:
        """Append ``count`` client completions (and their submit events) ending at the current fixture time."""
        now = _epoch_ms(self.clock.now)
        with self.recorder_csv.open("a", newline="", encoding="utf-8") as rec, self.submits_csv.open("a", newline="", encoding="utf-8") as sub:
            r, s = csv.writer(rec, lineterminator="\n"), csv.writer(sub, lineterminator="\n")
            for i in range(count):
                self.seq += 1
                final = now - 100 * (count - 1 - i)
                r.writerow([f"c{self.seq}", self.seq, int(final - latency_ms), final, int(latency_ms), "ok", status, "", "",
                            now, 0, 0, 1, "fixture"])
                s.writerow([self.seq, int(final - latency_ms - 50), 1])

    def commit_ledger(self, valid_tx: int) -> None:
        self.source.commit_valid(valid_tx)

    def poll(self):
        return self.collector.poll()

    def advance(self, seconds: float) -> None:
        self.clock.advance(seconds)

    def frame(self, index: int) -> TelemetryFrame:
        if self.wall_clock_steps is not None:
            self.runner_clock.wall_offset_s += float(self.wall_clock_steps.get(index, 0.0))
            observed, cutoff = None, self.runner_clock.utc_now()
        else:
            observed = cutoff = _iso(self.clock.now)
        if self.history is not None:
            self.history.advance_to(cutoff)        # only bins that had ended by the decision cutoff become visible
        return self.provider.collect(run_id=self.run_id, decision_index=index, observation_timestamp_utc=observed)

    def apply_step(self, step: Mapping[str, Any], default_reported: FabricConfig) -> None:
        """Time passes first; the step's client completions and ledger block land at its END, then the collector polls."""
        self.advance(step["advance_s"])
        client = step.get("client")
        if client:
            self.client(client["count"], latency_ms=client["latency_ms"], status=client.get("status", "success"))
        if step.get("ledger_valid_tx"):
            self.commit_ledger(step["ledger_valid_tx"])
        self.source.readable = step.get("ledger_read_ok", True)
        if step.get("poll", True):
            self.poll()
        self.prometheus_ok = step.get("prometheus_ok", True)
        self.config_reader_ok = step.get("config_reader_ok", True)
        self.reported = (default_reported if "reported_max_message_count" not in step
                         else replace(default_reported, max_message_count=step["reported_max_message_count"]))
        forecast = step.get("forecast", "ok")
        self.forecaster.value = (self.default_forecast if forecast == "ok" else None if forecast == "missing" else forecast)
        self.model.mode = step.get("model", "ok")

    def evidence_files(self) -> dict[str, Path]:
        return {"client_recorder": self.recorder_csv, "client_submits": self.submits_csv,
                "ledger_tx": Path(self.collector.paths["tx"]), "ledger_observations": Path(self.collector.paths["observations"])}


# ------------------------------------------------------------------------------------------------ run assembly
@dataclass(frozen=True)
class ShadowRunInputs:
    """Validated, immutable inputs of one fixture run."""

    params: pc.ValidatedParameters
    policy: ControllerPolicy
    fixture: Fixture
    static_config: FabricConfig
    contract: Stage3FrameQualityContract
    raw_bytes: Mapping[str, bytes]

    @property
    def values(self) -> Mapping[str, Any]:
        return self.params.values


def static_config_from_cell(cell: Mapping[str, Any], fixture_config: FabricConfig) -> FabricConfig:
    """The static configuration is defined by the experimental cell. Its two provenance-only fields (absolute byte cap and
    orderer count) are not part of the cell, so they are taken from the fixture's declared channel configuration."""
    return coerce_config({"max_message_count": cell["max_message_count"], "batch_timeout": cell["batch_timeout"],
                          "preferred_max_bytes": cell["preferred_max_bytes"],
                          "absolute_max_bytes": fixture_config.absolute_max_bytes, "orderers": fixture_config.orderers})


def load_inputs(params_path: Path, policy_path: Path, fixture_path: Path, *, require_operator_freshness: bool = False) -> ShadowRunInputs:
    """Read and validate the three input files. Reads only; creates nothing.

    ``require_operator_freshness=True`` refuses a ``stage3_controller_policy/1`` file: it has no operator-supplied source-skew,
    future-skew or clock-discrepancy limit, so the library defaults would apply silently.
    """
    raw_bytes: dict[str, bytes] = {}
    docs: dict[str, Any] = {}
    for label, path in (("params", params_path), ("controller_policy", policy_path), ("fixture", fixture_path)):
        try:
            raw_bytes[label] = Path(path).read_bytes()
            docs[label] = json.loads(raw_bytes[label].decode("utf-8"))
        except (OSError, ValueError) as exc:
            raise Stage3RunnerError(f"{label} file {path} cannot be read as JSON: {exc}") from exc
    try:
        params = pc.validate_operator_parameters(docs["params"])
    except pc.PreliveParameterError as exc:
        raise Stage3RunnerError(f"operator parameters rejected: {exc}") from exc
    values = params.values
    cs.select_experiment_channel(values["channel"])                      # raises ChannelSelectionError for protected channels
    policy = validate_controller_policy(docs["controller_policy"], cell_max_message_count=values["cell"]["max_message_count"])
    if require_operator_freshness and policy.freshness_basis != FRESHNESS_OPERATOR:
        raise Stage3RunnerError(f"strict Stage 3 execution needs {POLICY_SCHEMA_V2} with operator-supplied max_source_skew_s, "
                                f"max_future_skew_s and max_clock_discrepancy_s; got {policy.schema}")
    fixture = validate_fixture(docs["fixture"])
    static_config = static_config_from_cell(values["cell"], fixture.channel_configuration)
    if values["cell"]["max_message_count"] not in fixture.performance_table:
        raise Stage3RunnerError("fixture_performance_model has no row for the cell's max_message_count")
    contract = Stage3FrameQualityContract(max_consecutive_abstentions=values["stage3"]["max_consecutive_abstentions"],
                                          max_initialization_frames=values["stage3"]["max_initialization_frames"])
    return ShadowRunInputs(params, policy, fixture, static_config, contract, raw_bytes)


def fixture_predictive_components(rig: FixtureRig) -> sp.PredictiveComponents:
    """The authorized predictive mode: labelled scripted stand-ins. Neither the frozen HFBTP model nor the frozen EWMA is loaded.

    With a captured history (A0h) the forecaster sits behind the real ``GuardedForecaster`` and the performance table behind the real
    workload-range check; the values are still scripted fixtures."""
    if rig.history is not None:
        source = rig.history.source
        return sp.captured_history_components(
            rig.model, rig.forecaster, performance_identity=FIXTURE_PERFORMANCE_IDENTITY, forecast_identity=FIXTURE_FORECAST_IDENTITY,
            history_basis=f"captured history {source.source_id} ({source.kind}; zero policy {rig.history.zero_policy}); "
                          "not Fabric observations")
    return sp.fixture_components(rig.model, rig.forecaster, performance_identity=FIXTURE_PERFORMANCE_IDENTITY,
                                 forecast_identity=FIXTURE_FORECAST_IDENTITY)


class Stage3ShadowRunner(IntegratedControllerRunner):
    """``IntegratedControllerRunner`` with the operator's complete freshness policy applied to its frame validation.

    The library runner builds ``FreshnessPolicy(max_age_s=...)`` itself, so its future-skew limit could not be set. This subclass
    applies the validated policy afterwards (``freshness_policy`` is a plain attribute the library reads for every frame check and
    records in its summary). ``max_age_s`` must agree with ``max_telemetry_age_s``; nothing else is derived or defaulted. The
    library file is not edited, so every other caller is unchanged.
    """

    def __init__(self, *args: Any, freshness_policy: FreshnessPolicy, **kwargs: Any):
        super().__init__(*args, **kwargs)
        if not isinstance(freshness_policy, FreshnessPolicy):
            raise Stage3RunnerError("freshness_policy must be a FreshnessPolicy")
        if freshness_policy.max_age_s != self.freshness_policy.max_age_s:
            raise Stage3RunnerError("freshness_policy.max_age_s conflicts with max_telemetry_age_s; supply one validated limit")
        self.freshness_policy = freshness_policy

    def run_provider(self, provider, **kwargs):
        """The library compares only ``max_age_s`` between provider and runner. Here ALL four limits must be the same policy, so no
        frame can be fresh, skewed, future-dated or clock-stepped by one set of limits for the provider and another for the runner."""
        provider_policy = getattr(provider, "freshness_policy", None)
        if provider_policy is not None and provider_policy != self.freshness_policy:
            raise Stage3RunnerError(f"provider freshness limits {provider_policy.as_dict()} differ from the runner's "
                                    f"{self.freshness_policy.as_dict()}; one operator policy must reach both")
        return super().run_provider(provider, **kwargs)


def build_controllers(inputs: ShadowRunInputs, rig: FixtureRig, predictive: sp.PredictiveComponents | None = None):
    config, policy = inputs.static_config, inputs.policy
    predictive = predictive or fixture_predictive_components(rig)
    space = ApprovedMaxMessageCountSpace(frozenset(policy.block_size_map), dict(policy.block_size_map), config)
    candidates = [replace(config, max_message_count=size) for size in sorted(policy.block_size_map)]
    for candidate in candidates:
        space.validate(candidate)
    r, p = policy.reactive, policy.predictive
    controllers = {
        "static": StaticController(config),
        "reactive": ReactiveController(
            config, congested_config=replace(config, max_message_count=r["congested_max_message_count"]),
            latency_high_ms=r["latency_high_ms"], latency_low_ms=r["latency_low_ms"], consecutive_samples=r["consecutive_samples"],
            cooldown_samples=r["cooldown_samples"], max_telemetry_age_s=policy.freshness_max_age_s),
        "predictive": PredictiveController(
            predictive.performance_predictor, config, arrival_forecaster=predictive.forecaster, candidate_configs=candidates,
            horizon_seconds=p["horizon_seconds"], latency_penalty_per_ms=p["latency_penalty_per_ms"],
            backlog_penalty_per_tps=p["backlog_penalty_per_tps"], minimum_utility_improvement=p["minimum_utility_improvement"],
            cooldown_samples=p["cooldown_samples"], max_telemetry_age_s=policy.freshness_max_age_s),
    }
    return controllers, space


def _rel(path: Any, base: Path) -> str | None:
    if not path:
        return None
    try:
        return Path(path).resolve().relative_to(Path(base).resolve()).as_posix()
    except ValueError:
        return Path(path).name


def summarize_frame(frame: TelemetryFrame, base: Path) -> dict[str, Any]:
    """The part of a frame the decision records need. Absolute paths are made relative to the evidence directory."""
    state, prov = frame.state, frame.provenance
    health = prov.get("continuous_caliper_health") or {}
    latency, ledger, metrics = health.get("latency") or {}, health.get("ledger") or {}, prov.get("metrics") or {}

    def window(metric: str) -> dict[str, Any]:
        m = metrics.get(metric) or {}
        return {"start_utc": m.get("window_start_utc"), "end_utc": m.get("window_end_utc"),
                "sampling_interval_s": m.get("sampling_interval_s"), "incomplete_window": m.get("incomplete_window"),
                "missing_reason": m.get("missing_reason")}

    source = prov.get("continuous_caliper_source") or {}
    # When the configuration read failed the library fills ``state.current_config`` with the dataclass default. That is not
    # evidence of anything, so the record says "unknown" instead of repeating it (an unknown configuration is never reported).
    config_unknown = any(str(code).startswith("configuration_read_error") for code in frame.critical_missing)
    return {
        "decision_index": frame.decision_index, "input_digest": frame.input_digest,
        "observation_timestamp_utc": frame.observation_timestamp_utc,
        "observed": {"client_offered_rate_tps": state.observed_arrival_rate_tps,
                     "ledger_committed_throughput_tps": state.observed_throughput_tps,
                     "client_latency_mean_ms": state.observed_latency_ms, "client_latency_p95_ms": state.observed_latency_p95_ms,
                     "client_backlog": state.queue_depth, "failed_transactions": state.failed_transactions,
                     "telemetry_age_s": state.telemetry_age_s},
        "measurement": {
            "client_latency": {"definition": latency.get("definition"), "unit": latency.get("unit"),
                               "window_start_utc": latency.get("window_start_utc"), "window_end_utc": latency.get("window_end_utc"),
                               "eligible_samples": latency.get("eligible_samples_in_window"),
                               "event_time_basis": latency.get("event_time_basis")},
            "client_offered_rate": window("offered_tps"),
            "ledger_committed_throughput": {
                "definition": ledger.get("definition"), "state": ledger.get("state"), "reason": ledger.get("reason"),
                "interval_start_utc": ledger.get("interval_start_utc"), "interval_end_utc": ledger.get("interval_end_utc"),
                "interval_s": ledger.get("interval_s"), "valid_in_interval": ledger.get("valid_in_interval"),
                "invalid_in_interval": ledger.get("invalid_in_interval"), "time_basis": ledger.get("time_basis")}},
        "evidence": {"client_recorder": _rel(source.get("recorder"), base), "client_submits": _rel(source.get("submits"), base),
                     "ledger_tx": _rel(source.get("ledger"), base), "ledger_observations": _rel(source.get("ledger_observations"), base),
                     "ledger_latest_observation_seq": ledger.get("latest_observation_seq"),
                     "ledger_baseline_height": ledger.get("baseline_height"), "ledger_rows_read": ledger.get("rows_read"),
                     "client_integrity": (health.get("integrity") or {}).get("status")},
        "reported_config": (None if config_unknown or state.current_config is None else state.current_config.as_dict()),
        "reported_config_unknown": config_unknown,
        "reported_config_source": prov.get("configuration_source"),
        "critical_missing": list(frame.critical_missing), "inconsistencies": list(frame.inconsistencies),
        "history_reference": None if state.arrival_history_ref is None else state.arrival_history_ref.as_dict(),
    }


def read_jsonl_tolerant(path: Path) -> tuple[list[dict[str, Any]], bool]:
    """Rows of a JSONL file; an unparsable LAST line (an interrupted write) is dropped and reported, any other bad line raises."""
    rows: list[dict[str, Any]] = []
    if not Path(path).exists():
        return rows, False
    lines = [line for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]
    for number, line in enumerate(lines):
        try:
            rows.append(json.loads(line))
        except ValueError:
            if number == len(lines) - 1:
                return rows, True
            raise Stage3RunnerError(f"{path}: line {number + 1} is not valid JSON and is not the last line")
    return rows, False


def _frame_status(record: Mapping[str, Any]) -> tuple[str, str | None, str | None, Mapping[str, Any]]:
    rejected = (record.get("integration") or {}).get("observation_rejected")
    if not rejected:
        return "accepted", None, None, {}
    code = rejected.get("reason_code")
    status = "initialization" if code == INITIALIZATION_REASON_CODE else "rejected"
    return status, code, rejected.get("abstention_reason"), rejected.get("details") or {}


def build_decision_rows(records: Iterable[Mapping[str, Any]], frames: Mapping[int, Mapping[str, Any]], *, run_id: str,
                        channel: str, cell: Mapping[str, Any], mode: str, evidence_class: str,
                        predictive_label: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    """One flat row per (frame, controller). Unavailable values are ``null``; a genuine zero is ``0``/``0.0``.

    With ``predictive_label`` (a /2 run) rows are ``stage3_shadow_decision/2``: the /1 fields plus ``predictive_evidence``.
    """
    cell_id = digest(cell)
    rows: list[dict[str, Any]] = []
    for record in records:
        integration = record["integration"]
        index, controller = integration["decision_index"], integration["strategy"]
        summary = frames.get(index)
        if summary is None:
            raise Stage3RunnerError(f"record for decision {index} has no captured frame summary")
        if integration["input_digest"] != summary["input_digest"]:
            raise Stage3RunnerError(f"frame identity mismatch at decision {index}: runner digest != captured digest")
        status, code, abstention, details = _frame_status(record)
        decision, trace = record["decision"], record.get("decision_trace") or {}
        outcome = decision["outcome"]
        application, conf = record.get("application"), record.get("configuration_state") or {}
        forecast = trace.get("forecast")
        row = {
            "schema": DECISION_SCHEMA if predictive_label is None else DECISION_SCHEMA_V2, "run_id": run_id, "mode": mode, "evidence_class": evidence_class,
            "channel": channel, "cell": dict(cell), "cell_id": cell_id,
            "frame": {"index": index, "id": summary["input_digest"],
                      "observation_timestamp_utc": summary["observation_timestamp_utc"], "status": status,
                      "reason_code": code, "abstention_reason": abstention, "reason_details": dict(details),
                      "critical_missing": summary["critical_missing"], "inconsistencies": summary["inconsistencies"]},
            "measurement": summary["measurement"], "evidence": summary["evidence"], "observed": summary["observed"],
            "controller": controller, "outcome": outcome, "reason": decision["reason"],
            "abstention_scope": (None if outcome != "abstain" else "frame" if status != "accepted" else "controller"),
            "recommended_config": decision["config"] if outcome in ("propose", "hold") else None,
            "config_change_proposed": outcome == "propose",
            "known_actual_config": summary["reported_config"], "known_actual_config_source": summary["reported_config_source"],
            "known_actual_config_basis": ("unknown: the configuration read failed and no value is substituted"
                                          if summary["reported_config_unknown"] else
                                          "fixture-declared channel configuration, not independently verified"
                                          if mode == MODE_FIXTURE else "not established"),
            "recommendation_matches_known_actual_config": (
                None if summary["reported_config"] is None or outcome not in ("propose", "hold")
                else all(summary["reported_config"].get(k) == v for k, v in decision["config"].items())),
            "proposal_applied": bool(conf.get("proposal_applied", False)),
            "remote_state_unknown": bool(conf.get("remote_state_unknown", False)),
            "actuator": {"kind": "ShadowActuator", "live_actuator_available": False, "configuration_changes_applied": 0,
                         "proposal_recorded_by_shadow_actuator": application is not None,
                         "submitted_to_fabric": False},
            "forecast": forecast, "predicted_arrival_rate_tps": decision.get("predicted_arrival_rate_tps"),
            "predicted_performance": decision.get("predicted_performance"),
            "model": ({"identity": FIXTURE_PERFORMANCE_IDENTITY, "frozen_model_loaded": False}
                      if controller == "predictive" else None),
            "error": (decision.get("metadata") or {}).get("error"),
        }
        if predictive_label is not None:
            row["predictive_evidence"] = sp.predictive_evidence(predictive_label, controller=controller, decision=decision,
                                                                frame_status=status, history_reference=summary.get("history_reference"),
                                                                coverage=summary.get("coverage_provenance"))
        rows.append(row)
    return rows


def check_comparability(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Every frame must have one row per controller, with one frame id, one status and one set of observed values."""
    by_frame: dict[int, list[Mapping[str, Any]]] = {}
    for row in rows:
        by_frame.setdefault(row["frame"]["index"], []).append(row)
    problems: list[str] = []
    for index, group in sorted(by_frame.items()):
        names = sorted(r["controller"] for r in group)
        if names != sorted(CONTROLLERS):
            problems.append(f"frame {index}: controllers present {names}")
            continue
        for key in ("frame", "observed", "measurement", "evidence", "known_actual_config"):
            if len({_canonical(r[key]) for r in group}) != 1:
                problems.append(f"frame {index}: controllers received different {key}")
    return {"frames": len(by_frame), "comparable": not problems, "problems": problems}


def summarize_run(rows: list[Mapping[str, Any]], *, status: str, stop_reasons: list[str], native: Mapping[str, Any] | None,
                  frames_expected: int) -> dict[str, Any]:
    frame_status: dict[int, str] = {}
    for row in rows:
        frame_status[row["frame"]["index"]] = row["frame"]["status"]
    counts = {"accepted": 0, "initialization": 0, "rejected": 0}
    for value in frame_status.values():
        counts[value] += 1
    outcomes = {name: {"propose": 0, "hold": 0, "abstain": 0} for name in CONTROLLERS}
    for row in rows:
        outcomes[row["controller"]][row["outcome"]] += 1
    comparability = check_comparability(rows)
    return {"schema": SUMMARY_SCHEMA, "status": status, "frames_in_fixture": frames_expected,
            "frames_processed": len(frame_status), "frame_status_counts": counts, "controller_outcome_counts": outcomes,
            "comparability": comparability, "stop_reasons": stop_reasons,
            "native": None if native is None else {k: native.get(k) for k in (
                "abstained_frame_count", "initialization_frame_count", "longest_consecutive_abstention_run",
                "consecutive_abstention_limit_reached", "initialization_limit_exceeded", "identical_input_digests",
                "invalid_frame_policy", "mode", "live_submission_enabled")},
            "configuration_changes_applied": 0, "live_ready": False}


def _write_new(path: Path, data: bytes) -> None:
    with Path(path).open("xb") as stream:
        stream.write(data)


def _write_new_json(path: Path, doc: Any) -> None:
    _write_new(path, (json.dumps(doc, indent=2, sort_keys=True) + "\n").encode("utf-8"))


def _hash_tree(root: Path, *, skip: tuple[str, ...] = ()) -> dict[str, str]:
    return {p.relative_to(root).as_posix(): sha256_file(p) for p in sorted(Path(root).rglob("*"))
            if p.is_file() and p.relative_to(root).as_posix() not in skip}


def run_fixture_shadow(inputs: ShadowRunInputs, out_dir: Path, *, tool_files: Mapping[str, Path] | None = None,
                       prelive_link: Mapping[str, Any] | None = None,
                       predictive_mode: str = sp.PREDICTIVE_MODE_FIXTURE,
                       captured_history: CapturedHistory | None = None,
                       _frame_hook: Callable[[int], None] | None = None,
                       _frame_transform: Callable[[int, TelemetryFrame], TelemetryFrame] | None = None,
                       _wall_clock_steps: Mapping[int, float] | None = None) -> dict[str, Any]:
    """Mode A. Run the whole Stage 3 pipeline over the fixture into the NEW directory ``out_dir``. Returns the run manifest.

    Raises ``FileExistsError`` if ``out_dir`` exists. A KeyboardInterrupt (or a hook raising it) ends the run with status
    ``INTERRUPTED``: what was streamed is kept, summarised and hashed, and nothing is presented as complete.

    ``predictive_mode`` is ``"fixture"`` (the only authorized mode) or ``"real"``, which is prepared but DISABLED: it raises
    ``RealInferenceNotAuthorized`` before any directory, pin, model or history is touched. There is no other way to select it.

    ``captured_history`` (A0h) is a ``CapturedHistory``: authentic, stored, timestamped arrival observations that replace the placeholder
    history. The history store is fed only with bins that had ended by each frame's decision time and is read through the provider's
    atomic snapshot; the real ``GuardedForecaster`` checks it before the (still scripted) fixture forecaster sees it. It needs a
    ``stage3_controller_policy/2`` policy (so the records carry the evidence), ``predictive.horizon_seconds == 60`` (the frozen
    forecast's only horizon) and a fixture whose whole timeline lies inside the captured UTC day. Nothing is loaded or forecast.

    ``_frame_hook``, ``_frame_transform`` and ``_wall_clock_steps`` are TEST hooks for fault injection (an interruption after a frame;
    a frame whose timestamps are altered before the runner sees it; wall-clock steps in the runner clock, which turn on the
    provider's clock-discrepancy check). The CLI never passes them.
    """
    out_dir = Path(out_dir)
    if predictive_mode not in sp.PREDICTIVE_MODES:
        raise Stage3RunnerError(f"predictive_mode must be one of {sp.PREDICTIVE_MODES}, got {predictive_mode!r}")
    if predictive_mode == sp.PREDICTIVE_MODE_REAL:
        # Not a fixture route. Refused while ``REAL_INFERENCE_AUTHORIZED`` is False, before anything is created. Even when
        # it is True this runner has no captured-frame or live route that supplies a real workload history (OPEN, next task).
        if sp.REAL_INFERENCE_AUTHORIZED is not True:
            raise sp.RealInferenceNotAuthorized("real_inference_not_authorized",
                                                "real predictive mode is prepared but disabled; fixture is the only authorized mode")
        raise Stage3RunnerError("the runner has no captured-frame route for real predictive inputs yet")
    if captured_history is not None:
        if inputs.policy.schema != POLICY_SCHEMA_V2:
            raise Stage3RunnerError(f"a captured history needs {POLICY_SCHEMA_V2} so the records can carry its evidence")
        if inputs.policy.predictive["horizon_seconds"] != captured_history.forecast_horizon_s:
            raise Stage3RunnerError(f"a captured history serves the {captured_history.forecast_horizon_s} s forecast only; the policy's "
                                    f"predictive.horizon_seconds is {inputs.policy.predictive['horizon_seconds']}")
        day_start, day_end = captured_history.day_bounds()
        timeline_end = inputs.fixture.start + timedelta(seconds=sum(step["advance_s"] for step in inputs.fixture.steps))
        if not (day_start <= inputs.fixture.start and timeline_end <= day_end):
            raise Stage3RunnerError("the fixture timeline must lie inside the captured UTC day "
                                    f"{captured_history.source.utc_day}; it runs {_iso(inputs.fixture.start)} to {_iso(timeline_end)}")
    if out_dir.exists():
        raise FileExistsError(f"refusing to reuse {out_dir}: every run gets a new directory")
    values, fixture = inputs.values, inputs.fixture
    run_id, channel = values["run_id"], values["channel"]
    out_dir.mkdir(parents=True)
    for name, label in (("params", "params.json"), ("controller_policy", "controller_policy.json"), ("fixture", "fixture.json")):
        (out_dir / "inputs").mkdir(exist_ok=True)
        _write_new(out_dir / "inputs" / label, inputs.raw_bytes[name])
    endpoints = {}
    for item in values["telemetry_sources"]["prometheus_endpoints"]:
        name, _, url = item.partition("=")
        endpoints[name] = url
    freshness = inputs.policy.freshness_policy()
    rig = FixtureRig(out_dir / "fixture_evidence", channel=channel, run_id=run_id, fixture=fixture,
                     window_s=values["telemetry_adapter"]["window_s"], stale_after_s=inputs.policy.client_stale_after_s,
                     recorder_latency=values["telemetry_adapter"]["recorder_latency"], endpoints=endpoints,
                     reported=fixture.channel_configuration, freshness=freshness,
                     horizon_seconds=inputs.policy.predictive["horizon_seconds"], history=captured_history,
                     wall_clock_steps=_wall_clock_steps)
    components = fixture_predictive_components(rig)
    controllers, space = build_controllers(inputs, rig, components)
    runner = Stage3ShadowRunner(
        controllers, inputs.static_config, run_id=run_id, shadow=True, configuration_policy=space, freshness_policy=freshness,
        artifact_provenance={"model_identity": FIXTURE_PERFORMANCE_IDENTITY, "frozen_model_loaded": False,
                             "forecast_identity": FIXTURE_FORECAST_IDENTITY, "frozen_forecaster_loaded": False},
        max_record_bytes=RECORD_BYTES_LIMIT, max_telemetry_age_s=inputs.policy.freshness_max_age_s,
        clock=rig.runner_clock, invalid_frame_policy="abstain", frame_quality=inputs.contract)
    assert_no_live_actuation(ResearchStage.LIVE_SHADOW, runner.actuators.values())
    if not all(isinstance(a, ShadowActuator) for a in runner.actuators.values()):
        raise Stage3RunnerError("every actuator must be a ShadowActuator")
    frames: dict[int, dict[str, Any]] = {}
    history_frames: list[dict[str, Any]] = []

    def frame_stream():
        for index, step in enumerate(fixture.steps):
            rig.apply_step(step, fixture.channel_configuration)
            frame = rig.frame(index)
            if _frame_transform is not None:
                frame = _frame_transform(index, frame)
            frames[index] = summarize_frame(frame, rig.work_dir)
            if captured_history is not None:
                evidence = captured_history.evidence(frame.observation_timestamp_utc)
                history_frames.append({"frame": index, **{k: evidence[k] for k in (
                    "decision_cutoff_utc", "validation_outcome", "valid_bin_count", "history_start_utc", "history_end_utc",
                    "missing_interval_count", "missing_seconds", "unavailable_seconds", "no_future_data", "reasons",
                    "coverage_mode", "coverage_provenance")}})
                frames[index]["coverage_provenance"] = evidence["coverage_provenance"]       # A0i: the decision row carries it (build_decision_rows)
            yield frame
            if _frame_hook is not None:
                _frame_hook(index)

    native_dir = out_dir / "native"
    status, native, error = "COMPLETED", None, None
    try:
        native = runner.run(frame_stream(), output_dir=native_dir, retain_records=False)
        if native["stop_reasons"]:
            status = "STOPPED_BY_SAFETY_LIMIT"
    except KeyboardInterrupt:
        status = "INTERRUPTED"
    except BaseException as exc:                                  # record the failure, then re-raise after writing the manifest
        status, error = "FAILED", f"{type(exc).__name__}: {exc}"
    records, truncated = read_jsonl_tolerant(native_dir / "controller_decisions.jsonl")
    rows = build_decision_rows(records, frames, run_id=run_id, channel=channel, cell=values["cell"], mode=MODE_FIXTURE,
                               evidence_class=f"{fixture.provenance}_FIXTURE",
                               predictive_label=components.label() if inputs.policy.schema == POLICY_SCHEMA_V2 else None)
    stop_reasons = list(native["stop_reasons"]) if native else []
    summary = summarize_run(rows, status=status, stop_reasons=stop_reasons, native=native, frames_expected=len(fixture.steps))
    if truncated:
        summary["interrupted_partial_record_dropped"] = True
    if status == "COMPLETED" and not summary["comparability"]["comparable"]:
        summary["status"] = status = "INCONSISTENT"
    shadow_proposals = sum(len(a.proposals) for a in runner.actuators.values())
    if captured_history is not None:
        outcomes: dict[str, int] = {}
        for item in history_frames:
            outcomes[item["validation_outcome"]] = outcomes.get(item["validation_outcome"], 0) + 1
        summary["captured_history_frame_outcomes"] = outcomes
        summary["coverage_mode"] = captured_history.coverage_mode
    if inputs.policy.schema == POLICY_SCHEMA_V2:
        summary["freshness_limits"] = freshness.as_dict()
        summary["predictive_mode"] = components.mode
        summary["real_inference_performed"] = False
    summary["shadow_actuator_proposals_recorded"] = shadow_proposals
    summary["shadow_actuator_proposals_accepted"] = sum(1 for a in runner.actuators.values() for p in a.proposals if p.accepted)
    with (out_dir / "shadow_decisions.jsonl").open("x", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True) + "\n")
    _write_new_json(out_dir / "run_summary.json", summary)
    manifest = {
        "schema": MANIFEST_SCHEMA, "tool": TOOL_VERSION, "status": status, "mode": MODE_FIXTURE, "live_ready": False,
        "evidence_class": f"{fixture.provenance}_FIXTURE", "fixture_description": fixture.description,
        "run_id": run_id, "channel": channel, "cell": dict(values["cell"]), "cell_id": digest(values["cell"]),
        "static_configuration": inputs.static_config.as_dict(),
        "input_sha256": {name: sha256_bytes(data) for name, data in sorted(inputs.raw_bytes.items())},
        "tool_sha256": {name: sha256_file(path) for name, path in sorted((tool_files or {}).items())},
        "prelive_link": None if prelive_link is None else dict(prelive_link),
        "controllers": list(CONTROLLERS),
        "actuation": {"configuration_changes_applied": 0, "actuator": "ShadowActuator (records proposals only)",
                      "live_actuator_available": False},
        "models": {"frozen_hfbtp_model_loaded": False, "frozen_ewma_loaded": False,
                   "performance_model": FIXTURE_PERFORMANCE_IDENTITY, "forecaster": FIXTURE_FORECAST_IDENTITY},
        "fixture_time_basis": "scripted fixture clock; no wall clock was read for any recorded timestamp",
        "limitations": list(LIMITATIONS), "error": error,
    }
    if inputs.policy.schema == POLICY_SCHEMA_V2:
        manifest["freshness"] = {"limits_s": freshness.as_dict(), "basis": inputs.policy.freshness_basis,
                                 "note": "operator-supplied; no scientific justification is claimed; Windows-WSL clock offset is not compensated"}
        manifest["predictive"] = {**components.label(), "feature_schema": {"id": sp.FEATURE_SCHEMA_ID}}
    if captured_history is not None:
        final = captured_history.evidence(history_frames[-1]["decision_cutoff_utc"]) if history_frames else None
        manifest["captured_history"] = {
            "schema": "stage3_captured_history_evidence/1", "source": captured_history.source.as_dict(),
            "zero_policy": captured_history.zero_policy, "live_semantics": captured_history.zero_policy == "verified_only",
            "coverage_mode": captured_history.coverage_mode,
            "forecast_horizon_seconds": captured_history.forecast_horizon_s, "min_history_bins": captured_history.min_history_bins,
            "input_report": dict(captured_history.binned.report), "bins_fed": dict(captured_history.counts_fed),
            "frames": history_frames, "final_frame_evidence": final,
            "limitations": ["Ethereum mempool arrivals, not Fabric observations", "forecast and performance values are scripted fixtures",
                            "a valid history shows the input contract was met, not that any forecast is accurate"]}
    manifest["files"] = _hash_tree(out_dir, skip=("run_manifest.json",))
    _write_new_json(out_dir / "run_manifest.json", manifest)
    if error is not None:
        raise Stage3RunnerError(f"run failed after writing a FAILED manifest to {out_dir}: {error}")
    return manifest


def verify_run_directory(run_dir: Path) -> list[str]:
    """Re-hash a finished run directory against its manifest. Returns the list of problems (empty means intact)."""
    run_dir = Path(run_dir)
    try:
        manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return [f"run_manifest.json unreadable: {exc}"]
    actual = _hash_tree(run_dir, skip=("run_manifest.json",))
    recorded = manifest.get("files", {})
    problems = [f"{name}: {'missing' if name not in actual else 'modified'}" for name, d in sorted(recorded.items()) if actual.get(name) != d]
    problems += [f"{name}: not in the manifest" for name in sorted(set(actual) - set(recorded))]
    return problems


# ------------------------------------------------------------------------------------------------ mode B: live gates
GATE_NOT_MET, GATE_NOT_AVAILABLE = "NOT_MET", "NOT_AVAILABLE"


def evaluate_live_gates(params_path: Path, *, candidate_manifest_path: Path | None = None, repo_root: Path | None = None,
                        prelive_run_dir: Path | None = None) -> dict[str, Any]:
    """Mode B. A read-only report of the independent gates a future live shadow run needs. It never permits a live run.

    It reads files only. It opens no connection, starts nothing and consults no environment variable. No authorization
    record format exists in the repository, so channel approval and execution authorization are ``NOT_AVAILABLE`` by
    construction, and ``live_permitted`` is the constant ``False``.
    """
    gates: list[dict[str, Any]] = []
    try:
        values = pc.validate_operator_parameters(json.loads(Path(params_path).read_text(encoding="utf-8"))).values
        channel = cs.select_experiment_channel(values["channel"])
        gates.append({"gate": "G1_channel", "status": GATE_NOT_MET, "met": False,
                      "detail": f"channel {channel!r} is syntactically eligible for owner consideration only. No owner approval "
                                "record format exists and the channel's existence is UNVERIFIED."})
    except (OSError, ValueError, pc.PreliveParameterError, cs.ChannelSelectionError) as exc:
        gates.append({"gate": "G1_channel", "status": GATE_NOT_MET, "met": False, "detail": f"{type(exc).__name__}: {exc}"})
    artifact: dict[str, Any] = {"gate": "G2_artifact_integrity", "status": GATE_NOT_MET, "met": False}
    try:
        from . import prelive_integrity as pi
        manifest = json.loads(Path(candidate_manifest_path).read_text(encoding="utf-8")) if candidate_manifest_path else None
        if manifest is None:
            artifact["detail"] = "no candidate manifest supplied"
        else:
            findings = pi.verify_candidate_manifest(manifest, Path(repo_root) if repo_root else Path.cwd())
            artifact["detail"] = (f"candidate manifest status {manifest.get('status')!r}, approved={manifest.get('approved')!r}; "
                                  f"{len(findings)} integrity finding(s). Hash agreement is not owner approval.")
            artifact["findings"] = [str(f) for f in findings]
    except (OSError, ValueError, KeyError) as exc:
        artifact["detail"] = f"{type(exc).__name__}: {exc}"
    gates.append(artifact)
    gates.append({"gate": "G3_clock_preflight", "status": GATE_NOT_MET, "met": False,
                  "detail": "a Windows-WSL clock agreement needs measured round-trip samples evaluated by clock_preflight; none "
                            "were measured. A fixture clock or an offline file is not a measurement."
                            + (f" Prepared run directory supplied: {Path(prelive_run_dir).name}." if prelive_run_dir else "")})
    gates.append({"gate": "G4_operational_execution", "status": GATE_NOT_AVAILABLE, "met": False,
                  "detail": "no authorization mechanism exists in the repository and no live collector wiring exists in this runner. "
                            "Environment variables and command-line flags are not authorization."})
    return {"mode": "live_shadow", "live_permitted": False, "connections_opened": 0, "gates": gates,
            "result": "REFUSED: live shadow observation is not authorized and not implemented in this task."}
