"""Read-only streaming workload contracts and shadow controller assembly.

No live workload connector or configuration actuator is supplied here.
"""
from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from time import perf_counter_ns
from typing import Protocol

from .adapters import ApprovedMaxMessageCountSpace, ForecastResult, HFBTPArtifactPerformancePredictor
from .base import FabricConfig, StaticController, confirmed_config
from .live_integration import IntegratedControllerRunner, ProductionTelemetryProvider
from .offline_replay import verified_inputs
from .strategies import PredictiveController, ReactiveController
from .research_stages import ResearchStage, assert_no_live_actuation
from .stage3_frame_quality import Stage3FrameQualityContract
from .telemetry import PrometheusTelemetryAdapter, parse_observation_time
from .telemetry_freshness import FreshnessPolicy
from .workload_history import ArrivalHistoryStore, WorkloadHistoryError, reproduce_ewma_forecast
from fabric_perf.forecasting.horizon_benchmark import ewma_states


class WorkloadSource(Protocol):
    """Provide verified completed 1-second bins, never inferred empty bins."""

    source: str
    def history(self) -> tuple[tuple[str, ...], tuple[float, ...]]: ...


class StreamingWorkload(ArrivalHistoryStore):
    """One source's 1-second arrival counts, held once in a shared append-only store.

    Defaults to ``retain_days=LIVE_SHADOW_RETAIN_DAYS``; pass ``log_path`` to keep an append-only
    record of every observation so evicted days stay reproducible.

    Strict live gap policy (owner Decision 2, 2026-11-17): a healthy zero is recorded as 0; a
    missing or unavailable second is never zero, interpolated or forward-filled. The UTC day
    stays incomplete (forecasting abstains) unless verified historical observations backfill
    it. Rejected duplicates, future/stale samples and invalid values do not mutate the store.
    See ``workload_history.ArrivalHistoryStore`` for the full contract.
    """


# Explicit resource limits for the live shadow foundation, set from measurements on 2026-11-17
# (D3_MEMORY_AND_WORKLOAD_GAP_FIX_2026-11-17.md): the largest compact record measured was
# 23,679 bytes (predictive arm, 2 candidates, 86,396-bin history) and did not vary with history
# length; a full UTC day held in the store measured 15.7 MB (182 B per bin).
LIVE_SHADOW_MAX_RECORD_BYTES = 65_536
LIVE_SHADOW_RETAIN_DAYS = 2

EWMA_PARAMETERS = {"alpha": 0.005, "horizon_seconds": 60, "frequency_seconds": 1, "min_history_bins": 600,
                   "initialization": "e_0 = x_0 at 00:00:00 UTC; complete current UTC-day prefix",
                   "recurrence": "e_k = alpha*x_k + (1-alpha)*e_(k-1); forecast at origin t = e_(t-1)"}


class StreamingEWMA:
    """Reconstruct the frozen recurrence from the complete UTC-day prefix."""

    requires_timestamped_history = True
    accepts_history_reference = True

    def __init__(self, source: str):
        if not source:
            raise ValueError("workload provenance is required")
        self.source = source
        self.last_result = None

    def predict_timestamped(self, history_timestamps_utc, history_tps, horizon_seconds,
                            *, now_utc, max_history_age_s=5.0, history_ref=None):
        self.last_result = None
        times = tuple(parse_observation_time(t) for t in history_timestamps_utc)
        values = tuple(history_tps)
        now = parse_observation_time(now_utc)
        if horizon_seconds != 60 or len(times) < 600 or len(times) != len(values):
            raise ValueError("frozen EWMA requires H=60 and at least 600 contiguous bins")
        if times[0].hour or times[0].minute or times[0].second or times[0].microsecond or times[-1].date() != times[0].date():
            raise ValueError("frozen EWMA requires the complete current UTC-day prefix from midnight")
        if any(t.microsecond for t in times) or any((b-a).total_seconds() != 1 for a, b in zip(times, times[1:])):
            raise ValueError("history must contain ordered, contiguous whole-second bins")
        if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0 or not float(v).is_integer() for v in values):
            raise ValueError("1-second arrival counts must be finite non-negative integers")
        # The origin (end of the last bin) must lie in the prefix's UTC day. At 00:00:00 the
        # current day's prefix is empty; a previous-day state would join days, which the
        # frozen benchmark never does. (Independent audit 2026-11-17, defect D1.)
        if (times[-1] + timedelta(seconds=1)).date() != times[0].date():
            raise ValueError("frozen EWMA requires the complete current UTC-day prefix from midnight; origin is on the next UTC day")
        age = (now-times[-1]).total_seconds()
        if isinstance(max_history_age_s, bool) or not isinstance(max_history_age_s, (int, float)) or not math.isfinite(max_history_age_s) or max_history_age_s < 1:
            raise ValueError("max_history_age_s must be finite and at least 1")
        if not 1 <= age <= max_history_age_s:
            raise ValueError("latest bin is incomplete, stale or in the future")
        reference = None
        if history_ref is not None:
            # The reference must name exactly this prefix (O(1) boundary check; the store
            # computed the digest atomically with the bins it handed out).
            reference = history_ref.as_dict() if hasattr(history_ref, "as_dict") else dict(history_ref)
            if (reference.get("status") != "complete" or reference.get("source") != self.source
                    or reference.get("bin_count") != len(times)
                    or reference.get("first_bin_timestamp_utc") != times[0].isoformat()
                    or reference.get("last_bin_timestamp_utc") != times[-1].isoformat()):
                raise ValueError("history reference does not describe the supplied UTC-day prefix")
        started = perf_counter_ns()
        forecast = float(ewma_states(values, 0.005)[-1])
        self.last_result = ForecastResult(value_tps=forecast, horizon_seconds=60, frequency_seconds=1,
            model_identity="ewma-alpha-0.005-h60-utc-day-prefix", source=self.source,
            initialization_provenance={"method": "reconstructed_utc_day_prefix",
                "source": self.source, "first_bin_timestamp_utc": times[0].isoformat(),
                "last_bin_timestamp_utc": times[-1].isoformat(), "bin_count": len(times),
                "ewma_parameters": dict(EWMA_PARAMETERS),
                **({"history_reference": reference} if reference is not None else {})},
            inference_duration_ms=(perf_counter_ns()-started)/1_000_000,
            origin_timestamp_utc=(times[-1]+timedelta(seconds=1)).isoformat(),
            history_last_timestamp_utc=times[-1].isoformat())
        return self.last_result


def make_shadow_foundation(*, root: Path, manifest_path: Path, endpoints: dict[str, str], channel_id: str,
                           current_config: FabricConfig, config_reader, workload: WorkloadSource,
                           candidate_map: dict[int, int], latency_high_ms: float, latency_low_ms: float,
                           congested_max_message_count: int, consecutive_samples: int,
                           fetch=None, run_id: str = "live-foundation", continuous_source=None,
                           max_record_bytes: int | None = LIVE_SHADOW_MAX_RECORD_BYTES,
                           freshness_policy: FreshnessPolicy | None = None,
                           invalid_frame_policy: str = "stop",
                           required_prometheus_metrics: tuple[str, ...] = ("raft_leader_count",),
                           frame_quality: Stage3FrameQualityContract | None = None):
    """Assemble existing controllers and provider, exclusively in shadow mode.

    Caller supplies independently read config and explicit experiment policy;
    no live source or deployment mapping is implied by this factory.
    ``continuous_source`` is an optional existing ``ContinuousCaliperLedgerAdapter``
    supplying client latency/backlog; without it Prometheus alone gives the
    reactive controller no latency, so it abstains on every frame.

    ``freshness_policy`` is the one validated freshness limit shared by the provider, the runner and
    both controllers (default 5 s). ``invalid_frame_policy`` is ``"stop"`` (the established default:
    a rejected frame is recorded and the run ends) or ``"abstain"`` (each strategy records an
    abstention carrying the structured cause and the run continues; nothing is ever applied).
    ``required_prometheus_metrics`` names adapter metrics whose absence makes a frame incomplete; the
    Raft leader count is required by default because a frame without it is not a healthy observation.

    ``frame_quality`` (task A0c) is the opt-in Stage 3 contract (``stage3_frame_quality``). Its default, None, changes
    nothing: this factory still declares no required client fields and bounds no abstentions. With a contract, every
    frame must carry the required client measurements from a continuous source, each controller receives the same
    abstention for an unusable frame, and the run stops after ``max_consecutive_abstentions`` abstained frames in a
    row. It needs ``invalid_frame_policy="abstain"``; any other value is refused.
    """
    config = confirmed_config(current_config)
    if not channel_id or config_reader is None or workload is None or not getattr(workload, "source", None):
        raise ValueError("channel, configuration reader and named workload source are required")
    keys = ("selection", "model_metadata", "throughput_model", "latency_model")
    pins = verified_inputs(root, manifest_path, required_keys=keys)
    manifest_entries = json.loads(Path(manifest_path).read_text(encoding="utf-8"))["files"]
    selection = json.loads(pins["selection"].read_text(encoding="utf-8"))
    if selection["horizons"]["60"]["selected_model"] != "ewma" or selection["horizons"]["60"]["alpha"] != 0.005 or selection["min_history_s"] != 600:
        raise ValueError("forecast selection differs from frozen profile")
    space = ApprovedMaxMessageCountSpace(frozenset(candidate_map), candidate_map, config)
    if congested_max_message_count not in candidate_map:
        raise ValueError("reactive congested target must be in the supplied candidate map")
    predictor = HFBTPArtifactPerformancePredictor(pins["model_metadata"].parent, candidate_space=space)
    candidates = [replace(config, max_message_count=size) for size in sorted(candidate_map)]
    for candidate in candidates:
        predictor.validate_config(candidate)
    freshness_policy = freshness_policy or FreshnessPolicy()
    age_limit = freshness_policy.max_age_s
    controllers = {
        "static": StaticController(config),
        "reactive": ReactiveController(config, congested_config=replace(config, max_message_count=congested_max_message_count),
                                       latency_high_ms=latency_high_ms, latency_low_ms=latency_low_ms,
                                       consecutive_samples=consecutive_samples, max_telemetry_age_s=age_limit),
        "predictive": PredictiveController(predictor, config, arrival_forecaster=StreamingEWMA(workload.source),
                                            candidate_configs=candidates, horizon_seconds=60,
                                            max_telemetry_age_s=age_limit),
    }
    adapter = PrometheusTelemetryAdapter(endpoints, **({"fetch": fetch} if fetch is not None else {}))
    provider = ProductionTelemetryProvider(adapter, channel_id, config_reader=config_reader,
        continuous_source=continuous_source, arrival_history_reader=workload.history, workload=workload,
        required_fields=(), freshness_policy=freshness_policy,
        required_prometheus_metrics=tuple(required_prometheus_metrics))
    artifact = {"model_identity": "HFBTP:frozen-artifact-v1:random_forest",
                "sha256": {key: manifest_entries[key]["sha256"] for key in keys if key != "selection"},
                "forecast_selection_sha256": manifest_entries["selection"]["sha256"]}
    runner = IntegratedControllerRunner(controllers, config, run_id=run_id, shadow=True, configuration_policy=space,
                                        artifact_provenance=artifact, max_record_bytes=max_record_bytes,
                                        max_telemetry_age_s=age_limit, invalid_frame_policy=invalid_frame_policy,
                                        frame_quality=frame_quality)
    # Stage 3 (live shadow integration) permits only the shadow actuator: no controller can change Fabric.
    assert_no_live_actuation(ResearchStage.LIVE_SHADOW, runner.actuators.values())
    return provider, runner


def reproduce_recorded_forecast(record: dict, store: ArrivalHistoryStore) -> dict:
    """Re-derive one compact predictive decision record's forecast from the shared history.

    The record holds only the history reference (day, boundaries, bin count, SHA-256). The
    prefix is fetched from ``store`` (in memory or rebuilt with ``ArrivalHistoryStore.from_log``),
    its digest re-verified, and the frozen recurrence recomputed. Exact float equality is required.
    """
    trace = record.get("decision_trace") or {}
    reference, forecast = trace.get("history_reference"), trace.get("forecast")
    if reference is None or forecast is None:
        raise WorkloadHistoryError("record has no history reference and forecast to reproduce")
    alpha = forecast["ewma_parameters"]["alpha"]
    value = reproduce_ewma_forecast(store, reference, alpha=alpha)
    return {"recorded_tps": forecast["value_tps"], "reproduced_tps": value, "exact_match": value == forecast["value_tps"],
            "bin_count": reference["bin_count"], "prefix_sha256": reference["prefix_sha256"],
            "first_bin_timestamp_utc": reference["first_bin_timestamp_utc"],
            "last_bin_timestamp_utc": reference["last_bin_timestamp_utc"]}
