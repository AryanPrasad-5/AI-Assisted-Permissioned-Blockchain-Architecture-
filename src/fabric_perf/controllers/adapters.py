"""Adapters from verified forecasting/model interfaces to controller protocols."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from time import perf_counter_ns
from typing import Iterable, Mapping

import pandas as pd
import numpy as np

from fabric_perf.models.artifacts import load_hfbtp_artifact
from fabric_perf.forecasting.flashbots import forecast_baseline
from fabric_perf.forecasting.horizon_benchmark import ewma_states
from fabric_perf.models.baseline import FEATURE_COLUMNS, add_engineered_features

from .base import FabricConfig, PerformancePrediction, coerce_config, _finite_number
from .telemetry import parse_observation_time


class RollingMeanArrivalForecaster:
    """Transparent online forecaster using only supplied arrival history.

    This is an adapter for a baseline experiment, not a TimesFM substitute.
    """

    def __init__(self, window: int = 60):
        if window < 1:
            raise ValueError("window must be positive")
        self.window = window

    def predict(self, history_tps: Iterable[float], horizon_seconds: int) -> float:
        history = tuple(float(x) for x in history_tps)
        if not history:
            raise ValueError("history is empty")
        return sum(history[-self.window:]) / min(len(history), self.window)


@dataclass(frozen=True)
class ForecastResult:
    """Auditable result for one Flashbots forecast call."""

    value_tps: float
    horizon_seconds: int
    frequency_seconds: int
    model_identity: str
    inference_duration_ms: float
    source: str
    origin_timestamp_utc: str | None = None
    forecast_start_timestamp_utc: str | None = None
    forecast_completion_timestamp_utc: str | None = None
    history_last_timestamp_utc: str | None = None
    value_unit: str = "transactions/s"
    initialization_provenance: dict | None = None

    def __post_init__(self) -> None:
        _finite_number("forecast TPS", self.value_tps, minimum=0)
        _finite_number("forecast inference duration ms", self.inference_duration_ms, minimum=0)
        if self.value_unit != "transactions/s":
            raise ValueError("forecast value_unit must be transactions/s")
        if any(isinstance(v, bool) or not isinstance(v, int) or v < 1
               for v in (self.horizon_seconds, self.frequency_seconds)):
            raise ValueError("forecast horizon and frequency must be positive integers")
        if self.horizon_seconds % self.frequency_seconds:
            raise ValueError("forecast horizon must be a multiple of the sampling frequency")
        for name in ("origin_timestamp_utc", "forecast_start_timestamp_utc", "forecast_completion_timestamp_utc", "history_last_timestamp_utc"):
            if getattr(self, name) is not None:
                parse_observation_time(getattr(self, name), field=name)
        if self.origin_timestamp_utc and self.history_last_timestamp_utc and parse_observation_time(self.history_last_timestamp_utc) > parse_observation_time(self.origin_timestamp_utc):
            raise ValueError("forecast history ends after its origin")

    def as_dict(self) -> dict:
        result = self.__dict__.copy()
        result["target"] = "mean arrival rate over [origin, origin + horizon)"
        result["target_end_timestamp_utc"] = ((parse_observation_time(self.origin_timestamp_utc) + timedelta(seconds=self.horizon_seconds)).isoformat()
                                               if self.origin_timestamp_utc else None)
        return result


class FlashbotsSeriesForecaster:
    """Use one approved baseline over one processed UTC daily series.

    The series is loaded from ``data/processed/flashbots`` and is never joined
    to the other observation day.  ``predict`` receives only observations
    before the origin; ``forecast_at`` is provided for deterministic replay
    against a stored series.
    """

    SUPPORTED_FREQUENCIES = (1, 5, 10)

    def __init__(self, series_path: Path, *, baseline: str, frequency_seconds: int | None = None):
        self.series_path = Path(series_path)
        if not self.series_path.is_file():
            raise FileNotFoundError(self.series_path)
        frame = pd.read_csv(self.series_path, parse_dates=["timestamp_utc"])
        required = {"timestamp_utc", "arrival_count", "frequency_seconds", "date_utc"}
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"processed Flashbots series is missing columns: {sorted(missing)}")
        if frame.empty:
            raise ValueError("processed Flashbots series is empty")
        frequencies = set(pd.to_numeric(frame["frequency_seconds"], errors="raise"))
        dates = {str(v) for v in frame["date_utc"].dropna().unique()}
        actual_frequency = next(iter(frequencies)) if len(frequencies) == 1 else None
        if actual_frequency not in self.SUPPORTED_FREQUENCIES:
            raise ValueError(f"series must contain exactly one supported frequency, got {frequencies}")
        if len(dates) != 1:
            raise ValueError("a forecaster must contain exactly one UTC observation day")
        if frequency_seconds is not None and int(frequency_seconds) != actual_frequency:
            raise ValueError("requested frequency does not match the processed series")
        if baseline not in {
            "persistence", "rolling_mean_60s", "ewma_alpha_0.2", "ewma_alpha_0.5",
            "seasonal_naive_60s", "seasonal_naive_300s", "poisson_rate_300s", "ewma_alpha_0.005",
        }:
            raise ValueError(f"baseline {baseline!r} is not in the evaluated Flashbots baseline set")
        values = pd.to_numeric(frame["arrival_count"], errors="coerce")
        if not values.map(math.isfinite).all() or (values < 0).any():
            raise ValueError("arrival_count must be finite and non-negative")
        timestamps = pd.to_datetime(frame["timestamp_utc"], utc=True, errors="raise")
        if timestamps.isna().any() or timestamps.duplicated().any() or not timestamps.is_monotonic_increasing:
            raise ValueError("processed Flashbots series is not chronological")
        if not timestamps.diff().iloc[1:].eq(pd.Timedelta(seconds=actual_frequency)).all():
            raise ValueError("processed Flashbots series has missing or irregular bins")
        if frame["date_utc"].isna().any() or set(timestamps.dt.strftime("%Y-%m-%d")) != dates:
            raise ValueError("series dates disagree with UTC timestamps")
        frame["timestamp_utc"] = timestamps
        self.frame = frame
        self.values = values.to_numpy(dtype=float)
        self.baseline = baseline
        self.frequency_seconds = actual_frequency
        self.date_utc = next(iter(dates))
        self.model_identity = f"flashbots-baseline:{baseline}:v1:{self.frequency_seconds}s"
        self.last_result: ForecastResult | None = None
        self.requires_timestamped_history = baseline == "ewma_alpha_0.005"
        if baseline == "ewma_alpha_0.005" and actual_frequency != 1:
            raise ValueError("frozen EWMA requires one-second counts")
        if self.requires_timestamped_history and timestamps.iloc[0] != timestamps.iloc[0].normalize():
            raise ValueError("frozen EWMA requires a stored series starting at UTC midnight")

    @classmethod
    def from_frozen_selection(cls, series_path: Path, selection_path: Path, *,
                              selection_sha256: str, series_sha256: str) -> "FlashbotsSeriesForecaster":
        """Wire the frozen H=60 selection without retuning or opening targets."""
        for path, expected in ((selection_path, selection_sha256), (series_path, series_sha256)):
            if hashlib.sha256(Path(path).read_bytes()).hexdigest().upper() != expected.upper():
                raise ValueError(f"frozen input SHA-256 mismatch: {path}")
        selected = json.loads(Path(selection_path).read_text(encoding="utf-8"))
        info = selected["horizons"]["60"]
        if info["selected_model"] != "ewma" or info["alpha"] != 0.005 or selected["min_history_s"] != 600:
            raise ValueError("selection differs from frozen EWMA alpha=0.005, H=60, minimum history=600")
        forecaster = cls(series_path, baseline="ewma_alpha_0.005")
        forecaster.selection_sha256 = selection_sha256.upper()
        return forecaster

    def _forecast_counts(self, history_counts: Iterable[float], horizon_seconds: int,
                         *, origin_timestamp_utc: str | None = None) -> ForecastResult:
        if horizon_seconds < 1 or horizon_seconds % self.frequency_seconds:
            raise ValueError("horizon_seconds must be a positive multiple of series frequency")
        history_values = tuple(float(v) for v in history_counts)
        if not history_values or any(not math.isfinite(v) or v < 0 for v in history_values):
            raise ValueError("forecast history must be finite, non-negative and non-empty")
        if self.baseline == "ewma_alpha_0.005":
            if horizon_seconds != 60:
                raise ValueError("frozen EWMA requires horizon_seconds=60")
            if len(history_values) < 600:
                raise ValueError("frozen EWMA requires at least 600 seconds of history")
        started_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        started = perf_counter_ns()
        if self.baseline == "ewma_alpha_0.005":
            # Reuse the frozen causal recurrence, including its first-bin seed.
            value_tps = float(ewma_states(history_values, 0.005)[-1])
        else:
            forecast = forecast_baseline(self.baseline, history_values,
                                         horizon_seconds // self.frequency_seconds, self.frequency_seconds)
            value_tps = float(forecast.mean()) / self.frequency_seconds
        elapsed = (perf_counter_ns() - started) / 1_000_000
        completed_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        result = ForecastResult(
            value_tps=value_tps,
            horizon_seconds=horizon_seconds,
            frequency_seconds=self.frequency_seconds,
            model_identity=self.model_identity,
            inference_duration_ms=elapsed,
            source=str(self.series_path),
            origin_timestamp_utc=origin_timestamp_utc,
            forecast_start_timestamp_utc=started_utc,
            forecast_completion_timestamp_utc=completed_utc,
            history_last_timestamp_utc=((datetime.fromisoformat(origin_timestamp_utc) - pd.Timedelta(seconds=1)).isoformat()
                                        if self.requires_timestamped_history and origin_timestamp_utc else origin_timestamp_utc),
        )
        self.last_result = result
        return result

    def predict(self, history_tps: Iterable[float], horizon_seconds: int) -> float:
        """Return a scalar TPS forecast for the controller protocol."""
        if self.requires_timestamped_history:
            raise ValueError("frozen EWMA requires timestamped complete-day history for freshness and prefix validation")
        # ControllerState stores rates. The processed series stores counts per
        # bin, so convert only at this adapter boundary.
        history_counts = (float(v) * self.frequency_seconds for v in history_tps)
        return self._forecast_counts(history_counts, horizon_seconds).value_tps

    def predict_timestamped(
        self,
        history_timestamps_utc: Iterable[str],
        history_tps: Iterable[float],
        horizon_seconds: int,
        *,
        now_utc: str | None = None,
        max_history_age_s: float = 5.0,
    ) -> ForecastResult:
        """Forecast only when the last observed bin is fresh and ordered."""
        timestamps = tuple(str(value) for value in history_timestamps_utc)
        rates = tuple(float(value) for value in history_tps)
        if len(timestamps) != len(rates) or not timestamps:
            raise ValueError("timestamped forecast history must have equal non-empty timestamps and values")
        parsed = [datetime.fromisoformat(value.replace("Z", "+00:00")) for value in timestamps]
        if any(value.tzinfo is None for value in parsed):
            raise ValueError("forecast history timestamps need timezone")
        if any((right - left).total_seconds() != self.frequency_seconds for left, right in zip(parsed, parsed[1:])):
            raise ValueError("forecast history timestamps are not chronological")
        reference = datetime.now(timezone.utc) if now_utc is None else datetime.fromisoformat(now_utc.replace("Z", "+00:00"))
        age_s = (reference - parsed[-1]).total_seconds()
        if age_s < 0 or age_s > max_history_age_s:
            raise ValueError(f"forecast history is stale by {age_s:.3f}s")
        if self.baseline == "ewma_alpha_0.005":
            if age_s < self.frequency_seconds:
                raise ValueError("last count bin has not completed at forecast origin")
            expected_times = tuple(self.frame["timestamp_utc"].iloc[:len(parsed)].dt.to_pydatetime())
            if tuple(parsed) != expected_times or rates != tuple(self.values[:len(rates)]):
                raise ValueError("frozen replay requires the complete stored day prefix; truncated or mismatched history")
        history_counts = (value * self.frequency_seconds for value in rates)
        origin = (parsed[-1] + pd.Timedelta(seconds=self.frequency_seconds)).isoformat() if self.requires_timestamped_history else timestamps[-1]
        result = self._forecast_counts(history_counts, horizon_seconds, origin_timestamp_utc=origin)
        return result

    def forecast_at(self, origin_bin: int, horizon_seconds: int) -> ForecastResult:
        """Forecast at a stored-series origin using values strictly before it."""
        if not isinstance(origin_bin, int) or origin_bin < 1 or origin_bin > len(self.values):
            raise ValueError("origin_bin must select at least one observed bin")
        origin_timestamp = self.frame.iloc[origin_bin - 1]["timestamp_utc"].isoformat()
        if self.requires_timestamped_history:
            origin_timestamp = (self.frame.iloc[origin_bin - 1]["timestamp_utc"] + pd.Timedelta(seconds=1)).isoformat()
        return self._forecast_counts(self.values[:origin_bin], horizon_seconds, origin_timestamp_utc=origin_timestamp)


@dataclass(frozen=True)
class ApprovedMaxMessageCountSpace:
    """Explicit live candidate policy and HFBTP feature mapping.

    HFBTP's ``Block Size`` is a dataset feature; it is not silently assumed to
    be Fabric ``MaxMessageCount``.  The mapping is supplied by the experiment
    owner and must be complete for every live candidate.
    """

    allowed_max_message_counts: frozenset[int]
    hfbtp_block_size_by_max_message_count: Mapping[int, int]
    baseline_config: FabricConfig

    def __post_init__(self) -> None:
        values = (*self.allowed_max_message_counts, *self.hfbtp_block_size_by_max_message_count.keys(),
                  *self.hfbtp_block_size_by_max_message_count.values())
        if any(isinstance(v, bool) or not isinstance(v, int) for v in values):
            raise ValueError("candidate space and HFBTP mapping require integers")
        allowed = frozenset(self.allowed_max_message_counts)
        if not allowed:
            raise ValueError("at least one approved MaxMessageCount is required")
        mapping = {int(k): int(v) for k, v in self.hfbtp_block_size_by_max_message_count.items()}
        if set(mapping) != set(allowed):
            raise ValueError("HFBTP mapping must cover exactly the approved live candidates")
        if int(self.baseline_config.max_message_count) not in allowed:
            raise ValueError("baseline configuration must be one of the approved live candidates")
        if any(v < 1 for v in allowed | frozenset(mapping.values())):
            raise ValueError("candidate and HFBTP block sizes must be positive")
        object.__setattr__(self, "allowed_max_message_counts", allowed)
        object.__setattr__(self, "hfbtp_block_size_by_max_message_count", mapping)

    def validate(self, config: FabricConfig) -> None:
        config = coerce_config(config)
        if config.max_message_count not in self.allowed_max_message_counts:
            raise ValueError(f"MaxMessageCount {config.max_message_count} is not approved")
        for field in ("batch_timeout_ms", "preferred_max_bytes", "absolute_max_bytes", "orderers"):
            if getattr(config, field) != getattr(self.baseline_config, field):
                raise ValueError(f"live policy permits only MaxMessageCount; {field} changed")

    def hfbtp_block_size(self, config: FabricConfig) -> int:
        self.validate(config)
        return self.hfbtp_block_size_by_max_message_count[config.max_message_count]


class HFBTPArtifactPerformancePredictor:
    """Use the existing frozen HFBTP artifacts without retraining or mutation."""

    # The verified HFBTP artifact was trained on this documented domain. The
    # adapter rejects extrapolation rather than presenting an unsupported model
    # output to the predictive controller.
    ARRIVAL_MIN, ARRIVAL_MAX = 10.0, 200.0
    BLOCK_MIN, BLOCK_MAX = 10, 800
    ORDERERS_MIN, ORDERERS_MAX = 3, 9

    def __init__(self, artifact_dir: Path, *, candidate_space: ApprovedMaxMessageCountSpace):
        self.artifact_dir = Path(artifact_dir)
        self.candidate_space = candidate_space
        self._estimators = {}
        self._metadata = {}
        for target in ("Throughput", "Avg Latency"):
            estimator, metadata = load_hfbtp_artifact(self.artifact_dir, target)
            if metadata.get("features") != FEATURE_COLUMNS:
                raise ValueError("HFBTP artifact feature schema does not match this package")
            if metadata.get("model_name") != "random_forest" or metadata.get("rows") != 24687:
                raise ValueError("controller requires the full-grid HFBTP random forest artifact")
            self._estimators[target] = estimator
            self._metadata[target] = metadata

    def validate_config(self, config: FabricConfig) -> int:
        """Validate dataset configuration support without running an estimator."""
        config = coerce_config(config)
        block_size = self.candidate_space.hfbtp_block_size(config)
        if not self.BLOCK_MIN <= block_size <= self.BLOCK_MAX:
            raise ValueError("MaxMessageCount is outside the verified HFBTP artifact domain [10, 800]")
        if block_size % 5 or config.orderers not in {3, 5, 7, 9}:
            raise ValueError("block size/orderer count is not on the supported HFBTP configuration grid")
        if (block_size, config.orderers) in {(245, 9), (525, 9), (725, 5)}:
            raise ValueError("configuration is one of HFBTP's absent grid cells")
        return block_size

    def predict(self, arrival_rate_tps: float, config: FabricConfig) -> PerformancePrediction:
        started = perf_counter_ns()
        config = coerce_config(config)
        arrival_rate_tps = _finite_number("arrival rate TPS", arrival_rate_tps, minimum=0)
        if not self.ARRIVAL_MIN <= arrival_rate_tps <= self.ARRIVAL_MAX:
            raise ValueError("arrival rate is outside the verified HFBTP artifact domain [10, 200] tx/s")
        block_size = self.validate_config(config)
        row = pd.DataFrame([{
            "Set Transaction Arrival Rate": float(arrival_rate_tps),
            "Block Size": float(block_size),
            "Orderers": int(config.orderers),
        }])
        engineered = add_engineered_features(row)[FEATURE_COLUMNS]
        output = {}
        for target, estimator in self._estimators.items():
            values = np.asarray(estimator.predict(engineered))
            if values.shape != (1,):
                raise ValueError(f"{target} estimator must return one scalar per input row")
            output[target] = _finite_number(f"{target} prediction", values[0].item(), minimum=0)
        prediction = PerformancePrediction(
            throughput_tps=float(output["Throughput"]),
            latency_ms=float(output["Avg Latency"]) * 1000.0,
        )
        self.last_prediction_metadata = {
            "model_identity": "HFBTP:frozen-artifact-v1:random_forest",
            "inference_duration_ms": (perf_counter_ns() - started) / 1_000_000,
            "hfbtp_block_size": block_size,
            "feature_order": list(FEATURE_COLUMNS),
            "feature_values": {name: float(engineered.iloc[0][name]) for name in FEATURE_COLUMNS},
            "feature_units": {
                "Set Transaction Arrival Rate": "tx/s", "Block Size": "HFBTP dataset block-size units",
                "Orderers": "count", "log_set_rate": "log10(numeric rate in tx/s)",
                "log_block_size": "log10(numeric HFBTP block size)", "block_per_unit_load": "block size / (tx/s)",
                "set_x_orderers": "(tx/s) * orderer count", "block_over_orderers": "block size / orderer count",
            },
            "target_units": {"Throughput": "tx/s", "Avg Latency": "s", "controller_latency": "ms"},
            "construct": "HFBTP finite-round response surface; not validated local Fabric capacity",
            "arrival_rate_tps": float(arrival_rate_tps),
            "config": config.as_dict(),
        }
        return prediction
