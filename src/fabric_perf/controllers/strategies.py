"""Reactive and predictive controller implementations."""

from __future__ import annotations

import math
from dataclasses import replace
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable, Mapping, Protocol

from .base import (
    BaseController,
    ControllerDecision,
    ControllerState,
    FabricConfig,
    PerformancePrediction,
    coerce_config,
    confirmed_config,
    _finite_number,
)


class ArrivalForecaster(Protocol):
    def predict(self, history_tps: Iterable[float], horizon_seconds: int) -> float: ...


class PerformancePredictor(Protocol):
    def predict(self, arrival_rate_tps: float, config: FabricConfig) -> PerformancePrediction: ...


def _samples(name: str, value: int, minimum: int = 0) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")


def _history_times(state: ControllerState, max_age_s: float) -> None:
    """Validate times even when an injected forecaster has no timestamp API."""
    if not state.arrival_history_timestamps_utc:
        return
    if len(state.arrival_history_timestamps_utc) != len(state.arrival_history_tps):
        raise ValueError("forecast history timestamps and rates must have equal lengths")
    parsed = [datetime.fromisoformat(t.replace("Z", "+00:00")) for t in state.arrival_history_timestamps_utc]
    now = datetime.fromisoformat(state.timestamp_utc.replace("Z", "+00:00"))
    if now.tzinfo is None or any(t.tzinfo is None for t in parsed):
        raise ValueError("forecast timestamps must include timezone")
    if any(left >= right for left, right in zip(parsed, parsed[1:])):
        raise ValueError("forecast timestamps must be strictly increasing")
    age = (now - parsed[-1]).total_seconds()
    if age < 0 or age > max_age_s:
        raise ValueError("forecast history is stale or in the future")


def _config_step(config: FabricConfig, direction: int) -> FabricConfig:
    """Small bounded fallback used only when an explicit reactive target is omitted."""
    return replace(config, max_message_count=max(1, config.max_message_count + direction * 5))


class ReactiveController(BaseController):
    """Telemetry-driven controller with threshold confirmation and hysteresis."""

    def __init__(
        self,
        default_config: FabricConfig | Mapping[str, Any],
        latency_threshold: float | None = None,
        *,
        congested_config: FabricConfig | Mapping[str, Any] | None = None,
        latency_high_ms: float | None = None,
        latency_low_ms: float | None = None,
        queue_high: float | None = None,
        queue_low: float | None = None,
        consecutive_samples: int = 1,
        cooldown_samples: int = 3,
        max_telemetry_age_s: float = 5.0,
    ):
        self.default_config = coerce_config(default_config)
        self.congested_config = coerce_config(congested_config) if congested_config is not None else _config_step(self.default_config, -1)
        high = latency_high_ms if latency_high_ms is not None else latency_threshold
        if high is None:
            raise ValueError("latency_threshold or latency_high_ms is required")
        self.latency_high_ms = _finite_number("latency_high_ms", high, minimum=0)
        self.latency_low_ms = _finite_number("latency_low_ms", latency_low_ms if latency_low_ms is not None else high * 0.8, minimum=0)
        if self.latency_low_ms >= self.latency_high_ms:
            raise ValueError("latency_low_ms must be below latency_high_ms")
        self.queue_high = None if queue_high is None else _finite_number("queue_high", queue_high, minimum=0)
        self.queue_low = None if queue_low is None else _finite_number("queue_low", queue_low, minimum=0)
        for name in ("queue_high", "queue_low"):
            if getattr(self, name) is not None:
                _finite_number(name, getattr(self, name), minimum=0)
        if self.queue_low is not None and self.queue_high is None:
            raise ValueError("queue_low requires queue_high")
        if self.queue_high is not None and (self.queue_low is None or self.queue_low >= self.queue_high):
            raise ValueError("queue_low must be below queue_high when queue thresholds are used")
        _samples("consecutive_samples", consecutive_samples, 1)
        _samples("cooldown_samples", cooldown_samples)
        self.consecutive_samples = consecutive_samples
        self.cooldown_samples = cooldown_samples
        _finite_number("max_telemetry_age_s", max_telemetry_age_s, minimum=0)
        if max_telemetry_age_s <= 0:
            raise ValueError("max_telemetry_age_s must be positive")
        self.max_telemetry_age_s = float(max_telemetry_age_s)
        self._active = False
        self._congestion_streak = 0
        self._recovery_streak = 0
        self._since_change = cooldown_samples
        self._verified_config = self.default_config
        self._last_observation_time: datetime | None = None

    def configuration_applied(self, config: FabricConfig) -> None:
        config = confirmed_config(config)
        self._verified_config = config
        self._active = config == self.congested_config and config != self.default_config
        self._since_change = 0
        self._congestion_streak = self._recovery_streak = 0

    def policy_metadata(self) -> dict:
        return {
            "latency_high_ms": self.latency_high_ms, "latency_low_ms": self.latency_low_ms,
            "queue_high": self.queue_high, "queue_low": self.queue_low,
            "consecutive_samples": self.consecutive_samples, "cooldown_samples": self.cooldown_samples,
            "max_telemetry_age_s": self.max_telemetry_age_s,
            "default_config": self.default_config.as_dict(), "congested_config": self.congested_config.as_dict(),
            "verified_config": self._verified_config.as_dict(), "active": self._active,
            "last_observation_timestamp_utc": None if self._last_observation_time is None else self._last_observation_time.isoformat(),
            "congestion_streak": self._congestion_streak, "recovery_streak": self._recovery_streak,
            "cooldown_remaining_samples": max(0, self.cooldown_samples - self._since_change),
        }

    def reject_observation(self, reason: str, *, error: str | None = None) -> ControllerDecision:
        """Break consecutive confirmation without advancing cooldown or applied state."""
        self._congestion_streak = self._recovery_streak = 0
        return ControllerDecision("reactive", self._verified_config, False, reason,
                                  metadata={**self.policy_metadata(), "error": error})

    def _is_congested(self, state: ControllerState) -> bool:
        latency_hit = state.observed_latency_ms is not None and state.observed_latency_ms >= self.latency_high_ms
        queue_hit = self.queue_high is not None and state.queue_depth is not None and state.queue_depth >= self.queue_high
        return bool(latency_hit or queue_hit)

    def _is_recovered(self, state: ControllerState) -> bool:
        latency_ok = state.observed_latency_ms is not None and state.observed_latency_ms <= self.latency_low_ms
        queue_ok = self.queue_low is None or (state.queue_depth is not None and state.queue_depth <= self.queue_low)
        return bool(latency_ok and queue_ok)

    def decide(self, state: ControllerState) -> ControllerDecision:
        # ControllerState is mutable, so validate at use as well as at ingestion.
        try:
            observed = datetime.fromisoformat(state.timestamp_utc.replace("Z", "+00:00"))
            if observed.tzinfo is None:
                raise ValueError("observation timestamp must include timezone")
            observed = observed.astimezone(timezone.utc)
            for name in ("observed_latency_ms", "observed_latency_p95_ms", "queue_depth",
                         "observed_arrival_rate_tps", "observed_throughput_tps", "telemetry_age_s"):
                value = getattr(state, name)
                if value is not None:
                    _finite_number(name, value, minimum=0)
            confirmed_config(state.current_config)
        except (TypeError, ValueError, AttributeError) as exc:
            return self.reject_observation("invalid_telemetry_fail_safe", error=str(exc))
        if state.current_config != self._verified_config:
            return self.reject_observation("configuration_mismatch_fail_safe", error="telemetry cannot confirm a proposed configuration")
        if state.telemetry_age_s is None:
            return self.reject_observation("telemetry_freshness_unknown_fail_safe")
        if state.telemetry_age_s > self.max_telemetry_age_s:
            return self.reject_observation("stale_telemetry_fail_safe", error=f"telemetry age {state.telemetry_age_s}s")
        if self._last_observation_time is not None and observed <= self._last_observation_time:
            return self.reject_observation("nonadvancing_telemetry_fail_safe")
        self._last_observation_time = observed
        if state.observed_latency_ms is None and (self.queue_high is None or state.queue_depth is None):
            return self.reject_observation("telemetry_missing_fail_safe")
        congested = self._is_congested(state)
        recovered = self._is_recovered(state)
        if congested:
            self._congestion_streak += 1
            self._recovery_streak = 0
        elif recovered:
            self._recovery_streak += 1
            self._congestion_streak = 0
        else:
            self._congestion_streak = 0
            self._recovery_streak = 0
        self._since_change += 1

        desired = state.current_config
        reason = "within_hysteresis"
        if not self._active and self._congestion_streak >= self.consecutive_samples:
            if self._since_change >= self.cooldown_samples:
                desired = self.congested_config
                reason = "congestion_threshold_crossed"
            else:
                reason = "congestion_detected_cooldown"
        elif self._active and self._recovery_streak >= self.consecutive_samples:
            if self._since_change >= self.cooldown_samples:
                desired = self.default_config
                reason = "recovery_threshold_crossed"
            else:
                reason = "recovery_detected_cooldown"
        elif self._active:
            desired = self.congested_config
            reason = "congested_state_hold"

        return ControllerDecision(
            controller="reactive",
            config=desired,
            changed=desired != state.current_config,
            reason=reason,
            metadata={
                **self.policy_metadata(),
                "observation_timestamp_utc": state.timestamp_utc,
                "telemetry_age_s": state.telemetry_age_s,
                "observed_latency_ms": state.observed_latency_ms,
                "queue_depth": state.queue_depth,
                "congested": congested,
                "recovered": recovered,
                "congestion_streak": self._congestion_streak,
                "recovery_streak": self._recovery_streak,
                "cooldown_remaining_samples": max(0, self.cooldown_samples - self._since_change),
                "latency_high_ms": self.latency_high_ms,
                "latency_low_ms": self.latency_low_ms,
                "queue_high": self.queue_high,
                "queue_low": self.queue_low,
            },
        )


class PredictiveController(BaseController):
    """Prediction-driven controller using only injected, validated predictors."""

    def __init__(
        self,
        model: PerformancePredictor | None = None,
        default_config: FabricConfig | Mapping[str, Any] | None = None,
        *,
        arrival_forecaster: ArrivalForecaster,
        candidate_configs: Iterable[FabricConfig | Mapping[str, Any]],
        horizon_seconds: int = 10,
        latency_penalty_per_ms: float = 0.01,
        backlog_penalty_per_tps: float = 1.0,
        minimum_utility_improvement: float = 0.0,
        cooldown_samples: int = 3,
        max_telemetry_age_s: float = 5.0,
    ):
        _samples("horizon_seconds", horizon_seconds, 1)
        _samples("cooldown_samples", cooldown_samples)
        for name, value in (("latency_penalty_per_ms", latency_penalty_per_ms),
                            ("backlog_penalty_per_tps", backlog_penalty_per_tps),
                            ("minimum_utility_improvement", minimum_utility_improvement)):
            _finite_number(name, value, minimum=0)
        self.default_config = coerce_config(default_config or FabricConfig())
        self.performance_predictor = model
        self.arrival_forecaster = arrival_forecaster
        self.candidate_configs = tuple(coerce_config(c) for c in candidate_configs)
        if not self.candidate_configs:
            raise ValueError("at least one candidate configuration is required")
        self.horizon_seconds = horizon_seconds
        self.latency_penalty_per_ms = latency_penalty_per_ms
        self.backlog_penalty_per_tps = backlog_penalty_per_tps
        self.minimum_utility_improvement = minimum_utility_improvement
        self.cooldown_samples = cooldown_samples
        _finite_number("max_telemetry_age_s", max_telemetry_age_s, minimum=0)
        if max_telemetry_age_s <= 0:
            raise ValueError("max_telemetry_age_s must be positive")
        self.max_telemetry_age_s = float(max_telemetry_age_s)
        self._since_change = cooldown_samples
        self._verified_config = self.default_config
        self._last_observation_time: datetime | None = None
        self._last_history_time: datetime | None = None
        self._last_forecast_time: datetime | None = None

    def configuration_applied(self, config: FabricConfig) -> None:
        self._verified_config = confirmed_config(config)
        self._since_change = 0

    def policy_metadata(self) -> dict:
        return {
            "horizon_seconds": self.horizon_seconds,
            "latency_penalty_per_ms": self.latency_penalty_per_ms,
            "backlog_penalty_per_tps": self.backlog_penalty_per_tps,
            "minimum_utility_improvement": self.minimum_utility_improvement,
            "cooldown_samples": self.cooldown_samples, "max_telemetry_age_s": self.max_telemetry_age_s,
            "cooldown_remaining_samples": max(0, self.cooldown_samples - self._since_change),
            "verified_config": self._verified_config.as_dict(),
            "candidate_configs": [config.as_dict() for config in self.candidate_configs],
            "last_observation_timestamp_utc": None if self._last_observation_time is None else self._last_observation_time.isoformat(),
            "last_history_timestamp_utc": None if self._last_history_time is None else self._last_history_time.isoformat(),
            "last_forecast_origin_utc": None if self._last_forecast_time is None else self._last_forecast_time.isoformat(),
        }

    def reject_observation(self, reason: str, *, error: str | None = None, stage: str = "observation",
                           arrival: float | None = None, forecast: dict | None = None) -> ControllerDecision:
        return ControllerDecision("predictive", self._verified_config, False, reason,
            predicted_arrival_rate_tps=arrival if arrival is not None and math.isfinite(arrival) and arrival >= 0 else None,
            metadata={**self.policy_metadata(), "decision_kind": "abstain", "error": error,
                      "abstention_stage": stage, "forecast": forecast or {}})

    def _coerce_prediction(self, value: Any) -> PerformancePrediction:
        if isinstance(value, PerformancePrediction):
            return value
        if isinstance(value, Mapping):
            if "latency_ms" in value and "Avg Latency" in value:
                raise ValueError("performance mapping has ambiguous latency units")
            latency = value.get("latency_ms", value.get("Avg Latency"))
            throughput = value.get("throughput_tps", value.get("Throughput"))
            if latency is None or throughput is None:
                raise ValueError("performance prediction mapping lacks throughput/latency")
            throughput = _finite_number("predicted throughput TPS", throughput, minimum=0)
            latency = _finite_number("predicted latency", latency, minimum=0)
            # HFBTP's documented latency target is seconds. Controller-native
            # predictions are milliseconds. Adapters should normally convert it.
            if "latency_ms" not in value:
                latency = float(latency) * 1000
            return PerformancePrediction(float(throughput), float(latency))
        raise TypeError("performance predictor must return PerformancePrediction or a mapping")

    def _utility(self, arrival_rate: float, prediction: PerformancePrediction) -> float:
        backlog = max(0.0, arrival_rate - prediction.throughput_tps)
        return prediction.throughput_tps - self.latency_penalty_per_ms * prediction.latency_ms - self.backlog_penalty_per_tps * backlog

    def decide(self, state: ControllerState) -> ControllerDecision:
        try:
            observed = datetime.fromisoformat(state.timestamp_utc.replace("Z", "+00:00"))
            if observed.tzinfo is None:
                raise ValueError("observation timestamp must include timezone")
            observed = observed.astimezone(timezone.utc)
            confirmed_config(state.current_config)
            if state.telemetry_age_s is not None:
                _finite_number("telemetry_age_s", state.telemetry_age_s, minimum=0)
            for value in state.arrival_history_tps:
                _finite_number("arrival history TPS", value, minimum=0)
        except (ValueError, TypeError, AttributeError) as exc:
            return self.reject_observation("invalid_telemetry_fail_safe", error=str(exc))
        if state.current_config != self._verified_config:
            return self.reject_observation("configuration_mismatch_fail_safe", error="telemetry is not actuator confirmation")
        if state.telemetry_age_s is None:
            return self.reject_observation("telemetry_freshness_unknown_fail_safe")
        if state.telemetry_age_s > self.max_telemetry_age_s:
            return self.reject_observation("stale_telemetry_fail_safe")
        if self._last_observation_time is not None and observed <= self._last_observation_time:
            return self.reject_observation("nonadvancing_telemetry_fail_safe")
        self._last_observation_time = observed
        if self.performance_predictor is None:
            return self.reject_observation("performance_predictor_missing")
        reference = getattr(state, "arrival_history_ref", None)
        if reference is not None and reference.status == "incomplete":
            # Strict live gap policy: a missing/unavailable second in the current UTC day is
            # never filled, so the frozen UTC-day-prefix EWMA cannot be formed.
            return self.reject_observation(
                "workload_prefix_incomplete_fail_safe", stage="forecast",
                error=(f"UTC day {reference.utc_day}: {reference.missing_seconds} missing and "
                       f"{reference.unavailable_seconds} unavailable seconds after {reference.complete_prefix_bins} "
                       f"complete bins; not interpolated, forward-filled or zero-filled ({reference.gap_policy})"))
        if not state.arrival_history_tps:
            return self.reject_observation("arrival_history_missing")
        arrival = None
        phase = "forecast"
        forecast_metadata = {}
        scored = []
        prediction_metadata = {}
        pending_config = None
        try:
            if getattr(self.arrival_forecaster, "requires_timestamped_history", False) and not state.arrival_history_timestamps_utc:
                raise ValueError("forecaster requires timestamped history")
            _history_times(state, self.max_telemetry_age_s)
            if state.arrival_history_timestamps_utc:
                history_time = datetime.fromisoformat(state.arrival_history_timestamps_utc[-1].replace("Z", "+00:00"))
                if self._last_history_time is not None and history_time <= self._last_history_time:
                    raise ValueError("arrival history did not advance; repeated history cannot advance cooldown")
                self._last_history_time = history_time
            forecast_result = None
            timestamped_forecast = bool(state.arrival_history_timestamps_utc and hasattr(self.arrival_forecaster, "predict_timestamped"))
            if timestamped_forecast:
                reference_kwargs = ({"history_ref": reference}
                                    if reference is not None and getattr(self.arrival_forecaster, "accepts_history_reference", False)
                                    else {})
                forecast_result = self.arrival_forecaster.predict_timestamped(
                    state.arrival_history_timestamps_utc,
                    state.arrival_history_tps,
                    self.horizon_seconds,
                    now_utc=state.timestamp_utc,
                    max_history_age_s=self.max_telemetry_age_s,
                    **reference_kwargs,
                )
                raw_arrival = forecast_result.value_tps
            else:
                raw_arrival = self.arrival_forecaster.predict(state.arrival_history_tps, self.horizon_seconds)
            arrival = _finite_number("forecast TPS", raw_arrival, minimum=0)
            forecast_result = forecast_result or getattr(self.arrival_forecaster, "last_result", None)
            if forecast_result is not None:
                if forecast_result.horizon_seconds != self.horizon_seconds:
                    raise ValueError("forecast horizon does not match controller horizon")
                if _finite_number("forecast result TPS", forecast_result.value_tps, minimum=0) != arrival:
                    raise ValueError("forecast value disagrees with returned forecast metadata")
                if getattr(forecast_result, "value_unit", None) != "transactions/s":
                    raise ValueError("forecast result must explicitly use transactions/s")
                if timestamped_forecast and forecast_result.origin_timestamp_utc is None:
                    raise ValueError("timestamped forecast result lacks origin")
                if forecast_result.origin_timestamp_utc is not None:
                    origin = datetime.fromisoformat(forecast_result.origin_timestamp_utc.replace("Z", "+00:00"))
                    now = datetime.fromisoformat(state.timestamp_utc.replace("Z", "+00:00"))
                    if origin.tzinfo is None or now.tzinfo is None:
                        raise ValueError("forecast origin must include timezone")
                    age = (now - origin).total_seconds()
                    if age < 0 or age > self.max_telemetry_age_s:
                        raise ValueError("forecast result is stale or in the future")
                    if state.arrival_history_timestamps_utc and origin < history_time:
                        raise ValueError("forecast origin precedes its supplied history")
                    if getattr(self.arrival_forecaster, "requires_timestamped_history", False):
                        frequency = forecast_result.frequency_seconds
                        _samples("forecast frequency_seconds", frequency, 1)
                        if origin != history_time + timedelta(seconds=frequency):
                            raise ValueError("forecast origin does not equal the complete-prefix boundary")
                    if self._last_forecast_time is not None and origin <= self._last_forecast_time:
                        raise ValueError("forecast origin did not advance")
                    last_input = getattr(forecast_result, "history_last_timestamp_utc", None)
                    if last_input is not None and state.arrival_history_timestamps_utc and datetime.fromisoformat(last_input.replace("Z", "+00:00")) != history_time:
                        raise ValueError("forecast metadata refers to a different history prefix")
                    self._last_forecast_time = origin
            forecast_metadata = forecast_result.as_dict() if hasattr(forecast_result, "as_dict") else {}
            phase = "performance_model"
            for config in self.candidate_configs:
                pending_config = config
                prediction = self._coerce_prediction(self.performance_predictor.predict(arrival, config))
                prediction_metadata[config] = dict(getattr(self.performance_predictor, "last_prediction_metadata", {}))
                utility = self._utility(arrival, prediction)
                if not math.isfinite(utility):
                    raise ValueError("non-finite candidate utility")
                scored.append((utility, config, prediction))
            scored.sort(key=lambda row: (-row[0], row[1].max_message_count, row[1].batch_timeout_ms))
            best_utility, best_config, best_prediction = scored[0]
            current = next((row for row in scored if row[1] == state.current_config), None)
            if current is None:
                pending_config = state.current_config
                current_prediction = self._coerce_prediction(self.performance_predictor.predict(arrival, state.current_config))
                prediction_metadata[state.current_config] = dict(getattr(self.performance_predictor, "last_prediction_metadata", {}))
                current = (self._utility(arrival, current_prediction), state.current_config, current_prediction)
            if not math.isfinite(current[0]):
                raise ValueError("non-finite current utility")
            improvement = best_utility - current[0]
            if not math.isfinite(improvement):
                raise ValueError("non-finite utility improvement")
            self._since_change += 1
            if best_config != state.current_config and improvement > self.minimum_utility_improvement and self._since_change >= self.cooldown_samples:
                reason = "predicted_utility_improvement"
                selected = best_config
                chosen_prediction = best_prediction
                changed = True
            else:
                reason = "prediction_hold"
                selected = state.current_config
                chosen_prediction = current[2]
                changed = False
            hold_cause = ("current_is_best" if best_config == state.current_config else
                          "insufficient_margin" if improvement <= self.minimum_utility_improvement else "cooldown")
            return ControllerDecision(
                "predictive", selected, changed, reason,
                predicted_arrival_rate_tps=arrival,
                predicted_performance=chosen_prediction,
                metadata={
                    **self.policy_metadata(),
                    "decision_kind": "select" if changed else "hold",
                    "forecast": forecast_metadata,
                    "performance_predictor": prediction_metadata[selected],
                    "candidate_utilities": [
                        {"config": config.as_dict(), "utility": utility, "throughput_tps": prediction.throughput_tps,
                         "latency_ms": prediction.latency_ms, "prediction_metadata": prediction_metadata[config]}
                        for utility, config, prediction in scored
                    ],
                    "current_utility": current[0],
                    "selected_utility": best_utility if changed else current[0],
                    "utility_improvement": improvement,
                    "minimum_utility_improvement": self.minimum_utility_improvement,
                    "hold_cause": None if changed else hold_cause,
                    "cooldown_remaining_samples": max(0, self.cooldown_samples - self._since_change),
                },
            )
        except Exception as exc:
            decision = self.reject_observation("prediction_failure_fail_safe", error=str(exc), stage=phase,
                                               arrival=arrival, forecast=forecast_metadata)
            decision.metadata["error_type"] = type(exc).__name__
            decision.metadata["candidate_evaluations"] = [
                {"config": config.as_dict(), "utility": utility, "throughput_tps": prediction.throughput_tps,
                 "latency_ms": prediction.latency_ms, "prediction_metadata": prediction_metadata.get(config, {})}
                for utility, config, prediction in scored]
            decision.metadata["failed_candidate"] = None if pending_config is None else pending_config.as_dict()
            return decision
