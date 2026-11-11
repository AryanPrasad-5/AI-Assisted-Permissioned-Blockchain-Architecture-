"""Reactive and predictive controller implementations."""

from __future__ import annotations

import math
from dataclasses import replace
from typing import Any, Iterable, Mapping, Protocol

from .base import (
    BaseController,
    ControllerDecision,
    ControllerState,
    FabricConfig,
    PerformancePrediction,
    coerce_config,
)


class ArrivalForecaster(Protocol):
    def predict(self, history_tps: Iterable[float], horizon_seconds: int) -> float: ...


class PerformancePredictor(Protocol):
    def predict(self, arrival_rate_tps: float, config: FabricConfig) -> PerformancePrediction: ...


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
        self.latency_high_ms = float(high)
        self.latency_low_ms = float(latency_low_ms if latency_low_ms is not None else high * 0.8)
        if self.latency_low_ms >= self.latency_high_ms:
            raise ValueError("latency_low_ms must be below latency_high_ms")
        self.queue_high = None if queue_high is None else float(queue_high)
        self.queue_low = None if queue_low is None else float(queue_low)
        if self.queue_high is not None and (self.queue_low is None or self.queue_low >= self.queue_high):
            raise ValueError("queue_low must be below queue_high when queue thresholds are used")
        if consecutive_samples < 1 or cooldown_samples < 0:
            raise ValueError("consecutive_samples must be positive and cooldown_samples non-negative")
        self.consecutive_samples = consecutive_samples
        self.cooldown_samples = cooldown_samples
        if max_telemetry_age_s <= 0:
            raise ValueError("max_telemetry_age_s must be positive")
        self.max_telemetry_age_s = float(max_telemetry_age_s)
        self._active = False
        self._congestion_streak = 0
        self._recovery_streak = 0
        self._since_change = cooldown_samples

    def _is_congested(self, state: ControllerState) -> bool:
        latency_hit = state.observed_latency_ms is not None and state.observed_latency_ms >= self.latency_high_ms
        queue_hit = self.queue_high is not None and state.queue_depth is not None and state.queue_depth >= self.queue_high
        return bool(latency_hit or queue_hit)

    def _is_recovered(self, state: ControllerState) -> bool:
        latency_ok = state.observed_latency_ms is not None and state.observed_latency_ms <= self.latency_low_ms
        queue_ok = self.queue_low is None or (state.queue_depth is not None and state.queue_depth <= self.queue_low)
        return bool(latency_ok and queue_ok)

    def decide(self, state: ControllerState) -> ControllerDecision:
        if state.telemetry_age_s is not None and state.telemetry_age_s > self.max_telemetry_age_s:
            return ControllerDecision(
                controller="reactive", config=state.current_config, changed=False,
                reason="stale_telemetry_fail_safe",
                metadata={"telemetry_age_s": state.telemetry_age_s,
                          "max_telemetry_age_s": self.max_telemetry_age_s},
            )
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
                self._active = True
                self._since_change = 0
                reason = "congestion_threshold_crossed"
            else:
                reason = "congestion_detected_cooldown"
        elif self._active and self._recovery_streak >= self.consecutive_samples:
            if self._since_change >= self.cooldown_samples:
                desired = self.default_config
                self._active = False
                self._since_change = 0
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
        if horizon_seconds < 1 or cooldown_samples < 0:
            raise ValueError("horizon_seconds must be positive and cooldown_samples non-negative")
        if latency_penalty_per_ms < 0 or backlog_penalty_per_tps < 0 or minimum_utility_improvement < 0:
            raise ValueError("predictive utility penalties and improvement must be non-negative")
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
        if max_telemetry_age_s <= 0:
            raise ValueError("max_telemetry_age_s must be positive")
        self.max_telemetry_age_s = float(max_telemetry_age_s)
        self._since_change = cooldown_samples

    def _coerce_prediction(self, value: Any) -> PerformancePrediction:
        if isinstance(value, PerformancePrediction):
            return value
        if isinstance(value, Mapping):
            latency = value.get("latency_ms", value.get("Avg Latency"))
            throughput = value.get("throughput_tps", value.get("Throughput"))
            if latency is None or throughput is None:
                raise ValueError("performance prediction mapping lacks throughput/latency")
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
        self._since_change += 1
        if state.telemetry_age_s is not None and state.telemetry_age_s > self.max_telemetry_age_s:
            return ControllerDecision(
                "predictive", state.current_config, False, "stale_telemetry_fail_safe",
                metadata={"telemetry_age_s": state.telemetry_age_s,
                          "max_telemetry_age_s": self.max_telemetry_age_s},
            )
        if self.performance_predictor is None:
            return ControllerDecision("predictive", state.current_config, False, "performance_predictor_missing")
        if not state.arrival_history_tps:
            return ControllerDecision("predictive", state.current_config, False, "arrival_history_missing")
        try:
            if state.arrival_history_timestamps_utc and hasattr(self.arrival_forecaster, "predict_timestamped"):
                forecast_result = self.arrival_forecaster.predict_timestamped(
                    state.arrival_history_timestamps_utc,
                    state.arrival_history_tps,
                    self.horizon_seconds,
                    now_utc=state.timestamp_utc,
                    max_history_age_s=self.max_telemetry_age_s,
                )
                arrival = float(forecast_result.value_tps)
            else:
                arrival = float(self.arrival_forecaster.predict(state.arrival_history_tps, self.horizon_seconds))
            if not math.isfinite(arrival) or arrival < 0:
                raise ValueError(f"invalid predicted arrival rate {arrival!r}")
            forecast_result = getattr(self.arrival_forecaster, "last_result", None)
            forecast_metadata = forecast_result.as_dict() if hasattr(forecast_result, "as_dict") else {}
            scored = []
            for config in self.candidate_configs:
                prediction = self._coerce_prediction(self.performance_predictor.predict(arrival, config))
                scored.append((self._utility(arrival, prediction), config, prediction))
            scored.sort(key=lambda row: (-row[0], row[1].max_message_count, row[1].batch_timeout_ms))
            best_utility, best_config, best_prediction = scored[0]
            current = next((row for row in scored if row[1] == state.current_config), None)
            if current is None:
                current_prediction = self._coerce_prediction(self.performance_predictor.predict(arrival, state.current_config))
                current = (self._utility(arrival, current_prediction), state.current_config, current_prediction)
            improvement = best_utility - current[0]
            if best_config != state.current_config and improvement > self.minimum_utility_improvement and self._since_change >= self.cooldown_samples:
                self._since_change = 0
                reason = "predicted_utility_improvement"
                selected = best_config
                chosen_prediction = best_prediction
                changed = True
            else:
                reason = "prediction_hold"
                selected = state.current_config
                chosen_prediction = current[2]
                changed = False
            return ControllerDecision(
                "predictive", selected, changed, reason,
                predicted_arrival_rate_tps=arrival,
                predicted_performance=chosen_prediction,
                metadata={
                    "forecast": forecast_metadata,
                    "performance_predictor": getattr(self.performance_predictor, "last_prediction_metadata", {}),
                    "candidate_utilities": [
                        {"config": config.as_dict(), "utility": utility, "throughput_tps": prediction.throughput_tps, "latency_ms": prediction.latency_ms}
                        for utility, config, prediction in scored
                    ],
                    "current_utility": current[0],
                    "selected_utility": best_utility if changed else current[0],
                    "utility_improvement": improvement,
                    "cooldown_remaining_samples": max(0, self.cooldown_samples - self._since_change),
                },
            )
        except Exception as exc:
            return ControllerDecision(
                "predictive", state.current_config, False, "prediction_failure_fail_safe",
                metadata={"error_type": type(exc).__name__, "error": str(exc)},
            )
