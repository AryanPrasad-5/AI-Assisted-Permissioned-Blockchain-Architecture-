"""Shared contracts for the three Fabric controller strategies.

The controller layer is deliberately independent of Docker, Caliper and Fabric
CLI execution. A controller decides; an experiment harness and an injected
actuator perform and measure any configuration change. This keeps unit tests
deterministic and prevents a model test from changing a live channel.
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping


UINT32_MAX = 2**32 - 1
MIN_TIMEOUT_MS, MAX_TIMEOUT_MS = 1, 60_000


def _finite_number(name: str, value: Any, *, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric, got {value!r}")
    number = float(value)
    if not math.isfinite(number) or (minimum is not None and number < minimum):
        raise ValueError(f"{name} must be finite and >= {minimum}, got {value!r}")
    return number


@dataclass(frozen=True)
class FabricConfig:
    """Validated channel batch configuration used by controller decisions.

    ``max_message_count`` is Fabric ``BatchSize.MaxMessageCount`` (B),
    ``batch_timeout_ms`` is ``BatchTimeout`` (tau), and
    ``preferred_max_bytes`` is ``BatchSize.PreferredMaxBytes`` (P). The
    controller experiment does not change the absolute byte cap or orderer
    count, but keeps them in the immutable configuration record for provenance.
    """

    max_message_count: int = 45
    batch_timeout_ms: int = 2_000
    preferred_max_bytes: int = 10_485_760
    absolute_max_bytes: int | None = 103_809_024
    orderers: int = 3

    def __post_init__(self) -> None:
        for name, value in (
            ("max_message_count", self.max_message_count),
            ("preferred_max_bytes", self.preferred_max_bytes),
            ("orderers", self.orderers),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} must be an integer")
        if not 1 <= self.max_message_count <= UINT32_MAX:
            raise ValueError("max_message_count is outside Fabric's uint32 range")
        if not MIN_TIMEOUT_MS <= self.batch_timeout_ms <= MAX_TIMEOUT_MS:
            raise ValueError("batch_timeout_ms must be between 1 and 60000")
        if not 1 <= self.preferred_max_bytes <= UINT32_MAX:
            raise ValueError("preferred_max_bytes is outside Fabric's uint32 range")
        if self.absolute_max_bytes is not None:
            if not 1 <= self.absolute_max_bytes <= UINT32_MAX:
                raise ValueError("absolute_max_bytes is outside Fabric's uint32 range")
            if self.preferred_max_bytes > self.absolute_max_bytes:
                raise ValueError("preferred_max_bytes cannot exceed absolute_max_bytes")
        if self.orderers < 1:
            raise ValueError("orderers must be positive")

    @property
    def batch_timeout_s(self) -> float:
        return self.batch_timeout_ms / 1000.0

    @property
    def batch_timeout(self) -> str:
        return f"{self.batch_timeout_ms // 1000}s" if self.batch_timeout_ms % 1000 == 0 else f"{self.batch_timeout_ms}ms"

    def as_dict(self) -> dict[str, Any]:
        return {
            "max_message_count": self.max_message_count,
            "batch_timeout_ms": self.batch_timeout_ms,
            "batch_timeout": self.batch_timeout,
            "preferred_max_bytes": self.preferred_max_bytes,
            "absolute_max_bytes": self.absolute_max_bytes,
            "orderers": self.orderers,
        }


def coerce_config(value: FabricConfig | Mapping[str, Any]) -> FabricConfig:
    """Normalize canonical and legacy controller configuration mappings."""
    if isinstance(value, FabricConfig):
        return value
    if not isinstance(value, Mapping):
        raise TypeError(f"configuration must be a mapping or FabricConfig, got {type(value).__name__}")
    b = value.get("max_message_count", value.get("batch_size", value.get("B", 45)))
    timeout = value.get("batch_timeout_ms", value.get("batch_timeout", value.get("tau", 2_000)))
    if isinstance(timeout, str):
        text = timeout.strip()
        if text.endswith("ms"):
            timeout = float(text[:-2])
        elif text.endswith("s"):
            timeout = float(text[:-1]) * 1000
        else:
            raise ValueError(f"unsupported batch timeout {timeout!r}")
    if isinstance(timeout, float) and not timeout.is_integer():
        raise ValueError("batch timeout must resolve to whole milliseconds")
    return FabricConfig(
        max_message_count=int(b),
        batch_timeout_ms=int(timeout),
        preferred_max_bytes=int(value.get("preferred_max_bytes", value.get("P", 10_485_760))),
        absolute_max_bytes=value.get("absolute_max_bytes", value.get("A", 103_809_024)),
        orderers=int(value.get("orderers", 3)),
    )


@dataclass(frozen=True)
class PerformancePrediction:
    """Performance prediction at one candidate configuration."""

    throughput_tps: float
    latency_ms: float

    def __post_init__(self) -> None:
        _finite_number("throughput_tps", self.throughput_tps, minimum=0.0)
        _finite_number("latency_ms", self.latency_ms, minimum=0.0)


@dataclass
class ControllerState:
    """Decision-time observations and strictly pre-decision predictions."""

    timestamp_utc: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    current_config: FabricConfig = field(default_factory=FabricConfig)
    observed_arrival_rate_tps: float | None = None
    observed_throughput_tps: float | None = None
    observed_latency_ms: float | None = None
    observed_latency_p95_ms: float | None = None
    queue_depth: float | None = None
    failed_transactions: int = 0
    arrival_history_tps: tuple[float, ...] = ()
    arrival_history_timestamps_utc: tuple[str, ...] = ()
    telemetry_age_s: float | None = None

    def __post_init__(self) -> None:
        self.current_config = coerce_config(self.current_config)
        for name in (
            "observed_arrival_rate_tps",
            "observed_throughput_tps",
            "observed_latency_ms",
            "observed_latency_p95_ms",
            "queue_depth",
            "telemetry_age_s",
        ):
            value = getattr(self, name)
            if value is not None:
                _finite_number(name, value, minimum=0.0)
        if isinstance(self.failed_transactions, bool) or self.failed_transactions < 0:
            raise ValueError("failed_transactions must be a non-negative integer")
        self.arrival_history_tps = tuple(
            _finite_number("arrival_history_tps", item, minimum=0.0)
            for item in self.arrival_history_tps
        )
        self.arrival_history_timestamps_utc = tuple(str(item) for item in self.arrival_history_timestamps_utc)
        if self.arrival_history_timestamps_utc and len(self.arrival_history_timestamps_utc) != len(self.arrival_history_tps):
            raise ValueError("arrival_history_timestamps_utc must match arrival_history_tps length")

    @classmethod
    def from_mapping(cls, state: Mapping[str, Any]) -> "ControllerState":
        """Accept the legacy dictionary shape while keeping one canonical state."""
        return cls(
            timestamp_utc=str(state.get("timestamp_utc", datetime.now(timezone.utc).isoformat())),
            current_config=coerce_config(state.get("current_config", state)),
            observed_arrival_rate_tps=state.get("observed_arrival_rate_tps", state.get("arrival_rate")),
            observed_throughput_tps=state.get("observed_throughput_tps", state.get("tps")),
            observed_latency_ms=state.get("observed_latency_ms", state.get("current_latency_ms", state.get("current_latency"))),
            observed_latency_p95_ms=state.get("observed_latency_p95_ms"),
            queue_depth=state.get("queue_depth", state.get("backlog")),
            failed_transactions=int(state.get("failed_transactions", 0)),
            arrival_history_tps=tuple(state.get("arrival_history_tps", state.get("arrival_history", ()))),
            arrival_history_timestamps_utc=tuple(state.get("arrival_history_timestamps_utc", ())),
            telemetry_age_s=state.get("telemetry_age_s"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "timestamp_utc": self.timestamp_utc,
            "current_config": self.current_config.as_dict(),
            "observed_arrival_rate_tps": self.observed_arrival_rate_tps,
            "observed_throughput_tps": self.observed_throughput_tps,
            "observed_latency_ms": self.observed_latency_ms,
            "observed_latency_p95_ms": self.observed_latency_p95_ms,
            "queue_depth": self.queue_depth,
            "failed_transactions": self.failed_transactions,
            "arrival_history_tps": list(self.arrival_history_tps),
            "arrival_history_timestamps_utc": list(self.arrival_history_timestamps_utc),
            "telemetry_age_s": self.telemetry_age_s,
        }


@dataclass
class ControllerDecision:
    controller: str
    config: FabricConfig
    changed: bool
    reason: str
    predicted_arrival_rate_tps: float | None = None
    predicted_performance: PerformancePrediction | None = None
    decision_overhead_ms: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "controller": self.controller,
            "config": self.config.as_dict(),
            "changed": self.changed,
            "reason": self.reason,
            "predicted_arrival_rate_tps": self.predicted_arrival_rate_tps,
            "predicted_performance": None if self.predicted_performance is None else {
                "throughput_tps": self.predicted_performance.throughput_tps,
                "latency_ms": self.predicted_performance.latency_ms,
            },
            "decision_overhead_ms": self.decision_overhead_ms,
            "metadata": self.metadata,
        }


class BaseController(ABC):
    """Common decision interface for static, reactive and predictive control."""

    @abstractmethod
    def decide(self, state: ControllerState) -> ControllerDecision:
        """Return a decision without performing external side effects."""

    def select_configuration(self, current_state: Mapping[str, Any]) -> dict[str, Any]:
        """Backward-compatible configuration-only view of :meth:`decide`."""
        state = current_state if isinstance(current_state, ControllerState) else ControllerState.from_mapping(current_state)
        return self.decide(state).config.as_dict()


class StaticController(BaseController):
    """A fixed configuration baseline; never requests mid-run adaptation."""

    def __init__(self, fixed_config: FabricConfig | Mapping[str, Any]):
        self.fixed_config = dict(fixed_config) if isinstance(fixed_config, Mapping) else fixed_config.as_dict()
        self.config = coerce_config(fixed_config)

    def decide(self, state: ControllerState) -> ControllerDecision:
        return ControllerDecision(
            controller="static",
            config=self.config,
            changed=False,
            reason="fixed_configuration",
            metadata={"initial_config_must_match": state.current_config == self.config},
        )

    def select_configuration(self, current_state: Mapping[str, Any]) -> dict[str, Any]:
        # Preserve the original skeleton's legacy behavior for existing callers.
        return dict(self.fixed_config)
