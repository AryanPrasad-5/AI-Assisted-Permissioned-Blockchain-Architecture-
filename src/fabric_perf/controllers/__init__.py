"""Controller strategies and the common experiment contract."""

from fabric_perf.controllers.base import (
    BaseController,
    ControllerDecision,
    ControllerState,
    FabricConfig,
    PerformancePrediction,
    StaticController,
)
from fabric_perf.controllers.strategies import PredictiveController, ReactiveController
from fabric_perf.controllers.harness import (
    ApplyResult,
    CallableActuator,
    ControllerExperimentHarness,
    RecordingActuator,
    ShadowActuator,
    VerifiedCallableActuator,
)
from fabric_perf.controllers.telemetry import (
    ContinuousCaliperLedgerAdapter,
    FabricTelemetrySnapshot,
    MetricObservation,
    PrometheusTelemetryAdapter,
    TelemetryDataError,
    TelemetryTimestampError,
    caliper_snapshot_from_summary,
)
from fabric_perf.controllers.telemetry_errors import TelemetryReadFailure
from fabric_perf.controllers.telemetry_freshness import FreshnessPolicy, FreshnessStatus
from fabric_perf.controllers.metric_baseline import BaselineState, DeltaStatus
from fabric_perf.controllers.config_evidence import ConfigEvidence, ConfigEvidenceError
from fabric_perf.controllers.live_integration import (
    AdaptationLatencyEvidence,
    IntegratedControllerRunner,
    IndependentConfigurationVerifier,
    IntegrationSafetyError,
    LatencyEvidence,
    ProductionTelemetryProvider,
    StopPolicy,
    TelemetryFrame,
    TelemetryQualityError,
    TimestampContract,
    ledger_observation_from_blocks_csv,
    load_direct_cut_events,
    load_jsonl_frames,
)

__all__ = [
    "ApplyResult",
    "BaseController",
    "CallableActuator",
    "ControllerDecision",
    "ControllerExperimentHarness",
    "ControllerState",
    "FabricConfig",
    "ApprovedMaxMessageCountSpace",
    "FlashbotsSeriesForecaster",
    "ForecastResult",
    "HFBTPArtifactPerformancePredictor",
    "PerformancePrediction",
    "PredictiveController",
    "ReactiveController",
    "RecordingActuator",
    "ShadowActuator",
    "VerifiedCallableActuator",
    "RollingMeanArrivalForecaster",
    "StaticController",
    "FabricTelemetrySnapshot",
    "ContinuousCaliperLedgerAdapter",
    "MetricObservation",
    "PrometheusTelemetryAdapter",
    "TelemetryDataError",
    "TelemetryTimestampError",
    "TelemetryReadFailure",
    "FreshnessPolicy",
    "FreshnessStatus",
    "BaselineState",
    "DeltaStatus",
    "ConfigEvidence",
    "ConfigEvidenceError",
    "TelemetryQualityError",
    "caliper_snapshot_from_summary",
    "AdaptationLatencyEvidence",
    "IntegratedControllerRunner",
    "IndependentConfigurationVerifier",
    "IntegrationSafetyError",
    "LatencyEvidence",
    "ProductionTelemetryProvider",
    "StopPolicy",
    "TelemetryFrame",
    "TimestampContract",
    "ledger_observation_from_blocks_csv",
    "load_direct_cut_events",
    "load_jsonl_frames",
]


def __getattr__(name):
    """Load pandas-backed model adapters only when explicitly requested.

    Fabric CLI utilities run under the WSL system Python and intentionally do
    not require the project science stack. The lazy boundary keeps the
    standard-library actuator importable there.
    """
    if name in {"ApprovedMaxMessageCountSpace", "FlashbotsSeriesForecaster", "ForecastResult",
                "HFBTPArtifactPerformancePredictor", "RollingMeanArrivalForecaster"}:
        from fabric_perf.controllers.adapters import (
            ApprovedMaxMessageCountSpace,
            FlashbotsSeriesForecaster,
            ForecastResult,
            HFBTPArtifactPerformancePredictor,
            RollingMeanArrivalForecaster,
        )
        return {
            "ApprovedMaxMessageCountSpace": ApprovedMaxMessageCountSpace,
            "FlashbotsSeriesForecaster": FlashbotsSeriesForecaster,
            "ForecastResult": ForecastResult,
            "HFBTPArtifactPerformancePredictor": HFBTPArtifactPerformancePredictor,
            "RollingMeanArrivalForecaster": RollingMeanArrivalForecaster,
        }[name]
    raise AttributeError(name)
