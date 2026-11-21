"""Predictive-input contracts and guarded adapters for the Stage 3 shadow runner (task A0g, 2026-11-21).

The predictive controller combines two different operations that this module keeps apart:

* a WORKLOAD FORECAST: the frozen EWMA (alpha 0.005, H=60 s, 1 s bins, complete UTC-day prefix) predicts the mean arrival
  rate over the next 60 s. Interface: ``live_foundation.StreamingEWMA.predict_timestamped`` over a ``StreamingWorkload``
  history store.
* a PERFORMANCE PREDICTION: the frozen HFBTP random forests estimate throughput and average latency of a candidate
  configuration at that forecast arrival rate. Interface: ``adapters.HFBTPArtifactPerformancePredictor.predict``.

This module adds NO model, NO forecaster and NO controller algorithm. It adds:

* ``FORECAST_CONTRACT`` / ``PERFORMANCE_CONTRACT``: what the existing interfaces require, written down once, with a
  ``FEATURE_SCHEMA_ID`` that identifies the exact feature schema;
* ``GuardedForecaster`` / ``GuardedPerformancePredictor``: wrappers that refuse incompatible or insufficient input and
  invalid output instead of repairing it. Nothing is clipped, interpolated, defaulted or substituted. Extrapolation outside
  the HFBTP training grid raises ``UnsupportedExtrapolationError`` so a record can say so;
* ``PredictiveComponents``: the one object through which the runner receives its forecaster and performance model
  (dependency injection). ``fixture_components`` labels scripted stand-ins as fixture data; ``build_real_predictive_components``
  is the prepared, DISABLED route to the real interfaces.

COMPUTATION BOUNDARY. ``REAL_INFERENCE_AUTHORIZED`` is a module constant, ``False``. It is not a flag, parameter, environment
variable or file: this module reads none of them. Turning real inference on is a deliberate, reviewable source change that
alters this file's hash (and so a candidate manifest). Until then ``build_real_predictive_components`` raises before it reads
a pin, a model file or a history store. Nothing in this module loads a model.

A0h (2026-11-21) adds, still fixture-only: ``TimestampedFixtureForecaster`` (a scripted estimator with the real ``predict_timestamped`` signature),
``RangeCheckedFixtureModel`` and ``captured_history_components``, so a CAPTURED, timestamped workload history (``captured_history``) passes through
the real ``GuardedForecaster`` and the real workload-range check (``check_arrival_rate``, factored out of ``GuardedPerformancePredictor.predict``
unchanged). The values stay scripted; ``real_inference_performed`` stays False.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from fabric_perf.models.baseline import FEATURE_COLUMNS, TARGETS

from .adapters import ApprovedMaxMessageCountSpace, ForecastResult
from .base import FabricConfig, PerformancePrediction, coerce_config

PREDICTIVE_MODE_FIXTURE = "fixture"
PREDICTIVE_MODE_REAL = "real"
PREDICTIVE_MODES = (PREDICTIVE_MODE_FIXTURE, PREDICTIVE_MODE_REAL)

# The single switch for frozen-model inference. Deliberately a constant in source: no flag, argument or environment variable
# reaches it, and tests assert that nothing in this package reads one. The A0g task does not authorize inference.
REAL_INFERENCE_AUTHORIZED = False

REAL_FORECAST_IDENTITY = "ewma-alpha-0.005-h60-utc-day-prefix"          # StreamingEWMA's ForecastResult.model_identity
REAL_PERFORMANCE_IDENTITY = "HFBTP:frozen-artifact-v1:random_forest"    # HFBTPArtifactPerformancePredictor metadata identity
HFBTP_DATASET_SHA256 = "CE295D6451787FEFCFE2101579238CDA4BEDE8539BC302E6A482AA4E5782BD5E"

OOD_WITHIN_GRID = "within_training_grid"
OOD_OUTSIDE_GRID = "outside_training_grid"
OOD_NOT_EVALUATED = "not_evaluated"
OOD_NOT_APPLICABLE_FIXTURE = "not_applicable_fixture_model"


class PredictiveIntegrationError(ValueError):
    """Input or output of a predictive component is incompatible or insufficient. ``code`` is machine-readable."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


class UnsupportedExtrapolationError(PredictiveIntegrationError):
    """The request lies outside the HFBTP training grid. It is refused, never clipped, so the record shows the gap."""


class RealInferenceNotAuthorized(PredictiveIntegrationError):
    """Real frozen-model inference is not authorized. Raised before any pin, model file or history is read."""


# ----------------------------------------------------------------------------------------------------------- contracts
FORECAST_CONTRACT: Mapping[str, Any] = {
    "interface": "fabric_perf.controllers.live_foundation.StreamingEWMA.predict_timestamped",
    "history_store": "fabric_perf.controllers.live_foundation.StreamingWorkload (shared append-only store)",
    "target": "mean arrival rate over [origin, origin + horizon)",
    "unit": "transactions/s",
    "horizon_seconds": 60,
    "frequency_seconds": 1,
    "min_history_bins": 600,
    "alpha": 0.005,
    "history": "complete current UTC-day prefix, whole-second contiguous bins from 00:00:00 UTC; "
               "a missing second is never zero-filled, interpolated or forward-filled",
    "origin": "end of the last complete bin (last bin start + 1 s); origin <= decision time",
    "model_identity": REAL_FORECAST_IDENTITY,
}

PERFORMANCE_CONTRACT: Mapping[str, Any] = {
    "interface": "fabric_perf.controllers.adapters.HFBTPArtifactPerformancePredictor.predict",
    "raw_inputs": {"Set Transaction Arrival Rate": "tx/s", "Block Size": "HFBTP dataset block-size units (operator mapping "
                   "from MaxMessageCount)", "Orderers": "count"},
    "feature_order": list(FEATURE_COLUMNS),
    "targets": list(TARGETS),
    "target_units": {"Throughput": "tx/s", "Avg Latency": "s (the adapter converts to ms for the controller)"},
    "domain": {"arrival_rate_tps": [10.0, 200.0], "block_size": [10, 800], "block_size_step": 5, "orderers": [3, 5, 7, 9],
               "absent_grid_cells_block_orderers": [[245, 9], [525, 9], [725, 5]]},
    "model_identity": REAL_PERFORMANCE_IDENTITY,
    "artifact": {"dataset_sha256": HFBTP_DATASET_SHA256, "model_name": "random_forest", "rows": 24687, "artifact_version": 1},
    "construct": "HFBTP finite-round response surface; strong within-grid accuracy does not establish extrapolation or local "
                 "Fabric capacity",
}


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


FEATURE_SCHEMA_ID = hashlib.sha256(_canonical({"feature_order": PERFORMANCE_CONTRACT["feature_order"],
                                               "targets": PERFORMANCE_CONTRACT["targets"],
                                               "raw_inputs": PERFORMANCE_CONTRACT["raw_inputs"],
                                               "domain": PERFORMANCE_CONTRACT["domain"]}).encode()).hexdigest()


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _finite(value: Any) -> bool:
    return _is_number(value) and math.isfinite(value)


def _parse_utc(value: Any, field_name: str) -> datetime:
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise PredictiveIntegrationError("timestamp_invalid", f"{field_name} is not an ISO-8601 timestamp: {value!r}") from exc
    if moment.tzinfo is None:
        raise PredictiveIntegrationError("timestamp_invalid", f"{field_name} has no timezone: {value!r}")
    return moment.astimezone(timezone.utc)


def check_arrival_rate(arrival_rate_tps: Any, low: float, high: float) -> None:
    """The one workload-range check (A0h: factored out of ``GuardedPerformancePredictor.predict`` unchanged, so the fixture path and
    the real guard cannot drift apart). Non-finite or negative -> ``performance_input_invalid``; outside [low, high] -> refused as
    ``UnsupportedExtrapolationError`` and never clipped. The bounds are inclusive: ``low`` and ``high`` themselves are inside."""
    if not _finite(arrival_rate_tps) or arrival_rate_tps < 0:
        raise PredictiveIntegrationError("performance_input_invalid", f"arrival rate must be finite and non-negative: {arrival_rate_tps!r}")
    if not low <= arrival_rate_tps <= high:
        raise UnsupportedExtrapolationError(
            "arrival_rate_outside_training_grid",
            f"{arrival_rate_tps} tx/s is outside the HFBTP training range [{low}, {high}] tx/s; "
            "it is refused, not clipped (a level-165+ request would be a flat extrapolation)")


# ----------------------------------------------------------------------------------------------------------- forecaster
class GuardedForecaster:
    """Validate the workload history BEFORE it reaches a forecaster, and the forecast AFTER.

    Wraps any object with ``predict_timestamped(history_timestamps_utc, history_tps, horizon_seconds, *, now_utc,
    max_history_age_s[, history_ref])`` (the interface ``StreamingEWMA`` provides). It changes nothing and fills nothing: a
    problem raises ``PredictiveIntegrationError`` (the controller turns it into a recorded abstention).

    ``expected_identity`` is the identity the result must carry. For the real interface it is ``REAL_FORECAST_IDENTITY``; a
    labelled fixture passes its own label, so a fixture result can never be mistaken for the real forecaster's.
    """

    requires_timestamped_history = True

    def __init__(self, inner: Any, *, expected_identity: str, horizon_seconds: int = FORECAST_CONTRACT["horizon_seconds"],
                 min_history_bins: int = FORECAST_CONTRACT["min_history_bins"],
                 frequency_seconds: int = FORECAST_CONTRACT["frequency_seconds"]):
        method = getattr(inner, "predict_timestamped", None)
        if not callable(method):
            raise PredictiveIntegrationError("forecast_interface_incompatible", "forecaster has no predict_timestamped")
        parameters = inspect.signature(method).parameters
        if not {"now_utc", "max_history_age_s"} <= set(parameters):
            raise PredictiveIntegrationError("forecast_interface_incompatible",
                                             "predict_timestamped must accept now_utc and max_history_age_s")
        self.inner = inner
        self.expected_identity = expected_identity
        self.horizon_seconds, self.min_history_bins, self.frequency_seconds = horizon_seconds, min_history_bins, frequency_seconds
        self.accepts_history_reference = bool(getattr(inner, "accepts_history_reference", False))
        self._accepts_ref = "history_ref" in parameters
        self.source = getattr(inner, "source", None)

    @property
    def last_result(self):
        return getattr(self.inner, "last_result", None)

    def predict(self, history_tps: Iterable[float], horizon_seconds: int):
        raise PredictiveIntegrationError("forecast_interface_incompatible",
                                         "the frozen EWMA needs timestamped complete-day history; an untimestamped call is refused")

    def validate_history(self, timestamps: Iterable[Any], values: Iterable[Any], horizon_seconds: Any, *, now_utc: Any,
                         max_history_age_s: Any) -> None:
        """Every history-side check. Raises ``PredictiveIntegrationError``; returns nothing."""
        if horizon_seconds != self.horizon_seconds or isinstance(horizon_seconds, bool):
            raise PredictiveIntegrationError("forecast_horizon_unavailable",
                                             f"the frozen forecast is available for H={self.horizon_seconds}s only, got {horizon_seconds!r}")
        times, rates = tuple(timestamps), tuple(values)
        if not times or not rates:
            raise PredictiveIntegrationError("forecast_history_missing", "no observed workload history was supplied")
        if len(times) != len(rates):
            raise PredictiveIntegrationError("forecast_history_inconsistent", "timestamps and values differ in length")
        if (isinstance(max_history_age_s, bool) or not _finite(max_history_age_s) or max_history_age_s < self.frequency_seconds):
            raise PredictiveIntegrationError("forecast_history_age_limit_invalid",
                                             f"max_history_age_s must be finite and at least {self.frequency_seconds}")
        parsed = [_parse_utc(t, "history timestamp") for t in times]
        if any(t.microsecond for t in parsed):
            raise PredictiveIntegrationError("forecast_history_timestamps_inconsistent", "bins must start on whole seconds")
        step = timedelta(seconds=self.frequency_seconds)
        if any(right - left != step for left, right in zip(parsed, parsed[1:])):
            raise PredictiveIntegrationError("forecast_history_timestamps_inconsistent",
                                             "history is not ordered, contiguous whole-second bins")
        if any(not _finite(v) or v < 0 for v in rates):
            raise PredictiveIntegrationError("forecast_history_value_invalid",
                                             "arrival counts must be finite, non-negative numbers in tx per 1 s bin (= tx/s)")
        if len(rates) < self.min_history_bins:
            raise PredictiveIntegrationError("forecast_history_insufficient",
                                             f"{len(rates)} bins supplied, {self.min_history_bins} required")
        now = _parse_utc(now_utc, "now_utc")
        origin = parsed[-1] + step
        if origin > now:
            raise PredictiveIntegrationError("forecast_input_from_future",
                                             "the last bin has not completed at the decision time: the history would use "
                                             "observations that were not yet available")
        age = (now - parsed[-1]).total_seconds()
        if age > max_history_age_s:
            raise PredictiveIntegrationError("forecast_history_stale", f"last bin is {age:.3f}s old, limit {max_history_age_s}s")
        if parsed[0].date() != parsed[-1].date() or (parsed[0].hour, parsed[0].minute, parsed[0].second) != (0, 0, 0):
            raise PredictiveIntegrationError("forecast_history_not_utc_day_prefix",
                                             "history must be the complete current UTC-day prefix from 00:00:00")

    def predict_timestamped(self, history_timestamps_utc, history_tps, horizon_seconds, *, now_utc, max_history_age_s=5.0,
                            history_ref=None):
        times, rates = tuple(history_timestamps_utc), tuple(history_tps)
        self.validate_history(times, rates, horizon_seconds, now_utc=now_utc, max_history_age_s=max_history_age_s)
        kwargs: dict[str, Any] = {"now_utc": now_utc, "max_history_age_s": max_history_age_s}
        if history_ref is not None and self._accepts_ref:
            kwargs["history_ref"] = history_ref
        result = self.inner.predict_timestamped(times, rates, horizon_seconds, **kwargs)
        self._validate_result(result, times, horizon_seconds)
        return result

    def _validate_result(self, result: Any, times: tuple, horizon_seconds: int) -> None:
        if not isinstance(result, ForecastResult):
            raise PredictiveIntegrationError("forecast_result_invalid", "forecaster did not return a ForecastResult")
        if not _finite(result.value_tps) or result.value_tps < 0:
            raise PredictiveIntegrationError("forecast_result_invalid", f"forecast value is not finite and non-negative: {result.value_tps!r}")
        if result.value_unit != "transactions/s":
            raise PredictiveIntegrationError("forecast_unit_mismatch", f"forecast unit is {result.value_unit!r}, expected transactions/s")
        if result.horizon_seconds != horizon_seconds or result.frequency_seconds != self.frequency_seconds:
            raise PredictiveIntegrationError("forecast_horizon_mismatch",
                                             f"result H={result.horizon_seconds}s/freq={result.frequency_seconds}s differs from the request")
        if result.model_identity != self.expected_identity:
            raise PredictiveIntegrationError("forecast_model_identity_mismatch",
                                             f"result identity {result.model_identity!r} is not {self.expected_identity!r}")
        last = _parse_utc(times[-1], "history timestamp")
        if result.origin_timestamp_utc is None or _parse_utc(result.origin_timestamp_utc, "forecast origin") != last + timedelta(seconds=self.frequency_seconds):
            raise PredictiveIntegrationError("forecast_origin_inconsistent", "forecast origin is not the end of the last complete bin")
        if result.history_last_timestamp_utc is None or _parse_utc(result.history_last_timestamp_utc, "forecast history end") != last:
            raise PredictiveIntegrationError("forecast_origin_inconsistent", "forecast metadata refers to a different history prefix")


# ------------------------------------------------------------------------------------------------ performance predictor
class GuardedPerformancePredictor:
    """Validate a candidate and the model's answer around an existing performance predictor. Nothing is clipped.

    Wraps any object with ``predict(arrival_rate_tps, config) -> PerformancePrediction`` and a ``last_prediction_metadata``
    mapping (the interface ``HFBTPArtifactPerformancePredictor`` provides). It refuses: a non-finite or negative arrival
    rate; a rate outside the training grid; a candidate that is not approved, off the HFBTP grid, on an absent grid cell or
    outside the block-size range; an output that is not a finite ``PerformancePrediction``; and metadata that does not match
    the feature schema (missing feature, different order, wrong identity, wrong units, different block size).
    """

    def __init__(self, inner: Any, *, space: ApprovedMaxMessageCountSpace, expected_identity: str = REAL_PERFORMANCE_IDENTITY,
                 contract: Mapping[str, Any] = PERFORMANCE_CONTRACT):
        if not callable(getattr(inner, "predict", None)):
            raise PredictiveIntegrationError("performance_interface_incompatible", "predictor has no predict(arrival, config)")
        self.inner, self.space, self.expected_identity, self.contract = inner, space, expected_identity, contract
        self.last_prediction_metadata: dict[str, Any] = {}
        domain = contract["domain"]
        self._arrival_lo, self._arrival_hi = domain["arrival_rate_tps"]
        self._block_lo, self._block_hi = domain["block_size"]
        self._step, self._orderers = domain["block_size_step"], set(domain["orderers"])
        self._absent = {tuple(cell) for cell in domain["absent_grid_cells_block_orderers"]}

    def check_candidate(self, config: FabricConfig) -> int:
        """Return the HFBTP block size for ``config`` or raise. Runs no estimator."""
        try:
            config = coerce_config(config)
            block = self.space.hfbtp_block_size(config)
        except (ValueError, TypeError, KeyError) as exc:
            raise PredictiveIntegrationError("candidate_not_approved", str(exc)) from exc
        if not self._block_lo <= block <= self._block_hi:
            raise UnsupportedExtrapolationError("candidate_block_size_outside_training_grid",
                                                f"HFBTP block size {block} is outside [{self._block_lo}, {self._block_hi}]")
        if block % self._step or config.orderers not in self._orderers:
            raise PredictiveIntegrationError("unsupported_candidate_configuration",
                                             f"block size {block} / orderers {config.orderers} is not on the HFBTP grid")
        if (block, config.orderers) in self._absent:
            raise PredictiveIntegrationError("unsupported_candidate_configuration",
                                             f"({block}, {config.orderers}) is an absent HFBTP grid cell")
        return block

    def predict(self, arrival_rate_tps: float, config: FabricConfig) -> PerformancePrediction:
        check_arrival_rate(arrival_rate_tps, self._arrival_lo, self._arrival_hi)
        block = self.check_candidate(config)
        self.last_prediction_metadata = {}
        output = self.inner.predict(arrival_rate_tps, config)
        if not isinstance(output, PerformancePrediction):
            raise PredictiveIntegrationError("performance_output_invalid", f"predictor returned {type(output).__name__}, not PerformancePrediction")
        if not _finite(output.throughput_tps) or output.throughput_tps < 0 or not _finite(output.latency_ms) or output.latency_ms < 0:
            raise PredictiveIntegrationError("performance_output_invalid", "predicted throughput and latency must be finite and non-negative")
        metadata = dict(getattr(self.inner, "last_prediction_metadata", None) or {})
        self._check_metadata(metadata, block, arrival_rate_tps)
        self.last_prediction_metadata = {**metadata, "guard": {"feature_schema_id": FEATURE_SCHEMA_ID, "ood_status": OOD_WITHIN_GRID,
                                                                 "clipping_applied": False}}
        return output

    def _check_metadata(self, metadata: Mapping[str, Any], block: int, arrival: float) -> None:
        if metadata.get("model_identity") != self.expected_identity:
            raise PredictiveIntegrationError("performance_metadata_mismatch",
                                             f"model identity {metadata.get('model_identity')!r} is not {self.expected_identity!r}")
        order = list(self.contract["feature_order"])
        if list(metadata.get("feature_order") or ()) != order:
            raise PredictiveIntegrationError("performance_schema_mismatch", "feature order differs from the HFBTP artifact schema")
        values = metadata.get("feature_values")
        if not isinstance(values, Mapping) or list(values) != order or any(not _finite(values[k]) for k in order):
            raise PredictiveIntegrationError("performance_schema_mismatch",
                                             "feature values are missing, reordered or non-finite; none is substituted")
        if values["Set Transaction Arrival Rate"] != float(arrival) or values["Block Size"] != float(block):
            raise PredictiveIntegrationError("performance_schema_mismatch", "feature values differ from the requested arrival rate / block size")
        units = metadata.get("target_units") or {}
        if units.get("Throughput") != "tx/s" or units.get("Avg Latency") != "s" or units.get("controller_latency") != "ms":
            raise PredictiveIntegrationError("performance_unit_mismatch", "target units are not Throughput tx/s, Avg Latency s, controller latency ms")
        if metadata.get("hfbtp_block_size") != block:
            raise PredictiveIntegrationError("performance_metadata_mismatch", "model block size differs from the approved mapping")


# ------------------------------------------------------------------------------------------------------- components
@dataclass(frozen=True)
class PredictiveComponents:
    """What the runner needs to build the predictive controller, and how to label it in every record."""

    mode: str
    performance_predictor: Any
    forecaster: Any
    performance_identity: str
    forecast_identity: str
    fixture_data: bool
    real_inference_performed: bool
    provenance: Mapping[str, Any] = field(default_factory=dict)
    history_basis: str = ""          # A0h: names the captured history behind the workload input; empty = the A0g placeholder history

    def label(self) -> dict[str, Any]:
        label = {"mode": self.mode, "fixture_data": self.fixture_data, "real_inference_performed": self.real_inference_performed,
                 "forecast_identity": self.forecast_identity, "model_identity": self.performance_identity,
                 "feature_schema_id": FEATURE_SCHEMA_ID, "feature_order": list(FEATURE_COLUMNS)}
        if self.history_basis:                                   # absent for A0g components, so A0g records are unchanged
            label["workload_history_basis"] = self.history_basis
        return label


def fixture_components(model: Any, forecaster: Any, *, performance_identity: str, forecast_identity: str) -> PredictiveComponents:
    """Wrap labelled fixture stand-ins. They are NOT guarded against HFBTP domains: a fixture table has no training grid."""
    return PredictiveComponents(PREDICTIVE_MODE_FIXTURE, model, forecaster, performance_identity, forecast_identity,
                                fixture_data=True, real_inference_performed=False)


class TimestampedFixtureForecaster:
    """Scripted stand-in with the SAME call signature as ``StreamingEWMA.predict_timestamped``. It forecasts nothing.

    It exists so that real, timestamped, captured history travels through ``GuardedForecaster`` (the real history guard) and the
    controller's own timestamp checks, while the forecast VALUE stays a labelled fixture number. ``value`` is set by the caller
    (``None`` = a missing forecast; ``"nan"`` / ``"negative"`` model a broken estimator). The history is read for its last bin only
    (to place the origin); the values in it are not used, which is why nothing here says anything about forecast skill.
    """

    requires_timestamped_history = True
    accepts_history_reference = True
    source = "fixture"

    def __init__(self, identity: str, default_value: Any = None):
        self.identity, self.value, self.last_result, self.calls = identity, default_value, None, 0

    def predict_timestamped(self, history_timestamps_utc, history_tps, horizon_seconds, *, now_utc, max_history_age_s=5.0,
                            history_ref=None):
        self.calls += 1
        self.last_result = None
        times = tuple(history_timestamps_utc)
        value = self.value
        if value is None:
            return None                                          # a missing forecast; the guard refuses it by name
        value = float("nan") if value == "nan" else -1.0 if value == "negative" else value
        last = _parse_utc(times[-1], "history timestamp")
        reference = history_ref.as_dict() if hasattr(history_ref, "as_dict") else (dict(history_ref) if history_ref else None)
        self.last_result = ForecastResult(
            value_tps=value, horizon_seconds=horizon_seconds, frequency_seconds=1, model_identity=self.identity,
            inference_duration_ms=0.0, source=self.source, origin_timestamp_utc=(last + timedelta(seconds=1)).isoformat(),
            history_last_timestamp_utc=last.isoformat(),
            initialization_provenance={"method": "fixture_scripted_value", "history_bins_received": len(times),
                                       "history_reference": reference, "value_is_fixture": True})
        return self.last_result


class RangeCheckedFixtureModel:
    """A scripted performance table behind the REAL workload-range check (``check_arrival_rate``).

    It lets a fixture run show what the predictive controller does with a forecast outside the HFBTP range (abstain, recorded as
    ``outside_training_grid``) without loading the model. The table values stay scripted; the range check is the real code.
    """

    def __init__(self, inner: Any, *, low: float = PERFORMANCE_CONTRACT["domain"]["arrival_rate_tps"][0],
                 high: float = PERFORMANCE_CONTRACT["domain"]["arrival_rate_tps"][1]):
        if not callable(getattr(inner, "predict", None)):
            raise PredictiveIntegrationError("performance_interface_incompatible", "predictor has no predict(arrival, config)")
        self.inner, self.low, self.high = inner, low, high

    @property
    def last_prediction_metadata(self):
        return getattr(self.inner, "last_prediction_metadata", {})

    def predict(self, arrival_rate_tps: float, config: FabricConfig):
        check_arrival_rate(arrival_rate_tps, self.low, self.high)
        return self.inner.predict(arrival_rate_tps, config)


def captured_history_components(model: Any, forecaster: Any, *, performance_identity: str, forecast_identity: str,
                                history_basis: str, apply_range_check: bool = True) -> PredictiveComponents:
    """Fixture components whose workload input is a CAPTURED history (A0h).

    The forecaster is wrapped in ``GuardedForecaster`` with the fixture's identity, so every history check (at least 600
    contiguous whole-second bins, the complete UTC-day prefix, freshness, no bin that was incomplete at the decision time, H=60)
    runs on the real input. The performance model is a scripted table, optionally behind the real range check. Still a fixture:
    ``real_inference_performed`` is False and no frozen model or forecaster exists on this path.
    """
    guarded = GuardedForecaster(forecaster, expected_identity=forecast_identity)
    predictor = RangeCheckedFixtureModel(model) if apply_range_check else model
    return PredictiveComponents(PREDICTIVE_MODE_FIXTURE, predictor, guarded, performance_identity, forecast_identity,
                                fixture_data=True, real_inference_performed=False, history_basis=history_basis)


def build_real_predictive_components(*, root, manifest_path, space: ApprovedMaxMessageCountSpace, workload_source: str,
                                     operator_freshness: bool) -> PredictiveComponents:
    """The prepared route to the real frozen interfaces. DISABLED: raises before reading any pin, model or history.

    When ``REAL_INFERENCE_AUTHORIZED`` is changed (a reviewed source edit, with the owner's separate computation
    authorization), this verifies the pins, builds ``HFBTPArtifactPerformancePredictor`` and ``StreamingEWMA`` exactly as
    ``live_foundation.make_shadow_foundation`` does, and wraps both in the guards. It requires operator-supplied freshness
    limits. It does not run a forecast or a prediction.
    """
    if REAL_INFERENCE_AUTHORIZED is not True:
        raise RealInferenceNotAuthorized("real_inference_not_authorized",
                                         "frozen-model inference is not authorized in this task; the fixture mode is the only "
                                         "authorized predictive mode")
    if not operator_freshness:
        raise PredictiveIntegrationError("freshness_limits_not_operator_supplied",
                                         "real predictive mode needs stage3_controller_policy/2 with operator freshness limits")
    from .adapters import HFBTPArtifactPerformancePredictor                # imported here so the disabled path imports nothing new
    from .live_foundation import StreamingEWMA
    from .offline_replay import verified_inputs
    keys = ("selection", "model_metadata", "throughput_model", "latency_model")
    pins = verified_inputs(root, manifest_path, required_keys=keys)
    manifest_entries = json.loads(Path(manifest_path).read_text(encoding="utf-8"))["files"]
    selection = json.loads(pins["selection"].read_text(encoding="utf-8"))
    horizon = selection["horizons"]["60"]
    if horizon["selected_model"] != "ewma" or horizon["alpha"] != FORECAST_CONTRACT["alpha"] or selection["min_history_s"] != FORECAST_CONTRACT["min_history_bins"]:
        raise PredictiveIntegrationError("forecast_selection_mismatch", "frozen selection differs from EWMA alpha=0.005, H=60, min history 600")
    predictor = GuardedPerformancePredictor(HFBTPArtifactPerformancePredictor(pins["model_metadata"].parent, candidate_space=space),
                                            space=space)
    forecaster = GuardedForecaster(StreamingEWMA(workload_source), expected_identity=REAL_FORECAST_IDENTITY)
    provenance = {"sha256": {key: manifest_entries[key]["sha256"] for key in keys if key != "selection"},
                  "forecast_selection_sha256": manifest_entries["selection"]["sha256"]}
    return PredictiveComponents(PREDICTIVE_MODE_REAL, predictor, forecaster, REAL_PERFORMANCE_IDENTITY, REAL_FORECAST_IDENTITY,
                                fixture_data=False, real_inference_performed=False, provenance=provenance)


# ------------------------------------------------------------------------------------------------------- evidence rows
_EXTRAPOLATION_TYPES = ("UnsupportedExtrapolationError",)


def predictive_evidence(components_label: Mapping[str, Any], *, controller: str, decision: Mapping[str, Any],
                        frame_status: str, history_reference: Mapping[str, Any] | None,
                        coverage: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
    """The ``predictive_evidence`` block of a ``stage3_shadow_decision/2`` row (None for the non-predictive controllers).

    Predicted values are copied only from the controller's decision, never computed here. A fixture value stays labelled
    ``fixture_data``. ``ood_status`` is ``outside_training_grid`` only when the guard said so, ``within_training_grid`` only
    when a guarded real predictor accepted the request, and never ``within_training_grid`` for a fixture.
    """
    if controller != "predictive":
        return None
    metadata = decision.get("metadata") or {}
    error, error_type = metadata.get("error"), metadata.get("error_type")
    forecast = metadata.get("forecast") or {}
    performance = decision.get("predicted_performance")
    fixture = bool(components_label["fixture_data"])
    if error_type in _EXTRAPOLATION_TYPES or (error and "outside_training_grid" in str(error)):
        ood = OOD_OUTSIDE_GRID
    elif fixture:
        ood = OOD_NOT_APPLICABLE_FIXTURE
    elif performance is not None and error is None:
        ood = OOD_WITHIN_GRID
    else:
        ood = OOD_NOT_EVALUATED
    observation_extra = {} if coverage is None else {"coverage": dict(coverage)}        # A0i: absent for A0g/A0h records, so they are unchanged
    return {
        "mode": components_label["mode"], "fixture_data": fixture,
        "real_inference_performed": bool(components_label["real_inference_performed"]),
        "forecast": {"source": forecast.get("source"), "identity": forecast.get("model_identity") or components_label["forecast_identity"],
                     "horizon_seconds": forecast.get("horizon_seconds"), "unit": forecast.get("value_unit"),
                     "value_tps": forecast.get("value_tps"), "is_fixture_value": fixture and forecast.get("value_tps") is not None,
                     "origin_timestamp_utc": forecast.get("origin_timestamp_utc"),
                     "target": "mean arrival rate over [origin, origin + horizon); a forecast, not an observation"},
        "workload_observation": {"history_reference": None if history_reference is None else dict(history_reference),
                                 "basis": (components_label.get("workload_history_basis")
                                           or ("fixture placeholder history" if fixture else "complete UTC-day prefix reference")),
                                 **observation_extra},
        "performance_model": {"identity": components_label["model_identity"], "feature_schema_id": components_label["feature_schema_id"],
                              "feature_order": list(components_label["feature_order"]),
                              "feature_schema_is_the_contract_a_real_model_must_meet_not_what_a_fixture_uses": fixture,
                              "predicted_throughput_tps": None if performance is None else performance.get("throughput_tps"),
                              "predicted_latency_ms": None if performance is None else performance.get("latency_ms"),
                              "predictions_are_fixture_values": fixture and performance is not None,
                              "note": "a prediction, not a measured committed throughput or latency"},
        "ood_status": ood,
        "failure": None if error is None else {"stage": metadata.get("abstention_stage"), "error_type": error_type, "error": error},
        "frame_status": frame_status,
        "recommendation_is_not_applied": True,
    }
