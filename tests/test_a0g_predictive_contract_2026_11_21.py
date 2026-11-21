"""A0g: the forecast and performance-model input/output contracts of the real predictive interface (2026-11-21).

Checklist items 1-19. EVERYTHING here runs against test doubles or the real adapter code over FAKE estimators. The frozen EWMA is
never executed and no frozen HFBTP artifact is read or unpickled: ``joblib.load``, ``pickle.load`` and the artifact loader are
patched to raise in every test, so a regression that reaches a real model fails loudly. Passing these tests shows the contract is
enforced; it does NOT show that the real model's predictions are correct.
"""

from __future__ import annotations

import math
import pickle
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from tests import a0g_support as g  # noqa: F401  (sets sys.path)
from fabric_perf.controllers import adapters
from fabric_perf.controllers import stage3_predictive as sp
from fabric_perf.controllers.adapters import ApprovedMaxMessageCountSpace, ForecastResult, HFBTPArtifactPerformancePredictor
from fabric_perf.controllers.base import FabricConfig, PerformancePrediction
from fabric_perf.controllers.strategies import PredictiveController
from fabric_perf.models.baseline import FEATURE_COLUMNS

DAY = datetime(2026, 11, 21, 0, 0, 0, tzinfo=timezone.utc)
CONFIG = FabricConfig(max_message_count=40, orderers=3)
SPACE = ApprovedMaxMessageCountSpace(frozenset({40, 45}), {40: 40, 45: 45}, CONFIG)


@pytest.fixture(autouse=True)
def no_model_is_loaded(monkeypatch):
    import joblib

    def boom(*a, **k):
        raise AssertionError("a frozen model artifact was loaded")
    monkeypatch.setattr(joblib, "load", boom)
    monkeypatch.setattr(pickle, "load", boom)
    monkeypatch.setattr(pickle, "loads", boom)
    monkeypatch.setattr(adapters, "load_hfbtp_artifact", boom)


# ---------------------------------------------------------------------------------------------------- forecaster doubles
def history(bins=700, start=DAY, value=20):
    times = tuple((start + timedelta(seconds=i)).isoformat() for i in range(bins))
    return times, tuple(value for _ in range(bins))


class MockEwma:
    """A test double with the signature and result shape of ``StreamingEWMA.predict_timestamped``. It is NOT the frozen EWMA."""
    accepts_history_reference = True
    source = "mock-workload"

    def __init__(self, **overrides):
        self.overrides, self.calls, self.last_result = overrides, [], None

    def predict_timestamped(self, history_timestamps_utc, history_tps, horizon_seconds, *, now_utc, max_history_age_s=5.0, history_ref=None):
        self.calls.append((tuple(history_timestamps_utc), tuple(history_tps), horizon_seconds, now_utc, max_history_age_s, history_ref))
        last = datetime.fromisoformat(history_timestamps_utc[-1])
        fields = dict(value_tps=55.0, horizon_seconds=60, frequency_seconds=1, model_identity=sp.REAL_FORECAST_IDENTITY, source="mock",
                      inference_duration_ms=0.0, origin_timestamp_utc=(last + timedelta(seconds=1)).isoformat(),
                      history_last_timestamp_utc=last.isoformat())
        fields.update(self.overrides)
        self.last_result = ForecastResult(**fields)
        return self.last_result


def guard(inner=None, **kw):
    return sp.GuardedForecaster(inner or MockEwma(), expected_identity=sp.REAL_FORECAST_IDENTITY, **kw)


def now_after(times, seconds=1.5):
    return (datetime.fromisoformat(times[-1]) + timedelta(seconds=seconds)).isoformat()


def call(forecaster, times, values, *, horizon=60, now=None, age=5.0):
    return forecaster.predict_timestamped(times, values, horizon, now_utc=now or now_after(times), max_history_age_s=age)


def code_of(exc_info):
    return exc_info.value.code


# ---------------------------------------------------------------------------------------------- 1-4 forecast mapping
def test_01_history_is_passed_through_unchanged_with_the_decision_time_and_age_limit():
    inner, times, values = MockEwma(), *history()
    call(guard(inner), times, values, now=now_after(times), age=4.0)
    sent_times, sent_values, horizon, now, age, ref = inner.calls[0]
    assert sent_times == times and sent_values == values and horizon == 60 and age == 4.0 and ref is None
    assert now == now_after(times)


def test_02_history_must_be_ordered_contiguous_whole_second_bins():
    times, values = history()
    for bad in (tuple(reversed(times)), times[:300] + times[301:], times[:-1] + (times[-2],)):
        with pytest.raises(sp.PredictiveIntegrationError) as e:
            call(guard(), bad, values[:len(bad)], now=now_after(times))
        assert code_of(e) == "forecast_history_timestamps_inconsistent"
    fractional = tuple((DAY + timedelta(seconds=i, milliseconds=500)).isoformat() for i in range(700))
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        call(guard(), fractional, values, now=now_after(fractional))
    assert code_of(e) == "forecast_history_timestamps_inconsistent"


def test_03_only_the_frozen_horizon_is_available():
    times, values = history()
    inner = MockEwma()
    for horizon in (10, 30, 59, 61, 120, True):
        with pytest.raises(sp.PredictiveIntegrationError) as e:
            call(guard(inner), times, values, horizon=horizon)
        assert code_of(e) == "forecast_horizon_unavailable"
    assert inner.calls == []                                                   # refused before the forecaster ran
    assert call(guard(inner), times, values, horizon=60).horizon_seconds == 60


def test_04_the_forecast_unit_is_transactions_per_second_and_values_are_counts_per_one_second_bin():
    times, values = history()
    assert call(guard(), times, values).value_unit == "transactions/s"
    for bad in (-1, float("nan"), float("inf"), "20", None, True):
        with pytest.raises(sp.PredictiveIntegrationError) as e:
            call(guard(), times, values[:-1] + (bad,))
        assert code_of(e) == "forecast_history_value_invalid"
    inner = MockEwma()
    guarded = guard(inner)                                                     # built first: the guard inspects the real signature
    original = inner.predict_timestamped

    def wrong_unit(*a, **k):
        r = original(*a, **k)
        object.__setattr__(r, "value_unit", "transactions/min")
        return r
    inner.predict_timestamped = wrong_unit
    with pytest.raises(sp.PredictiveIntegrationError) as e:                    # a result in another unit is refused
        call(guarded, times, values)
    assert code_of(e) == "forecast_unit_mismatch"


# ---------------------------------------------------------------------------------------------- 5-9 forecast inputs
def test_05_missing_or_short_history_is_refused_never_filled():
    inner = MockEwma()
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        call(guard(inner), (), (), now=DAY.isoformat())
    assert code_of(e) == "forecast_history_missing"
    times, values = history(bins=599)
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        call(guard(inner), times, values)
    assert code_of(e) == "forecast_history_insufficient"
    times, values = history()
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        call(guard(inner), times, values[:-1])
    assert code_of(e) == "forecast_history_inconsistent"
    assert inner.calls == []
    assert call(guard(inner), *history(bins=600)).value_tps == 55.0            # exactly the minimum is accepted


def test_06_stale_history_is_refused_at_the_age_boundary():
    times, values = history()
    ok = call(guard(), times, values, now=now_after(times, 5.0), age=5.0)         # age exactly 5 s is fresh (library convention)
    assert ok.value_tps == 55.0
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        call(guard(), times, values, now=now_after(times, 5.001), age=5.0)
    assert code_of(e) == "forecast_history_stale"
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        call(guard(), times, values, age=float("nan"))
    assert code_of(e) == "forecast_history_age_limit_invalid"


def test_07_invalid_timestamps_are_refused():
    times, values = history()
    for bad in ("not-a-time", "2026-11-21T00:00:00", "", None):
        with pytest.raises(sp.PredictiveIntegrationError) as e:
            call(guard(), (bad,) + times[1:], values, now=now_after(times))
        assert code_of(e) == "timestamp_invalid"
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        call(guard(), times, values, now="garbage")
    assert code_of(e) == "timestamp_invalid"


def test_08_history_that_includes_data_not_yet_available_at_the_decision_time_is_refused_as_leakage():
    times, values = history()
    # the last bin [t, t+1) has not completed at t + 0.5 s: using it would use the future
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        call(guard(), times, values, now=now_after(times, 0.5))
    assert code_of(e) == "forecast_input_from_future"
    with pytest.raises(sp.PredictiveIntegrationError) as e:                    # a decision time before the history ends
        call(guard(), times, values, now=times[-100])
    assert code_of(e) == "forecast_input_from_future"
    assert call(guard(), times, values, now=now_after(times, 1.0)).value_tps == 55.0   # completed exactly at the decision time


def test_09_an_incompatible_forecaster_or_result_is_refused():
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        sp.GuardedForecaster(object(), expected_identity="x")
    assert code_of(e) == "forecast_interface_incompatible"

    class NoNow:
        def predict_timestamped(self, times, values, horizon): ...
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        sp.GuardedForecaster(NoNow(), expected_identity="x")
    assert code_of(e) == "forecast_interface_incompatible"
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        guard().predict([1.0] * 700, 60)                                          # the untimestamped route does not exist
    assert code_of(e) == "forecast_interface_incompatible"
    times, values = history()
    for overrides, expected in ((dict(model_identity="FIXTURE_MOCK_FORECAST"), "forecast_model_identity_mismatch"),
                                (dict(horizon_seconds=30), "forecast_horizon_mismatch"),
                                (dict(origin_timestamp_utc=(DAY + timedelta(seconds=5000)).isoformat()), "forecast_origin_inconsistent"),
                                (dict(history_last_timestamp_utc=(DAY + timedelta(seconds=10)).isoformat()), "forecast_origin_inconsistent")):
        with pytest.raises(sp.PredictiveIntegrationError) as e:
            call(guard(MockEwma(**overrides)), times, values)
        assert code_of(e) == expected, overrides

    class Plain:
        accepts_history_reference = False

        def predict_timestamped(self, times, values, horizon, *, now_utc, max_history_age_s=5.0):
            return 55.0
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        call(sp.GuardedForecaster(Plain(), expected_identity="x"), times, values)
    assert code_of(e) == "forecast_result_invalid"


def test_the_guard_is_a_noop_for_a_valid_request_and_the_controller_records_a_refusal_as_an_abstention():
    from fabric_perf.controllers.base import ControllerState
    times, values = history()
    state = ControllerState(timestamp_utc=now_after(times), current_config=CONFIG, observed_arrival_rate_tps=20.0,
                            arrival_history_tps=values, arrival_history_timestamps_utc=times, telemetry_age_s=1.0)
    model = GuardedModel(PerformancePrediction(50.0, 100.0))
    controller = PredictiveController(sp.GuardedPerformancePredictor(model, space=SPACE), CONFIG, arrival_forecaster=guard(),
                                      candidate_configs=[CONFIG, replace(CONFIG, max_message_count=45)], horizon_seconds=60)
    decision = controller.decide(state)
    assert decision.outcome == "hold" and decision.predicted_arrival_rate_tps == 55.0   # equal utilities: the current candidate is kept
    # too little history: the controller has no count check of its own, so the guard's code reaches the abstention record
    short_times, short_values = history(bins=599)
    short = replace(state, timestamp_utc=now_after(short_times), arrival_history_tps=short_values, arrival_history_timestamps_utc=short_times)
    controller = PredictiveController(sp.GuardedPerformancePredictor(model, space=SPACE), CONFIG, arrival_forecaster=guard(),
                                      candidate_configs=[CONFIG], horizon_seconds=60)
    refused = controller.decide(short)
    assert refused.outcome == "abstain" and "forecast_history_insufficient" in refused.metadata["error"]
    assert refused.metadata["abstention_stage"] == "forecast" and refused.metadata["error_type"] == "PredictiveIntegrationError"


# ---------------------------------------------------------------------------------------------- performance doubles
def good_metadata(arrival, block, **over):
    values = {"Set Transaction Arrival Rate": float(arrival), "Block Size": float(block), "Orderers": 3.0}
    for name in FEATURE_COLUMNS[3:]:
        values[name] = 1.0
    metadata = {"model_identity": sp.REAL_PERFORMANCE_IDENTITY, "feature_order": list(FEATURE_COLUMNS), "feature_values": values,
                "target_units": {"Throughput": "tx/s", "Avg Latency": "s", "controller_latency": "ms"}, "hfbtp_block_size": block}
    metadata.update(over)
    return metadata


class GuardedModel:
    """A test double with the shape of ``HFBTPArtifactPerformancePredictor``. It is NOT the frozen model."""

    def __init__(self, output, **metadata_over):
        self.output, self.over, self.calls, self.last_prediction_metadata = output, metadata_over, [], {}

    def predict(self, arrival, config):
        self.calls.append((arrival, config))
        block = SPACE.hfbtp_block_size(config)
        self.last_prediction_metadata = good_metadata(arrival, block, **self.over)
        return self.output


_DEFAULT = object()


def predictor(output=_DEFAULT, **over):
    inner = GuardedModel(PerformancePrediction(50.0, 120.0) if output is _DEFAULT else output, **over)
    return sp.GuardedPerformancePredictor(inner, space=SPACE), inner


# ---------------------------------------------------------------------------------------------- 10-12 real adapter, fake estimators
class FakeEstimator:
    def __init__(self, value):
        self.value, self.seen = value, []
        self.feature_names_in_ = np.array(FEATURE_COLUMNS)

    def predict(self, frame):
        self.seen.append(frame.copy())
        return np.array([self.value])


def real_adapter_over_fakes(monkeypatch, throughput=123.4, latency=0.25):
    estimators = {"Throughput": FakeEstimator(throughput), "Avg Latency": FakeEstimator(latency)}
    metadata = {"features": list(FEATURE_COLUMNS), "model_name": "random_forest", "rows": 24687}
    monkeypatch.setattr(adapters, "load_hfbtp_artifact", lambda directory, target: (estimators[target], dict(metadata)))
    return HFBTPArtifactPerformancePredictor("not-a-real-directory", candidate_space=SPACE), estimators


def test_10_the_real_adapter_maps_a_candidate_to_the_documented_feature_columns_in_order(monkeypatch):
    real, estimators = real_adapter_over_fakes(monkeypatch)
    real.predict(55.0, CONFIG)
    frame = estimators["Throughput"].seen[0]
    assert list(frame.columns) == FEATURE_COLUMNS and len(frame) == 1
    row = frame.iloc[0]
    assert (row["Set Transaction Arrival Rate"], row["Block Size"], row["Orderers"]) == (55.0, 40.0, 3.0)
    assert row["log_set_rate"] == pytest.approx(math.log10(55.0)) and row["block_per_unit_load"] == pytest.approx(40.0 / 55.0)
    assert row["set_x_orderers"] == 165.0 and row["block_over_orderers"] == pytest.approx(40.0 / 3.0)
    assert not ({"Actual Transaction Arrival Rate", "Min Latency", "Max Latency", "Throughput", "Avg Latency"} & set(frame.columns))


def test_11_12_throughput_and_latency_outputs_are_mapped_and_latency_is_converted_from_seconds_to_milliseconds(monkeypatch):
    real, _ = real_adapter_over_fakes(monkeypatch, throughput=123.4, latency=0.25)
    prediction = real.predict(55.0, CONFIG)
    assert prediction.throughput_tps == 123.4 and prediction.latency_ms == 250.0
    assert real.last_prediction_metadata["target_units"] == {"Throughput": "tx/s", "Avg Latency": "s", "controller_latency": "ms"}


def test_the_guard_accepts_exactly_what_the_real_adapter_reports_over_fake_estimators(monkeypatch):
    real, _ = real_adapter_over_fakes(monkeypatch)
    guarded = sp.GuardedPerformancePredictor(real, space=SPACE)
    prediction = guarded.predict(55.0, CONFIG)
    assert prediction.throughput_tps == 123.4
    meta = guarded.last_prediction_metadata
    assert meta["guard"] == {"feature_schema_id": sp.FEATURE_SCHEMA_ID, "ood_status": sp.OOD_WITHIN_GRID, "clipping_applied": False}
    assert meta["feature_order"] == FEATURE_COLUMNS and list(meta["feature_values"]) == FEATURE_COLUMNS


def test_the_real_adapter_rejects_a_nonfinite_estimator_output(monkeypatch):
    real, _ = real_adapter_over_fakes(monkeypatch, throughput=float("nan"))
    with pytest.raises(ValueError):
        real.predict(55.0, CONFIG)


# ---------------------------------------------------------------------------------------------- 13-14 schema
def test_13_missing_features_are_refused_and_never_substituted():
    for drop in FEATURE_COLUMNS:
        model = GuardedModel(PerformancePrediction(1.0, 1.0))
        inner_predict = model.predict

        def predict(arrival, config, _drop=drop, _inner=inner_predict, _model=model):
            result = _inner(arrival, config)
            values = dict(_model.last_prediction_metadata["feature_values"])
            del values[_drop]
            _model.last_prediction_metadata = {**_model.last_prediction_metadata, "feature_values": values}
            return result
        model.predict = predict
        with pytest.raises(sp.PredictiveIntegrationError) as e:
            sp.GuardedPerformancePredictor(model, space=SPACE).predict(55.0, CONFIG)
        assert code_of(e) == "performance_schema_mismatch", drop


def test_14_a_different_feature_order_is_refused():
    reordered = list(reversed(FEATURE_COLUMNS))
    p, _ = predictor(feature_order=reordered)
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        p.predict(55.0, CONFIG)
    assert code_of(e) == "performance_schema_mismatch"
    swapped = list(FEATURE_COLUMNS)
    swapped[0], swapped[1] = swapped[1], swapped[0]
    p, _ = predictor(feature_order=swapped)
    with pytest.raises(sp.PredictiveIntegrationError):
        p.predict(55.0, CONFIG)
    p, inner = predictor()
    values = good_metadata(55.0, 40)["feature_values"]
    p, _ = predictor(feature_values={k: values[k] for k in reversed(list(values))})
    with pytest.raises(sp.PredictiveIntegrationError):
        p.predict(55.0, CONFIG)


# ---------------------------------------------------------------------------------------------- 15-16 outputs
@pytest.mark.parametrize("bad", [None, {"throughput_tps": 1.0, "latency_ms": 1.0}, (1.0, 1.0), 3.0, "x"])
def test_15_an_output_that_is_not_a_performance_prediction_is_refused(bad):
    p, _ = predictor(bad)
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        p.predict(55.0, CONFIG)
    assert code_of(e) == "performance_output_invalid"


@pytest.mark.parametrize("field,value", [("throughput_tps", float("nan")), ("throughput_tps", float("inf")), ("latency_ms", float("nan")),
                                         ("latency_ms", float("-inf")), ("throughput_tps", -1.0), ("latency_ms", -0.5)])
def test_16_nonfinite_or_negative_predictions_are_refused(field, value):
    out = PerformancePrediction(10.0, 10.0)
    object.__setattr__(out, field, value)                  # bypass the dataclass check to model a corrupted adapter output
    p, _ = predictor(out)
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        p.predict(55.0, CONFIG)
    assert code_of(e) == "performance_output_invalid"


# ---------------------------------------------------------------------------------------------- 17-18 domain
@pytest.mark.parametrize("arrival", [0.0, 9.999, 5.0, 200.001, 250.0, 1000.0])
def test_17_an_arrival_rate_outside_the_training_grid_is_refused_not_clipped(arrival):
    p, inner = predictor()
    with pytest.raises(sp.UnsupportedExtrapolationError) as e:
        p.predict(arrival, CONFIG)
    assert code_of(e) == "arrival_rate_outside_training_grid"
    assert inner.calls == []                               # the estimator was never asked: nothing was clipped to 10 or 200


@pytest.mark.parametrize("arrival", [10.0, 55.5, 200.0])
def test_the_grid_boundaries_are_inclusive(arrival):
    p, inner = predictor()
    p.predict(arrival, CONFIG)
    assert inner.calls[0][0] == arrival


@pytest.mark.parametrize("arrival", [float("nan"), float("inf"), -1.0, "55", None, True])
def test_17b_a_nonfinite_or_non_numeric_arrival_rate_is_an_input_error(arrival):
    p, inner = predictor()
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        p.predict(arrival, CONFIG)
    assert code_of(e) == "performance_input_invalid" and inner.calls == []


def space_with(mapping, baseline=CONFIG):
    return ApprovedMaxMessageCountSpace(frozenset(mapping), mapping, baseline)


def test_18_unsupported_candidate_configurations_are_refused():
    cases = [({40: 805}, "candidate_block_size_outside_training_grid", True), ({40: 5}, "candidate_block_size_outside_training_grid", True),
             ({40: 42}, "unsupported_candidate_configuration", False), ({40: 245}, None, False)]
    for mapping, code, extrap in cases:
        space = space_with(mapping, FabricConfig(max_message_count=40, orderers=9 if mapping[40] == 245 else 3))
        guarded = sp.GuardedPerformancePredictor(GuardedModel(PerformancePrediction(1, 1)), space=space)
        config = FabricConfig(max_message_count=40, orderers=9 if mapping[40] == 245 else 3)
        with pytest.raises(sp.PredictiveIntegrationError) as e:
            guarded.predict(55.0, config)
        assert (code_of(e) == code) if code else code_of(e) == "unsupported_candidate_configuration"
        assert isinstance(e.value, sp.UnsupportedExtrapolationError) == extrap
    p, inner = predictor()
    with pytest.raises(sp.PredictiveIntegrationError) as e:                    # a MaxMessageCount the owner did not approve
        p.predict(55.0, replace(CONFIG, max_message_count=99))
    assert code_of(e) == "candidate_not_approved" and inner.calls == []
    with pytest.raises(sp.PredictiveIntegrationError) as e:                    # an orderer count HFBTP never measured
        sp.GuardedPerformancePredictor(GuardedModel(PerformancePrediction(1, 1)), space=space_with({40: 40}, FabricConfig(max_message_count=40, orderers=4))
                                       ).predict(55.0, FabricConfig(max_message_count=40, orderers=4))
    assert code_of(e) == "unsupported_candidate_configuration"
    for absent in ((525, 9), (725, 5)):
        cfg = FabricConfig(max_message_count=40, orderers=absent[1])
        guarded = sp.GuardedPerformancePredictor(GuardedModel(PerformancePrediction(1, 1)), space=space_with({40: absent[0]}, cfg))
        with pytest.raises(sp.PredictiveIntegrationError) as e:
            guarded.predict(55.0, cfg)
        assert "absent" in str(e.value)


# ---------------------------------------------------------------------------------------------- 19 metadata
@pytest.mark.parametrize("over", [dict(model_identity="FIXTURE_MOCK_PERFORMANCE_MODEL"), dict(model_identity=None),
                                  dict(target_units={"Throughput": "tx/s", "Avg Latency": "ms", "controller_latency": "ms"}),
                                  dict(target_units={}), dict(hfbtp_block_size=45), dict(feature_values=None)])
def test_19_metadata_that_does_not_match_the_artifact_contract_is_refused(over):
    p, _ = predictor(**over)
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        p.predict(55.0, CONFIG)
    assert code_of(e) in {"performance_metadata_mismatch", "performance_schema_mismatch", "performance_unit_mismatch"}


def test_feature_values_that_disagree_with_the_request_are_refused():
    values = good_metadata(60.0, 40)["feature_values"]                          # the model "saw" 60 tx/s, we asked for 55
    p, _ = predictor(feature_values=values)
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        p.predict(55.0, CONFIG)
    assert code_of(e) == "performance_schema_mismatch"


def test_the_contract_constants_agree_with_the_existing_interfaces_without_executing_them():
    from fabric_perf.controllers import live_foundation as lf
    contract = sp.FORECAST_CONTRACT
    assert (contract["alpha"], contract["horizon_seconds"], contract["frequency_seconds"], contract["min_history_bins"]) == (
        lf.EWMA_PARAMETERS["alpha"], lf.EWMA_PARAMETERS["horizon_seconds"], lf.EWMA_PARAMETERS["frequency_seconds"],
        lf.EWMA_PARAMETERS["min_history_bins"])
    domain = sp.PERFORMANCE_CONTRACT["domain"]
    H = HFBTPArtifactPerformancePredictor
    assert domain["arrival_rate_tps"] == [H.ARRIVAL_MIN, H.ARRIVAL_MAX] and domain["block_size"] == [H.BLOCK_MIN, H.BLOCK_MAX]
    assert [domain["orderers"][0], domain["orderers"][-1]] == [H.ORDERERS_MIN, H.ORDERERS_MAX]
    assert sp.PERFORMANCE_CONTRACT["feature_order"] == FEATURE_COLUMNS
    assert sp.REAL_FORECAST_IDENTITY == "ewma-alpha-0.005-h60-utc-day-prefix"
    assert len(sp.FEATURE_SCHEMA_ID) == 64 and sp.FEATURE_SCHEMA_ID == sp.FEATURE_SCHEMA_ID.lower()


def test_the_feature_schema_id_changes_when_the_feature_order_changes():
    import hashlib
    base = {"feature_order": sp.PERFORMANCE_CONTRACT["feature_order"], "targets": sp.PERFORMANCE_CONTRACT["targets"],
            "raw_inputs": sp.PERFORMANCE_CONTRACT["raw_inputs"], "domain": sp.PERFORMANCE_CONTRACT["domain"]}
    assert hashlib.sha256(sp._canonical(base).encode()).hexdigest() == sp.FEATURE_SCHEMA_ID
    changed = {**base, "feature_order": list(reversed(base["feature_order"]))}
    assert hashlib.sha256(sp._canonical(changed).encode()).hexdigest() != sp.FEATURE_SCHEMA_ID
