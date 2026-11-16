"""Frozen forecast/model and genuine recorded replay tests; no training or live calls."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from fabric_perf.controllers import (
    ApprovedMaxMessageCountSpace, ControllerState, FabricConfig, FlashbotsSeriesForecaster,
    ForecastResult, HFBTPArtifactPerformancePredictor, PredictiveController,
)
from fabric_perf.controllers.offline_replay import verified_inputs, workload_frames
from fabric_perf.controllers.live_integration import IntegratedControllerRunner, load_jsonl_frames
from fabric_perf.controllers import ReactiveController, StaticController
from fabric_perf.models.baseline import FEATURE_COLUMNS

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "Integration/system_integration/offline_replay_inputs.json"
BASE = FabricConfig(max_message_count=45)
SPACE = ApprovedMaxMessageCountSpace(frozenset({10, 40, 45}), {10: 10, 40: 40, 45: 45}, BASE)


@pytest.fixture(scope="module")
def recorded_inputs():
    entries = json.loads(MANIFEST.read_text())["files"]
    missing = [v["path"] for v in entries.values() if not (ROOT / v["path"]).is_file()]
    if missing:
        pytest.skip(f"genuine recorded inputs unavailable: {missing}")
    return verified_inputs(ROOT, MANIFEST)


@pytest.fixture(scope="module")
def real_predictor(recorded_inputs):
    return HFBTPArtifactPerformancePredictor(recorded_inputs["model_metadata"].parent, candidate_space=SPACE)


@pytest.fixture
def unit_series(tmp_path):
    # Explicit deterministic unit fixture, never labelled historical Flashbots.
    path = tmp_path / "unit_counts.csv"
    start = datetime(2026, 9, 21, tzinfo=timezone.utc)
    frame = pd.DataFrame({"timestamp_utc": [(start + timedelta(seconds=i)).isoformat() for i in range(660)],
                          "arrival_count": [10 + i % 7 for i in range(660)],
                          "frequency_seconds": 1, "date_utc": "2026-09-21"})
    frame.to_csv(path, index=False)
    return path


def test_frozen_ewma_matches_independent_recurrence_and_correct_origin(unit_series):
    forecaster = FlashbotsSeriesForecaster(unit_series, baseline="ewma_alpha_0.005")
    expected = forecaster.values[0]
    for value in forecaster.values[1:600]:
        expected = 0.005 * value + 0.995 * expected
    result = forecaster.forecast_at(600, 60)
    assert result.value_tps == expected
    assert result.origin_timestamp_utc == "2026-09-21T00:10:00+00:00"
    assert result.history_last_timestamp_utc == "2026-09-21T00:09:59+00:00"
    assert result.horizon_seconds == 60


def test_frozen_forecast_is_invariant_to_future_bins(unit_series):
    forecaster = FlashbotsSeriesForecaster(unit_series, baseline="ewma_alpha_0.005")
    expected = forecaster.forecast_at(600, 60).value_tps
    forecaster.values[600:] = 10000
    assert forecaster.forecast_at(600, 60).value_tps == expected


@pytest.mark.parametrize("origin,horizon", [(599, 60), (600, 10), (600, 0)])
def test_frozen_profile_rejects_short_history_and_wrong_horizon(unit_series, origin, horizon):
    forecaster = FlashbotsSeriesForecaster(unit_series, baseline="ewma_alpha_0.005")
    with pytest.raises(ValueError):
        forecaster.forecast_at(origin, horizon)


def test_frozen_profile_rejects_untimestamped_and_truncated_history(unit_series):
    forecaster = FlashbotsSeriesForecaster(unit_series, baseline="ewma_alpha_0.005")
    with pytest.raises(ValueError, match="timestamped"):
        forecaster.predict(forecaster.values[:600], 60)
    times = tuple(t.isoformat() for t in forecaster.frame.timestamp_utc.iloc[1:601])
    with pytest.raises(ValueError, match="prefix"):
        forecaster.predict_timestamped(times, forecaster.values[1:601], 60, now_utc="2026-09-21T00:10:01Z")


@pytest.mark.parametrize("change", ["nan", "inf", "negative", "duplicate", "gap", "date", "frequency"])
def test_series_rejects_invalid_schema_and_bins(unit_series, change):
    frame = pd.read_csv(unit_series)
    if change in {"nan", "inf", "negative"}:
        frame["arrival_count"] = frame["arrival_count"].astype(str)
        frame.loc[0, "arrival_count"] = {"nan": "nan", "inf": "inf", "negative": "-1"}[change]
    elif change == "duplicate":
        frame.loc[1, "timestamp_utc"] = frame.loc[0, "timestamp_utc"]
    elif change == "gap":
        frame = frame.drop(index=1)
    elif change == "date":
        frame.loc[0, "date_utc"] = "2026-10-12"
    else:
        frame["frequency_seconds"] = 1.5
    frame.to_csv(unit_series, index=False)
    with pytest.raises((ValueError, TypeError)):
        FlashbotsSeriesForecaster(unit_series, baseline="ewma_alpha_0.005")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1])
def test_forecast_result_rejects_invalid_rates(value):
    with pytest.raises(ValueError):
        ForecastResult(value, 60, 1, "unit", 0, "unit")


@pytest.mark.parametrize("horizon,origin", [(10, "2026-09-21T00:10:00Z"),
                                           (60, "2026-09-21T00:09:00Z"),
                                           (60, "2026-09-21T00:10:01Z"), (60, None)])
def test_controller_rejects_cached_or_wrong_horizon_forecast(horizon, origin):
    class Cached:
        def predict_timestamped(self, *args, **kwargs):
            return ForecastResult(20, horizon, 1, "unit", 0, "unit", origin_timestamp_utc=origin)
    class NoModel:
        def predict(self, *args):
            raise AssertionError("invalid forecast reached model")
    controller = PredictiveController(NoModel(), BASE, arrival_forecaster=Cached(), candidate_configs=[BASE], horizon_seconds=60)
    decision = controller.decide(ControllerState(timestamp_utc="2026-09-21T00:10:00Z", current_config=BASE,
                                                telemetry_age_s=0, arrival_history_tps=(20,), arrival_history_timestamps_utc=("2026-09-21T00:09:59Z",)))
    assert decision.outcome == "abstain"
    assert decision.metadata["error_type"] == "ValueError"


@pytest.mark.parametrize("mapping", [{40: 40.9, 45: 45}, {40: True, 45: 45}])
def test_candidate_map_never_truncates(mapping):
    with pytest.raises(ValueError):
        ApprovedMaxMessageCountSpace(frozenset({40, 45}), mapping, BASE)


def test_model_feature_order_and_seconds_to_ms_at_adapter(monkeypatch):
    import fabric_perf.controllers.adapters as adapters
    class Estimator:
        def __init__(self, value):
            self.value = value
        def predict(self, frame):
            assert list(frame.columns) == FEATURE_COLUMNS
            assert list(frame.iloc[0]) == pytest.approx([20, 40, 3, np.log10(20), np.log10(40), 2, 60, 40 / 3])
            return [self.value]
    def loader(path, target):
        return Estimator(19 if target == "Throughput" else 0.5), {"features": FEATURE_COLUMNS, "model_name": "random_forest", "rows": 24687}
    monkeypatch.setattr(adapters, "load_hfbtp_artifact", loader)
    predictor = HFBTPArtifactPerformancePredictor(Path("unused"), candidate_space=SPACE)
    prediction = predictor.predict(20, replace(BASE, max_message_count=40))
    assert prediction.throughput_tps == 19
    assert prediction.latency_ms == 500


def test_invalid_model_metadata_refused_before_deserialization(tmp_path, monkeypatch):
    import fabric_perf.models.artifacts as artifacts
    metadata = json.loads((ROOT / "experiments/artifacts/hfbtp/metadata.json").read_text())
    metadata["features"] = list(reversed(FEATURE_COLUMNS))
    (tmp_path / "metadata.json").write_text(json.dumps(metadata))
    monkeypatch.setattr(artifacts.joblib, "load", lambda *_: pytest.fail("deserialized bad schema"))
    with pytest.raises(ValueError, match="schema"):
        artifacts.load_hfbtp_artifact(tmp_path, "Throughput")


@pytest.mark.parametrize("key,value", [("sklearn_version", "0.0"), ("dataset_sha256", "bad"),
                                      ("artifact_version", 99), ("targets", ["Throughput"])])
def test_model_loading_rejects_metadata_mismatch_before_loading(tmp_path, monkeypatch, key, value):
    import fabric_perf.models.artifacts as artifacts
    metadata = json.loads((ROOT / "experiments/artifacts/hfbtp/metadata.json").read_text())
    metadata[key] = value
    (tmp_path / "metadata.json").write_text(json.dumps(metadata))
    monkeypatch.setattr(artifacts.joblib, "load", lambda *_: pytest.fail("loaded incompatible artifact"))
    with pytest.raises(ValueError):
        artifacts.load_hfbtp_artifact(tmp_path, "Throughput")


@pytest.mark.parametrize("prediction", [[-1], [float("nan")], [float("inf")]])
def test_model_adapter_rejects_nonphysical_predictions(monkeypatch, prediction):
    import fabric_perf.controllers.adapters as adapters
    class BadEstimator:
        def predict(self, frame):
            return prediction
    monkeypatch.setattr(adapters, "load_hfbtp_artifact", lambda *_: (BadEstimator(),
        {"features": FEATURE_COLUMNS, "model_name": "random_forest", "rows": 24687}))
    predictor = HFBTPArtifactPerformancePredictor(Path("unused"), candidate_space=SPACE)
    with pytest.raises(ValueError):
        predictor.predict(20, BASE)


def test_frozen_inputs_and_model_reproduce_stored_small_reference(recorded_inputs, real_predictor):
    old = FlashbotsSeriesForecaster(recorded_inputs["series"], baseline="ewma_alpha_0.2")
    arrival = old.forecast_at(20, 10).value_tps
    assert arrival == pytest.approx(12.0428, abs=0.0001)
    # Stored 2026-11-11 Phase 3.1 values, repeated in the D-E2-10 audit.
    prediction = real_predictor.predict(arrival, BASE)
    assert prediction.throughput_tps == pytest.approx(9.9385, abs=0.001)
    assert prediction.latency_ms == pytest.approx(1077.45, abs=0.02)
    repeated = real_predictor.predict(arrival, BASE)
    # Frozen forests use parallel aggregation; roundoff is below 1e-12.
    assert repeated.throughput_tps == pytest.approx(prediction.throughput_tps, abs=1e-12)
    assert repeated.latency_ms == pytest.approx(prediction.latency_ms, abs=1e-9)


@pytest.mark.parametrize("rate", [0, 9.99, 200.01, float("nan"), float("inf"), True])
def test_real_model_refuses_out_of_domain_forecasts(real_predictor, rate):
    with pytest.raises(ValueError):
        real_predictor.predict(rate, BASE)


@pytest.mark.parametrize("block,orderers", [(11, 3), (40, 4), (245, 9), (525, 9), (725, 5)])
def test_model_refuses_unsupported_configurations(recorded_inputs, block, orderers):
    # Reuse the loaded estimators; the domain guard must run before inference.
    predictor = object.__new__(HFBTPArtifactPerformancePredictor)
    config = replace(BASE, max_message_count=block, orderers=orderers)
    predictor.candidate_space = ApprovedMaxMessageCountSpace(frozenset({block}), {block: block}, config)
    with pytest.raises(ValueError):
        predictor.predict(20, config)


def test_genuine_fabric_replay_keeps_recorded_measurements_and_shadow_state(recorded_inputs):
    frames = load_jsonl_frames(recorded_inputs["snapshots"], run_id="real-fabric-replay")
    initial = frames[0].state.current_config
    assert len(frames) == 20 and frames[0].state.observed_latency_ms is None
    controllers = {"reactive": ReactiveController(initial, congested_config=replace(initial, max_message_count=45),
                                                  latency_high_ms=1500), "static": StaticController(initial)}
    result = IntegratedControllerRunner(controllers, initial, run_id="real-fabric-replay").run(frames)
    assert result["record_count"] == 40 and not result["stop_reasons"]
    assert result["live_submission_enabled"] is False
    for row in result["records"]:
        original = frames[row["integration"]["decision_index"]].state
        assert row["state"]["observed_latency_ms"] == original.observed_latency_ms
        assert row["state"]["observed_throughput_tps"] == original.observed_throughput_tps
        assert row["configuration_state"]["after"] == original.current_config.as_dict()


def test_genuine_flashbots_frozen_path_and_model_selection(recorded_inputs, real_predictor):
    entries = json.loads(MANIFEST.read_text())["files"]
    forecaster = FlashbotsSeriesForecaster.from_frozen_selection(recorded_inputs["series"], recorded_inputs["selection"],
        selection_sha256=entries["selection"]["sha256"], series_sha256=entries["series"]["sha256"])
    controller = PredictiveController(real_predictor, BASE, arrival_forecaster=forecaster,
                                      candidate_configs=[replace(BASE, max_message_count=10), replace(BASE, max_message_count=40)],
                                      horizon_seconds=60)
    frames = list(workload_frames(forecaster, BASE, run_id="real-flashbots-replay", start=43200, samples=3))
    result = IntegratedControllerRunner({"predictive": controller}, BASE, run_id="real-flashbots-replay").run(frames)
    assert result["record_count"] == 3 and not result["stop_reasons"]
    for row in result["records"]:
        assert row["decision"]["outcome"] in {"hold", "propose"}
        assert row["decision"]["metadata"]["forecast"]["horizon_seconds"] == 60
        assert row["state"]["observed_throughput_tps"] is None
        assert row["state"]["observed_latency_ms"] is None
        selected = row["decision"]["config"]
        assert row["decision"]["metadata"]["performance_predictor"]["config"] == selected


def test_input_hash_mismatch_is_not_skipped(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"files": {"bad": {"path": "absent", "sha256": "0" * 64}}}))
    with pytest.raises(ValueError, match="SHA-256"):
        verified_inputs(tmp_path, path)
