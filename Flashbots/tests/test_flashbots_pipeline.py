"""Tests for the Flashbots pipeline (synthetic data unless marked; nothing here writes outside tmp_path).

    .venv/Scripts/python.exe -B -m pytest -p no:cacheprovider Flashbots/tests -q
"""

from __future__ import annotations

import hashlib
import json
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

FB = Path(__file__).resolve().parents[1]
for sub in ("", "forecasting", "evaluation", "time_series", "preprocessing", "data_audit"):
    sys.path.insert(0, str(FB / sub))

import fbcommon as fc  # noqa: E402
import metrics as em  # noqa: E402
import models as fm  # noqa: E402
import build_target_series as bts  # noqa: E402
import extract_transactions as ext  # noqa: E402
import audit_raw_archives as ara  # noqa: E402

DATE = "2026-01-01"
DAY0 = fc.day_start_ms(DATE)


# ------------------------------------------------------------------------------------------- helpers
def raw_rows(offsets_ms, hashes=None, **overrides):
    rows = []
    for i, off in enumerate(offsets_ms):
        row = {"timestamp_ms": DAY0 + off, "hash": (hashes[i] if hashes else "0x" + f"{i:064x}"), "chain_id": 1,
               "from": "0x" + "a" * 40, "to": "0x" + "b" * 40, "value": 10 ** 18, "nonce": i, "gas": 21000,
               "gas_price": 2 * 10 ** 9, "gas_tip_cap": 10 ** 9, "gas_fee_cap": 2 * 10 ** 9, "data_size": 0,
               "data_4bytes": "", "sources": "", "included_at_block_height": 100, "included_block_timestamp_ms": DAY0 + off + 5000,
               "inclusion_delay_ms": 5000, "tx_type": 2}
        row.update({k: v[i] for k, v in overrides.items()})
        rows.append(row)
    return pd.DataFrame(rows, columns=fc.EXPECTED_COLUMNS)


def make_zip(tmp_path: Path, frame: pd.DataFrame) -> tuple[Path, dict]:
    csv_path = tmp_path / f"{DATE}.csv"
    frame.to_csv(csv_path, index=False)
    zip_path = tmp_path / f"{DATE}.csv.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.write(csv_path, arcname=csv_path.name)
    entry = {"source_path": str(zip_path), "sha256": hashlib.sha256(zip_path.read_bytes()).hexdigest().upper(),
             "rows": len(frame), "archive_entry": csv_path.name}
    return zip_path, entry


# ------------------------------------------------------------------------------------------- loading / schema
def test_checksum_mismatch_refuses_to_read(tmp_path):
    _, entry = make_zip(tmp_path, raw_rows([0, 10]))
    entry["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="checksum mismatch"):
        fc.verify_archive(entry)


def test_extract_keeps_every_row_and_converts_units(tmp_path):
    zip_path, _ = make_zip(tmp_path, raw_rows([0, 1500, 1500, 86_399_999]))
    data = ext.extract(zip_path)
    assert len(data["timestamp_ms"]) == 4  # exact tie kept, nothing deduplicated
    assert np.allclose(data["gas_fee_cap_gwei"], 2.0) and np.allclose(data["gas_tip_cap_gwei"], 1.0)
    assert np.allclose(data["value_eth"], 1.0)
    assert set(data) >= {"timestamp_ms", "gas", "data_size", "tx_type", "inclusion_delay_ms"}
    assert "hash" not in data and "from" not in data  # identity fields are not copied


def test_audit_counts_schema_duplicates_decreases_and_gaps(tmp_path):
    frame = raw_rows([0, 2000, 1000, 9000], hashes=["0x" + "1" * 64, "0x" + "1" * 64, "0x" + "2" * 64, "0xzz"])
    _, entry = make_zip(tmp_path, frame)
    report, profile, checks, _, gaps = ara.audit_day(DATE, entry)
    c = checks.set_index("check")["failing_rows"]
    assert report["schema_matches_registered"] and report["rows"] == 4
    assert c["duplicate_tx_hash_rows"] == 1
    assert c["timestamp_decreases"] == 1
    assert c["hash_format_0x_64hex"] == 1
    assert report["timestamps"]["max_interarrival_ms"] == 8000
    assert report["timestamps"]["longest_zero_arrival_run_s"] == 86_400 - 9 - 1 + 0  # seconds 10..86399 are empty
    assert set(profile["column"]) == set(fc.EXPECTED_COLUMNS)


def test_audit_flags_inclusion_formula_and_fee_cap_violations(tmp_path):
    frame = raw_rows([0, 10, 20], inclusion_delay_ms=[5000, 4000, 5000], gas_tip_cap=[10 ** 9, 3 * 10 ** 9, 10 ** 9])
    _, entry = make_zip(tmp_path, frame)
    _, _, checks, _, _ = ara.audit_day(DATE, entry)
    c = checks.set_index("check")["failing_rows"]
    assert c["inclusion_delay_formula_mismatch"] == 1
    assert c["tip_cap_gt_fee_cap"] == 1


# ------------------------------------------------------------------------------------------- timestamps / aggregation
def test_second_index_bins_are_left_closed_utc_seconds():
    ms = np.array([0, 999, 1000, 86_399_999]) + DAY0
    assert bts.second_index(ms, DATE).tolist() == [0, 0, 1, 86_399]


@pytest.mark.parametrize("offset", [-1, 86_400_000])
def test_timestamps_outside_the_utc_day_are_rejected(offset):
    with pytest.raises(ValueError):
        bts.second_index(np.array([DAY0 + offset]), DATE)


def test_aggregate_fills_missing_seconds_with_zero_and_leaves_medians_undefined():
    tx = {"timestamp_ms": np.array([0, 400, 5000]) + DAY0, "gas": np.array([21000, 50000, 100000]),
          "data_size": np.array([0, 68, 100]), "gas_fee_cap_gwei": np.array([1.0, 3.0, 5.0]), "gas_tip_cap_gwei": np.array([0.1, 0.3, 0.5])}
    frame = bts.aggregate(tx, DATE)
    assert len(frame) == 86_400 and frame["arrival_count"].sum() == 3
    assert frame.loc[0, "arrival_count"] == 2 and frame.loc[0, "gas_limit_sum"] == 71000
    assert frame.loc[1, "arrival_count"] == 0 and frame.loc[1, "gas_limit_sum"] == 0
    assert frame.loc[0, "median_fee_cap_gwei"] == 2.0 and np.isnan(frame.loc[1, "median_fee_cap_gwei"])
    assert str(frame.loc[5, "timestamp_utc"]) == "2026-01-01 00:00:05+00:00"


# ------------------------------------------------------------------------------------------- chronological splits
def test_origins_keep_targets_inside_the_pool_and_never_overlap():
    for horizon in (1, 10, 60, 300):
        org = fm.origins(900, 43_200, horizon)
        assert org[0] == 900 and (org + horizon).max() <= 43_200
        assert (np.diff(org) == horizon).all()
        v = fm.origins(43_200, 86_400, horizon)
        assert v.min() >= 43_200 and (v + horizon).max() <= 86_400


def test_ridge_refuses_training_targets_beyond_the_development_series():
    series = np.ones(2000)
    with pytest.raises(ValueError):
        fm.fit_ridge(series, np.array([1900, 1950]), 60, [1, 900], [1.0], 2)


def test_config_pools_are_chronological_and_disjoint():
    config = json.loads((FB / "experiments" / "configs" / "FB-FC-v1.json").read_text(encoding="utf-8"))
    d, v, f = (config["pools"][k] for k in ("D", "V", "F"))
    assert d["date"] == v["date"] < f["date"]
    assert d["end_s"] <= v["first_origin_s"]


# ------------------------------------------------------------------------------------------- forecasts
@pytest.fixture(scope="module")
def synthetic_day():
    rng = np.random.default_rng(1)
    level = 10 + 3 * np.sin(np.arange(86_400) / 5000)
    return rng.poisson(level).astype(float)


def test_models_for_horizon_drop_undefined_and_duplicate_variants():
    assert "seasonal_naive_12" in fm.models_for_horizon(1) and "seasonal_naive_12" not in fm.models_for_horizon(60)
    assert "rolling_mean_60" not in fm.models_for_horizon(60)
    assert "seasonal_naive_300" not in fm.models_for_horizon(300)
    assert fm.models_for_horizon(10)[0] == fm.REFERENCE


@pytest.mark.parametrize("horizon", [1, 10, 60])
def test_every_forecaster_is_causal(synthetic_day, horizon):
    """Changing everything at and after the origin must not change the forecast made at that origin."""
    org_d = fm.origins(900, 43_200, horizon)
    ridge, _ = fm.fit_ridge(synthetic_day[:43_200], org_d, horizon, [1, 5, 10, 30, 60, 300, 900], [1.0, 10.0], 3)
    origin = np.array([50_000])
    future_changed = synthetic_day.copy()
    future_changed[50_000:] = 1e6
    for model in fm.models_for_horizon(horizon):
        a = fm.forecast(model, synthetic_day, origin, horizon, 0.05, ridge)
        b = fm.forecast(model, future_changed, origin, horizon, 0.05, ridge)
        assert np.array_equal(a, b), model


def test_persistence_seasonal_and_tod_values():
    series = np.arange(2000, dtype=float)
    org = np.array([1000])
    assert fm.forecast("persistence_interval", series, org, 10)[0] == np.mean(series[990:1000])
    assert fm.forecast("seasonal_naive_12", series, org, 1)[0] == series[988]
    ref = np.full(2000, 7.0)
    assert fm.tod_profile(ref, org, 10)[0] == 7.0


def test_ridge_predictions_are_non_negative_and_deterministic(synthetic_day):
    org = fm.origins(900, 43_200, 10)
    p1, t1 = fm.fit_ridge(synthetic_day[:43_200], org, 10, [1, 5, 10, 30, 60, 300, 900], [0.1, 10.0], 3)
    p2, t2 = fm.fit_ridge(synthetic_day[:43_200], org, 10, [1, 5, 10, 30, 60, 300, 900], [0.1, 10.0], 3)
    assert p1 == p2 and t1.equals(t2)
    pred = fm.ridge_predict(p1, synthetic_day, fm.origins(43_200, 86_400, 10), 10)
    assert (pred >= 0).all() and np.isfinite(pred).all()
    with pytest.raises(ValueError):
        fm.ridge_predict(p1, synthetic_day, org, 60)


def test_selection_near_tie_prefers_the_simpler_model():
    assert fm.select({"persistence_interval": 1.005, "ewma": 1.0}, 0.01) == "persistence_interval"
    assert fm.select({"persistence_interval": 1.2, "ewma": 1.0, "ridge_ar": 0.995}, 0.01) == "ewma"


# ------------------------------------------------------------------------------------------- metrics
def test_metrics_known_values():
    y, yhat = np.array([2.0, 4.0]), np.array([3.0, 2.0])
    assert em.mae(y, yhat) == 1.5
    assert em.rmse(y, yhat) == pytest.approx(np.sqrt(2.5))
    assert em.bias(y, yhat) == -0.5
    assert em.wape(y, yhat) == pytest.approx(50.0)
    assert em.mape(y, yhat) == (pytest.approx(50.0), 0)
    assert em.smape(y, yhat) == pytest.approx(100 * np.mean([2 / 5, 4 / 6]))


def test_percentage_metrics_handle_zero_observations():
    y, yhat = np.array([0.0, 0.0, 4.0]), np.array([0.0, 1.0, 2.0])
    value, excluded = em.mape(y, yhat)
    assert excluded == 2 and value == pytest.approx(50.0)
    assert em.smape(y, yhat) == pytest.approx(100 * (0 + 2 + 4 / 6) / 3)  # terms: 0/0 -> 0, 2*1/1, 2*2/6
    assert np.isnan(em.wape(np.zeros(3), np.ones(3)))
    with pytest.raises(ValueError):
        em.mae(np.array([1.0, np.nan]), np.array([1.0, 1.0]))


def test_refuse_overwrite(tmp_path):
    target = tmp_path / "x.csv"
    target.write_text("a")
    with pytest.raises(FileExistsError):
        fc.refuse_overwrite([target], force=False)
    fc.refuse_overwrite([target], force=True)


# ------------------------------------------------------------------------------------------- stored-result reproducibility
STORED = FB / "results" / "FB-FC-v1"


@pytest.mark.skipif(not (STORED / "final" / "scores_F.csv").exists() or not (FB / "time_series" / "target_series_2026-10-12_1s.csv.gz").exists(),
                    reason="stored FB-FC-v1 results or target series not present")
def test_stored_test_day_scores_reproduce_from_frozen_selection():
    frozen = json.loads((STORED / "select" / "selection_frozen.json").read_text(encoding="utf-8"))
    scores = pd.read_csv(STORED / "final" / "scores_F.csv").set_index(["target", "horizon_s", "model"])
    series = fc.load_target_series("2026-10-12")
    sel = frozen["selection"]["arrival_rate|60"]
    org = fm.origins(900, 86_400, 60)
    arrivals = series["arrival_count"].to_numpy(float)
    y = fm.targets(arrivals, org, 60)
    yhat = fm.forecast("ewma", arrivals, org, 60, sel["ewma_alpha"])
    assert em.mae(y, yhat) == pytest.approx(scores.loc[("arrival_rate", 60, "ewma"), "mae"], abs=1e-12)


@pytest.mark.skipif(not (FB / "time_series" / "series_verification.json").exists(), reason="verification not run")
def test_stored_series_verification_passed():
    record = json.loads((FB / "time_series" / "series_verification.json").read_text(encoding="utf-8"))
    assert record["all_six_stored_series_identical"] is True
