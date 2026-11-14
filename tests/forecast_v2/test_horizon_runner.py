"""The freeze is enforced in code: separate days, hash-pinned selection, code pinned, single pass. Synthetic series only."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from fabric_perf.forecasting import horizon_benchmark as hb
from fabric_perf.forecasting import horizon_runner as hr
from fabric_perf.replay import trace
from fabric_perf.replay.common import IntegrityError
from tests.replay.helpers import synthetic_offsets, write_series


@pytest.fixture(scope="module")
def series_dir(tmp_path_factory):
    """Both days, synthetic, with different statistics."""
    path = tmp_path_factory.mktemp("series")
    write_series(path, synthetic_offsets(seed=21, n=300_000), hr.D_DATE)
    write_series(path, synthetic_offsets(seed=22, n=250_000), hr.F_DATE)
    return path


@pytest.fixture(scope="module")
def frozen(series_dir, tmp_path_factory):
    out = tmp_path_factory.mktemp("bench")
    result = hr.select_run(series_dir, out)
    return out, result


def test_select_loads_only_the_21_sep_series(series_dir, tmp_path):
    only_d = tmp_path / "only_d"
    only_d.mkdir()
    for freq in (1, 5, 10):
        name = f"series_{hr.D_DATE}_{freq}s.csv.gz"
        (only_d / name).write_bytes((series_dir / name).read_bytes())
    result = hr.select_run(only_d, tmp_path / "out")  # would fail if it touched 12 Oct
    assert set(result["horizons"]) == {"60", "10"}
    assert set(result["series_sha256"]) == {hr.D_DATE}


def test_frozen_selection_content(frozen):
    out, result = frozen
    for horizon, info in result["horizons"].items():
        assert info["selected_model"] in hb.MODELS and info["alpha"] in hb.ALPHA_GRID
        assert min(info["validation_mae"].values()) <= info["validation_mae"][info["selected_model"]] <= min(info["validation_mae"].values()) * 1.0100001
        assert info["origins"]["D"] > 0 and info["origins"]["V"] > 0
    saved = json.loads((out / "select" / "selection_frozen.json").read_text(encoding="utf-8"))
    assert saved["code_sha256"] == hr._code_hashes() and set(saved["series_sha256"]) == {hr.D_DATE}
    assert trace.sha256_file(out / "select" / "selection_frozen.json") == result["frozen_file_sha256"]


def test_selection_is_deterministic(series_dir, frozen, tmp_path):
    again = hr.select_run(series_dir, tmp_path / "again")
    _, first = frozen
    for key in ("horizons", "series_sha256", "code_sha256"):  # created_utc differs, so the file hash does too
        assert again[key] == first[key]


def test_selecting_twice_into_the_same_directory_is_refused(series_dir, frozen):
    out, _ = frozen
    with pytest.raises(FileExistsError):
        hr.select_run(series_dir, out)


def test_final_requires_the_preregistered_hash(series_dir, frozen, tmp_path):
    out, result = frozen
    frozen_path = out / "select" / "selection_frozen.json"
    with pytest.raises(IntegrityError, match="SHA-256"):
        hr.final_run(series_dir, frozen_path, "0" * 64, tmp_path / "f")
    assert not (tmp_path / "f").exists()


def test_final_refuses_if_the_code_changed_after_the_freeze(series_dir, frozen, tmp_path):
    out, _ = frozen
    doctored = tmp_path / "doctored.json"
    data = json.loads((out / "select" / "selection_frozen.json").read_text(encoding="utf-8"))
    data["code_sha256"]["horizon_benchmark.py"] = "0" * 64
    doctored.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(IntegrityError, match="code changed"):
        hr.final_run(series_dir, doctored, trace.sha256_file(doctored), tmp_path / "f")


def test_final_loads_only_12_oct_runs_once_and_uses_the_frozen_choice(series_dir, frozen, tmp_path):
    out, result = frozen
    only_f = tmp_path / "only_f"
    only_f.mkdir()
    for freq in (1, 5, 10):
        name = f"series_{hr.F_DATE}_{freq}s.csv.gz"
        (only_f / name).write_bytes((series_dir / name).read_bytes())
    frozen_path = out / "select" / "selection_frozen.json"
    report = hr.final_run(only_f, frozen_path, result["frozen_file_sha256"], tmp_path / "f")  # would fail if it touched 21 Sep
    for horizon, info in report["horizons"].items():
        assert info["selected_model"] == result["horizons"][horizon]["selected_model"]
        assert info["origins_F"] == len(hb.origins("F", int(horizon)))
    assert set(report["series_sha256"]) == {hr.F_DATE}
    produced = {p.name for p in (tmp_path / "f" / "final").iterdir()}
    assert {"final_report.json", "scores_F_H60.csv", "dm_F_H60.csv", "forecasts_F_H60.csv"} <= produced
    with pytest.raises(FileExistsError, match="exactly once"):
        hr.final_run(only_f, frozen_path, result["frozen_file_sha256"], tmp_path / "f")


def test_dm_family_flags_identical_forecasts_and_applies_holm_to_the_rest():
    counts = np.random.default_rng(31).poisson(11, size=43_200).astype(float)
    res = hb.evaluate_pool("D", counts, 60, 0.05)
    family = hr._dm_family(res, "persistence_interval")
    twin = family.set_index("comparator").loc["seasonal_naive_60"]
    assert bool(twin["identical"]) and np.isnan(twin["p_holm"])  # at H = 60 the two are the same forecast
    testable = family[~family["identical"]]
    assert (testable["p_holm"] >= testable["p"] - 1e-12).all() and (testable["p_holm"] <= 1).all()
