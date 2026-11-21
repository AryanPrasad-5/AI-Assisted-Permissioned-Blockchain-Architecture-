"""A0h: boundary statistics for STORED 60 s forecasts (2026-11-21). Checklist items 17-28.

The pure classification and counting code is tested on small synthetic tables. The stored forecast files are READ (never written,
never recomputed) to pin the counts the A0h report states; those counts are re-derived here by a separate, minimal implementation
so the script and the test cannot share a mistake.
"""

from __future__ import annotations

import csv
import json
import math

import pytest

from tests import a0h_support as h
from fabric_perf.controllers import forecast_boundary as fb

import e1_a0h_forecast_boundary as script

V = h.REPO / "experiments/results/flashbots_horizon_benchmark_v2/select/forecasts_V_H60.csv"
F = h.REPO / "experiments/results/flashbots_horizon_benchmark_v2/final/forecasts_F_H60.csv"
STORED_SHA = {V: "99d73e43cf2e7d779b56eaa8027ba6b5d30e6f5b71ffde411331802a8b41ab41",
              F: "5f65ff6e4e152698243e37181f67d154c119fde701c974126e0a0c8d82a7bb0b"}


# ------------------------------------------------------------------------------------------- 17-21 strict boundary rule
@pytest.mark.parametrize("value,expected", [
    (9.999999, fb.BELOW), (0.0, fb.BELOW), (7.5, fb.BELOW),                           # 17 below 10
    (10, fb.INSIDE), (10.0, fb.INSIDE),                                              # 18 exactly 10 is inside
    (10.000001, fb.INSIDE), (55.0, fb.INSIDE), (199.999999, fb.INSIDE),              # 19 within the grid
    (200, fb.INSIDE), (200.0, fb.INSIDE),                                            # 20 exactly 200 is inside
    (200.000001, fb.ABOVE), (1e9, fb.ABOVE)])                                        # 21 above 200
def test_17_to_21_the_boundary_rule_is_strict(value, expected):
    assert fb.classify_forecast(value) == expected


# ---------------------------------------------------------------------------------------------- 22-23 missing and invalid
@pytest.mark.parametrize("value", [None, "", "  ", "abc", float("nan"), float("inf"), float("-inf"), -0.5, True, False, [], {}])
def test_22_23_missing_blank_nan_infinite_negative_and_non_numeric_values_are_invalid(value):
    assert fb.classify_forecast(value) == fb.INVALID


def test_22b_numbers_stored_as_text_are_read_as_numbers_and_invalid_values_leave_every_denominator():
    result = fb.summarize_forecasts(["9.5", "10", 12, "200.5", "", None, "x", float("nan")])
    assert result["eligible_forecasts"] == 8 and result["valid"] == 4 and result["invalid_or_missing"] == 4
    assert (result["below_grid"], result["inside_grid"], result["above_grid"]) == (1, 2, 1)
    assert result["below_grid_pct_of_valid"] == 25.0 and result["inside_grid_pct_of_valid"] == 50.0


# ----------------------------------------------------------------------------------------- 27 counts and percentages
def test_27_counts_and_percentages_are_exact_on_a_hand_checked_table():
    values = [8, 9.99, 10, 10, 15, 200, 200.01, 300, 11, 12]
    result = fb.summarize_forecasts(values, origins_s=[600 + 60 * i for i in range(10)])
    assert (result["below_grid"], result["inside_grid"], result["above_grid"], result["valid"]) == (2, 6, 2, 10)
    assert math.isclose(result["below_grid_pct_of_valid"], 20.0) and math.isclose(result["inside_grid_pct_of_valid"], 60.0)
    assert math.isclose(result["above_grid_pct_of_valid"], 20.0)
    assert result["near_lower_boundary"] == {"below_by_at_most_1_tps": 1, "inside_by_at_most_1_tps": 3, "exactly_at_low": 2, "exactly_at_high": 1}   # 10, 10 and 11 lie within 1 tx/s above
    assert result["below_grid_runs"] == 1 and result["longest_below_grid_run_forecasts"] == 2
    assert result["distribution_tps"]["min"] == 8.0 and result["distribution_tps"]["max"] == 300.0
    assert fb.summarize_forecasts([])["below_grid_pct_of_valid"] is None and fb.summarize_forecasts([])["valid"] == 0


def test_27b_hourly_breakdown_places_below_grid_forecasts_in_their_utc_hour():
    origins = [600, 3500, 3600, 7199, 7200]
    result = fb.summarize_forecasts([5, 12, 5, 5, 20], origins_s=origins)
    assert result["by_utc_hour"] == [{"utc_hour": 0, "valid_forecasts": 2, "below_grid": 1},
                                     {"utc_hour": 1, "valid_forecasts": 2, "below_grid": 2},
                                     {"utc_hour": 2, "valid_forecasts": 1, "below_grid": 0}]
    assert result["utc_hours_with_below_grid"] == [0, 1]
    with pytest.raises(ValueError):
        fb.summarize_forecasts([1, 2], origins_s=[1])


# ------------------------------------------------------------------------------------------------- 24 horizons
def table(rows):
    return ["origin_s", "target_end_s", "yhat_ewma"], [{"origin_s": str(o), "target_end_s": str(e), "yhat_ewma": str(v)} for o, e, v in rows]


def test_24_a_table_with_another_horizon_or_too_little_history_is_not_eligible():
    ok = table([(600, 660, 12), (660, 720, 13)])
    assert fb.eligibility_problems(*ok, forecast_column="yhat_ewma", horizon_s=60, min_origin_s=600) == []
    assert "10.0 s, not 60 s" in fb.eligibility_problems(*table([(600, 610, 12)]), forecast_column="yhat_ewma", horizon_s=60, min_origin_s=600)[0]
    assert "less than 600" in fb.eligibility_problems(*table([(300, 360, 12)]), forecast_column="yhat_ewma", horizon_s=60, min_origin_s=600)[0]
    assert "strictly increase" in fb.eligibility_problems(*table([(600, 660, 1), (600, 660, 1)]), forecast_column="yhat_ewma", horizon_s=60, min_origin_s=600)[0]
    assert fb.eligibility_problems(["origin_s", "target_end_s"], [], forecast_column="yhat_ewma", horizon_s=60, min_origin_s=600) == ["column 'yhat_ewma' is absent"]


# ------------------------------------------------------------------- 25 mixed provenance, 26 none eligible, 28 reproducibility
def read_values(path):
    with path.open(newline="", encoding="utf-8") as stream:
        return [row["yhat_ewma"] for row in csv.DictReader(stream)]


def independent_counts(path):
    """A separate minimal implementation: plain comparisons on the stored text, no module code."""
    below = inside = above = 0
    for text in read_values(path):
        x = float(text)
        if x < 10:
            below += 1
        elif x > 200:
            above += 1
        else:
            inside += 1
    return below, inside, above


@pytest.fixture(scope="module")
def report():
    return script.analyse()


def test_25_only_the_eligible_frozen_ewma_horizon_60_arrival_tables_are_counted(report):
    counted = {s["path"] for s in report["segments"]}
    assert counted == {"experiments/results/flashbots_horizon_benchmark_v2/select/forecasts_V_H60.csv",
                       "experiments/results/flashbots_horizon_benchmark_v2/final/forecasts_F_H60.csv"}
    excluded = {e["path"]: e["reason"] for e in report["excluded"]}
    assert all(e["reason"] for e in report["excluded"])
    assert any("wrong horizon" in r for r in excluded.values()) and any("wrong target" in r for r in excluded.values())
    assert any("HFBTP held-out performance predictions" in r for r in excluded.values())
    assert not counted & set(excluded)
    assert [c["treatment"].startswith("NOT pooled") for c in report["cross_checks"]] == [True, True]
    assert all(c["max_abs_difference_tps_on_shared_origins"] == 0.0 and c["eligibility_problems"] == [] for c in report["cross_checks"])
    assert report["pooled_descriptive"]["segments"] == ["v2_V_H60", "v2_F_H60"]


def test_26_no_eligible_forecasts_is_stated_not_invented(monkeypatch, tmp_path):
    bad = tmp_path / "wrong_horizon.csv"
    bad.write_text("origin_s,target_end_s,y_observed,yhat_ewma\n600,610,10,5\n610,620,10,6\n", encoding="utf-8")
    monkeypatch.setattr(script, "PRIMARY", (("tmp_H10", bad, "2026-09-21", "synthetic", "v2"),))
    result = script.analyse()
    assert result["verdict"] == "BOUNDARY FREQUENCY NOT ESTABLISHED — NO ELIGIBLE STORED FORECASTS"
    assert result["segments"] == [] and result["eligible_rows_total"] == 0 and result["pooled_descriptive"]["below_grid"] == 0
    assert any("not 60 s" in e["reason"] for e in result["excluded"]) and any(e["path"].endswith("wrong_horizon.csv") for e in result["excluded"])
    assert script.main([]) == 1                                           # the CLI exits non-zero when nothing is eligible


def test_26b_a_failed_contract_check_establishes_nothing(monkeypatch):
    monkeypatch.setattr(script, "contract_problems", lambda: (["frozen selection is not EWMA alpha 0.005"], {}))
    result = script.analyse()
    assert result["verdict"].startswith("BOUNDARY FREQUENCY NOT ESTABLISHED") and result["segments"] == []


def test_27c_the_stored_counts_equal_an_independent_recount(report):
    for entry, path in zip(report["segments"], (V, F)):
        stats = entry["forecast_stats"]
        assert (stats["below_grid"], stats["inside_grid"], stats["above_grid"]) == independent_counts(path), path.name
        assert stats["valid"] == stats["eligible_forecasts"] == entry["rows_in_file"] and stats["invalid_or_missing"] == 0
    pooled = report["pooled_descriptive"]
    totals = [sum(c) for c in zip(independent_counts(V), independent_counts(F))]
    assert [pooled["below_grid"], pooled["inside_grid"], pooled["above_grid"]] == totals
    assert math.isclose(pooled["below_grid_pct_of_valid"], 100 * totals[0] / sum(totals))


def test_27d_the_counts_stated_in_the_report_are_these(report):
    v, f = (s["forecast_stats"] for s in report["segments"])
    assert (v["eligible_forecasts"], v["below_grid"], v["inside_grid"], v["above_grid"]) == (720, 59, 661, 0)
    assert (f["eligible_forecasts"], f["below_grid"], f["inside_grid"], f["above_grid"]) == (1430, 469, 961, 0)
    p = report["pooled_descriptive"]
    assert (p["eligible_forecasts"], p["below_grid"], p["inside_grid"], p["above_grid"]) == (2150, 528, 1622, 0)
    assert report["eligible_rows_total"] == 2150 and report["excluded_rows_total"] == 301_575 and len(report["excluded"]) == 18
    assert "established" in report["verdict"].lower() and "proxy" in report["verdict"]


def test_28_source_selection_is_reproducible_and_the_inputs_are_the_pinned_files(report):
    assert json.dumps(script.analyse(), sort_keys=True) == json.dumps(report, sort_keys=True)
    assert {s["path"].rsplit("/", 1)[-1]: s["sha256"] for s in report["segments"]} == {
        "forecasts_V_H60.csv": STORED_SHA[V], "forecasts_F_H60.csv": STORED_SHA[F]}
    assert report["contract"]["v2_h60"]["alpha"] == 0.005 and report["contract_problems"] == []
    assert report["computation_performed"].startswith("none")


def test_the_script_writes_only_a_new_file_and_never_overwrites(tmp_path, capsys):
    out = tmp_path / "r.json"
    assert script.main(["--out", str(out)]) == 0 and json.loads(out.read_text(encoding="utf-8"))["segments"]
    before = out.read_bytes()
    assert script.main(["--out", str(out)]) == 2 and out.read_bytes() == before
    capsys.readouterr()
