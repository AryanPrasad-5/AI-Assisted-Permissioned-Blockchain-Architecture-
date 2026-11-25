"""Pure rules of protocol M2-v1: design from X1, reactive thresholds, window/k, comparison verdicts. No I/O except the optional real X1 file."""
import json
from pathlib import Path

import pytest

from Fabric_Live_Testbed.closed_loop_m2 import protocol_m2 as P

X1_REAL = Path(r"D:\PLAN_BLOCK18\x1_block_size_headroom_2026-11-25\x1_decision_primary_cb01-06\x1_decision.json")
RATES = (5, 10, 15, 20, 25, 30, 35, 40, 50, 60)


def x1_cells(valid=True):
    """Model of the X1 pre-registration (p95 = 900 B / rate + 24 ms); delivered ratio 0.998."""
    cells = []
    for b in (2, 5, 10):
        for r in RATES:
            mean_ms = 0.5 * (900 * b / r) + 24 * 0.5 + 10
            cells.append({"B": b, "rate_tps": float(r), "n_reps": 2, "n_valid": 2, "cell_valid": valid, "saturated": False, "mean_ms": mean_ms,
                          "p95_ms": 900 * b / r + 24, "delivered_ratio": 0.998})
    return cells


def x1_doc(**over):
    doc = {"status": "STOP_NO_HEADROOM", "inputs": {"runs_with_rows": [f"cb{i:02d}" for i in range(1, 7)]}, "rungs": {"r_max": 60.0},
           "rung_table": x1_cells()}
    doc.update(over)
    return doc


RANKED = [{"rank": 1, "start_s": 20400, "segment_rates_tps": [13.6, 12.6, 9.3, 16.4, 25.6, 8.8, 7.1, 10.5, 12.5, 8.9], "preroll_rate_tps": 11.9,
           "warmup_rates_tps": [10.0] * 10},
          {"rank": 2, "start_s": 25200, "segment_rates_tps": [17.4, 16.8, 17.1, 15.6, 13.6, 13.8, 13.0, 9.0, 9.4, 10.7], "preroll_rate_tps": 11.1,
           "warmup_rates_tps": [10.0] * 10}]


def test_menu_arms_and_initial_conditions_are_the_declared_ones():
    assert P.MENU_B == (2, 5, 10) and P.PRIMARY_ARMS == ("static_b2", "reactive", "predictive_live") and P.INTERPRETIVE_ARMS == ("static_b10",)
    assert {P.ARM_INITIAL_B[a] for a in P.PRIMARY_ARMS} == {2}                 # equivalent initial condition for the three compared controllers
    assert P.ARM_INITIAL_B["static_b10"] == 10
    assert all(P.ARM_INITIAL_B[a] in P.MENU_B for a in P.ARMS)
    assert P.FORECAST["alpha"] == 0.005 and P.FORECAST["horizon_s"] == 60 and P.FORECAST["min_history_s"] == 600
    assert P.PREDICTIVE["cooldown_samples"] == 3 and P.PREDICTIVE["minimum_utility_improvement"] == 0.0     # repo defaults, as in LPC-v1


def test_design_from_x1_surface_reactive_window_and_k():
    d = P.design_from_x1(x1_doc(), RANKED)
    assert d["r_max_tps"] == 60.0 and d["window"]["start_s"] == 20400
    assert d["k"] == pytest.approx(0.9 * 60 / 25.6, abs=1e-3) and d["window_selection"]["detail"]["scaled_max_tps"] == pytest.approx(54.0, abs=0.1)
    assert {p["B"] for p in d["surface_points"]} == {2, 5, 10} and len(d["surface_points"]) == 30
    hi = max(c["mean_ms"] for c in x1_cells() if c["B"] == 2)
    assert d["reactive"]["latency_high_ms"] == pytest.approx(2.0 * hi, abs=1e-3)
    assert d["reactive"]["latency_low_ms"] == pytest.approx(0.8 * 2.0 * hi, abs=1e-3)
    assert d["reactive"]["default_B"] == 2 and d["reactive"]["congested_B"] == 5 and d["reactive"]["cooldown_samples"] == 0
    assert d["reactive"]["consecutive_samples"] == 2


def test_design_refuses_anything_but_the_primary_x1_set_and_missing_r_max():
    with pytest.raises(ValueError, match="PRIMARY"):
        P.design_from_x1(x1_doc(inputs={"runs_with_rows": [f"cb{i:02d}" for i in range(1, 8)]}), RANKED)
    with pytest.raises(ValueError, match="r_max"):
        P.design_from_x1(x1_doc(rungs={"r_max": None}), RANKED)
    with pytest.raises(ValueError, match="fewer than 2"):
        P.design_from_x1(x1_doc(rung_table=[c for c in x1_cells() if not (c["B"] == 5 and c["rate_tps"] > 5)]), RANKED)


def test_saturated_or_invalid_x1_cells_never_enter_the_surface_or_the_reactive_threshold():
    cells = x1_cells()
    for c in cells:
        if c["B"] == 2 and c["rate_tps"] == 5.0:
            c["saturated"] = True
    pts = P.surface_points_from_x1(cells, 60.0)
    assert not any(p["B"] == 2 and p["rate_tps"] == 5.0 for p in pts)
    assert P.reactive_params_from_x1(cells, 60.0)["max_healthy_default_mean_ms"] < max(c["mean_ms"] for c in cells if c["B"] == 2)


def test_k_rule_is_the_largest_scale_inside_the_client_valid_range_and_falls_back_deterministically():
    r = P.choose_k([10.0, 20.0], 60.0)
    assert r["k"] == pytest.approx(2.7, abs=1e-3) and r["feasible"] and r["scaled_max_tps"] <= 0.9 * 60 + 1e-6
    bad = P.choose_k([1.0, 50.0], 60.0)                 # scaled minimum would be 1.08 tx/s: below the measured surface
    assert not bad["feasible"] and "below the lowest" in bad["infeasible_reason"]
    ranked = [{"rank": 1, "start_s": 1, "segment_rates_tps": [1.0, 50.0]}, {"rank": 2, "start_s": 2, "segment_rates_tps": [10.0, 20.0]}]
    sel = P.select_window(ranked, 60.0)
    assert sel["status"] == "WINDOW_AND_K_SELECTED" and sel["window"]["start_s"] == 2 and [t["feasible"] for t in sel["tried"]] == [False, True]
    assert P.select_window(ranked[:1], 60.0)["status"] == "NO_FEASIBLE_WINDOW"


def test_compare_values_meaningful_equivalent_and_not_distinguishable():
    a, b = [100.0, 101.0, 99.0], [60.0, 61.0, 59.0]
    r = P.compare_values(a, b, 25.0, lower_is_better=True)
    assert r["verdict"] == "MEANINGFUL_A_WORSE" and r["exact_permutation_p_two_sided"] == pytest.approx(0.1)   # 3 v 3: the smallest possible p
    assert P.compare_values(b, a, 25.0, lower_is_better=True)["verdict"] == "MEANINGFUL_B_WORSE"
    assert P.compare_values([60.0, 61.0, 59.0], [62.0, 60.5, 61.5], 25.0, lower_is_better=True)["verdict"] == "EQUIVALENT_WITHIN_SESOI"
    # mean gap above the SESOI but one run overlaps: not complete separation -> not distinguishable
    r2 = P.compare_values([100.0, 101.0, 40.0], [60.0, 61.0, 59.0], 25.0, lower_is_better=True)
    assert r2["verdict"] == "NOT_DISTINGUISHABLE" or r2["verdict"].startswith("EQUIVALENT")
    assert P.compare_values([], b, 25.0, lower_is_better=True)["verdict"] == "NOT_ASSESSABLE"
    # throughput: higher is better, the smaller arm is the worse one
    t = P.compare_values([10.0, 10.1, 9.9], [12.0, 12.1, 11.9], 0.24, lower_is_better=False)
    assert t["verdict"] == "MEANINGFUL_A_WORSE"


def test_sesoi_is_the_x0_x1_rule():
    assert P.sesoi_latency_ms(100.0, 300.0) == 25.0 and P.sesoi_latency_ms(500.0, 400.0) == 40.0


def test_protocol_doc_is_json_serializable_and_stable():
    d1, d2 = json.dumps(P.protocol_doc(), sort_keys=True), json.dumps(P.protocol_doc(), sort_keys=True)
    assert d1 == d2 and "M2-v1" in d1


@pytest.mark.skipif(not X1_REAL.is_file(), reason="X1 primary decision file not on this machine")
def test_real_x1_primary_decision_gives_the_documented_design():
    from Fabric_Live_Testbed.closed_loop_v2.window_selection import load_ranking
    ranked, _ = load_ranking()
    d = P.design_from_x1(json.loads(X1_REAL.read_text()), ranked)
    assert d["r_max_tps"] == 60.0 and d["window"]["start_s"] == 20400 and d["k"] == pytest.approx(2.114, abs=1e-3)
    assert d["reactive"]["latency_high_ms"] == pytest.approx(2 * 121.545, abs=0.01)
    assert {p["B"] for p in d["surface_points"]} == {2, 5, 10}
