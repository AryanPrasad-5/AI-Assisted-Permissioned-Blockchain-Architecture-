"""Pre-registered X1 rules on synthetic measurement worlds. No process, no Fabric, no data file."""
import pytest

from Fabric_Live_Testbed.closed_loop_v2 import protocol as P2
from Fabric_Live_Testbed.closed_loop_x1 import protocol_x1 as X
from Fabric_Live_Testbed.closed_loop_x1.tests.worlds import NOISE, RANKED, RATES, physics, row, win, world


# ------------------------------------------------------------------------------------------------ constants
def test_shared_constants_equal_the_x0_values_so_x1_uses_the_same_validity_and_effect_size_rules():
    for name in ("CLIENT_LAG_P99_MS", "FAILED_FRACTION_LIMIT", "SESOI_REL", "SESOI_FLOOR_MS", "CONFIRM_RUNGS"):
        assert getattr(X, name) == getattr(P2, name.replace("CONFIRM_RUNGS", "CONFIRM_RUNGS")), name
    assert X.sesoi_ms(100, 400) == P2.sesoi_ms(100, 400) == 25.0 and X.sesoi_ms(1000, 2000) == 100.0


def test_design_constants():
    assert X.CONFIGS == (2, 5, 10) and X.BASELINE_B in X.CONFIGS and list(X.RUNG_RATES_TPS) == sorted(X.RUNG_RATES_TPS)
    assert max(X.RUNG_RATES_TPS) == 60 and X.MIN_VALID_REPS == 2 and X.REPLICATES_PER_B == 3


# ------------------------------------------------------------------------------------------------ classification
@pytest.mark.parametrize("kw,want", [
    ({}, "OK"),
    ({"short": 1}, "INCOMPLETE"),
    ({"lag": 100.1}, "CLIENT_LIMITED"), ({"lag": 100.0}, "OK"), ({"lag": None}, "CLIENT_LIMITED"), ({"overflow": 1}, "CLIENT_LIMITED"),
    ({"fail": 40}, "FABRIC_SATURATED"),                                      # 40/600 = 6.7% failed
    ({"saturated": True}, "FABRIC_SATURATED"),                               # delivered ratio 0.8
    ({"saturated": True, "lag": 500.0}, "CLIENT_LIMITED"),                   # the driver being late outranks saturation (cannot attribute)
])
def test_round_classification(kw, want):
    assert X.classify_round(row(10, 10, 1, 100.0, **kw)) == want


def test_failed_fraction_limit_is_exactly_half_a_percent():
    r = row(10, 10, 1, 100.0, fail=3)                                         # 3/600 = 0.5% -> not above the limit
    assert X.classify_round(r) == "OK"
    assert X.classify_round(row(10, 10, 1, 100.0, fail=4)) == "FABRIC_SATURATED"


def test_delivered_ratio_threshold():
    r = row(10, 10, 1, 100.0)
    r["round_wall_s"] = 60.0 / 0.95 + 0.01
    assert X.classify_round(r) == "FABRIC_SATURATED"
    r["round_wall_s"] = 60.0 / 0.95 - 0.01
    assert X.classify_round(r) == "OK"


# ------------------------------------------------------------------------------------------------ rung table and validity
def test_one_client_limited_replicate_is_dropped_and_the_cell_stays_valid():
    rows = [row(5, 10, 1, 480.0), row(5, 10, 2, 484.0, lag=300.0), row(5, 10, 3, 486.0)]
    c = X.build_rung_table(rows)[0]
    assert c["cell_valid"] and c["n_valid"] == 2 and c["p95_reps"] == [480.0, 486.0] and c["p95_ms"] == 483.0


def test_two_client_limited_replicates_invalidate_the_cell():
    rows = [row(5, 10, 1, 480.0), row(5, 10, 2, 484.0, lag=300.0), row(5, 10, 3, 486.0, lag=300.0)]
    assert not X.build_rung_table(rows)[0]["cell_valid"]


def test_rung_status_distinguishes_ambiguity_from_client_limit_and_caps_r_max_from_below():
    rows = world()
    for r in rows:                                                           # rung 20: only B=2 client-limited -> ambiguous
        if r["rate_tps"] == 20.0 and r["B"] == 2:
            r["lag_p99_ms"] = 400.0
    t = X.build_rung_table(rows)
    s = X.rung_status(t)
    assert s[20.0] == "CO_LOCATION_AMBIGUOUS" and s[15.0] == "VALID" and s[25.0] == "VALID"
    a = X.analyse_rungs(t)
    assert a["r_max"] == 15.0 and a["usable_rates"] == [5.0, 10.0, 15.0]     # contiguous from below, nothing above the excluded rung
    rows = world()
    for r in rows:
        if r["rate_tps"] == 20.0:
            r["lag_p99_ms"] = 400.0
    assert X.rung_status(X.build_rung_table(rows))[20.0] == "CLIENT_LIMITED"


def test_missing_cell_marks_the_rung_missing():
    rows = [r for r in world() if not (r["B"] == 5 and r["rate_tps"] == 30.0)]
    assert X.rung_status(X.build_rung_table(rows))[30.0] == "MISSING"


def test_no_valid_lowest_rung_is_not_established():
    rows = world()
    for r in rows:
        if r["rate_tps"] == 5.0:
            r["lag_p99_ms"] = 400.0
    d = X.decide(rows, RANKED)
    assert d["status"] == "STOP_NOT_ESTABLISHED" and d["rungs"]["r_max"] is None


# ------------------------------------------------------------------------------------------------ pairwise rule
def cell(p95s, sat=False):
    return {"p95_reps": p95s, "p95_ms": sum(p95s) / len(p95s), "saturated": sat}


def test_pairwise_needs_both_the_sesoi_and_non_overlapping_replicates():
    assert X.compare(cell([100, 101, 102]), cell([200, 201, 202])) == "A"
    assert X.compare(cell([200, 201, 202]), cell([100, 101, 102])) == "B"
    assert X.compare(cell([100, 101, 102]), cell([120, 121, 122])) == "TIE"            # 20 ms < the 25 ms floor
    assert X.compare(cell([100, 190, 102]), cell([180, 181, 182])) == "TIE"            # mean gap > SESOI but replicates overlap
    assert X.compare(cell([180, 181, 182]), cell([100, 190, 102])) == "TIE"            # the mirrored case: overlap blocks the verdict either way
    assert X.compare(cell([1000, 1001]), cell([1090, 1091])) == "TIE"                  # 90 < 10% of 1000
    assert X.compare(cell([1000, 1001]), cell([1110, 1111])) == "A"


def test_saturated_configuration_loses_to_an_unsaturated_one_regardless_of_latency():
    assert X.compare(cell([50, 51], sat=True), cell([400, 401])) == "B"
    assert X.compare(cell([400, 401]), cell([50, 51], sat=True)) == "A"
    assert X.compare(cell([50, 51], sat=True), cell([400, 401], sat=True)) == "TIE"


# ------------------------------------------------------------------------------------------------ rung analysis on the physics world
def test_physics_world_gives_two_regimes_b2_then_b5_and_a_transition_between_30_and_40():
    a = X.analyse_rungs(X.build_rung_table(world()))
    assert a["r_max"] == 60.0 and a["ordering_reversal"] and a["regime_configs"] == [2, 5]
    assert a["regimes"][2] == [5.0, 10.0, 15.0, 20.0, 25.0, 30.0] and a["regimes"][5] == [35.0, 40.0, 50.0, 60.0]
    assert a["transition_rate"] == pytest.approx((30 * 35) ** 0.5, abs=0.01)
    assert 10 not in a["regimes"]                                                 # B=10 is never decisively best


def test_a_single_dominant_configuration_is_no_reversal():
    a = X.analyse_rungs(X.build_rung_table(world(block_limit=None)))             # B=2 never saturates
    assert a["regime_configs"] == [2] and not a["ordering_reversal"] and a["transition_rate"] is None


def test_regime_needs_two_consecutive_rungs():
    rows = world()
    for r in rows:                                                                # make B=5 decisive at ONE rung only: break rung 40 and 50
        if r["B"] == 5 and r["rate_tps"] in (40.0, 60.0):
            r["p95_ms"] += 800.0
    a = X.analyse_rungs(X.build_rung_table(rows))
    assert 5 not in a["regimes"] or a["regimes"][5] != [35.0, 40.0, 50.0, 60.0]


# ------------------------------------------------------------------------------------------------ window and k
def test_choose_k_formula_and_straddle_rule():
    m = RANKED[1]["segment_rates_tps"]
    res = X.choose_k(m, 34.2, 60.0)
    assert res["k"] == pytest.approx(min(34.2 / (min(m) * max(m)) ** 0.5, 0.9 * 60 / max(m)), abs=0.001) and res["feasible"]
    assert X.choose_k(RANKED[0]["segment_rates_tps"], 34.2, 60.0)["feasible"] is False      # only one scaled segment above the transition
    low = X.choose_k([30.0] * 10, 20.0, 60.0)
    assert not low["feasible"] and "below 1" in low["infeasible_reason"]


def test_window_selection_uses_the_first_structurally_feasible_ranked_window():
    sel = X.select_window(RANKED, 34.2, 60.0)
    assert sel["status"] == "WINDOW_AND_K_SELECTED" and sel["window"]["rank"] == 2 and [t["feasible"] for t in sel["tried"]] == [False, True]
    assert X.select_window([RANKED[0]], 34.2, 60.0)["status"] == "NO_FEASIBLE_WINDOW"


def test_selected_window_does_not_depend_on_how_large_the_headroom_is():
    d1 = X.decide(world(), RANKED)
    rows = world()
    for r in rows:
        if r["B"] == 2 and r["rate_tps"] <= 30.0:
            r["p95_ms"] *= 0.5                                                    # a much larger headroom at the low rates
    d2 = X.decide(rows, RANKED)
    assert d1["window_selection"]["window"]["start_s"] == d2["window_selection"]["window"]["start_s"] == 25200
    assert d1["selected_window"]["k"] == d2["selected_window"]["k"]


# ------------------------------------------------------------------------------------------------ the decision chain
def test_physics_world_establishes_headroom_with_menu_2_and_5():
    d = X.decide(world(), RANKED)
    assert d["status"] == "GO_LIVE_DESIGN" and d["reason"] == "HEADROOM_ESTABLISHED" and d["menu"] == [2, 5]
    ev = d["selected_evaluation"]
    assert ev["coverage"] == 1.0 and ev["tuned_static_B"] == 5
    assert ev["headroom_oracle"] >= X.HEADROOM_ORACLE_MIN and ev["segments_with_sesoi_gain"] >= X.MIN_GAIN_SEGMENTS
    assert ev["oracle_saturated_segments"] == 0 and d["gate_failures"] == []
    assert len(d["leave_one_replicate_out"]) == 3 and all(e["coverage"] == 1.0 for e in d["leave_one_replicate_out"])
    assert d["selected_window"]["requires_rate_scaling"] is True


def test_dominant_small_block_stops_with_no_ordering_reversal_and_reports_descriptive_headroom():
    d = X.decide(world(block_limit=None), RANKED)
    assert d["status"] == "STOP_NO_HEADROOM" and d["reason"] == "NO_ORDERING_REVERSAL" and "B=2" in d["meaning"]
    assert d["descriptive_workloads"]["all_eligible_windows_k1"]["tuned_static_B"] == 2
    assert d["descriptive_workloads"]["all_eligible_windows_k1"]["headroom_oracle"] == pytest.approx(0.0, abs=1e-9)


def test_reversal_without_enough_headroom_stops():
    def flat(rate, b):
        sat = b == 2 and rate > 30
        return {2: 360.0, 5: 400.0, 10: 800.0}[b], sat                           # B=2 better by 40 ms (decisive) below 30 tx/s, saturated above

    d = X.decide(world(flat), RANKED)
    assert d["rungs"]["ordering_reversal"]
    assert d["status"] == "STOP_NO_HEADROOM" and d["reason"] == "HEADROOM_GATE_NOT_MET"
    assert any("oracle headroom" in f for f in d["gate_failures"]) and d["selected_evaluation"]["headroom_oracle"] < X.HEADROOM_ORACLE_MIN


def test_no_feasible_window_stops():
    d = X.decide(world(), [win(1, 1, [30.0] * 10)])
    assert d["status"] == "STOP_NO_HEADROOM" and d["reason"] == "NO_FEASIBLE_WINDOW"


def test_gate_requires_every_leave_one_replicate_out_evaluation_to_pass():
    d = X.decide(world(), RANKED)
    ev, loo = d["selected_evaluation"], [dict(e) for e in d["leave_one_replicate_out"]]
    assert X.gate_failures(ev, loo) == []
    loo[1]["headroom_oracle"], loo[1]["headroom_oracle_ms"] = 0.02, 3.0
    f = X.gate_failures(ev, loo)
    assert len(f) == 1 and f[0].startswith(f"leave-out-{loo[1]['left_out']}")
    loo[1]["coverage"] = 0.6
    assert any("not fully covered" in x for x in X.gate_failures(ev, loo))


def test_gate_failure_when_the_oracle_itself_saturates_or_the_menu_is_a_single_configuration():
    d = X.decide(world(), RANKED)
    ev = dict(d["selected_evaluation"])
    assert any("saturated" in x for x in X.gate_failures({**ev, "oracle_saturated_segments": 2}, []))
    assert any("menu" in x for x in X.gate_failures({**ev, "menu": [5]}, []))
    assert any("segments gain" in x for x in X.gate_failures({**ev, "segments_with_sesoi_gain": 1}, []))
    assert X.gate_failures({**ev, "headroom_persistence": -5.0, "headroom_persistence_ms": -900.0}, []) == []     # persistence never gates


def test_surface_interpolates_in_log_rate_and_refuses_outside_the_measured_range():
    t = X.build_rung_table(world(rates=(10, 40), configs=X.CONFIGS))
    s = X.Surface(t, [10.0, 40.0])
    p10, p40 = s.at(5, 10.0)[0], s.at(5, 40.0)[0]
    mid = s.at(5, 20.0)[0]                                                         # log2(20/10)/log2(40/10) = 0.5
    assert mid == pytest.approx((p10 + p40) / 2, rel=1e-9)
    assert s.at(5, 9.0) is None and s.at(5, 41.0) is None
    ts = X.build_rung_table(world(rates=(30, 40), configs=X.CONFIGS))              # B=2 saturated at 40 only: the interval counts as saturated
    ss = X.Surface(ts, [30.0, 40.0])
    assert ss.at(2, 35.0)[2] is True and ss.at(2, 30.0)[2] is False


def test_oracle_never_picks_a_saturated_configuration_when_an_unsaturated_one_exists():
    t = X.build_rung_table(world())
    ev = X.evaluate_workload(X.Surface(t, [float(x) for x in RATES]), [50.0, 55.0, 20.0])
    assert ev["oracle_choice"] == [5, 5, 2] and ev["oracle_saturated_segments"] == 0
    assert ev["static"][2]["saturated_segments"] == 2 and ev["tuned_static_B"] in (5, 10)


def test_persistence_policy_uses_the_previous_minutes_rate_not_the_current_one():
    t = X.build_rung_table(world())
    ev = X.evaluate_workload(X.Surface(t, [float(x) for x in RATES]), [20.0, 50.0, 20.0, 50.0], preroll_rate=20.0)
    assert ev["oracle_choice"] == [2, 5, 2, 5] and ev["persistence_choice"] == [2, 2, 5, 2]
    assert ev["headroom_persistence"] < ev["headroom_oracle"]


LPC_JSON = __import__("pathlib").Path("D:/PLAN_BLOCK18/live_predictive_control_2026-11-25/protocol/PROTOCOL_LPC_v1.json")


@pytest.mark.skipif(not LPC_JSON.exists(), reason="frozen LPC-v1 protocol file not on this machine")
def test_predicted_world_on_the_real_frozen_ranking_goes_and_reports_the_persistence_blunder_as_a_diagnostic():
    """Model-based dry run on the PRE-REGISTERED predictions (not a measurement): window 25200 at k=2.59 starts after a 28.7 tx/s pre-roll but its first
    segment is 45 tx/s, past B=2's saturation cliff, so the no-skill persistence rule picks B=2 and saturates on segment 0. That is reported, not gated."""
    from Fabric_Live_Testbed.closed_loop_x1.decide_x1 import load_ranking
    ranked, _ = load_ranking(LPC_JSON)
    d = X.decide(world(), ranked)
    ev = d["selected_evaluation"]
    assert d["status"] == "GO_LIVE_DESIGN" and d["menu"] == [2, 5] and d["selected_window"]["start_s"] == 25200 and d["selected_window"]["k"] == pytest.approx(2.59, abs=0.01)
    assert ev["headroom_oracle"] == pytest.approx(0.205, abs=0.01) and ev["segments_with_sesoi_gain"] == 3
    assert ev["persistence_saturated_segments"] == 1 and ev["headroom_persistence"] < 0
