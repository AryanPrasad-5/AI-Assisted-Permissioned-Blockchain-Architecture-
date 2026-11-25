"""Unit tests of the M2 analysis on hand-made records (no Fabric, no stage): validity classes, comparable set, direction of each metric,
meaningful-difference verdicts, replication floor, decision table."""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Fabric_Live_Testbed.closed_loop_m2 import analysis_m2 as A  # noqa: E402
from Fabric_Live_Testbed.closed_loop_m2 import protocol_m2 as P  # noqa: E402


def rec(arm, rep, *, p95=100.0, mean=60.0, thr=10.0, failed=0, comparable=True, valid=True, targets=None, changes=0, abstained=0):
    tc = targets if targets is not None else ({"2": 10} if arm != "static_b10" else {"10": 10})
    return {"run_id": f"{arm}-{rep}", "arm_label": arm, "replicate": rep, "valid_run": valid, "comparable": comparable, "p95_ms": p95, "mean_ms": mean,
            "p99_ms": p95 * 1.2, "throughput_tps": thr, "delivered_ratio": 0.998, "failure_rate": failed / 6000, "failed": failed, "success_rate": 1 - failed / 6000,
            "config_changes": changes, "decisions_changed": changes, "n_decisions": 10, "decisions_abstained": abstained, "target_counts": tc,
            "B_by_segment": [2] * 10, "segments": [], "forecast": {}}


def trio(rep_values: dict, n=3, **over):
    """rep_values: arm -> dict of per-arm record kwargs lists or scalars."""
    out = []
    for arm in P.ARMS:
        kw = rep_values.get(arm, {})
        for i in range(n):
            vals = {k: (v[i] if isinstance(v, list) else v) for k, v in kw.items()}
            out.append(rec(arm, i + 1, **vals))
    return out


# ------------------------------------------------------------------------------------------------------------------ validity
def arm_doc():
    return {"run_id": "cc01", "arm_label": "static_b2", "arm": "static", "replicate": 1, "initial_B": 2,
            "rounds": [{"index": 0, "n_tx": 600, "length_ms": 60000}, {"index": 1, "n_tx": 600, "length_ms": 60000}]}


def analysis_doc(lag_p99=5.0, rows=600, failed=0, problems=None):
    rnd = {"round": 1, "measured": True, "segment": 0, "scheduled": 600, "rows": rows, "success": rows - failed, "failed": failed, "offered_tps": 10.0,
           "latency_ms": {"mean": 60.0, "p95": 90.0}, "pending_at_deactivate": 0, "committed_under_B": {"2": 600},
           "dispatch": {"present": True, "lag_ms": {"p99": lag_p99}, "overflow_callbacks": 0}}
    return {"run_level_measured": {"scheduled_measured": 600, "success": rows - failed, "failed": failed, "throughput_tps_over_round_walls": 9.9,
                                   "offered_tps": 10.0, "latency_ms": {"mean": 60.0, "p50": 55.0, "p95": 90.0, "p99": 120.0, "max": 200.0}},
            "rounds": [rnd], "decisions": [{"decision_index": 0, "target_B": 2, "changed": False, "abstained": False}], "transitions": [],
            "configuration_regimes": [{"from_after_block": 0, "max_message_count": 2}], "flags": [], "problems": problems or []}


def test_a_client_limited_round_makes_a_run_valid_but_not_comparable():
    ok = A.run_record(arm_doc(), analysis_doc(), {"verdict": "VALID"})
    assert ok["valid_run"] and ok["comparable"] and ok["n_client_limited"] == 0
    lag = A.run_record(arm_doc(), analysis_doc(lag_p99=150.0), {"verdict": "VALID_WITH_FLAGS"})
    assert lag["valid_run"] and lag["n_client_limited"] == 1 and not lag["comparable"]          # dispatch lag > 100 ms: the driver, not Fabric, was late
    inc = A.run_record(arm_doc(), analysis_doc(rows=590), {"verdict": "VALID_WITH_FLAGS"})
    assert inc["n_incomplete"] == 1 and not inc["comparable"]
    bad = A.run_record(arm_doc(), analysis_doc(problems=["reconciliation"]), {"verdict": "INVALID_INCOMPLETE"})
    assert not bad["valid_run"] and not bad["comparable"]
    fail = A.run_record(arm_doc(), analysis_doc(failed=40), {"verdict": "VALID_WITH_FLAGS"})     # failures are data, not invalidity
    assert fail["valid_run"] and fail["comparable"] and fail["failure_rate"] == pytest.approx(40 / 600)
    assert not A.run_record(arm_doc(), analysis_doc(), {"verdict": "VALID"}, independent_pass=False)["comparable"]


def test_summary_uses_comparable_runs_only_in_the_primary_set_and_all_valid_in_the_sensitivity_set():
    recs = trio({"static_b2": {"p95": [100.0, 100.0, 400.0], "comparable": [True, True, False]}})
    prim, sens = A.summarize_arms(recs), A.summarize_arms(recs, analysis_set="all_valid")
    assert prim["static_b2"]["runs_used"] == 2 and prim["static_b2"]["run_ids_excluded"] == ["static_b2-3"] and prim["static_b2"]["p95_ms"]["mean"] == 100.0
    assert sens["static_b2"]["runs_used"] == 3 and sens["static_b2"]["p95_ms"]["mean"] == pytest.approx(200.0)


# ------------------------------------------------------------------------------------------------------------------ directions and verdicts
def test_complete_separation_is_required_for_a_meaningful_latency_difference():
    overlap = trio({"reactive": {"p95": [200.0, 205.0, 90.0]}, "static_b2": {"p95": [100.0, 101.0, 99.0]}})     # mean gap 53 ms > SESOI but one run overlaps
    pw = A.pairwise(A.summarize_arms(overlap), "reactive", "static_b2")
    assert pw["p95_ms"]["verdict"] == "NOT_DISTINGUISHABLE"
    clean = trio({"reactive": {"p95": [200.0, 205.0, 195.0]}, "static_b2": {"p95": [100.0, 101.0, 99.0]}})
    assert A.pairwise(A.summarize_arms(clean), "reactive", "static_b2")["p95_ms"]["verdict"] == "MEANINGFUL_A_WORSE"


def test_metric_directions_latency_lower_is_better_throughput_higher_is_better():
    s = A.summarize_arms(trio({"reactive": {"p95": 200.0, "thr": 8.0}, "static_b2": {"p95": 100.0, "thr": 10.0}}))
    pw = A.pairwise(s, "reactive", "static_b2")
    assert pw["p95_ms"]["verdict"] == "MEANINGFUL_A_WORSE"            # reactive has the larger latency: worse
    assert pw["throughput_tps"]["verdict"] == "MEANINGFUL_A_WORSE"    # reactive has the smaller throughput: worse
    s2 = A.summarize_arms(trio({"reactive": {"p95": 100.0, "thr": 12.0}, "static_b2": {"p95": 200.0, "thr": 10.0}}))
    pw2 = A.pairwise(s2, "reactive", "static_b2")
    assert pw2["p95_ms"]["verdict"] == "MEANINGFUL_B_WORSE" and pw2["throughput_tps"]["verdict"] == "MEANINGFUL_B_WORSE"


def test_failure_rate_difference_above_half_a_point_is_meaningful():
    s = A.summarize_arms(trio({"reactive": {"failed": 120}, "static_b2": {"failed": 0}}))      # 2.0% vs 0%
    assert A.pairwise(s, "reactive", "static_b2")["failure_rate"]["verdict"] == "MEANINGFUL_A_WORSE"


def test_verdict_equal_controllers_and_the_b10_interpretive_arm_does_not_leak_into_the_primary_verdict():
    recs = trio({"static_b10": {"p95": 400.0, "mean": 250.0}})       # three primary arms identical; B=10 much worse
    t = A.final_table(recs)
    assert t["verdict"]["status"] == "NO_MEANINGFUL_DIFFERENCE" and "every controller selected b=2" in t["verdict"]["text"].lower()
    assert t["pairwise"]["static_b10_vs_static_b2"]["p95_ms"]["verdict"] == "MEANINGFUL_A_WORSE"
    assert t["header"][1:] == [A.LABELS[a] for a in P.ARMS]
    assert [r[0] for r in t["rows"]][:5] == ["Runs used / run (valid)", "Throughput, committed tx/s", "Delivered / offered", "Mean latency, ms", "p95 latency, ms"]


def test_verdict_names_a_meaningful_difference_when_a_controller_is_clearly_worse():
    recs = trio({"reactive": {"p95": [300.0, 305.0, 295.0]}, "static_b2": {"p95": 100.0}, "predictive_live": {"p95": 100.0}})
    v = A.final_table(recs)["verdict"]
    assert v["status"] == "MEANINGFUL_DIFFERENCE_FOUND" and any(k == "reactive_vs_static_b2" and m == "p95_ms" for k, m, _, _ in v["meaningful"])
    assert v["best_by_p95_point_estimate"] in ("static_b2", "predictive_live") and v["ranking_by_p95"][-1] == "reactive"


def test_one_comparable_run_is_not_enough_for_a_claim():
    recs = trio({"predictive_live": {"comparable": [True, False, False]}})
    v = A.final_table(recs)["verdict"]
    assert v["status"] == "INSUFFICIENT_REPLICATION" and "Predictive" in v["text"]


def test_decision_table_counts_selections_changes_and_abstentions():
    recs = trio({"predictive_live": {"targets": {"2": 7, "5": 3}, "changes": [1, 1, 1], "abstained": [2, 0, 0]}})
    d = A.decision_table(recs)
    assert d["predictive_live"]["selected_B_counts"] == {2: 21, 5: 9} and d["predictive_live"]["config_changes_applied"] == 3
    assert d["predictive_live"]["abstentions"] == 2 and d["all_primary_arms_held_B2"] is False
    assert A.decision_table(trio({}))["all_primary_arms_held_B2"] is True


def test_forecast_accuracy_against_persistence_and_skill():
    arm = {"rounds": [{"index": i, "n_tx": n, "length_ms": 60000} for i, n in enumerate((600, 600, 1200, 600))]}
    decs = [{"decision_index": i, "abstained": False, "controller_metadata": {"forecast_tps": f}} for i, f in enumerate((10.0, 12.0, 15.0))]
    fa = A.forecast_accuracy(arm, decs)["summary"]
    # realized next-minute rates: 10, 20, 10 ; forecasts 10, 12, 15 ; persistence 10, 10, 20
    assert fa["n"] == 3 and fa["mae_ewma_tps"] == pytest.approx((0 + 8 + 5) / 3) and fa["mae_naive_tps"] == pytest.approx((0 + 10 + 10) / 3)
    assert fa["skill_vs_naive"] == pytest.approx(1 - (13 / 3) / (20 / 3))
    assert fa["bias_ewma_tps"] == pytest.approx((0 - 8 + 5) / 3)
