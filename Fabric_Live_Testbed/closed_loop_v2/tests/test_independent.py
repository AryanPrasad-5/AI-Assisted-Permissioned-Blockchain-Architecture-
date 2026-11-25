"""The independent verifier re-derives the live predictive decisions without importing the controller, and fails when they are wrong."""
import copy
import json

import pytest

from Fabric_Live_Testbed.closed_loop_v2 import independent as I
from Fabric_Live_Testbed.closed_loop_v2.tests.test_end_to_end import mstage, run_one  # noqa: F401  (fixtures reused)


def predictive_run(mstage, tmp_path):
    root, m = mstage
    run = next(r for r in m["runs"] if r["arm_label"] == "predictive_live")
    res, world, ctx, arm = run_one(tmp_path, root, m, run["run_id"])
    assert res["verdict"] == "VALID", res.get("step_failure")
    return arm, res["control"]["decisions"], ctx.run_ws


def test_independent_check_passes_on_a_real_policy_run(mstage, tmp_path):
    arm, decisions, ws = predictive_run(mstage, tmp_path)
    checks = I.check_predictive_live(arm, decisions, ws)
    assert all(c["pass"] for c in checks), [c for c in checks if not c["pass"]]
    assert len(checks) == 8


def test_independent_check_detects_a_forecast_that_used_other_data(mstage, tmp_path):
    arm, decisions, ws = predictive_run(mstage, tmp_path)
    bad = copy.deepcopy(decisions)
    bad[3]["controller_metadata"]["forecast_tps"] += 0.5
    failed = [c["check"] for c in I.check_predictive_live(arm, bad, ws) if not c["pass"]]
    assert any("independent EWMA" in n for n in failed)


def test_independent_check_detects_future_leakage_in_history_length(mstage, tmp_path):
    arm, decisions, ws = predictive_run(mstage, tmp_path)
    bad = copy.deepcopy(decisions)
    bad[2]["controller_metadata"]["history_seconds_used"] += 60       # claims to have seen one more round
    assert any("history length" in c["check"] for c in I.check_predictive_live(arm, bad, ws) if not c["pass"])


def test_independent_check_detects_a_tampered_stage_count_and_a_wrong_choice(mstage, tmp_path):
    arm, decisions, ws = predictive_run(mstage, tmp_path)
    bad_arm = copy.deepcopy(arm)
    bad_arm["predictive_live"]["round_counts"]["4"][0] += 1
    assert any("recount" in c["check"] for c in I.check_predictive_live(bad_arm, decisions, ws) if not c["pass"])
    bad = copy.deepcopy(decisions)
    bad[0]["target_B"], bad[0]["changed"] = 10, True               # claims a switch to the WORSE candidate
    bad[0]["current_B"] = 40
    assert any("argmax" in c["check"] for c in I.check_predictive_live(arm, bad, ws) if not c["pass"])
