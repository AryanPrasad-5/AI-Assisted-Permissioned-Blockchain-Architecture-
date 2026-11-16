"""Preregistration fields stay PROPOSED / OWNER_DECISION_REQUIRED; the LP4/LP1 conflict is pinned as arithmetic on the stored table;
the run-order generator is deterministic; the approval interlock refuses every defective file."""

import itertools

import pytest

from p4_replay import prereg as pr

HASH = "c" * 64
GOOD = {"approved_by": "owner", "approved_utc": "2026-11-15T09:30:00Z", "prereg_sha256": HASH, "window": "F_82800", "dry_run_window": "D_33000",
        "schedule": "A", "configs": "10,40", "replicates": "3", "run_order_seed": "7", "law_p95_mapping": "M3_p95ofintervals_1s",
        "thresholds_status": "APPROVED", "rel_lag_p99_ms": "20", "rel_lag_max_ms": "100", "missed_schedule_lag_ms": "500", "per_second_count_tv": "0.02",
        "drain_deadline_s": "10", "s1_route": "THIRD_SNAPSHOT", "s1_evidence_sha256": "d" * 64, "lp4_resolution": "LP4b-split", "live_phase": "DRY_RUN_ONLY"}


def text(**over) -> str:
    kv = {**GOOD, **over}
    return "\n".join(f"{k}={v}" for k, v in kv.items() if v is not None) + "\n"


def test_no_field_is_approved():
    fields = pr.proposed_preregistration()
    assert set(fields) >= {"window", "schedule", "configuration_arms", "seeds", "fidelity_thresholds", "run_order", "law_to_run_p95_mapping", "stop_criteria"}
    assert all(f["status"] in ("CARRIED_FROZEN", "PROPOSED", "OWNER_DECISION_REQUIRED") for f in fields.values())
    assert "APPROVED" not in {f["status"] for f in fields.values()}
    assert fields["fidelity_thresholds"]["value"]["numeric_criteria_PROPOSED"]["status"] == "PROPOSED"
    assert fields["seeds"]["value"]["run_order_seed"].startswith("NOT CHOSEN")             # no run-order seed is chosen here
    assert fields["seeds"]["value"]["window_selection_seed"] == 20261114


def test_lp4_conflicts_with_two_of_the_four_law_columns():
    c = pr.lp_consistency()
    lp4 = {k: v["holds"] for k, v in c["LP4_A>=BO>=BF_at_B10"].items()}
    assert lp4 == {"M1_mean_1s": False, "M2_mean_60s": False, "M3_p95ofintervals_1s": True, "M4_p95ofintervals_60s": True}
    assert c["LP4_A>=BO>=BF_at_B10"]["M1_mean_1s"]["values_A_BO_BF"] == (861, 869, 907)    # the two mean columns put BF highest
    assert c["LP4_A>=BO>=BF_at_B10"]["M2_mean_60s"]["values_A_BO_BF"] == (892, 879, 916)


def test_lp1_gap_of_0_6_s_fails_on_the_one_second_quantile_proxy_for_every_arm():
    c = pr.lp_consistency()
    assert c["LP1_gap_B40_minus_B10_ms"]["M3_p95ofintervals_1s"] == {"A": 164, "BO": 415, "BF": 415}
    assert c["LP1_holds_gap_ge_600"] == {"M1_mean_1s": True, "M2_mean_60s": True, "M3_p95ofintervals_1s": False, "M4_p95ofintervals_60s": True}
    # so no single column supports both LP1 and LP4 at 1-s resolution: LP4 needs M3/M4, LP1 needs not-M3
    both = [m for m in pr.MAPPINGS if c["LP4_A>=BO>=BF_at_B10"][m]["holds"] and c["LP1_holds_gap_ge_600"][m]]
    assert both == ["M4_p95ofintervals_60s"]


def test_lp2_and_lp3_bands_cannot_discriminate_between_mappings():
    c = pr.lp_consistency()
    assert all(c["LP2_in_band"].values()) and all(c["LP3_in_band"].values())              # all four mappings sit inside both bands


def test_run_order_is_deterministic_and_balanced():
    a, b = pr.run_order(7), pr.run_order(7)
    assert a == b
    assert [x["position"] for x in a] == list(range(1, 7))
    assert sorted((x["B"], x["replicate"]) for x in a) == [(10, 1), (10, 2), (10, 3), (40, 1), (40, 2), (40, 3)]
    assert max(len(list(g)) for _, g in itertools.groupby(x["B"] for x in a)) <= 2
    assert len({tuple(x["B"] for x in pr.run_order(s)) for s in range(40)}) > 1           # the seed matters


def test_strict_alternation_has_exactly_two_orders():
    orders = {tuple(x["B"] for x in pr.run_order(s, max_consecutive_same=1)) for s in range(60)}
    assert orders == {(10, 40, 10, 40, 10, 40), (40, 10, 40, 10, 40, 10)}


def test_run_order_rejects_bad_seed_and_impossible_limit():
    with pytest.raises(ValueError):
        pr.run_order(True)
    with pytest.raises(ValueError):
        pr.run_order(3.5)
    with pytest.raises(ValueError):
        pr.run_order(7, max_consecutive_same=0)


def test_valid_approval_is_accepted_and_thresholds_become_approved():
    a = pr.check_approval(text(), prereg_sha256=HASH)
    assert a.thresholds.status == "APPROVED" and a.thresholds.approved_by_prereg_sha256 == HASH
    assert a.run_order_seed == 7 and a.mapping == "M3_p95ofintervals_1s"


@pytest.mark.parametrize("over", [
    {"approved_by": "claude"}, {"approved_by": "OWNER"}, {"approved_utc": "tomorrow"}, {"prereg_sha256": "e" * 64}, {"window": "D_33000"},
    {"schedule": "BO"}, {"configs": "10,40,45"}, {"thresholds_status": "PROPOSED"}, {"s1_route": "SKIP"}, {"s1_evidence_sha256": "xyz"},
    {"law_p95_mapping": "mean"}, {"live_phase": "SERIES"}, {"run_order_seed": "abc"}, {"lp4_resolution": "NONE"}, {"rel_lag_max_ms": "fast"},
    {"drain_deadline_s": "0"}, {"per_second_count_tv": "-0.1"}])
def test_defective_approval_is_refused(over):
    with pytest.raises(pr.ApprovalRefused):
        pr.check_approval(text(**over), prereg_sha256=HASH)


@pytest.mark.parametrize("missing", list(GOOD))
def test_every_key_is_required(missing):
    with pytest.raises(pr.ApprovalRefused):
        pr.check_approval(text(**{missing: None}), prereg_sha256=HASH)


def test_unknown_duplicate_and_malformed_lines_are_refused():
    with pytest.raises(pr.ApprovalRefused):
        pr.check_approval(text() + "extra=1\n", prereg_sha256=HASH)
    with pytest.raises(pr.ApprovalRefused):
        pr.check_approval(text() + "approved_by=owner\n", prereg_sha256=HASH)
    with pytest.raises(pr.ApprovalRefused):
        pr.check_approval(text() + "approved by owner\n", prereg_sha256=HASH)


def test_the_preregistration_itself_is_not_an_approval():
    template = "\n".join(f"{k}={v}" for k, v in {"approved_by": "owner"}.items())
    with pytest.raises(pr.ApprovalRefused):
        pr.check_approval(template, prereg_sha256=HASH)
