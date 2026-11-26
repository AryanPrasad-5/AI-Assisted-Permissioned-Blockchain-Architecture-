"""The owner-approval layer: nothing is approved by code. An approval the owner writes must be complete, coherent, and bound to the exact snapshot, staged schedules,
preregistration and stop conditions it names; any open decision refuses the series. Seeds and hashes here are TEST VALUES."""

import re

import pytest

from p4_replay import classify as cl, prereg as pr, series as se, series_approval as sa, snapshot as sn
from p4_replay.tests import support as sp

SUMS = "cd" * 32


def text(**over):
    base = sa.example_approval_text(phase="DRY_RUN_THEN_SERIES", thresholds_status="APPROVED", prereg_sha256=sp.PREREG_SHA, stage_sums_sha256=SUMS, seed=11)
    for k, v in over.items():
        base = re.sub(rf"^{k}=.*$", f"{k}={v}", base, flags=re.M)
    return base


def check(t):
    return sa.check_series_approval(t, prereg_sha256=sp.PREREG_SHA, stage_sums_sha256=SUMS)


def test_a_complete_approval_is_accepted_and_binds_everything_it_names():
    a = check(text())
    assert a.phase == "DRY_RUN_THEN_SERIES" and a.run_order_seed == 11 and a.thresholds.status == "APPROVED" and a.thresholds.fidelity.approved_by_prereg_sha256 == sp.PREREG_SHA
    assert a.caliper_timeout_s == 1200.0 and a.host_max_seconds == 1500.0 and a.lp1_min_gap_ms == 100 and a.continue_after_limited is False and a.stage_sums_sha256 == SUMS


def test_a_dry_run_only_approval_may_carry_proposed_thresholds_and_stays_provisional():
    a = check(text(live_phase="DRY_RUN_ONLY", thresholds_status="PROPOSED"))
    assert a.phase == "DRY_RUN_ONLY" and a.thresholds.status == "PROPOSED" and a.thresholds.fidelity.approved_by_prereg_sha256 is None and a.thresholds.burst_status == "PROPOSED"


def test_the_series_phase_demands_approved_thresholds():
    with pytest.raises(pr.ApprovalRefused, match="thresholds_status must be APPROVED"):
        check(text(thresholds_status="PROPOSED"))                                           # thresholds can be finalised after the dry run, never after the series started


def test_phase_1_check_approval_alone_would_make_the_dry_run_impossible():
    """The finding behind this layer: prereg.check_approval demands APPROVED thresholds even for DRY_RUN_ONLY, yet the P4 draft finalises them after the dry run."""
    plain = sa.example_approval_text(phase="DRY_RUN_ONLY", thresholds_status="PROPOSED", prereg_sha256=sp.PREREG_SHA, stage_sums_sha256=SUMS)
    phase1_only = "\n".join(l for l in plain.splitlines() if l.split("=")[0] in pr.APPROVAL_KEYS)
    with pytest.raises(pr.ApprovalRefused, match="PROPOSED thresholds cannot authorise"):
        pr.check_approval(phase1_only, prereg_sha256=sp.PREREG_SHA)
    assert check(plain).thresholds.status == "PROPOSED"


@pytest.mark.parametrize("key", list(pr.APPROVAL_KEYS) + list(sa.EXTRA_KEYS))
def test_every_key_is_required(key):
    t = "\n".join(l for l in text().splitlines() if not l.startswith(key + "="))
    with pytest.raises(pr.ApprovalRefused, match="lacks"):
        check(t)


def test_unknown_keys_and_repeated_keys_are_refused():
    with pytest.raises(pr.ApprovalRefused, match="unknown keys"):
        check(text() + "extra=1\n")
    with pytest.raises(pr.ApprovalRefused, match="repeated"):
        check(text() + "approved_by=owner\n")


@pytest.mark.parametrize("over,match", [
    ({"approved_by": "assistant"}, "approved_by must be 'owner'"),
    ({"prereg_sha256": "00" * 32}, "does not equal the hash of the preregistration"),
    ({"stage_sums_sha256": "11" * 32}, "stage_sums_sha256 does not equal"),
    ({"snapshot_id": "ARCHIVE_20261112T213119Z"}, "must name snapshot"),
    ({"snapshot_sums_sha256": "22" * 32}, "must name snapshot"),
    ({"dry_run_window": "D_99999"}, "dry_run_window must be D_33000"),
    ({"caliper_timeout_s": "300"}, "cannot fit a replay"),
    ({"host_max_seconds": "1319"}, "must outlive the Caliper timeout"),
    ({"lp4_resolution": "NONE"}, "lp4_resolution"),
    ({"stop_conditions_sha256": "33" * 32}, "stop_conditions_sha256"),
    ({"continue_after_limited_measured": "maybe"}, "continue_after_limited_measured"),
    ({"burst_rel_dev_max": "0"}, "burst_rel_dev_max"),
    ({"burst_rel_dev_max": "1.5"}, "burst_rel_dev_max"),
    ({"lp1_min_gap_ms": "0"}, "not attainable"),
    ({"law_p95_mapping": "M9"}, "law_p95_mapping"),
    ({"live_phase": "EVERYTHING"}, "live_phase"),
    ({"rel_lag_p99_ms": "0"}, "positive"),
    ({"s1_route": "TRUST_ME"}, "s1_route"),
])
def test_incoherent_or_unbound_values_are_refused(over, match):
    with pytest.raises(pr.ApprovalRefused, match=match):
        check(text(**over))


def test_the_lp1_threshold_must_be_attainable_under_the_selected_mapping():
    assert sa.LAW_GAP_A_MS == {"M1_mean_1s": 982, "M2_mean_60s": 1048, "M3_p95ofintervals_1s": 164, "M4_p95ofintervals_60s": 858}     # arithmetic on the stored P4-draft table
    with pytest.raises(pr.ApprovalRefused, match="law gap for arm A is 164 ms"):
        check(text(law_p95_mapping="M3_p95ofintervals_1s", lp1_min_gap_ms="600"))            # the draft's 0.6 s gap cannot follow from M3
    assert check(text(law_p95_mapping="M1_mean_1s", lp1_min_gap_ms="600")).lp1_min_gap_ms == 600
    assert check(text(law_p95_mapping="M3_p95ofintervals_1s", lp1_min_gap_ms="164")).lp1_min_gap_ms == 164


def test_stop_conditions_are_bound_by_hash_and_do_not_include_lp4():
    assert sa.stop_conditions_sha256().startswith("4521f2bd8f8611a8")                       # a silent edit of the stop conditions changes this: update the pin only deliberately
    body = sa.stop_conditions_text()
    assert "SC1" in body and "SC10" in body and "LP4" not in body and "BO" not in body.split() and "BF" not in body.split()
    assert check(text()).raw["stop_conditions_sha256"] == sa.stop_conditions_sha256()


def test_decision_register_lists_the_eight_owner_decisions_and_resolves_them_only_by_a_valid_approval():
    open_ = {d["id"]: d for d in sa.decision_register(None)}
    assert [d for d in open_ if d.startswith("D")] == [f"D{i}" for i in range(1, 9)] and all(d["status"] == "OWNER_DECISION_REQUIRED" for d in open_.values())
    assert "run_order_seed" in open_["D1"]["keys"] and "dry_run_window" in open_["D5"]["keys"] and "caliper_timeout_s" in open_["D6"]["keys"] and "host_max_seconds" in open_["D7"]["keys"]
    resolved = {d["id"]: d for d in sa.decision_register(check(text()))}
    assert all(resolved[f"D{i}"]["status"] == "RESOLVED_BY_APPROVAL" for i in range(1, 9))
    assert resolved["A3"]["status"] == resolved["A4"]["status"] == "OWNER_DECISION_REQUIRED"      # the drain-deadline reading and the keep-awake helper are not settled by any file


def test_require_resolved_refuses_none_and_returns_a_valid_approval():
    with pytest.raises(sa.UnresolvedPreregistration, match="unresolved owner decisions"):
        sa.require_resolved(None)
    a = check(text())
    assert sa.require_resolved(a) is a


def test_the_proposed_preregistration_still_has_every_decision_open():
    """Phase 1's proposed preregistration is the document the approval is bound to; its statuses are not changed by anything in Phase 2."""
    p = pr.proposed_preregistration()
    assert p["seeds"]["status"] == p["run_order"]["status"] == p["law_to_run_p95_mapping"]["status"] == "OWNER_DECISION_REQUIRED"
    assert p["fidelity_thresholds"]["status"] == "PROPOSED" and p["window"]["dry_run_window"]["status"] == "OWNER_DECISION_REQUIRED"
    assert "run_order_seed" in p["seeds"]["value"] and "NOT CHOSEN" in p["seeds"]["value"]["run_order_seed"]


def test_the_readiness_command_reports_open_decisions_and_resolves_with_a_valid_approval(tmp_path, capsys):
    staged = sp.build_stage(tmp_path)
    prereg = tmp_path / "prereg.md"
    prereg.write_text("the preregistration the owner approved", encoding="utf-8")
    import hashlib
    sha = hashlib.sha256(prereg.read_bytes()).hexdigest()
    approval = tmp_path / "approval.txt"
    approval.write_text(sa.example_approval_text(phase="DRY_RUN_ONLY", thresholds_status="PROPOSED", prereg_sha256=sha, stage_sums_sha256=staged.sums_sha256), encoding="utf-8")
    assert se.main(["readiness", "--prereg", str(prereg), "--approval", str(approval), "--stage-root", str(staged.stage_dir)]) == 0
    assert "RESOLVED" in capsys.readouterr().out
    assert se.main(["readiness"]) == 3 and "UNRESOLVED" in capsys.readouterr().out
