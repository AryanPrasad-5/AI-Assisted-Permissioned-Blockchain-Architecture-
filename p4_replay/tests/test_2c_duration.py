"""Phase 2C, Package B: the keep-awake duration budget. Documented upper bounds per component, starting where the helper starts (before the pre-flight) and ending where its cleanup
ends. Pure arithmetic; no measurement and no claim about how long a launch is expected to take."""

import math

import pytest

from p4_replay import caliper_check as cc, driver as dr, duration_model as dm, launch_series as ls
from p4_replay.tests import support_2c as s2

p1b = dr.p1b_module()
THR = p1b.THR
DRY_TX, MEAS_TX = 7490, 6777                      # D_33000 6,887 + 603 pre-roll; F_82800 6,229 + 548 pre-roll
MEASURED = [(10, MEAS_TX), (40, MEAS_TX), (10, MEAS_TX), (40, MEAS_TX), (10, MEAS_TX), (40, MEAS_TX)]


def budget(runs, timeout=1200.0, **kw):
    return dm.build_budget("X", runs, caliper_timeout_s=timeout, gate_ids=ls.GATE_ORDER_V2, thr=THR, **kw)


def test_every_launch_gate_has_a_documented_bound_and_nothing_else_has():
    assert set(dm.gate_bounds(THR, 6)) == set(ls.GATE_ORDER_V2)
    assert len(ls.GATE_ORDER_V2) == 17 and set(ls.GATE_ORDER_V2) - {"R6"} == set(pf_gate_order())


def pf_gate_order():
    from p4_replay import preflight as pf
    return pf.GATE_ORDER


def test_a_gate_without_a_bound_is_refused_not_given_a_default():
    with pytest.raises(ValueError, match="no documented duration bound"):
        dm.build_budget("X", MEASURED, caliper_timeout_s=1200.0, gate_ids=ls.GATE_ORDER_V2 + ("R9",), thr=THR)
    with pytest.raises(ValueError, match="at least one run"):
        budget([])


def test_every_component_is_positive_and_names_its_basis():
    b = budget(MEASURED)
    assert all(c.seconds > 0 and c.basis.strip() for c in b.components)
    assert b.total_s == math.ceil(sum(c.seconds for c in b.components)) and {c.phase for c in b.components} == {"helper", "preflight", "run", "end", "margin"}


def test_the_budget_includes_what_phase_2_left_out():
    """Phase 2: n x (timeout + 900) + 1200, from the first run. Here the helper's start, the gates (S1 alone is 3,600 s), the probe, the runs, the end and the margin are all inside."""
    b = budget(MEASURED)
    names = " | ".join(c.name for c in b.components)
    for needed in ("keep-awake helper start wait", "gate S1", "gate S9", "gate R4", "gate R6", "series root, manifest, probe start wait", "run 1: caliper run", "run 6: block fetch",
                   "run 6: precheck", "end: fingerprint", "end: probe flush", "end: reconciliation", "end: helper cleanup", "cleanup margin"):
        assert needed in names, needed
    s1 = next(c for c in b.components if c.name == "gate S1")
    assert s1.seconds == 3600.0 and "3600" in s1.basis
    assert b.preflight_s >= 3600
    assert b.total_s > cc.keep_awake_seconds(6, 1200.0) and budget([(10, DRY_TX)]).total_s > cc.keep_awake_seconds(1, 1200.0)
    # Phase 2's figure does exceed (pre-flight + six Caliper timeouts), but it leaves only this much for every other coded per-run limit (channel, staging, fetches, block fetch, ...):
    slack = cc.keep_awake_seconds(6, 1200.0) - (b.preflight_s + 6 * 1200)
    other_per_run = sum(c.seconds for c in b.components if c.phase == "run" and not c.name.endswith(("caliper run", "caliper abort path")))
    assert 0 < slack < 400 and other_per_run > 10 * slack


def test_the_helper_budget_covers_the_phase_2_formula_plus_the_preflight():
    b = budget(MEASURED)
    assert b.total_s >= cc.keep_awake_seconds(6, 1200.0) + b.preflight_s - cc.SERIES_MARGIN_S


def test_the_probe_lives_for_everything_after_the_preflight():
    b = budget(MEASURED)
    assert b.probe_s == b.total_s - b.preflight_s and b.probe_s > 6 * 1200


def test_monotone_in_runs_and_in_the_caliper_timeout():
    one, six = budget([(10, DRY_TX)]), budget(MEASURED)
    assert six.total_s > one.total_s
    assert budget(MEASURED, 1800.0).total_s > budget(MEASURED, 1200.0).total_s
    assert budget([(10, 20000)]).total_s > budget([(10, 2000)]).total_s, "more transactions, more blocks, a longer block fetch"
    assert budget([(10, MEAS_TX)]).total_s > budget([(40, MEAS_TX)]).total_s, "B=10 cuts more blocks than B=40"


def test_the_block_height_bound_follows_the_documented_formula():
    comps = dm.per_run_components(THR, 1200.0, 10, MEAS_TX)
    fetch = next(c for c in comps if c[0].startswith("block fetch"))
    height = math.ceil(MEAS_TX / 10) + math.ceil(1200.0 / 2.0) + 16
    assert f"height bound {height}" in fetch[0] and fetch[1] == max(THR.fetch_timeout_base_s, 120.0 + THR.fetch_timeout_per_block_s * height)


def test_the_caliper_component_is_the_approved_timeout_and_the_other_p1b_limits_are_the_coded_ones():
    comps = {c[0]: c[1] for c in dm.per_run_components(THR, 1500.0, 40, MEAS_TX)}
    assert comps["caliper run"] == 1500.0 and comps["channel creation"] == THR.channel_timeout_s and comps["stage copy, plan, verify, metrics_before"] == 4 * THR.tool_timeout_s


def test_the_margin_is_p1bs_and_can_be_overridden_only_explicitly():
    assert budget(MEASURED).components[-1].seconds == cc.SERIES_MARGIN_S
    assert budget(MEASURED, margin_s=60.0).total_s == budget(MEASURED).total_s - int(cc.SERIES_MARGIN_S - 60.0)


def test_the_orchestrator_hands_exactly_the_budget_to_the_helper(tmp_path):
    from p4_replay import launch_ids as li
    L = s2.prepare_launch(tmp_path, li.DRY)
    seen = {}
    real_spawn = L.be.spawn
    L.be.spawn = lambda argv, env=None, *a, **kw: (seen.setdefault("env", dict(env)) if argv[0] == "powershell.exe" else None, real_spawn(argv, env, *a, **kw))[1]
    summary = L.run()
    assert summary["status"] == "COMPLETE" and seen["env"]["QL_SECONDS"] == str(summary["keep_awake_budget_s"])
    expected = dm.build_budget(li.DRY, [(10, L.staged.manifest["runs"]["rp90"]["tx_total"])], caliper_timeout_s=1200.0, gate_ids=ls.GATE_ORDER_V2, thr=L.session.thr).total_s
    assert summary["keep_awake_budget_s"] == expected
    launch_manifest = __import__("json").loads((L.ctx.root / "launch_manifest.json").read_text())
    assert launch_manifest["keep_awake_budget"]["total_s"] == expected and len(launch_manifest["keep_awake_budget"]["components"]) > 20
