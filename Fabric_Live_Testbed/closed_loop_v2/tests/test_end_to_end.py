"""Whole v2 runs against the fake world (no process, no network): v2 stage -> UNCHANGED closed_loop runner (v2 policy factory) -> control
loop -> actuator -> ledger -> analysis -> v2 analysis. Proves the plumbing, not Fabric performance (the fake plant is fixed:
mean latency 520 ms at B=10, 1030 ms at B=40)."""
import json
from pathlib import Path

import pytest

from Fabric_Live_Testbed.closed_loop import runner as v1_runner
from Fabric_Live_Testbed.closed_loop.stage_verify import verify_stage_tree
from Fabric_Live_Testbed.closed_loop.tests.test_runner_fake import FakeWorld, auth_file
from Fabric_Live_Testbed.closed_loop_v2 import analysis_v2 as A
from Fabric_Live_Testbed.closed_loop_v2 import protocol as P
from Fabric_Live_Testbed.closed_loop_v2 import runner_v2 as rn
from Fabric_Live_Testbed.closed_loop_v2 import stage_v2 as ST


class World2(FakeWorld):
    """FakeWorld whose channel starts at the arm's initial B (static-40 arms)."""

    def __init__(self, ctx, arm, **kw):
        super().__init__(ctx, arm, **kw)
        self.B = int(arm["initial_B"])
        self.events = [("config", self.B)]


def run_one(tmp_path, root, manifest, run_id, **world_kw):
    run = next(r for r in manifest["runs"] if r["run_id"] == run_id)
    auth = auth_file(tmp_path, manifest["stage_sha256"], [r["run_id"] for r in manifest["runs"]], [r["channel"] for r in manifest["runs"]])
    ctx = v1_runner.Ctx(root, run_id, auth, tmp_path / "out", Path("/net.json"), tmp_path / "cws", Path("/msp"), python="python3")
    arm = json.loads((root / run_id / "arm.json").read_text())
    world = World2(ctx, arm, **world_kw)
    return rn.execute(ctx, world), world, ctx, arm


# ------------------------------------------------------------------------------------------------ X0
@pytest.fixture(scope="module")
def x0(tmp_path_factory):
    mp = pytest.MonkeyPatch()
    mp.setattr(P, "X0_RUNG_RATES_TPS", (10, 20, 40))
    root = tmp_path_factory.mktemp("x0") / "s"
    m = ST.stage_x0(root, protocol_sha256="f" * 64)
    mp.undo()
    return root, m


def test_x0_stage_is_deterministic_hashed_and_covers_both_b_with_replicates(x0, tmp_path):
    root, m = x0
    assert verify_stage_tree(root) == m["stage_sha256"]
    assert len(m["runs"]) == 4 and sorted((r["B"], r["replicate"]) for r in m["runs"]) == [(10, 1), (10, 2), (40, 1), (40, 2)]
    assert all(sorted(r["rung_order_tps"]) == [10, 20, 40] for r in m["runs"])
    a = ST.x0_run_specs()
    assert a == ST.x0_run_specs()                       # same seed, same order
    arm = json.loads((root / m["runs"][0]["run_id"] / "arm.json").read_text())
    assert arm["arm"] == "static" and arm["decision_times_utc"] == [] and arm["rounds"][0]["n_tx"] == 20 * 60
    assert [r["n_tx"] / 60 for r in arm["rounds"][1:]] == [float(x) for x in m["runs"][0]["rung_order_tps"]]
    with pytest.raises(FileExistsError):
        ST.stage_x0(root)


def test_x0_runs_through_the_unchanged_runner_and_the_crossover_analysis(x0, tmp_path):
    root, m = x0
    rows = []
    for run in m["runs"]:
        workdir = tmp_path / run["run_id"]
        workdir.mkdir()
        res, world, ctx, arm = run_one(workdir, root, m, run["run_id"])
        assert res["verdict"] == "VALID", res.get("step_failure")
        assert not any("apply" in c for c in world.calls)                         # static: no actuation
        analysis = json.loads((ctx.rd / "analysis.json").read_text())
        rows += A.x0_round_rows(arm, analysis)
    assert len(rows) == 12 and {r["class"] for r in rows} == {"OK"}
    assert {(r["B"], r["rate_tps"]) for r in rows} == {(b, r) for b in (10, 40) for r in (10.0, 20.0, 40.0)}
    rep = A.crossover_report(rows)
    assert rep["crossover"]["outcome"] == "NO_CROSSOVER_WITHIN_CLIENT_CAPACITY" and rep["crossover"]["r_max"] == 40
    pts = rep["surface_points"]
    assert len(pts) == 6 and all(p["delivered_fraction"] <= 1.0 for p in pts)
    assert {p["B"] for p in pts} == {10, 40}


# ------------------------------------------------------------------------------------------------ M
SURF = []
for rate, m10, m40, f10, f40 in [(10, 500, 1000, 1, 1), (20, 520, 950, 1, 1), (40, 700, 850, 1, 1),
                                 (80, 1800, 800, 0.8, 1), (160, 9000, 900, 0.4, 1)]:
    SURF += [{"B": 10, "rate_tps": rate, "mean_ms": m10, "delivered_fraction": f10, "p95_ms": m10 * 1.6, "client_valid": True},
             {"B": 40, "rate_tps": rate, "mean_ms": m40, "delivered_fraction": f40, "p95_ms": m40 * 1.6, "client_valid": True}]
WINDOW = {"start_s": 20400, "segment_rates_tps": [22.5] * 10, "preroll_rate_tps": 22.5, "warmup_rates_tps": [22.5] * 10}


@pytest.fixture(scope="module")
def mstage(tmp_path_factory):
    root = tmp_path_factory.mktemp("m") / "s"
    reactive = P.reactive_params(500.0)                 # 520 ms at B=10 exceeds 500 ms: this reactive rule fires in the fake plant
    m = ST.stage_m(root, window=WINDOW, k=2.0, reactive=reactive, surface_points=SURF,
                   x0_provenance={"x0_stage_sha256": "a" * 64}, replicates=1, protocol_sha256="f" * 64)
    return root, m


def test_m_stage_has_four_arms_identical_schedules_and_all_inputs_from_x0(mstage):
    root, m = mstage
    assert verify_stage_tree(root) == m["stage_sha256"]
    assert sorted(r["arm_label"] for r in m["runs"]) == sorted(P.M_ARMS)
    arms = {r["arm_label"]: json.loads((root / r["run_id"] / "arm.json").read_text()) for r in m["runs"]}
    sched = {a: [(x["index"], x["n_tx"]) for x in d["rounds"]] for a, d in arms.items()}
    assert len({json.dumps(v) for v in sched.values()}) == 1                        # equivalent workload for every arm
    assert [n for _, n in sched["static10"]] == [int(22.5 * 2.0 * 60)] * 11
    assert arms["static40"]["initial_B"] == 40 and all(arms[a]["initial_B"] == 10 for a in ("static10", "reactive", "predictive_live"))
    assert arms["reactive"]["reactive"]["latency_high_ms"] == 500.0 and arms["reactive"]["reactive"]["variant"] == "X0-calibrated"
    pl = arms["predictive_live"]["predictive_live"]
    assert len(pl["warmup_counts"]) == 600 and pl["surface_points"] == SURF and pl["x0"] == {"x0_stage_sha256": "a" * 64}
    for a, d in arms.items():                                                       # the three files each policy sees are in one stage
        assert d["window"]["k"] == 2.0 and d["window"]["scaled_segment_rates_tps"] == [45.0] * 10
    h0 = Path(root) / m["runs"][0]["run_id"] / "workspace" / "tx_map.csv"
    assert "scaled-2026-10-12-w20400-k2.0" in h0.read_text().splitlines()[1]
    with pytest.raises(ValueError):
        ST.stage_m(root.parent / "s2", window=WINDOW, k=1.0, reactive={}, surface_points=SURF, x0_provenance={})     # k outside range


def test_static_arms_hold_their_configuration_and_static40_starts_at_40(mstage, tmp_path):
    root, m = mstage
    for label, b in (("static10", 10), ("static40", 40)):
        run = next(r for r in m["runs"] if r["arm_label"] == label)
        (tmp_path / label).mkdir()
        res, world, ctx, arm = run_one(tmp_path / label, root, m, run["run_id"])
        assert res["verdict"] == "VALID", res.get("step_failure")
        a = json.loads((ctx.rd / "analysis.json").read_text())
        assert a["configuration_regimes"] == [{"from_after_block": 0, "max_message_count": b}]
        assert [d["target_B"] for d in a["decisions"]] == [b] * 10 and not any("apply" in c for c in world.calls)


def test_reactive_arm_switches_when_its_x0_threshold_is_crossed(mstage, tmp_path):
    root, m = mstage
    run = next(r for r in m["runs"] if r["arm_label"] == "reactive")
    res, world, ctx, arm = run_one(tmp_path, root, m, run["run_id"])
    assert res["verdict"] == "VALID", res.get("step_failure")
    t = res["control"]["transitions"]
    assert len(t) == 1 and t[0]["to_B"] == 40 and t[0]["success"] and res["final_config"]["max_message_count"] == 40


def test_predictive_live_forecasts_decides_and_applies_through_the_actuator(mstage, tmp_path):
    root, m = mstage
    run = next(r for r in m["runs"] if r["arm_label"] == "predictive_live")
    res, world, ctx, arm = run_one(tmp_path, root, m, run["run_id"])
    assert res["verdict"] == "VALID", res.get("step_failure")
    dec = res["control"]["decisions"]
    assert len(dec) == 10 and dec[0]["arm"] == "predictive_live" and not dec[0]["abstained"]
    # at 45 tx/s the synthetic surface prefers B=40 (lower predicted latency, no backlog): applied through the real actuator path
    assert dec[0]["target_B"] == 40 and dec[0]["applied"] and any("apply" in c for c in world.calls)
    assert all(d["target_B"] == 40 for d in dec) and res["final_config"]["max_message_count"] == 40
    meta = dec[0]["controller_metadata"]
    assert meta["history_seconds_used"] == 660 and meta["forecast_tps"] == pytest.approx(45.0, rel=0.02)
    assert [d["controller_metadata"]["history_seconds_used"] for d in dec] == [660 + 60 * i for i in range(10)]
    analysis = json.loads((ctx.rd / "analysis.json").read_text())
    assert analysis["transitions"][0]["ledger_config_block"] is not None            # native/independent config evidence on the ledger
    fa = A.forecast_accuracy(arm, dec)
    assert fa["summary"]["n"] == 10 and fa["summary"]["mae_ewma_tps"] < 1.0 and fa["summary"]["mae_naive_tps"] == pytest.approx(0.0)


def test_regret_tables_from_surface_and_static_references(mstage, tmp_path):
    from Fabric_Live_Testbed.closed_loop_v2.predictor import LocalSurfacePredictor
    root, m = mstage
    run = next(r for r in m["runs"] if r["arm_label"] == "predictive_live")
    res, world, ctx, arm = run_one(tmp_path, root, m, run["run_id"])
    analysis = json.loads((ctx.rd / "analysis.json").read_text())
    ref = {10: {s: 800.0 for s in range(10)}, 40: {s: 1000.0 for s in range(10)}}
    t = A.regret_tables(arm, analysis, LocalSurfacePredictor(SURF), ref)
    assert len(t["rows"]) == 10 and all(r["rate_tps"] == 45.0 for r in t["rows"])
    # segment 0 is committed under B=40 (the update lands before the first measured transactions): surface says B=40 is best at 45 tx/s
    assert t["rows"][-1]["B_in_force"] == 40 and t["rows"][-1]["model_regret_ms"] == pytest.approx(0.0, abs=1e-6)
    assert all("realized_regret_p95_ms" in r for r in t["rows"])


def test_m_run_order_is_block_randomized_so_any_prefix_of_four_is_a_complete_replicate():
    specs = ST.m_run_specs(replicates=3)
    assert [s["pos"] for s in specs] == list(range(12))
    for k in (1, 2, 3):
        block = specs[4 * (k - 1):4 * k]
        assert sorted(s["arm"] for s in block) == sorted(P.M_ARMS) and {s["rep"] for s in block} == {k}
    assert specs == ST.m_run_specs(replicates=3)                       # deterministic for the frozen seed
