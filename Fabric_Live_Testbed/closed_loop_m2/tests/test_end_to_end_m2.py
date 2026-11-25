"""Whole M2 runs against the fake world (no process, no network): M2 stage -> runner_m2 (policy factory v2 + M2 actuator wrapper + range policy)
-> control loop -> actuator -> ledger -> analysis -> final comparison table. Proves plumbing, decision logic and the table; NOT Fabric performance
(the fake plant is a fixed latency per B)."""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
for p in (ROOT, ROOT / "src", ROOT / "scripts" / "e1"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import e1_config_actuator as ca  # noqa: E402

from Fabric_Live_Testbed.closed_loop import runner as v1_runner  # noqa: E402
from Fabric_Live_Testbed.closed_loop.stage_verify import verify_stage_tree  # noqa: E402
from Fabric_Live_Testbed.closed_loop.tests import test_runner_fake as fk  # noqa: E402
from Fabric_Live_Testbed.closed_loop_m2 import final_comparison_m2 as FC  # noqa: E402
from Fabric_Live_Testbed.closed_loop_m2 import protocol_m2 as P  # noqa: E402
from Fabric_Live_Testbed.closed_loop_m2 import runner_m2 as rn  # noqa: E402
from Fabric_Live_Testbed.closed_loop_m2 import stage_m2 as ST  # noqa: E402

ORIGINAL_RUNNER_GLOBALS = (v1_runner.build_policy, v1_runner.actuator_argv)

PLANT = {2: 60, 5: 150, 10: 480}
RATES = [6.0, 8.0, 10.0, 12.0, 9.0, 7.0, 11.0, 13.0, 8.0, 6.0]
WINDOW = {"start_s": 20400, "segment_rates_tps": RATES, "preroll_rate_tps": 8.0, "warmup_rates_tps": [8.0] * 10}


def surface(best=2):
    """Synthetic X1-like surface: the `best` B has the lowest mean latency at every rate."""
    pts = []
    for b in (2, 5, 10):
        for r in (5, 10, 20, 40, 60):
            mean = 100 + (0 if b == best else 150 * (1 + abs(b - best) / 10))
            pts.append({"B": b, "rate_tps": float(r), "mean_ms": mean, "p95_ms": 2 * mean, "delivered_fraction": 1.0, "client_valid": True})
    return pts


REACTIVE = {"latency_high_ms": 300.0, "latency_low_ms": 240.0, "consecutive_samples": 2, "cooldown_samples": 0, "max_telemetry_age_s": 5.0,
            "congested_B": 5, "default_B": 2, "rule": "test"}


class World(fk.FakeWorld):
    """FakeWorld whose channel starts at the arm's initial B and whose actuator is the M2 wrapper."""

    def __init__(self, ctx, arm, **kw):
        super().__init__(ctx, arm, **kw)
        self.B = int(arm["initial_B"])
        self.events = [("config", self.B)]
        self.actuator_calls = []

    def applies(self):
        return [c for c in self.actuator_calls if "apply" in c]

    def run(self, argv, timeout_s, env=None):
        argv = [str(a) for a in argv]
        if any(a.endswith("e1_config_actuator_m2.py") for a in argv):
            self.actuator_calls.append(argv)
            assert argv[argv.index("--min-b") + 1] == "2" and argv[argv.index("--max-b") + 1] == "10"
            argv = [a.replace("e1_config_actuator_m2.py", "e1_config_actuator.py") for a in argv]
        else:
            assert not any(a.endswith("e1_config_actuator.py") for a in argv), "the runner must call the M2 actuator wrapper, not the frozen tool"
        return super().run(argv, timeout_s, env)


@pytest.fixture(autouse=True)
def plant(monkeypatch):
    for b, v in PLANT.items():
        monkeypatch.setitem(fk.LAT, b, v)


def auth(tmp_path, stage_sha, runs, channels, targets="2,5,10", limit="10"):
    return fk.auth_file(tmp_path, stage_sha, runs, channels, broadcast_targets=targets, max_broadcasts_per_run=limit)


def run_one(tmp_path, root, manifest, run_id, **kw):
    tmp_path.mkdir(parents=True, exist_ok=True)
    a = auth(tmp_path, manifest["stage_sha256"], [r["run_id"] for r in manifest["runs"]], [r["channel"] for r in manifest["runs"]], **kw.pop("auth", {}))
    ctx = v1_runner.Ctx(root, run_id, a, tmp_path / "out", Path("/net.json"), tmp_path / "cws", Path("/msp"), python="python3")
    arm = json.loads((root / run_id / "arm.json").read_text())
    world = World(ctx, arm, **kw)
    return rn.execute(ctx, world), world, ctx, arm


@pytest.fixture(scope="module")
def mstage(tmp_path_factory):
    root = tmp_path_factory.mktemp("m2") / "s"
    m = ST.stage_m2(root, window=WINDOW, k=1.0, reactive=REACTIVE, surface_points=surface(2), provenance={"x1": "synthetic"}, replicates=2,
                    protocol_sha256="f" * 64, source_freeze_sha256="e" * 64)
    return root, m


def label_run(m, label, rep=1):
    return next(r for r in m["runs"] if r["arm_label"] == label and r["replicate"] == rep)["run_id"]


# ------------------------------------------------------------------------------------------------------------------ stage
def test_stage_is_hashed_balanced_blockrandomized_and_arms_share_one_schedule(mstage):
    root, m = mstage
    assert verify_stage_tree(root) == m["stage_sha256"] and m["kind"] == "M2" and len(m["runs"]) == 8
    for rep in (1, 2):
        assert sorted(r["arm_label"] for r in m["runs"] if r["replicate"] == rep) == sorted(P.ARMS)
    assert sorted(r["arm_label"] for r in m["runs"][:4]) == sorted(P.ARMS)          # any prefix of 4 is a full replicate
    assert all(m["runs"][i]["arm_label"] != m["runs"][i + 1]["arm_label"] for i in range(7))
    arms = {r["run_id"]: json.loads((root / r["run_id"] / "arm.json").read_text()) for r in m["runs"]}
    sched = {json.dumps([(x["index"], x["n_tx"]) for x in d["rounds"]]) for d in arms.values()}
    assert len(sched) == 1                                                          # equivalent workload for every arm and replicate
    for d in arms.values():
        assert d["menu_B"] == [2, 5, 10] and d["initial_B"] == P.ARM_INITIAL_B[d["arm_label"]] and d["protocol"] == P.PROTOCOL_ID
        assert d["channel"].startswith("e1-cc") and d["batch_timeout"] == "2s"
    pl = next(d for d in arms.values() if d["arm_label"] == "predictive_live")["predictive_live"]
    assert len(pl["warmup_counts"]) == 600 and pl["surface_points"] == surface(2)
    assert next(d for d in arms.values() if d["arm_label"] == "reactive")["reactive"]["default_B"] == 2
    assert ST.run_specs() == ST.run_specs() and len(ST.run_specs()) == 12
    with pytest.raises(FileExistsError):
        ST.stage_m2(root, window=WINDOW, k=1.0, reactive=REACTIVE, surface_points=surface(2), provenance={})
    with pytest.raises(ValueError, match="primary"):
        ST.stage_m2(root.parent / "x", window=WINDOW, k=1.0, reactive=REACTIVE, surface_points=surface(2), provenance={}, arms=("static_b2", "reactive"))
    with pytest.raises(ValueError, match="outside"):
        ST.stage_m2(root.parent / "y", window=WINDOW, k=0.5, reactive=REACTIVE, surface_points=surface(2), provenance={})


# ------------------------------------------------------------------------------------------------------------------ arms
@pytest.mark.parametrize("label,b", [("static_b2", 2), ("static_b10", 10)])
def test_static_arms_hold_their_b_and_never_actuate(mstage, tmp_path, label, b):
    root, m = mstage
    res, world, ctx, arm = run_one(tmp_path, root, m, label_run(m, label))
    assert res["verdict"] == "VALID", res.get("step_failure")
    a = json.loads((ctx.rd / "analysis.json").read_text())
    assert a["configuration_regimes"] == [{"from_after_block": 0, "max_message_count": b}] and not world.applies()
    assert [d["target_B"] for d in a["decisions"]] == [b] * 10


def test_predictive_with_a_b2_dominant_surface_holds_b2_through_the_live_forecast(mstage, tmp_path):
    root, m = mstage
    res, world, ctx, arm = run_one(tmp_path, root, m, label_run(m, "predictive_live"))
    assert res["verdict"] == "VALID", res.get("step_failure")
    dec = res["control"]["decisions"]
    assert len(dec) == 10 and all(d["target_B"] == 2 and not d["changed"] and not d["abstained"] for d in dec)
    assert [d["controller_metadata"]["history_seconds_used"] for d in dec] == [660 + 60 * i for i in range(10)]
    assert not world.applies() and res["control"]["transitions"] == []


def test_predictive_moves_inside_the_menu_when_the_surface_says_so_and_applies_it_through_the_m2_actuator(tmp_path):
    root = tmp_path / "s"
    m = ST.stage_m2(root, window=WINDOW, k=1.0, reactive=REACTIVE, surface_points=surface(5), provenance={}, replicates=1)
    res, world, ctx, arm = run_one(tmp_path / "w", root, m, label_run(m, "predictive_live"))
    assert res["verdict"] == "VALID", res.get("step_failure")
    dec = res["control"]["decisions"]
    assert dec[0]["target_B"] == 5 and dec[0]["applied"] and all(d["target_B"] == 5 for d in dec)
    assert len(world.applies()) == 1 and world.applies()[0][world.applies()[0].index("--target-b") + 1] == "5"
    assert res["final_config"]["max_message_count"] == 5
    a = json.loads((ctx.rd / "analysis.json").read_text())
    assert a["transitions"][0]["ledger_config_block"] is not None


def test_reactive_holds_b2_when_healthy(mstage, tmp_path):
    root, m = mstage
    res, world, ctx, arm = run_one(tmp_path, root, m, label_run(m, "reactive"))
    assert res["verdict"] == "VALID" and res["control"]["transitions"] == [] and not world.applies()


def test_reactive_steps_to_b5_on_degradation_and_back_on_recovery(tmp_path, monkeypatch):
    monkeypatch.setitem(fk.LAT, 2, 400)                       # B=2 "degrades" above the 300 ms threshold; B=5 (150 ms) is healthy
    root = tmp_path / "s"
    m = ST.stage_m2(root, window=WINDOW, k=1.0, reactive=REACTIVE, surface_points=surface(2), provenance={}, replicates=1)
    res, world, ctx, arm = run_one(tmp_path / "w", root, m, label_run(m, "reactive"))
    assert res["verdict"] == "VALID", res.get("step_failure")
    t = [(x["decision_index"], x["from_B"], x["to_B"]) for x in res["control"]["transitions"]]
    assert t[0] == (1, 2, 5)                                  # two consecutive samples >= 300 ms (rounds 0 and 1), then a step to 5
    assert any(x[1:] == (5, 2) for x in t)                    # recovery: B=5 mean 150 <= 240 for two samples -> back to the default B=2
    assert all(c[c.index("--target-b") + 1] in ("2", "5") for c in world.applies())


def test_broadcast_limit_zero_refuses_any_switch(tmp_path, monkeypatch):
    monkeypatch.setitem(fk.LAT, 2, 400)
    root = tmp_path / "s"
    m = ST.stage_m2(root, window=WINDOW, k=1.0, reactive=REACTIVE, surface_points=surface(2), provenance={}, replicates=1)
    res, world, ctx, arm = run_one(tmp_path / "w", root, m, label_run(m, "reactive"), auth={"limit": "0"})
    assert res["verdict"] == "FAILED_ACTUATION" and not world.applies()


# ------------------------------------------------------------------------------------------------------------------ actuation check
def test_actuation_check_exercises_every_menu_target_in_both_directions(tmp_path):
    root = tmp_path / "act"
    m = ST.stage_act(root, protocol_sha256="f" * 64)
    assert m["kind"] == "ACT" and [r["run_id"] for r in m["runs"]] == ["ca01"] and m["schedule_B"] == [5, 10, 5, 2, 2, 2]
    res, world, ctx, arm = run_one(tmp_path / "w", root, m, "ca01", auth={"limit": "4"})
    assert res["verdict"] == "VALID", res.get("step_failure")
    assert [(t["from_B"], t["to_B"]) for t in res["control"]["transitions"]] == [(2, 5), (5, 10), (10, 5), (5, 2)]
    assert len(world.applies()) == 4 and res["final_config"]["max_message_count"] == 2


# ------------------------------------------------------------------------------------------------------------------ environment hygiene
def test_runner_environment_is_scoped_and_restores_everything(mstage, tmp_path):
    root, m = mstage
    run_one(tmp_path, root, m, label_run(m, "static_b2"))
    assert (v1_runner.build_policy, v1_runner.actuator_argv) == ORIGINAL_RUNNER_GLOBALS                  # captured at import, before any M2 run
    assert v1_runner.build_policy.__module__.endswith("closed_loop.policies") and v1_runner.actuator_argv.__module__.endswith("closed_loop.runner")
    assert (ca.DEFAULT_MIN_B, ca.DEFAULT_MAX_B) == (10, 800)


def test_without_the_m2_environment_the_owner_file_with_targets_2_5_10_is_refused(mstage, tmp_path):
    root, m = mstage
    run = m["runs"][0]
    a = auth(tmp_path, m["stage_sha256"], [r["run_id"] for r in m["runs"]], [r["channel"] for r in m["runs"]])
    ctx = v1_runner.Ctx(root, run["run_id"], a, tmp_path / "o", Path("/n"), tmp_path / "c", Path("/m"))
    with pytest.raises(v1_runner.RunRefused, match="allowed MaxMessageCount range"):
        v1_runner.execute(ctx, None)


def test_a_wrong_actuator_pin_stops_the_runner_before_anything_runs(mstage, tmp_path):
    root, m = mstage
    run = m["runs"][0]
    a = auth(tmp_path, m["stage_sha256"], [r["run_id"] for r in m["runs"]], [r["channel"] for r in m["runs"]])
    ctx = v1_runner.Ctx(root, run["run_id"], a, tmp_path / "o", Path("/n"), tmp_path / "c", Path("/m"))
    with pytest.raises(rn.policy.PolicyError):
        rn.execute(ctx, None, expected_actuator_sha256="0" * 64)
    assert not (tmp_path / "o").exists()


def test_runner_main_dry_listing_does_not_execute(mstage, tmp_path, capsys):
    root, m = mstage
    run = m["runs"][0]
    a = auth(tmp_path, m["stage_sha256"], [r["run_id"] for r in m["runs"]], [r["channel"] for r in m["runs"]])
    rc = rn.main(["--stage", str(root), "--run-id", run["run_id"], "--authorization", str(a), "--out-root", str(tmp_path / "o")])
    assert rc == 0 and "DRY LISTING" in capsys.readouterr().out and not (tmp_path / "o").exists()


# ------------------------------------------------------------------------------------------------------------------ final table
def test_final_table_from_fake_runs_reports_equal_controllers_and_a_meaningful_b10_contrast(mstage, tmp_path):
    root, m = mstage
    out_root = tmp_path / "out"
    for run in m["runs"]:
        a = auth(tmp_path, m["stage_sha256"], [r["run_id"] for r in m["runs"]], [r["channel"] for r in m["runs"]])
        ctx = v1_runner.Ctx(root, run["run_id"], a, out_root, Path("/net.json"), tmp_path / "cws", Path("/msp"), python="python3")
        res = rn.execute(ctx, World(ctx, json.loads((root / run["run_id"] / "arm.json").read_text())))
        assert res["verdict"] == "VALID", (run["run_id"], res.get("step_failure"))
    doc = FC.build(root, out_root, tmp_path / "report", label="SYNTHETIC")
    prim = doc["primary"]
    assert set(prim["columns"]) == set(P.ARMS)
    assert prim["verdict"]["status"] == "NO_MEANINGFUL_DIFFERENCE"                  # Static B=2 == Reactive == Predictive in this plant
    assert "every controller selected b=2" in prim["verdict"]["text"].lower()
    s = prim["summary"]
    assert s["static_b2"]["mean_ms"]["mean"] == 60 and s["reactive"]["mean_ms"]["mean"] == 60 and s["predictive_live"]["mean_ms"]["mean"] == 60
    assert s["static_b10"]["mean_ms"]["mean"] == 480 and s["static_b2"]["runs_used"] == 2
    assert prim["pairwise"]["static_b10_vs_static_b2"]["mean_ms"]["verdict"] == "MEANINGFUL_A_WORSE"
    assert prim["pairwise"]["predictive_live_vs_static_b2"]["p95_ms"]["verdict"] in ("EQUIVALENT_WITHIN_SESOI", "NOT_DISTINGUISHABLE")
    assert prim["decisions"]["all_primary_arms_held_B2"] is True
    assert prim["decisions"]["predictive_live"]["decisions"] == 20 and prim["decisions"]["predictive_live"]["config_changes_applied"] == 0
    names = {p.name for p in (tmp_path / "report").iterdir()}
    assert {"final_comparison.md", "final_comparison.json", "run_records.csv", "segments.csv", "forecast_accuracy.csv", "SHA256SUMS.txt"} <= names
    md = (tmp_path / "report" / "final_comparison.md").read_text()
    assert "SYNTHETIC" in md and "Static (B=2)" in md and "Predictive" in md and "p95 latency" in md
    with pytest.raises(FileExistsError):
        FC.build(root, out_root, tmp_path / "report")
    fa = [r for r in doc["records"] if r["arm_label"] == "predictive_live"][0]["forecast"]["summary"]
    assert fa["n"] == 10 and fa["mae_ewma_tps"] >= 0


def test_missing_runs_are_listed_never_imputed(mstage, tmp_path):
    root, m = mstage
    (tmp_path / "out").mkdir()
    doc = FC.build(root, tmp_path / "out", tmp_path / "report")
    assert len(doc["runs_missing"]) == 8 and doc["primary"]["verdict"]["status"] == "INSUFFICIENT_REPLICATION"
